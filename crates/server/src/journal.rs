//! The streaming input log the log writer appends to as records arrive (architecture section 5:
//! an append-only file, one record per message). A header (length-prefixed postcard), then one
//! postcard `WireRecord` per message, each self-delimiting. A cut-off last record (a crash
//! mid-write) reads as the end of the log.
//!
//! A market run's journal is also converted, when the run ends, into the batch format of
//! `sim::log` (the one `sim::replay` re-simulates).

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use engine::LogRecord;
use serde::{Deserialize, Serialize};
use sim::log::WireRecord;

pub const JOURNAL_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalHeader {
    pub version: u32,
    /// The record format, `sim::log::FORMAT_VERSION`.
    pub log_format: u32,
    /// "market" or "stress".
    pub mode: String,
    /// The scenario name, or "stress".
    pub scenario: String,
    pub seed: u64,
    /// The scenario TOML (market), or the stress parameters as TOML.
    pub config: String,
}

pub struct JournalWriter {
    out: BufWriter<File>,
    bytes: u64,
}

impl JournalWriter {
    pub fn create(path: &Path, header: &JournalHeader) -> io::Result<Self> {
        let mut out = BufWriter::with_capacity(1 << 20, File::create(path)?);
        let h = postcard::to_allocvec(header).map_err(io::Error::other)?;
        out.write_all(&(h.len() as u32).to_le_bytes())?;
        out.write_all(&h)?;
        Ok(Self {
            out,
            bytes: 4 + h.len() as u64,
        })
    }

    pub fn append(&mut self, r: &LogRecord) -> io::Result<()> {
        let mut buf = [0u8; 64];
        let used = postcard::to_slice(&WireRecord::from(r), &mut buf).map_err(io::Error::other)?;
        self.bytes += used.len() as u64;
        self.out.write_all(used)
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Hand buffered bytes to the OS.
    pub fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }

    /// Flush and wait for the disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.out.flush()?;
        self.out.get_ref().sync_data()
    }
}

/// Reads a journal back, one record at a time, without loading it into memory.
pub struct JournalReader {
    input: BufReader<File>,
    buf: Vec<u8>,
    pos: usize,
    eof: bool,
    pub header: JournalHeader,
}

impl JournalReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut input = BufReader::with_capacity(1 << 20, File::open(path)?);
        let mut len = [0u8; 4];
        input.read_exact(&mut len)?;
        let mut h = vec![0u8; u32::from_le_bytes(len) as usize];
        input.read_exact(&mut h)?;
        let header: JournalHeader = postcard::from_bytes(&h).map_err(io::Error::other)?;
        if header.version != JOURNAL_VERSION {
            return Err(io::Error::other(format!(
                "unsupported journal version {}",
                header.version
            )));
        }
        Ok(Self {
            input,
            buf: Vec::with_capacity(1 << 20),
            pos: 0,
            eof: false,
            header,
        })
    }

    /// Keep at least 64 bytes (more than any record) ahead of `pos`, unless at the end.
    fn fill(&mut self) -> io::Result<()> {
        if self.eof || self.buf.len() - self.pos >= 64 {
            return Ok(());
        }
        self.buf.drain(..self.pos);
        self.pos = 0;
        let mut chunk = [0u8; 1 << 16];
        while self.buf.len() < 64 && !self.eof {
            let n = self.input.read(&mut chunk)?;
            if n == 0 {
                self.eof = true;
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
        // Top up the buffer in larger reads too.
        if !self.eof && self.buf.len() < (1 << 16) {
            let n = self.input.read(&mut chunk)?;
            if n == 0 {
                self.eof = true;
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
        Ok(())
    }

    /// The next record, or `None` at the end (including a cut-off last record).
    pub fn next_record(&mut self) -> io::Result<Option<LogRecord>> {
        self.fill()?;
        if self.pos >= self.buf.len() {
            return Ok(None);
        }
        match postcard::take_from_bytes::<WireRecord>(&self.buf[self.pos..]) {
            Ok((w, rest)) => {
                self.pos = self.buf.len() - rest.len();
                Ok(Some(LogRecord::from(&w)))
            }
            Err(postcard::Error::DeserializeUnexpectedEnd) if self.eof => Ok(None),
            Err(e) => Err(io::Error::other(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::{MsgKind, Side, Tif};

    #[test]
    fn round_trips_and_tolerates_a_cut_off_tail() {
        let dir = std::env::temp_dir().join(format!("journal-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("t.journal");
        let header = JournalHeader {
            version: JOURNAL_VERSION,
            log_format: sim::log::FORMAT_VERSION,
            mode: "stress".into(),
            scenario: "stress".into(),
            seed: 9,
            config: "rate = 1000".into(),
        };
        let recs: Vec<LogRecord> = (1..=50_000u64)
            .map(|seq| LogRecord {
                seq,
                t_ns: seq * 1_000,
                block: seq - 1,
                agent: (seq % 64) as u32,
                kind: match seq % 3 {
                    0 => MsgKind::New {
                        side: Side::Buy,
                        px_ticks: 10_000 + (seq % 17) as i64,
                        qty_lots: seq % 20 + 1,
                        tif: Tif::Gtc,
                        priority_fee: 0,
                    },
                    1 => MsgKind::Cancel {
                        order_id: seq / 2,
                        priority_fee: 0,
                    },
                    _ => MsgKind::Config {
                        param: 6,
                        value: seq,
                    },
                },
            })
            .collect();
        let mut w = JournalWriter::create(&path, &header).unwrap();
        for r in &recs {
            w.append(r).unwrap();
        }
        w.sync().unwrap();
        assert_eq!(w.bytes(), std::fs::metadata(&path).unwrap().len());
        drop(w);

        let mut rd = JournalReader::open(&path).unwrap();
        assert_eq!(rd.header, header);
        let mut back = Vec::new();
        while let Some(r) = rd.next_record().unwrap() {
            back.push(r);
        }
        assert_eq!(back, recs);

        // Cut the last record in half: the reader stops before it.
        let len = std::fs::metadata(&path).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(len - 3).unwrap();
        let mut rd = JournalReader::open(&path).unwrap();
        let mut n = 0;
        while rd.next_record().unwrap().is_some() {
            n += 1;
        }
        assert_eq!(n, recs.len() - 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
