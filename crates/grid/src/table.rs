//! A tiny column table written to parquet through arrow.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub enum Col {
    Str(Vec<String>),
    I64(Vec<i64>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
}

#[derive(Default)]
pub struct Table {
    cols: Vec<(&'static str, Col)>,
}

impl Table {
    pub fn push(&mut self, name: &'static str, col: Col) {
        self.cols.push((name, col));
    }

    pub fn write(self, path: &Path) -> Result<(), String> {
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for (name, col) in self.cols {
            let (dt, arr): (DataType, ArrayRef) = match col {
                Col::Str(v) => (DataType::Utf8, Arc::new(StringArray::from(v))),
                Col::I64(v) => (DataType::Int64, Arc::new(Int64Array::from(v))),
                Col::F64(v) => (DataType::Float64, Arc::new(Float64Array::from(v))),
                Col::Bool(v) => (DataType::Boolean, Arc::new(BooleanArray::from(v))),
            };
            fields.push(Field::new(name, dt, false));
            arrays.push(arr);
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|e| e.to_string())?;
        let file = File::create(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut w = ArrowWriter::try_new(file, schema, Some(props)).map_err(|e| e.to_string())?;
        w.write(&batch).map_err(|e| e.to_string())?;
        w.close().map_err(|e| e.to_string())?;
        Ok(())
    }
}
