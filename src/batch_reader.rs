use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};

pub struct SingleBatchReader {
    batch: Option<RecordBatch>,
    schema: SchemaRef,
}

impl Iterator for SingleBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl RecordBatchReader for SingleBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
