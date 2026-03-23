use crate::database::DruidDatabase;
use adbc_core::Driver;
use adbc_core::error::Result;
use adbc_core::options::{OptionDatabase, OptionValue};

#[derive(Default)]
pub struct DruidDriver {}

impl Driver for DruidDriver {
    type DatabaseType = DruidDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        self.new_database_with_opts(vec![])
    }

    fn new_database_with_opts(
        &mut self,
        _opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        Ok(DruidDatabase {})
    }
}
