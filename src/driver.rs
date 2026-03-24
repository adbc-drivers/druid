use crate::database::DruidDatabase;
use adbc_core::Driver;
use adbc_core::Optionable;
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
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut db = DruidDatabase::new();
        for (key, value) in opts {
            db.set_option(key, value)?;
        }
        Ok(db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::Optionable;

    #[test]
    fn test_new_database() {
        let mut driver = DruidDriver::default();
        let result = driver.new_database();
        assert!(result.is_ok());
    }

    #[test]
    fn test_new_database_with_uri() {
        let mut driver = DruidDriver::default();
        let result = driver.new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String("http://localhost:8888".to_string()),
        )]);
        assert!(result.is_ok());
        let db = result.unwrap();
        assert_eq!(
            db.get_option_string(OptionDatabase::Uri).unwrap(),
            "http://localhost:8888"
        );
    }
}
