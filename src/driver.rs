// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
