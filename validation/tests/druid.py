# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import functools
from pathlib import Path

from adbc_drivers_validation import model, quirks


class DruidQuirks(model.DriverQuirks):
    name = "druid"
    driver = "adbc_driver_druid"
    driver_name = "ADBC Druid Driver"
    vendor_name = "Apache Druid"
    vendor_version = "37.0.0"
    short_version = "37"
    features = model.DriverFeatures(
        connection_get_table_schema=False,
        connection_set_current_catalog=False,
        connection_set_current_schema=False,
        connection_transactions=True,
        get_objects=False,
        get_objects_constraints_check=False,
        get_objects_constraints_foreign=False,
        get_objects_constraints_primary=False,
        get_objects_constraints_unique=False,
        select_fixture_setup=False,
        statement_bind=True,
        statement_bind_test_mode="select",
        statement_bulk_ingest=False,
        statement_bulk_ingest_catalog=False,
        statement_bulk_ingest_schema=False,
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=True,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        statement_rows_affected_ddl=True,
        supported_xdbc_fields=[],
    )
    setup = model.DriverSetup(
        database={"uri": model.FromEnv("DRUID_URI")},
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        error_str = str(error).lower()
        return "not found" in error_str

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement, dialect=self.name)


@functools.cache
def get_quirks(version: str) -> DruidQuirks:
    quirks = DruidQuirks()
    if version != quirks.short_version:
        raise ValueError(f"Unsupported Druid version: {version}")
    return quirks
