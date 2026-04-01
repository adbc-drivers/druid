import re
from pathlib import Path

from adbc_drivers_validation import model, quirks


class DruidQuirks(model.DriverQuirks):
    name = "druid"
    driver = "adbc_driver_druid"
    driver_name = "ADBC Druid Driver"
    vendor_name = "Apache Druid"
    vendor_version = re.compile(r"36\.[0-9]+\.[0-9]+")
    short_version = "36"
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
        statement_bind=True,
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
        database={"uri": "http://localhost:8888"},
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        error_str = str(error).lower()
        return "not found" in error_str

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement, dialect=self.name)


QUIRKS = [DruidQuirks()]
