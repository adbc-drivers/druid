import sys
from pathlib import Path

import adbc_drivers_validation
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    manual_test,
    noci,
    pytest_addoption,
    pytest_collection_modifyitems,
)

from .druid import DruidQuirks


@pytest.fixture(scope="session")
def driver(request) -> adbc_drivers_validation.model.DriverQuirks:
    driver = request.param
    assert driver.startswith("druid")
    return DruidQuirks()


@pytest.fixture(scope="session")
def driver_path(driver: adbc_drivers_validation.model.DriverQuirks) -> str:
    ext = {
        "win32": "dll",
        "darwin": "dylib",
    }.get(sys.platform, "so")
    return str(
        Path(__file__).parent.parent.parent
        / f"target/debug/libadbc_driver_{driver.name}.{ext}"
    )
