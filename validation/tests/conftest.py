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

import sys
from pathlib import Path

import adbc_drivers_validation
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    db_kwargs,
    manual_test,
    noci,
    pytest_collection_modifyitems,
)

from .druid import get_quirks
from .druid_fixtures import DruidFixtures


def pytest_addoption(parser):
    adbc_drivers_validation.tests.conftest.pytest_addoption(parser)
    parser.addoption("--vendor-version", action="store", default="36")


@pytest.fixture(scope="session")
def driver(request, pytestconfig) -> adbc_drivers_validation.model.DriverQuirks:
    driver = request.param
    assert driver.startswith("druid")
    return get_quirks(pytestconfig.getoption("vendor_version"))


@pytest.fixture(scope="session")
def driver_path(driver: adbc_drivers_validation.model.DriverQuirks) -> str:
    ext = {
        "win32": "dll",
        "darwin": "dylib",
    }.get(sys.platform, "so")
    return str(
        Path(__file__).parent.parent.parent
        / f"build/libadbc_driver_{driver.name}.{ext}"
    )


@pytest.fixture(scope="session", autouse=True)
def setup_druid_fixtures(
    driver: adbc_drivers_validation.model.DriverQuirks,
    request: pytest.FixtureRequest,
) -> None:
    database_options = request.getfixturevalue("db_kwargs")
    DruidFixtures(str(database_options["uri"])).load(driver.query_set)
