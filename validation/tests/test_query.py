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

import pytest
from adbc_drivers_validation.tests.query import (
    TestQuery as BaseTestQuery,
)
from adbc_drivers_validation.tests.query import (
    generate_tests,
)

from .druid import get_quirks


class TestQuery(BaseTestQuery):
    @pytest.mark.xfail(
        strict=True, reason="Druid driver only supports one bound parameter row"
    )
    @pytest.mark.parametrize(
        "batch_size,empty_batches",
        [
            pytest.param(None, False, id="single_batch"),
            pytest.param(2, False, id="multiple_batches"),
            pytest.param(2, True, id="multiple_batches_with_empty"),
        ],
    )
    def test_query_bind_stream(
        self, driver, conn, query, batch_size, empty_batches
    ) -> None:
        super().test_query_bind_stream(driver, conn, query, batch_size, empty_batches)


def pytest_generate_tests(metafunc) -> None:
    quirks = [get_quirks(metafunc.config.getoption("vendor_version"))]
    return generate_tests(quirks, metafunc)
