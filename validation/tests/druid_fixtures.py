# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import json
import time
import typing
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

import pyarrow
from adbc_drivers_validation import model

_POLL_INTERVAL_SECONDS = 0.5
_TIMEOUT_SECONDS = 300
_TIMESTAMP = "2000-01-01T00:00:00Z"


@dataclass(frozen=True)
class SelectFixture:
    data_source: str
    dimension_type: str
    values: list[typing.Any]


class DruidFixtures:
    def __init__(self, uri: str) -> None:
        self._uri = uri.rstrip("/")

    def load(self, query_set: model.QuerySet) -> None:
        fixtures = _select_fixtures(query_set)
        existing = set(
            typing.cast(list[str], self._request("/druid/coordinator/v1/datasources"))
        )
        missing = {
            name: fixture for name, fixture in fixtures.items() if name not in existing
        }
        tasks = {self._submit(fixture): fixture for fixture in missing.values()}
        self._wait_for_tasks(tasks)
        self._wait_for_datasources(missing)

    def _submit(self, fixture: SelectFixture) -> str:
        rows = (
            json.dumps(
                {"__time": _TIMESTAMP, "idx": index, "res": value},
                ensure_ascii=False,
                separators=(",", ":"),
            )
            for index, value in enumerate(fixture.values, start=1)
        )
        response = self._request(
            "/druid/indexer/v1/task",
            {
                "type": "index_parallel",
                "spec": {
                    "dataSchema": {
                        "dataSource": fixture.data_source,
                        "timestampSpec": {"column": "__time", "format": "iso"},
                        "dimensionsSpec": {
                            "dimensions": [
                                {"name": "idx", "type": "long"},
                                {
                                    "name": "res",
                                    "type": fixture.dimension_type,
                                },
                            ]
                        },
                        "metricsSpec": [],
                        "granularitySpec": {
                            "type": "uniform",
                            "segmentGranularity": "day",
                            "queryGranularity": "none",
                            "rollup": False,
                        },
                    },
                    "ioConfig": {
                        "type": "index_parallel",
                        "inputSource": {
                            "type": "inline",
                            "data": "\n".join(rows),
                        },
                        "inputFormat": {
                            "type": "json",
                            "keepNullColumns": True,
                        },
                        "appendToExisting": False,
                    },
                    "tuningConfig": {
                        "type": "index_parallel",
                        "maxNumConcurrentSubTasks": 1,
                    },
                },
            },
        )
        return typing.cast(str, response["task"])

    def _wait_for_tasks(self, tasks: dict[str, SelectFixture]) -> None:
        pending = tasks.copy()
        deadline = time.monotonic() + _TIMEOUT_SECONDS
        while pending:
            for task_id, fixture in list(pending.items()):
                task = urllib.parse.quote(task_id, safe="")
                response = self._request(f"/druid/indexer/v1/task/{task}/status")
                status = response["status"]["status"]
                if status == "SUCCESS":
                    del pending[task_id]
                elif status == "FAILED":
                    raise RuntimeError(
                        f"Druid ingestion failed for {fixture.data_source}: {response}"
                    )

            if pending:
                if time.monotonic() >= deadline:
                    names = ", ".join(f.data_source for f in pending.values())
                    raise TimeoutError(f"Druid ingestion timed out: {names}")
                time.sleep(_POLL_INTERVAL_SECONDS)

    def _wait_for_datasources(self, fixtures: dict[str, SelectFixture]) -> None:
        pending = fixtures.copy()
        deadline = time.monotonic() + _TIMEOUT_SECONDS
        while pending:
            for name, fixture in list(pending.items()):
                escaped_name = name.replace('"', '""')
                try:
                    response = self._request(
                        "/druid/v2/sql",
                        {
                            "query": f'SELECT COUNT(*) FROM "{escaped_name}"',
                            "resultFormat": "array",
                        },
                    )
                except RuntimeError:
                    continue
                if response == [[len(fixture.values)]]:
                    del pending[name]

            if pending:
                if time.monotonic() >= deadline:
                    names = ", ".join(pending)
                    raise TimeoutError(
                        f"Druid datasources did not become ready: {names}"
                    )
                time.sleep(_POLL_INTERVAL_SECONDS)

    def _request(
        self, path: str, payload: dict[str, typing.Any] | None = None
    ) -> typing.Any:
        data = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            f"{self._uri}{path}",
            data=data,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            method="GET" if payload is None else "POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            with error:
                detail = error.read().decode(errors="replace")
            raise RuntimeError(
                f"Druid request failed ({error.code} {path}): {detail}"
            ) from error
        except urllib.error.URLError as error:
            raise RuntimeError(
                f"Druid request failed ({path}): {error.reason}"
            ) from error


def _select_fixtures(query_set: model.QuerySet) -> dict[str, SelectFixture]:
    fixtures = {}
    for name, query in query_set.queries.items():
        if not name.startswith("type/select/"):
            continue

        table_name = query.metadata().setup.drop
        if table_name is None or not isinstance(query.query, model.SelectQuery):
            continue

        result = query.query.expected_result()
        column = result.column("res")
        fixtures[table_name] = SelectFixture(
            data_source=table_name,
            dimension_type=_dimension_type(column.type),
            values=_values(column),
        )
    return fixtures


def _dimension_type(data_type: pyarrow.DataType) -> str:
    if data_type == pyarrow.float32():
        return "float"
    if pyarrow.types.is_floating(data_type) or pyarrow.types.is_decimal(data_type):
        return "double"
    if (
        pyarrow.types.is_boolean(data_type)
        or pyarrow.types.is_integer(data_type)
        or pyarrow.types.is_timestamp(data_type)
    ):
        return "long"
    return "string"


def _values(column: pyarrow.ChunkedArray) -> list[typing.Any]:
    data_type = column.type
    if pyarrow.types.is_timestamp(data_type):
        values = column.cast(pyarrow.int64()).to_pylist()
        divisor = {"s": 0.001, "ms": 1, "us": 1_000, "ns": 1_000_000}[data_type.unit]
        if divisor == 0.001:
            return [None if value is None else value * 1_000 for value in values]
        return [None if value is None else value // divisor for value in values]

    values = column.to_pylist()
    if pyarrow.types.is_boolean(data_type):
        return [None if value is None else int(value) for value in values]
    if pyarrow.types.is_binary(data_type):
        return [
            None if value is None else base64.b64encode(value).decode()
            for value in values
        ]
    if pyarrow.types.is_decimal(data_type):
        return [None if value is None else float(value) for value in values]
    if pyarrow.types.is_date(data_type) or pyarrow.types.is_time(data_type):
        return [None if value is None else value.isoformat() for value in values]
    return values
