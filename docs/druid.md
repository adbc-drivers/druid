---
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
{}
---

(driver-druid-prerelease)=
# Druid Driver (unknown)

{badge-primary}`Driver Version|(unknown)` {badge-success}`Tested With|Apache Druid 36`

:::{warning}
This is documentation for a prerelease version.
:::

This driver provides access to [Apache Druid][druid], a high performance, real-time analytics database.

## Installation

The Druid driver can be installed with [dbc](https://docs.columnar.tech/dbc):

```bash
dbc install druid
```

## Connecting

To use the driver, provide the URI of a Druid database as the `uri` option.

```python
from adbc_driver_manager import dbapi

connection = dbapi.connect(
    driver="druid",
    db_kwargs={
        "uri": "http://localhost:8888",
    },
)
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers. See [adbc-quickstarts](https://github.com/columnar-tech/adbc-quickstarts).
