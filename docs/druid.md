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

{{ cross_reference|safe }}
# Apache Druid Driver {{ version }}

{{ heading|safe }}

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
        "uri": "druid://localhost:8888?tls=false",
    },
)
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers. See [adbc-quickstarts](https://github.com/columnar-tech/adbc-quickstarts).

### Connection String Format

```text
druid://[username[:password]@]host[:port][/path][?tls=true|false&tls_ca=path]
```

Components:

- Scheme: `druid://` (also accepts `http://` and `https://`)
- `username`: HTTP Basic authentication username (optional)
- `password`: HTTP Basic authentication password (optional; requires a username)
- `host`: Druid Router or Broker host (required)
- `port`: Service port (optional; defaults to 443 for HTTPS and 80 for HTTP)
- `path`: Base path when Druid is exposed through a reverse proxy (optional)
- `tls`: Whether to use HTTPS; defaults to `true` and only applies to
  `druid://` URIs
- `tls_ca`: Path to a PEM CA certificate used to verify the server

#### HTTPS/SSL Configuration

The `druid://` scheme uses HTTPS and the system trust store by default. To
connect to a plaintext Druid endpoint, set `tls=false`.

Examples:

- `druid://druid.example.com` → HTTPS on port 443
- `druid://druid.example.com:9088` → HTTPS on port 9088
- `druid://localhost:9088?tls_ca=/path/to/ca.crt` → HTTPS with a
  custom CA
- `druid://localhost:8888?tls=false` → HTTP on port 8888
- `https://druid.example.com:9088` → Explicit HTTPS URL
- `http://localhost:8888` → Explicit HTTP URL

Reserved characters in credentials must be percent-encoded. For example, `@`
becomes `%40`. Credentials can instead be supplied with the ADBC `username`
and `password` database options; those options override credentials in the URI
and are recommended when the URI may appear in logs or shell history.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

{{ footnotes|safe }}

## Options

### Statement Options

``druid.statement.<option_name>``
: Type: string, integer, or double.

  Sets a Druid SQL query-context parameter. The driver removes the
  ``druid.statement.`` prefix and sends the remaining option name to Druid.
  For example, ``druid.statement.timeout`` sets Druid's ``timeout`` context
  parameter. Byte values are not supported.

## Compatibility

{{ compatibility_info|safe }}

[druid]: https://druid.apache.org/
