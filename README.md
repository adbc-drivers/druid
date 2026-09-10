<!--
Copyright (c) 2026 ADBC Drivers Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ADBC Driver for Apache Druid

![Vendor: Apache Druid](https://img.shields.io/badge/vendor-Apache%20Druid-blue?style=flat-square)
![Implementation: Rust](https://img.shields.io/badge/implementation-Rust-violet?style=flat-square)
![Status: Experimental](https://img.shields.io/badge/status-experimental-red?style=flat-square)

This project is not part of the Apache Software Foundation.

An [ADBC driver](https://arrow.apache.org/adbc/) for
[Apache Druid](https://druid.apache.org/).

## Installation

Pre-packaged prerelease builds are available for various platforms from the
[Columnar](https://columnar.tech/) CDN. They can be installed by any tool that
supports [ADBC](https://arrow.apache.org/adbc/) Driver Manifests, such as
[dbc](https://docs.columnar.tech/dbc):

```sh
dbc install --pre druid
```

Only prerelease versions of the driver are currently available, so `--pre` is
required.

See [Building](#building) if you would rather build the driver yourself.

## Usage

The driver can be loaded by any ADBC driver manager. For example, with the
[Python ADBC driver manager](https://pypi.org/project/adbc-driver-manager/):

```python
from adbc_driver_manager import dbapi

connection = dbapi.connect(
    driver="druid",
    db_kwargs={"uri": "druid://localhost:8888?tls=false"},
)
```

See the [driver documentation](docs/druid.md) for connection options, TLS
configuration, and supported features and types.

## Building

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
