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

An [ADBC driver](https://arrow.apache.org/adbc/current/index.html) for [Apache Druid](https://druid.apache.org).

## Local Druid

The repository includes a single-container Apache Druid 36 micro-quickstart environment. Start it with:

```bash
docker compose up --detach --wait test-service
```

The Druid SQL API and web console are available at <http://localhost:8888>. The container starts without any datasources; loading test data is a separate step.

Verify it with a simple query:

```bash
curl --fail --silent --show-error \
  --header 'Content-Type: application/json' \
  --data '{"query":"SELECT 1","resultFormat":"array"}' \
  http://localhost:8888/druid/v2/sql
```

Stop the stack with `docker compose down`. To also remove its persisted metadata and segments, run `docker compose down --volumes`.
