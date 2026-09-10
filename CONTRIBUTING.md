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

# How to Contribute

All contributors are expected to follow the [Code of
Conduct](https://github.com/adbc-drivers/druid?tab=coc-ov-file#readme).

## Reporting Issues and Making Feature Requests

Please file bug reports and feature requests on the [GitHub issue
tracker](https://github.com/adbc-drivers/druid/issues). For bugs, include
reproduction steps, the driver and Druid versions, and any relevant logs.

Potential security vulnerabilities should not be reported in a public issue.
Email
[security@adbc-drivers.org](mailto:security@adbc-drivers.org) and review the
[Security Policy](https://github.com/adbc-drivers/druid?tab=security-ov-file#readme).

## Build and Test

Install these prerequisites:

- [Rust](https://www.rust-lang.org/tools/install); the repository's
  `rust-toolchain.toml` selects the supported toolchain and components.
- [pixi](https://pixi.sh/) for packaging and validation tasks.
- Docker with Compose for integration and validation tests.
- [pre-commit](https://pre-commit.com/) for repository checks.

For basic development, the driver can be built like any Rust project. From the
repository root:

```shell
cargo build
```

The integration tests require the included Apache Druid 37 micro-quickstart
service. Start it before running the full test suite:

```shell
docker compose up --detach --wait test-service
cargo test -- --include-ignored
docker compose down
```

The Druid SQL API and web console are available at <http://localhost:8888>.
Use `docker compose down --volumes` if you also want to remove persisted test
data.

`cargo build` does not produce the shared library used by ADBC driver
managers. Install [pixi](https://pixi.sh/), then run:

```shell
pixi install
pixi run make
```

To run the ADBC validation suite against the local Druid service:

```shell
set -a
source .env.linux
set +a
pixi run validate --vendor-version 37
```

This produces a test report that can be rendered as MyST Markdown:

```shell
pixi run gendocs --output generated/
```

Then look at `./generated/druid.md`.

## Opening a Pull Request

Before opening a pull request:

- Use red/green test-driven development.
- Review your changes and make sure no stray files are included.
- Ensure applicable new files have the Apache license header.
- Check for an existing issue. If there is none, file one unless the change is
  trivial. Assign the issue to yourself by commenting just the word `take`.
- Keep Rust code idiomatic, concise, and formatted with `cargo fmt`.
- Address all `cargo clippy` warnings.
- Install [pre-commit](https://pre-commit.com/) and run the static checks. Make
  sure your changes are staged or committed because unstaged changes are
  ignored by the license check.

Run the full check before submitting:

```shell
pre-commit run --all-files
cargo fmt --check && cargo clippy -- -D warnings && cargo test -- --include-ignored
```

When writing the pull request description:

- Ensure the title follows [Conventional
  Commits](https://www.conventionalcommits.org/en/v1.0.0/) format. The component
  can be omitted. Example titles:

  - `feat: support a new Druid type`
  - `chore: update action versions`
  - `fix!: change timestamp precision`

  Flag breaking changes with a `!`, as shown in the last example.
- End the description with `Closes #NNN`, `Fixes #NNN`, or similar so the
  issue is linked to the pull request.
