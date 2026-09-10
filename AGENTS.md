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

# AGENTS.md

## Project Overview

An ADBC driver for Apache Druid, written in Rust.

## General Guidelines

- IMPORTANT: Use red/green TDD
- Use idiomatic, elegant, and concise code
- Run the full check command before considering code complete
- Use conventional commits for commit messages

## Commands

- Add dependencies: `cargo add [options] <crate>`
- Build the driver: `cargo build`
- Run tests: `cargo test -- --include-ignored`
- Run linter: `cargo clippy`
- Run formatter: `cargo fmt`
- Full check: `cargo fmt --check && cargo clippy -- -D warnings && cargo test -- --include-ignored`
