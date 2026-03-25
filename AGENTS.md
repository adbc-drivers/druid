# AGENTS.md

## Project Overview

An ADBC driver for Apache Druid, written in Rust.

## General Guidelines

- IMPORTANT: Use red/green TDD
- Use conventional commits for commit messages

## Commands

- Add dependencies: `cargo add [options] <crate>`
- Build the driver: `cargo build`
- Run tests: `cargo test -- --include-ignored`
- Run linter: `cargo clippy`
- Run formatter: `cargo fmt`
- Full check: `cargo fmt --check && cargo clippy -- -D warnings && cargo test -- --include-ignored`
