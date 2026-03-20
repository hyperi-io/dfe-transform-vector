# [1.0.0-dev.5](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.0-dev.4...v1.0.0-dev.5) (2026-03-20)


### Bug Fixes

* migrate e2e tests to rustlib transport-kafka, bump rustlib to v1.16.7 ([da4584a](https://github.com/hyperi-io/dfe-transform-vector/commit/da4584a46002e1e81ffba26ddf737cd0e51b6e02))

# [1.0.0-dev.4](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.0-dev.3...v1.0.0-dev.4) (2026-03-19)


### Bug Fixes

* add DfeSource convention, bump rustlib to v1.16.5 ([5bd7727](https://github.com/hyperi-io/dfe-transform-vector/commit/5bd7727e2bc105392d0b494e50efa67f9b2d39b3))

# [1.0.0-dev.3](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.0-dev.2...v1.0.0-dev.3) (2026-03-19)


### Bug Fixes

* rustlib v1.16.3 observability remediation ([0bcab5a](https://github.com/hyperi-io/dfe-transform-vector/commit/0bcab5ae63654a2b39131057d389dbb02ce8b209))

# [1.0.0-dev.2](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.0-dev.1...v1.0.0-dev.2) (2026-03-18)


### Bug Fixes

* code review remediation — lints, metrics, error handling, style ([045b0fc](https://github.com/hyperi-io/dfe-transform-vector/commit/045b0fcb9da427330dceed58e26d75a375343cd1))
* hot-reload allowlist — only transforms are safe, all else requires restart ([2bfef0f](https://github.com/hyperi-io/dfe-transform-vector/commit/2bfef0faf3be15a21924d889a88e5a4428c75fa7))

# 1.0.0-dev.1 (2026-03-17)


### Bug Fixes

* add build.type app, remove legacy publish workflow ([6ffbff3](https://github.com/hyperi-io/dfe-transform-vector/commit/6ffbff3bbd7b2f983c45bd1025cfe31099320f3e))
* add figment env cascade and flat env var overrides ([c06f75a](https://github.com/hyperi-io/dfe-transform-vector/commit/c06f75ab7bf2e2c5fc1b9b1983295f1a94077793))
* buffer config, Kafka production tuning, central librdkafka defaults [skip ci] ([573df67](https://github.com/hyperi-io/dfe-transform-vector/commit/573df67753d01a499be1e029df9224314f73337c))
* enable cross-compilation, container, and Helm publishing in CI ([e099958](https://github.com/hyperi-io/dfe-transform-vector/commit/e0999586d793deb3d491c91de948235c936053b8))
* exclude ai, ci, chart, docs dirs from cargo publish package [skip ci] ([cfb324b](https://github.com/hyperi-io/dfe-transform-vector/commit/cfb324bd8737768e661b1469d7738352d289b842))
* migrate to hyperi-ci, switch rustlib to crates.io ([1d02b06](https://github.com/hyperi-io/dfe-transform-vector/commit/1d02b06b0230869b91a6a5ac0d057790d26e4f43))
* use ubuntu 24.04 LTS base image in Dockerfile ([7a41141](https://github.com/hyperi-io/dfe-transform-vector/commit/7a41141debd0ede6244e0452bfb5879db1685a11))


### Features

* add config engine — YAML generation, transform loader, DAG wiring, assembler ([8fc3996](https://github.com/hyperi-io/dfe-transform-vector/commit/8fc39967a455cbdb619fe60a295f76aefad29cb3))
* add full big-dial config schema with validation ([ee89e22](https://github.com/hyperi-io/dfe-transform-vector/commit/ee89e22131c0d3f12143038008659a15b302ce05))
* add health and metrics HTTP servers ([3ab26eb](https://github.com/hyperi-io/dfe-transform-vector/commit/3ab26eb11e17328bbbbef7782c0ba06d0bd5e3de))
* add Helm chart with StatefulSet, KEDA, and PVC support ([1d1fc04](https://github.com/hyperi-io/dfe-transform-vector/commit/1d1fc0482003a8f1fc2b059f28fc7a5e21f9a6fb))
* add hot-reload system — poll-based file watcher + SIGHUP ([59779fd](https://github.com/hyperi-io/dfe-transform-vector/commit/59779fd68cc5f8675fbba23fb11d97fcfe34d226))
* add multi-stage Dockerfile with Vector binary ([2345818](https://github.com/hyperi-io/dfe-transform-vector/commit/234581801c788ae736461b20bd01c71b78db3f65))
* add Vector subprocess manager with lifecycle, signals, and crash recovery ([5067a8b](https://github.com/hyperi-io/dfe-transform-vector/commit/5067a8b389f10a47e07ab8e9140e53db11d68187))
* proxy Vector internal metrics from prometheus_exporter sink ([27be0a0](https://github.com/hyperi-io/dfe-transform-vector/commit/27be0a08e9e40f2b1300e10d4297378e6a4ca06e))
* rust project scaffold with config, CLI, and build tooling ([2008a04](https://github.com/hyperi-io/dfe-transform-vector/commit/2008a045869fc00aaf2b6d10fd90606967a1afee))
* wire hyperi-rustlib CLI and deployment modules ([4919a26](https://github.com/hyperi-io/dfe-transform-vector/commit/4919a262d415b02b5b4c486d4b0568188c246888))
* wire orchestrator loop in main — config, subprocess, health, metrics ([8f64aa0](https://github.com/hyperi-io/dfe-transform-vector/commit/8f64aa05ccdfe40a67d38413292c6b4efd365d69))

# Changelog
