## [1.0.12](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.11...v1.0.12) (2026-05-02)


### Bug Fixes

* **deployment:** wire DfeApp::deployment_contract trait hook + bump rustlib to >=2.7.0 ([c240a1b](https://github.com/hyperi-io/dfe-transform-vector/commit/c240a1b460a1674cbb50ec27bbda9e95c4d53476))
* **deps:** pin rustlib to >=2.6.1 ([b92dc51](https://github.com/hyperi-io/dfe-transform-vector/commit/b92dc51efba53c1e1ae11ab970a2617623631a99))

## [1.0.11](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.10...v1.0.11) (2026-04-29)


### Bug Fixes

* **deps:** track rustlib 2.6.0 cli→cli-service rename ([0d29d4c](https://github.com/hyperi-io/dfe-transform-vector/commit/0d29d4c5252a2b14b42c7a30ff41185abca49f29))

## [1.0.10](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.9...v1.0.10) (2026-04-29)


### Bug Fixes

* **deps:** clear 9 security advisories via cargo update ([1185a86](https://github.com/hyperi-io/dfe-transform-vector/commit/1185a862958d6b468d7a011362e59fa93c515152))
* wire Tier 1 jemalloc allocator under feature flag ([fcf97c5](https://github.com/hyperi-io/dfe-transform-vector/commit/fcf97c53c3003c92d24092d710a665ea14584652))

## [1.0.9](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.8...v1.0.9) (2026-04-16)


### Bug Fixes

* add e2e tests with live-first testcontainers fallback ([df2f728](https://github.com/hyperi-io/dfe-transform-vector/commit/df2f728ec1689b209cd9e603ad26ad5e613c6287))

## [1.0.8](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.7...v1.0.8) (2026-04-16)


### Bug Fixes

* config validation hardening, metrics proxy fixes, deny.toml migration ([856aa7d](https://github.com/hyperi-io/dfe-transform-vector/commit/856aa7d8e0e057976a4e254a4b685539f4151344))

## [1.0.7](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.6...v1.0.7) (2026-04-16)


### Bug Fixes

* bump rustlib to v2.5.4, add breaking rule to releaserc ([05454e0](https://github.com/hyperi-io/dfe-transform-vector/commit/05454e046e46f708779a0854ca85f7ab3eb07419))

## [1.0.6](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.5...v1.0.6) (2026-04-02)


### Bug Fixes

* bump rustlib to v2.4.3, add debug/trace logging, fix SensitiveString in tests ([56806f5](https://github.com/hyperi-io/dfe-transform-vector/commit/56806f5455bf7b800d8b460411d7656b550e992b))
* update DfeMetrics::register() to pass &MetricsManager for manifest ([9f56d3c](https://github.com/hyperi-io/dfe-transform-vector/commit/9f56d3c9f888065743a176ee9e24b7ac53900c03))
* update to rustlib v2.x ServiceRuntime + deployment contract fields ([d74ea37](https://github.com/hyperi-io/dfe-transform-vector/commit/d74ea373be76ab74067e82c31d003db99976900e))

## [1.0.5](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.4...v1.0.5) (2026-03-29)


### Bug Fixes

* add log spam protection, security events, disable broken e2e test ([6782774](https://github.com/hyperi-io/dfe-transform-vector/commit/6782774470c8236aa23da64ff3ed594670e26c94))
* remove unused security self-import (clippy) ([35717cd](https://github.com/hyperi-io/dfe-transform-vector/commit/35717cd8da7acb75391337914f9548396d637a19))
* use process.shutdown security events for signals, clarify crash audit intent ([e05b7eb](https://github.com/hyperi-io/dfe-transform-vector/commit/e05b7eb1c733c4594bcb676c1abd4810dacf8942))

# [1.0.0-dev.8](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.0-dev.7...v1.0.0-dev.8) (2026-03-25)


### Bug Fixes

* migrate health/metrics to rustlib HttpServer, add version-check, bump to >=1.19 ([95efad6](https://github.com/hyperi-io/dfe-transform-vector/commit/95efad6fd88d95699119745805857d6e40ebf83a))
* restructure tests to HyperI standards, add smoke and edge-case coverage ([b6d212e](https://github.com/hyperi-io/dfe-transform-vector/commit/b6d212e425e9ddd92661140cd12ef8d047dba8d2))

# [1.0.0-dev.7](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.0-dev.6...v1.0.0-dev.7) (2026-03-21)


### Bug Fixes

* inline Renovate config (preset resolution broken) ([208d9ab](https://github.com/hyperi-io/dfe-transform-vector/commit/208d9ab2123e05a6a6351f48654aee00fac4544b))

# [1.0.0-dev.6](https://github.com/hyperi-io/dfe-transform-vector/compare/v1.0.0-dev.5...v1.0.0-dev.6) (2026-03-20)


### Bug Fixes

* migrate metrics from prometheus crate to MetricsManager ([cd087c4](https://github.com/hyperi-io/dfe-transform-vector/commit/cd087c4161380ccac7665a014c5bb2df8de15445))

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
