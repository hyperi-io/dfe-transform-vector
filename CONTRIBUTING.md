# Contributing

We welcome contributions to this project. By contributing, you agree to the
terms outlined below.

## Commit Message Format

HyperI projects use [Conventional Commits](https://www.conventionalcommits.org/)
and [semantic-release](https://semantic-release.gitbook.io/) for automated
versioning and changelog generation. All commits must follow this format:

```text
<type>(<scope>): <subject>

[optional body]

[optional footer(s)]
```

### Types

| Type | Description | Version Bump |
|------|-------------|--------------|
| `feat` | A new feature | Minor (0.X.0) |
| `fix` | A bug fix | Patch (0.0.X) |
| `docs` | Documentation only | None |
| `style` | Code style (formatting, semicolons, etc.) | None |
| `refactor` | Code change that neither fixes a bug nor adds a feature | None |
| `perf` | Performance improvement | Patch (0.0.X) |
| `test` | Adding or correcting tests | None |
| `build` | Changes to build system or dependencies | None |
| `ci` | Changes to CI configuration | None |
| `chore` | Other changes that don't modify src or test files | None |
| `revert` | Reverts a previous commit | Varies |

### Breaking Changes

For breaking changes that require a major version bump, add `!` after the type
or include `BREAKING CHANGE:` in the footer:

```text
feat!: remove deprecated API endpoints

BREAKING CHANGE: The /v1/users endpoint has been removed. Use /v2/users instead.
```

### Examples

```text
feat(auth): add OAuth2 support for Google login

fix(api): handle null response from upstream service

docs: update installation instructions for Windows

refactor(core)!: restructure module exports

BREAKING CHANGE: Named exports are now used instead of default exports.
```

### Scope

Scope is optional but recommended. Use it to indicate the area of the codebase
affected (e.g., `api`, `auth`, `core`, `cli`, `docs`).

## Semantic Versioning

This project follows [Semantic Versioning 2.0.0](https://semver.org/):

- **MAJOR** (X.0.0): Breaking changes that require users to modify their code
- **MINOR** (0.X.0): New features that are backwards-compatible
- **PATCH** (0.0.X): Bug fixes and minor improvements

Versions are automatically determined by semantic-release based on commit
messages. Do not manually update version numbers.

## Developer Certificate of Origin

This project uses the Developer Certificate of Origin (DCO) to ensure that
contributors have the right to submit their contributions.

By making a contribution to this project, you certify that:

1. The contribution was created in whole or in part by you and you have the
   right to submit it under the license indicated in the file; or

2. The contribution is based upon previous work that, to the best of your
   knowledge, is covered under an appropriate open source license and you
   have the right under that license to submit that work with modifications;
   or

3. The contribution was provided directly to me by some other person who
   certified (1), (2) or (3) and you have not modified it.

4. You understand and agree that this project and the contribution are public
   and that a record of the contribution (including all personal information
   you submit with it, including your sign-off) is maintained indefinitely
   and may be redistributed consistent with this project or the license(s)
   involved.

## How to Sign Off Your Commits

You must sign off each commit to indicate your acceptance of the DCO. Combine
the signoff with your conventional commit message:

```bash
git commit --signoff -m "feat(auth): add two-factor authentication"
```

This produces:

```text
feat(auth): add two-factor authentication

Signed-off-by: Your Name <your.email@example.com>
```

Make sure your Git configuration has your correct name and email:

```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

## License for Contributions

All contributions to this project are licensed under the Business Source
License 1.1 (BUSL-1.1), the same license that covers the project.

Each version of the software (including your contributions) will automatically
become available under the Apache License, Version 2.0 on the third
anniversary of its release.

## How to Contribute

1. **Fork the repository** and create your branch from `main`
2. **Make your changes** following the commit message format above
3. **Sign off your commits** with the DCO
4. **Test your changes** to ensure they work as expected
5. **Submit a pull request** with a clear description of what you've done

### Pull Request Checklist

- [ ] Commits follow the conventional commit format
- [ ] All commits are signed off (DCO)
- [ ] Tests pass (if applicable)
- [ ] Documentation is updated (if applicable)

## CI/CD Workflow

When your pull request is merged to `main`:

1. **semantic-release** analyses commit messages since the last release
2. Determines the next version number based on commit types
3. Generates/updates the CHANGELOG
4. Creates a new GitHub release with release notes
5. Publishes the package (if applicable)

This happens automatically - no manual intervention required.

## Questions

If you have questions about contributing, please open an issue or contact us.

## Context

### What this is

A Rust supervisor that runs Vector.dev as a child process so a Vector pipeline
behaves like every other DFE app -- one big-dial config, one ops port, an OTLP
push, and a chart assembled from its deployment contract. It is NOT
the transform engine and it does not build Vector: the image downloads the
upstream release binary at the version pinned by `VECTOR_VERSION` in
`src/deployment.rs`, and cargo compiles only the supervisor. It is also not a
singleton and not part of the default deploy -- `dfe-infra/apps.yaml` declares
it `multiplicity: per_config` with `scale_deployed: true` and an empty
`default_in`, so one deployment exists per source config and only when a
deployment asks for it.

### Where things live

| Path | What it holds |
|---|---|
| `src/main.rs` | CLI entry. Fills scalo's `CommonArgs` from the loaded config before `run_app`, which is what makes `metrics.address` and `logging.*` take effect |
| `src/config/` | `loader` -> `validate` -> `generate` -> `wiring` -> `assembler`, plus `reload`, `transforms` and `kafka_defaults` |
| `src/vector/` | `process` spawn and backoff, `lifecycle` states, and `binary` -- version selection kept pure and NOT on the run path, which its own header explains |
| `src/bridge.rs` | The direct-transport translation between scalo `Transport/Push` and Vector `PushEvents`. Does nothing on `bus` |
| `src/health.rs`, `src/metrics.rs`, `src/metrics/scrape.rs` | Readiness publishing, and the scrape that merges `vector_*` into scalo's registry |
| `src/deployment.rs`, `src/vector-layer.dockerfile` | `contract()`, the source the Dockerfile and the released chart are generated from, and the two-binary image override spliced onto scalo's generated Dockerfile |
| `templates/bus.yaml`, `templates/direct.yaml` | Whole runnable Vector topologies, one per transport, commented per field |
| `pipelines/filebeat/` | The shipped filebeat pipeline |
| `docs/` | `architecture.md` for the shape and the invariants, `internals.md` for field-by-field depth, `migration.md`, `librdkafka.md`, the generated `config-schema.*` and `capability-catalog.*` |
| `tests/` | `integration`, `e2e`, `smoke`, and `TESTING.md` for how the broker and Vector binary are resolved |
| `scripts/fetch-vector.sh`, `scripts/pgo-workload.sh` | Downloads the pinned Vector for tests, and drives the PGO/BOLT workload |

### Commands that prove a change

```bash
make check                                   # hyperi-ci check -- quality + test
cargo nextest run --lib                      # unit only, no infrastructure
cargo nextest run --test integration         # config assembly, wiring, lifecycle, metrics
cargo nextest run --test smoke               # CLI surface
cargo nextest run --test e2e                 # needs Docker or a live broker
cargo nextest run --run-ignored all          # everything, opt-in cases included
```

What green does NOT mean:

- **The default run skips the Vector validator.** Seven `#[ignore]` cases need a
  real Vector binary -- six in `tests/integration/vector_validate.rs` and one in
  `tests/e2e/kafka.rs`. Until you pass `--run-ignored all`, nothing has run
  Vector's own validator over an assembled config.
- **Locally, a missing broker or Vector binary SKIPS rather than fails.** In CI
  that same absence is an assertion failure instead, so CI is stricter than a
  green laptop run.
- **`e2e::filebeat_kafka` runs by default but skips without a sibling repo.** The
  corpus and elastic's golden events live in dfe-transform-vrl, so it needs that
  repo checked out beside this one or `DFE_TRANSFORM_VRL_DIR` pointing at it.
- **A docs-only push runs no CI at all.** `paths-ignore` for `docs/**` and
  `**.md` is on the `push` trigger but not on `pull_request`, so the PR is the
  only place a docs change is checked. A silent push is not a pass.

### What tends to bite

| Don't | Do | Why |
|---|---|---|
| Hand-edit `Dockerfile`, or commit a chart | Fix `src/deployment.rs::contract()` and regenerate | The Dockerfile is generator output and the release assembles the chart from the contract, so a hand edit is reverted or never ships |
| Bump scalo and leave `release.helm.library` behind | Move `release.helm.library` in `.hyperi-ci.yaml` to the same scalo version | A scalo-service release renders only the contract version its scalo release writes |
| Turn the Kafka lag trigger back on in `contract()` | Leave `KafkaLagTrigger::disabled()` and scale on CPU plus scaling pressure | Lag rises when a downstream stage breaks, so a lag trigger adds pods that wait on the same broken stage. The library's trigger also reads `config.kafka.*`, which this app does not have, and refuses to render |
| Set `vector.version` in a deployment's config and forget it | Leave it unset, so the default reads `VECTOR_VERSION` | A pinned version that differs from the binary the image bakes refuses to start under `strict`, and runs only because `version_check: warn` lets it |
| Mount a whole `templates/*.yaml` as a transform file | Copy only its `transforms:` block | The supervisor generates `sources:` and `sinks:` from the big dials and refuses a transform file declaring them |
| Change `password` to a `SensitiveString` to look safer | Leave it a `String` | `SensitiveString` serialises as `***REDACTED***` and the figment serialize-merge-deserialize round trip in `apply_figment_env()` destroys the value. Masking happens in logs and `Debug`, and Vector reads the credential from files through its `directory` secret backend, so it never lands in the Vector config |
| Put a `${VAR}` placeholder in a credential or a transform | Mount the secret and set `sasl.secret_dir`, or give the credential through `DFE_TRANSFORM_VECTOR_{SOURCE,SINK}_SASL_*`, and read the environment in VRL with `get_env_var` | Vector 0.57+ expands no `${VAR}` without `--dangerously-allow-env-var-interpolation`, which the supervisor never passes, so a credential holding `${` is refused at load rather than reaching the broker as the password |
| Assume a config key you added is wired because it parses and validates | Prove it changes behaviour, and add it to the standing check that every committed config file loads | A key can parse, validate and reach nothing, so a deployment that moves a port on it keeps the old port and loses every probe |
| Set `enable.auto.commit: false` here, as the shared DFE consumer baseline does | Leave auto-commit on | Vector's kafka source only stores offsets and leaves librdkafka's commit timer to flush them, and that timer is armed only when auto-commit is on, so with it off the stored offsets never reach the broker and consumer lag never clears |
| Read `/readyz` as "carrying traffic" | Read it as "the child was alive 500ms after spawn, and the sink is not stalled" | Readiness is those two signals and nothing more, so a pod can be Ready with no record moving |
| Swap the reload poller for inotify | Keep polling | S3-backed mounts -- s3fs, goofys, Mountpoint for S3 -- generate no filesystem notification events, so inotify works on every laptop and silently stops reloading in production |
| Add or rename a credential field without a schema marker | Schema it as `scalo::SensitiveString` with `#[schemars(with = ...)]`, as `SaslConfig::password` is | That puts `x-scalo-secret` and `writeOnly` on the field in the emitted config-schema, which is what tells a console to mask it. Without it, only dfe-engine's leaf-name fallback (`password`, `secret`, `token`, `api_key`, `private_key`, `passphrase`) redacts it, so a name outside that set returns an operator's Kafka password in the clear |

### Where this sits

Generated from `dfe-infra/suite.yaml` via
`python3 ../dfe-infra/scripts/dfe-stack suite --consumer dfe-transform-vector`
and `--producer dfe-transform-vector`.

Inbound -- what this repo depends on:

- **scalo-rs**, `cargo-dep`. `Cargo.toml` declares the `scalo` crate by range,
  with a second range covering the dev dependency. A scalo release reaches this
  repo, so widen the range if it does not admit the new version, then rebuild.
- **scalo-rs**, `generated-file`, lockstep. The committed `Dockerfile` is written
  by `scalo::deployment::generate_dockerfile()` -- its own header names the
  generator and the schema version -- and this repo splices the Vector layer onto
  it. When the generator or its schema moves, regenerate with
  `dfe-transform-vector emit-dockerfile > Dockerfile` and commit the diff. The
  released chart is assembled on the scalo-service library chart at
  `release.helm.library`, which moves with the scalo version in `Cargo.toml`.

Outbound -- what depends on this repo:

- **dfe-infra**, `image-pin`, lockstep. `dfe-infra/versions.yaml` pins this
  repo's container image as a tag plus the digest that makes the tag
  immutable, and the thin chart released here, which Argo deploys. Bump the
  tag and re-resolve the digest.

Two more relationships that are real but are NOT declared edges in the suite
graph, so no gate enforces them:

- **dfe-infra `apps.yaml`** is the SSoT for this app's shape -- multiplicity,
  scaling, transports, endpoints and which compiler derives its routing.
  dfe-engine reads that manifest, so adding or changing the app is a manifest
  edit, never an engine release.
- **dfe-transform-vrl** holds the filebeat corpus, the bundled pipeline and the
  documented divergences that `e2e::filebeat_kafka` grades against. The
  dependency is a test-time repo checkout only, and the case skips without it.
