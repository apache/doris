# AGENTS.md — fe-connector

Architecture, the module map, and the new-connector walkthrough live in
`README.md` next to this file — read that first. This file is operational:
how to build and test, and which invariants you must not break.

## Build and Test Recipes

Run from the repository root; `-am` also builds upstream reactor deps.

```bash
# Build one connector module and run its tests
mvn -f fe/pom.xml -pl :fe-connector-<module> -am test

# Run a single test class. Keep -DfailIfNoTests=false: with -am, upstream
# modules have no matching tests and would fail the build otherwise.
mvn -f fe/pom.xml -pl :fe-connector-spi -am test \
    -Dtest=ConnectorMetadataSurfaceTest -DfailIfNoTests=false
```

- When validating deletions or refactors, add
  `-Dmaven.build.cache.enabled=false`; the build cache can mask stale
  artifacts.
- **fe-connector-paimon is verified with `install` (or `package`), not
  `test`**: its plugin zip and its shade dependency bind to the `package`
  phase, so `test`-level runs skip what actually ships.
- Connector tests use recording fakes (e.g. `RecordingConnectorContext` in
  fe-connector-paimon and fe-connector-iceberg); the connector poms carry no
  mocking framework — keep it that way.
- Checkstyle is part of the build: `UnusedImports` and `AvoidStaticImport`
  are enforced, and test sources are scanned too
  (`includeTestSourceDirectory`).

## Machine-Checked Obligations

Two architecture gates run in the `validate` phase (scripts and their
self-tests live in `build-support/` and `build-support/tests/`):

1. **Forbidden imports** — `build-support/check-fe-connector-imports.sh`,
   wired into this directory's `pom.xml`. fe-connector modules must not
   import fe-core internals, in main OR test sources. When a connector needs
   something from the engine, the fix is to extend the SPI in
   fe-connector-spi — never to import fe-core.
2. **Metadata funnel** — `build-support/check-fe-core-metadata-funnel.sh`,
   wired into fe-core's `pom.xml`. Inside fe-core, only
   `PluginDrivenMetadata` may call `Connector#getMetadata`; exempt call
   sites carry a `getMetadata-funnel-exempt` marker, and deleting a marker
   auto-tightens the gate.

**Changing the shared SPI surface (fe-connector-spi):** regenerate BOTH
recorded baselines in the SAME commit — `connector-metadata-methods.txt`
(`ConnectorMetadataSurfaceTest`) and `connector-plugin-surface.txt`
(`ConnectorPluginSurfaceTest`), under
`fe-connector-spi/src/test/resources/`; run the test and copy the "actual"
set from the failure output. Any surface change is a MAJOR change: bump
`connector.plugin.api.version` in `fe/fe-connector/pom.xml` and the version
pinned in `ConnectorPluginSurfaceTest` in that same commit. Always run
fe-connector-spi's OWN suite after an SPI change; running only consumer
modules will not catch a stale baseline. The same suite pins the
`@ConnectorMustImplement` set, so promoting a method into the minimum
implementation set is a deliberate, reviewed change.

## Invariants Without a Gate

Guarded by tests and reviewed comments rather than build gates. Every one of
these has a concrete failure mode.

- **Generic planning code stays connector-agnostic.** No source-name
  branching in shared fe-core nodes; per-source behavior goes through the
  SPI, optional behavior through the capability opt-ins (README, "Reading
  the API").
- **fe-core parses no connector properties.** Metadata-connection
  properties: the connector or the metastore layer. Storage properties:
  `fe-filesystem`.
- **TCCL pinning at plugin boundaries.** Plugins load child-first; by-name
  reflection or worker threads inside a plugin resolve classes against the
  thread-context classloader, and an unpinned TCCL yields split-brain class
  duplicates (`ClassCastException`, poisoned statics). Engine-side loci are
  the `onPluginClassLoader` call sites (`PluginDrivenScanNode`,
  `PluginDrivenExternalCatalog`, `PluginDrivenSysExternalTable`,
  `MetastoreEventSyncDriver`); connector-side templates are
  `TcclPinningConnectorContext` (paimon),
  `IcebergConnector#pinIcebergWorkerPoolToPluginClassLoader` (library worker
  pools), and `HmsConfHelper` (conf-cached classloader). Enumerate loci with
  `grep -rn "onPluginClassLoader\|TcclPinning"`.
- **Authorization-sensitive cache isolation.** A cross-query cache must
  never serve metadata whose loading path carries per-user authorization —
  list-access must not imply load-access. Reviewed invariant with no gate:
  see the `ATTN` comment in `IcebergConnector` and
  `IcebergConnectorCacheTest`.
- **Nested schema field names are lowercased level by level** before
  publishing historical schema info (the BE indexes struct children by
  lowercase key; a mixed-case child crashes it). Guard:
  `IcebergSchemaUtilsTest`.
- **Statement-scope namespaces are `getType()`-prefixed constants.** Each
  connector's own unit test guards its prefix (`EsStatementScopeTest`,
  `IcebergStatementScopeTest`, `HudiStatementMemoTest`, ...). Convention:
  README, "Reading the API".

## Task Recipes

- **Change the SPI surface**: edit fe-connector-spi → regenerate both
  baselines and bump the plugin API version in the same commit → run
  fe-connector-spi's suite → adjust
  affected connectors (grep for overrides) → run their module tests.
- **Fix a connector bug**: module-scoped build + tests (recipes above;
  paimon via `install`). For user-visible behavior, extend the e2e suites
  under `regression-test/suites/external_table_p0` / `external_table_p2`
  (docker environments: `docker/thirdparties/docker-compose/`).
- **Add a connector**: follow README "Adding a New Connector" top to bottom;
  the gates and the invariants above apply from the first commit.

## Commit Conventions

See the repository-root `AGENTS.md` for commit-message format, the PR
template, and code-review checkpoints. Nothing here overrides it.
