# fe-connector Developer Guide

This directory holds the connector plugin framework of the Doris FE: the
contracts a connector implements, the shared infrastructure connectors reuse,
and the connectors themselves. A connector teaches the FE to serve metadata,
scans, and writes for an external source without fe-core knowing anything
about that source.

Two companion documents divide the work with this one:

- `AGENTS.md` (next to this file) — build/test recipes, the architecture
  gates, and the invariants that are not expressible as gates. Read it before
  changing code here.
- Generic plugin machinery is NOT defined here: contracts live in
  `fe/fe-extension-spi` and directory loading in `fe/fe-extension-loader`;
  storage properties and filesystem access live in `fe/fe-filesystem`. Each
  has its own README.

## What This Is

Three design rules shape everything in this directory:

1. **Connectors never import fe-core internals.** Whatever a connector needs
   from the engine must come through the SPI (`org.apache.doris.connector.*`)
   or shared neutral types. A build gate enforces this (see `AGENTS.md`).
2. **fe-core parses no connector properties.** Metadata-connection properties
   are parsed inside connectors (usually via the metastore layer below);
   storage properties belong to `fe-filesystem`.
3. **Generic engine code stays connector-agnostic.** There is no
   `if (source == ...)` in shared planning code; per-source behavior is
   reached only through the SPI, and optional behavior is opt-in via
   capabilities (see "Reading the API").

## Module Map

Roles only — one line each. For anything deeper, read the module's javadoc
(start at `package-info.java`) and pom comments; both are kept authoritative.

**Contracts**

| Module | Role |
|---|---|
| `fe-connector-spi` | The whole engine <-> connector contract, in both directions. What a connector implements: `ConnectorProvider` (discovery identity + factory), `Connector`, `ConnectorMetadata` with its Ops sub-interfaces, plus handle / pushdown / mvcc / scan / write / ddl / procedure / event / rest types. What the engine implements and hands down: `ConnectorContext` (including sibling-connector creation), `ConnectorStorageContext`, `ConnectorSession`, `ConnectorConf`. The javadoc here **is** the API reference. |

The two directions are one module on purpose: the boundary is bidirectional
(`ConnectorProvider.create` takes a `ConnectorContext`, and `ConnectorContext`
hands back a `Connector`), so splitting it by "who implements" would be
circular. Trino makes the same call with `trino-spi`. Contrast the metastore
layer below, where the split is acyclic and the usual api/spi convention holds.

**Metastore layer** (how connectors reach a metastore without hand-parsing
endpoint properties)

| Module | Role |
|---|---|
| `fe-connector-metastore-api` | Typed carriers of metastore connection facts (HMS / REST / filesystem / JDBC flavors). |
| `fe-connector-metastore-spi` | `MetaStoreProvider` discovery SPI + the `MetaStoreProviders` dispatcher. Adding a backend = one provider + one `META-INF/services` line; no central switch. |
| `fe-connector-metastore-hms`, `-iceberg`, `-paimon` | The backend implementations (ServiceLoader providers), consumed by the connectors that need them. |

**Shared infrastructure**

| Module | Role |
|---|---|
| `fe-connector-cache` | Self-contained caching framework used by several connectors. No fe-core dependency; it is bundled into each consuming plugin, so shared third-party libraries stay at the consumers' lowest common version (see the version notes in consumer poms). |
| `fe-connector-hms-hive-shade` | Slim, relocated HMS metastore-client closure for connectors that speak HMS thrift. The pom comments say exactly what relocates where and why. |
| `fe-connector-paimon-hive-shade` | Paimon-private relocated HMS-thrift closure; same idea, different owner. |

**Connectors**

`fe-connector-es`, `-jdbc`, `-maxcompute`, `-trino`, `-hms`, `-hive`,
`-iceberg`, `-paimon`, `-hudi` — one module per source. Reactor build order is
dependency-driven; the ordering constraints (shades before their consumers,
metastore backends before connectors) are documented as comments in this
directory's `pom.xml`.

## How a Connector Runs

**Startup.** `Env` creates the engine-side `ConnectorPluginManager`
(fe-core, `org.apache.doris.connector`), which discovers providers in two
rounds: first `ServiceLoader` on the classpath (built-ins and tests), then
`DirectoryPluginRuntimeManager` over the plugin directories named by
`Config.connector_plugin_root` (production). Classpath providers win over
directory ones. At registration every provider passes an API-version gate
(`ApiVersionGate`) and a type-uniqueness check: a duplicate on the classpath
fails loud, a bad plugin in a directory is logged and skipped so one broken
plugin cannot stop FE startup.

**CREATE CATALOG.** Routing happens on `ConnectorProvider.getType()` — a
connector's globally unique, case-insensitive identity. `CatalogFactory`
(fe-core) asks `ConnectorFactory` for a standalone-catalog connector; on a
match the catalog is materialized as a `PluginDrivenExternalCatalog`, and on
no match the engine falls back to the catalog types it implements itself (the
error message lists the installed standalone types). The engine keeps no list
of accepted types: `isStandaloneCatalogType()` is the only switch. A
connector that returns `false` there is *sibling-only* — still registered and
reachable by a gateway connector through
`ConnectorContext.createSiblingConnector` (the hive connector owning a hudi
sibling is the live example), but never a catalog of its own.

**A statement.** Every metadata acquisition in fe-core funnels through
`PluginDrivenMetadata` — the single place allowed to call
`Connector#getMetadata` (an architecture gate enforces this). It memoizes
exactly one `ConnectorMetadata` instance per catalog on the statement's
`ConnectorStatementScope` and closes it deterministically at statement end.
Scan planning follows the same shape: the generic `PluginDrivenScanNode`
(fe-core) delegates all per-source planning to the connector's
`ConnectorScanPlanProvider` — note the interface lives in fe-connector-spi
(`spi.scan`), not in fe-core.

**Classloading.** Plugins load child-first, each carrying its own runtime
closure. Wherever engine code crosses into a plugin — or a bundled library
resolves classes by name, or a plugin spawns worker threads — the
thread-context classloader must be pinned to the plugin's classloader. The
concrete loci and the failure modes are listed in `AGENTS.md`.

**Packaging.** Each connector assembles a plugin zip via
`src/main/assembly/plugin-zip.xml`; FE loads the zips from
`Config.connector_plugin_root` at startup. Classpath built-ins exist for
tests and embedded use.

## Reading the API

This document never lists SPI methods. The truth lives in code, behind four
mechanisms:

1. **Javadoc is the API reference.** Start at `ConnectorMetadata` (and its
   Ops sub-interfaces) and `ConnectorProvider`, both in fe-connector-spi.
   Every SPI method has a default body, so each sub-interface's class javadoc
   states its minimum implementation set, lifecycle, and threading rules.
2. **`@ConnectorMustImplement`** is the machine-readable half of the minimum
   implementation set: it marks the default methods a connector is
   nevertheless expected to override, with `when` naming the capability that
   triggers the obligation. A unit test pins the annotated set, so promoting
   a method is a deliberate, reviewed change.
3. **The recorded surface.**
   `fe-connector-spi/src/test/resources/connector-metadata-methods.txt`
   freezes the public method surface of `ConnectorMetadata`;
   `ConnectorMetadataSurfaceTest` fails on any drift. Adding, removing, or
   moving SPI methods must regenerate this baseline in the same commit (run
   the test, copy the "actual" set from the failure message). That file is
   the API inventory — do not maintain one in prose.
4. **Capabilities are opt-in.** `ConnectorCapability` declares optional
   switches consumed by static planning code; every constant states whether
   it is catalog-scoped (`Connector#getCapabilities`) or table-scoped (union
   with the table's own set — how a heterogeneous catalog admits only the
   tables that qualify). Write/sink traits live on
   `ConnectorWritePlanProvider` (via `Connector#getWritePlanProvider`), and
   scan-side opt-ins are `default`-false `supports*` methods on
   `ConnectorScanPlanProvider`. Generic engine code never special-cases a
   source; if a connector does not opt in, the feature stays off.

Statement-scoped memoization has one framework-wide convention worth knowing
before you read connector code: `ConnectorStatementScopes` (fe-connector-spi)
keys per-statement values by `(catalogId, db, table, queryId)` plus a
connector-owned namespace constant prefixed with the connector's
`getType()`. Each connector guards its own prefix with a unit test
(`EsStatementScope` / `EsStatementScopeTest` is the minimal example).

## Adding a New Connector

Copy from the reference connector: **`fe-connector-paimon`** exercises the
whole framework (metastore layer, cache framework, a shade sibling, TCCL
pinning). `fe-connector-es` is the minimal contrast (REST source, no
metastore/shade/cache). For a write path, the richest example is
`fe-connector-iceberg`.

1. **Module.** Create `fe-connector-<type>`, register it in this directory's
   `pom.xml` `<modules>` (respect the ordering comments). Start the pom from
   `fe-connector-es` (minimal) or `fe-connector-paimon` (full stack).
2. **Type name.** Read the `ConnectorProvider.getType()` contract first:
   globally unique, case-insensitive, must not collide with an engine
   built-in catalog type.
3. **Provider.** Implement `ConnectorProvider` (example:
   `PaimonConnectorProvider`) and register it in
   `src/main/resources/META-INF/services/org.apache.doris.connector.spi.ConnectorProvider`.
   Providers must have a no-arg constructor and no fe-core dependency.
4. **Connector + metadata.** Implement `Connector` and `ConnectorMetadata`
   (example: `PaimonConnector`, `PaimonConnectorMetadata`). Scope the first
   cut by the minimum implementation set: `@ConnectorMustImplement` plus each
   Ops interface's class javadoc.
5. **Metastore access.** If the source's metadata sits behind HMS / REST /
   JDBC / a filesystem, bind through the metastore layer
   (`MetaStoreProviders`; see `PaimonCatalogFactory` for the consumption
   pattern) instead of hand-parsing endpoint properties. A new backend
   flavor is a new `MetaStoreProvider` in a `fe-connector-metastore-<x>`
   module plus its services line.
6. **Property ownership.** Metadata-connection properties are parsed in your
   connector (or the metastore layer). Storage properties belong to
   `fe-filesystem`. Do not add parsing to fe-core — rule 2 above. How to
   organize the keys themselves: see "Property Organization" below.
7. **Caching.** Reuse `fe-connector-cache` (example:
   `PaimonLatestSnapshotCache`). Bundle the caching library into your plugin
   zip and keep shared third-party versions aligned with the other consumers
   (see the version notes in `fe-connector-paimon/pom.xml`). Respect the
   authorization invariant in `AGENTS.md`: a cross-query cache must never
   serve metadata that would bypass per-user, load-time authorization.
8. **Shading.** If your client stack drags a conflicting closure (hive/thrift
   is the recurring case), give it a dedicated relocated shade module; the
   pom comments of `fe-connector-hms-hive-shade` and
   `fe-connector-paimon-hive-shade` document the pattern.
9. **Handles and scan.** Implement `ConnectorScanPlanProvider` plus your
   handle types (example: `PaimonScanPlanProvider`, `PaimonTableHandle`,
   `PaimonScanRange`). Optional behavior goes through `supports*` /
   `ConnectorCapability` opt-ins only — the engine will not special-case
   your source.
10. **Write path** (if any). Implement the write Ops and
    `Connector#getWritePlanProvider`; copy from `fe-connector-iceberg`.
11. **Statement-scoped memos.** Follow the `ConnectorStatementScopes`
    convention: a namespace constant prefixed with your `getType()`, guarded
    by a unit test (copy `EsStatementScopeTest`).
12. **TCCL.** If your libraries reflect by class name or you spawn worker
    threads, pin the plugin classloader at those boundaries (see the loci in
    `AGENTS.md`; `TcclPinningConnectorContext` in fe-connector-paimon is a
    template).
13. **Tests.** Connector tests use recording fakes, not mocking frameworks
    (example: `RecordingConnectorContext`). Never touch
    `connector-metadata-methods.txt` unless you changed the shared SPI
    surface itself.
14. **Deployment-level settings** (if any). A value that is one-per-FE rather
    than one-per-catalog goes in your plugin's own settings file, NOT in
    fe.conf: ship `src/main/resources/<name>.conf.template`, add it to your
    assembly's `<files>` at the zip root, and read it with
    `ConnectorConf.get(context, "<key>", null, "<default>")`. `<name>` is
    `ConnectorProvider.name()`, which is **not** necessarily your plugin
    directory name — `plugins/connector/hive/` holds `hms.conf` and
    `plugins/connector/trino/` holds `trino-connector.conf`. Guard that with a
    test asserting `name() + ".conf.template"` is on the classpath (copy
    `IcebergConnectorConfTest#theConfTemplateIsNamedAfterTheProvider`); a
    template under any other name deploys a file the engine never opens.
    `build.sh` seeds the live `.conf` from the template generically, so it
    needs no change. Do NOT add a key to `Config.java` or to
    `DefaultConnectorContext.buildEnvironment` — that is an engine change per
    setting, and the keys still there are only the ones several connectors
    share plus the fe.conf fallbacks kept for existing deployments.
15. **Packaging.** Add `src/main/assembly/plugin-zip.xml` (copy from es or
    paimon). Verify your module through `package`/`install`, not just
    `test-compile` — shades and the plugin zip only materialize then.
16. **Gates and e2e.** Your module must pass the forbidden-import gate (runs
    automatically; see `AGENTS.md`). Add a docker environment under
    `docker/thirdparties/docker-compose/` and regression suites under
    `regression-test/suites/external_table_p0` /
    `regression-test/suites/external_table_p2`.

## Property Organization

Two named classes per connector, nothing else:

- **`<Xxx>CatalogProperties`** — everything a user writes in `CREATE CATALOG`.
  A typed holder: `@ConnectorProperty` fields bound by
  `ConnectorPropertiesUtils`, plus derived read-only fields (enums, prefix maps,
  bounded numbers), plus the raw map, plus the key-name constants. `of(map)`
  binds, derives and validates in one step and throws `IllegalArgumentException`
  on a bad value, so an instance that exists has valid properties — which is
  what lets every reader downstream take a getter instead of parsing the map
  again. `ConnectorProvider.validateProperties` is then exactly one line,
  `XxxCatalogProperties.of(properties)`, and that same line guards ALTER through
  the SPI default `validatePropertiesForUpdate`.
- **`<Xxx>Conf`** — the keys of the plugin's own `<name>.conf` (item 14 above),
  their defaults, their legacy fe.conf fallback keys, and small static readers
  wrapping `ConnectorConf.get`. Only create it if the connector really has such
  settings.

Rules for `of(map)`:

- **No I/O and no heavyweight types.** It runs at CREATE, at ALTER validation,
  and on every connector rebuild — including on an FE replaying the edit log,
  where reaching the filesystem would let a missing file stop FE from starting.
  Remote and filesystem checks belong in `Connector#preCreateValidation`, which
  runs on interactive CREATE only; note that ALTER does not run it, so those
  checks are deferred to first access after an ALTER.
- **Reject bad values, never unknown keys.** The catalog property map also
  carries engine keys (`type`, `meta.cache.*`, ...) and storage keys (`s3.*`,
  ...), and `ALTER CATALOG` merges properties — it can overwrite a key but never
  remove one, so a rejected unknown key would leave a catalog that no statement
  can repair.
- **Required-ness is expressed by `ParamRules.require` alone.** The annotation's
  `required` attribute is not read by the connector binder; set it truthfully as
  documentation and mark optional fields `required = false` explicitly. Mind the
  polarity of `ParamRules.check(condition, message)`: it throws when the
  condition *holds*, so the lambda states the failure case — the opposite of
  Guava's `checkArgument`. Reversing it compiles cleanly and inverts the check.
- **Mark secrets `sensitive = true`** and implement `toString()` as
  `ConnectorPropertiesUtils.toMaskedString(this)`. That covers logs only; the
  `SHOW CREATE CATALOG` masking is a separate, hand-maintained list in fe-core
  (`DatasourcePrintableMap.SENSITIVE_KEY`).
- **Never pass the raw map into anything that can reach user-visible output.**
  It holds the credentials. `ConnectorTableSchema`'s table properties are the
  trap: `SHOW CREATE TABLE` renders them unmasked, and the only thing keeping
  that rendering away from a given connector is that the connector does not
  declare the `SUPPORTS_SHOW_CREATE_DDL` capability.

Everything else — session variable names, DDL statement keys, remote
table-parameter keys, outbound SDK/BE payload keys, value literals — lives next
to its single reader, not in a properties class.

Binding and annotation reflection must happen inside the plugin.
`org.apache.doris.foundation.` is child-first for the connector family (it is
parent-first for the filesystem family), so each plugin carries its own
`@ConnectorProperty` class, and fe-core reflecting on a plugin object would find
no annotations at all — silently, since `getAnnotation` just returns null.
Referencing another plugin's key therefore means copying the literal with a
comment naming the owner; `IcebergSiblingProperties` in the hive connector is
the precedent.

When migrating a connector that is already released, audit every numeric key
first. The binder throws on a malformed number, while the hand-written `getInt`
helpers it replaces silently fell back to the default — so a stored dirty value
becomes "catalog unusable until `ALTER CATALOG` overwrites it". List each such
key in the PR description and decide strict (the default) or lenient (bind as
String, convert inside `of()`, log a warning) explicitly.

`fe-connector-adbc` is the reference implementation of both classes.

## Testing and Verification

- **Unit tests** live in each module; build/test recipes are in `AGENTS.md`.
- **The shared SPI surface** is guarded by fe-connector-spi's own suite
  (`ConnectorMetadataSurfaceTest` and `ConnectorPluginSurfaceTest`). Whenever
  you touch fe-connector-spi, run that module's tests — a consumer-only test
  run will not catch a stale baseline.
- **Architecture gates** run in the `validate` phase of every FE build: the
  forbidden-import gate for this directory and the metadata-funnel gate for
  fe-core. Scripts and their self-tests live in `build-support/` and
  `build-support/tests/`.
- **End-to-end**: docker environments under
  `docker/thirdparties/docker-compose/`, suites under
  `regression-test/suites/external_table_p0` and `external_table_p2`.

## When to Update This Document

Update this file ONLY when a framework-level fact changes:

- a module is added, removed, or renamed under `fe/fe-connector/`;
- the loading / lifecycle model changes (provider discovery, the
  per-statement metadata funnel, classloading, packaging);
- a durable invariant appears that no build gate or test can express.

Do NOT update this file for SPI method changes (javadoc is the API
reference; `connector-metadata-methods.txt` is the recorded surface), new
capability flags, or bug fixes.
