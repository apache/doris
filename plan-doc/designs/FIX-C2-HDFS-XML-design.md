# Problem

P6 clean-room finding **C2** (MAJOR; classification: missing-port / regression on a live read path).

A paimon catalog whose HDFS HA / connection topology lives **only** in a `hadoop.config.resources`
XML file (e.g. `hdfs-site.xml` declaring `dfs.nameservices` + per-nameservice namenodes + failover
proxy provider) **cannot resolve its nameservice** when created through the SPI plugin path for the
`filesystem` / `jdbc` flavors. At first metadata access the paimon SDK opens an HDFS `FileSystem`
against a Configuration that never parsed the XML, so an HA URI like `hdfs://ns1/warehouse` fails to
resolve `ns1`.

Scope (wave-2 confirmed): the gap is strictly the **FE-side catalog-create Configuration**. The
**BE/scan path is NOT affected** — `PaimonScanPlanProvider.java:619-620` consumes
`sp.toBackendProperties().toMap()`, which for HDFS returns the full backend map *including* the
XML-loaded keys. Wave 2 **refuted** the wave-1 kerberos-by-alias sub-claim (the per-FS Configuration
auth marker is not load-bearing; JVM-global `UserGroupInformation.setConfiguration` from the
authenticator's first `doAs` governs SASL). **This fix addresses the XML-resource gap ONLY.**

# Root Cause

The FE catalog-create Configuration for `filesystem`/`jdbc` is built by
`PaimonCatalogFactory.buildHadoopConfiguration(props, storageHadoopConfig)` →
`applyStorageConfig(...)` (`PaimonCatalogFactory.java:247-298`), from two sources:

1. `storageHadoopConfig` — assembled by `PaimonConnector.buildStorageHadoopConfig()`
   (`PaimonConnector.java:222-228`) by iterating `ctx.getStorageProperties()` and merging each
   `sp.toHadoopProperties().toHadoopConfigurationMap()`.
2. The connector-local `paimon.*` re-key + the **raw `fs.`/`dfs.`/`hadoop.` passthrough**
   (`applyStorageConfig`, `PaimonCatalogFactory.java:287-297`), which copies those keys **verbatim**.

For an HDFS-warehouse catalog:

- **`HdfsFileSystemProperties` deliberately does NOT implement `HadoopStorageProperties`** (its class
  "Scope note" javadoc, `HdfsFileSystemProperties.java:53-58`), so `sp.toHadoopProperties()` returns
  `Optional.empty()`. HDFS therefore contributes **nothing** to `storageHadoopConfig`.
- The raw passthrough copies the **`hadoop.config.resources` key itself** verbatim, but a Hadoop
  `Configuration` does **not** treat that key as a resource directive — it is a Doris-specific key.
  **The XML contents are never loaded.**

So inline `dfs.*` keys passed directly in the catalog properties still work (they ride the raw
passthrough), but an HA topology that lives only inside the referenced XML file is silently dropped.

## Parity baseline

Legacy `HdfsProperties.initNormalizeAndCheckProps()` (`HdfsProperties.java:126-138`) built the FE
Hadoop `Configuration` **directly from `backendConfigProperties`** (`new Configuration();
backendConfigProperties.forEach(set)`), and the per-flavor builders overlaid it for filesystem/jdbc
**and hms** (`PaimonFileSystemMetaStoreProperties:44`, `PaimonJdbcMetaStoreProperties:117`,
`PaimonHMSMetaStoreProperties.buildHiveConfiguration:80-84` — all iterate all storage props and
`conf.addResource(sp.getHadoopStorageConfig())`). Only DLF filtered to OSS/OSS_HDFS
(`PaimonAliyunDLFMetaStoreProperties:90-96`). The typed `HdfsFileSystemProperties.backendConfigProperties`
(`:198-222`, exposed via `toMap()`) is a faithful line-for-line port of legacy
`initBackendConfigProperties()`, already loaded at bind time (the BE path uses it today). The only
thing missing is a way to surface the XML/HA/auth keys to the FE Hadoop-config pipeline.

# Design

## Decision: `HdfsFileSystemProperties implements HadoopStorageProperties`, returning a **defaults-free** Hadoop map.

`toHadoopProperties()` returns `Optional.of(this)`; `toHadoopConfigurationMap()` returns the XML-loaded
keys + user `hadoop./dfs./fs./juicefs.` overrides + synthesized keys (`fs.defaultFS`, ipc fallback,
`hdfs.security.authentication`, kerberos, `hadoop.username`) — **WITHOUT** Hadoop's built-in framework
defaults. The connector code does not change (the existing `buildStorageHadoopConfig` loop already
consumes `toHadoopProperties()`).

### Why defaults-free (the red-team's decisive finding)

`HdfsConfigFileLoader.loadConfigMap` creates a `new Configuration()` (which loads `core-default.xml`)
and iterates **every** entry (`:88-101`). So when `hadoop.config.resources` is set,
`backendConfigProperties` carries ~323 keys, of which **62 are `fs.s3a.*` Hadoop defaults**
(`fs.s3a.path.style.access=false`, `fs.s3a.connection.maximum=96`, `fs.s3a.aws.credentials.provider=<chain>`,
…). `S3FileSystemProperties.toHadoopConfigurationMap()` emits those exact keys **unconditionally**
(`:321-324`: `connection.maximum` / `path.style.access`). `buildStorageHadoopConfig` does
`merged.putAll(...)` per provider (`PaimonConnector.java:223-226`), so for a **multi-backend** catalog
(object store + HDFS-with-XML) merged last-write-wins: if HDFS merges after S3, its
`fs.s3a.path.style.access=false` **clobbers** the S3/MinIO provider's tuned `true` → MinIO reads break.

This is a **regression vs the current branch** (today HDFS contributes nothing to `storageHadoopConfig`,
so the object-store tuning is intact) — independent of any legacy `addResource`-vs-`set` argument.
Reachable by a Kerberized HMS paimon catalog (`hadoop.kerberos.principal` triggers
`HdfsFileSystemProvider.supports()`) carrying `hadoop.config.resources` + MinIO table data, or any
`dfs.nameservices`/`hdfs://`-scheme catalog co-bound with an object store. Narrow, but a silent
data-access failure.

**Emitting the framework defaults serves no purpose** for the FE config — the base `new Configuration()`
in `buildHadoopConfiguration` (`PaimonCatalogFactory.java:249`) already supplies every Hadoop default.
The HDFS map only needs to contribute its *own* keys (XML + HA + auth). Dropping the defaults:
- removes the clobber entirely (the HDFS map no longer carries `fs.s3a.*`);
- is unambiguously safe vs legacy — whether legacy's `addResource` clobbered (then this is strictly
  better) or preserved (then this matches), the result is correct either way;
- for a single-backend HDFS catalog (the common C2 target) yields the **identical final Configuration**
  (base defaults + XML/synthesized keys).

Implemented with `new Configuration(false)` (no default resources) when building the FE map.

### BE map stays byte-identical

`toMap()` (BE) keeps returning the **defaults-laden** `backendConfigProperties` — byte-parity with
legacy `getBackendConfigProperties()` is preserved (the BE builds `THdfsParams` from specific keys and
ignores the `fs.s3a.*` noise; the historical FE↔BE divergence hazards argue for not perturbing the BE
map). The FE and BE maps then differ **only** in the inert Hadoop framework defaults; every meaningful
HDFS key (`fs.defaultFS`, `dfs.*`, `hadoop.security.*`, `ipc.*`, the XML's own keys) is identical in
both. For HDFS, the FE Hadoop config and BE map carry the same *meaningful* set — the legacy invariant.

## Cross-flavor reach

`buildStorageHadoopConfig()` is computed once for all flavors. Per-flavor parity:

| flavor | legacy HDFS overlay? | after fix | verdict |
|---|---|---|---|
| filesystem | yes | HDFS map → `buildHadoopConfiguration` | **parity — the C2 fix** |
| jdbc | yes | HDFS map → `buildHadoopConfiguration` | **parity** |
| hms | yes (`buildHiveConfiguration:80-84`) | HDFS map → HiveConf | **parity (bonus: closes the gap for HMS)** |
| dlf | no (OSS/OSS_HDFS only) | full `storageHadoopConfig` overlaid (`DlfMetaStorePropertiesImpl.toDlfCatalogConf:141`) | **deviation only for a DLF catalog that also binds HDFS — see Risk** |
| rest | n/a (Options-only) | `storageHadoopConfig` unused (`PaimonConnector:147-150`) | unaffected |

Pure object-store / pure-S3 catalogs bind **no** `HdfsFileSystemProperties`
(`HdfsFileSystemProvider.supports()` needs `_STORAGE_TYPE_=HDFS` / an `hdfs|viewfs|ofs|jfs|oss`-scheme
`fs.defaultFS`/`HDFS_URI` / `dfs.nameservices` / `hadoop.kerberos.principal` — none present on an
`s3.*`/`AWS_*`/`oss.*` map) → byte-unchanged.

# Implementation Plan

## File 1 — `fe-filesystem-hdfs/.../HdfsConfigFileLoader.java`

Add a `loadHadoopDefaults` overload; keep the existing 1-arg method delegating with `true` (BE
behavior unchanged):
```java
public static Map<String,String> loadConfigMap(String resourcesPath) {
    return loadConfigMap(resourcesPath, true);
}
public static Map<String,String> loadConfigMap(String resourcesPath, boolean loadHadoopDefaults) {
    ...
    Configuration conf = new Configuration(loadHadoopDefaults);   // false => only the named XML, no core-default.xml
    ...
}
```
(`loadConfigMap` has exactly one caller; `HdfsConfigBuilder` is a separate runtime path that does not
call it, so this is isolated.)

## File 2 — `fe-filesystem-hdfs/.../HdfsFileSystemProperties.java`

- `import ...HadoopStorageProperties;` + `implements FileSystemProperties, BackendStorageProperties, HadoopStorageProperties`.
- Refactor `buildBackendConfigProperties(origProps)` → `buildConfigProperties(origProps, boolean loadHadoopDefaults)`
  (the only internal change: `loadConfigMap(hadoopConfigResources, loadHadoopDefaults)`).
- Constructor builds **two** immutable maps from the same logic:
  - `backendConfigProperties = unmodifiable(buildConfigProperties(raw, true))` — BE (unchanged).
  - `hadoopConfigProperties = unmodifiable(buildConfigProperties(raw, false))` — FE Hadoop (no defaults).
- `toHadoopProperties()` → `Optional.of(this)`; `toHadoopConfigurationMap()` → `return hadoopConfigProperties;`.
- Rewrite the class "Scope note" javadoc: it now implements `HadoopStorageProperties` to surface the
  XML/HA/auth keys to the FE catalog Hadoop config (C2); the FE map is defaults-free to avoid clobbering
  co-bound object-store keys, while `toMap()` stays defaults-laden for BE byte-parity; the real
  `UGI.doAs` still lives in fe-core/ctx and this class builds no authenticator (K1).

(`validate()` keeps calling `checkHaConfig(backendConfigProperties)` — the XML's HA keys are present in
both maps, so HA validation is unchanged.)

## File 3 — stale comment-only updates (no logic change)

These comments assert the now-false invariant "HDFS contributes nothing to `storageHadoopConfig` /
`toHadoopProperties`"; my change inverts it, so they must be corrected to avoid misleading the next
reader: `PaimonConnector.java:136-137` and `:219-220`; `PaimonCatalogFactory.java:240-242` and
`:281-283`; `MetaStoreParseUtils.java` HDFS-absent note. (REST remains correctly unaffected.)

## Tests

`HdfsFileSystemPropertiesTest` (fe-filesystem-hdfs):
1. Flip `classifiersMatchHdfs:203` `toHadoopProperties().isEmpty()` → `.isPresent()` + fix the comment.
2. `xmlKeysReachHadoopConfigMap` (new, mirrors `xmlResourcesAreLoadedIntoBackendMap:207-230`): the XML's
   `dfs.custom.key` is present in `toHadoopProperties().get().toHadoopConfigurationMap()`. **C2 regression
   pin** (RED pre-fix: `toHadoopProperties()` empty → `.get()` throws).
3. `hadoopConfigMapExcludesFrameworkDefaultsButBeMapKeepsThem` (new — the clobber guard, encodes WHY):
   with `hadoop.config.resources` set, `toHadoopConfigurationMap()` does **NOT** contain
   `fs.s3a.path.style.access` / `fs.s3a.connection.maximum` (framework defaults excluded), while
   `toMap()` (BE) **does** (defaults-laden, BE parity). Pins both the clobber-safety and the FE/BE
   asymmetry. (Replaces the tautological `toMap()==toHadoopConfigurationMap()` idea.)
4. `hadoopConfigMapKeepsMeaningfulKeys` (new): `toHadoopConfigurationMap()` still contains the XML key +
   `fs.defaultFS` + `hdfs.security.authentication` (defaults-free ≠ empty).

`PaimonCatalogFactoryTest` (connector) — close the end-to-end leg the red-team flagged:
5. `buildStorageHadoopConfigFoldsInHdfsHadoopMap` (new): a stub `StorageProperties`+`HadoopStorageProperties`
   returning `{dfs.custom.key=v}` (a key NOT in the raw props, so it cannot ride the passthrough), placed
   in a `RecordingConnectorContext.getStorageProperties()`, flows through
   `PaimonConnector.buildStorageHadoopConfig()` → `PaimonCatalogFactory.buildHadoopConfiguration` and
   `conf.get("dfs.custom.key")` is non-null. Requires: `RecordingConnectorContext` gains a
   `storageProperties` field + `getStorageProperties()` override; `buildStorageHadoopConfig()` becomes
   package-private (`// visible for testing`). Combined with the existing
   `buildHadoopConfigurationAppliesStorageHadoopConfig`, this proves the full XML-key→Configuration chain.

## E2E (gated — `enablePaimonTest=true`, NOT run here)

A `filesystem`-flavor paimon catalog on HA HDFS (`hdfs://ns1/...`) with HA config supplied **only** via
`hadoop.config.resources=hdfs-site.xml` should `SHOW DATABASES`/`SELECT *` succeed. Pre-fix symptom:
nameservice `ns1` unresolved.

# Risk Analysis

## Blast radius — only `PaimonConnector` consumes `toHadoopProperties()`
Repo-wide, the only runtime caller of `.toHadoopProperties()` is `PaimonConnector:225` (every other hit
is a declaration/override/javadoc, and `grep 'instanceof HadoopStorageProperties'` = 0). fe-core /
iceberg / hive / hudi use the **separate** legacy `datasource.property.storage` hierarchy, untouched.

## Multi-backend (object store + HDFS-with-XML) — the clobber, now fixed
The defaults-free FE map carries **no** `fs.s3a.*`, so it cannot overwrite a co-bound object-store
provider's tuned `fs.s3a.*` regardless of merge order. (See Design §Why defaults-free for the empirical
basis: a defaults-laden HDFS map *would* reset MinIO `fs.s3a.path.style.access` true→false.) The
defaults-free map is byte-equivalent to the legacy meaningful set for single-backend and strictly safer
for multi-backend.

## DLF deviation — accepted
Legacy DLF overlaid only OSS/OSS_HDFS storage; the new `DlfMetaStorePropertiesImpl.toDlfCatalogConf`
overlays the full `storageHadoopConfig` (`:141`). After the fix, a DLF catalog that **also** binds an
`HdfsFileSystemProperties` would get HDFS keys in its DLF HiveConf. Triggers (per
`HdfsFileSystemProvider.supports()`): `dfs.nameservices`, an `hdfs|viewfs|ofs|jfs`-scheme bare
`fs.defaultFS`, `_STORAGE_TYPE_=HDFS`, or `hadoop.kerberos.principal`. A real DLF catalog uses
`oss.*`/`dlf.*` + `oss.hdfs.fs.defaultFS=oss://…` (not a bare `fs.defaultFS`), so this requires a
nonsensical DLF config; the result is additive/inert (defaults-free HDFS keys), never a credential or
correctness break. Documented as accepted in `deviations-log.md`.

## Pre-existing (out of C2 scope) — `fs.hdfs.impl.disable.cache`
Legacy HDFS `getHadoopStorageConfig()` carried `fs.hdfs.impl.disable.cache=true` (via
`StorageProperties.ensureDisableCache`); the typed `backendConfigProperties` never adds it (it lives
only on `HdfsConfigBuilder`'s runtime `create()` path, `:44-48`). This is absent from the paimon HDFS
catalog Configuration **regardless of C2** (HDFS contributed nothing pre-fix), so it is a separate
pre-existing gap, not introduced or worsened here. Functional risk is low (FS-cache by scheme+authority+ugi
is benign). Noted for the deviations log / a possible follow-up; **not** folded into C2 (which is scoped
to the XML-resource gap).

## Thread-safety / aliasing
Both maps are built once in the ctor as `Collections.unmodifiableMap`; the sole FE consumer copies via
`merged.putAll` into a method-local map, so the shared maps cannot be mutated. `loadConfigMap` creates a
fresh `Configuration` per call; the only static field (`hadoopConfigDirOverride`) is test-only.

# Open Questions

1. **DLF+HDFS-keys deviation** — recommend ACCEPT (nonsensical config, additive/inert). Sign off in
   `deviations-log.md`.
2. **`fs.hdfs.impl.disable.cache` pre-existing gap** — recommend a separate follow-up (not C2). Flag in
   `deviations-log.md`.
3. **HMS parity bonus** — the fix also closes the same XML gap for the HMS flavor (legacy overlaid HDFS
   there too); this is parity, not scope creep.
