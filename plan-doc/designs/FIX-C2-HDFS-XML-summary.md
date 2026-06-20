# Summary — FIX-C2-HDFS-XML

## Problem

P6 clean-room finding **C2** (MAJOR). A paimon catalog whose HDFS HA topology lives **only** in a
`hadoop.config.resources` XML file could not resolve its nameservice on the SPI plugin path
(filesystem/jdbc flavors): the FE catalog-create `Configuration` copied the `hadoop.config.resources`
key verbatim but never loaded the XML contents, so `hdfs://ns1/...` failed to resolve `ns1`. (BE/scan
path was unaffected — it already consumes the XML-loaded `toBackendProperties().toMap()`.)

## Root Cause

`HdfsFileSystemProperties` deliberately did **not** implement `HadoopStorageProperties`, so its
`toHadoopProperties()` returned `Optional.empty()` and HDFS contributed nothing to the connector's
`buildStorageHadoopConfig()` → FE Configuration. The XML keys (already parsed into
`backendConfigProperties` at bind time for the BE path) never reached the FE config.

## Fix

`HdfsFileSystemProperties implements HadoopStorageProperties`:
- `toHadoopProperties()` → `Optional.of(this)`.
- `toHadoopConfigurationMap()` → a **defaults-free** FE map (built via `new Configuration(false)`):
  the XML keys + user `hadoop./dfs./fs./juicefs.` overrides + synthesized `fs.defaultFS`/ipc/auth/
  kerberos keys, but **without** Hadoop's 359 framework defaults.
- `toMap()` (BE) keeps the **defaults-laden** map for byte-parity with legacy `getBackendConfigProperties()`.

**Why defaults-free** (the design red-team's decisive finding, empirically verified on hadoop 3.4.2):
`new Configuration()` carries 62 `fs.s3a.*` defaults (`path.style.access=false`, `connection.maximum=500`,
…). A naive "return `backendConfigProperties`" would inject those into the shared `storageHadoopConfig`
and, in a multi-backend catalog (object store + HDFS-with-XML), **clobber** a co-bound S3/MinIO
provider's tuned `fs.s3a.path.style.access=true` → MinIO reads break. A **regression vs the current
branch** (where HDFS contributes nothing). The defaults belong to the base `Configuration` anyway, so
the FE map only contributes HDFS's own keys.

Per-flavor: parity for filesystem/jdbc (the C2 fix) and hms (legacy overlaid HDFS too); a documented,
accepted, barely-reachable deviation for DLF (`DV-036`); REST unaffected. Single-backend HDFS yields an
identical final Configuration.

Also updated 4 stale comments (`PaimonConnector`, `PaimonCatalogFactory`, `MetaStoreParseUtils`,
`ConnectorContext`) that asserted the now-false "HDFS contributes nothing to storageHadoopConfig".

## Tests

- `HdfsFileSystemPropertiesTest`: flipped `classifiersMatchHdfs` (`toHadoopProperties().isEmpty()`→
  `.isPresent()`, RED pre-fix); added `xmlKeysReachHadoopConfigMap` (C2 regression pin — an XML-only key
  reaches the FE map), `hadoopConfigMapExcludesFrameworkDefaultsButBeMapKeepsThem` (clobber guard +
  FE/BE asymmetry), `hadoopConfigMapKeepsMeaningfulKeys` (defaults-free ≠ empty).
- `PaimonCatalogFactoryTest.buildStorageHadoopConfigFoldsInHdfsHadoopMap`: end-to-end seam — a stub
  storage prop's `toHadoopConfigurationMap()` key (absent from raw props) flows through
  `buildStorageHadoopConfig()` → `buildHadoopConfiguration` into the `Configuration`. Required making
  `buildStorageHadoopConfig()` package-private + a `getStorageProperties()` seam on
  `RecordingConnectorContext`.

## Result

- fe-filesystem-hdfs full suite: **GREEN** (`HdfsFileSystemPropertiesTest` 28/28).
- fe-connector-paimon full suite: **279/0/1-skip** (skip = gated `PaimonLiveConnectivityTest`).
- fe-connector-spi compile + checkstyle: **GREEN**. Connector import-restriction check: **GREEN**.
- Process: one design red-team (6 agents) + one adversarial impl-verification (empirically re-validated
  the defaults-free claim against hadoop-common-3.4.2).
- **Docker e2e (`enablePaimonTest=true`): NOT run (gated).**
- Deviations recorded: `DV-036` (DLF+HDFS), `DV-037` (`fs.hdfs.impl.disable.cache` pre-existing gap).
