# Task List — P6 paimon full-path review fixes

> Source: [`reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md`](./reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md) §Coverage gaps & follow-ups → prioritized fix-task list.
> Process **one at a time** (single-task loop): design → design red-team → implement → impl verify → build+UT → commit → summary → check off.
> B8 phased deletion (HANDOFF backlog item 1) is a separate effort, NOT in this list.

## Code-change fixes (priority order)

- [x] **P6-C1** MinIO `minio.*` aliases (MAJOR / BLOCKER-if-deployment-uses-`minio.*`) — **DONE `9967846ef64`**
      — added `minio.*` aliases to `S3FileSystemProperties` + `S3FileSystemProvider`; preserved MinIO defaults
      (region `us-east-1`, tuning 100/10000/10000 via gated normalize hook). 28/0/0 UT (FE `fs.s3.impl`/`fs.s3a.*`
      + BE `AWS_*` + tuning-preserve + s3-outranks-minio precedence). `s3.*` path byte-unchanged. e2e gated/not-run.
      Decision: PRESERVE tuning defaults (red-team refuted the "accept deviation" pass). See FIX-C1-MINIO-{design,summary}.md.
- [x] **P6-C2** HDFS `hadoop.config.resources` XML into FE catalog-create Configuration (MAJOR) — **DONE**
      — `HdfsFileSystemProperties implements HadoopStorageProperties`; FE `toHadoopConfigurationMap()` returns a
      **defaults-free** map (XML + HA + auth keys, no Hadoop framework defaults) so it never clobbers a co-bound
      object-store provider's tuned `fs.s3a.*` (multi-backend clobber found by design red-team, empirically
      verified on hadoop 3.4.2); BE `toMap()` stays defaults-laden (byte-parity). Parity for filesystem/jdbc/hms;
      DLF deviation = `DV-036` (accept). 28/0 fe-filesystem-hdfs UT + 279/0/1skip paimon + connector glue test;
      checkstyle + import-check clean; e2e gated/not-run. See FIX-C2-HDFS-XML-{design,summary}.md.
- [x] **P6-R3-residual** drop `"paimon".equals` gate on `appendBackendScanRangeDetail`; emit unconditionally under VERBOSE
      — **DONE** — removed the source-name conjunct (gate now `VERBOSE && !isBatchMode()`, identical to parent
      `FileScanNode`) + rewrote the false comment. **Scope (red-team-corrected, broader than review's "maxcompute"):**
      all 5 `SPI_READY_TYPES` route through this node — paimon unchanged, maxcompute/trino-connector parity-RESTORED,
      es/jdbc gain new (NPE-safe, rule-mandated) VERBOSE output. New `PluginDrivenScanNodeVerboseExplainTest` (3 tests,
      RED→GREEN mutation-verified); 45/0/0 `PluginDrivenScanNode*` + checkstyle clean; e2e gated/not-run.
      es_http `ES terminate_after:` gate left as separate residual (R3-LAYER-2). See FIX-R3-RESIDUAL-{design,summary}.md.
- [ ] **P6-R1-table** bridge `createTable`: add `remoteExists && !ifNotExists` arm → `ERR_TABLE_EXISTS_ERROR` (1050).
- [ ] **P6-C4** thread `hive_metastore_client_timeout_second` through `ConnectorContext.getEnvironment()`.
- [ ] **P6-R2-catalog** warn-and-strip now-dead `meta.cache.paimon.table.*` keys at CREATE CATALOG.
- [ ] **P6-R3-catalog** include catalog name in `listDatabaseNames` `LOG.warn` (decide keep best-effort swallow).

## Accept-as-deviation (no code; needs user sign-off)

- [ ] **P6-DEVIATIONS** — ~10 MINOR + ~12 NIT intentional deviations + wave-2 new items + uncheckedFallbacks
      (see report §Legacy-diff ledger "intended=Yes" rows + §Wave 2 new findings). Record each in `deviations-log.md`.
