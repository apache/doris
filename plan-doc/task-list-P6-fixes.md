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
- [ ] **P6-C2** HDFS `hadoop.config.resources` XML into FE catalog-create Configuration (MAJOR)
      — filesystem/jdbc flavor; recommend `HdfsFileSystemProperties` expose its already-XML-loaded backend map.
      **XML-resource gap ONLY** (kerberos-by-alias sub-claim was refuted: per-FS auth marker non-load-bearing).
- [ ] **P6-R3-residual** drop `"paimon".equals` gate on `appendBackendScanRangeDetail`; emit unconditionally under VERBOSE
      (fixes MaxCompute regression + generic-node-no-source-branch rule + false comment).
- [ ] **P6-R1-table** bridge `createTable`: add `remoteExists && !ifNotExists` arm → `ERR_TABLE_EXISTS_ERROR` (1050).
- [ ] **P6-C4** thread `hive_metastore_client_timeout_second` through `ConnectorContext.getEnvironment()`.
- [ ] **P6-R2-catalog** warn-and-strip now-dead `meta.cache.paimon.table.*` keys at CREATE CATALOG.
- [ ] **P6-R3-catalog** include catalog name in `listDatabaseNames` `LOG.warn` (decide keep best-effort swallow).

## Accept-as-deviation (no code; needs user sign-off)

- [ ] **P6-DEVIATIONS** — ~10 MINOR + ~12 NIT intentional deviations + wave-2 new items + uncheckedFallbacks
      (see report §Legacy-diff ledger "intended=Yes" rows + §Wave 2 new findings). Record each in `deviations-log.md`.
