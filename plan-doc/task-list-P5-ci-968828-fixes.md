# Task List — CI build 968828 fixes (RCA: plan-doc/reviews/P5-paimon-ci-968828-rca-2026-06-13.md)

Decisions (2026-06-13): RC-3 + RC-5 → self-contained bundling. Sequence → contained fixes first.

## Now (contained, independent commits)
- [x] RC-1 — Thrift libthrift classloader split (exclude libthrift+fe-thrift from paimon+hudi plugin-zip; broaden encodeSchemaEvolution catch to Exception|LinkageError)  [~19 tests, BLOCKER]
- [x] RC-2 — Predicate not serialized for no-filter JNI scans (always serialize empty predicate list + BE getPredicates null backstop + UT)  [5 tests, BLOCKER]
- [x] RC-6 — DESC Key parity (mapFields ConnectorColumn isKey=true + UT)  [3 tests, MAJOR]
- [x] RC-7 — Sys-table schema-cache (override getSchemaCacheValue in PluginDrivenSysExternalTable)  [3 tests, MAJOR]

## Self-contained (committed; runtime behavior gated on docker paimon suite enablePaimonTest=true)
- [x] RC-3 — S3A AWS-SDK static collision: bundle software.amazon.awssdk:s3 + apache-client child-first
      (`b5205c41531`). Verified: plugin zip's sdk-core contains its own ExecutionAttribute. Docker-gate: S3 read + STS/assumed-role; plugin uses unpatched SdkDefaultClientBuilder.
- [x] RC-5 — HMS metastore-client reflection split: bundle org.apache.hive:hive-metastore:2.3.7 child-first
      with exclusions (`7841830809b`). Verified: 5 getProxy(HiveConf) overloads, 0 fastutil, no hadoop-2.7.2.
      Docker-gate: thrift 0.9.3-vs-host-0.16.0 wire skew (real metastore handshake); DLF ProxyMetaStoreClient still uncovered.
- [x] RC-4 — OSS JindoOssFileSystem split: build.sh copies thirdparty jindofs jars into the paimon plugin
      lib so JindoOssFileSystem loads child-first (`e881247857d`). Maven route is unbuildable (jindo bound
      to undeclared jindodata repo; ver skew 6.7.7/6.9.1/6.10.4). Docker-gate: jindo-core native single-load
      (UnsatisfiedLinkError if a concurrent non-paimon path also loads jindo from fe/lib/jindofs).

## Out of scope (flag to owners; no code on this branch)
- [ ] RC-8 — hive CTAS strict-mode stale test expectation (hive/auto-partition owner)
- [ ] RC-9 — BE shutdown ASAN teardown segfault (BE/CI-infra owner)
