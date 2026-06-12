# Task List — CI build 968828 fixes (RCA: plan-doc/reviews/P5-paimon-ci-968828-rca-2026-06-13.md)

Decisions (2026-06-13): RC-3 + RC-5 → self-contained bundling. Sequence → contained fixes first.

## Now (contained, independent commits)
- [x] RC-1 — Thrift libthrift classloader split (exclude libthrift+fe-thrift from paimon+hudi plugin-zip; broaden encodeSchemaEvolution catch to Exception|LinkageError)  [~19 tests, BLOCKER]
- [x] RC-2 — Predicate not serialized for no-filter JNI scans (always serialize empty predicate list + BE getPredicates null backstop + UT)  [5 tests, BLOCKER]
- [x] RC-6 — DESC Key parity (mapFields ConnectorColumn isKey=true + UT)  [3 tests, MAJOR]
- [x] RC-7 — Sys-table schema-cache (override getSchemaCacheValue in PluginDrivenSysExternalTable)  [3 tests, MAJOR]

## After (self-contained, direction signed off; gate on docker paimon suite)
- [ ] RC-3 — S3A AWS-SDK static collision (bundle AWS SDK S3 modules into plugin, child-first)  [4 tests, BLOCKER]
- [ ] RC-4 — OSS JindoOssFileSystem split  [2 tests, BLOCKER] — MOVED here: not a clean pom add. jindo-sdk
      (com.aliyun.jindodata, has JindoOssFileSystem) is NOT cleanly maven-available at runtime ver 6.8.2
      (only stale 6.7.7 in .m2; runtime jar is deployed via the jindofs build, not maven). paimon-jindo
      gives a NATIVE JindoFileIO/JindoLoader but its HadoopCompliantFileIO may still hit the cast. Needs
      focused investigation + docker validation, same gate as RC-3/RC-5.
- [ ] RC-5 — HMS metastore-client reflection split (self-contained version-matched HMS client closure)  [1 test, BLOCKER]

## Out of scope (flag to owners; no code on this branch)
- [ ] RC-8 — hive CTAS strict-mode stale test expectation (hive/auto-partition owner)
- [ ] RC-9 — BE shutdown ASAN teardown segfault (BE/CI-infra owner)
