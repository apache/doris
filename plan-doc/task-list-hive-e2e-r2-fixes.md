# Task List — hive e2e round-2 remaining fixes

Source triage: `plan-doc/reviews/hive-e2e-r2-triage-2026-07-11.md`.
Discipline per task: 设计(`tasks/designs/FIX-<id>-design.md`) → 设计红队 → 实现 → build+靶向 UT → 独立 commit → 勾表 → 更新 HANDOFF.

## Done (batch-1+2 — earlier sessions)
- [x] R1 getDatabase LOCATION (test_hive_ddl, test_hms_event_notification[_multi_catalog])
- [x] orc binary client options (test_hive_orc)
- [x] R6 decimal partition prune (test_hive_partitions)
- [x] R11 special-char partition key (test_hive_special_char_partition)
- [x] meta_cache ttl validation (test_hive_meta_cache)
- [x] R5 cardinality explain (test_hive_statistics_p0)
- [x] TEST_ALIGN case_sensibility (test_hive_case_sensibility)

## Remaining code fixes
### fe-connector 中型
- [x] R7  SHOW PARTITIONS bypass cache (test_hive_use_meta_cache_true)   `4df95ad44ac` — design red-teamed 4/4 SOUND, UT 19/19
- [x] R10 openx json ignore.malformed (test_hive_openx_json)   `9c70d4acf9a` — openx-only gate, red-teamed 3-lens, UT 13/13
- [x] R12/serde OpenCSV all-STRING schema (test_open_csv_serde, test_hive_serde_prop)   `3936434bc9a` — connector-side coerce, red-teamed 3-lens, UT 306/306
- [x] text_write LZ4FRAME→LZ4BLOCK read (test_hive_text_write_insert)   `e1d48045bee` — connector opt-in adjustFileCompressType, hive+hudi parity

### fe-connector / fe-core 大型
- [x] R2 SHOW CREATE TABLE native DDL (test_hive_show_create_table, test_hive_ddl_text_format)   `17764d03665` — lazy fresh-fetch connector render, red-teamed
- [x] R3 $partitions sys table (test_hive_partition_values_tvf)   `e697f189c59` — SPI isPartitionValuesSysTable opt-in → PartitionsSysTable(TVF); red-teamed GO_WITH_FIXES, UT hive 312/312 + fe-core PluginDrivenSysTable 11/11

### fe-core / SPI 大改 (user signed off → option A)
- [ ] query_cache: port SQL result cache to SPI + connector stable invalidation token (test_hive_query_cache)
- [ ] default_partition: connector-supplied per-value null flag via SPI (test_hive_default_partition)
- [ ] hive_config_test: restore recursive listing honoring hive.recursive_directories

## ENV (告知用户，非代码)
- [ ] Reset external hive docker → resolves test_hive_lzo_text_format, test_hive_varbinary_type, hive_config_test tag1
