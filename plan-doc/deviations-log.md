# 设计偏差日志

> **Append-only**：实施中发现原计划/RFC 设计**不可行 / 不必要 / 需要重新设计**时记入本文件。
> 与"决策"的区别见 [README §3.1](./README.md)：
> - 决策（D-NNN）= **事前**确定的选择
> - 偏差（DV-NNN）= **事后**对原计划的修正
>
> 编号规则：`DV-NNN` 三位数字，从 001 起单调递增，永不复用。
>
> 维护规则见 [README §4.3](./README.md)：**先记偏差再改文档**，不要 silent edit。

---

## 📋 索引

> 时间倒序；当前共 **31** 项。

| 编号 | 偏差主题 | 原计划位置 | 日期 | 当前状态 |
|---|---|---|---|---|
| DV-031 | P5-fix#6 FIX-KERBEROS-DOAS 两接受项：① **真 doAs 端到端 = live-Kerberos-e2e only**——M-8（filesystem/jdbc over Kerberized HDFS）+ M-11（Kerberos HMS read RPC）的 FE-unit 测只覆盖 **wiring**（M-8 断言 `getExecutionAuthenticator()` 返 `HadoopExecutionAuthenticator` 类型、不调 initializeCatalog；M-11 用 `RecordingConnectorContext.failAuth`/`authCount` 断言 read 经 `executeAuthenticated`），**无 paimon-kerberos regression 套件**（现有 `regression-test/.../kerberos/` 4 套仅 hive+iceberg、gated by `enableKerberosTest`）→ 真 KDC doAs 留给 live-e2e 门（翻闸前必验）。fail-safe：非 Kerberos 部署 no-op authenticator 与真 authenticator 行为一致（`ExecutionAuthenticator.execute`=`task.call()`）、无回归。② **跨连接器 follow-up**：read-vs-DDL doAs 缺口（M-11）+ 翻闸-authenticator-wiring 缺口（M-8，`initializeCatalog` 死代码）在 hudi/iceberg full-adopter **同样复发**（`cutover-fe-dispatch-gap` 姊妹）；与 [DV-028]（#4 CREATE-time-only 校验）/[DV-030]（#5 mapping-flag 键）同属「新连接器读法/翻闸 vs fe-core 既有约定」类缝，将来可批量 close。**M-8 新增 fe-core `MetastoreProperties.initExecutionAuthenticator` hook 是 fe-core 内部扩展、非连接器 SPI**（`ConnectorContext`/`Connector` 表面未改）→ 01-spi-extensions-rfc.md 无须改 | [task-list #6](./task-list-P5-rereview2-fixes.md) / [P5-fix-KERBEROS-DOAS 设计](./tasks/designs/P5-fix-KERBEROS-DOAS-design.md) / [D-052](./decisions-log.md) / [D-053](./decisions-log.md) | 2026-06-11 | 🟢 已登记（live-e2e 真值闸 + 跨连接器 follow-up）|
| DV-030 | P5-fix#5 FIX-MAPPING-FLAG-KEYS 跨连接器 follow-up（用户定本轮 paimon-only）：**新 hive + iceberg 连接器同根因**——读**下划线** mapping-flag 键而 fe-core 只写/读/藏**点分** catalog 键（`CatalogProperty:50,52`），`PluginDrivenExternalCatalog.createConnectorFromProperties` 喂原始 catalog map、中间无点分→下划线归一化 → 用户在 CREATE CATALOG 开 `enable.mapping.varbinary`/`enable.mapping.timestamp_tz` 对 hive/iceberg 亦**静默失效**（BINARY→STRING、LTZ→DATETIMEV2）。**iceberg** = `enable_mapping_varbinary`/`enable_mapping_timestamp_tz`（`IcebergConnectorProperties:46,47`→`IcebergConnectorMetadata:151,154`），仅分隔符差、语义不反转。**hive** = `enable_mapping_binary_as_string`/`enable_mapping_timestamp_tz`（`HiveConnectorProperties:52,53`→`HiveConnectorMetadata:317,319`），binary 键既改分隔符又改 token，但 `binary_as_string` 是**误名非语义反转**（`HmsTypeMapping:90-93` true→VARBINARY，喂 `mapBinaryToVarbinary` 字段）。JDBC 是唯一正确的新连接器（点分）。legacy hive/iceberg 经 `getCatalog().getEnableMappingVarbinary()` 读点分（`HMSExternalTable:791`/`IcebergUtils:1083`）→ 翻闸回归。**用户签 [D-051] = 本轮只修 paimon**（保 commit surgical、单任务）；**follow-up（close 时）**：hive+iceberg 两常量重指 canonical 点分键（hive `binary_as_string` token 复原为 `varbinary`，**勿**反转 boolean）+ 各加 dotted-key honor UT；与 paimon #5 同形修。scope 经验证 workflow `wf_a3626c54-0db`（g5 + synthesizer，静态 trace 未 live） | [task-list #5](./task-list-P5-rereview2-fixes.md) / [P5-fix-MAPPING-FLAG-KEYS 设计](./tasks/designs/P5-fix-MAPPING-FLAG-KEYS-design.md) / [D-051](./decisions-log.md) | 2026-06-11 | 🟡 待修（跨连接器 follow-up，用户定本轮 paimon-only）|
| DV-028 | P5-fix#4 FIX-JDBC-DRIVER-URL：driver_url 安全校验**仅 CREATE CATALOG**（`PaimonConnector.preCreateValidation`→`ConnectorValidationContext.validateAndResolveDriverPath`），**FE-restart reload / ALTER CATALOG / scan-time 不复校**——与 legacy 分歧（legacy `getBackendPaimonOptions`→`JdbcResource.getFullDriverUrl` 每 scan 复校 format/whitelist/secure-path）。根因 = pre-existing **fe-core 架构缝**、非本 fix/非 paimon 专属：`CatalogFactory:164` replay(`isReplay=true`) 跳 `checkWhenCreating`→`preCreateValidation` 不跑；`PluginDrivenExternalCatalog.checkProperties`(ALTER 路) 只调 `validateProperties`(无 driver 校验)、不调 `preCreateValidation`；`getBackendPaimonOptions` 仅 resolve 不 validate（连接器 scan-time 只有 `ConnectorContext`、无 driver-path 校验 hook）。**与 JDBC 参考连接器 `JdbcDorisConnector` 完全 parity**（其亦 CREATE-time-only）。**用户定接受**（[D-050]）：默认配置 permissive（`secure_path="*"`/whitelist 空）无可绕，唯一暴露 = 硬化部署后**收紧** whitelist/secure-path 又**不重建** catalog。**复评/follow-up（跨连接器）**：若需 close，须 fe-core 改（ALTER 路 `checkProperties`→`preCreateValidation`，注意会触发 JDBC 连接器的 BE 连通测）+ scan-time 校验须新 `ConnectorContext` SPI hook——影响全 plugin 连接器、独立工单 | [task-list #4](./task-list-P5-rereview2-fixes.md) / [P5-fix-JDBC-DRIVER-URL 设计](./tasks/designs/P5-fix-JDBC-DRIVER-URL-design.md) / [D-050](./decisions-log.md) | 2026-06-11 | 🟢 已登记（CREATE-time parity，用户接受+跨连接器 follow-up）|
| DV-029 | P5-fix#4 FIX-JDBC-DRIVER-URL 两 scope-out（surgical）：① 连接器 `PaimonCatalogFactory.resolveDriverUrl` 是 legacy `JdbcResource.getFullDriverUrl` 的**简化子集**——只做 scheme 解析（裸名→`file://{jdbc_drivers_dir}/{name}`），**不**做文件存在性 / legacy 旧 `jdbc_drivers/` 回退 / 云下载。常见情形（`mysql.jar`+默认 dir）两者等价；仅装旧 dir 的 jar 会 BE 找不到（pre-existing 简化、FE 注册路本就如此、复用未改）。② **BE-side `paimon.jdbc.{user,password,uri}` 别名丢弃不修**——同 `startsWith("jdbc.")` filter 也丢这些别名键，但 **BE 不需要**：`PaimonJniScanner.initTable` 从 `serialized_table` 反序列化整表、**不**从 options_json 重建 JdbcCatalog；BE 唯一消费 jdbc 选项处 `PaimonJdbcDriverUtils.registerDriverIfNeeded` 只读 driver_url/driver_class。legacy `getBackendPaimonOptions` 亦仅发 driver_url+driver_class（窄）。故 B-8a 只修 driver_url/class 即 parity（scope-critic lens LGTM 确认） | [task-list #4](./task-list-P5-rereview2-fixes.md) / [P5-fix-JDBC-DRIVER-URL 设计](./tasks/designs/P5-fix-JDBC-DRIVER-URL-design.md) / [D-050](./decisions-log.md) | 2026-06-11 | 🟢 已登记（surgical scope-out，BE 经 trace 确认安全）|
| DV-027 | P5-fix#3 FIX-SCHEMA-EVOLUTION：history_schema_info 用 **eager 全量** `SchemaManager.listAllIds()`+`schema(id)`（每 scan、**无 cache**），非 legacy 的 per-split 引用 schema 懒读+缓存（`PaimonScanNode.putHistorySchemaInfo`→`PaimonUtils.getSchemaCacheValue`）。理由：Design C 的 scan 级缝 `populateScanLevelParams` 拿不到 split 集（那是 `planScan` 才有），故无法只读引用到的 schema；listAllIds() 全集**保证**覆盖任意 native 文件的 `schema_id`（BE `table_schema_change_helper.h:259-263` 缺 entry 会 fail-loud `InternalError`，全集即杜绝）。**两点接受**：① perf——K 个 schema 版本= K 次小 JSON 读/scan（props 每 node 缓存一次、非 per-split）；② 鲁棒性微回归——某**未被引用**的 schema-N JSON 瞬时不可读会令本 scan 失败（fail-loud 传播，镜像 legacy `putHistorySchemaInfo` 不吞异常），而 legacy 因只读引用 schema 不碰它、可完成。correctness-safe（全集是 legacy 引用集的超集、绝不触发 BE InternalError）；review 评 MINOR。未来优化=引用集（需 split-aware 缝）或连接器侧 cache | [task-list #3](./task-list-P5-rereview2-fixes.md) / [P5-fix-SCHEMA-EVOLUTION 设计](./tasks/designs/P5-fix-SCHEMA-EVOLUTION-design.md) / [D-049](./decisions-log.md) | 2026-06-11 | 🟢 已登记（MINOR perf+鲁棒性，接受 fail-loud）|
| DV-026 | P5-fix#3：**M-10（`Column.uniqueId=-1`）deferred 不修**（task-list #3 原含 M-10）。Design C 直接从 paimon `DataField.id()` 建 `history_schema_info` 的 `TField.id`，B-1a（field-id 匹配）**完全独立于** Doris `Column.uniqueId` → M-10 对 B-1a correctness 无关。rereview2 §4 已 majority-refute M-10 standalone repro（BE field-id 路不读 tuple descriptor、唯一 legacy `Column.uniqueId` 消费者 `ExternalUtil.initSchemaInfo` 经 legacy scan node 翻闸后已死）→ 无 demonstrated user-visible 消费者。故 deferred（非本 fix 必需、Design C 不穿 ConnectorColumn/ConnectorType field-id channel）。**复评触发**：若未来出现 field-id 消费者（如 SPI-on iceberg/hudi 经 `ExternalUtil` 从 Doris 列建 history schema），须重启 M-10（穿 `ConnectorColumn.fieldId`+`ConnectorType` 嵌套 id+`ConnectorColumnConverter.setUniqueId` 递归）| [task-list #3](./task-list-P5-rereview2-fixes.md) / [P5-fix-SCHEMA-EVOLUTION 设计](./tasks/designs/P5-fix-SCHEMA-EVOLUTION-design.md) / [D-049](./decisions-log.md) | 2026-06-11 | 🟢 已登记（M-10 deferred，无消费者）|
| DV-025 | P5-fix-FIX-URI-NORMALIZE：`normalizeStorageUri` 用 catalog **静态** `getStoragePropertiesMap()` 做 scheme 归一化，**非** legacy `PaimonScanNode:171` 的 vended-overlay 版（`VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials`）。理由：scheme 归一化（oss/cos/obs/s3a→s3、bucket.endpoint→bucket）与 vended 凭据正交——vended 只改 `AWS_*` 键、不改 scheme/bucket 形；只要 warehouse endpoint 静态配置（OSS/COS/OBS 绝大多数情形必配，否则连不上）静态 map 即含该 type entry，归一化与 legacy 等价。唯一分歧 = *纯-vended、无静态存储配* 的 REST catalog：静态 map 可能缺 entry → `LocationPath.of` fail-loud 抛（legacy vended-overlay 版不抛）。该边角**与凭据缝重叠、本 fix 显式不收**，归 task-list #2 `FIX-STATIC-CREDS-BE` / `FIX-REST-VENDED`（review §9.3 三道凭据缝之一）。fail-loud 优于静默送裸 `oss://`（后者 DV 错行）| [task-list #1](./task-list-P5-rereview2-fixes.md) / [P5-fix-URI-NORMALIZE 设计](./tasks/designs/P5-fix-URI-NORMALIZE-design.md) / [SPI RFC §21](./01-spi-extensions-rfc.md) | 2026-06-11 | 🟢 已登记（scope 决策，凭据边角归 #2/#3）|
| DV-024 | P5-B4 揭出并修复 B2 遗留缺陷（普通 paimon plugin 表 BE 描述符错型）：`PaimonConnectorMetadata` 不 override `buildTableDescriptor`（SPI default 返 null）→ `PluginDrivenExternalTable.toThrift` 走 fallback `SCHEMA_TABLE`（BE `descriptors.cpp:635` 建 `SchemaTableDescriptor`），而 legacy `PaimonExternalTable.toThrift` + sys 表须 `HIVE_TABLE`（`:644` `HiveTableDescriptor`）。B4/T19 加 `buildTableDescriptor` override（`HIVE_TABLE`+`THiveTable`，镜像 legacy + MC `MaxComputeConnectorMetadata.buildTableDescriptor`），**一处修同时正普通表+sys 表**。inert until 翻闸（paimon 未入 `SPI_READY_TYPES`），真值闸=live-e2e BE 描述符 | [tasks/P5 T19](./tasks/P5-paimon-migration.md) / [D-039](./decisions-log.md) | 2026-06-10 | 🟢 已修正（T19，live-e2e 待验）|
| DV-023 | RFC §10（E7 Sys Tables）设计被 P5-B4 取代：RFC §10 的「sys-table = `$`-后缀普通表 + 连接器 `getTableHandle` 内解析后缀 + `listSysTableSuffixes`」**从未实现**；live fe-core 实为 `SysTableResolver`+`NativeSysTable`+`TableIf.getSupportedSysTables/findSysTable`（iceberg + legacy-paimon 共用）。B4 按 [D-039](./decisions-log.md) 复用该 live 机制（连接器 `listSupportedSysTables`+`getSysTableHandle`，fe-core 通用 `PluginDrivenSysExternalTable`），RFC §10 加脚注标 superseded | [01-spi-extensions-rfc.md §10](./01-spi-extensions-rfc.md) / [D-039](./decisions-log.md) | 2026-06-10 | 🟢 已修正（RFC §10 脚注 + D-039）|
| DV-022 | P4-T09 §8：fe-common 去 odps 暴露隐藏传递依赖（依赖卫生，非缺陷）——`odps-sdk-core` 此前**传递**为 fe-common 自身 `DorisHttpException`(io.netty) / `GsonUtilsBase`(com.google.protobuf) 提供 jar；删 odps-sdk-core 后编译暴露缺失，故 fe-common/pom 显式补 `netty-all`+`protobuf-java`（parent dependencyManagement 管版本）。设计 §8 原假设「odps 仅服务 MCUtils」不全 | [Batch-D 设计 §8](./tasks/designs/P4-batchD-maxcompute-removal-design.md) / [D-027] | 2026-06-09 | 🟢 已修正（显式声明，`409300a75b8`）|
| DV-021 | P4-T3：Batch-D 删除后 4 条 Tier-3 接受项（minor，legacy 已删故现为既定行为，非丢数据，用户定接受不修）——**GAP3** CREATE DB 非-IFNE 远端已存→本地预抛 `ERR_DB_CREATE_EXISTS`(1007)；**GAP4** DROP TABLE 非-IF-EXISTS+远端缺→通用 `ERR_UNKNOWN_TABLE`(1109)；**GAP9** SHOW PARTITIONS `LIMIT`：sort-then-paginate（vs legacy paginate-then-sort，更合 ORDER-BY-LIMIT）；**GAP10** partitions() TVF schema-分区零实例表→返 0 行（vs legacy 抛，in-code 注释声明 intentional） | [Batch-D 红线](./task-list-batchD-redline-gaps.md) | 2026-06-09 | 🟢 已登记（Tier-3 接受）|
| DV-020 | P4-T06e FIX-CAST-PUSHDOWN：getSplits 的 limit-suppress wiring + MC 端到端 CAST-strip 无 fe-core 单测（KNOWN-LIMITATION）+ JDBC applyLimit 同类 under-return（OUT-OF-SCOPE 备查）。**① harness gap**：纯静态 `effectiveSourceLimit(limit,stripped)` 已 UT 2 + mutation 2/2（drop-suppression/always-suppress）向红 pin；连接器 `supportsCastPredicatePushdown=false` 已 UT + mutation(false→true 红) pin；但「`getSplits` 据 `filteredToOriginalIndex!=null` 调 `effectiveSourceLimit`」+「`buildRemainingFilter` 对 MC 真剥 CAST conjunct 并保留 BE-only」的端到端 wiring **无 offline 直测**（构造 `PluginDrivenScanNode` 需 harness、本模块缺，同 [DV-015]）。覆盖经：strip-when-false 是 fe-core 共享逻辑（JDBC false 分支既覆盖）+ 纯 helper UT/mutation + **live e2e 真值闸**（STRING 列存 `"5"/"05"/" 5"`，`WHERE CAST(code AS INT)=5` 返回全部 3 行 / limit-opt ON+CAST+LIMIT 不 under-return；EXPLAIN 证 CAST 谓词不在下推 filter）。**② OUT-OF-SCOPE（Rule 12 surface）**：JDBC 若 session 关 cast-pushdown 且经 `applyLimit` 推 limit，理论同类 under-return；但 MaxCompute 不 override `applyLimit`（no-op）、F9 的 getSplits limit-param 抑制对 MC 完整，JDBC `applyLimit` 路径非本修范围（pre-existing、非 MC），登记备查、待评估。fail-safe：误关下推退化为多读行交 BE（非丢数据） | [FIX-CAST-PUSHDOWN 设计](./tasks/designs/P4-T06e-FIX-CAST-PUSHDOWN-design.md) / [D-036] | 2026-06-08 | 🟢 已登记（helper+capability UT/mutation；wiring 待 live e2e；JDBC applyLimit 备查）|
| DV-019 | P4-T06e FIX-BATCH-MODE-SPLIT 异步 batch wiring + `computeBatchMode` null-guard 无 fe-core 单测（KNOWN-LIMITATION，NG-7）：纯静态四闸 `shouldUseBatchMode` 已 UT 9 + mutation 5/5 向红 pin；但 ① `computeBatchMode` 的 SF-1 `scanProvider != null` null-guard（provider-less full-adopter 防 NPE，跑 dispatch+explain 两路径）与 ② `startSplit` 的 async 分批循环（`getScheduleExecutor` outer/inner CompletableFuture + `SplitAssignment` `needMoreSplit/addToQueue/finishSchedule/setException/isStop` 契约 + init 30s 首-split）+ ③ `numApproximateSplits` 取值——三处 wiring **无 offline 直测**：构造 `PluginDrivenScanNode`（`FileQueryScanNode` 子类）需绕 ctor + stub connector/session/handle/desc/sessionVariable/splitAssignment，本模块无现成轻量 spy/analyze harness（同 [DV-015]/[DV-014] 因）。覆盖经：逐字镜像 legacy `MaxComputeScanNode:214-298`（已验 parity）+ 纯 helper UT/mutation + **大分区 live e2e 真值闸**（EXPLAIN/profile 证 batched/streamed split、规划耗时/内存 ≪ 同步路；阈值边界 `num_partitions_in_batch_mode`=0/大于选中数→回退非-batch；全空选/单分区）。impl-review `wve7y1jst` TQ-1 已据此把测试 javadoc 的「null-provider 已覆盖」声明诚实降级。fail-safe：去 batch 退化为同步 `getSplits`（非丢数据） | [FIX-BATCH-MODE-SPLIT 设计](./tasks/designs/P4-T06e-FIX-BATCH-MODE-SPLIT-design.md) / [D-035] | 2026-06-08 | 🟢 已登记（helper UT+mutation，wiring 待外表 scan harness / live e2e）|
| DV-018 | P4-T06e FIX-POSTCOMMIT-REFRESH cutover post-commit 刷新 swallow 有意分歧于 legacy（无产线逻辑改动，NG-8/F15=F21 minor，regression=no）：`PluginDrivenInsertExecutor.doAfterCommit()` 用 try/catch 吞 `super.doAfterCommit()`（=`handleRefreshTable`）刷新失败、INSERT 报 OK；legacy `MCInsertExecutor` 不 override → 异常传播 → 报 FAILED。**cutover 更安全**：按生命周期序数据已落 ODPS/远端、FE 无法回滚，`handleRefreshTable` 只刷 FE 缓存 + 写 external-table refresh editlog（follower 失效提示、非数据真相源）、不碰已提交数据 → 报 FAILED 诱发重试→重复写。**用户定（2026-06-08）接受 + Javadoc 泛化（[D-034]）、不回退**。改 = 仅 Javadoc(`:164-176`) 从「只讲 JDBC_WRITE」泛化到覆盖 MC connector-transaction 路径（两路径数据均已持久；swallow 最坏只瞬时缓存 stale 自愈；显式注明分歧 legacy）。对抗性安全核查：master 先本地刷新(`RefreshManager:152`)后写 editlog(`:155`)，丢 editlog 仅 follower 缓存暂 stale 自愈、无正确性损失/无主从分裂。swallow 路径无新增 UT（注释 only、无可 pin 逻辑变化；异常吞行为 offline 直测受同类 harness 缺位限制，同 [DV-015]）；真值闸=CI-skip live e2e（MC INSERT 后人为令 refresh 失败→断言报 OK + warn）。守门 checkstyle 0、import-gate 净 | [FIX-POSTCOMMIT-REFRESH 设计](./tasks/designs/P4-T06e-FIX-POSTCOMMIT-REFRESH-design.md) / [D-034] | 2026-06-08 | 🟢 已登记（无逻辑改动，行为收敛接受；live 真值闸待跑）|
| DV-017 | P4-T06e FIX-ISKEY-METADATA `getTableSchema→buildColumn` wiring 无连接器内单测（KNOWN-LIMITATION）：`buildColumn` 助手 isKey=true 不变式已 UT+mutation pin，但两 `getTableSchema` 调用点经 `buildColumn` 的 wiring 无 offline 测——`getTableSchema` deref live `com.aliyun.odps.Table`（唯一 ctor package-private）、模块无 Mockito（同 [DV-014]/[DV-015]/[DV-016] 类）；唯一 offline 变通=`com.aliyun.odps` 包内 fixture 子类 override `getSchema()`，repo 无先例（sibling `getColumnHandles` 同样未测）。绕过 `buildColumn`（回退 5 参 ctor）的回归仅由 CI-skip live e2e `DESCRIBE <mc_table>` 显 Key=YES 捕获（load-bearing gate）。**作用域注**：`information_schema.columns.COLUMN_KEY` 受 `FrontendServiceImpl:962-965` OlapTable 门控、MC 前后皆空、已 parity、out-of-scope（不可断言其变非空）；isKey 非纯展示（亦喂 `UnequalPredicateInfer`/BE descriptor），但 legacy 即喂 true → 本修恢复既有值 | [FIX-ISKEY-METADATA 设计](./tasks/designs/P4-T06e-FIX-ISKEY-METADATA-design.md) / [D-033] | 2026-06-08 | 🟢 已登记（helper UT+mutation，wiring 待 live DESCRIBE）|
| DV-016 | P4-T06e FIX-LIMIT-SPLIT-DEFAULT 三点（均 opt-in 默认 OFF、非丢行/非回归）：① **CAST-unwrap 致 limit-opt 资格略宽于 legacy**——converter `convert(CastExpr)→convert(child)` 在所有位置剥 CAST（左列/右 literal/IN 元素），故 `CAST(partcol AS T)=lit`、`partcol=CAST(lit AS T)`、`partcol IN (CAST(lit,…))` 经 `checkOnlyPartitionEquality` 判资格，legacy 见原始 `CastExpr` 子节点 instanceof 失败→false；② **嵌套-AND-作单 conjunct 略宽**——converter `flattenAnd` 把单 conjunct `(pt=1 AND region=cn)` 摊平成 flat `ConnectorAnd`→资格，legacy 见 `CompoundPredicate` conjunct→false（与①同安全类，且 conjunct 拆分通常上游已分）；③ **`LIMIT 0` 路径差**——本 fix `limit<=0` 拒 limit-opt 走标准多 split 路，legacy `hasLimit()`(`limit>-1`) 走 limit-opt 路；两者皆 0 行、且 `LIMIT 0` 被 Nereids 折成 EmptySet 不可达。①②均纯分区、correctness-safe（裁剪 Nereids `SelectedPartitions` 同算 + 转换后 `filterPredicate` 仍下推 read session 作 backstop，`:191/:208/:353`；LIMIT 无 ORDER BY 无序）。**另**：planScan 两行 wiring（`isLimitOptEnabled(session.getSessionProperties())` + `shouldUseLimitOptimization(...)` 收 live filter/partitionColumnNames）无连接器内单测——`planScan` 需 live odps `Table`、模块无 fe-core/Mockito（同 [DV-014]/[DV-015] 因）；纯 helper 全 UT(26)+mutation(8 向红) pin，wiring 半由 CI-skip live E2E 守。**附**：本 fix 实 `checkOnlyPartitionEquality` 同闭 F2/F12（旧恒 false stub minors）| [FIX-LIMIT-SPLIT-DEFAULT 设计](./tasks/designs/P4-T06e-FIX-LIMIT-SPLIT-DEFAULT-design.md) / [D-032] | 2026-06-08 | 🟢 已登记（opt-in 非回归 + 逻辑 UT/mutation，wiring 待 live E2E）|
| DV-015 | P4-T06e FIX-PRUNE-PUSHDOWN 端到端裁剪下推 wiring 无 fe-core 单测（KNOWN-LIMITATION）：`getSplits()` pruned-to-zero 短路 + translator `setSelectedPartitions` 注入 + `getSplits→planScan` 6 参 threading 无 fe-core 端到端 UT（连接器 scan 无轻量 analyze/spy harness，同 [DV-014] 因）。逻辑半（`PluginDrivenScanNode.resolveRequiredPartitions` 三态 + `MaxComputeScanPlanProvider.toPartitionSpecs` 转换）已 UT+mutation pin；wiring 半 + 真实裁剪生效由 p2 live `test_max_compute_partition_prune.groovy` 覆盖（真值=EXPLAIN/profile 仅扫目标分区 + `WHERE pt='不存在'`→0 行不建全分区 session）。与既有约定一致（`HiveScanNodeTest` 亦直构 node 测 setter、不经 translator）| [FIX-PRUNE-PUSHDOWN 设计](./tasks/designs/P4-T06e-FIX-PRUNE-PUSHDOWN-design.md) / [D-031] | 2026-06-08 | 🟢 已登记（逻辑 UT+mutation，wiring 待 live；外表 scan analyze/spy harness 落地后补）|
| DV-014 | P4-T06e FIX-BIND-STATIC-PARTITION bind 期投影无 fe-core 单测（KNOWN-LIMITATION）：`bindConnectorTableSink` 的 full-schema 投影（NULL 填充 + 分区列在末尾 + 按位置投影）未被 connector-path 单测直接 pin——`bind()` 走 `RelationUtil.getDbAndTable` 真 Env 解析，外表 PluginDriven catalog 需连接器插件,无现成轻量 analyze harness（OLAP analyze 测仅覆盖 `createTable` 内表）。覆盖经：①与 legacy `bindMaxComputeTableSink` 及 Iceberg 路径**共享** helper `getColumnToOutput`/`getOutputProjectByCoercion`（被既有 OLAP/Hive/Iceberg insert 测充分覆盖）；②列选择 helper `selectConnectorSinkBindColumns` 单测 + 分布 full-schema 索引测（要求 child full-schema 序方过）；③p2 live `test_mc_write_insert` Test 3/3b（部分/重排列名）+ `test_mc_write_static_partitions`。capability 声明/reader 按既有约定不单测（既有 readers 亦仅被 mock）| [FIX-BIND-STATIC-PARTITION 设计](./tasks/designs/P4-T06e-FIX-BIND-STATIC-PARTITION-design.md) / [D-030] | 2026-06-07 | 🟢 已登记（无 harness,parity+p2 覆盖；待外表 analyze harness 落地补）|
| DV-013 | P4-T06e FIX-WRITE-DISTRIBUTION 两处 planner 写分发 parity 微差（均非回归，default `strict` 下与 legacy MC 同果）：① `ShuffleKeyPruner` connector 分支缺 `enableStrictConsistencyDml` 短路 → non-strict 下少剪 shuffle-key（更保守 missed optimization）；② `enable_strict_consistency_dml=false` 下动态分区 local-sort 被丢（legacy MC 亦丢）| [FIX-WRITE-DISTRIBUTION 设计](./tasks/designs/P4-T06e-FIX-WRITE-DISTRIBUTION-design.md) / [D-029] | 2026-06-07 | 🟢 已登记（非回归，接受）|
| DV-012 | P4-T04 `TMaxComputeTableSink.partition_columns`(field 14) 源：legacy `MaxComputeTableSink` 取 `targetTable.getPartitionColumns()`（fe-core Doris `Column`）；连接器 `MaxComputeWritePlanProvider.planWrite` 取 `odpsTable.getSchema().getPartitionColumns()`（odps-sdk 列）——**源不同、值同**（分区列名）| [tasks/P4 P4-T04](./tasks/P4-maxcompute-migration.md) / [P4-T04 设计](./tasks/designs/P4-T04-write-plan-design.md) | 2026-06-06 | 🟢 已落地（P4-T04，值等价）|
| DV-011 | P4-T03 连接器事务 block 上限源：legacy fe-core `Config.max_compute_write_max_block_count`（fe.conf 可调，默认 20000）→ 连接器常量 `MAX_BLOCK_COUNT=20000L`（import-gate 禁 `common.Config`，丢可调性）；附 legacy `throws UserException`→`DorisConnectorException`（unchecked，SPI 面无 checked throws）| [tasks/P4 P4-T03](./tasks/P4-maxcompute-migration.md) / [P4-T03 设计](./tasks/designs/P4-T03-write-txn-design.md) | 2026-06-06 | 🟢 已修正（P4-T03 硬编 → GC1 经 session-property 透传恢复 fe.conf 可调，`95575a4954d`）|
| DV-010 | P4-T01 修共享 fe-core `ConnectorColumnConverter.toConnectorType` 丢 CHAR/VARCHAR 长度（写 `precision=0`；长度存 `len` 非 `precision`）→ CREATE TABLE 经 SPI 丢长度。特判 CHAR/VARCHAR 把 `getLength()` 写入 precision 字段（与逆 `convertScalarType`+`MCTypeMapping` 约定一致）| [tasks/P4 P4-T01](./tasks/P4-maxcompute-migration.md) / `ConnectorColumnConverter` | 2026-06-06 | 🟢 已修正（P4-T01）|
| DV-009 | W5 写 sink 收口位置：RFC/handoff「route 3 个 visitPhysicalXxxTableSink + 新建 PluginDrivenTableSink」与代码不符；plugin-driven 写经 `visitPhysicalConnectorTableSink` + 既有 `PluginDrivenTableSink`，W5 改为在其上 layer `planWrite()` | [写 RFC §5.5/§12 W5](./tasks/designs/connector-write-spi-rfc.md) / [HANDOFF W5](./HANDOFF.md) | 2026-06-06 | 🟢 已修正（W5 `9ebe5e27fa4`）|
| DV-008 | P3-T07 parity 两处 SPI↔legacy 偏差：列名 casing 当场修；Hudi meta-field 推迟批 E | [tasks/P3 §批C/T07](./tasks/P3-hudi-migration.md) | 2026-06-05 | 🟢 已修正 |
| DV-007 | P3 批 B scope 校正：T05 `listPartitions*` override 推迟批 E（零 live caller、Hive 不 override）；T06 MVCC 保持 default opt-out（非抛异常 override）| [HANDOFF 未完成 #1/#2](./HANDOFF.md) / [tasks/P3 T05/T06](./tasks/P3-hudi-migration.md) | 2026-06-05 | 🟢 已修正（T05 裁剪已落地；list*/MVCC 入批 E）|
| DV-006 | P3-T03 schema_id/history 非批 A 可修（连接器缺 field-id/InternalSchema/type→thrift；裸基线会回归）；推迟批 E | [HANDOFF 1b ①](./HANDOFF.md) / [tasks/P3 T03](./tasks/P3-hudi-migration.md) | 2026-06-05 | 🟡 推迟（批 E）|
| DV-005 | P3 hudi「HMS-over-SPI 前置依赖」与代码不符；真阻塞=catalog 模型错配 | [connectors/hudi.md](./connectors/hudi.md) / [master plan §3.4](./00-connector-migration-master-plan.md) / D-005 | 2026-06-04 | 🟡 待修正（P3 模型决策）|
| DV-004 | T13 用户向安装文档不在本代码仓（在 doris-website 仓） | [tasks/P2 T13](./tasks/P2-trino-connector-migration.md) | 2026-06-04 | 🟢 已修正 |
| DV-003 | T12 回归测试引用不存在的先例/目录且本地不可运行 | [tasks/P2 T12](./tasks/P2-trino-connector-migration.md) | 2026-06-04 | 🟡 推迟 |
| DV-002 | T11 无法 mock Trino plugin；JsonSerializer 非纯单元 | [tasks/P2 T11](./tasks/P2-trino-connector-migration.md) | 2026-06-04 | 🟢 已修正 |
| DV-001 | 批 D 范围遗漏 ExternalCatalog db 路由 + legacy test | [tasks/P2 T08-T10](./tasks/P2-trino-connector-migration.md) | 2026-06-04 | 🟢 已修正 |

---

## 详细记录（时间倒序）

### DV-015 — P4-T06e FIX-PRUNE-PUSHDOWN：端到端裁剪下推 wiring 无 fe-core 单测（KNOWN-LIMITATION）

- **发现日期**：2026-06-08
- **发现 session / agent**：FIX-PRUNE-PUSHDOWN clean-room review（workflow `w31i0vfo5`，test-quality lens，4 finding 全 verifier 判 minor/非 must-fix）
- **当前状态**：🟢 已登记（逻辑半 UT+mutation 守门，wiring 半 + 真实裁剪生效待 live e2e）
- **原计划位置**：[FIX-PRUNE-PUSHDOWN 设计](./tasks/designs/P4-T06e-FIX-PRUNE-PUSHDOWN-design.md) §Test Plan
- **偏差描述**：本 fix 三处产线点无 fe-core 端到端 UT：① `PluginDrivenScanNode.getSplits()` 的 pruned-to-zero 短路（`requiredPartitions!=null && isEmpty()→return emptyList()`）；② `PhysicalPlanTranslator` plugin 分支 `setSelectedPartitions(fileScan.getSelectedPartitions())` 注入；③ `getSplits→planScan` 6 参 requiredPartitions threading。原因：`PluginDrivenScanNode` 是 `FileQueryScanNode` 子类，裸构造需绕 ctor 链 + stub `getScanPlanProvider`/`buildColumnHandles`/`buildRemainingFilter`/`applyLimit`（无现成轻量 analyze/spy harness；同 [DV-014] 外表 bind harness 缺位）。
- **覆盖经**：① 最易错的三态映射逻辑（NOT_PRUNED→null / pruned-非空→names / pruned-空→空 list）由 `PluginDrivenScanNodePartitionPruningTest`（5 测）+ mutation（去 `!isPruned` 守卫双红）pin；② 名→PartitionSpec 转换由 `MaxComputeScanPlanProviderTest`（3 测）+ mutation（恒 emptyList 红）pin；③ wiring 半（短路/注入/threading 单变量直线流）+ **真实裁剪生效** 由 p2 live `test_max_compute_partition_prune.groovy` 覆盖——真值证据 = EXPLAIN/profile 仅扫目标分区（split 数/规划耗时 ≪ 全表）+ `WHERE pt='不存在'`→0 行且不建全分区 session。
- **为何可接受**：与既有约定一致（`HiveScanNodeTest`/legacy-MC/Hudi 的 translator 注入均无 translator 级测，`HiveScanNodeTest:99-115` 直构 node 调 setter）；fail-safe（默认 `selectedPartitions=NOT_PRUNED`→`resolveRequiredPartitions`→null→scan all，去 wiring 退化为修前全表扫**非丢数据**）。
- **影响范围**：仅测试覆盖层；产线行为正确。
- **关联**：[D-031]、[review-rounds](./reviews/P4-T06e-FIX-PRUNE-PUSHDOWN-review-rounds.md)、[复审 §B DG-1](./reviews/P4-maxcompute-full-rereview-2026-06-07.md)、[DV-014]（同类 harness 缺位）
- **后续动作**：
  - [ ] 待外表 scan 的 fe-core spy/analyze harness 落地（`MaxComputeScanNodeTest`/`PaimonScanNodeTest` 用 `Mockito.spy`+反射，可借鉴），补 `getSplits()` 短路 + threading 的 CI 级测，把 correctness 不变式从 live-only 提到 CI。
  - [ ] **live e2e（必经）**：真实 ODPS 跑 `test_max_compute_partition_prune.groovy`，并核 EXPLAIN/profile 证裁剪真正下推（行正确不足以证——修前行已正确）。

### DV-014 — P4-T06e FIX-BIND-STATIC-PARTITION：bind 期 full-schema 投影无 fe-core 单测（KNOWN-LIMITATION）

> 补登：本条索引行（见上）此前已录，详细记录段遗漏，现补齐（doc-sync 横切债）。

- **发现日期**：2026-06-07
- **发现 session / agent**：FIX-BIND-STATIC-PARTITION clean-room review（workflow `wi3mnjymb`/`wy299gtsh`/`wlwpw0b2s`，test-quality lens）
- **当前状态**：🟢 已登记（无 harness，parity + p2 覆盖；待外表 analyze harness 落地补）
- **原计划位置**：[FIX-BIND-STATIC-PARTITION 设计](./tasks/designs/P4-T06e-FIX-BIND-STATIC-PARTITION-design.md) / [D-030]
- **偏差描述**：`bindConnectorTableSink` 的 full-schema 投影（NULL 填充 + 分区列末尾 + 按位置投影）未被 connector-path 单测直接 pin——`bind()` 经 `RelationUtil.getDbAndTable` 真 Env 解析，外表 PluginDriven catalog 需连接器插件，无现成轻量 analyze harness（OLAP analyze 测仅覆盖 `createTable` 内表）。
- **覆盖经**：① 与 legacy `bindMaxComputeTableSink` 及 Iceberg 路径**共享** helper `getColumnToOutput`/`getOutputProjectByCoercion`（被既有 OLAP/Hive/Iceberg insert 测覆盖）；② 列选择 helper `selectConnectorSinkBindColumns` 单测 + 分布 full-schema 索引测；③ p2 live `test_mc_write_insert` Test 3/3b + `test_mc_write_static_partitions`。
- **关联**：[D-030]、[review-rounds](./reviews/P4-T06e-FIX-BIND-STATIC-PARTITION-review-rounds.md)、[DV-015]（同类 harness 缺位）
- **后续动作**：[ ] 待外表 analyze harness 落地补 bind 投影 CI 级测。

### DV-013 — P4-T06e FIX-WRITE-DISTRIBUTION：两处 planner 写分发 parity 微差（均非回归）

- **发现日期**：2026-06-07
- **发现 session / agent**：FIX-WRITE-DISTRIBUTION clean-room review（workflow `ww1g95bba`，Phase A parity/delivery lens）
- **当前状态**：🟢 已登记（非回归，接受；default `enable_strict_consistency_dml=true` 下与 legacy MC 同果）
- **原计划位置**：[FIX-WRITE-DISTRIBUTION 设计](./tasks/designs/P4-T06e-FIX-WRITE-DISTRIBUTION-design.md)（§"Known minor divergence — ShuffleKeyPruner" + §"Why no change in RequestPropertyDeriver"）
- **偏差描述**：
  - **① ShuffleKeyPruner**：`ShuffleKeyPruner.visitPhysicalConnectorTableSink`（通用 connector 分支，`:286-295`）缺 legacy `visitPhysicalMaxComputeTableSink`（`:272-283`）的 `enableStrictConsistencyDml==false → childAllowShuffleKeyPrune=true` 短路；通用分支恒 `required.equals(ANY)?true:false`。
  - **② local-sort under non-strict**：`enable_strict_consistency_dml=false` 时 `RequestPropertyDeriver` 对 connector sink（required≠GATHER）下推 `ANY` → 动态分区 hash+local-sort 需求被丢。
- **为何非回归**：default `enable_strict_consistency_dml=`**`true`**（`SessionVariable.java:1566`）下——① 两路均 `required≠ANY → prune=false`（**同果**）；② `RequestPropertyDeriver` 下推 `getRequirePhysicalProperties()` = hash+local-sort（**enforce**，与 legacy MC 同）。仅 non-strict（用户显式关）时分歧：① 通用分支**少剪**（更保守 = missed optimization，无正确性损）；② local-sort 被丢——但 **legacy MC 在 non-strict 下亦丢**（`visitPhysicalMaxComputeTableSink` 同样下推 ANY）→ parity，非本 fix 引入。clean-room review Phase B 把 ① 多数 refute 为 non-regression。
- **影响范围**：仅 `enable_strict_consistency_dml=false` 的 MaxCompute 动态分区写；default 不触及。① 纯性能（少剪 shuffle-key）；② 与 legacy 同行为。
- **关联**：[D-029]、[review-rounds](./reviews/P4-T06e-FIX-WRITE-DISTRIBUTION-review-rounds.md)、[复审 §A.NG-2/NG-4](./reviews/P4-maxcompute-full-rereview-2026-06-07.md)
- **后续动作**：
  - [ ] 如需 non-strict 下完全 parity：给 `ShuffleKeyPruner` 通用 connector 分支补 `enableStrictConsistencyDml` 短路（影响 jdbc/es 共享分支，超本 fix scope）

### DV-012 — P4-T04：`partition_columns` 取 ODPS 表列（源不同、值同）

- **发现日期**：2026-06-06
- **发现 session / agent**：P4 Batch B session（P4-T04 写计划实现，核读 legacy `MaxComputeTableSink.bindDataSink`）
- **当前状态**：🟢 已落地（P4-T04，值等价）
- **原计划位置**：[P4-T04 设计](./tasks/designs/P4-T04-write-plan-design.md)（港 legacy `MaxComputeTableSink` 静态字段）
- **偏差描述**：legacy `MaxComputeTableSink.bindDataSink` 填 `TMaxComputeTableSink.partition_columns`(field 14) 取 `targetTable.getPartitionColumns()`（fe-core Doris `Column` 名）。连接器 import-gate 禁 fe-core `catalog.Column`，且 planWrite 持的是 `MaxComputeTableHandle`（携 odps-sdk `Table`）非 fe-core 表。
- **新方案**：连接器 `MaxComputeWritePlanProvider.planWrite` 取 `mcHandle.getOdpsTable().getSchema().getPartitionColumns()`（odps-sdk `com.aliyun.odps.Column` 名）。**源不同（ODPS schema vs fe-core Column）、值同（分区列名字符串）**——BE 经 field 14 收到相同分区列名 list。同源亦用于静态分区串的列序（`MCTransaction.beginInsert` 用 fe-core 列序，连接器用 ODPS 列序，序同）。
- **影响范围**：连接器 `MaxComputeWritePlanProvider`（dormant，gate 关，零 live）。行为等价：BE 收到的 `partition_columns` 内容不变。
- **关联**：P4-T04、[P4-T04 设计](./tasks/designs/P4-T04-write-plan-design.md)、[D-025]

---

### DV-011 — P4-T03：连接器事务 block 上限 + 异常类型（import-gate 禁 fe-core common）

- **发现日期**：2026-06-06
- **发现 session / agent**：P4 Batch B session（P4-T03 写前核实 import-gate 边界：`org.apache.doris.common.{Config,UserException}` 均在禁列）
- **当前状态**：🟢 已修正（P4-T03 硬编 → GC1 经 session-property 透传恢复 fe.conf 可调性，`95575a4954d`）
- **原计划位置**：[P4-T03 设计](./tasks/designs/P4-T03-write-txn-design.md)（港 legacy `MCTransaction` block 分配 + commit）
- **偏差描述**：legacy `MCTransaction.allocateBlockIdRange` 用 fe-core `Config.max_compute_write_max_block_count`（默认 20000，fe.conf 可调）作上限、并 `throws UserException`。连接器 import-gate 禁 `org.apache.doris.common.*`（含 `Config`/`UserException`），二者均不可 import。
- **新方案**：① 上限改连接器常量 `MaxComputeConnectorTransaction.MAX_BLOCK_COUNT = 20000L`（镜像 legacy 默认值，**丢 fe.conf 可调性**；Rule 2 不投机，如需再经 `MCConnectorProperties` 暴露）。② 校验失败抛 `DorisConnectorException`（unchecked；SPI `ConnectorTransaction.allocateWriteBlockRange` 面无 checked throws，W4 `PluginDrivenTransaction` 适配）。
- **影响范围**：连接器 `MaxComputeConnectorTransaction`（dormant，gate 关，零 live）。行为：block 上限值不变（20000），仅来源 Config→常量；异常类型 UserException→DorisConnectorException（语义等价的写失败）。
- **关联**：P4-T03、[P4-T03 设计](./tasks/designs/P4-T03-write-txn-design.md)、[D-024]
- **后续动作**：
  - [x] 已恢复 fe.conf 可调（GC1 FIX-BLOCKID-CAP-CONFIG，`95575a4954d`）：经 **session-property 透传**——fe-core `ConnectorSessionBuilder.extractSessionProperties` 注入 `Config.max_compute_write_max_block_count`（镜像既有 `lower_case_table_names`），连接器 `MaxComputeConnectorMetadata.resolveMaxBlockCount` 读 `ConnectorSession.getSessionProperties()` 透传 ctor。**非**原拟 `MCConnectorProperties`（那是 catalog-scoped、错 scope）；本机制读 fe-core 全局 Config = true legacy parity。

### DV-010 — P4-T01：共享 fe-core ConnectorColumnConverter 丢 CHAR/VARCHAR 长度，特判修复（用户签字）

- **发现日期**：2026-06-06
- **发现 session / agent**：P4 Batch A session（P4-T01 启动前 code-grounded 核读 `ConnectorColumnConverter.toConnectorType` + `ScalarType`：CHAR/VARCHAR 长度存 `len`、`getScalarPrecision()` 返 `precision`=0；既有 `ConnectorColumnConverterTest` 无 CHAR/VARCHAR 断言）
- **当前状态**：🟢 已修正（P4-T01；fe-core `ConnectorColumnConverter` 特判 + 回归测 `testCharVarcharLengthPreserved`，Tests run 9/0F0E）
- **原计划位置**：P4-T01 原框定「连接器-only、gate 关」；`ConnectorColumnConverter.toConnectorType`（P0-T15 期建）ScalarType 分支统一用 `getScalarPrecision()`/`getScalarScale()`
- **偏差描述**：连接器 `createTable` 消费的 `ConnectorCreateTableRequest` 列类型经 `ConnectorColumnConverter.toConnectorType(Type)` 产生；其 ScalarType 分支对 CHAR/VARCHAR 用 `getScalarPrecision()`（=`precision` 字段，CHAR/VARCHAR 默认 0），而长度实存 `len`（`getLength()`）→ 请求里 CHAR(n)/VARCHAR(n) **丢长度**（legacy `dorisScalarTypeToMcType` 用 `getLength()` 保留）。这是 P0 转换器的**逆一致性 bug**（其逆向 `convertScalarType` + 连接器 `MCTypeMapping` 约定「CHAR/VARCHAR 长度在 precision 字段」），是 CHAR/VARCHAR DDL 经 SPI 真正达 parity 的唯一路径。
- **新方案**（用户 AskUserQuestion 签字「修 fe-core 转换器」）：`toConnectorType` 特判 CHAR/VARCHAR，把 `getLength()` 写入 ConnectorType precision 字段（与逆向约定一致）；其余类型不变；加回归测 `ConnectorColumnConverterTest#testCharVarcharLengthPreserved`。
- **替代方案**：连接器侧对 CHAR/VARCHAR 缺长度 fail-loud + 记 OQ 推迟（保 Batch A 连接器-only 边界，但 CHAR/VARCHAR DDL 暂不可用）——用户否决。
- **影响范围**：
  - 代码：fe-core `ConnectorColumnConverter.toConnectorType`（+ import `PrimitiveType`）+ test。**触碰共享 P0 代码**：对 live 的 jdbc/es CREATE TABLE CHAR/VARCHAR 行为变更（「丢长度」→「保留长度」，严格更正确，低风险）。
  - 文档：本条 + [tasks/P4](./tasks/P4-maxcompute-migration.md) + [PROGRESS](./PROGRESS.md)（§四/§六计数）。
  - 计划：P4-T01 范围从「连接器-only」微扩至含 1 处 fe-core 转换器修复。
- **关联**：P4-T01、P0-T15（converter）、[D-023]
- **后续动作**：
  - [x] 修 `toConnectorType` + 回归测（P4-T01）
  - [ ] Batch E：连接器 DDL parity 测覆盖 CHAR/VARCHAR 端到端

### DV-009 — W5 写 sink 收口位置与 RFC/handoff 措辞不符：plugin-driven 写已有专路，改为 layer planWrite

- **发现日期**：2026-06-06
- **发现 session / agent**：W-phase 实现 session（W5 启动前 2 路 Explore code-grounded recon：sink 入参 + nereids 写 sink 接线；主线 firsthand 核读 `PhysicalPlanTranslator.visitPhysicalConnectorTableSink` / `planner/PluginDrivenTableSink`）
- **当前状态**：🟢 已修正（W5 commit `9ebe5e27fa4`；用户 AskUserQuestion 签字「Corrected W5 (layer planWrite)」）
- **原计划位置**：[写 RFC §5.5 / §12 W5](./tasks/designs/connector-write-spi-rfc.md)、[HANDOFF W5 锚点](./HANDOFF.md)——原措辞：「新建 fe-core `PluginDrivenTableSink` + `PhysicalPlanTranslator` 各 `visitPhysicalXxxTableSink`（hive/iceberg/mc）→ `planWrite()`，保 PhysicalXxxSink fallback」。
- **偏差描述**：RFC/handoff 写于不知既有路径之时。实测（recon + firsthand 核读）：
  1. `PluginDrivenTableSink` **已存在**（`planner/PluginDrivenTableSink.java`，P0/P1 JDBC 期建），非新建。
  2. plugin-driven 写 INSERT **不**走 `visitPhysicalHive/Iceberg/MaxComputeTableSink`（那 3 个服务 legacy 非 plugin-driven 表）；走专路 `UnboundConnectorTableSink → LogicalConnectorTableSink → PhysicalConnectorTableSink → visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:644`），已据 `ConnectorWriteConfig`（config-bag）建 `PluginDrivenTableSink`。mc/hive/iceberg 迁 plugin-driven 后走此专路 → 在那 3 个 concrete 方法加 planWrite 路由是**死代码**。
  3. 两写-sink 模型并存：既有 **config-bag**（连接器返 `ConnectorWriteConfig` 属性包，fe-core 建 `THiveTableSink`/`TJdbcTableSink`；表达不了 mc/iceberg）⊥ 新 **opaque-sink**（W1 `ConnectorWritePlanProvider.planWrite()` 连接器自建 `TDataSink`，RFC §5.5 E 决策，可泛化）。RFC 未察 config-bag 已存在，故未调和二者。
- **新方案**（用户签字）：在既有 `visitPhysicalConnectorTableSink` + `PluginDrivenTableSink.bindDataSink` 上 **layer** `planWrite()` 为优先路径（`connector.getWritePlanProvider() != null` 时），config-bag 为 fallback。**不动** 3 个 concrete visit 方法。零行为变更（无连接器 override `getWritePlanProvider`，jdbc 仍走 config-bag）。`ConnectorWriteHandle`/`ConnectorSinkPlan`（W1）形状经使用确认充分，无需改。
- **缩界（R12 不静默）**：overwrite / 静态分区 / writePath 等 connector-specific write context 的 handle 填充留 P4 adopter（base `InsertCommandContext` 为空 marker，无通用 overwrite；强行 instanceof 子类会再耦合 fe-core）。W5 仅建 seam（空 context）。

---

### DV-008 — P3-T07 parity 暴露两处 SPI↔legacy 偏差：列名 casing 当场修；Hudi meta-field 纳入推迟批 E

- **发现日期**：2026-06-05
- **发现 session / agent**：P3 批 C session（T07 启动前 5-agent code-grounded recon workflow `p3-t07-recon`：cow-mor / legacy-types / spi-types / hms-surface / hive-surface + 主线核读 `HudiConnectorMetadata`/`HudiTypeMapping`/`HMSExternalTable.initHudiSchema`/`ThriftHmsClient`）
- **当前状态**：🟢 已修正（gap-1 casing 已修 + 测；gap-2 meta-field 推迟批 E 实证）
- **原计划位置**：[tasks/P3 §批 C/T07](./tasks/P3-hudi-migration.md)（「parity 测试——SPI `HudiConnectorMetadata` schema/partition 输出 vs legacy `getHudiTableSchema`」）——原计划隐含假定 SPI schema 输出与 legacy parity，仅需写测试验证
- **偏差描述**：parity recon 实证 SPI avro→column 变换与 legacy `HMSExternalTable.initHudiSchema` 有两处偏差（其余逐类型一致，见设计备忘矩阵）：
  1. **gap-1 列名 casing**：SPI `HudiConnectorMetadata.avroSchemaToColumns` 用 `field.name()` 原样；legacy 在 `HMSExternalTable.java:745` `toLowerCase(Locale.ROOT)`（**仅顶层列名**；嵌套 struct 字段名两侧均不降）。mixed-case avro 列名时 SPI 保留原 case → 破 parity（BE name-match 大小写敏感，见 DV-006 / T03）。
  2. **gap-2 Hudi meta-field 纳入**：SPI `getSchemaFromMetaClient` 调无参 `TableSchemaResolver.getTableAvroSchema()`；legacy `getHudiTableSchema:852` 调 `getTableAvroSchema(true)`。`true` 很可能强制纳入 `_hoodie_*` meta 列，无参默认随 Hudi 版本/表配置（`populateMetaFields`）变 → 可能改变列集合。无真实 metaclient 不可单测判定（同 T03 族）。
- **触发场景**：T07 parity recon（golden-value 法，因 fe-core 只依赖 fe-connector-api/-spi、不依赖具体连接器模块，无跨模块编译路径）+ 用户 AskUserQuestion 签字（2026-06-05，「Also fix casing now」+「Focused baseline」）。
- **新方案**：
  - **gap-1 当场修**（用户签字）：`avroSchemaToColumns` 顶层列名改 `toLowerCase(Locale.ROOT)`，镜像 legacy:745（仅顶层；嵌套 struct 名保持 raw，两侧一致）。已核安全：`ThriftHmsClient.convertFieldSchemas:303` 用 `fs.getName()` 不防御降字，但 Hive Metastore 自身存小写标识符 → 降 avro 路径列名与小写 HMS partition key 对齐（改善 `getColumnHandles` 匹配），无回归。`avroSchemaToColumns` 由 `private`→package-private `static`（零行为变更，使可单测）。
  - **gap-2 推迟批 E**（DV-006 同族）：无真实 fixture 不可判定 + 属 schema-evolution/meta-field 机制，与 hive/HMS migration 一并实证。T07 parity 测不依赖该差异（测纯 avro→column 变换）。
  - **缩界（R12 不静默）**：`ThriftHmsClient` 源头防御性降字（与 hive 模块共享）**不在 T07 改**——触碰 hive 行为属 P7/批 E。
- **替代方案**：(gap-1) 不修、仅 pin 现状 + 记 DV 推批 E（precedent T03/T05）——用户否决，选当场修（trivially-correct，对齐 legacy + 小写 HMS）；(gap-2) 当场加 `(true)`——否决（无真实 metaclient 不可验证语义，脆测）。
- **影响范围**：
  - 文档：本条 + [tasks/P3](./tasks/P3-hudi-migration.md)（T07 ✅ + 验收 + 阶段日志）+ [PROGRESS](./PROGRESS.md)（§一/二/三/四/六/七）+ [connectors/hudi.md](./connectors/hudi.md)（概况 + playbook 12 + 进度日志）+ [HANDOFF](./HANDOFF.md)。
  - 代码：gap-1 `HudiConnectorMetadata.avroSchemaToColumns`（降字 + 可见性）+ 6 测试文件（hudi 3 改/新 + hms 1 + hive 2）；gap-2 零代码。
  - 计划：批 C = {三模块测试基线 ✅, COW/MOR schema parity ✅, gap-1 casing 修 ✅}；gap-2 meta-field 入批 E。
- **关联**：P3-T07、DV-006（同族 schema-evolution 推批 E）、P3-T10/T11（批 E）、[D-019](./decisions-log.md)（hybrid）、[`designs/P3-T07-test-baseline-design.md`](./tasks/designs/P3-T07-test-baseline-design.md)
- **后续动作**：
  - [x] gap-1 casing 修 + `HudiSchemaParityTest` casing pin（顶层降、嵌套 struct 名保留）
  - [x] 三模块测试基线（hms `HmsTypeMappingTest` 12 / hive `HiveFileFormatTest` 6 + `HiveConnectorMetadataPartitionPruningTest` 8 / hudi `HudiTypeMappingTest`+7 + `HudiSchemaParityTest` 3 + `HudiTableTypeTest` 4 = 33 全绿）
  - [ ] 批 E：gap-2 meta-field 纳入（`getTableAvroSchema(true)` vs 无参）真实 fixture 实证
  - [ ] 批 E/P7：`ThriftHmsClient` 源头防御性降字（与 hive 共享）

### DV-007 — P3 批 B scope 校正：T05 `listPartitions*` override 推迟批 E；T06 MVCC 保持 default opt-out（非抛异常 override）

- **发现日期**：2026-06-05
- **发现 session / agent**：P3 批 B session（T05/T06 启动前 5-reader code-grounded recon workflow：hudi-current / hudi-resolve / hive-ref / spi-invoke / mvcc-t06 + 主线核读 `HudiConnectorMetadata`/`HiveConnectorMetadata` 全文 + grep fe-core 调用方）
- **当前状态**：🟢 已修正（T05 applyFilter EQ/IN 裁剪已落地 commit `10b72d4`；list*/MVCC 完整实现入批 E）
- **原计划位置**：[HANDOFF.md 未完成 #1/#2](./HANDOFF.md)（「T05：`listPartitions/listPartitionNames/listPartitionValues` override + 真实 applyFilter EQ/IN 分区裁剪」；「T06：大概率**显式 unsupported**（与 T04 fail-loud 一致）」）+ [tasks/P3 §T05/T06](./tasks/P3-hudi-migration.md)
- **偏差描述**：原计划把 T05 的「`listPartitions*` override」与「applyFilter 裁剪」并列为批 B 交付；并暗示 T06 应**新增抛异常的 MVCC override**。recon 实测两点前提失真：
  1. **T05 `listPartitions*` 零 live caller + Hive 不 override**：SPI `ConnectorMetadata.listPartitionNames/listPartitions/listPartitionValues` 在 fe-core **无任何调用方**——`PluginDrivenScanNode` 不调用（分区经 `applyFilter`→`prunedPartitionPaths`→`resolvePartitions` 链路）；`ShowPartitionsCommand`/`HudiExternalMetaCache`/`MetadataGenerator` 调的是 **legacy** metastore 路径（`dorisTable.getRemoteName()`），非 SPI。对标 `HiveConnectorMetadata`（批 B 基准）**也不 override** 这三方法。→ 现 override = 不可测的死代码（违 R2 nothing speculative / R9 测意图）。
  2. **T06「显式 unsupported」违 SPI opt-out 约定**：三个 MVCC 方法 default 即 `Optional.empty()`（= 不支持），`FakeConnectorPluginTest` 有显式断言；`Iceberg`/`Paimon`/`Hive`/`Trino` **全部依赖 default**，无一 override；MVCC 方法**无 production caller**（仅测试用 adapter）；且 T04 已在唯一可触发点（time-travel）`visitPhysicalHudiScan` 抛 `AnalysisException`。→ 新增抛异常 override = 唯一打破约定 + 不可达死代码（违 R11 conformance / R3 surgical）。
- **触发场景**：T05/T06 启动前 recon + grep fe-core 调用方；用户 AskUserQuestion 签字（2026-06-05，「Pruning only, defer list*」+「Keep defaults + document」）。
- **新方案**：
  - **T05** = 仅 applyFilter 真实 EQ/IN 裁剪（忠实镜像 Hive 7 步 + 7 helper，保留 `List<String>` 路径表示与 `-1` 上限）；`listPartitions*` override **推迟批 E**（届时 fe-core 长出 SPI 消费 + `SHOW PARTITIONS` 改走 SPI 时一并做）。已落地 `10b72d4`（8 单测、checkstyle 0、import-gate 通过）。
  - **T06** = **不 override，保持 default `Optional.empty()` opt-out + 文档化**（零代码）；正确的 fail-loud 已在 T04 的 translator 守卫。完整 MVCC（`HudiMvccSnapshot`、snapshot 透传、增量时序）入批 E。见 [`designs/P3-T06-mvcc-design.md`](./tasks/designs/P3-T06-mvcc-design.md)。
- **替代方案**：(T05) 现 override 三方法委托 HMS——否决（死代码、无可测意图、Hive 无先例）；(T06) 新增抛异常 override——否决（破 opt-out 约定、不可达、与全体连接器分叉、T04 已覆盖）。
- **影响范围**：
  - 文档：本条 + [tasks/P3](./tasks/P3-hudi-migration.md)（T05 ✅ 裁剪 + T06 ✅ 决策 + 验收标准 + 阶段日志）+ [PROGRESS](./PROGRESS.md)（§一 P3 / §三 / §四 / §六计数）+ [connectors/hudi.md](./connectors/hudi.md)（E5/E10 + 进度日志）。
  - 代码：T05 已合入 `10b72d4`（applyFilter 裁剪 + 单测）；T06 零代码。
  - 计划：批 B 范围由 {T05 裁剪+list* override, T06 throwing override} 收为 {T05 裁剪 ✅, T06 keep-defaults ✅}；list*/完整 MVCC 与 T03/T09–T11 同批 E。
- **关联**：[DV-005](#dv-005--p3-hudi-的hms-over-spi-前置依赖与代码实际状态不符真正阻塞是-catalog-模型错配)（其后续动作「listPartitions override + 真实 applyFilter 裁剪」本条落地裁剪部分）、P3-T05、P3-T06、P3-T10/T11（批 E）、[D-019](./decisions-log.md)（hybrid）、[P3-T04](./tasks/designs/P3-T04-fail-loud-design.md)
- **后续动作**：
  - [x] T05 applyFilter EQ/IN 裁剪 + 单测（`10b72d4`）
  - [ ] 批 E：`listPartitions*` override（fe-core SPI 消费就绪 + `SHOW PARTITIONS` 走 SPI 后）
  - [ ] 批 E：完整 MVCC（`HudiMvccSnapshot` + snapshot 透传 + 增量时序），time-travel 从 T04 fail-loud 转为正确快照

### DV-006 — P3-T03（schema_id / history_schema_info）不是 model-agnostic 的批 A SPI-surface 修复；推迟到批 E

- **发现日期**：2026-06-05
- **发现 session / agent**：P3 批 A session（T03 启动前 code-grounded recon：4-reader workflow 读 SPI hook + Paimon/ES 参照 + legacy 路径 + thrift/BE 消费端；主线对 BE `table_schema_change_helper.h` 二次核读）
- **当前状态**：🟡 推迟（批 E，并入 hive/HMS migration）
- **原计划位置**：[HANDOFF.md 关键认知 1b HIGH ①](./HANDOFF.md) + [DV-005 后续动作 ①](#dv-005--p3-hudi-的hms-over-spi-前置依赖与代码实际状态不符真正阻塞是-catalog-模型错配) + [tasks/P3 §P3-T03](./tasks/P3-hudi-migration.md)：「schema_id/history 缺→退化名匹配；可经现有 SPI hook `populateScanLevelParams`（Paimon/ES 已 override）+ `HudiScanRange` 设 schema_id 修复，**无需 fe-core 改动**」
- **偏差描述**：原评估认为 ① 是「多在 SPI surface 内可修」的 model-agnostic 修复。recon 实测发现**前提不成立**：
  1. **BE 语义**（`be/src/format/table/table_schema_change_helper.h:219-267`）：`history_schema_info` **unset** → `by_parquet_name`/`by_orc_name`（**鲁棒名匹配**，处理大小写 / 缺列）——**即当前 SPI hudi 路径行为**；`current_schema_id == file_schema_id` → **`ConstNode`**（`:92-121`）= **纯 identity-by-name**、**大小写敏感**、假设精确匹配（其注释自陈需注意大小写）；id 不同 → `by_table_field_id`（**唯一**做 field-id / 改名 / evolution 的路径）。
  2. **「Paimon/ES 已 override」前提失真**：二者 override `populateScanLevelParams` 是为 **predicate / docvalue**，**并不设** schema evolution 元数据（recon 实证）——**无任何 SPI 先例**发 schema_id/history。
  3. **连接器缺料**：`HudiColumnHandle` **无 field id**（仅 `name`/`typeName` 串/`isPartitionKey`）；SPI hudi 连接器**无 Hudi `InternalSchema` 版本跟踪**（legacy 走 `getCommitInstantInternalSchema`）；连接器模块**无 type→`TColumnType` thrift 转换**（legacy 在 fe-core `ExternalUtil.getExternalSchema`，import gate 禁止复用）。
  4. **裸基线会回归**：若仅设 `current==file==-1`（→ ConstNode）= identity-by-name 大小写敏感，**严格弱于**当前名匹配（丢大小写 / 缺列处理）——**净回归**；而真正的 field-id evolution 路径需上述全部缺料。
- **触发场景**：T03 启动前 recon + 主线核读 BE `gen_table_info_node_by_field_id` / `ConstNode` / `StructNode`。
- **新方案**：**T03 推迟到批 E**，与 hive/HMS migration 一次性建齐机制（column-handle field id + Hudi `InternalSchema` 版本 + Avro/ConnectorType→`TColumnType` thrift + `populateScanLevelParams` 设 current+history + 每-split `THudiFileDesc.schema_id`）。批 A 不发任何 schema 元数据（保持现状名匹配，**零回归**），不 ship 裸 ConstNode 基线。用户已签字（2026-06-05，AskUserQuestion「Defer T03 to batch E」）。
- **替代方案**：(a) 批 A 内建全套 field-id/InternalSchema/type→thrift 机制——否决（大、与批 E 重叠、触碰 live 可读 schema 路径、回归风险）；(b) 裸 ConstNode 基线——否决（净回归大小写/缺列）。
- **影响范围**：
  - 文档：本条 + [tasks/P3](./tasks/P3-hudi-migration.md)（T03 移入批 E、备注现状名匹配 + evolution gap）+ [PROGRESS](./PROGRESS.md)（§三 parity 行 / §六计数）+ [connectors/hudi.md](./connectors/hudi.md)。
  - 代码：无（recon + 决策，零改动）。
  - 计划：批 A 范围由 {T02,T03,T04} 收为 {T02 ✅, T04}；T03 与 T09–T11 同批 E。
- **关联**：[DV-005](#dv-005--p3-hudi-的hms-over-spi-前置依赖与代码实际状态不符真正阻塞是-catalog-模型错配)（其后续 ① 本条修正）、P3-T03、P3-T10/T11（批 E）、[D-019](./decisions-log.md)（hybrid）、R-001
- **后续动作**：
  - [ ] 批 E：连接器 schema field-id + InternalSchema 版本 + type→thrift + `populateScanLevelParams` + per-split `schema_id`（faithful field-id evolution parity）
  - [x] 现状行为登记：SPI hudi 走 BE 名匹配（`by_parquet_name`/`by_orc_name`），common 无 evolution 可用；改名 / reorder-with-evolution 退化（非崩溃）

### DV-005 — P3 hudi 的「HMS-over-SPI 前置依赖」与代码实际状态不符；真正阻塞是 catalog 模型错配

- **发现日期**：2026-06-04
- **发现 session / agent**：P3 启动 recon session（8-agent code-grounded workflow + 2 路对抗验证；verdict `hmsMetadataOverSpiReady=false`, high confidence）
- **当前状态**：🟡 待修正（P3 catalog 模型决策，待用户签字）
- **原计划位置**：[connectors/hudi.md](./connectors/hudi.md)（「P3 启动前必须 P5 paimon 或 P7 hive 进入到至少完成 hms metadata 路径」）、[master plan §3.4/§3.8](./00-connector-migration-master-plan.md)、决策 D-005（用 `tableFormatType` 区分 DLA）
- **偏差描述**：原计划假设 HMS-over-SPI 元数据读路径要等 P5/P7 才落地、是 P3 的前置硬依赖。recon 实测（`branch-catalog-spi` HEAD `0793f032662`）发现该读路径**代码早已存在且非 stub**（源自更早的 #62183/#62821，一直 dormant 在 gate 后）：
  - `fe-connector-hms` = 共享 **HMS Thrift 客户端库**（`HmsClient`/`ThriftHmsClient`，**不是** ConnectorMetadata）；
  - `fe-connector-hive` `HiveConnectorMetadata`(type `"hms"`) 真实读路径 + applyFilter 真分区裁剪；
  - `fe-connector-hudi` `HudiConnectorMetadata`(type `"hudi"`) 从 Hudi Avro MetaClient 读 schema（HMS fallback）+ COW/MOR 探测 + `HudiScanPlanProvider` 快照扫描；
  - D-005 区分符 `ConnectorTableSchema.tableFormatType`(`:33/:58`) 已存在并被各连接器写入。

  但全部 **dormant**：`CatalogFactory.SPI_READY_TYPES = {jdbc, es, trino-connector}`(`CatalogFactory.java:52`) 不含 hms/hudi → HMS 系 catalog 永远走 legacy `HMSExternalCatalog`（零 live caller）。**真正阻塞不是缺 HMS 读码，而是 catalog 模型错配**：现存连接器注册独立 `"hudi"` catalog type（`HudiConnectorProvider.getType()=="hudi"`），而 Doris 真实模型是 hudi 寄生在 `"hms"` catalog 内、以 `HMSExternalTable.DLAType.HUDI` 暴露；fe-core 无 `"hudi"` catalog type，且 `PluginDrivenExternalTable` 从不消费 `tableFormatType`（只读 `getColumns()`，按 catalog TYPE 字串路由）→ 单个 `"hms"` 连接器没有 per-table HUDI/HIVE/ICEBERG 分流的 SPI 机制。附带确认缺口：增量读无 SPI 表示（P1-T04 `visitPhysicalHudiScan` SPI 分支丢弃 `getIncrementalRelation()`；MVCC trio 未实现；4 个 `*IncrementalRelation` 仍在 fe-core）；hive/hudi 未 override `listPartitions*`（Hudi applyFilter 列全部分区不裁剪，Hive applyFilter 做 EQ/IN 裁剪）；三模块零测试。**已验证非阻塞**：SPI scan/split 通用链路（`PluginDrivenScanNode.planScan`→BE）已被合入的 trino-connector 走通；hudi-specific 的「单 ScanNode 混合 COW-native + MOR-JNI 每-split 格式」正确性才是待验证项。
- **触发场景**：用户准备启动 P3，要求 code-grounded 确认 HMS 就绪情况。
- **新方案**：P3 不再以「等 P5/P7 交付 HMS-over-SPI」为前提；改为 (1) recon SPI scan/split 路径（hudi-specific 正确性），(2) 写 catalog 模型决策备忘（见下），用户签字后再编码。**不要直接 flip `SPI_READY_TYPES`**。
- **替代方案（catalog 模型，待用户决策）**：
  - **(a) hms-first**：`HiveConnectorProvider(type="hms")` 接入 `PluginDrivenExternalCatalog` + fe-core 消费 `tableFormatType` 分流，hudi 作薄增量。一次命中真正架构阻塞、契合现存 `type="hms"` 设计；但把 P7(hive/HMS) 范围拉进 P3、触碰 live 重度使用的 HMS 路径、零测试网，回归风险大。
  - **(b) gate 后建脚手架**：先做 format-dispatch / 增量 SPI hook / MVCC + 补测试（design+stub，不动 live 路径、零回归）；但 hudi 不单独端到端可用，推迟模型决策。
  - **(c) 直接 flip gate** —— **否决**（模型错配下 `"hudi"` provider 不可达；live hms catalog 推到未测 SPI；增量丢失；高回归）。
- **影响范围**：
  - 文档：本条 + [connectors/hudi.md](./connectors/hudi.md)（已加更正注）+ [PROGRESS.md](./PROGRESS.md)（§一 P3 / §二看板 / §四 / §六 / §七 已同步）+ [HANDOFF.md](./HANDOFF.md)（P3 起点）✅；master plan / hudi.md 章节正文待 P3 按选定模型重写。
  - 代码：无（recon only）。
  - 计划：P3 性质从「等依赖」变为「先定模型 + 补 SPI 分流/增量/测试」；可能与 P7(hive/HMS) 部分合并或重排序——待模型决策。
- **关联**：D-005、P1-T04（incrementalRelation gap）、R-001（image 兼容）、P3、master plan §3.4/§3.8
- **后续动作**：
  - [x] P3 session：recon SPI scan/split —— **完成**（verdict：混合 COW-native/MOR-JNI 非问题、与 legacy 结构等价；plumbing 正确；parity gap 见下，详见 HANDOFF 1b）
  - [ ] scan 侧 HIGH 修复（与模型无关、多在 SPI surface 内）：①`HudiScanPlanProvider` override `populateScanLevelParams` 设 current_schema_id+history_schema_info + `HudiScanRange` 设 `THudiFileDesc.schema_id`；②column_types 改发完整 Hive 类型串（弃 `getTypeName()`）+ 停止逗号 join/split（typed list 端到端）；③time-travel 透传 snapshot 否则 fail-loud；④增量读 fail-loud
  - [x] 写 catalog 模型决策备忘（a/b），用户签字 —— **完成**：定 **hybrid**（[D-019](./decisions-log.md)），建 [tasks/P3](./tasks/P3-hudi-migration.md)（批 A 现做 b、批 E 推迟 a）
  - [ ] 选定后：补 `tableFormatType` 分流消费、增量 SPI hook、`listPartitions` override + 真实 applyFilter 裁剪、三模块测试

### DV-004 — T13 用户向安装文档不在本代码仓（在 doris-website 仓）

- **发现日期**：2026-06-04
- **发现 session / agent**：P2 批 C+D+E session
- **当前状态**：🟢 已修正
- **原计划位置**：[tasks/P2 §P2-T13](./tasks/P2-trino-connector-migration.md)：「`docs-next/` 加 trino-connector 插件安装步骤」
- **偏差描述**：原计划假设本代码仓有 `docs-next/`；实际本仓只有 `docs/`，用户向文档（docs-next / i18n）在独立的 doris-website 仓。
- **新方案**：T13 在本 PR 内只同步 plan-doc 跟踪文档；用户向安装文档另在 doris-website 仓提交。
- **影响范围**：文档 — 本仓只更新 plan-doc；website 仓待办。代码/计划 — 无。
- **关联**：P2-T13
- **后续动作**：[ ] 在 doris-website 仓补 trino-connector 插件安装文档

### DV-003 — T12 迁移兼容回归测试：先例与目标目录均不存在，且本地不可运行

- **发现日期**：2026-06-04
- **发现 session / agent**：P2 批 C+D+E session
- **当前状态**：🟡 推迟
- **原计划位置**：[tasks/P2 §P2-T12](./tasks/P2-trino-connector-migration.md)：「类似 P0 的 ES/JDBC migration compat；放入 `regression-test/suites/external_catalog/`」
- **偏差描述**：(1) 不存在「P0 ES/JDBC migration_compat」先例套件；(2) 不存在 `external_catalog/` 目录（实际为 `external_table_p0/` 与 `external_table_p2/`）；(3) 该测试需真实 Trino plugin + 外部数据源 + 运行集群，本开发环境无 docker/集群，无法编写后验证。
- **触发场景**：批 E 启动 T12 时 recon 发现。
- **新方案**：推迟到有 Trino plugin + docker/集群的环境再编写并验证；不往本 PR 加无法验证的套件。
- **替代方案**：盲写 groovy 放 `external_table_p0/trino_connector/` 但本地不可验证——否决（违反"测试要可验证"）。
- **影响范围**：测试 — 迁移 image 兼容回归缺位（现有 trino_connector 功能套件仍在）。代码/计划 — 无。
- **关联**：P2-T12、R-001（image 兼容回归风险）
- **后续动作**：[ ] 集群/CI 环境补 `trino_connector_migration_compat`（CREATE CATALOG→image→重启读回 + 旧 image 含 `TRINO_CONNECTOR` 枚举反序列化）

### DV-002 — T11 单测无法 mock Trino plugin；`TrinoJsonSerializer` 非纯单元

- **发现日期**：2026-06-04
- **发现 session / agent**：P2 批 C+D+E session
- **当前状态**：🟢 已修正（commit `9bba12a44b2`）
- **原计划位置**：[tasks/P2 §P2-T11](./tasks/P2-trino-connector-migration.md)：「最少 4 个 test class（schema / predicate / type-map / json）；mock Trino plugin」
- **偏差描述**：(1) fe-connector-trino 仅依赖 junit-jupiter，无 Mockito；(2) `TrinoJsonSerializer` 构造需 `HandleResolver` + Trino `TypeRegistry`（来自已加载 plugin 的 `TrinoBootstrap`），非纯单元；(3) schema / applyFilter / preCreateValidation 需活的 connector。无 plugin 无法在单测覆盖。
- **触发场景**：T11 启动、读 3 个 SUT 源码时发现。
- **新方案**：写 3 个纯转换器 JUnit5 测试（`TrinoPredicateConverterTest` 14 / `TrinoTypeMappingTest` 11 / `TrinoConnectorProviderTest`=validateProperties 4 = 29 测试），本地 `mvn test` 全绿、不需 plugin；砍掉 json/schema，用 `validateProperties`（批 A T01）替补第 3 类。plugin 依赖路径由现有 `external_table_p0/p2` trino_connector regression 套件覆盖。
- **替代方案**：引 Mockito mock Trino connector 测 pushdown/metadata——否决（偏离 module 现有约定、脆弱、费时）。
- **影响范围**：测试 — 单测覆盖纯转换逻辑；集成路径靠 regression。代码/计划 — 无。
- **关联**：P2-T11、P2-T02
- **后续动作**：（无；plugin 路径覆盖见 T12 follow-up）

### DV-001 — 批 D（删 legacy）范围遗漏 `ExternalCatalog` db 路由与 legacy 测试

- **发现日期**：2026-06-04
- **发现 session / agent**：P2 批 C+D+E session
- **当前状态**：🟢 已修正（commit `ed81a063fe8`）
- **原计划位置**：[tasks/P2 §P2-T08..T10](./tasks/P2-trino-connector-migration.md) / HANDOFF：批 D 只列 T08（translator 分支）+ T09（CatalogFactory case）+ T10（删目录）
- **偏差描述**：recon 发现还有两处引用 legacy 目录、计划未列：(1) `ExternalCatalog.java:948` enum switch `case TRINO_CONNECTOR` 实例化 `TrinoConnectorExternalDatabase`；(2) 测试 `fe-core/.../trinoconnector/TrinoConnectorPredicateTest.java` 测被删的 `TrinoConnectorPredicateConverter`。删目录后两者编译失败。另：原 T10 描述「删 GsonUtils 3 个 class-token 注册」已过时（批 B/T03 已 atomic-replace，T10 不碰 GsonUtils）。
- **触发场景**：批 D 删目录前 `grep datasource.trinoconnector` 全仓 recon。
- **新方案**：(1) `case TRINO_CONNECTOR` 改返 `PluginDrivenExternalDatabase`（照搬已迁移的 JDBC case line 936）+ 删 import；(2) 删该 legacy 测试（新测试见 T11）。**有意保留** `MetastoreProperties.Type.TRINO_CONNECTOR` + `TrinoConnectorPropertiesFactory`（在 `property/metastore/` 子系统，不引用被删目录，SPI 路径可能仍需）。
- **替代方案**：`case TRINO_CONNECTOR` 整删落 default 返 null——否决（JDBC 先例显式返 PluginDrivenExternalDatabase，SPI 需要）。
- **影响范围**：代码 — 已合入批 D commit `ed81a063fe8`。文档 — 本条 + tasks/P2 T10 备注已更正。计划 — 无。
- **关联**：P2-T08、P2-T09、P2-T10
- **后续动作**：[ ] 评估 `MetastoreProperties` trino 条目是否真被 SPI 路径使用（若纯死代码可后续清）

---

## 附录：偏差模板

发现偏差时复制以下模板到 §详细记录 顶部，并更新 §📋 索引表。

```markdown
### DV-NNN — <一句话主题>

- **发现日期**：YYYY-MM-DD
- **发现 session / agent**：（哪次 session 发现的）
- **当前状态**：🟢 已修正 / 🟡 待修正 / 🔴 阻塞中
- **原计划位置**：[文档名 §章节](./xxx.md)，引用原句或代码片段
- **偏差描述**：原计划说 X，实施中发现 Y
- **触发场景**：什么操作 / 什么连接器 / 什么 corner case 引发的
- **新方案**：现在的处理方式
- **替代方案**：考虑过的其他修正
- **影响范围**：
  - 文档：哪些文件需要同步修改（已修改的标 ✅）
  - 代码：哪些已合 PR / 待提 PR
  - 计划：是否影响阶段时长 / 顺序
- **关联**：[task ID]、[PR #]、[decision D-NNN（如果偏差催生了新决策）]
- **后续动作**：
  - [ ] 同步修改文档 X
  - [ ] 提 PR 调整代码 Y
  - [ ] 通知相关 task owner
```

---

## 何时应该写偏差日志（典型场景）

1. RFC 中某 SPI 方法签名在实际实现时发现参数不够 / 太多
2. 原计划某阶段时长估算严重偏差（如 2 周变 4 周）
3. 实施中发现某连接器有未预料的特殊性（如 Iceberg 某 catalog flavor 不支持某操作）
4. 原计划的某 task 拆分粒度太粗 / 太细，重新拆分
5. 原计划假设某个三方库行为 X，实际是 Y
6. 决策（D-NNN）在落地时发现执行不了，需要重新评估
7. 跨连接器假设的一致性被打破（如某 SPI 默认行为对 connector A 合理但对 B 不合理）

## 何时**不**应该写偏差日志

- 普通 bug 修复（写 commit message）
- task 的子步骤微调（在 task 文件里加备注）
- 文档错别字 / 链接错误（直接改）
- 命名重构 / 重命名（直接改）
- 已知的实施细节决策（如选用 `HashMap` vs `LinkedHashMap`）
