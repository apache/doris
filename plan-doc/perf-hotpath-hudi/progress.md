# Progress Log — perf-hotpath-hudi

> append-only：每 session 追加一条（日期 / 做了什么 / 结论 / 下一步）。滚动上下文在 HANDOFF.md。

---

## 2026-07-24 — 立项 + 调研 + 设计 + 红队复核（未动产品代码）

- **做了什么**：
  1. owner 选定旗舰 hudi 为下一个连接器（连接器缓存统一伞形 WS-HUDI）。
  2. 动码前按 HEAD（`f2e9706df5a`）重侦察：13-agent workflow（`wf_3a0b9aca-966`）——9 路并行核对 5 个热点真实倍数/当前行号 + 框架接入缝 + 授权/生命周期 → 综合设计 → 3 路对抗红队。
  3. 自测独立核实 4 个 crux：`ConnectorStatementScopeImpl` 语句末关 AutoCloseable；`HoodieTableMetaClient`(1.0.2) 非 AutoCloseable（memo 活对象 close-pass-safe）；`CachingHmsClient`/fresh/flush API + hive fresh/cached 模板；hudi 三分区入口 + pom 已依赖 hms/api（无 pom 改动）。
  4. 写设计蓝图 `designs/round-1-memo-hms-cache-design.md`（3 块 + 红队 3 修正 + 验证 + 延后清单）。
- **结论（关键）**：
  - **侦察更正**：metaClient build ~5（非 5-6），schema 3×（非 4），过滤查询分区已去重~1（复核成立）。
  - **旗舰 = 每语句"不可变投影" memo**（{instant, 最新 schema, allHistoricalSchemas}），复用现成 `ConnectorStatementScopes.resolveInStatement`（iceberg 同款）→ **0 fe-core**。红队 3 修正：①不 memo Configuration（可变+非线程安全）②planScan 只读 memo schema、保留自己 lastInstant（文档化字节语义）③latest-only，at-instant 延后。~5 build→~2-3、schema 3×→1×、字节不变。
  - **HMS 缓存层 owner 拍板做全套**：wrap `CachingHmsClient` + 按 hive 补 fresh/cached 拆分（SHOW/TVF→`listPartitionNamesFresh`、剪枝/MTMV→cached）+ override `HudiConnector.invalidate*` flush（网关已 forEachBuiltSibling 转发 REFRESH）——否则 hive-sync 表 SHOW PARTITIONS 陈旧最多 24h 且 REFRESH 清不掉（红队 BLOCKER）。
  - **文档清理**：清 stale "dormant hms" 注释一批。
- **下一步**：实施 3 块（序 A→C→B，各独立 commit），动每个文件前按 HEAD 重 grep，守红队 3 修正 + 铁律 A（0 fe-core）+ 无 pom 改动。

---

## 2026-07-24 (2) — Piece A（文档）+ Piece C（HMS 缓存）实现+提交+验证；Piece B checkpoint 交下轮

- **做了什么**：
  - **Piece A**（commit `1cb0f95f8ed`）：4 处 stale "dormant until hms enters SPI_READY_TYPES" 注释改词（HudiConnectorMetadata / HudiScanPlanProvider / HudiConnectorOwnsHandleTest / HudiReadOnlyWriteRejectTest），纯 doc；保留同词异义两处（schema_id "dormant/inert"、"never add hudi"）；hive-side stale 注释留其自身模块 cleanup（不扩 blast radius）。
  - **Piece C**（commit `e26ab33b001`）：`HudiConnector.createClient` 经新 package-private `wrapWithCache` 包 `CachingHmsClient`；`collectPartitions` 加 `bypassCache`——`listPartitionNames`/`listPartitionValues`（SHOW/TVF 展示）走 `listPartitionNamesFresh` 绕缓存、`listPartitions`（剪枝/MTMV）走缓存；override `invalidateTable/Db/All` no-force-build flush（网关 forEachBuiltSibling 已转发 REFRESH 到 sibling）。**无 pom 改动**（hms 已直接依赖 + Caffeine 3.2.3 随 hudi-common）。新 `HudiConnectorHmsCacheTest` 8 测试（wrap / fresh-vs-cached 三入口 / 三 flush 钩子 / unbuilt no-op）。`mvn install -pl :fe-connector-hudi -am` **BUILD SUCCESS，hudi 全模块 183 测试 0 失败**（既有分区列举测试因 default listPartitionNamesFresh→listPartitionNames 仍绿）。
- **Piece B（旗舰）checkpoint 未动**：动码前把它细化到可编码（`designs/round-1-pieceB-flagship-impl-notes.md`），并侦察出关键测试交互坑（`HudiSchemaAtInstantTest` 2-arg control 因改挂会挂须改）。因它触及连接器最易 SIGABRT 的 schema/field-id/演进派生 + 需 session 穿线 + 测试改造，判定在长 session 尾部收尾风险高 → 按 owner"caution over speed"+ checkpoint 纪律交下一轮/新 session。
- **结论/决定**：**Scope B 定案**——planScan 一行不碰（省 build 不受影响、避开读热路径 + 红队 Issue-2）。旗舰 build 收敛 ~5→~3 由元数据侧 3 消费点收敛达成。
- **下一步**：按 `round-1-pieceB-flagship-impl-notes.md` 实现旗舰 memo（新 session 保证预算），动码前按 HEAD 重 grep + 复核 HudiColumnFieldIdTest/HudiSchemaParityTest 是否纯函数不受影响。

---

## 2026-07-24 (3) — Piece B（旗舰 memo, PERF-H03）实现+提交；重侦察推翻蓝图多处、净室复审后按 owner 拍板改为"两块独立 memo"

- **做了什么**：
  - 动码前按 HEAD 重侦察（自读 + 5-agent 净室 workflow），**推翻蓝图多处**：
    - 蓝图称会挂的 `HudiSchemaAtInstantTest` 2-arg 用例在"空会话每次加载"契约下其实不会挂（是否受影响取决于 loader 是否经 `getSchemaFromMetaClient`）。
    - 蓝图列为"元数据侧第 3 个消费点"的 `getScanNodeProperties` 实际在**另一个类** `HudiScanPlanProvider`：用不同存储配置源、**不经** `metaClientExecutor` 鉴权包装、跑在扫描线程 → 并入共享 loader 非字节等价且有重开 auth/TCCL 缝的风险，判定**本轮不碰扫描规划器**。
    - 多处行号/方法归属漂移已校正（分区构建在元数据类而非扫描器；`attachHudiFieldIds`/`avroSchemaToColumns` 在元数据类而非工具类等）。
  - 先按 owner 选的"变体二（schema+instant 合并一次 metaClient 构建）"实现并全绿；**4-agent 净室对抗复审**确认核心等价全部成立，但指出该合并的**两个 minor 固有边角**（取 schema 多依赖 instant 可解析；取快照顺带读 schema + 可能多打一条 WARN）。
  - owner 拍板**退回"两块独立 memo"**（零耦合）：schema 一块、instant 一块，各自 loader = 现成方法，字节等价白送、两个边角消失；代价 = 不再共用那一次构建（正常 SELECT 元数据侧 metaClient 构建 3→2，而非合并版的 →1）。
- **落地**（commit `26690775c81`，4 文件）：
  - `ConnectorStatementScopes`（fe-connector-api，**0 fe-core**）加两常量 `HUDI_LATEST_SCHEMA` / `HUDI_LATEST_INSTANT`（+ javadoc 从 Reserved 移入 Declared）。
  - 新 `HudiStatementScope`（包私有，两 helper `sharedLatestColumns`/`sharedLatestInstant`）；`HudiConnectorMetadata`：`getTableSchema` 最新分支走 `sharedLatestColumns`、`beginQuerySnapshot` 走 `sharedLatestInstant`、`latestInstant` 改包私有（供守门测试覆写）。**`getSchemaFromMetaClient`/`latestInstant` 方法体零改动；`HudiScanPlanProvider` 零改动；at-instant 路径未碰**。
  - 新守门测试 `HudiStatementMemoTest`（4 测试：schema 去重、instant 去重、**两 memo 独立**、NONE/null 每次加载）；`HudiSchemaAtInstantTest`/`HudiConnectorPartitionListingTest` **回到原样**（两块独立 memo 下最新路径仍经 `getSchemaFromMetaClient`，原断言成立）。
  - 验证：`install -pl :fe-connector-api,:fe-connector-hudi -am -Dmaven.build.cache.enabled=false` **BUILD SUCCESS**；hudi 全模块 + api scope 测试 0 失败；`git diff` 对 fe-core 空。
- **结论/收益**：一条语句内重复的"最新 schema"解析（getColumnHandles 2-arg + 3-arg 空 instant MVCC）合成 1 次；重复"最新 instant"合成 1 次；正常 SELECT 元数据侧 metaClient 构建 ~3→2、schema 解析去重；行为字节不变、零耦合。
- **下一步**：round-1 三块（文档 / HMS 缓存 / 旗舰 memo）**全部完成**。转 mc/es 两个小连接器，或 hudi round-2 延后项（跨查询缓存 PERF-H04 等）。e2e 需集群本地未跑（分区/时间旅行/schema 演进 parity 交集群回归）。
