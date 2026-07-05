# 交接：fe-connector 层 pre-existing UT 失败清单（2026-07-03）

## ✅ 已解决（2026-07-03，同日）

全部 **6 个失败已修复并关缓存全量复验通过**（两模块所有测试 0 fail / 0 error，checkstyle 0 违规，BUILD SUCCESS）。

- **A（1 个，改测试）** `IcebergMetaStoreProvidersDispatchTest`：把陈旧的 `hadoopAndS3TablesAreNoOpValidate`
  重写为 `hadoopValidatesWarehouseAndS3TablesIsNoOp` —— 断言 hadoop **缺 warehouse 抛**（对齐 `935e4fb9d80`
  恢复的 legacy 校验）/ **给 warehouse 过**，s3tables 保持 no-op。比原"塞个 warehouse 绕过"更强：能在校验被误删时变红（Rule 9）。
- **B（3 个，改测试）** `IcebergScanPlanProviderTest` 的 3 个 manifest-cache 用例：`planScan(null,…)` 的
  manifest 侧调用改传模块已有的 `emptySession()`（`getQueryId()` 返回 "q"）。SDK 侧 `null` 不解引用 session，保持不动（surgical）。
- **C（2 个，改生产 — ⚠️ 根因与本文档原猜测不同）** `IcebergPredicateConverterTest`：**不是时区问题**。
  失败格是 `c_ts(timestamp) × literal#11 = VARCHAR "2023-01-02"`，`converter()` 本就 pin 了 `ZoneOffset.UTC`。
  真根因：共享 `extractIcebergLiteral` 的 `String→case TIMESTAMP` 自解析分支（`parseDorisDateTime→toMicros`，
  commit `8b4eefcd349` 为 rewrite 加）**对所有 mode 生效**，SCAN 下把裸 VARCHAR 越权下推到 timestamp 列 —— 而
  legacy `IcebergUtils.extractDorisLiteral`（scan oracle）对 String→TIMESTAMP **返回裸串**，交给 iceberg bind，
  bind 拒绝 date-only/空格分隔串 → drop → false。修法：自解析**仅限非 SCAN**（`mode == Mode.SCAN` 返回裸串对齐
  legacy scan；REWRITE/CONFLICT 保留自解析，二者均委派 `IcebergNereidsUtils`，实测 rewrite/conflict mode UT 全绿未回归）。

> fe-core UT 仍未覆盖（本文档只含 fe-connector 层）。如需 fe-core 失败清单要另跑一轮。

---

## 背景 / 怎么发现的

在排查 TeamCity external regression（build 984925，PR #64689）时，为了验证本会话的连接器修复，
对**全部 17 个 `fe-connector/*` 模块跑了一轮全量 UT**。关键点：

- **Doris 的 maven build-cache 会按 checksum 恢复模块、跳过测试执行**，长期**掩盖**了这些失败
  （日常 `mvn test` 命中缓存 → 报 SUCCESS 但没真跑）。
- 必须**关缓存**才会暴露。且 `test` 阶段早于 `package`，`*-hive-shade` 的 shaded jar 只在
  `package` 阶段生成，所以要用 `package` 才能编译过 paimon/iceberg。

**复现命令**（关缓存 + 全量 + 不 fail-fast）：

```bash
cd /mnt/disk1/yy/git/wt-catalog-spi
mvn -o -f fe/pom.xml \
  -pl fe-connector/fe-connector-api,fe-connector/fe-connector-spi,fe-connector/fe-connector-cache,\
fe-connector/fe-connector-metastore-api,fe-connector/fe-connector-metastore-spi,\
fe-connector/fe-connector-metastore-iceberg,fe-connector/fe-connector-metastore-paimon,\
fe-connector/fe-connector-iceberg,fe-connector/fe-connector-paimon,fe-connector/fe-connector-paimon-hive-shade,\
fe-connector/fe-connector-hms,fe-connector/fe-connector-hive,fe-connector/fe-connector-hudi,\
fe-connector/fe-connector-es,fe-connector/fe-connector-jdbc,fe-connector/fe-connector-trino,\
fe-connector/fe-connector-maxcompute -am \
  -Dmaven.build.cache.enabled=false -Dmaven.test.failure.ignore=true \
  package
```
> ⚠️ 后台跑时**不要信 task 通知的 "exit 0"**——那是 echo 的、不是 maven 的。读日志里的
> `BUILD SUCCESS/FAILURE` 行 + 盯 maven PID（`tail --pid=<pid> -f /dev/null`）才是真结束。
> 单个失败测试可复现：`-Dtest='<ClassName>' -DfailIfNoTests=false`（仍需 `package` + 关缓存）。

## 结论

- **只有 iceberg 相关的 2 个模块失败**（`fe-connector-metastore-iceberg`、`fe-connector-iceberg`），
  共 **6 个失败测试**。其余 15 个模块（paimon/hms/hive/hudi/es/jdbc/trino/maxcompute/cache/api/spi/
  metastore-{api,spi,paimon}/paimon-hive-shade）**全绿**。
- 这 6 个**全部是 pre-existing**，与本会话的 4 个提交（`1e3a5fd` #1、`361d2dc` #2、`b9381ad`/`950a0cc`
  #3）**无关**——已用 `git stash` 掉 #3 后同样条件重跑，6 个失败**逐行完全一致**；#1/#2 也不碰这些文件。
- **未覆盖 `fe-core`**（体量大、另一条路径）。如需 fe-core UT 失败清单要另跑一轮。

---

## 明细（含 file:line 与修法）

### A. `fe-connector-metastore-iceberg` — 1 个【确定性·真 bug】

**`IcebergMetaStoreProvidersDispatchTest.hadoopAndS3TablesAreNoOpValidate`**
- 测试：`fe/fe-connector/fe-connector-metastore-iceberg/src/test/java/org/apache/doris/connector/metastore/iceberg/IcebergMetaStoreProvidersDispatchTest.java:71`
  ```java
  bind("hadoop").validate();   // 期望不抛；实际抛 IllegalArgumentException
  ```
- 生产：`.../metastore/iceberg/noop/IcebergNoOpMetaStoreProperties.java:55-63` —— `validate()` 对
  `providerName=="HADOOP"` 且 warehouse 为空时**抛异常**：
  `"Cannot initialize Iceberg HadoopCatalog because 'warehouse' must not be null or empty"`。
- 根因：commit `935e4fb9d80`（[fix](catalog) P6.6 M-1 恢复 hadoop iceberg warehouse 必填校验）
  加回了该校验，但**旧测试仍断言 hadoop 的 `validate()` 是 no-op（不抛）**——代码/测试冲突。
  测试方法名/注释（"The no-op backends exist only so bindForType resolves; validate() must not throw."）
  已过时。
- 修法（二选一，倾向前者）：
  1. **改测试**：给 hadoop case 传一个 warehouse（如 `'warehouse'='hdfs://x/wh'`）再 `.validate()`；
     或把 hadoop 拆出单独断言"缺 warehouse 抛、有 warehouse 过"，s3tables 保持 no-op 断言。
  2. 若认为 hadoop 不该在此层校验 → 撤 `935e4fb9d80` 的校验（**不推荐**，那是有意恢复的 legacy 行为）。

### B. `fe-connector-iceberg` — 3 个【确定性·真 bug（测试传 null session）】

**`IcebergScanPlanProviderTest.planScanManifestCacheEnabledMatchesSdkPathAndConsumesCache:1584`**
**`IcebergScanPlanProviderTest.planScanManifestCachePrunesPartitionLikeSdk:1610`**
**`IcebergScanPlanProviderTest.planScanManifestCacheAssociatesDeletesLikeSdk:1674`**
- 异常：`NullPointerException: Cannot invoke "...ConnectorSession.getQueryId()" because "session" is null`
- 测试：`.../fe-connector-iceberg/src/test/.../IcebergScanPlanProviderTest.java`（上述 3 行附近）
  都用 `manifestProvider(...).planScan(null, handle, ...)` —— **session 传 null**。
- 生产：`.../fe-connector-iceberg/src/main/.../IcebergScanPlanProvider.java` 的 **manifest-cache 路径**
  会解引用 session：`:1091`（`props.put(MANIFEST_CACHE_QUERYID_PROP, session.getQueryId())`）、
  `:1283`（`manifestCache.recordFailure(session.getQueryId())`）、`:1306`（`session.getQueryId()`）。
- 根因：该测试类里有 10 处 `planScan(null, ...)`，但**只有开启 manifest-cache 的这 3 个**会走到
  `session.getQueryId()` 分支 → NPE；其余非 cache 路径不碰 session 所以过。属**测试 bug**：
  manifest-cache 用例必须传非 null session。
- 修法：给这 3 个用例传一个 stub/mock `ConnectorSession`，其 `getQueryId()` 返回一个固定串
  （如 `"test-query-id"`）。参考同模块已有的 ConnectorSession 构造/stub 方式（搜 `ConnectorSession` 的
  其它测试用法；无 Mockito，可手写一个只覆写 `getQueryId()` 的匿名/内部类）。
  （备选：生产代码对 `session==null` 兜底——**不推荐**，会掩盖"cache 路径必须有 queryId"的契约。）

### C. `fe-connector-iceberg` — 2 个【先判真伪：疑似时区敏感】

**`IcebergPredicateConverterTest.binaryEqGridMatchesLegacy:130`**
**`IcebergPredicateConverterTest.inAndNotInGridMatchLegacy:147`**
- 断言失败：`EQ/IN grid mismatch at column c_ts literal#11 (2023-01-02) ==> expected: <false> but was: <true>`
- 测试：`.../fe-connector-iceberg/src/test/.../IcebergPredicateConverterTest.java`
  - 列定义 `:66`：`c_ts = Types.TimestampType.withoutZone()`（**TIMESTAMP WITHOUT ZONE**）。
  - literal `:77`：`ConnectorLiteral(ConnectorType.of("DATETIMEV2"), ...)`。
  - 用例对每个 (列 × literal) 生成一个"是否匹配"网格，和 legacy 期望网格逐格对比。
- 根因（待确认）：只有 **c_ts（timestamp）** 这列错，`expected false 实际 true` —— 新转换器把
  DATETIMEV2 literal 转成 iceberg timestamp 的 **epoch micros 值**与 legacy 不一致，导致某些格的匹配
  结果翻转。**强烈怀疑是本地时区把 `withoutZone` 的 datetime 当成本地时刻转 micros**（withoutZone
  本不该套 TZ）。poms 里**没找到** `-Duser.timezone` 固定，用的是运行机默认 TZ。
- 下个 session 先做**真伪判定**：
  ```bash
  # 本地用 UTC 复跑这两个用例，若变绿 => 时区特异（CI 若 UTC 则本不失败）
  TZ=UTC mvn -o -f fe/pom.xml -pl fe-connector/fe-connector-iceberg -am \
    -Dmaven.build.cache.enabled=false -Dmaven.user.timezone=UTC \
    -Dtest='IcebergPredicateConverterTest' -DfailIfNoTests=false package
  # 或在 surefire argLine 里加 -Duser.timezone=UTC
  ```
- 修法（视判定结果）：
  - 若确为时区特异且 CI 在 UTC 下通过 → 要么给该测试/surefire **pin `-Duser.timezone=UTC`**（对齐 CI），
    要么让 `IcebergPredicateConverter` 对 `TimestampType.withoutZone()` **不套本地 TZ**（用 UTC/无偏移）
    转 micros，与 legacy 一致。
  - 若 UTC 下仍错 → 是真的 new-vs-legacy 转换差异，需对齐 `IcebergPredicateConverter` 的
    timestamp-without-zone → micros 逻辑到 legacy。

---

## 建议优先级

1. **确定性、易修**（4 个）：B 组 3 个 NPE（补非 null session）+ A 组 1 个（测试给 warehouse / 拆断言）。
2. **先判真伪再动**（2 个）：C 组 `IcebergPredicateConverterTest`（先 `TZ=UTC` 复跑定性）。

## 注意事项

- 改完用**关缓存**的方式复验（否则 build-cache 会再次掩盖）。
- 这些属 **FE UT 流水线**，与触发本次排查的 **external regression（docker）** 是两条线。
- 本会话的 4 个已提交修复（#1/#2/#3 iceberg+paimon）**不引入任何新失败**，可独立推进。
