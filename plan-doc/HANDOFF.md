# 🤝 Session Handoff

> 滚动文档：每次 session 结束覆盖更新；历史见 `git log plan-doc/HANDOFF.md`。
> 新 session 必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → [写-SPI RFC](./tasks/designs/connector-write-spi-rfc.md) / [recon](./research/connector-write-spi-recon.md)。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-06（**实现 session**）
- **本 session 主题**：**W-phase W3 + W6 落地**——解耦 3 处热路径 concrete cast/instanceof（`Coordinator`/`LoadProcessor`/`FrontendServiceImpl`）走 SPI 多态 `Transaction` + 共享序列化 helper `CommitDataSerializer` + W6 golden 等价测。**behind gate、零行为变更、golden by TDD（先 RED 后 GREEN）**。守门全绿（真实 exit code 核验：compile BUILD SUCCESS + 9 测全过 + checkstyle 0 + import-gate）。**未提交**（待用户决定）。W4/W5/W7 留下一 session。
- **分支**：`catalog-spi-05`。**⚠️ W1+W2 已提交** = `be945476ba7`（上 handoff 写的"未提交"已**过时**——用户在上 session 之后提交了 W1/W2；故"把 W1–W3 合一个 commit"的旧建议作废，**W3+W6 应为独立 commit**叠在 `be945476ba7` 上）。本 session W3+W6 工作树：3 个 .java 改动 + 3 新 .java + PROGRESS/HANDOFF；未跟踪 `.audit-scratch/`（本场构建日志 `w3-*.log`）/`conf.cmy/`/`regression-conf.bak`（沿用，非本场）。

---

## ✅ 本 session 完成项（behind gate，零行为变更，golden by TDD）

| 项 | 结果 | 文件 |
|---|---|---|
| **W3** 解耦热路径 cast/instanceof | ✅ gated（compile BUILD SUCCESS + checkstyle 0 + import-gate + 三文件无 concrete import/usage） | `qe/Coordinator.java`·`qe/runtime/LoadProcessor.java`·`service/FrontendServiceImpl.java`·**新** `transaction/CommitDataSerializer.java` |
| **W6** golden 等价测 | ✅ 9 测全过（4 golden + 4 SPI default + 1 既有 block-alloc） | **新** `transaction/CommitDataSerializerTest.java`·`connector/fake/ConnectorTransactionDefaultsTest.java` |

**W3 细节**：
- **新 helper** `CommitDataSerializer.feed(Transaction txn, List<? extends TBase<?,?>> fragments)`（`org.apache.doris.transaction` 包）——**序列化协议单点定义** `new TSerializer(new TBinaryProtocol.Factory())`（对齐 W2 各 `addCommitData` 的 `TBinaryProtocol` 反序列化），逐元素 `txn.addCommitData(serializer.serialize(frag))`；fail-loud `catch(TException)→RuntimeException`。
- **Coordinator/LoadProcessor**：3 个独立 `if` + concrete cast → 1 个 **guarded 块**（见下红线）：`if (hive||iceberg||mc) { Transaction txn = …getTxnById(txnId); 对每个 set 字段 CommitDataSerializer.feed(txn, …); }`。删 3 concrete import（HMS/Iceberg/MC），加 `transaction.{CommitDataSerializer,Transaction}`。仍枚举 3 thrift 字段 = RFC §5.3 认可的 transitional shim（B1 消除的是 **cast**，非字段枚举）。
- **FrontendServiceImpl**：`!(transaction instanceof MCTransaction)` → `!transaction.supportsWriteBlockAllocation()`（**保留** legacy `"is not a MaxCompute transaction"` 文案）；`((MCTransaction)transaction).allocateBlockIdRange(…)` → `transaction.allocateWriteBlockRange(…)`（接口声明 `throws UserException`，外层既有 `catch(UserException)`）。删 MCTransaction import。

### 🔴 关键修正（**与上 handoff 字面冲突，务必读**）
`GlobalExternalTransactionInfoMgr.getTxnById(txnId)`（`transaction/GlobalExternalTransactionInfoMgr.java:30`）对未知 txnId **抛 `RuntimeException("Can't find txn for ...")`，不返 null**。legacy 仅在每个 `if (params.isSetXxx())` **内部**调 getTxnById；常规 OLAP load（无 hive/iceberg/mc commit 字段）→ 永不调。**所以 guarded 块必须把 `getTxnById` 包在 `if (任一 commit 字段 set)` 之内**——若像上 handoff 字面"取 `Transaction txn = …getTxnById(txnId)`"那样在 if 前**无条件**取一次，则每个常规 load 的 fragment report 都抛异常、**击穿所有普通导入**。本 session 已用 guarded 块修正（零行为变更）。
> 副作用差异（by design，RFC §5.3 接受）：legacy 对 txn 类型不符抛 ClassCastException（响）；新版多态 `addCommitData` 对非写 txn 是 default no-op（默）。实际不会发生（写连接器只 set 自己的字段），非回归。

**W6 细节 + golden 机制**：
- **TDD**：先写测（引用未存在的 `feed`）→ **故意用错协议 `TCompactProtocol` 实现 helper** → 跑出 **RED**（3 个 `feed` 测报 `RuntimeException: failed to deserialize … / Caused by: TProtocolException: Unrecognized type 24`，证测真守"序列化协议红线"且走真实 `feed→addCommitData` 生产路径）→ 翻 `TBinaryProtocol` → **GREEN**（9/9）。RED/GREEN 日志：`.audit-scratch/w3-red.log` / `w3-green.log`。
- 4 golden（`CommitDataSerializerTest`，JUnit4）：①`binaryProtocolRoundTripIsLossless`（3 型 serialize→deserialize `.equals`，钉协议 + 全字段无损）；②iceberg `feed` vs `updateIcebergCommitData` 比 `getCommitDataList()`；③hms 同比 `getHivePartitionUpdates()`（构造 `new HMSTransaction(null,null,null)` 需 `ConnectContext` 上线程，`@Before`/`@After` 仿 `HMSTransactionPathTest`）；④mc 比 `getUpdateCnt()`（**MC 无 list getter**——全字段保真靠①，**不**加测专用 getter（避反 anti-pattern）、不反射）。`new IcebergTransaction(null)`/`new MCTransaction(null)` 安全（addCommitData/update/getter 不碰 ops/catalog）。
- 4 SPI default（`ConnectorTransactionDefaultsTest`，JUnit5，仿 `FakeConnectorPluginTest`）：SPI `ConnectorTransaction`（fe-connector-api）4 新 default。fe-connector-api **无** test 目录，故测落 fe-core。
- 既有 `FrontendServiceImplTest#testGetMaxComputeBlockIdRange`（1 测）= FrontendServiceImpl 改动的**直接 golden**（first start=0 / second start=1，改后逐位等价）。
- **诚实范围**（Rule 12）：跑的是**定向** golden 测 + compile + checkstyle + import-gate（非全 fe-core 套件）；Coordinator/LoadProcessor 热路径无单测 harness（需集群），由 helper golden 测 + compile 守。

---

## 🚧 未完成 / 待办（下一 session = W4 + W5，再 W7）

### W4 PluginDrivenTransaction 桥（**已存在，扩展非新建**）
- `transaction/PluginDrivenTransactionManager.java:112` 已有 `private static final class PluginDrivenTransaction implements Transaction`（P0-T11）。现继承 4 个新 default（no-op，编译过）。
- W4：override 4 方法委派给 wrap 的 SPI `ConnectorTransaction`（`addCommitData`/`supportsWriteBlockAllocation`/`allocateWriteBlockRange`/`getUpdateCnt`）。注意 fe-core `Transaction.allocateWriteBlockRange` 声明 `throws UserException`，SPI `ConnectorTransaction.allocateWriteBlockRange` **不**声明 checked（抛 `UnsupportedOperationException`）——桥接处适配（catch/wrap 或直接传递，按签名）。

### W5 PluginDrivenTableSink + PhysicalPlanTranslator（写 sink 收口）
- 新 fe-core `PluginDrivenTableSink`；`PhysicalPlanTranslator.visitPhysicalXxxTableSink` → `ConnectorWritePlanProvider.planWrite()`（仿 scan），保 PhysicalXxxSink fallback。
- **此处定 `ConnectorWriteHandle`/`ConnectorSinkPlan` 最终字段形**（据 `*TableSink.bindDataSink()` 真实入参；W1 留的 minimal 形改 free，接口零 implementer）。

### W7 文档
- `decisions-log`：**D-021**(scope=C) + **D-022**(写 SPI A/B1/C1/D/E)——上上 session 用户已签字但**至今未 log**（traceability 缺口，优先补；§5.2「先 log 再改 RFC」）。
- `01-spi-extensions-rfc.md`：加「E11 写/事务 SPI」节（脚注引 D-022）。
- 同步 PROGRESS / connectors/maxcompute / 本 HANDOFF。

---

## ⚠️ 关键认知 / 坑（务必读）

1. **maven 必用绝对 `-f` 路径**：`mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl <m> ...`。Bash cwd 跨调用持久化——勿在命令里 `cd` 子目录（破相对 `-f`，假失败）。
2. **读真实 exit code，非后台通知**：后台 task-notification 的 "exit code 0" 是**末尾 `echo` 的**退出码，**非 maven 的**（本场 RED 真实 `MVN_EXIT=1` 但通知报 "exit code 0"——验证了此坑）。命令尾 `echo "MVN_EXIT=$?" >> log`，再 `grep -E "BUILD SUCCESS|BUILD FAILURE|MVN_EXIT|CS_EXIT|Checkstyle violations"` 日志核。
3. **序列化协议契约（golden 红线）**：W2 反序列化 + W3 序列化均 `TBinaryProtocol`，单点在 `CommitDataSerializer`。改协议即破 round-trip（`CommitDataSerializerTest` 会红，报 `Unrecognized type`）。
4. **getTxnById 抛异常非返 null**（见上 🔴 关键修正）——任何复用 `getTxnById` 的改动都要 guard。
5. **W3 已 live**：W2 override 不再 dead（Coordinator/LoadProcessor 经 `feed`→`addCommitData` 真调；block-alloc 经 `allocateWriteBlockRange` 真调），W6 golden 已护住。
6. **getUpdateCnt 已纯多态**：5 个 executor（Hive/Iceberg×3/MC）`transaction.getUpdateCnt()` 走接口 default + 各 override，无需改。
7. **checkstyle 规则**：`CustomImportOrder` = `org.apache.doris.*`（含 `org.apache.doris.thrift.*`/`transaction.*`，按包名字母序，`thrift`<`transaction`）→ 第三方（`com.*`/`org.apache.thrift.*`/`org.junit.*`，`org.apache.thrift`<`org.junit`）→ `java.*`，组间空行、组内字母序（大小写敏感：`T*`<`protocol.*`）；`UnusedImports`/`RedundantImport` 开；`LineLength` max **120**；无 IllegalThrows/IllegalCatch。
8.（沿用坑）rebase 后 fe-core stale `DorisParser` → clean fe-core；import-gate 只禁 connector→fe-core 单向、只扫 main。
9. **跑 W6 式测**：`mvn -f …/fe/pom.xml -pl fe-core -am -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true -Dtest=类A,类B#方法 -DfailIfNoTests=false test`（`-am` 编上游但 `-Dtest` 过滤使上游跑 0 测；`-Dcheckstyle.skip` 让测跑不被 style 中断，checkstyle 单独 `checkstyle:check` 跑）。慢，后台。

---

## 📂 关键文件锚点

```
RFC：     tasks/designs/connector-write-spi-rfc.md（§5 API / §8 fe-core 改动表 / §12 W1→W7）
recon：   research/connector-write-spi-recon.md · research/p4-maxcompute-migration-recon.md
W1(已提交 be94547)：fe-connector/fe-connector-api/.../connector/api/
            handle/{ConnectorTransaction,ConnectorWriteHandle}.java · Connector.java
            write/{ConnectorWritePlanProvider,ConnectorSinkPlan}.java
W2(已提交 be94547)：fe-core/.../transaction/Transaction.java · datasource/{maxcompute/MCTransaction,
            hive/HMSTransaction, iceberg/IcebergTransaction}.java（各 addCommitData = TBinaryProtocol 反序列化）
W3(本场)：qe/Coordinator.java · qe/runtime/LoadProcessor.java · service/FrontendServiceImpl.java
            + 新 transaction/CommitDataSerializer.java
W6(本场)：fe-core/src/test/.../transaction/CommitDataSerializerTest.java
            + connector/fake/ConnectorTransactionDefaultsTest.java
W4(待改)：transaction/PluginDrivenTransactionManager.java:112（PluginDrivenTransaction 内类）
W5(待改)：新 fe-core PluginDrivenTableSink + PhysicalPlanTranslator.visitPhysicalXxxTableSink
不动：     CatalogFactory.SPI_READY_TYPES / datasource/{maxcompute,hive,iceberg} legacy / RewriteDataFileExecutor(P6)
守门命令： # 编译+测（慢，后台，绝对 -f）
          mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am \
            -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true \
            -Dtest=CommitDataSerializerTest,ConnectorTransactionDefaultsTest -DfailIfNoTests=false test
          # checkstyle（含 test 源）
          mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core \
            -Dmaven.build.cache.enabled=false checkstyle:check
          bash tools/check-connector-imports.sh                           # 从 repo 根跑
```

---

## 🧠 给下一个 agent 的 meta 建议

- **W4/W5 是 SPI 写面的真正接线**（W3 只解耦了 legacy 路径的 cast；plugin-driven 写路径才是翻闸前提）。W5 落 `ConnectorWriteHandle`/`ConnectorSinkPlan` 最终字段形——先读 `*TableSink.bindDataSink()` 真实入参再定（Rule 8）。
- **守门循环**：compile（慢，后台）+ checkstyle（绝对 -f）+ import-gate；**读真实 BUILD/MVN_EXIT/CS_EXIT 行**，勿信后台 "exit code" 通知（坑 2）。
- **别越界**：W-phase 不翻闸（`SPI_READY_TYPES` 不动）/不搬连接器类/不删 legacy——那是 P4 maxcompute adopter 阶段（W-phase 之后）。RFC §12 已分清。
- **提交**：W1+W2 已是 `be945476ba7`；**W3+W6 应独立 commit**（behind-gate、零行为变更），title 形如 `[feat](connector) W-phase W3+W6 解耦热路径 cast/instanceof + golden 测`。由用户决定时机/粒度。
- **决策待补**：D-021(scope=C) + D-022(写 SPI 设计) 仍未入 decisions-log（W7 补；§5.2「先 log 再改 01-spi-rfc」）。
- Maven：cwd 无关（用绝对 `-f`）；`-pl <module> -am`；`-Dmaven.build.cache.enabled=false`；测试 `-DfailIfNoTests=false`；checkstyle 单独跑。
