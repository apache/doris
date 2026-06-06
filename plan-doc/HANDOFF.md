# 🤝 Session Handoff

> 滚动文档：每次 session 结束覆盖更新；历史见 `git log plan-doc/HANDOFF.md`。
> 新 session 必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → [写-SPI RFC](./tasks/designs/connector-write-spi-rfc.md) / [recon](./research/connector-write-spi-recon.md)。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-06（**实现 session**）
- **本 session 主题**：**W-phase W1+W2 落地**——写/事务 SPI 面（fe-connector-api）+ fe-core `Transaction` 泛化 + MC/HMS/Iceberg 三 txn override。**behind gate、零行为变更、golden 等价 by construction**。守门全绿（真实 exit code 核验）。**未提交**（待用户决定）。W3–W7 留下一 session。
- **分支**：`catalog-spi-05`（基于 `branch-catalog-spi`，含 P0–P3 #64143 `5c240dc7a34`）。工作树：6 个 .java 改动（fe-connector-api×2 + fe-core×4）+ PROGRESS/HANDOFF；未跟踪 `.audit-scratch/`/`conf.cmy/`/`regression-conf.bak`（沿用，非本场）。

---

## ✅ 本 session 完成项（behind gate，零行为变更）

| 项 | 结果 | 文件 |
|---|---|---|
| **W1** SPI 写/事务面 | ✅ gated（compile + import-gate + checkstyle 0） | `fe-connector-api`：`handle/ConnectorTransaction.java`(+4 default)·`Connector.java`(+getWritePlanProvider)·新 `write/ConnectorWritePlanProvider.java`·`write/ConnectorSinkPlan.java`(包 TDataSink)·`handle/ConnectorWriteHandle.java` |
| **W2** fe-core Transaction 泛化 + 3 override | ✅ gated（fe-core compile BUILD SUCCESS + checkstyle 0） | `transaction/Transaction.java`(+4 default)·`maxcompute/MCTransaction.java`·`hive/HMSTransaction.java`·`iceberg/IcebergTransaction.java` |

**W1 细节**：4 个 SPI default = `addCommitData(byte[])` no-op / `supportsWriteBlockAllocation()` false / `allocateWriteBlockRange(String,long)` throws / `getUpdateCnt()` 0。`ConnectorWriteHandle` 字段 minimal（`getTableHandle`/`getColumns`/`isOverwrite`/`getWriteContext`），**W5 据 `bindDataSink` 真实入参细化**（接口零 implementer，改 free）。`ConnectorSinkPlan` 已 pin（仅包 `TDataSink`）。

**W2 细节 + golden 等价机制**：
- `Transaction` 接口加 4 同名 default；`allocateWriteBlockRange(String,long) throws UserException`（对齐 MC `allocateBlockIdRange` 的 checked 异常 + `FrontendServiceImpl` 既有 `catch(UserException)`）。
- 3 个 `addCommitData(byte[])` override：`new TDeserializer(new TBinaryProtocol.Factory()).deserialize(typed, bytes)` → 走**既有** `updateXxxCommitData(Collections.singletonList(typed))`。既有方法 = `internalList.addAll(arg)`，故 `addAll(整 list)` ≡ 逐元素 `add` → **逐位等价**。MC 另 override `supportsWriteBlockAllocation()=true` + `allocateWriteBlockRange→allocateBlockIdRange`；3 处 `getUpdateCnt` 加 `@Override`。
- **⚠️ W2 override 现为 dead code**：W3 接线前 `Coordinator`/`LoadProcessor` 仍 concrete cast 直调 `updateXxxCommitData` → `addCommitData` 不被调用 → **本 session 零行为变更**，亦无 round-trip 故无协议失配风险（风险在 W3 落地时）。

---

## 🚧 未完成 / 待办（下一 session 第一件事 = W3 + W6）

### W3 解耦热路径（**golden 等价红线**，behind gate）— 精确锚点（本 session 核实）

- `qe/Coordinator.java:2530-2541`：3 个**独立 if**
  ```
  if (params.isSetHivePartitionUpdates()) ((HMSTransaction)   …getTxnById(txnId)).updateHivePartitionUpdates(params.getHivePartitionUpdates());
  if (params.isSetIcebergCommitDatas())   ((IcebergTransaction)…getTxnById(txnId)).updateIcebergCommitData(params.getIcebergCommitDatas());
  if (params.isSetMcCommitDatas())        ((MCTransaction)    …getTxnById(txnId)).updateMCCommitData(params.getMcCommitDatas());
  ```
- `qe/runtime/LoadProcessor.java:231-242`：同结构（`long txnId = loadContext.getTransactionId();`）。
- `service/FrontendServiceImpl.java:3697-3703`：`if (!(transaction instanceof MCTransaction)) throw new UserException(...); long start = ((MCTransaction) transaction).allocateBlockIdRange(request.getWriteSessionId(), request.getLength());`（`transaction` 已是 fe-core `Transaction`，见 :3695-3696；外层已 `catch(UserException)`）。

**改法**：
1. **Coordinator / LoadProcessor**：取 `Transaction txn = …getTxnById(txnId);`，对每个 set 的字段**逐元素** `txn.addCommitData(serializer.serialize(elem))`。**序列化协议必须 `new TSerializer(new TBinaryProtocol.Factory())`**（对齐 W2 反序列化）。fail-loud：`catch (TException) → throw new RuntimeException(...)`。删 3 个 cast + 删 `import …{HMSTransaction, IcebergTransaction, MCTransaction}`。
   - 仍枚举 3 个 thrift 字段 = RFC §5.3 认可的 transitional serialization shim（W3 目标 = 消除 concrete **cast**，非字段枚举；后者待 BE 加通用 `connector_commit_data` 字段后退休）。
   - **建议**：抽一个共享 static helper（serialize-list → `txn.addCommitData`），避免 Coordinator/LoadProcessor 重复且**协议单点定义**（保 golden）。位置自定（`org.apache.doris.transaction` 下小 util，或 `Transaction` 的 static）。
2. **FrontendServiceImpl**：`if (!transaction.supportsWriteBlockAllocation()) throw new UserException(... "is not a MaxCompute transaction"); long start = transaction.allocateWriteBlockRange(request.getWriteSessionId(), request.getLength());`。删 `import …MCTransaction`。
3. 守门：fe-core compile + checkstyle（**绝对 -f**，见坑 1）；验三文件无 concrete import：`grep -nE "import .*(MCTransaction|HMSTransaction|IcebergTransaction)" Coordinator.java LoadProcessor.java FrontendServiceImpl.java` 应空。

### W6 golden 测（与 W3 同验）
- `FakeConnector` 写 default 行为测。
- 3 txn golden 等价：构造 typed（`TMCCommitData`/`THivePartitionUpdate`/`TIcebergCommitData`）→ `TSerializer(TBinaryProtocol)` → `addCommitData(bytes)` → `getCommitDataList()`/`getHivePartitionUpdates()` **==** `updateXxxCommitData([typed])` 路径。checkstyle 含 test 源。
- 跑：`mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -Dtest=Xxx -DfailIfNoTests=false test`（慢，后台）。

### W4 PluginDrivenTransaction 桥（**已存在，扩展非新建**）
- `transaction/PluginDrivenTransactionManager.java:112` 已有 `private static final class PluginDrivenTransaction implements Transaction`（P0-T11）。现继承 4 个新 default（no-op，编译过）。
- W4：override 4 方法委派给 wrap 的 SPI `ConnectorTransaction`（`addCommitData`/`supportsWriteBlockAllocation`/`allocateWriteBlockRange`/`getUpdateCnt`）。

### W5 PluginDrivenTableSink + PhysicalPlanTranslator（写 sink 收口）
- 新 fe-core `PluginDrivenTableSink`；`PhysicalPlanTranslator.visitPhysicalXxxTableSink` → `ConnectorWritePlanProvider.planWrite()`（仿 scan），保 PhysicalXxxSink fallback。
- **此处定 `ConnectorWriteHandle`/`ConnectorSinkPlan` 最终字段形**（据 `*TableSink.bindDataSink()` 真实入参）。

### W7 文档
- `decisions-log`：**D-021**(scope=C) + **D-022**(写 SPI A/B1/C1/D/E)——上 session 用户已签字但**至今未 log**（traceability 缺口，优先补）。
- `01-spi-extensions-rfc.md`：加「E11 写/事务 SPI」节（脚注引 D-022；§5.2 纪律「先 log 再改 RFC」）。
- 同步 PROGRESS / connectors/maxcompute / 本 HANDOFF。

---

## ⚠️ 关键认知 / 坑（务必读）

1. **maven 必用绝对 `-f` 路径**：`mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl <m> -am -Dmaven.build.cache.enabled=false ...`。Bash cwd 跨调用持久化——一旦某命令 `cd` 进子目录，后续相对 `-f fe/pom.xml` 即 "does not exist" 假失败（本 session 因此踩 3 次）。**勿在命令里 `cd` 子目录**。
2. **读真实 exit code，非后台通知**：后台 `mvn … | tail; echo MVN_EXIT=$?` 的 task-notification "exit code 0" 是**整条 pipeline（末尾 echo）**的退出码，**非 maven 的**。必须 `grep -E "BUILD SUCCESS|BUILD FAILURE|MVN_EXIT|Checkstyle violations|CS_EXIT"` 输出文件确认。
3. **序列化协议契约（golden 红线）**：W2 反序列化用 `new TDeserializer(new TBinaryProtocol.Factory())`；W3 序列化**必须**同协议 `TBinaryProtocol`。建议 W3 抽共享 helper 单点定义协议。
4. **W3 仍枚举 3 thrift 字段**：B1 消除的是 concrete `*Transaction` **cast**，非字段枚举（后者是 RFC §5.3 认可的 transitional shim）。
5. **W2 override 现 dead**：W3 接线后才 live；W3 落地即需 W6 golden 测护住。
6. **getUpdateCnt 已半多态**：5 个 executor（Hive/Iceberg×3/MC `*InsertExecutor` 等）已 `transaction.getUpdateCnt()`；W2 加接口 default 后可纯多态，无需改它们（编译前后均过）。
7. **checkstyle 规则**：`MissingOverride`（default 模式，仅 `{@inheritDoc}` 强制 @Override）；`CustomImportOrder` = `org.apache.doris.*` → 第三方(`com.*`/`io.*`/`org.apache.hadoop.*`/`org.apache.thrift.*`...) → `java.*`，**组间空行、组内字母序**；`UnusedImports`/`RedundantImport` 开；**无** IllegalThrows/IllegalCatch（`throw new RuntimeException` 可用）。注意 `org.apache.doris.thrift.*` 属 doris 组，`org.apache.thrift.*` 属第三方组。
8.（沿用坑）rebase 后 fe-core stale `DorisParser` → clean fe-core；import-gate 只禁 connector→fe-core 单向、只扫 main。

---

## 📂 关键文件锚点

```
RFC：     tasks/designs/connector-write-spi-rfc.md（§5 API / §8 fe-core 改动表 / §12 W1→W7）
recon：   research/connector-write-spi-recon.md（§4 对比矩阵 / §5 现存 SPI / §6 leak）
          research/p4-maxcompute-migration-recon.md（P4 adopter：翻闸/gson §5、反向引用 §3）
W1(已改)：fe-connector/fe-connector-api/.../connector/api/
            handle/ConnectorTransaction.java · Connector.java · write/ConnectorWritePlanProvider.java
            write/ConnectorSinkPlan.java · handle/ConnectorWriteHandle.java
W2(已改)：fe-core/.../transaction/Transaction.java · datasource/{maxcompute/MCTransaction,
            hive/HMSTransaction, iceberg/IcebergTransaction}.java
W3(待改)：qe/Coordinator.java:2530-2541 · qe/runtime/LoadProcessor.java:231-242 ·
            service/FrontendServiceImpl.java:3697-3703
W4(待改)：transaction/PluginDrivenTransactionManager.java:112（PluginDrivenTransaction 内类）
不动：     CatalogFactory.SPI_READY_TYPES / datasource/{maxcompute,hive,iceberg} legacy /
            RewriteDataFileExecutor(procedure, P6)
守门命令： mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am \
            -DskipTests -Dmaven.build.cache.enabled=false compile        # 慢，后台
          mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core \
            -Dmaven.build.cache.enabled=false checkstyle:check            # 含 test 源
          bash tools/check-connector-imports.sh                           # 从 repo 根跑
```

---

## 🧠 给下一个 agent 的 meta 建议

- **W3 是 golden 红线**（Rule 9）：建议先写 W6 golden 测（或同步），改一处验一处，逐位对齐 legacy。
- **守门循环**：fe-core compile（慢，后台）+ checkstyle（绝对 -f）；**读真实 BUILD/exit 行**，勿信后台 "exit code" 通知（坑 2）。
- **别越界**：W-phase 不翻闸（`SPI_READY_TYPES` 不动）/不搬连接器类/不删 legacy——那是 P4 maxcompute adopter 阶段（W-phase 之后）。RFC §12 已分清 W-phase / P4 / P6-P7。
- **提交**：W1+W2 已 gated 但未提交，由用户决定时机/粒度（建议把 W1–W3+W6 合成一个 behind-gate、零行为变更的 commit，title 形如 `[feat](connector) W-phase 写/事务 SPI 解耦 (W1-W6)`）。
- **决策待补**：D-021(scope=C) + D-022(写 SPI 设计) 上 session 已签字但仍未入 decisions-log（W7 补；§5.2「先 log 再改 01-spi-rfc」）。
- Maven：cwd 无关（用绝对 `-f`）；`-pl <module> -am`；`-Dmaven.build.cache.enabled=false`；测试 `-DfailIfNoTests=false`；checkstyle 单独跑。
