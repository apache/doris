# [P4-T06e] FIX-BLOCKID-CAP-CONFIG (CRITICGAP1) — design

> 来源：Batch-D 红线扩充对抗复审 workflow `wbw4xszrg`（CRITICGAP1，Tier 2，minor，写路径）。
> 关联：legacy `MCTransaction.allocateBlockIdRange:165`（读可调 `Config.max_compute_write_max_block_count`）；`Config.java:2156`（`= 20000L`，fe.conf 可调）；既有偏差 `deviations-log.md` **DV-011**（P4-T03 把上限硬编为连接器常量、自承「丢 fe.conf 可调性，如需再经透传暴露」）。
> 用户定夺（2026-06-08）：**Option A — 全局 Config 透传**（true legacy parity，反转 DV-011 的 Rule-2 推迟决定）。

## Problem

翻闸后，写 block-id 分配上限**硬编**为连接器常量 `MAX_BLOCK_COUNT = 20000L`
（`MaxComputeConnectorTransaction.java:72`，用于 `:146` 的越限校验），无视 legacy
`MCTransaction.allocateBlockIdRange:165` 读取的**可调** `Config.max_compute_write_max_block_count`
（`Config.java:2156`，fe.conf 可调、默认 20000）。

后果：**调优部署静默回归**。管理员若在 fe.conf 把 `max_compute_write_max_block_count` 调离默认值：
- 调高（如 50000，为大写入放宽）→ 连接器仍在 20000 处拒绝 → legacy 能成功的大写入在翻闸后失败。
- 调低（如 10000，为限流）→ 连接器仍允许到 20000 → 比管理员意图更宽松。

20000 = 默认值，故仅**改过 fe.conf 的部署**受影响（窄但真实的 parity 回归）。

## Root Cause（已核码确认）

| # | 位置 | 现状 | legacy parity |
|---|---|---|---|
| 1 | `MaxComputeConnectorTransaction.java:72` | `private static final long MAX_BLOCK_COUNT = 20000L;`（硬编、用于 `:146`） | legacy `MCTransaction:165` 读 `Config.max_compute_write_max_block_count`（可调） |
| 2 | 连接器 import-gate | 禁 `org.apache.doris.common.Config` → 无法直接读 fe Config | legacy 在 fe-core、可直接 import `Config` |

**核心约束**：连接器禁 import fe-core（含 `Config`），故不能像 legacy 那样直接读。须经**透传通道**把 FE 全局 Config 值送到连接器。
`max_compute_write_max_block_count` 是 **FE 全局 Config**（`Config.java:2156`），**非** SessionVariable（`SessionVariable.java` 无此名）、**非** catalog property。

**为何 CI 没抓**：`MaxComputeConnectorTransaction` 当前**无任何单测**；cap 行为从未被 pin；DV-011 把硬编登记为「已接受偏差」。

## 透传通道调研（已核码）

`ConnectorSession` 三通道：`getSessionProperties()`（=session 变量，`VariableMgr.toMap`）/ `getCatalogProperties()`（=CREATE CATALOG 属性）/ `getProperty()`。**三者皆不天然携带 FE 全局 Config。**

**但有直接先例**：`ConnectorSessionBuilder.extractSessionProperties:115-120`（fe-core，可 import `Config`）已把一个**非-session-变量的 server 全局** `GlobalVariable.lowerCaseTableNames` 显式 `props.put` 进 session-properties map：
```java
Map<String, String> props = VariableMgr.toMap(ctx.getSessionVariable());
props.put("lower_case_table_names", String.valueOf(GlobalVariable.lowerCaseTableNames));  // ← 先例
return props;
```
连接器读 session 变量的既有约定见 P3-9（`MaxComputeScanPlanProvider` 的 `ENABLE_MC_LIMIT_SPLIT_OPTIMIZATION`：连接器内重复字面 key 常量 + 注「禁依赖 fe-core 常量、须 byte-identical」+ map-typed 可测 parse 方法）。

事务构造点 `MaxComputeConnectorMetadata.beginTransaction:357-359` **持有 `session`**（唯一 `new MaxComputeConnectorTransaction` 处），故连接器可在此读注入值并传入 ctor。

## Design（Option A：全局 Config 透传，true parity）

**Shape：fe-core 1 行注入（镜像 lower_case_table_names）+ 连接器 ctor 透传。无 SPI 签名变更。import-gate 净（连接器不 import Config，只读 session map）。**

### 改 1（fe-core）：`ConnectorSessionBuilder.java`

- 加 `import org.apache.doris.common.Config;`
- `extractSessionProperties` 在 `lower_case_table_names` 之后加（逐字镜像该先例）：
  ```java
  // MaxCompute write block-id cap: the connector cannot import fe-core Config, so the tunable
  // Config.max_compute_write_max_block_count is surfaced through this channel (same as
  // lower_case_table_names) and read back via ConnectorSession.getSessionProperties().
  // Key must stay byte-identical to MaxComputeConnectorMetadata.MAX_COMPUTE_WRITE_MAX_BLOCK_COUNT.
  props.put("max_compute_write_max_block_count",
          String.valueOf(Config.max_compute_write_max_block_count));
  ```
  注入值恒为合法 long（Config 字段是 `long`）。

### 改 2（连接器）：`MaxComputeConnectorTransaction.java`

- `MAX_BLOCK_COUNT` 常量 → 实例字段 + 默认常量：
  ```java
  /** Legacy default of Config.max_compute_write_max_block_count; fallback when the
   *  session does not carry the (tunable) value. */
  static final long DEFAULT_MAX_BLOCK_COUNT = 20000L;

  private final long maxBlockCount;
  ```
- ctor 加 `long maxBlockCount` 参（唯一 caller = beginTransaction）：
  ```java
  public MaxComputeConnectorTransaction(long transactionId, long maxBlockCount) {
      this.transactionId = transactionId;
      this.maxBlockCount = maxBlockCount;
  }
  ```
- `:146` 越限校验 `MAX_BLOCK_COUNT` → `maxBlockCount`（含异常 message）。

### 改 3（连接器）：`MaxComputeConnectorMetadata.java`

- 加 key 常量（byte-identical to fe-core，注同 P3-9）+ map-typed 可测 resolve：
  ```java
  // Must stay byte-identical to the key ConnectorSessionBuilder.extractSessionProperties injects.
  private static final String MAX_COMPUTE_WRITE_MAX_BLOCK_COUNT = "max_compute_write_max_block_count";

  static long resolveMaxBlockCount(Map<String, String> sessionProperties) {
      String v = sessionProperties.get(MAX_COMPUTE_WRITE_MAX_BLOCK_COUNT);
      if (v == null) {
          return MaxComputeConnectorTransaction.DEFAULT_MAX_BLOCK_COUNT;
      }
      try {
          return Long.parseLong(v.trim());
      } catch (NumberFormatException e) {
          return MaxComputeConnectorTransaction.DEFAULT_MAX_BLOCK_COUNT;
      }
  }
  ```
- `beginTransaction`：
  ```java
  long maxBlockCount = resolveMaxBlockCount(session.getSessionProperties());
  return new MaxComputeConnectorTransaction(session.allocateTransactionId(), maxBlockCount);
  ```

**契约**：live 路径 `from(ctx)` 必注入合法 long → 连接器读到调优值 = legacy parity。任何缺/坏值 → fallback 20000 = **当前行为，零回归**（replay/无 ctx 等边路安全）。

## Risk Analysis

- **无注入的边路**（如某 transaction 不经 `from(ctx)` 建的 session）：`getSessionProperties()` 默认空 map → resolve 返 20000 = 现状。✅ 无新回归面。
- **读时机**：在 `beginTransaction` 读一次、存入 transaction 实例（block 分配在写执行期由 BE 回调）。legacy 在分配时直读 Config；二者仅在「管理员写中途改 fe.conf」时有别（可忽略）。✅
- **key typo 风险**（最关键）：fe-core 注入 key 与连接器读取 key 须 byte-identical，否则连接器永远读不到 → 静默 fallback 20000 → 回归仍在但更隐蔽。缓解 = 双侧交叉引用注释（同 P3-9 `ENABLE_MC_LIMIT_SPLIT_OPTIMIZATION` 约定）+ key 取 Config 字段名自身（自文档）。⚠️ 见 Test Plan 测试缺口说明。
- **import-gate / SPI**：连接器零新增 fe-core import（只读 session map）；无 SPI 签名变更。fe-core 加 `Config` import（fe-core 本就依赖 fe-common）。✅
- **DV-011 反转**：本修反转 DV-011 的 Rule-2 推迟（用户定 Option A）。DV-011 须更新为「已修正（GC1，经 session-property 透传）」。

## Test Plan

### Unit Tests

**连接器（行为所在，fe-core-free / mockito-free）：**

1. **新增 `MaxComputeConnectorTransactionTest`**（Rule 9 — pin「cap 可配置且被强制」）：
   - 用小 cap（如 `maxBlockCount=5`）构造 + `setWriteSession` → `allocateWriteBlockRange` 在 cap 内 OK、越 cap 抛 `DorisConnectorException`（断言 message 含 maxBlockCount）。
   - 用**不同** cap（如 3 vs 10）证上限确随 ctor 参变化（非硬编 20000）。
2. **`resolveMaxBlockCount(Map)` parse 测**（加入连接器某 metadata test 或新 transaction test）：present 合法值→解析；absent→`DEFAULT_MAX_BLOCK_COUNT`(20000)；unparseable→20000。

> mutation：`resolveMaxBlockCount` 改为「忽略 prop、恒返 DEFAULT」→ 「不同 cap」/「present 值解析」test 向红；还原绿。另可把 `:146` 的 `maxBlockCount` 改回硬编 → transaction cap 测向红。

**fe-core（注入侧）— 测试缺口如实登记（Rule 12）：**

- `extractSessionProperties` 是 private、`from(ctx)` 需重型 `ConnectContext`，且**先例 `lower_case_table_names` 注入本身无专门 builder 单测**（仅被 datasource/lowercase 集成测间接覆盖）。
- 故 fe-core 注入侧**不加专门单测**（与既有约定一致），由**编译** + **连接器侧行为测** + **双侧 byte-identical key 注释**（同 P3-9）保证。
- 若实现时发现 `ConnectorSessionBuilder.from(new ConnectContext())` 可廉价构造并断言 key 注入，则**加一条** fe-core 测以闭 key-typo 风险；否则依约定。**实现时定夺并在 summary 记结果。**

### E2E / live（真实 ODPS，CI 跳，登记 DV）

- live：fe.conf 设 `max_compute_write_max_block_count` 为小值（如 3）→ 大写入触发越限抛错；设大值→放宽。证连接器尊重 fe.conf（= legacy parity）。归入 DV（写路径真值闸，CI 跳）。

## 实现清单

1. `ConnectorSessionBuilder.java`：+import Config + 1 行 `props.put`。
2. `MaxComputeConnectorTransaction.java`：常量→字段 + ctor 加参 + `:146` 用字段 + `DEFAULT_MAX_BLOCK_COUNT`。
3. `MaxComputeConnectorMetadata.java`：key 常量 + `resolveMaxBlockCount` + `beginTransaction` 透传。
4. 测：新 `MaxComputeConnectorTransactionTest` + resolve parse 测（+ 可选 fe-core 注入测）。
5. 守门：编译（fe-core + 连接器）+ UT + checkstyle（fe-core + 连接器）+ import-gate + mutation。
6. 单 Agent 对抗 impl-review。
7. 独立 `[P4-T06e]` commit + hash 回填 + tracker（GC1 行）+ **更新 DV-011（已修正）**。
