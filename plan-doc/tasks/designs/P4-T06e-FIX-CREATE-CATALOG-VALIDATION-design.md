# [P4-T06e] FIX-CREATE-CATALOG-VALIDATION (GAP6) — design

> 来源：Batch-D 红线扩充对抗复审 workflow `wbw4xszrg`（GAP6，Tier 2，major）。用户定 **Fix**（2026-06-08 批量 G6+G5+G7）。
> 关联：legacy 对照 `MaxComputeExternalCatalog.checkProperties:388-457`；SPI 钩子 `ConnectorProvider.validateProperties`（no-op 默认 :74-76）；wiring `PluginDrivenExternalCatalog.checkProperties:153-165` → `ConnectorFactory.validateProperties:97-103` → `ConnectorPluginManager.validateProperties:161-174` → `provider.validateProperties`。
> 同侧参照：`JdbcConnectorProvider.validateProperties:50-112`（已 override，本设计逐式镜像其风格：本地 `REQUIRED_PROPERTIES` + `IllegalArgumentException` + 私有 helper）。

## Problem

翻闸后，CREATE CATALOG 对 max_compute 的**属性校验全部缺失**。`MaxComputeConnectorProvider`（连接器 SPI 入口）只 override `getType()`/`create()`，**未 override `validateProperties`** → 继承 SPI no-op 默认（`ConnectorProvider:74-76`，「all properties accepted」）。其余翻闸连接器（jdbc/es/trino）均已 override。

后果（对照 legacy `checkProperties` 在 CREATE 时的六类校验）：
- **required PROJECT / ENDPOINT 缺失**：CREATE 接受 → 退化为首次使用时 `MaxComputeDorisConnector.doInit()`（懒初始化）才以 `defaultProject=null` / `resolveEndpoint=null` 晚失败，错误信息晦涩、远离 CREATE 现场。
- **account_format 非法值**（如 `'foo'`）：`doInit:98-107` **静默 coerce 为 DISPLAYNAME**（`else` 分支），用户的非法配置被悄悄吞掉。
- **connect/read timeout、retry_count ≤ 0 或非整数**：`buildSettings:131-139` 用 `Integer.parseInt` 在**首次使用**才解析、且**无 >0 校验** → 负值被静默接受（传给 ODPS RestOptions，行为未定）；非整数抛 `NumberFormatException`（use-time，非 create-time）。
- **split_strategy 非 byte_size/row_count、split_byte_size < 10485760 floor、split_row_count ≤ 0**：连接器侧根本不校验（split 参数在 scan provider 消费）。
- **auth 属性不完整**（如 ak_sk 缺 access_key/secret_key）：`MCConnectorClientFactory.checkAuthProperties:42-78` **已定义但零调用方**（dead code）→ CREATE 时不查，运行时建客户端才可能晚失败。

净效果：非法 catalog 在 CREATE 时被接受（fail-late 或 silently-accept-illegal），违反 legacy「create 即校验、fail-fast」契约。

## Root Cause（已核码确认）

| # | 位置 | 现状 | legacy parity 源 |
|---|---|---|---|
| 1 | `MaxComputeConnectorProvider:29-41` | 仅 `getType`/`create`，无 `validateProperties` override → 继承 no-op | `MaxComputeExternalCatalog.checkProperties:388-457`（override，6 类校验，throws DdlException） |
| 2 | `MCConnectorClientFactory.checkAuthProperties:42-78` | 定义完整但 **grep 全 repo 零调用方**（dead） | legacy 经 `MCUtils.checkAuthProperties(props)`（`checkProperties:456`）调用 |
| 3 | `MaxComputeDorisConnector.doInit:98-107` | account_format 非法值静默→DISPLAYNAME | legacy `checkProperties:423-430` 非法→`throw DdlException("...only support name and id")` |
| 4 | `MaxComputeDorisConnector.buildSettings:131-139` | timeout/retry 仅 parseInt、无 >0 校验、且 use-time | legacy `checkProperties:439-449` 各 >0、create-time |

**wiring 已就绪**（无需改）：`PluginDrivenExternalCatalog.checkProperties:153-165`（CREATE CATALOG 校验钩子，先 `super.checkProperties()` 再）调 `ConnectorFactory.validateProperties` 且 **`catch (IllegalArgumentException e) → throw new DdlException(e.getMessage())`**（:159-160）。即：本 override 抛 `IllegalArgumentException` → 包成 `DdlException` → 用户看到的错误形态**与 legacy（直接抛 DdlException）一致**。

**为何 CI 没抓**：连接器 provider 无 `validateProperties` 的任何 UT（grep 无 `MaxComputeConnectorProviderTest`）；live e2e 未覆盖非法属性 CREATE。

## Blast radius

- 改动集中在连接器模块 `fe-connector-maxcompute`：`MaxComputeConnectorProvider`（加 override + 私有 helper）+ `MCConnectorClientFactory.checkAuthProperties`（异常类型对齐，见下）。**无 SPI 签名变更**（`validateProperties` 钩子早已存在）。
- `validateProperties` 仅在 CREATE CATALOG / ALTER CATALOG 属性校验路径被调（`checkProperties`），**不在 replay**（持久化老 catalog 从 image 重建、不重跑 create 校验）→ 老 catalog（含 region/odps_endpoint 式）不受影响。
- import-gate 净：仅用连接器内 `MCConnectorProperties` / `MCConnectorClientFactory` + `java.lang.IllegalArgumentException`，不 import fe-core（`org.apache.doris.{catalog,common,datasource,...}`）。
- 对其余连接器（jdbc/es/trino/hive…）零影响（各自 provider 独立）。

## Design

**Shape：连接器局部，无 SPI 变更** —— `MaxComputeConnectorProvider` override `validateProperties`，逐项镜像 legacy `checkProperties` 的六类校验，抛 `IllegalArgumentException`；wire 既有 dead `checkAuthProperties`。

### 六类校验（逐字镜像 legacy `checkProperties:388-457`）

```java
private static final List<String> REQUIRED_PROPERTIES = Arrays.asList(
        MCConnectorProperties.PROJECT,
        MCConnectorProperties.ENDPOINT);

@Override
public void validateProperties(Map<String, String> properties) {
    // 1. required: PROJECT + ENDPOINT（字面 key，镜像 legacy REQUIRED_PROPERTIES）
    for (String required : REQUIRED_PROPERTIES) {
        if (!properties.containsKey(required)) {
            throw new IllegalArgumentException("Required property '" + required + "' is missing");
        }
    }

    // 2. split strategy + floor（镜像 legacy :397-412）
    String splitStrategy = properties.getOrDefault(
            MCConnectorProperties.SPLIT_STRATEGY, MCConnectorProperties.DEFAULT_SPLIT_STRATEGY);
    try {
        if (splitStrategy.equals(MCConnectorProperties.SPLIT_BY_BYTE_SIZE_STRATEGY)) {
            long splitByteSize = Long.parseLong(properties.getOrDefault(
                    MCConnectorProperties.SPLIT_BYTE_SIZE, MCConnectorProperties.DEFAULT_SPLIT_BYTE_SIZE));
            if (splitByteSize < 10485760L) {
                throw new IllegalArgumentException(
                        MCConnectorProperties.SPLIT_BYTE_SIZE + " must be greater than or equal to 10485760");
            }
        } else if (splitStrategy.equals(MCConnectorProperties.SPLIT_BY_ROW_COUNT_STRATEGY)) {
            long splitRowCount = Long.parseLong(properties.getOrDefault(
                    MCConnectorProperties.SPLIT_ROW_COUNT, MCConnectorProperties.DEFAULT_SPLIT_ROW_COUNT));
            if (splitRowCount <= 0) {
                throw new IllegalArgumentException(MCConnectorProperties.SPLIT_ROW_COUNT + " must be greater than 0");
            }
        } else {
            throw new IllegalArgumentException("property " + MCConnectorProperties.SPLIT_STRATEGY + " must be "
                    + MCConnectorProperties.SPLIT_BY_BYTE_SIZE_STRATEGY + " or "
                    + MCConnectorProperties.SPLIT_BY_ROW_COUNT_STRATEGY);
        }
    } catch (NumberFormatException e) {
        throw new IllegalArgumentException("property " + MCConnectorProperties.SPLIT_BYTE_SIZE + "/"
                + MCConnectorProperties.SPLIT_ROW_COUNT + " must be an integer");
    }

    // 3. account_format ∈ {name, id}（镜像 legacy :423-430）
    String accountFormat = properties.getOrDefault(
            MCConnectorProperties.ACCOUNT_FORMAT, MCConnectorProperties.DEFAULT_ACCOUNT_FORMAT);
    if (!accountFormat.equals(MCConnectorProperties.ACCOUNT_FORMAT_NAME)
            && !accountFormat.equals(MCConnectorProperties.ACCOUNT_FORMAT_ID)) {
        throw new IllegalArgumentException(
                "property " + MCConnectorProperties.ACCOUNT_FORMAT + " only support name and id");
    }

    // 4. connect/read timeout + retry_count > 0（镜像 legacy :437-451）
    checkPositiveInt(properties, MCConnectorProperties.CONNECT_TIMEOUT, MCConnectorProperties.DEFAULT_CONNECT_TIMEOUT);
    checkPositiveInt(properties, MCConnectorProperties.READ_TIMEOUT, MCConnectorProperties.DEFAULT_READ_TIMEOUT);
    checkPositiveInt(properties, MCConnectorProperties.RETRY_COUNT, MCConnectorProperties.DEFAULT_RETRY_COUNT);

    // 5. auth 完整性（wire 既有 dead checkAuthProperties；镜像 legacy :456）
    MCConnectorClientFactory.checkAuthProperties(properties);
}
```

`checkPositiveInt` 私有 helper（合并 legacy 三个 timeout 的 parse+>0+NumberFormat 处理，去重）：
```java
private static void checkPositiveInt(Map<String, String> properties, String key, String defaultValue) {
    int value;
    try {
        value = Integer.parseInt(properties.getOrDefault(key, defaultValue));
    } catch (NumberFormatException e) {
        throw new IllegalArgumentException("property " + key + " must be an integer");
    }
    if (value <= 0) {
        throw new IllegalArgumentException(key + " must be greater than 0");
    }
}
```

### checkAuthProperties 异常类型对齐（wire dead code）

`MCConnectorClientFactory.checkAuthProperties:42-78` 现抛 `new RuntimeException(...)`（4 处）。但 wiring 钩子 `PluginDrivenExternalCatalog.checkProperties:159` **只 `catch (IllegalArgumentException)`** → 裸 `RuntimeException` 会**漏 catch 上抛**（auth 错与其余校验错形态不一致、不被包成 DdlException）。

**修**：把 `checkAuthProperties` 4 处 `RuntimeException` → `IllegalArgumentException`。安全性：① 该方法 grep 全 repo **零调用方**（dead，本 fix 是其唯一调用方）；② `IllegalArgumentException extends RuntimeException` → 源码兼容、任何未来「期望 RuntimeException」的捕获仍生效；③ 与 SPI 约定（jdbc/es/trino 的 validateProperties 全抛 IllegalArgumentException）一致。

### 子决策：required ENDPOINT 取「字面 key」而非「resolveEndpoint != null」

legacy `REQUIRED_PROPERTIES` 要求**字面 `mc.endpoint` key**（`checkProperties:391`）。`MCConnectorEndpoint.resolveEndpoint` 虽接受 ENDPOINT/TUNNEL/ODPS_ENDPOINT/REGION 四源，但那是 **replay 老持久化 catalog 的 backward-compat**（legacy `generatorEndpoint` 同款四源亦只用于 init/replay，CREATE 仍要求 ENDPOINT——见 legacy 注 :150-154）。故 CREATE-time parity = **require 字面 PROJECT + ENDPOINT**。

- 取此（faithful parity）：region/odps_endpoint-only 的**新** CREATE 被拒（= legacy 行为）；老持久化 catalog 走 replay、不经 validateProperties、不受影响。
- 备选（impl-review/用户可推翻）：放宽为 `resolveEndpoint(properties) != null`（接受四源任一）。更贴「当前连接器 runtime 能力」但**比 legacy CREATE 宽**。本设计取 faithful parity（campaign 目标 = legacy parity），明列于此供审。

## Implementation Plan

1. `MaxComputeConnectorProvider`：加 `import java.util.Arrays; import java.util.List;`（`Map` 已在）；加 `REQUIRED_PROPERTIES` 常量 + override `validateProperties` + 私有 `checkPositiveInt`。
2. `MCConnectorClientFactory.checkAuthProperties`：4 处 `RuntimeException` → `IllegalArgumentException`（异常类型对齐）。
3. **新增 UT** `MaxComputeConnectorProviderTest`（连接器模块，纯 JUnit、无 fe-core/Mockito）——见 Test Plan。
4. 守门：编译（`:fe-connector-maxcompute`）+ UT + checkstyle + import-gate + mutation。

## Risk Analysis

| Risk | Mitigation |
|---|---|
| 校验逻辑与 legacy 分歧（floor/enum/>0 边界） | 逐字镜像 legacy `checkProperties`；UT 钉每条边界（floor=10485760-1 拒 / =10485760 过；timeout=0 拒 / =1 过；account_format='foo' 拒 / 'name'+'id' 过）。 |
| 默认值下「合法空配」被误拒 | 全部 getOrDefault + DEFAULT_*（DEFAULT_SPLIT_BYTE_SIZE=268435456 > floor；DEFAULT timeouts 10/120/4 > 0；DEFAULT_ACCOUNT_FORMAT=name）→ 仅含 PROJECT+ENDPOINT+合法 auth 的最小配过校验。UT 钉。 |
| checkAuthProperties 异常类型改动误伤调用方 | grep 证零调用方（dead）；IllegalArgumentException 为 RuntimeException 子类、源码兼容。 |
| required ENDPOINT 过严（over-restrict 回归） | 已论证 = legacy CREATE parity；replay 老 catalog 不经此路。备选放宽已明列供审。 |
| RuntimeException 漏 catch（auth 路径） | 已对齐 IllegalArgumentException → 被 checkProperties:159 catch → DdlException（parity）。UT 直接断言 IllegalArgumentException。 |

## Test Plan

### Unit Tests（新增 `MaxComputeConnectorProviderTest`，连接器模块，纯 JUnit）

钉 **WHY**（Rule 9）：CREATE CATALOG 必须 fail-fast 拒非法属性，否则退化 use-time 晚失败 / 静默接受非法值（account_format='foo'→DISPLAYNAME、负 timeout）。每条对应一类 legacy 校验。

构造 `MaxComputeConnectorProvider`，用 `validProps()` 工厂（PROJECT+ENDPOINT+ak_sk+ACCESS_KEY+SECRET_KEY）派生各 case：
1. **valid 最小配** → 不抛（getOrDefault 默认全过）。
2. **缺 PROJECT** / **缺 ENDPOINT** → `IllegalArgumentException`，message 含该 key。
3. **split_byte_size = 10485759（floor-1）** → 抛；**= 10485760** → 过；**非整数 "abc"** → 抛「must be an integer」。
4. **split_strategy = "foo"** → 抛「must be byte_size or row_count」；**= "row_count" + split_row_count = 0** → 抛；**= "row_count" + 正值** → 过。
5. **account_format = "foo"** → 抛「only support name and id」；**= "id"** → 过；**= "name"** → 过。
6. **connect_timeout = "0"** / **"-1"** → 抛「must be greater than 0」；**read_timeout = "abc"** → 抛「must be an integer」；**retry_count = "0"** → 抛。
7. **auth（wire checkAuthProperties）**：ak_sk 缺 SECRET_KEY → 抛 `IllegalArgumentException`（验证 dead code 已 wire 且异常类型已对齐）；ram_role_arn 缺 RAM_ROLE_ARN → 抛；未知 auth.type → 抛。

### mutation（守门）

还原任一校验 → 对应 UT 变红：
- M1：`splitByteSize < 10485760L` → `< 0L`（floor 永过）→ floor-1 用例变绿失败（断言期望抛）→ 红。
- M2：account_format 的 `&&` 取反 / 删 throw → account_format='foo' 用例红。
- M3：删 `checkAuthProperties` 调用 → 缺 SECRET_KEY 用例红。
还原 → 全绿。

### E2E Tests（CI 跳，真实 ODPS = 真值闸，登记 DV）

- `CREATE CATALOG ... PROPERTIES(...)` 缺 endpoint / account_format='foo' / 负 timeout / 缺 auth → CREATE **立即**报错（DdlException），不进入 use-time。
- 合法属性 CREATE 成功 + 可查。
- 归 DV（编号续 DV-022 之后，落 tracker），需用户 live 跑。

## 决策类型

明确修复（用户定 Fix，Tier 2 major）。连接器局部、无 SPI 变更、与 legacy `MaxComputeExternalCatalog.checkProperties` 达成 CREATE-time 校验 parity。

**设计内子决策（供 impl-review / 用户审）**：
- required ENDPOINT 取字面 key（faithful legacy parity）vs 放宽 resolveEndpoint!=null —— 取前者，已论证。
- `checkAuthProperties` 异常类型 RuntimeException→IllegalArgumentException（dead code wire 对齐 SPI 约定）。
