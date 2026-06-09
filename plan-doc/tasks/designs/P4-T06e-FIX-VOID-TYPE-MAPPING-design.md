# [P4-T06e] FIX-VOID-TYPE-MAPPING (GAP7) — design

> 来源：Batch-D 红线扩充对抗复审 workflow `wbw4xszrg`（GAP7，Tier 2，minor）。
> 关联：legacy 对照 `MaxComputeExternalTable.mcTypeToDorisType`（VOID→`Type.NULL`；default→hard-throw）；`ScalarType.createType:241`（认 `"NULL_TYPE"`→NULL，不认 `"NULL"`）；`ConnectorColumnConverter.convertScalarType`（无 "NULL" case、catch→UNSUPPORTED）。

## Problem

翻闸后 ODPS `VOID` 列类型映为 **UNSUPPORTED**（legacy=`Type.NULL`）。链（已核码）：
- `MCTypeMapping.toConnectorType:51-52`：`case VOID: return ConnectorType.of("NULL")`——emit token **"NULL"**。
- fe-core `ConnectorColumnConverter.convertScalarType`：**无 "NULL" case** → 落 default `ScalarType.createType("NULL")`。
- `ScalarType.createType:237-299`：只认 **"NULL_TYPE"**→`Type.NULL`（:241），**"NULL"** 落 default → `Preconditions.checkState(false)` **抛** → 被 convertScalarType catch → **`Type.UNSUPPORTED`**。

净：VOID 列静默成 UNSUPPORTED（legacy 为可用的 `Type.NULL`）。

**次生缺陷**（HANDOFF 标记）：未知 OdpsType 处置分歧。`MCTypeMapping.toConnectorType:99-100` `default: return of("UNSUPPORTED")`（**静默**）；legacy `mcTypeToDorisType` default `throw IllegalArgumentException("Cannot transform unknown type: ...")`（**硬抛 fail-fast**）。

## Root Cause（已核码确认）

| # | 位置 | 现状 | legacy parity |
|---|---|---|---|
| 1 | `MCTypeMapping.toConnectorType:52` | `of("NULL")` | VOID→`Type.NULL`（token 须为 `ScalarType.createType` 认的 `"NULL_TYPE"`） |
| 2 | `ConnectorColumnConverter.convertScalarType` | 无 "NULL" case，default `createType(name)` catch→UNSUPPORTED | — （token 修对后此处直接 `createType("NULL_TYPE")`→`Type.NULL`，无需改 fe-core） |
| 3 | `ScalarType.createType:241` | `case "NULL_TYPE": return NULL`；`"NULL"` 落 default 抛 | — |
| 4 | `MCTypeMapping.toConnectorType:99-100` | `default: return of("UNSUPPORTED")`（静默） | legacy default **hard-throw** |

**为何 CI 没抓**：连接器 `MCTypeMapping.toConnectorType` 无 UT（仅反向 `toMcType` 间接经 validateColumns 测）；live e2e 无 VOID 列覆盖。

## Blast radius

- 改动集中在连接器 `MCTypeMapping.toConnectorType`（VOID token + default throw）。**无 SPI 变更、无 fe-core 改动**（token 修对后 fe-core `convertScalarType` default 即正确处理 "NULL_TYPE"→Type.NULL）。
- VOID token 改 "NULL"→"NULL_TYPE"：仅影响 ODPS VOID 列读路径 schema 映射（→ Type.NULL，legacy parity）。
- default throw：BINARY/INTERVAL_DAY_TIME/INTERVAL_YEAR_MONTH 已是**显式** UNSUPPORTED case（:95-98），JSON 显式 UNSUPPORTED（:75-76），其余已知列类型皆有显式 case → **不受 default 影响**。`default` 仅被 `OdpsType.UNKNOWN`（ODPS SDK sentinel，非真实列类型；经 `TypeInfoFactory.UNKNOWN` 可构造）+ 未来未知 OdpsType 命中；legacy 对 UNKNOWN 亦无 case → 同样 throw（`MaxComputeExternalTable:294`）→ 故 fix-2 = legacy parity，真实表已知列类型零回归。
- import-gate 净（仅用连接器内 `DorisConnectorException`，已 import :21）。
- **out-of-scope（不改，Rule 3）**：ES 连接器 `EsTypeMapping:191` 亦 emit `of("NULL")`（同款 latent token bug），但 ES 非本翻闸/本 issue 范围，留。

## Design

**Shape：连接器局部，无 SPI / 无 fe-core 变更。**

### fix-1（primary，VOID token）：`MCTypeMapping.toConnectorType:52`

```java
case VOID:
    return ConnectorType.of("NULL_TYPE");   // 原 "NULL"
```

`"NULL_TYPE"` = `ScalarType.createType` 唯一认得、产 `Type.NULL` 的 token（:241）。fe-core `convertScalarType` default 即 `createType("NULL_TYPE")`→`Type.NULL`（不抛、不 catch、不降 UNSUPPORTED）。VOID→Type.NULL = legacy parity。**所有其它 MCTypeMapping token 已与 `ScalarType.createType` token 精确匹配，本修使 VOID 亦一致。**

### fix-2（secondary defect，default fail-fast）：`MCTypeMapping.toConnectorType:99-100`

```java
default:
    throw new DorisConnectorException(
            "Cannot transform unknown MaxCompute type: " + odpsType);   // 原 return of("UNSUPPORTED")
```

镜像 legacy `mcTypeToDorisType` default hard-throw（legacy :294）。**安全性**：BINARY/INTERVAL_*/JSON 等已知-不支持类型均**显式** UNSUPPORTED case（:75-76, :95-98）、不受影响；default 仅被 `OdpsType.UNKNOWN`（SDK sentinel）+ 未来未知类型命中——legacy 对 UNKNOWN 同样 throw（无 case）→ parity；真实表已知列类型零回归。

**决策（已定，供 user veto）**：fix-2 纳入。理由：① campaign 目标 = legacy parity（legacy 对 UNKNOWN throw）；② CLAUDE.md Rule 12「Fail loud」（静默 UNSUPPORTED 掩盖未知类型问题）；③ 用户本 campaign 一贯取 full parity（G8/P2-8/G5）；④ 真实表已知列类型零回归。**可 UT 覆盖**：`OdpsType.UNKNOWN`（经 `TypeInfoFactory.UNKNOWN`）落 default → assertThrows（legacy 对 UNKNOWN 同 throw）。若 user 倾向「保留 graceful UNSUPPORTED 降级」则单删 fix-2（一行 revert），不影响 fix-1。

## Implementation Plan

1. `MCTypeMapping.toConnectorType`：VOID `of("NULL")`→`of("NULL_TYPE")`（fix-1）；default `return of("UNSUPPORTED")`→`throw DorisConnectorException`（fix-2）。
2. **新增 UT** `MCTypeMappingTest`（连接器模块，纯 JUnit，用 `TypeInfoFactory` 构造 TypeInfo）——见 Test Plan。
3. 守门：编译 + UT + checkstyle + import-gate + mutation。

## Risk Analysis

| Risk | Mitigation |
|---|---|
| "NULL_TYPE" 下游不被认 | 已核 `ScalarType.createType:241` `case "NULL_TYPE": return NULL`；convertScalarType default 直接 createType、无 catch 命中。 |
| default throw 误伤已知-不支持类型 | BINARY/INTERVAL_*/JSON 均显式 UNSUPPORTED case；default 当前不可达（23 已知类型全显式）→ 零现表回归。UT 钉 BINARY→UNSUPPORTED（证显式 case 未被 throw 吞）。 |
| ARRAY/MAP/STRUCT 元素为 VOID | 复用同 `toConnectorType` → 元素 VOID 亦正确成 "NULL_TYPE"（嵌套递归一致）。 |
| fix-2 无 UT 覆盖 | 透明声明（default 当前不可达、不可触发）；不伪造覆盖。Rule 9/12。 |

## Test Plan

钉 **WHY**（Rule 9）：VOID 须映为下游产 `Type.NULL` 的 token（legacy parity），否则静默成 UNSUPPORTED（列不可用）。

### Unit Tests（新增 `MCTypeMappingTest`，连接器模块，纯 JUnit）

1. **VOID→"NULL_TYPE"（核心）**：`toConnectorType(TypeInfoFactory.VOID)` → `getTypeName()=="NULL_TYPE"`。MUTATION：还原 `of("NULL")` → 红。
2. **VOID 嵌套**：ARRAY<VOID> → 元素 ConnectorType typeName=="NULL_TYPE"（证递归一致）。
3. **BINARY→"UNSUPPORTED"**（守 fix-2 不误伤）：`toConnectorType(TypeInfoFactory.BINARY)` → "UNSUPPORTED"（**不**抛）。证已知-不支持类型仍走显式 UNSUPPORTED case、未被 default throw 吞。
4. **UNKNOWN→throw（fix-2）**：`toConnectorType(TypeInfoFactory.UNKNOWN)`（OdpsType.UNKNOWN 落 default）→ `assertThrows(DorisConnectorException)`、msg 含 "unknown"。证 fail-fast = legacy parity。
5. **smoke 已知类型**：INT→"INT"、STRING→"STRING"、BOOLEAN→"BOOLEAN"（防 token 漂移）。

### mutation（守门）
- M1：VOID token "NULL_TYPE"→"NULL" → test-1/2 红。
- M2：default `throw`→`return of("UNSUPPORTED")` → UNKNOWN 测（assertThrows）变红。

### E2E（CI 跳，真实 ODPS = 真值闸，登记 DV）
- ODPS VOID 列表 `DESCRIBE` / `SELECT` → 列类型为 NULL（非 UNSUPPORTED），可查。需用户 live 跑。

## 决策类型

明确修复（用户定 Fix，Tier 2 minor）。连接器局部、无 SPI/fe-core 变更、与 legacy `MaxComputeExternalTable.mcTypeToDorisType` 达成 parity（VOID→Type.NULL + unknown→fail-fast）。

**设计内决策（供 impl-review / user veto）**：
- VOID 取 Option A（连接器 token "NULL_TYPE"）而非 Option B（fe-core 加 "NULL" case）——更 surgical、token 拼写 canonical、不教 fe-core 连接器专有错拼。
- fix-2（secondary default throw）纳入（parity + Rule 12 fail-loud + 零现表风险）；透明声明不可 UT 覆盖。
- ES `EsTypeMapping:191` 同款 token bug out-of-scope（Rule 3）。
