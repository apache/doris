# FIX-L14 — paimon `ignore_split_type` session 变量插件路径静默 no-op

> reverify §1 L14 (P5-5)。严重度 🟡低。模块 = fe-connector-paimon（连接器局部）。

## Problem

`ignore_split_type`（fe-core `SessionVariable`，选项 `NONE`/`IGNORE_JNI`/`IGNORE_NATIVE`/`IGNORE_PAIMON_CPP`）
是一个**调试/隔离 reader-bug 的逃生阀**：设为 `IGNORE_JNI` 应跳过所有 JNI split、`IGNORE_NATIVE` 应跳过所有
native split。翻闸后的 paimon 连接器**完全不读**该变量 → 无论设何值都照常发全部 split（静默 no-op）。

DV-035 §(h) M1.1 曾把它列为 diagnostic 偏差，但注为「须 fe-core SessionVariable 类型」；实为**可在连接器侧就地
恢复 parity**（变量已经 `session.getSessionProperties()` 暴露为字符串，无需 import fe-core 类型）。

## HEAD recon（对 HEAD 复核）

`PaimonScanPlanProvider.planScan`：
- `:428` 已读 `cppReader = isCppReaderEnabled(session)`（同一 `session.getSessionProperties()` 通道）。
- `:446-449` nonDataSplits 臂 → `buildJniScanRange`（这些恒 JNI）。
- `:470-513` DataSplit for-loop：
  - `:471-477` count 臂（`isCountPushdownSplit`）→ `continue`（**count 不受 ignore 影响**，legacy 同）。
  - `:485-506` native 臂（`shouldUseNativeReader`）。
  - `:507-512` else = JNI 臂。
- 全文件无 `ignore_split_type` 引用（grep 证实）。

## Legacy parity target（git `dbc38a265e5^` `PaimonScanNode.getSplits`）

- `:380-381` 读 `IgnoreSplitType.valueOf(sessionVariable.getIgnoreSplitType())`。
- `:401` nonDataSplits：`if (IGNORE_JNI) continue;`。
- `:443` native 臂：`if (IGNORE_NATIVE) continue;`。
- `:483` DataSplit JNI else 臂：`if (IGNORE_JNI) continue;`。
- count 臂：**不**检查 ignore（never ignored）。
- **`IGNORE_PAIMON_CPP` 在 legacy `getSplits` 从不被引用 → 本身即 no-op**（grep 全 legacy 文件确认仅 JNI/NATIVE）。

结论：parity = 实现 `IGNORE_JNI`（nonDataSplit + DataSplit-JNI）与 `IGNORE_NATIVE`（native 臂）；
`IGNORE_PAIMON_CPP` 保持 no-op（**= legacy parity**，非新缺口），文档/注释显式登记。

## Design（surgical，仿 legacy `continue`）

常量：
```java
private static final String IGNORE_SPLIT_TYPE = "ignore_split_type";
private static final String IGNORE_SPLIT_TYPE_JNI = "IGNORE_JNI";
private static final String IGNORE_SPLIT_TYPE_NATIVE = "IGNORE_NATIVE";
```

`planScan` 内（`cppReader` 读取旁）读一次：
```java
String ignoreSplitType = session.getSessionProperties()
        .getOrDefault(IGNORE_SPLIT_TYPE, "NONE");
boolean ignoreJni = IGNORE_SPLIT_TYPE_JNI.equals(ignoreSplitType);
boolean ignoreNative = IGNORE_SPLIT_TYPE_NATIVE.equals(ignoreSplitType);
// IGNORE_PAIMON_CPP 刻意不实现：legacy PaimonScanNode.getSplits 亦从不引用它（parity no-op）。
```

三处 `continue`（严格对齐 legacy 位置）：
- nonDataSplits 循环顶：`if (ignoreJni) { continue; }`。
- native 臂（`if (shouldUseNativeReader...)` 内首行）：`if (ignoreNative) { continue; }`。
- JNI else 臂首行：`if (ignoreJni) { continue; }`。

count 臂**不加**（parity）。

## Risk

- 语义 = 用户显式设的调试变量：跳过 split ⇒ 丢行是**故意**的隔离行为（legacy 同）。非默认路径（默认 `NONE`
  无行为变更，逐字节不变）。
- `getOrDefault` 兜底 `"NONE"`：session 未设时 ignoreJni/ignoreNative 皆 false → 全部 split 照发。
- 大小写：SessionVariable checker 只允许枚举大写值；`.equals` 精确匹配即可（legacy `valueOf` 亦大小写敏感）。

## Test Plan

### Unit（RED-able）— `PaimonScanPlanProviderTest`（复用 `buildRealDataSplit`/`RecordingPaimonCatalogOps`/`sessionWithProps`）

- **新** `ignoreJniSkipsForcedJniDataSplit`：`force_jni_scanner=true`（DataSplit→JNI）+
  `ignore_split_type=IGNORE_JNI` → 断言无 `paimon.split` JNI range（数据 range 为空）。MUTATION：不实现 → 仍发 1 → RED。
- **新** `ignoreNativeSkipsNativeDataSplit`：native 路径（无 force_jni）+ `ignore_split_type=IGNORE_NATIVE` →
  断言无 native 数据 range。MUTATION：不实现 → 仍发 → RED。
- **新** `ignorePaimonCppIsNoOpParity`：`ignore_split_type=IGNORE_PAIMON_CPP` → range 集与 `NONE` 相同
  （钉 legacy parity no-op，防未来误加半套）。
- **guard** `ignoreNoneEmitsAllSplits`：`NONE`/未设 → range 照常（baseline）。

### E2E（live-gated，登记）

真集群设 `SET ignore_split_type='IGNORE_JNI'` / `'IGNORE_NATIVE'`，对含两类 split 的 paimon 表 scan，
断言对应类型 split 被跳过（行数变化符合预期）。→ 真集群回归。

---

## 设计红队结论（`wf_05574ccb-bd2`，3 lens · SOUND / SOUND_WITH_CHANGES，无 UNSOUND）

- **legacy-parity SOUND**：全 legacy 文件 + 全 legacy 树 grep 证 `IGNORE_PAIMON_CPP` **从不**被任何 scan 路引用
  (仅 enum/checker/错误串)→no-op 是**真 legacy parity**非新缺口。三 `continue` 位 1:1 对齐 legacy(:401/:443/:483)、count 臂不检查。
- **MINOR（null-guard，legacy-parity+correctness 双 lens 同报）已折入**：内联 `session.getSessionProperties()` 缺 `session==null`
  守卫(本文件所有 session 读取器 `isCppReaderEnabled`/`isForceJniScannerEnabled`/`sessionLong` 均先 null-guard)→纯 nonDataSplit/空 scan
  的 null-session 路径会 NPE(HEAD 现容忍)。**加 null-tolerant `resolveIgnoreSplitType(ConnectorSession)`**(null→"NONE"),镜像 `isCppReaderEnabled`。
- **MINOR（test-build）已折入**：RED 测**不能**用 detached `buildRealDataSplit`(catalog 关闭后 split 游离,仅静态 helper 可用);
  须走 live `ops.table=table + provider.planScan(...)` 端到端;`ignoreJniDropsForcedJniSplit` 需 2-entry props→用 `new HashMap<>()`。已照办。
- **MINOR（覆盖缺口，已登记）**：nonDataSplits 的 `IGNORE_JNI continue` 离线不可单测(planScan 枚举造不出 system-table 非-DataSplit);
  经 IGNORE_JNI(DataSplit else 臂)+IGNORE_NATIVE(native 臂)+IGNORE_PAIMON_CPP(no-op) 三测覆盖两 continue 位,nonDataSplit 位留 **E2E-only**。
