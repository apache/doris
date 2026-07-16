# FIX-L1 — import-gate 漏洞补齐（三洞 + 红队发现的第 4 洞）

> 来源：`plan-doc/reviews/catalog-spi-review-65185-reverify-2026-07-11.md` §1 表 L1（原 P0-5）。
> 严重度：🟡 低（门禁/防御性；无 fe 编译）。范围：`tools/check-connector-imports.sh` + 新增 `.test.sh`。
> HEAD 复核基线：`500472410d5`。
> **设计红队**：`wf_643c11b4-3fe`（3 lens：correctness / false-positive / scope，均 SOUND_WITH_CHANGES）
> **发现并已折入原设计漏掉的第 4 洞 + 修正 E3 定性 + 修正测试夹具**。已逐条独立复现验证。

## Problem

`tools/check-connector-imports.sh` 是「连接器模块不得 import fe-core 内部包」的防线（RFC §15.4）。
reverify §1 记为**三个漏洞**，红队复现时又发现**第 4 个、更致命的结构性漏洞**：

1. **漏 `import static`**：候选 grep 只匹配 `^import org.apache.doris.<pkg>.`，不匹配 `import static ...`。
2. **漏 6 个 fe-core 内部包**：`FORBIDDEN` 缺 `persist|transaction|fs|statistics|mysql|service`
   （fe-core 下各有 89/35/13/81/95/23 个 `.java`，都是连接器绝不应 reach 的内部包）。
3. **只扫 `src/main/java`**：glob 不覆盖 `src/test/java`（16 个连接器模块都有 test 源）。
4. **⭐【红队发现】白名单按「文件路径」而非「import 目标」抑制**：4 条 `grep -v 'org.apache.doris.<pkg>'`
   （thrift/connector/extension/filesystem）匹配的是**整行**（`<path>:<lineno>:import ...`）。BRE/ERE 里 `.`
   匹配任意字符**包括 `/`**，故 `grep -v 'org.apache.doris.connector'` 会命中**文件路径**
   `.../src/main/java/org/apache/doris/connector/**` → 把该文件里的**任何**违规 import 整行丢掉。
   而**全部 608 个连接器实现文件**都根在 `org.apache.doris.connector.**`（hive/paimon/jdbc/es 实测）
   → 门禁对**它本该守护的那些文件结构性失明**。实测：连接器命名空间文件里放 `import org.apache.doris.catalog.Type;`
   现版脚本 **exit 0**（放行）。→ 洞 1/2/3 的加宽对连接器文件**基本无效**（都被路径抑制先丢了）。

## Root Cause

denylist 门禁四处都写错：正则不含 `static`、包清单缺 6 项、glob 只有 main、**白名单抑制误用整行匹配（含路径）**。
关键洞察：4 个白名单包（connector/thrift/extension/filesystem）都是 fe-core 的**兄弟包**，**不是任何 FORBIDDEN 包的子包**
→ 候选 grep（`^import ${FORBIDDEN}[.]`）**从不捕获**合法 SPI import → 这 4 条 `grep -v` **对其本意（放行 SPI import）纯冗余**，
唯一实际效果就是**按路径误伤**。

## Design（4 处功能编辑 + 1 处配套 + 1 处文档；均已实测验证）

铁律对齐：tools 侧门禁强化，不碰任何 fe-core/连接器 Java 代码，无架构风险。

- **E1（洞 2）**行 48：`FORBIDDEN` 补 6 包。
- **E2a（洞 1）**候选 grep 正则：`^import ${FORBIDDEN}\.` → `^import[[:space:]]+(static[[:space:]]+)?${FORBIDDEN}[.]`
  （`[.]`≡`\.`；`[[:space:]]+` 顺带兜 tab/多空格——红队 nit，零风险 + 增强）。
- **E2b（洞 3）**glob：`${ROOT}/*/src/main/java` → 同时列 `${ROOT}/*/src/test/java`。
- **E2c（洞 4，红队发现）**：删 4 条按整行的 `grep -v`，换为**单条锚定到 import 目标**的排除：
  `grep -vE ':import[[:space:]]+(static[[:space:]]+)?org[.]apache[.]doris[.](connector|thrift|extension|filesystem)[.]'`
  （`:import` 前缀锁定到冒号后的 import 内容，不再命中文件路径）。
- **E3（配套；红队修正定性=正确性必需，非"cosmetic"）**行 85 fqn 抽取 sed：补 `(static[[:space:]]+)?`。
  **红队证**：无 E3 时 `import static org.apache.doris.datasource.hive.HiveVersionUtil.X;` 会被**误报**
  （is_vendored 拿到带 `static ` 前缀的 fqn → 永远找不到 vendored 文件 → 当违规上报）；有 E3 才正确 skip。
  当前树零此类 import 故 latent，但一旦出现即真误报 → 属正确性修，非装饰。
- **E4（文档同步）**头注把包清单、含 static / 含 test / import-anchored 白名单写进注释，保持自说明。

**保留不动**：`is_vendored()`（HiveVersionUtil vendored FP，memory
`catalog-spi-hms-hiveversionutil-gate-false-positive`）。其 `[A-Z]*` 段判定在 en_US collation 会连带匹配小写
（红队 nit）——当前树零 `import static org.apache.doris.*` 故 inert，本条不动，登记观察。

## Risk Analysis

- **不破当前构建**：对 HEAD 用**完整修复版**脚本实测（含 E2c 锚定），真实 `fe/fe-connector` 仍 `exit 0`
  （3× 确定性；仅 2 条 vendored HiveVersionUtil skip）。连接器今日 import 的 812 `connector`/143 `thrift`/
  30 `filesystem`/4 `extension` 全被锚定白名单正确放行，且**零** forbidden import → E2c 不 surface 任何存量违规。
  故第 4 洞 latent 非 live（红队证），折入安全。
- **`fs` vs `filesystem` 误伤**：`fs[.]` 需字面 `fs.`；`filesystem` 里 `fs` 后接 `ystem` 不匹配。已实测。
- **glob 边界**：`*/src/test/java` ≥1 模块匹配 → bash 只展开存在目录；`2>/dev/null`+`|| true`+`set -e` 安全。
- **denylist 仍非穷举**（`system/load/backup/...` 未列；**FQN-无-import 内联用法**、多空格/tab 等 grep-denylist 固有盲区）
  ——根治须 allowlist（Trino 用 ArchUnit 按字节码约束 `classesShouldOnlyDependOnAllowedPackages`，能连 FQN 内联一并拦）。
  **本条不做**，登记为设计观察（见文末）。当前树对这些盲区实测零命中。

## Implementation Plan

改 `tools/check-connector-imports.sh` 行 48 / 50-54 / 85 + 头注；新增 `tools/check-connector-imports.test.sh`。

## Test Plan

### Durable 自测（`tools/check-connector-imports.test.sh`，mktemp -d + trap 自清）

构造临时 fixture ROOT，一个假连接器模块，覆盖全部 4 洞 + E3 + 白名单 + vendored：
- `src/main/java/org/apache/doris/**fake**/FakeConn.java`（**token-free 包**，避开白名单 token 撞路径）：
  洞 1 `import static org.apache.doris.catalog.Type.INT;`；洞 2 `persist/fs/statistics/mysql/service` 各一；
  合法 `thrift/filesystem/connector.api`（须**不**报）；vendored `import ...HiveVersionUtil;`（须 skip）；
  **E3** `import static ...HiveVersionUtil.SOME_CONST;`（无 E3 会误报 → 须 skip）。
- `src/main/java/org/apache/doris/datasource/hive/HiveVersionUtil.java`：vendored 定义。
- **洞 4** `src/main/java/org/apache/doris/**connector**/fake/ConnPathConn.java`：`import org.apache.doris.catalog.Type;`
  ——文件在连接器命名空间下，**旧脚本按路径丢弃（放行）、新脚本须报**（锁死第 4 洞回归）。
- **洞 3** `src/test/java/org/apache/doris/fake/FakeConnTest.java`：`import org.apache.doris.transaction.TransactionState;`。

断言（已用 scratch 预演实证）：
- **RED**（改前脚本 / 或从 git 取旧版）：对该 fixture **exit 0、零上报**（四洞全漏，含连接器路径那条被路径抑制）。
- **GREEN**（改后脚本）：**exit 1**，恰报 **8** 条违规（static-catalog / persist / fs / statistics / mysql /
  service / **连接器路径-catalog** / test-transaction），**不**含 thrift/filesystem/connector.api，2 条 vendored 均 skip。
- **真树回归**：改后脚本对真实 `fe/fe-connector` 仍 `exit 0`。

### E2E

不涉及运行时行为，无 e2e。

## 观察（不在本条实现，登记设计债）

grep-denylist 有三类固有盲区，本条只补 4 洞、无法根治：① 新增 fe-core 内部包忘登记；② FQN-无-import 内联用法
（`^import` grep 抓不到，当前树零命中）；③ 非规范 import 间距（E2a 已兜多空格/tab）。根治=allowlist / ArchUnit
（Trino 先例，字节码级、连内联 FQN 一并拦）。属独立重构，登记观察，本条不做。
