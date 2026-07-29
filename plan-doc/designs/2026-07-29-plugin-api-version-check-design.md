# 插件 API 版本检查设计（四族统一）

- 日期：2026-07-29
- 状态：**已实现并验证**（2026-07-29）。实现记录、与本文的全部偏差及理由见 §14；正文中标注
  "勘误" 的段落是实现阶段被实证推翻的原始断言。
- 范围：CONNECTOR / FILESYSTEM / AUTHENTICATION / LINEAGE 四个插件族

---

## 1. 背景：现有机制存在，但恒真空转

`ConnectorProvider` 上有 `default int apiVersion() { return 1; }`，`ConnectorPluginManager`
有 `static final int CURRENT_API_VERSION = 1`，并在三处比较：

| 位置 | 行为 |
| --- | --- |
| `ConnectorPluginManager#createConnector` | 不等 → `LOG.warn` + `continue`，最终返回 `null` |
| `ConnectorPluginManager#findProvider` | 不等 → 静默 `continue` |
| `ConnectorPluginManager#validateProperties` | 不等 → 抛 `IllegalArgumentException` |

**这套检查不可能拦住任何插件**，原因是三个事实叠加：

1. 全仓 8 个 `ConnectorProvider` 实现（hive / iceberg / es / paimon / jdbc / maxcompute /
   trino / hudi）**没有任何一个 override `apiVersion()`**，唯一的 override 在测试桩里。
2. `ConnectorProvider` 落在 `ConnectorPluginManager.CONNECTOR_PARENT_FIRST_PREFIXES`
   （`org.apache.doris.connector.`）里，接口**永远由内核 classloader 加载**。
3. 各连接器的 `plugin-zip.xml` 显式排除 `fe-connector-api` / `fe-connector-spi` /
   `fe-extension-spi` —— 插件 zip 里根本没有连接器 SPI 的字节码。

   > **勘误 + 已修复（2026-07-29）**：原本 `fe-filesystem-api` 只有 8 个中的 6 个排除
   > （hive/hudi/iceberg/jdbc/maxcompute/paimon），**es 和 trino 没排**，它们的 `lib/` 里
   > 真的带了一份重复的 `fe-filesystem-api.jar`。因为 `org.apache.doris.filesystem.` 对
   > 连接器族是 parent-first，那份副本是死重量，不影响本设计的推理（论证只依赖前三个
   > artifact 被全部排除）。**本次一并修好**：给 es / trino 补上该排除项并对齐头注释，
   > 8/8 连接器 zip 现在排除同样 4 个 artifact。安全性依据不是"parent-first 所以无所谓"
   > 一句话，而是三条实证：① `fe-filesystem-api.jar` 内**零** `META-INF/services` 条目
   > （`ChildFirstClassLoader.getResources` 是 child-first + parent 兜底，故不会丢服务注册）；
   > ② es / trino 对 `org.apache.doris.filesystem` 的 import 数为 **0**（main + test）；
   > ③ 重建后逐 zip 比对，`fe-filesystem-api` 8/8 消失而 `fe-foundation` 8/8 保留
   > （foundation 对连接器族是 child-first，必须自带，不能连带删掉）。
   >
   > ⚠️ **测量陷阱**：验证这条时若用 `mvn -pl <module>` 而**不带 `-am`**，maven 会从
   > `~/.m2` 取**陈旧的** `fe-connector-spi` pom（其中还没有 `fe-filesystem-api` 这条依赖），
   > 于是 `fe-foundation` 看起来也一起消失了——那是假象。比对插件 zip 内容必须用带 `-am`
   > 的 reactor 构建。
   >
   > 另有一处**未改**：`"Provided by fe-core classloader (parent-first)"` 这句注释仍只出现在
   > 8 个中的 3 个（hudi/iceberg/paimon），纯措辞差异，无行为含义。

default 方法的字节码在接口的 class 文件里，而那份 class 来自内核。插件不 override，
`invokeinterface` 就解析到内核那份 default。于是插件"自称"的版本其实是**内核自己在说话**：

- 内核把 `CURRENT_API_VERSION` 提到 2，同时把 default 改成 2 → 所有旧插件跟着"自称 2"，全部放行；
- 只改 `CURRENT_API_VERSION` 不改 default → 所有插件（含新编译的）全自称 1，全部拒绝。

两种改法都是错的。**根因是版本号从未离开过内核**。

另外三族（FILESYSTEM / AUTHENTICATION / LINEAGE）**完全没有版本检查**。

`ManifestVersions` / `PluginRegistry` 读的 `Implementation-Version` 是纯展示数据
（喂 `information_schema.extensions`），不参与任何校验。

> **勘误（2026-07-29 实证）**：该属性**并非没人配**——`fe/pom.xml` 的父 pom 是
> `org.apache:apache:29`，其 `pluginManagement` 给 `maven-jar-plugin` 设了
> `addDefaultImplementationEntries=true`。实测 `output/fe/plugins` 下 8 个连接器 + 14 个
> filesystem 插件 jar **22/22 都带** `Implementation-Version: 1.2-SNAPSHOT`（= `<revision>`）。
> 所以 `extensions` 的 version 列今天不是 NULL，而是**全表同一个 FE 构建号**——信息量低，
> 但不是缺失。这条勘误作废了 §8 与 §13.1 的"顺带项"依据。

---

## 2. 目标与非目标

**目标**

- 版本号**物理上随插件产物走**，内核不能代答。
- 四族统一：机制族中立，四族全部接线。
- 生产路径（目录加载）fail-closed：不声明即拒绝。
- 拒绝要可诊断：日志带声明值、内核期望值、插件目录。

**非目标**

- 不做版本区间声明（插件只声明"我按哪个版本编译"，不声明"我要求内核 ≥ X"）。
- 不改 `information_schema.extensions` 的表结构（被拒插件本次只进日志）。
- 不为"忘记 bump major"做自动闭环——见 §7，说明为什么做不到。

---

## 3. 版本模型与判定规则

版本形如 `major.minor[.patch]`，**每族一个**，起始值四族均为 `1.0`。

### 3.1 判定

设内核期望版本为 K、插件声明版本为 P：

```
P.major != K.major   → 拒绝
否则                  → 放行（minor / patch 均忽略）
```

### 3.2 各段语义（bump 纪律）

| 段 | 含义 | 例子 |
| --- | --- | --- |
| **major** | 不兼容变化 | 删除接口、**新增**接口、修改接口参数 |
| **minor** | 兼容性改变 | 接口表面不变，内部实现变了 |
| **patch** | 兼容的 bugfix | 插件可不声明；校验忽略 |

此处"接口"指**插件与内核之间的 API 入口**，既包括接口类型本身（新增/删除一个
`interface`），也包括其上的方法（新增/删除一个方法、改其参数或返回类型）。判据是：
**SPI 表面是否发生任何变化**——是，就是 major；否，最多是 minor。

**为什么 minor 可以双向兼容**：因为任何 SPI 表面变化都归 major，minor 永远不改变插件能
看到的 API 集合。于是"新插件跑老内核"不可能调到内核没有的方法，两个方向都安全，无需额外
告警。这是"新增接口也算 major"这条定义换来的性质。

**代价**（需写进插件作者文档）：Doris 每次给 SPI 表面加东西都是 major 变更，所有已有插件
会被拒、必须重新编译。树内插件同批构建无影响；第三方插件作者要有此预期。

### 3.3 版本独立性与共享耦合

**四族的版本号彼此独立**：各有自己的 property、自己的 major，互不影响。改 `fe-connector-api`
只 bump CONNECTOR，filesystem / authentication / lineage 的插件一个都不用重编。

各族插件链接的"内核提供契约"（`plugin-zip.xml` 排除项 + parent-first 前缀共同决定）：

| 插件族 | 内核提供的契约 artifact |
| --- | --- |
| CONNECTOR | `fe-connector-api`、`fe-connector-spi`、**`fe-extension-spi`**、**`fe-filesystem-api`** |
| FILESYSTEM | `fe-filesystem-api`、`fe-filesystem-spi`、**`fe-extension-spi`** |
| AUTHENTICATION | `fe-authentication-api`、`fe-authentication-spi`、**`fe-extension-spi`** |
| LINEAGE | fe-core 的 `org.apache.doris.nereids.lineage` 包、**`fe-extension-spi`** |

由此得到"改什么要 bump 谁"：

| 改动了什么 | 要 bump 哪几族 |
| --- | --- |
| `fe-connector-api` / `fe-connector-spi` | 只 CONNECTOR |
| `fe-filesystem-spi` | 只 FILESYSTEM |
| `fe-authentication-api` / `fe-authentication-spi` | 只 AUTHENTICATION |
| fe-core 的 `nereids.lineage` 包 | 只 LINEAGE |
| **`fe-filesystem-api`** | FILESYSTEM **+ CONNECTOR** |
| **`fe-extension-spi`** | **四族全部** |

后两行是仅有的耦合，且它们是**物理事实，不是版本方案造成的**：

- `org.apache.doris.extension.spi.` 位于 `ChildFirstClassLoader.DEFAULT_PARENT_FIRST_PACKAGES`
  （`ChildFirstClassLoader.java:50`），对四族**强制** parent-first —— 四族插件都链接它；
- `fe-connector-hive` / `-iceberg` / `-paimon` 确有 `org.apache.doris.filesystem` 的 import
  （已验证），connector 插件真的用 filesystem 的类型。

即便给这两个 artifact 单独的版本号，"改了 `PluginFactory` 的参数 → 四族插件字节码层面全不兼容"
也不会变。版本号只能如实描述这个事实。

**处理方式（已决）**：保持 4 个版本号，共享 artifact 的耦合靠纪律 + §7 的基线提示。
`fe-extension-spi` 的 `Plugin` / `PluginFactory` / `PluginContext` 在四族基线里**都冻结**，
改它会让四个基线测试同时变红，失败信息提示"四个 property 都要 bump"。

**已知残留风险**：漏 bump 其中几族，那几族会静默放行。基线红会提醒，但不强制。接受此风险的
依据是 `fe-extension-spi` 表面极小（按录制基线实测：`Plugin` 2 个方法、`PluginFactory` 4 个、
`PluginContext` 1 个，合计 **7** 行；另有 `PluginException` 2 个构造器，构造器不进基线）
且是稳定的生命周期契约，实际变更频率极低。

### 3.4 解析规则

- 接受 `1`、`1.0`、`1.0.3` 三种写法；比较只取 major。
- 解析失败视为**未声明**，按 §5.2 的缺失策略处理（目录插件 → 拒绝）。
- minor / patch 虽不参与判定，仍写进 manifest：它们进 `information_schema.extensions`，
  运维排查有用；且将来若要收紧规则，数据已在。

---

## 4. 版本号的单一来源

朴素做法是"内核放 Java 常量 + 打包放 maven property + 写测试断言相等"。那是**两个数字靠
测试同步**，而本文 §1 的教训正是"约定失效但没人发现"。

本设计改为**一个数字物理分发到两处**：每族在自己的父 pom 定一个 property，同时流向

1. 该族 **SPI 模块**的一个 filtered resource → 内核启动时从自己 classpath 读出 K；
2. 各**插件 jar 的 MANIFEST** → 插件声明的 P。

同一次构建里两者必然相同（同一个 property，不可能不一致）；跨构建不同才正是要检测的东西。
**不需要同步测试，因为没有第二个数字可漂。**

```
fe/fe-connector/pom.xml
   <connector.plugin.api.version>1.0</connector.plugin.api.version>
        │
        ├──► fe-connector-spi/src/main/resources-filtered/META-INF/doris/
        │       connector-plugin-api-version.properties   （filtering=true）
        │       （单独一个 resources-filtered 目录：${...} 替换只作用于这个文件，
        │         绝不会误改同模块的 META-INF/services 描述符）
        │       → 内核读它 = K
        │
        └──► 父 pom 的 maven-jar-plugin <manifestEntries>（8 个连接器继承）
                Doris-Connector-Plugin-Api-Version: 1.0
                → 加载器从插件 jar 读它 = P
```

四族对照：

| 族 | property 落点 | filtered resource 落在 | manifest 属性名 |
| --- | --- | --- | --- |
| CONNECTOR | `fe/fe-connector/pom.xml` | `fe-connector-spi` | `Doris-Connector-Plugin-Api-Version` |
| FILESYSTEM | `fe/fe-filesystem/pom.xml` | `fe-filesystem-spi` | `Doris-Filesystem-Plugin-Api-Version` |
| AUTHENTICATION | `fe/fe-authentication/pom.xml` | `fe-authentication-spi` | `Doris-Authentication-Plugin-Api-Version` |
| LINEAGE | `fe/fe-core/pom.xml` | `fe-core`（SPI 就在它自己里） | `Doris-Lineage-Plugin-Api-Version` |

**注意**：CONNECTOR 族的"契约"横跨 4 个 artifact（`fe-connector-api`、`fe-connector-spi`、
`fe-extension-spi`、`fe-filesystem-api`），其中后两个在别的父 pom 下。改它们同样要 bump
connector 的 property——这是 §3.3 那条纪律的具体体现。

属性值读取用的是插件 zip 中**定义 provider 类的那个 jar**，
沿用 `ManifestVersions.jarOf()` 的既有逻辑（它已处理"同包不同 jar 时 `Package
.getImplementationVersion()` 会串味"的坑）。

---

## 5. 校验点与失败行为

### 5.1 校验点

放在 `DirectoryPluginRuntimeManager#loadAll` 内部，**工厂类实例化之后、`PluginHandle`
发布之前**。

> **实现时提前到了实例化之前**（见 §14）：`loadClass` 之后、`asSubclass` + `newInstance`
> 之前。两点好处——不兼容插件的构造器代码一行都不会跑；而且"按另一个 major 编译"的插件恰恰
> 是类型可能对不上的那个，先判版本才能给出带两个版本号的诊断，而不是一个
> `ClassCastException`。

- 族中立：gate 以参数传入（manifest 属性名 + 内核期望 major），loader 里零族专有代码。
- 拒绝时复用现有 `closeClassLoader`，不泄漏 classloader（与现有 name-conflict 拒绝路径
  的处理一致）。
- 失败表达：新增 `LoadFailure` stage `STAGE_API_VERSION`，走各族**已有**的失败日志路径，
  不新建通道。

### 5.2 缺失声明的策略：分路径

| 路径 | 策略 | 理由 |
| --- | --- | --- |
| 目录加载（`loadPlugins` / `loadAll`） | **缺声明即拒绝**（fail-closed） | 生产唯一路径（`build.sh:1072` 把连接器装到 `fe/plugins/connector/`，不上 classpath）。第三方想绕过只能主动谎报，而不是"什么都不写"。 |
| classpath / ServiceLoader（`loadBuiltins`） | **无条件放行** | 生产不存在该路径；开发/单测里它与内核同一次编译产出，天然兼容。且此时类常来自 `target/classes` 目录，`ManifestVersions.jarOf` 明确只认 `Files.isRegularFile`，本就读不到 manifest。 |

### 5.3 各族拒绝行为

| 族 | 行为 |
| --- | --- |
| CONNECTOR / FILESYSTEM / LINEAGE | 启动期加载 → 跳过该插件 + ERROR 日志。保持现有 partial-success 契约：一个坏插件不能挡住 FE 启动。 |
| AUTHENTICATION | 懒加载（`AuthenticationIntegrationRuntime#ensurePluginFactoryLoaded`）→ 被拒插件不进 `factories`，现有的 `"No authentication plugin factory found for type"` 会触发。**必须把版本原因带进那条 `AuthenticationException` 的消息**，否则用户只看到"找不到"，无从诊断。 |

### 5.4 内核期望值读不到

filtered resource 缺失或损坏 → **启动即失败**，不跳过。这是构建缺陷而非部署问题，
必须 fail loud。

---

## 6. 旧机制的删除（不并存）

新机制取代旧机制，两套并存只会制造困惑：

- 删 `ConnectorProvider#apiVersion()`（fe-connector-spi）；
- 删 `ConnectorPluginManager.CURRENT_API_VERSION` 及其三处比较
  （`createConnector` / `findProvider` / `validateProperties`）；
- 改写 `ConnectorPluginManagerTest` 中依赖它的 3 个用例
  （`testCompatibleApiVersionCreatesConnector` / `testIncompatibleApiVersionReturnsNull` /
  `testIncompatibleApiVersionValidateThrows`）及 `createProvider(type, apiVersion, ...)` 工厂。

**已核实**：`connector-metadata-methods.txt` 基线只冻结 `ConnectorMetadata` 及其 6 个 Ops
子接口（72 个方法），**不含 `ConnectorProvider`**，故删除该方法不需要刷新该基线。

---

## 7. 防漂移：能做到什么，做不到什么

新规矩是"SPI 表面一变就 major+1"。**谁保证有人真的 bump？**

**这条闭不了环，必须诚实记录原因**：任何单元测试只能看当前状态，看不到"相对上一次改了
什么"。即便基线文件里同时记录表面内容与 major，改了表面 → 测试红 → 刷新基线内容但不动
major 行、也不动 pom → 测试又绿。要真正闭合必须比较 base 与 head 两个版本，那是 CI diff
级别的检查，不是单测能做的事。

因此采用**强提示 + 评审**（符合既定规矩："已有单测证明该不变量时，优先 ATTN 注释 + 单测 +
评审，别硬上脆弱静态门禁"）：

1. 表面基线测试红 = 不可能忽略的信号，说明表面变了；
2. 测试失败信息**写死**那句话：*"这是 major 变更——刷新基线的同一个提交必须把
   `<family>.plugin.api.version` 的 major 加一"*；
3. 基线 diff 出现在 review 里，审阅者可直接对照 pom 有没有跟着改。

### 7.1 基线范围：四族各冻结顶层契约

只冻结"插件必须实现或调用"的顶层接口，**不做全表面**。冻结的正是"插件与内核之间的合同"，
与 major 的定义对齐；内部重构不会误报（全表面基线会高频误报 → 麻木 → 退化成仪式）。

| 族 | 冻结类型 |
| --- | --- |
| CONNECTOR | `ConnectorProvider`、`ConnectorContext`、`Connector`（+ 现有 `ConnectorMetadata` 基线保持不动） |
| FILESYSTEM | `FileSystemProvider`、`ObjFileSystem`、`ObjStorage` |
| AUTHENTICATION | `AuthenticationPluginFactory`、`AuthenticationPlugin` |
| LINEAGE | `LineagePluginFactory`、`LineagePlugin` |
| 四族共享 | `fe-extension-spi` 的 `Plugin`、`PluginFactory`、`PluginContext` |

实现范式沿用现有 `ConnectorMetadataSurfaceTest`：反射取公开方法签名 → 与 `src/test/
resources` 下的录制基线逐字节比对。

---

## 8. 可见性

被拒插件本次**只做日志**：ERROR 级，带插件目录、声明值、内核期望值。不改
`information_schema.extensions` 表结构。

~~**待定的顺带项**：`extensions` 已有 version 列读 `Implementation-Version`，但所有 pom 都
没配 manifest，该列今天大概率全 NULL。既然已要在两个父 pom 加 `<manifestEntries>`，多加一行
`Implementation-Version` 几乎零成本。但这属 adjacent 改进，**默认不做**，待 owner 明示。~~

**已作废（实现阶段实证）**：前提是错的——见 §1 的勘误。该属性已由 ASF 父 pom
（`org.apache:apache:29` 的 `addDefaultImplementationEntries=true`）自动写入**每一个** FE jar，
22/22 插件 jar 实测都有。再加一行 `<Implementation-Version>${revision}</Implementation-Version>`
是纯 no-op。真正的（潜在）缺陷换了一副面孔：该列对所有插件恒等于 FE 构建号 `1.2-SNAPSHOT`，
**不是**插件自己的发布版本，因此对运维几乎无鉴别力。要改善得让各插件声明**自己的**版本，
那是另一个议题，本次不做。

---

## 9. 测试策略

| 层 | 内容 |
| --- | --- |
| `ApiVersionGate` 单测 | major 相等/不等、minor 双向、patch 有无、只有 major、缺声明、格式非法 |
| 加载器级（每族一个） | 造带/不带 manifest 属性的临时 jar：断言目录插件被拒、classpath 内建被豁免、classloader 被关闭、`LoadFailure` stage 正确 |
| AUTHENTICATION 专项 | 断言拒绝原因带进 `AuthenticationException` 消息，未退化成"找不到该类型" |
| 表面基线（四族） | §7.1 的五组录制基线 |
| 回归 | 删除旧 `apiVersion()` 后改写的 3 个 `ConnectorPluginManagerTest` 用例 |

---

## 10. 改动清单

**构建侧**

- `fe/fe-connector/pom.xml`：加 property + `maven-jar-plugin` `<manifestEntries>`（8 个连接器继承）
- `fe/fe-filesystem/pom.xml`：同上（14 个 filesystem 插件继承）
- `fe/fe-authentication/pom.xml`：加 property（树内 0 个 plugin-zip，仅供第三方）
- `fe/fe-core/pom.xml`：加 property（LINEAGE，树内 0 个实现，仅供第三方）
- 4 个 SPI 模块加 resource filtering + 单行 properties 文件

**内核侧**

- `fe-extension-loader`：新增 `ApiVersionGate`（族中立）、`LoadFailure.STAGE_API_VERSION`、
  `DirectoryPluginRuntimeManager#loadAll` 增加 gate 参数
- `ConnectorPluginManager` / `FileSystemPluginManager` / `LineageEventProcessor` /
  `AuthenticationPluginManager`：各自接线
- `AuthenticationIntegrationRuntime`：把版本拒绝原因带进异常消息

**删除**

- `ConnectorProvider#apiVersion()`
- `ConnectorPluginManager.CURRENT_API_VERSION` 及三处比较

---

## 11. 已决取舍记录

| 议题 | 决定 | 否决项及理由 |
| --- | --- | --- |
| 发布模型 | 允许插件独立发版 | —— |
| 版本来源 | jar MANIFEST 属性 | **编译期常量内联**：忘写 override 就静默退回内核 default，今天这个 bug 原样复发。**改抽象方法**：老插件抛 `AbstractMethodError`（崩溃而非优雅拒绝），且引入机制本身就是一次破坏性变更。 |
| 缺声明策略 | 目录插件拒绝，classpath 豁免 | **一律拒绝**：单测类在 `target/classes` 无 jar，会全红，被迫开测试旁路——旁路本身就是绕过口。**缺失放行**：不写就永远不受检，与今天"恒真"同构。 |
| 覆盖范围 | 四族全接 | —— |
| 判定规则 | major 相等，minor/patch 忽略 | **单向 `P.minor <= K.minor`**：owner 明确要求 minor 双向兼容。 |
| 单一来源 | property → filtered resource + manifest | **Java 常量 + 相等性测试**：两个数字靠测试同步，与本文 §1 的失败模式同构。 |
| 基线范围 | 四族各冻结顶层契约 | **只加提示**：另三族改了表面无任何信号。**全表面**：高频误报致麻木失效。 |
| 版本粒度 | 每族一个，共 4 个（见 §3.3） | **共用一个版本号**：改一族会逼另三族全部重编，owner 明确要求分开。 |
| 共享 artifact 耦合 | 靠纪律 + 基线提示 | **给 `fe-extension-spi` 单独第 5 个版本号**：能让"改 extension → 四族自动全拒"不依赖人记得，但插件作者要声明两个数字；因该 artifact 表面极小、极少变更，判定不值这个复杂度。**按 artifact 逐个版本化**：connector 插件要声明 4 个数字，实质是在造小型 OSGi。 |

---

## 12. 版本变更流程（runbook）

以 CONNECTOR 族为例，其余三族把 `connector` 换成对应族名即可。

### 12.1 第一步：判定 major 还是 minor

```
改动是否让 SPI 表面发生任何变化？
（新增/删除接口类型、新增/删除方法、改参数或返回类型）
   │
   ├─ 是 ──► major + 1，minor 归零        1.3 → 2.0
   │
   └─ 否 ──► 接口表面不变，只是实现变了？
              │
              ├─ 是（行为/性能/内部重构）──► minor + 1     1.3 → 1.4
              │
              └─ 只是 bugfix，行为不变 ──► patch + 1（可选）1.3 → 1.3.1
```

判据只有一条：**SPI 表面动没动**。拿不准时看表面基线测试红不红——它红了就是 major。

### 12.2 Doris 侧：一次 major 变更的完整步骤

1. 改 SPI 代码，例如给 `ConnectorProvider` 加一个方法。
2. 跑该模块测试 → 表面基线测试变红，失败信息提示"这是 major 变更"。
3. 刷新基线文件（把失败信息里的 actual 集合抄进 `src/test/resources` 下对应的 `.txt`）。
4. **bump 一个数字**：`fe/fe-connector/pom.xml` 的
   `<connector.plugin.api.version>` 由 `1.3` 改成 `2.0`。
5. 重新构建。这一步之后**自动发生**两件事，无需人工干预：
   - `fe-connector-spi` 的 filtered resource 变成 `2.0` → 内核期望值更新；
   - 8 个连接器 jar 的 MANIFEST 变成 `2.0` → 插件声明值更新（它们继承父 pom）。
6. 全反应堆验证。**用 `package` 不要用 `test-compile`**（见 §14.10：冷缓存下 `test-compile`
   拿不到 shade 产物）：
   ```bash
   mvn -o -f fe/pom.xml -Dmaven.build.cache.enabled=false \
       -Dcheckstyle.skip=true -Dexec.skip=true -DskipTests package
   ```
   禁缓存这一条对**本流程尤其关键**：bump 版本号时构建缓存正是最容易骗过你的东西（§14.8c）。

**树内 8 个连接器的 pom 一个都不用改。** 这是 property 继承的直接结果——版本号只在父 pom
里出现一次。

唯一需要改连接器代码的情况：新增的 SPI 方法是**抽象**的（无 default）。那是编译错误驱动的
代码改动，与版本机制无关。

### 12.3 Doris 侧：minor / patch 变更

只改 §12.1 判出的那一位数字，其余同 §12.2 的第 5、6 步。表面基线不会红（表面没变），
所以**没有自动信号提醒你 bump minor**——minor 不参与校验，漏 bump 只影响
`information_schema.extensions` 的展示，不影响正确性。

### 12.4 第三方插件：要做什么

**查出内核期望哪个版本**，两个途径：

```bash
# 途径一：读 SPI jar 里的声明
unzip -p fe-connector-spi-*.jar META-INF/doris/connector-plugin-api-version.properties

# 途径二：被拒时 FE 日志的 ERROR 行会同时打印声明值与内核期望值
```

**在插件 pom 里声明**（首次接入时加，之后每次 Doris major 变更时更新这一个数字）：

```xml
<properties>
  <!-- 取值 = 你编译所用 fe-connector-spi 版本对应的 connector.plugin.api.version -->
  <doris.connector.plugin.api.version>1.0</doris.connector.plugin.api.version>
</properties>

<build>
  <plugins>
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-jar-plugin</artifactId>
      <configuration>
        <archive>
          <manifestEntries>
            <Doris-Connector-Plugin-Api-Version>${doris.connector.plugin.api.version}</Doris-Connector-Plugin-Api-Version>
          </manifestEntries>
        </archive>
      </configuration>
    </plugin>
  </plugins>
</build>
```

**打包时必须排除**内核提供的契约 artifact（否则插件 zip 里会带重复副本）：
`fe-connector-api`、`fe-connector-spi`、`fe-extension-spi`、`fe-filesystem-api`。
参照树内**任一**连接器的 `src/main/assembly/plugin-zip.xml`——8 个现在一致
（es / trino 原本漏了 `fe-filesystem-api`，已在本次修复，见 §1 勘误）。

**Doris 升 major 之后的适配顺序**：

1. 依赖升到新版 `fe-connector-spi` / `fe-connector-api`；
2. 修编译错误（新 major 可能删了或改了你用的接口）；
3. 把 pom 里那个 property 改成新值；
4. 重新打包、替换 plugin 目录、重启 FE。

**写错版本号会怎样**：写小了（声明 1.0、实际按 2.0 编译）→ 被拒，安全失败。写大了
（声明 2.0、实际按 1.0 编译）→ 放行，但运行期可能报 `NoSuchMethodError`。后者属于主动
谎报，本机制不防；§5.2 的 fail-closed 只保证"什么都不写"不能通过。

### 12.5 部署与运维

- **内核与插件必须同批升级**。major 严格相等，没有过渡期、没有兼容窗口。
- 树内插件天然同步：`build.sh` 把它们装进 `output/fe/plugins/<family>/`，与 FE 同批产出。
- 升级前的检查清单：所有**第三方**插件是否已有对应新 major 的版本。
- 升级后的验证：
  - 查 FE 日志有无 `STAGE_API_VERSION` 的拒绝记录；
  - 查 `information_schema.extensions`，**被拒的插件不会出现在这张表里**——某个插件消失
    就是它被拒了。

### 12.6 诊断：插件没加载出来

| 症状 | 查什么 |
| --- | --- |
| `CREATE CATALOG` 报未知类型 | FE 日志搜该插件目录名 + `STAGE_API_VERSION` |
| `extensions` 表里少了某插件 | 同上；ERROR 行会打印声明值 vs 内核期望值 |
| 认证类型报 "No authentication plugin factory found" | 该消息已带版本拒绝原因（§5.3）；否则是真的没装 |
| 日志说"缺少版本声明" | 插件 jar 的 MANIFEST 没有对应属性，按 §12.4 补 |

---

## 13. 未决 / 后续

1. ~~§8 的 `Implementation-Version` 顺带项，待 owner 明示做或不做。~~ **已关闭**：前提被实证
   推翻（ASF 父 pom 已自动写入，22/22 插件 jar 实测都有），加了是 no-op。见 §8。
2. 被拒插件是否要出现在 `information_schema.extensions`（带状态列）——本次不做，
   若运维反馈诊断困难再议。
3. 第三方插件作者文档：需说明 §3.2 的 bump 纪律与"SPI 表面变化必须重编"的预期，
   §12.4 的 pom 片段可直接作为素材。
4. 考虑发布一个插件 parent pom（如 `doris-connector-plugin-parent`），预置 property 与
   `manifestEntries` 配置，第三方继承即自动正确，免去 §12.4 手写和写错的风险。
   Trino 的 `trino-plugin` packaging 是同一思路。本次不做——它是新增发布物，
   且要先有真实的第三方插件需求才能定其形态。

---

## 14. 实现记录（2026-07-29 完成）

状态：**已实现并验证**。以下是实现相对本设计的偏差与理由，凡与设计不同的都在此列明。

### 14.1 与设计一致的部分（不赘述）

§3 判定规则、§4 单一来源（property → filtered resource + manifest）、§5 校验点与 fail-closed、
§6 旧机制删除、§7.1 四族基线冻结范围、§10 改动清单，均按设计落地。四族版本号起始 `1.0`。
~~AUTHENTICATION / LINEAGE 按设计**只加 property、不加 `<manifestEntries>`**~~ —— **这条已在
§14.8c 推翻**：不加 `<manifestEntries>` 会让版本号从 maven build-cache 的哈希输入里消失，
bump 之后 `clean package` 会恢复出带旧版本的陈旧 jar（已复现）。四族现在**一律**都加。

### 14.2 偏差一：门禁按"族名"派生，而不是传三个字面量

`ApiVersionGate.forFamily(String family, Class<?> spiAnchor)` 只收族名（`connector`）与
SPI 锚点类型，资源路径与 manifest 属性名按约定派生：

```
family = "connector"
  → 资源 /META-INF/doris/connector-plugin-api-version.properties  （键统一为 api.version）
  → 属性 Doris-Connector-Plugin-Api-Version
```

理由：设计要求 loader 族中立，而"族中立"用一条成文约定表达比在四个管理器里散落 12 个字面量
更难写错。代价是 `grep Doris-Connector-Plugin-Api-Version` 只能命中 pom，命不中 Java；用
`ApiVersionGateTest` + `PluginApiVersionWiringTest` 把四个派生结果**逐字钉死**补偿。

**残留人工环节**：pom 里的 `<Doris-Connector-Plugin-Api-Version>` 是 XML 元素名，无法由属性
插值，因此它与派生规则的一致性没有任何构建期检查。两处测试把期望字面量写出来供评审对照。

### 14.3 偏差二：`loadAll` 的门禁参数是**必填**（owner 签字）

`DirectoryPluginRuntimeManager#loadAll` 由 4 参改 5 参，`Objects.requireNonNull(apiVersionGate)`。
否决了"保留 4 参重载、gate 可空"——可空重载等于留了一个"不传就不校验"的静默旁路，正是 §1
要消灭的失效模式。代价是改了 6 处调用点（4 个生产管理器 + 2 个测试，含
`AuthenticationPluginManagerTest.StaticRuntimeManager` 的 override）。

### 14.4 偏差三：表面基线**记录返回类型**（owner 签字）

行格式 `<冻结类型>#<方法名>(<参数类型>):<返回类型>`，与既有
`connector-metadata-methods.txt`（只有 `名(参数)`）不同。理由：§3.2 把"改返回类型"定义为
major，不记返回类型就漏掉这一整类。既有那个基线**本次不动**。

另一个取舍：每行以**被冻结的根类型**为键，而不是 `getDeclaringClass()`。即
`ConnectorProvider#description():java.lang.String` 与
`PluginFactory#description():java.lang.String` 同时出现。这样"插件在这个类型上能调到什么"
被完整钉住，而把 default 方法在父接口链上挪位置（对实现方无感）不会误报。

四份基线规模：connector 49 行、filesystem 64 行、authentication 22 行、lineage 16 行，
其中 fe-extension-spi 的 `Plugin`/`PluginFactory`/`PluginContext` 共 **7** 行**四份都有**
（四份基线取交集实测正好这 7 行）——改它们四个基线同时红，各自要求 bump 自己那个 property，
即 §3.3 要的效果。

### 14.5 偏差四：测试分层与设计 §9 不同

设计 §9 要求"加载器级（每族一个）"造临时 jar。实现改为：

| 层 | 位置 | 内容 |
| --- | --- | --- |
| 判定规则 | `ApiVersionGateTest`（fe-extension-loader，13 例） | major 相等/不等、minor 双向、patch 有无、只有 major、缺声明、6 种畸形值、内核资源缺失、内核资源**未被 filtering**（`${...}` 原样） |
| 加载器行为（族中立，测一次） | `DirectoryPluginRuntimeManagerApiVersionTest`（6 例） | 目录插件被收/被拒、stage=`apiVersion`、消息含声明值与期望值、拒绝后无残留且可重复、gate 必填 |
| 族接线（端到端真 jar） | `PluginApiVersionWiringTest`（fe-core，7 例） | CONNECTOR / FILESYSTEM 各测"版本对→进路由表""major 不同→不进""不声明→不进"，外加四族属性名/资源相互独立 |
| AUTHENTICATION 专项 | `AuthenticationPluginManagerTest` +3 例 | 拒绝原因进 `apiVersionRejectionHint()`、进 `AuthenticationException`、成功加载后提示被清空 |
| 表面基线 | 四族各一个 | §14.4 |

不给四族各写一份加载器级测试，是因为 loader 里零族专有代码——四族跑的是同一段字节码，
重复四遍只是重复测同一件事。族**独有**的是"有没有真的传门禁""门禁指向自己的资源"，
这些由族接线测试覆盖。

LINEAGE 只做到门禁接线断言，没有端到端目录加载：`LineageEventProcessor#discoverPlugins`
是私有的、且与 `Config.plugin_dir` + 工作线程耦合，树内又是 0 个实现。

**明确没做到的一条**：设计 §9 要求断言"classloader 被关闭"。被拒插件的 classloader 不会
交还给调用方，从测试里不可观测。它挂在与其他所有"创建 classloader 之后被拒"路径共用的
`catch (PluginLoadException)` 上（代码可见，测试不可见）。已在测试 javadoc 里写明。

### 14.5b 偏差：校验点提前到实例化之前

设计 §5.1 写的是"工厂类实例化之后"，实现放在 `classLoader.loadClass()` 之后、
`asSubclass()` + `newInstance()` 之前。不兼容插件的构造器一行不跑；且"按另一个 major 编译"的
插件正是类型可能对不上的那个，先判版本才能给出带两个版本号的诊断而不是 `ClassCastException`
（后者会逃出 `loadAll` 的 `catch (PluginLoadException)`，是一条既有的、本次未触碰的裸奔路径）。

### 14.6 偏差五：AUTHENTICATION 有**两处**降级点，都改了（owner 签字）

设计 §5.3 只点名 `AuthenticationIntegrationRuntime#ensurePluginFactoryLoaded`。实际还有
`AuthenticationPluginAuthenticator#ensurePluginFactoryLoaded`（fe-core，MySQL 握手认证路径），
同样退化成 `No AuthenticationPluginFactory found for plugin: <type>`。两处都追加
`AuthenticationPluginManager#apiVersionRejectionHint()`。

### 14.7 连带的语义变化：只带 service 描述符的插件 jar 现在会被拒

版本值取自**定义 provider 类的那个 jar**（设计 §4）。因此一个只放了
`META-INF/services/...`、实现类却来自 FE 自身 classpath 的插件目录，现在"什么都没声明"→被拒。
生产不存在这种布局（插件不在 classpath 上），但
`AuthenticationPluginManagerTest.createServiceOnlyJar` 造的正是这种 jar，已改为同时写入
类字节 + manifest，与真实产物一致。

### 14.8 `ConnectorPluginManagerTest` 的处置

- `testCompatibleApiVersionCreatesConnector` → 改名 `testRegisteredProviderCreatesConnector`（保留创建路径覆盖）
- `testIncompatibleApiVersionReturnsNull` → 删（版本判定已移到加载期，由上表的测试覆盖）
- `testIncompatibleApiVersionValidateThrows` → 换成 `testValidatePropertiesDelegatesToTheMatchingProvider`（保留 `validateProperties` 覆盖，断言错误来自 provider 自己）
- `testFallsBackToCompatibleProvider` → **删**（设计未点名；去掉版本后它与第一条逐字重复，且"回退"这个名字已不成立；provider 优先级已由 `testRegisterProviderOverridesDiscovered` 覆盖）
- `createProvider(type, apiVersion, ...)` → 去掉 `apiVersion` 参数

### 14.8b 顺带修复：es / trino 的 plugin-zip 补齐 `fe-filesystem-api` 排除

owner 在实现中途要求一并做。改动 = 两个 `plugin-zip.xml` 各加一行
`<exclude>org.apache.doris:fe-filesystem-api</exclude>` + 对齐头注释。
安全性与验证方式见 §1 勘误（含"不带 `-am` 会测出假象"那条陷阱）。
结果：8/8 连接器 zip 的 `lib/` 里 `fe-filesystem-api` 均已消失、`fe-foundation` 均保留。

### 14.8c 复审确认项一：maven build-cache 让 AUTHENTICATION / LINEAGE 的 bump 失效（已修）

对抗复审的多数票**判错了**这条，是唯一动手复现的那个 refuter 救回来的；我自己又复现了一遍：

```
# 修复前，只改 fe/fe-authentication/pom.xml 的 1.0 -> 2.0，其余不动
mvn -o -pl fe-authentication/fe-authentication-spi -am clean package
  RUN A (1.0): XX checksum [4c1d34a6e04836c6] -> jar 里 api.version=1.0
  RUN B (2.0): XX checksum [4c1d34a6e04836c6] -> jar 里 api.version=1.0   ← 陈旧！
```

根因：`maven-build-cache-extension` 的 key-pom 只含 `<dependencies>` 与 `<build><plugins>`，
**不含 `<properties>`**；而 filtered resource 的**源文件**内容是字面量 `${...}` 占位符，
哈希永不变。于是 property 单独变化时模块 checksum 纹丝不动，`clean package` 直接恢复缓存产物。

CONNECTOR / FILESYSTEM 侥幸无事，纯粹因为它们的属性值被插值进了 `<build><plugins>`
（maven-jar-plugin 的 `<manifestEntries>`）——那是被哈希的输入。

**修法**：给 AUTHENTICATION 与 LINEAGE 也加上 `<manifestEntries>`。这就推翻了 §10 与 §14.1 里
"这两族只加 property、不加 manifestEntries" 的原始决定——那个决定在功能上说得通（树内 0 个插件
zip，没人读这个属性），但它同时把版本号从构建缓存的哈希输入里踢了出去。顺带三个好处：四族
彻底对称；第一个树内 auth/lineage 插件 zip 出现时无需改 pom；第三方也能从产物里直接读到。

修复后同样的 A/B：checksum `a388a287b5a7e172` → `cfb1d83e91b92b98`，jar 内 resource 与 MANIFEST
双双跟随 1.0 → 2.0。

### 14.8d 复审确认项二：ASF license header 门禁（已修）

四份表面基线 `.txt` **不能**加 header——各 `*PluginSurfaceTest.readBaseline()` 把每一非空行都当
签名读，header 会变成 16 行幽灵签名让测试变红。沿用分支上的既有先例（commit `37f6087e2ba`
对 `connector-metadata-methods.txt` 的处置），把四份基线加进 `.licenserc.yaml` 的 `paths-ignore`
并写明原因；fe-extension-loader 下两个测试用 `.properties` 则正常补 `#` 开头的 ASF header
（`Properties.load` 忽略注释行，加完测试仍绿）。

### 14.9 验证

- fe-extension-loader / fe-connector-spi / fe-filesystem-spi / fe-authentication-spi /
  fe-authentication-handler：全模块测试 BUILD SUCCESS
- fe-core 定向（一次 `-Dtest=...` 选择性运行，surefire 汇总 56 例全绿）：
  `PluginApiVersionWiringTest`(7) `LineagePluginSurfaceTest`(1) `ConnectorPluginManagerTest`(14)
  `AuthenticationIntegrationRuntimeTest`(9) `AuthenticationPluginAuthenticatorTest`(4)
  `LineageEventProcessorTest`(16) `FileSystemPluginManagerTest`(5)。
  同一条命令还在 **fe-authentication-handler** 模块（不是 fe-core）跑到了
  `AuthenticationServiceTest` 的 3 例——该类共 32 例，这次只匹配到 3 例，故不计入上面的 56
- **全反应堆 `package`（74 模块、`-Dmaven.build.cache.enabled=false`、测试源全部编译）：BUILD SUCCESS**
- **对抗复审**：7 视角独立找问题 + 每条 3 个 refuter，16 条候选 / 15 条被推翻 / 1 条确认
  （license header，§14.8d）；另有 1 条被多数票误杀但经实测复现后采纳（build-cache，§14.8c）
- **checkstyle**：6 个改动模块各 `0 Checkstyle violations`
- **真实产物实测**：`doris-fe-connector-es.jar` 的 MANIFEST 有
  `Doris-Connector-Plugin-Api-Version: 1.0`（且 ASF 父 pom 的 `Implementation-*` 条目仍在，
  证明 configuration 是合并不是覆盖）；`doris-fe-filesystem-local.jar` 有
  `Doris-Filesystem-Plugin-Api-Version: 1.0`；`doris-fe-connector-spi.jar` 里
  `META-INF/doris/connector-plugin-api-version.properties` 的值是 `api.version=1.0` 而非
  `${...}`，四族 filtered resource 全部实测已替换

### 14.10 已知的坑（给后来者）

全反应堆 **`test-compile` 在冷缓存下会失败**，与本次改动无关：`fe-connector-hms` 编译需要
`fe-connector-hms-hive-shade` 的 **shade 产物**，而 maven-shade-plugin 绑在 `package` 阶段，
`test-compile` 根本不产那个 jar（`ThriftHmsClient` 报一片 `org.apache.hadoop.hive.*`
cannot find symbol）。

**说清楚触发条件**：仓库默认开着 maven-build-cache（`fe/.mvn/maven-build-cache-config.xml`），
它会在 `test-compile` 期间把 shade 模块 `package` 阶段的 jar 从缓存**恢复**到 `target/`，于是
缓存热的时候 `test-compile` 是能过的。只有缓存冷、或显式 `-Dmaven.build.cache.enabled=false`
时才暴露这个相位断层——本次 §14.9 的全量验证正是用禁缓存跑的，所以撞上了。

结论：全量验证请用
`mvn -o -f fe/pom.xml -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true -Dexec.skip=true -DskipTests package`
（`-DskipTests` 仍编译测试源，只是不执行；禁缓存是为了不让缓存掩盖真实产物问题）。
