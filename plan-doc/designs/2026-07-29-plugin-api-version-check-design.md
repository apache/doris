# 插件 API 版本检查设计（四族统一）

- 日期：2026-07-29
- 状态：设计已确认，待实现
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
   `fe-extension-spi` / `fe-filesystem-api`，注释写明 "Provided by fe-core classloader
   (parent-first)" —— 插件 zip 里根本没有 SPI 的字节码。

default 方法的字节码在接口的 class 文件里，而那份 class 来自内核。插件不 override，
`invokeinterface` 就解析到内核那份 default。于是插件"自称"的版本其实是**内核自己在说话**：

- 内核把 `CURRENT_API_VERSION` 提到 2，同时把 default 改成 2 → 所有旧插件跟着"自称 2"，全部放行；
- 只改 `CURRENT_API_VERSION` 不改 default → 所有插件（含新编译的）全自称 1，全部拒绝。

两种改法都是错的。**根因是版本号从未离开过内核**。

另外三族（FILESYSTEM / AUTHENTICATION / LINEAGE）**完全没有版本检查**。

`ManifestVersions` / `PluginRegistry` 读的 `Implementation-Version` 是纯展示数据
（喂 `information_schema.extensions`），不参与任何校验。

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

### 3.3 跨族耦合

`fe-extension-spi`（`Plugin` / `PluginFactory` / `PluginContext` / `PluginException`）被
四族共用。**它一改，四个 property 全要 bump major。** 这条无法靠 pom 结构强制，只能靠纪律
加 §7 的基线提示。

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
        ├──► fe-connector-spi/src/main/resources/META-INF/doris/
        │       connector-plugin-api-version.properties   （filtering=true）
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

**待定的顺带项**：`extensions` 已有 version 列读 `Implementation-Version`，但所有 pom 都
没配 manifest，该列今天大概率全 NULL。既然已要在两个父 pom 加 `<manifestEntries>`，多加一行
`Implementation-Version` 几乎零成本。但这属 adjacent 改进，**默认不做**，待 owner 明示。

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

---

## 12. 未决 / 后续

1. §8 的 `Implementation-Version` 顺带项，待 owner 明示做或不做。
2. 被拒插件是否要出现在 `information_schema.extensions`（带状态列）——本次不做，
   若运维反馈诊断困难再议。
3. 第三方插件作者文档：需说明 §3.2 的 bump 纪律与"SPI 表面变化必须重编"的预期。
