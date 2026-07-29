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
依据是 `fe-extension-spi` 表面极小（`Plugin` 2 个方法、`PluginFactory` 3 个、`PluginContext`
2 个、`PluginException` 2 个构造器）且是稳定的生命周期契约，实际变更频率极低。

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
6. 全反应堆 `test-compile` 验证（记得 `-Dcheckstyle.skip=true`）。

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
参照树内任一连接器的 `src/main/assembly/plugin-zip.xml`。

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

1. §8 的 `Implementation-Version` 顺带项，待 owner 明示做或不做。
2. 被拒插件是否要出现在 `information_schema.extensions`（带状态列）——本次不做，
   若运维反馈诊断困难再议。
3. 第三方插件作者文档：需说明 §3.2 的 bump 纪律与"SPI 表面变化必须重编"的预期，
   §12.4 的 pom 片段可直接作为素材。
4. 考虑发布一个插件 parent pom（如 `doris-connector-plugin-parent`），预置 property 与
   `manifestEntries` 配置，第三方继承即自动正确，免去 §12.4 手写和写错的风险。
   Trino 的 `trino-plugin` packaging 是同一思路。本次不做——它是新增发布物，
   且要先有真实的第三方插件需求才能定其形态。
