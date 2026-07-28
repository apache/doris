# 需要拍板的决策清单

> 只放**动手前必须先定**的事。每条给出背景、选项、代价、推荐值。
> 拍板后**就地**标 ✅ 并写明结论与日期，**不要删除条目**（历史要留痕）。
> 细节与证据在 [`design.md`](./design.md)，这里不重复。
>
> 下面条目里的「我」指做调研的那个 session，「你」指做决定的人。

---

## ⬜ OD-1 —— 删掉 metastore 后，null-supplier 分支该 fail-loud 还是 fail-silent？

**阻塞**：[`tasklist.md`](./tasklist.md) **FPC-03**。不定这条就不能写 FPC-03 的代码。

### 背景（先讲清楚在说什么）

`CatalogProperty` 有个方法叫 `resolveDerivedStorageDefaults()`，作用是**给存储配置补默认值**。
比如你建了个 iceberg hadoop 目录，只写了 `warehouse=hdfs://myns/wh`，没写 `fs.defaultFS`，
那么系统要能自己推出 `fs.defaultFS=hdfs://myns` —— 这个推导就叫「派生存储默认值」。

今天它的逻辑是**二选一**：

```java
if (pluginSupplier != null) {
    return pluginSupplier.get();     // ← 路 A：问连接器要（插件目录走这条）
}
return getMetastoreProperties().getDerivedStorageProperties();   // ← 路 B：fe-core 自己算（要删的）
```

FPC-03 要删掉路 B。问题是：**路 A 的 `pluginSupplier` 为 null 时怎么办？**

### 为什么会有「supplier 为 null」这种时刻

`PluginDrivenExternalCatalog` 的初始化顺序是：

```
:150  createConnectorFromProperties()      ← 先造连接器
        └─ :206-208 造 DefaultConnectorContext，此时已经把 catalogProperty 的
                     storage supplier 接上去了
:177  setPluginDerivedStorageDefaultsSupplier(...)   ← 后装派生 supplier
```

也就是说，**在第 150 行到第 177 行之间存在一个窗口**：context 已经能访问 storage 了，
但派生 supplier 还是 null。

**这个窗口今天走不进去**（我逐个复核过）：
- `PaimonConnector` 构造函数（`:151-166`）只传了个方法引用 `this::pluginAuthenticator`，
  那是**惰性**的（`:174-186` double-check 之后才真正计算），构造时不碰 storage
- `IcebergConnector` 构造函数（`:215+`）同样
- `HiveConnector` / `HudiConnector` 构造函数里**没有任何** `storage()` 调用
- `validateProperties` 显式传 `Collections.emptyMap()`，javadoc 明写「验证不需要 storage」

**所以这不是一个今天存在的 bug，而是一个「万一将来有连接器在构造期碰 storage，会怎样」的问题。**

### 关键：删除会把「响亮报错」变成「静默出错」

| | supplier 为 null 时会发生什么 |
|---|---|
| **今天** | 走路 B → `MetastoreProperties.create(props)` → 注册表里没有 iceberg/paimon/hms 的工厂 → **抛 `IllegalArgumentException`**，FE 日志里响亮报错 |
| **删完之后（若写 `return emptyMap()`）** | 静默返回空 → 丢掉 `warehouse → fs.defaultFS` 的桥接 → **而且**因为 `setPluginDerivedStorageDefaultsSupplier`（`:279-281`）**故意不重置缓存**，这个错误的 `StorageBindings` 会被**永久缓存**到下次 ALTER |

一个 HA nameservice 的 hadoop iceberg 目录会因此**绑不上 HDFS**，而且不报错。

### 三个选项

| | 做法 | 代价 |
|---|---|---|
| **A（推荐）** | null 分支写 `throw new IllegalStateException("...")` | **精确保留今天的行为**（今天走到这里就是抛）。不新增 fe-core 能力（是替换现有 throw，不是发明新逻辑），守铁律 A。守 Rule 12「fail loud」。<br>⚠️ 唯一不精确之处：今天属性图为**空**时 `getMetastoreProperties()` 返回 null 而**不抛**（`:327-329`），最终得到 `emptyMap`；选 A 会变成抛。但空属性图的目录本来也没有存储可派生，且 `RemoteDorisExternalCatalog`/`TestExternalCatalog` 根本不走 storage 路径 |
| **B** | null 分支写 `return Collections.emptyMap()` | 代码最简，但**把响亮失败变成静默降级**。违 Rule 12。今天不可达，但一旦将来某个连接器在构造期碰 storage，这就是个查半天的幽灵 bug |
| **C（不推荐）** | 照调研报告原方案，把 `setPluginDerivedStorageDefaultsSupplier` 那段**语句提前**到造连接器之前 | **修不干净**：提前之后 lambda 捕获的 `connector` 字段**仍然是旧值/null**（`connector = newConnector` 发生在 `createConnectorFromProperties()` 返回之后），所以窗口内照样拿到 `emptyMap`。是 churn 不是修复。<br>⚠️ 这是调研报告把它列为 HIGH 风险前置项的方案，我复核后判定**无效**，特此记下免得下次又被报告带偏 |

### 我的推荐

**选 A。** 理由：它是唯一「零行为变更 + 守 fail-loud」的写法，代码量和 B 一样是一行，
而且把一个**今天靠人工审计才知道不可达**的窗口，变成**万一走进去会立刻自曝**。

> **拍板结果**：⏳ **按推荐值 A 执行，待用户追认**。2026-07-28 用户指示「直接开始编码」但未就本条表态，
> 遂按文档推荐值 A（`throw new IllegalStateException`）落地于 FPC-03（commit 见 `progress.md`）。
> **要翻成 B 只需改一行** —— `CatalogProperty.resolveDerivedStorageDefaults()` 的 null 分支改成
> `return Collections.emptyMap();`，并同步删掉 `CatalogPropertyPluginStorageDerivationTest`
> 的 `unwiredSupplierFailsLoudInsteadOfDerivingNothing` 用例。
> **日期**：2026-07-28

---

## ⬜ OD-2 —— FPC-02（删 AWS 死构造臂）做不做？

**不阻塞任何其它任务**，可以随时决定，也可以永远不做。

### 背景

`StorageAdapter.getAwsCredentialsProvider()`（`:383-458`，含两个私有 helper）和
`AwsCredentialsProviderFactory` 的 `createV2` / `createDefaultV2` / 单参 `getV2ClassName`
加起来约 **146 行，零调用者**（我复核过：全仓只有它自己的声明和 javadoc）。

### 🔴 2026-07-28 已查上游：**前置条件不成立，推荐值翻转为 B**

原推荐是「先 grep 一次上游 master；**无调用者**就删」。**查了，有调用者**
（`upstream-apache/master` @ `2faf819fa89`）：

```
fe/fe-core/.../datasource/connectivity/AbstractS3CompatibleConnectivityTester.java:71
        adapter.getAwsCredentialsProvider()          ← 就是 StorageAdapter 的这个方法
fe/fe-core/.../datasource/property/common/IcebergAwsClientCredentialsProperties.java:84
        s3Adapter.getAwsCredentialsProvider()        ← 同上
```

**它在上游是活的，在本分支才是死的** —— 因为本分支的迁移已经把这两个消费者
（连同整个 `datasource/connectivity/` 包）删光了。

**这就改变了性价比**：`StorageAdapter.java` 本身**两边都在**、会被 rebase 三方合并。
今天上游对 `getAwsCredentialsProvider()` 或其邻近代码的任何改动都能干净合入；
一旦我把方法删掉，这些改动就变成**每次 rebase 都要人工处理的冲突 hunk** ——
而换来的只是 146 行**本来就没人执行**的代码，功能收益为零。

（`AwsCredentialsProviderFactory` 的 `createV2`/`createDefaultV2` 是同一处的下游，
删了方法才轮得到它们，所以一并搁置。）

### 选项（更新后）

| | 做法 |
|---|---|
| **B（现推荐）** | **不做 FPC-02。** 死代码留着零成本，避免给一个高频 rebase 的分支平添长期冲突面 |
| **A** | 仍然删。若判断本分支终局是「不再跟随上游 `StorageAdapter`」（例如该文件本就要整体重写/删除），那冲突面是虚的，删掉更干净 |

**我的推荐：B。** 判据是 memory 里那条「上游定期 rebase + force-push」——
**为零功能收益长期承担 rebase 摩擦不划算**。
若你认为 `StorageAdapter` 本来就要在后续阶段整体退役，那 A 更好，请直接说。

> **拍板结果**：（待填 —— 已停在此处，未擅自执行）
> **日期**：（待填）

---

## ✅ 已拍板

（暂无。第一条拍板后移到这里，保留原文并补上结论与日期。）
