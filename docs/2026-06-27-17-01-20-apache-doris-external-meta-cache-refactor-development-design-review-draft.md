# 2026-06-27 17:01:20 CST Apache Doris 外表元数据缓存机制重构开发方案（Review Draft）

## 0. 实现时修正

本节记录开发实现过程中相对下文伪代码与自检清单确认过的修正。若本节与后文存在冲突，以本节为准；后文相关章节后续再做统一回写。

### 0.1 冷缓存上的增量注册不再主动预热 names/object/id cache

原方案在 `registerDatabase()` / `registerTable()` 相关伪代码中要求统一执行：

```text
names entry -> object entry -> id map
```

实现阶段确认该语义会让增量同步事件把当前 FE 从未消费过的 database/table 缓存主动预热到本地，包括：

- `databaseNames/tableNames`
- `databases/tables`
- `dbIdToName/tableIdToName`

这与当前实现目标不一致。对于某个 FE 实例而言，外表增量同步涉及的库表不一定后续会被访问；如果仅因为收到增量事件就主动把冷缓存填充到本地，会引入额外状态、无价值缓存和潜在副作用。

因此实现中修正为：

- `register/create` 只维护已经处于热状态的 cache entry。
- 如果对应 names/object/id cache 当前均未命中，则不因为增量事件主动从“冷”变“热”。
- 后续只有当当前 FE 确实访问过相关 database/table，增量事件才继续维护这部分本地热状态。

该修正的直接意图是：避免把“当前 FE 从未消费过的外表元数据”仅因增量事件而预热进来。换言之，增量同步负责维护已存在的本地热状态，而不是为所有 FE 无条件创建新的本地缓存状态。

## 1. 文档目标

本文档用于指导代码开发 Agent 基于 Apache Doris master 分支完成外表元数据缓存机制重构。

本方案不依赖其他设计文档，完整说明：

- 为什么要重构外表元数据缓存。
- 当前代码中相关类的职责。
- 需要修改、删除、新增的方法和字段。
- `MetaCacheEntry` 需要增强的具体 API 和并发语义。
- `ExternalCatalog` / `ExternalDatabase` 如何从 Legacy `MetaCache` 迁移到两个 `MetaCacheEntry`。
- names list 与 lower-case index 如何通过不可变 `NameCacheValue` 形成同一缓存版本。
- 关键测试点和实现自检清单。

本文档中的代码片段是开发指导级伪代码，需要结合当前分支代码风格、imports、泛型声明和 checkstyle 要求落地。

### 1.1 生产现象与影响

本次重构最初来源于生产环境中的 FE Follower 元数据回放停滞问题。问题节点上的内部 OLAP 表查询持续出现：

```text
errorCode=OLAP_ERR_VERSION_ALREADY_MERGED
errorMsg=[E-230] versions are already compacted
```

该错误由 BE 在请求的旧版本 Rowset 已被 compaction 合并、清理后返回。直接现象是 FE 向 BE 下发了过旧的扫描版本范围；进一步排查发现，问题 FE 的 Replayer 线程在回放 refresh catalog 等 journal 时，因外表元数据缓存失效与查询线程的慢速缓存加载发生锁竞争而长时间阻塞。

Replayer 是按顺序回放 edit log 的单线程。一旦它被阻塞，该 Follower 不仅无法继续回放外表 refresh，还无法及时回放后续内部表 visible version、DDL 和分区元数据变化。因此，外表元数据加载只是触发点，最终受影响的可能是与该外表完全无关的内部 OLAP 表查询。

该问题通常具有以下特征：

- 只发生在部分 FE Follower，其他 FE 节点正常。
- 查询可能随机成功或失败，取决于客户端命中了哪个 FE。
- 重启问题 FE 后缓存被清空，可能暂时恢复，但慢加载再次出现后仍可能复现。
- CPU、内存和网络监控不一定异常，主要证据来自 Replayer lag、jstack 中的缓存锁等待和外部元数据加载栈。

### 1.2 Replayer 被慢外表元数据加载阻塞的链路

外表元数据 loader 可能访问 HMS、JDBC catalog、Paimon catalog、HDFS、对象存储、schema、partition、snapshot、manifest 或 table list。不同引擎、不同 cache entry 的具体 I/O 不同，但共同特点是耗时受外部系统影响，可能达到秒级甚至分钟级。

需要注意，当前 master 中 Paimon table cache loader 主要加载 table handle，latest snapshot、partition 等信息有相当一部分通过 lazy supplier 延后执行，不能把旧生产栈中出现过的 snapshot/manifest 调用机械等同于当前 `loadTableCacheValue()`。本方案面向所有可能执行慢外部 I/O 的 names、object、schema 和 engine-specific entry，不依赖某一个 Paimon 调用栈成立。

旧同步加载路径的关键链路为：

```text
查询线程
  -> LoadingCache.get(key)
  -> Caffeine 同步 miss load / compute 路径
  -> CacheLoader.load(key)
  -> HMS / catalog / 文件系统 / 对象存储慢 I/O

Replayer
  -> replay refresh catalog / db / table
  -> invalidate external metadata cache
  -> 等待与同步 load 相关的 Caffeine 内部 key/bin 更新路径
  -> journal 回放停滞
```

问题不在于 invalidate 本身需要访问外部系统，而在于同步 loader 把不可控的外部 I/O 放进了 Caffeine 的同步 miss-load 关键路径。invalidate、remove 或 refresh 写路径与其竞争时，Replayer 可能间接等待整段外部 I/O。

本方案不通过“跳过 invalidate”规避问题。Replayer 仍执行正常失效；变化是把 normal cache miss 的外部加载移到 Caffeine 同步 load 路径之外，使 invalidate 不需要等待查询线程的慢 I/O。

### 1.3 当前 master 的两套缓存与剩余风险

当前 master 中，较新的 `MetaCacheEntry` 已合入 manual miss load：先通过 `getIfPresent()` 判断命中，miss 时在 Caffeine 同步 loader 路径之外执行外部 I/O，完成后再写入 cache。该改造已经降低了使用 `MetaCacheEntry` 的 schema、table 等新缓存路径阻塞 Replayer 的风险。

但是，`ExternalCatalog` 和 `ExternalDatabase` 仍使用 Legacy `MetaCache`。Legacy `MetaCache` 内部维护：

```text
namesCache: LoadingCache<String, List<Pair<String, String>>>
metaObjCache: LoadingCache<String, Optional<T>>
idToName: Map<Long, String>
```

其中：

- `namesCache.get("")` 可能触发 database/table names 枚举，例如 HMS list databases、list tables。
- `metaObjCache.get(name)` 可能触发 `ExternalDatabase` / `ExternalTable` 构造，并继续访问外部 catalog。
- object miss load 还通过 `synchronized (metaObjCache)` 做全局串行化，不同 key 的慢加载也可能互相影响。

因此，仅增强 `MetaCacheEntry` 并不能覆盖所有外表元数据加载路径。只要 Legacy `MetaCache` 继续存在，refresh/invalidate/Replayer 仍可能进入旧的同步 LoadingCache 路径。这也是本次重构必须删除 Legacy 包装层，而不是继续在两套实现中分别打补丁的原因。

### 1.4 重构过程中确认的其他当前实现问题

对 Legacy `MetaCache` 和迁移调用链进行完整 review 后，还确认了以下问题。这些问题不全是 Replayer 阻塞的直接原因，但与缓存一致性、回放安全和后续维护成本直接相关：

1. **Replay cache-only 契约未完全落实**：`getDbForReplay(long)` 和 `getTableForReplay(long)` 当前通过 `getMetaObjById()` 进入 `getMetaObj()`，object cache miss 时仍可能触发远端 loader，与方法注释不一致。
2. **Names list 是共享可变对象**：Legacy `MetaCache.updateCache()/invalidate()` 在 `compute()` 中原地修改缓存内的 `List`，而其他线程可能已在 cache 锁外遍历同一对象。
3. **Names list 与大小写索引分离**：`namesCache` 与 `lowerCaseToDatabaseName/lowerCaseToTableName` 由不同路径更新，可能出现旧 list 配新 index、cache 已失效但 side map 仍保留等状态组合。
4. **对象不存在使用 `Optional.empty()` 表示**：negative value 与真正 cache miss 混合，导致 `getMetaObj()` 需要额外失效和重载逻辑，增加并发语义复杂度。
5. **ID 映射可能使用重新生成的 ID**：部分查询或失效路径按名称重新调用 `Util.genIdByName()`，而实际对象可能携带来自 edit log 或外部同步事件的 ID。
6. **Mutation 与 in-flight load 缺少统一版本保护**：现有 generation 主要覆盖 manual load 与 invalidate，尚未完整覆盖 public `put()`、`compute()` 和异步 refresh 写回。
7. **两套缓存能力和约束重复维护**：TTL、refresh、removal listener、load stats、cache disabled、并发去重和失效语义分散在 `MetaCacheEntry`、`MetaCache` 和调用方中，容易出现修复只覆盖其中一套的情况。

### 1.5 为什么选择统一到增强版 MetaCacheEntry

本方案将 `MetaCacheEntry` 定位为 Doris 外表元数据缓存的统一基础对象，并把 Legacy `MetaCache` 拆成 database/table 两级各自的 names entry、object entry 和 ID map。选择该方向的原因是：

- 已合入 master 的 manual miss load、load stats、cache-disabled 和 generation 逻辑可以直接复用，改动集中在一个基础对象。
- `ExternalCatalog` / `ExternalDatabase` 本来就掌握 names、object、ID、replay 和系统库语义，删除只包装两个 LoadingCache 的 `MetaCache` 后，职责边界更清晰。
- 所有 normal miss load 统一绕开 Caffeine 同步 loader 路径，从机制上覆盖新旧缓存的主要 Replayer 长阻塞来源。
- 不改变 `ExternalCatalog` / `ExternalDatabase` 的对外 API、FE image、edit log 和 FE-BE 协议，重构范围仍限制在 FE 内存缓存实现。
- `refreshAfterWrite`、TTL、capacity、eviction、stats 和 removal listener 继续由 Caffeine 提供，不需要重新实现完整缓存框架。

该方案重点解决的是 Caffeine 同步 miss load 包含慢外部 I/O 导致的长时间阻塞。`ExternalDatabase.resetMetaToUninitialized()` 等路径仍存在 Java monitor 层面的短暂竞争，本次不调整对象锁顺序，也不声称消除所有形式的 Replayer 等待。

### 1.6 设计 review 中确认的兼容约束

方案 review 过程中还确认了若干实现必须显式保留的边界：

- replay 方法必须先检查 `isInitialized()`，并始终使用 cache-only 查询。
- 普通 FE UT 需要保留 `FeConstants.runningUnitTest` 的存在性检查旁路；`TestExternalCatalog/TestExternalDatabase` 仍可主动覆盖检查逻辑。
- `information_schema`、`mysql` 和系统表枚举继续使用 Doris 本地专用实现。
- sync removal listener 与 `refreshAfterWrite` 不能组合，这是当前实现已有的约束，不是新方案新增限制。
- `HMSExternalCatalog.registerDatabase()` 必须迁移到统一 helper，且 names 冲突检查要先于 object/ID 写入，避免异常后留下部分更新。
- `NameCacheValue` 只校验精确 remote-name 唯一映射；与大小写模式相关的冲突仍由具有 catalog 配置上下文的 loader/调用方判断。

### 1.7 本版 Review 决策

本版相对上一轮方案统一落实以下四项决策：

1. names entry 使用不可变 `NameCacheValue`，将 remote/local names list 与 lower-case index 合并为同一个缓存 value，删除独立 side map。
2. `dbIdToName/tableIdToName` 在对象加载或注册成功后使用实际对象 `getId()`，不在查询完成后按名称重复生成 ID。
3. copy-on-write 明确定位为对当前 Legacy `MetaCache` 原地修改共享 List 风险的修复，不是新方案引入的额外问题。
4. sync removal listener 与 `refreshAfterWrite` 的互斥关系明确定位为当前 Legacy `MetaCache` 已遵守的约束，新方案通过命名工厂和 private 底层构造器固化该约束。

## 2. 当前代码结构

### 2.1 新缓存机制：MetaCacheEntry

文件：

```text
fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/MetaCacheEntry.java
```

当前 `MetaCacheEntry` 已具备：

- `LoadingCache<K, V> loadingData`
- `Cache<K, V> data`
- manual miss load
- load locks
- invalidate generation
- cache disabled 语义
- load stats
- `refreshAfterWrite`

但仍存在以下不足：

- 仍保留 `Config.enable_external_meta_cache_manual_miss_load` 开关。
- `put()` 不递增 generation。
- manual load 内部写回和 public `put()` 没有区分。
- generation 是 entry 级单个 `AtomicLong`，不是 stripe 级。
- 没有 `compute()`。
- 没有 removal listener 构造入口。
- 没有 generation-aware `asyncReload()`。

### 2.2 旧缓存机制：Legacy MetaCache

文件：

```text
fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/MetaCache.java
fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/LegacyMetaCacheFactory.java
```

`MetaCache<T>` 内部封装：

```text
namesCache: LoadingCache<String, List<Pair<String, String>>>
metaObjCache: LoadingCache<String, Optional<T>>
idToName: Map<Long, String>
```

主要调用方：

```text
fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalCatalog.java
fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalDatabase.java
```

本次改造删除 `MetaCache` 和 `LegacyMetaCacheFactory`，将其语义迁移到 `ExternalCatalog` / `ExternalDatabase` 内部。

当前 Legacy `MetaCache` 还存在两项需要在迁移时一并修正的实现问题：

1. `namesCache.asMap().compute()` 对缓存中的可变 `List` 执行原地 `add/removeIf`。`compute()` 只保护 key 到 value 引用的映射，不保护已经被查询线程持有并遍历的 `List`，因此存在并发遍历不稳定和 `ConcurrentModificationException` 风险。
2. names list 与 `lowerCaseToDatabaseName/lowerCaseToTableName` 是两份独立状态。loader、直接 miss 补全、register/unregister 和 names-only reset 并非总是原子更新两者，可能出现旧 list 配新 index、空 cache 配旧 index，或 refresh 期间 index 部分可见。

因此，本次 copy-on-write 和 `NameCacheValue` 不是新架构引入问题后的补丁，而是重构过程中对当前实现已有并发与一致性风险的增强修复。

## 3. 总体目标

1. 增强 `MetaCacheEntry`，使其成为外表元数据缓存统一基础对象。
2. 删除 Legacy `MetaCache` / `LegacyMetaCacheFactory`。
3. `ExternalCatalog` 直接维护：
   - `databaseNames`
   - `databases`
   - `dbIdToName`
4. `ExternalDatabase` 直接维护：
   - `tableNames`
   - `tables`
   - `tableIdToName`
5. 保持 `ExternalCatalog` / `ExternalDatabase` 对外 API 不变。
6. 固定启用 manual miss load，不再保留 `Config.enable_external_meta_cache_manual_miss_load`。
7. 保留 Caffeine TTL、capacity、eviction、stats、removal listener、`refreshAfterWrite` 能力。
8. 避免慢外部 I/O 发生在 Caffeine 同步 miss load 路径。
9. 避免 refresh / invalidate / put / compute 与 in-flight load 并发时旧值写回。
10. 使用不可变 `NameCacheValue` 将 names list 与 lower-case index 绑定为同一缓存版本。
11. ID 映射使用实际 database/table 对象的 `getId()`，不在查询完成后重复按名称生成 ID。
12. 保留并通过 `withSyncRemovalListener()` 显式固化当前 sync removal listener 与 `refreshAfterWrite` 的互斥约束。

## 4. 需要修改的文件

### 4.1 必改文件

```text
fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/MetaCacheEntry.java
fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/NameCacheValue.java
fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalCatalog.java
fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalDatabase.java
fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalMetaCacheMgr.java
fe/fe-core/src/main/java/org/apache/doris/datasource/hive/HMSExternalCatalog.java
fe/fe-core/src/main/java/org/apache/doris/common/Config.java
```

### 4.2 删除文件

```text
fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/MetaCache.java
fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/LegacyMetaCacheFactory.java
```

### 4.3 测试文件

需要调整或新增：

```text
fe/fe-core/src/test/java/org/apache/doris/datasource/metacache/MetaCacheEntryTest.java
fe/fe-core/src/test/java/org/apache/doris/datasource/metacache/NameCacheValueTest.java
fe/fe-core/src/test/java/org/apache/doris/datasource/MetaCacheTest.java
fe/fe-core/src/test/java/org/apache/doris/datasource/metacache/MetaCacheDeadlockTest.java
fe/fe-core/src/test/java/org/apache/doris/external/hms/HmsCatalogTest.java
```

`MetaCacheTest` 和 `MetaCacheDeadlockTest` 需要改写或删除。对应语义迁移到 `MetaCacheEntryTest`、`ExternalCatalog` / `ExternalDatabase` 相关 UT。

## 5. NameCacheValue 与 MetaCacheEntry 详细改造

### 5.1 新增不可变 NameCacheValue

新增：

```text
fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/NameCacheValue.java
```

它同时保存 remote/local names list 和基于 remote name 的 lower-case index：

```java
public final class NameCacheValue {
    private final ImmutableList<Pair<String, String>> names;
    private final ImmutableMap<String, String> lowerCaseToRemoteName;

    private NameCacheValue(List<Pair<String, String>> names) {
        this.names = ImmutableList.copyOf(names);
        Map<String, String> index = Maps.newHashMap();
        for (Pair<String, String> pair : names) {
            index.put(pair.key().toLowerCase(), pair.key());
        }
        this.lowerCaseToRemoteName = ImmutableMap.copyOf(index);
    }

    public static NameCacheValue of(List<Pair<String, String>> names) {
        return new NameCacheValue(Objects.requireNonNull(names, "names can not be null"));
    }

    public static NameCacheValue empty() {
        return of(ImmutableList.of());
    }

    public List<Pair<String, String>> names() {
        return names;
    }

    public String remoteNameOfLocalName(String localName) {
        return names.stream()
                .filter(pair -> pair.value().equals(localName))
                .map(Pair::key)
                .findFirst()
                .orElse(null);
    }

    public boolean containsLocalName(String localName) {
        return names.stream().anyMatch(pair -> pair.value().equals(localName));
    }

    public String remoteNameForCaseInsensitiveLookup(String name) {
        return lowerCaseToRemoteName.get(name.toLowerCase());
    }

    public NameCacheValue withName(String remoteName, String localName) {
        for (Pair<String, String> pair : names) {
            if (pair.key().equals(remoteName)
                    && !pair.value().equals(localName)) {
                throw new IllegalArgumentException(
                        "remote name already maps to another local name: " + remoteName);
            }
        }
        List<Pair<String, String>> copy = Lists.newArrayList(names);
        copy.removeIf(pair -> pair.value().equals(localName));
        copy.add(Pair.of(remoteName, localName));
        return of(copy);
    }

    public NameCacheValue withoutLocalName(String localName) {
        List<Pair<String, String>> copy = Lists.newArrayList(names);
        copy.removeIf(pair -> pair.value().equals(localName));
        return of(copy);
    }
}
```

实现约束：

- `NameCacheValue` 创建后不可变，不暴露可修改的 `List/Map`。
- lower-case index 与 names list 必须在构造函数中从同一份输入同时生成。
- remote name 大小写冲突检查仍由现有 database/table names loader 按当前配置规则完成，`NameCacheValue` 不改变现有冲突判定语义。
- `remoteNameForCaseInsensitiveLookup()` 明确返回远端原始大小写名称，只用于 `lower_case_database_names=2` 或 `lower_case_table_names=2` 的大小写不敏感查找，不是通用的 local-name 归一化方法。
- mode 2 下现有 loader 会刻意把 local cache key 设置为 remote/original-case name，因此该方法返回的 remote name 同时也是 object cache 使用的 local key。若未来改变 mode 2 的 local-key 规则，必须同步调整该索引和调用方，不能继续依赖这一等价关系。
- `withName()` / `withoutLocalName()` 必须返回新对象，不能修改旧对象。
- `withName()` 必须保证同一个精确 `remoteName` 不能同时映射到两个不同 `localName`。相同 remote/local pair 的重复注册保持幂等；同一个 local name 更新为另一个 remote name 时，继续按当前 upsert 语义替换旧条目。
- `Foo` 与 `foo` 是否冲突取决于大小写配置，不能由无 catalog 配置上下文的 `NameCacheValue` 无条件判断。大小写冲突检查仍由 database/table names loader 或 register 调用方按现有规则完成。
- register/unregister 相对低频，允许 O(N) copy；读取路径保持 O(1) lower-case lookup 和稳定快照。

当前 Legacy `MetaCache.updateCache()/invalidate()` 会在 `compute()` 中原地修改共享 `List`。查询线程可能已通过 `listNames()/getRemoteName()` 持有并遍历同一个 List，因此当前实现本来就存在共享可变对象的并发风险。这里采用 copy-on-write 是对现有问题的修复，不是新方案新增约束。

### 5.2 删除 manual miss load 开关

删除 `MetaCacheEntry` 中：

```java
private boolean isManualMissLoadEnabled() {
    return Config.enable_external_meta_cache_manual_miss_load;
}
```

删除 `get()` / `get(key, missLoader)` 中的回退逻辑：

```java
if (!isManualMissLoadEnabled()) {
    return loadingData.get(key);
}
```

改为固定 manual path：

```java
public V get(K key) {
    return getWithManualLoad(key, this::applyDefaultLoader);
}

public V get(K key, Function<K, V> missLoader) {
    Function<K, V> loadFunction = Objects.requireNonNull(missLoader, "missLoader can not be null");
    return getWithManualLoad(key, loadFunction);
}
```

同时删除 `Config.enable_external_meta_cache_manual_miss_load` 配置项及相关测试。

### 5.3 将 generation 改为 stripe generation

当前：

```java
private static final int LOAD_LOCK_STRIPES = 128;
private final AtomicLong invalidateGeneration = new AtomicLong(0);
```

改为：

```java
private static final int LOAD_LOCK_STRIPES = 4096;
private final AtomicLongArray generations = new AtomicLongArray(LOAD_LOCK_STRIPES);
```

新增 helper：

```java
private int stripe(K key) {
    int hash = key == null ? 0 : key.hashCode();
    return (hash & Integer.MAX_VALUE) % LOAD_LOCK_STRIPES;
}

private Object loadLock(K key) {
    return loadLocks[stripe(key)];
}

private long generationOf(K key) {
    return generations.get(stripe(key));
}

private void bumpGeneration(K key) {
    generations.incrementAndGet(stripe(key));
}

private void bumpAllGenerations() {
    for (int i = 0; i < LOAD_LOCK_STRIPES; i++) {
        generations.incrementAndGet(i);
    }
}
```

注意：

- `invalidateKey(key)` 只 bump 当前 key stripe。
- `put(key)` 只 bump 当前 key stripe。
- `compute(key)` 只 bump 当前 key stripe。
- `invalidateAll()` / `invalidateIf()` bump all stripes。

### 5.4 区分 public mutation 和 internal load write-back

public `put()` 是主动 mutation，必须 bump generation：

```java
public void put(K key, V value) {
    Objects.requireNonNull(key, "key can not be null");
    Objects.requireNonNull(value, "value can not be null");
    if (!effectiveEnabled) {
        return;
    }
    bumpGeneration(key);
    data.put(key, value);
}
```

`put()` 的 key/value 必须非空。对象不存在只通过 cache miss 或 loader 返回 `null` 表达，不允许通过显式 `put(key, null)` 表达。

manual load 内部写回不能 bump generation：

```java
private void putLoadedValueWithoutGenerationBump(K key, V loaded) {
    data.put(key, loaded);
}
```

内部 stale cleanup 也不能 bump generation：

```java
private void removeLoadedValueWithoutGenerationBump(K key, V loaded) {
    data.asMap().computeIfPresent(
            key,
            (ignored, currentValue) -> currentValue == loaded ? null : currentValue);
}
```

`getWithManualLoad()` 核心流程：

```java
private V getWithManualLoad(K key, Function<K, V> loadFunction) {
    if (!effectiveEnabled) {
        return loadAndTrack(key, loadFunction);
    }

    V value = data.getIfPresent(key);
    if (value != null) {
        return value;
    }

    synchronized (loadLock(key)) {
        value = data.asMap().get(key);
        if (value != null) {
            return value;
        }

        long generation = generationOf(key);
        V loaded = loadAndTrack(key, loadFunction);
        if (generation != generationOf(key)) {
            return loaded;
        }
        if (loaded == null) {
            return null;
        }

        beforeManualCachePutForTest(key, loaded);
        putLoadedValueWithoutGenerationBump(key, loaded);
        if (generation != generationOf(key)) {
            removeLoadedValueWithoutGenerationBump(key, loaded);
        }
        return loaded;
    }
}
```

### 5.5 增加 compute API

新增 public `compute()`：

```java
public V compute(K key, BiFunction<K, V, V> remappingFunction) {
    Objects.requireNonNull(remappingFunction, "remappingFunction can not be null");
    if (!effectiveEnabled) {
        return null;
    }
    bumpGeneration(key);
    return data.asMap().compute(key, remappingFunction);
}
```

用途：

- 替代 Legacy `namesCache.asMap().compute("", ...)`。
- 保证 names list 更新也能 bump generation。

### 5.6 loader 返回 null 的语义

新 object cache 不再使用 `Optional<T>` 作为 value，直接缓存对象本身。统一语义如下：

- cache 中有对象：`get()` 直接返回对象。
- cache miss：`get()` 在 stripe load lock 内执行 loader。
- loader 返回对象：按 generation 规则写入 cache。
- loader 返回 `null`：向调用方返回 `null`，不执行 `data.put()`，cache 继续保持 miss。
- 后续再次调用 `get()` 时会重新执行 loader。

现有 `getWithManualLoad()` 中的以下判断即为该语义的实现边界：

```java
V loaded = loadAndTrack(key, loadFunction);
if (generation != generationOf(key)) {
    return loaded;
}
if (loaded == null) {
    return null;
}
putLoadedValueWithoutGenerationBump(key, loaded);
```

因此不新增 `getIfValidOrLoad()`，也不在 cache 中保存 `Optional.empty()`。`buildDbForInit()` 和 `buildTableForInit()` 均允许返回 `null`，由上述逻辑处理。

### 5.7 增加 removal listener 构造入口

保留当前三个 public 构造器及其现有语义：

```java
MetaCacheEntry(name, loader, cacheSpec, refreshExecutor)
MetaCacheEntry(name, loader, cacheSpec, refreshExecutor, autoRefresh)
MetaCacheEntry(name, loader, cacheSpec, refreshExecutor, autoRefresh, contextualOnly)
```

三个 public 构造器统一委托给一个 private 底层构造器：

```java
private MetaCacheEntry(
        String name,
        @Nullable Function<K, V> loader,
        CacheSpec cacheSpec,
        ExecutorService refreshExecutor,
        boolean autoRefresh,
        boolean contextualOnly,
        @Nullable RemovalListener<K, V> removalListener,
        boolean syncRemovalListener)
```

现有 public 构造器只按以下规则委托，不改变调用方语义：

```java
// 4 args
this(name, loader, cacheSpec, refreshExecutor, true, false, null, false);

// 5 args
this(name, loader, cacheSpec, refreshExecutor, autoRefresh, false, null, false);

// 6 args
this(name, loader, cacheSpec, refreshExecutor, autoRefresh, contextualOnly, null, false);
```

不公开该八参数构造器，避免上层通过多个 boolean 自由组合出不支持的模式。同步 removal listener 使用命名工厂创建：

```java
public static <K, V> MetaCacheEntry<K, V> withSyncRemovalListener(
        String name,
        Function<K, V> loader,
        CacheSpec cacheSpec,
        ExecutorService refreshExecutor,
        RemovalListener<K, V> removalListener) {
    return new MetaCacheEntry<>(
            name,
            loader,
            cacheSpec,
            refreshExecutor,
            false,
            false,
            Objects.requireNonNull(removalListener, "removalListener can not be null"),
            true);
}
```

构造入口与使用场景固定为：

| 使用场景 | 入口 | 固定语义 |
| --- | --- | --- |
| 现有默认 entry | 四参数构造器 | `autoRefresh=true`、`contextualOnly=false`、无 removal listener |
| names entry | 五参数构造器，显式传 `true` | 启用 `refreshAfterWrite`、无 removal listener |
| 无 removal listener 的 object entry | 五参数构造器，显式传 `false` | 不启用 `refreshAfterWrite` |
| contextual-only entry | 六参数构造器 | 保持现有 loader/null 校验 |
| 需要同步 removal listener 的 object entry | `withSyncRemovalListener()` | 固定关闭 `refreshAfterWrite` 和 contextual-only |

该互斥约束不是新方案新增限制。当前 Legacy `MetaCache` 已经采用以下固定分工：

- `namesCache` 使用普通 refresh executor 并配置 `refreshAfterWrite`，不使用 sync removal listener。
- `metaObjCache` 通过 `CacheFactory.buildCacheWithSyncRemovalListener()` 使用 sync removal listener，并显式不配置 `refreshAfterWrite`。

原因是当前 `buildCacheWithSyncRemovalListener()` 通过 `builder.executor(Runnable::run)` 让 removal listener 同步执行，以避免 listener 在有界公共线程池中调用其他 cache 的 `invalidateAll()` 时发生递归提交和线程池自阻塞。Caffeine 同一个 builder executor 也负责 refresh；如果同时配置 `refreshAfterWrite`，refresh loader 会退化为调用线程同步执行，重新引入慢外部 I/O 阻塞。

本次改造只是把当前隐含且已遵守的约束下沉到 `MetaCacheEntry` 的命名工厂和 private 底层构造中，防止未来误配。public API 不提供“sync removal listener + autoRefresh”的自由组合入口。

构造约束：

```java
if (contextualOnly) {
    if (loader != null) {
        throw new IllegalArgumentException("contextual-only entry loader must be null");
    }
    if (autoRefresh) {
        throw new IllegalArgumentException("contextual-only entry can not enable auto refresh");
    }
} else {
    Objects.requireNonNull(loader, "loader can not be null");
}

if (syncRemovalListener && autoRefresh) {
    throw new IllegalArgumentException("sync removal listener cache can not enable refreshAfterWrite");
}
```

上述校验统一放在 private 八参数底层构造器中。现有 `contextualOnly` 语义必须完整保留：contextual-only entry 不绑定默认 loader，也不能启用 auto refresh；普通 entry 必须提供非 null loader。sync removal listener 校验同样属于底层构造器的内部不变量保护；正常调用方通过 `withSyncRemovalListener()` 时，`autoRefresh` 已固定为 `false`。

构造底层 cache：

- 无 removal listener：普通 Caffeine builder + refresh executor。
- 有 sync removal listener：使用 `CacheFactory.buildCacheWithSyncRemovalListener()`，且必须无 refresh。
- 如果未来需要异步 removal listener，可另行扩展。本次不做。
- `databaseNames`、`tableNames` 使用五参数构造器并传 `true`；`tables` 使用五参数构造器并传 `false`；`databases` 使用 `withSyncRemovalListener()`。上层不得直接调用 private 八参数构造器。

### 5.8 实现 generation-aware asyncReload

不能继续只传：

```java
cacheFactory.buildCache(this::loadFromDefaultLoader, refreshExecutor)
```

需要构造自定义 `CacheLoader<K, V>`：

```java
private CacheLoader<K, V> newCacheLoader() {
    return new CacheLoader<K, V>() {
        @Override
        public V load(K key) {
            return loadFromDefaultLoader(key);
        }

        @Override
        public CompletableFuture<V> asyncReload(K key, V oldValue, Executor executor) {
            long generation = generationOf(key);
            CompletableFuture<V> result = new CompletableFuture<>();
            CompletableFuture.supplyAsync(() -> loadFromDefaultLoader(key), executor)
                    .whenComplete((loaded, error) -> {
                        if (error != null) {
                            result.completeExceptionally(error);
                        } else if (generation == generationOf(key)) {
                            result.complete(loaded);
                        } else {
                            result.cancel(false);
                        }
                    });
            return result;
        }
    };
}
```

注意：

- `asyncReload()` 只影响 refreshAfterWrite。
- 普通 `get()` 不再使用 Caffeine sync load path。
- `load()` 仍保留，用于 Caffeine refresh 机制或测试，但不作为 normal miss load 入口。
- generation 变化时必须直接取消最终返回给 Caffeine 的 `result` future。不能在 `thenCompose()` 中返回一个以 `CancellationException` 异常完成的 child future，否则异常可能被包装成 `CompletionException`，产生 refresh warning。
- 不采用 `CompletableFuture.completedFuture(oldValue)`。该方案虽然不写入新加载值，但会把被丢弃的 refresh 记录为成功，并可能推迟下一次 refresh。
- `CacheFactory` 构造 LoadingCache 时必须传入 `newCacheLoader()`，不能继续传 `this::loadFromDefaultLoader`。

### 5.9 invalidate 方法更新

`invalidateKey()`：

```java
public void invalidateKey(K key) {
    bumpGeneration(key);
    if (data.asMap().remove(key) != null) {
        invalidateCount.incrementAndGet();
    }
}
```

`invalidateIf()`：

```java
public void invalidateIf(Predicate<K> predicate) {
    bumpAllGenerations();
    data.asMap().keySet().removeIf(key -> {
        if (predicate.test(key)) {
            invalidateCount.incrementAndGet();
            return true;
        }
        return false;
    });
}
```

`invalidateAll()`：

```java
public void invalidateAll() {
    bumpAllGenerations();
    long size = data.estimatedSize();
    data.invalidateAll();
    invalidateCount.addAndGet(size);
}
```

## 6. ExternalCatalog 详细改造

### 6.1 字段替换

删除：

```java
protected MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache;
```

新增：

```java
protected MetaCacheEntry<String, NameCacheValue> databaseNames;
protected MetaCacheEntry<String, ExternalDatabase<? extends ExternalTable>> databases;
protected Map<Long, String> dbIdToName = Maps.newConcurrentMap();
```

注意：

- 新增字段不要加 `@SerializedName`。
- `dbIdToName` 替代旧 `MetaCache.idToName`。
- 删除独立的 `lowerCaseToDatabaseName` 字段，lower-case index 改由 `NameCacheValue` 持有。
- 同步删除 `gsonPostProcess()`、reset、register/unregister 等路径中对 `lowerCaseToDatabaseName` 的初始化、clear、put 和 remove。

### 6.2 buildMetaCache()

当前通过 `legacyMetaCacheFactory().build(...)` 创建 `MetaCache`。

改为：

```java
private void buildMetaCache() {
    if (databaseNames != null && databases != null) {
        return;
    }

    CacheSpec namesSpec = CacheSpec.of(
            true,
            Config.external_cache_expire_time_seconds_after_access,
            1);
    databaseNames = new MetaCacheEntry<>(
            name + ".database_names",
            ignored -> NameCacheValue.of(getFilteredDatabaseNames()),
            namesSpec,
            Env.getCurrentEnv().getExtMetaCacheMgr().commonRefreshExecutor(),
            true);

    CacheSpec objSpec = CacheSpec.of(
            true,
            Config.external_cache_expire_time_seconds_after_access,
            Math.max(Config.max_meta_object_cache_num, 1));
    databases = MetaCacheEntry.withSyncRemovalListener(
            name + ".databases",
            localDbName -> buildDbForInit(
                    null, localDbName, Util.genIdByName(name, localDbName), logType, true),
            objSpec,
            Env.getCurrentEnv().getExtMetaCacheMgr().commonRefreshExecutor(),
            (key, value, cause) -> value.resetMetaToUninitialized());
}
```

开发注意：

- `ExternalMetaCacheMgr` 当前可能没有公开 `commonRefreshExecutor()`，需要新增 package/public 方法，或在 `ExternalCatalog` 构造路径中传入 executor。
- object cache 使用 sync removal listener，`autoRefresh=false`。
- names cache 使用 `autoRefresh=true`，不使用 removal listener。
- `getFilteredDatabaseNames()` 改为纯粹构建并返回 remote/local pairs，不再 clear/put 独立 lower-case side map。

### 6.3 list DB names

原逻辑：

```java
metaCache.listNames()
```

新 helper：

```java
private NameCacheValue getDatabaseNamesValue() {
    return Objects.requireNonNull(databaseNames.get(""));
}

private List<String> listLocalDatabaseNamesFromCache() {
    return getDatabaseNamesValue().names().stream()
            .map(Pair::value)
            .collect(Collectors.toList());
}
```

将原 `metaCache.listNames()` 替换为 `listLocalDatabaseNamesFromCache()`。

### 6.4 getRemoteDatabaseName()

新增 private helper：

```java
private String getRemoteDatabaseName(String localDbName) {
    return getDatabaseNamesValue().remoteNameOfLocalName(localDbName);
}
```

必须保持 load-through，不能只用 `getIfPresent()`。

### 6.5 删除独立 lowerCaseToDatabaseName

当前 `getFilteredDatabaseNames()` 同时返回 names list 并通过副作用更新 `lowerCaseToDatabaseName`。两份状态不是原子发布，且 `getLocalDatabaseName()` 的 miss path 还会直接调用 loader 方法绕过 names cache。

迁移后删除独立 `lowerCaseToDatabaseName`。大小写不敏感查询直接使用当前 `NameCacheValue` 内的 `lowerCaseToRemoteName` index。names list 和 index 随同一个不可变 value 被 Caffeine 原子替换，cache hit 不需要重建 index。

当前必须迁移的真实入口包括：

```text
ExternalCatalog.getLocalDatabaseName(String dbName, boolean isReplay)
```

`getLocalDatabaseName()` 中当前存在：

```java
getFilteredDatabaseNames();
```

迁移后应改成：

```java
private String getLocalDatabaseName(String dbName, boolean isReplay) {
    String finalName = dbName;
    int mode = getLowerCaseDatabaseNames();
    if (mode == 1) {
        return dbName.toLowerCase();
    }
    if (mode != 2) {
        return finalName;
    }

    NameCacheValue names = isReplay
            ? databaseNames.getIfPresent("")
            : databaseNames.get("");
    if (names == null) {
        return null;
    }
    return names.remoteNameForCaseInsensitiveLookup(dbName);
}
```

要求：

- `isReplay=true` 时仍保持 cache-only，不触发远端 load。
- `isReplay=false` 时通过 `databaseNames.get("")` 触发 names entry manual miss load。
- 不允许继续直接调用 `getFilteredDatabaseNames()` 补 index。
- 只有 `mode == 2` 才调用 `remoteNameForCaseInsensitiveLookup()`。mode 2 按现有规则保留远端原始大小写作为 local object-cache key，因此这里虽然位于 `getLocalDatabaseName()`，返回 remote/original-case name 仍符合现有 local-key 语义。

### 6.6 getDbNullable(String)

原逻辑：

```java
return metaCache.getMetaObj(dbName, Util.genIdByName(name, dbName)).orElse(null);
```

改为：

```java
ExternalDatabase<? extends ExternalTable> db = databases.get(dbName);
if (db != null) {
    dbIdToName.put(db.getId(), dbName);
}
return db;
```

`buildDbForInit()` 返回 `null` 时，`databases.get()` 返回 `null` 且不产生缓存条目。
成功获取对象后必须使用 `db.getId()` 更新 `dbIdToName`，不要再次调用 `Util.genIdByName()`。loader 构造对象时可能仍需要按当前规则生成初始 ID，但查询后的映射应以实际对象 ID 为准。

### 6.7 buildDbForInit()

`buildDbForInit()` 是 `databases` object entry 的 loader，必须完整迁移其名称存在性检查和 remote/local name 解析，不能继续直接访问旧 `MetaCache` 或绕过 `databaseNames` 调用 `getFilteredDatabaseNames()`。

当前 Step 2 首先通过 `getDbNames()` 访问 Legacy names cache；`getDbNames()` 内部调用 `metaCache.listNames()`。第一次判断未命中后，当前代码再直接调用 `getFilteredDatabaseNames()` 进行二次确认：

```java
List<String> dbNames = getDbNames();
if (!dbNames.contains(localDbName)) {
    dbNames = getFilteredDatabaseNames().stream()
            .map(Pair::value)
            .collect(Collectors.toList());
}
```

该调用会绕过 `databaseNames`，使本次存在性判断使用的 names 与 `NameCacheValue` 中缓存的 names/index 不属于同一版本。迁移后必须保留现有 `checkExists` 和 FE UT 旁路条件，并只在该条件成立时通过 names entry 完成二次确认：

```java
if (checkExists
        && (!FeConstants.runningUnitTest || this instanceof TestExternalCatalog)) {
    NameCacheValue names = getDatabaseNamesValue();
    if (!names.containsLocalName(localDbName)) {
        databaseNames.invalidateKey("");
        names = getDatabaseNamesValue();
        if (!names.containsLocalName(localDbName)) {
            LOG.warn("Database {} does not exist in the remote system. Skipping initialization.",
                    localDbName);
            return null;
        }
    }
}
```

这里的 `invalidateKey("")` 只使 names entry 失效；随后 `databaseNames.get("")` 仍通过统一的 manual miss load 和 generation 保护执行远端加载。`getFilteredDatabaseNames()` 只能作为 `databaseNames` 的 loader 使用，其他业务路径不得直接调用。

UT 兼容语义必须保持不变：

- 普通 FE UT 中 `FeConstants.runningUnitTest=true`，且 catalog 不是 `TestExternalCatalog` 时，跳过远端存在性检查和 names reload。很多测试只在内存中注入 database，并未在真实外部系统创建对象。
- `TestExternalCatalog` 即使运行在 UT 环境，仍允许在 `checkExists=true` 时进入该分支，用于覆盖存在性检查逻辑。
- 非 UT 环境按照 `checkExists` 正常决定是否执行检查。
- 原有 RuntimeException/Exception 处理和 name-conflict 异常传播语义保持不变，本节只替换 names 获取方式。

当前 Step 3 使用：

```java
remoteDbName = metaCache.getRemoteName(localDbName);
```

迁移后只替换方法调用，并完整保留 `remoteDbName == null` 及是否需要 remote-name 解析的外层条件：

```java
if (remoteDbName == null) {
    if (Boolean.parseBoolean(getLowerCaseMetaNames())
            || !Strings.isNullOrEmpty(getMetaNamesMapping())) {
        remoteDbName = getRemoteDatabaseName(localDbName);
        if (remoteDbName == null) {
            LOG.warn("Could not resolve remote database name for local database: {}", localDbName);
            return null;
        }
    } else {
        remoteDbName = localDbName;
    }
}
```

`getRemoteDatabaseName()` 通过当前 `NameCacheValue` 完成 local-to-remote name 解析；不能残留 `metaCache.getRemoteName()`。mode 0 且没有 meta-name mapping 时继续直接使用 `localDbName`，不得无条件访问 names entry。Step 4 中 `information_schema` 和 `mysql` 的专用对象构造分支必须原样保留，不能并入普通 external database 的 `logType` 分支。

### 6.8 getDbNullable(long)

原逻辑：

```java
return metaCache.getMetaObjById(dbId).orElse(null);
```

改为：

```java
String dbName = dbIdToName.get(dbId);
if (dbName == null) {
    return null;
}
return databases.get(dbName);
```

### 6.9 getDbForReplay(long)

必须 cache-only，不访问远端：

```java
public Optional<ExternalDatabase<? extends ExternalTable>> getDbForReplay(long dbId) {
    if (!isInitialized()) {
        return Optional.empty();
    }
    String dbName = dbIdToName.get(dbId);
    if (dbName == null) {
        return Optional.empty();
    }
    return Optional.ofNullable(databases.getIfPresent(dbName));
}
```

这是明确的行为修正。当前 `metaCache.getMetaObjById(dbId)` 会在命中 `idToName` 但 object cache miss 时继续调用 `getMetaObj()`，进而通过 LoadingCache loader 访问远端，与方法“replay cache-only”的注释矛盾。迁移后使用 `databases.getIfPresent()`，真正保证 replay-by-id 不触发远端加载。

### 6.10 getDbForReplay(String)

原 `tryGetMetaObj()` 语义是 cache-only。

改为：

```java
public Optional<ExternalDatabase<? extends ExternalTable>> getDbForReplay(String dbName) {
    if (!isInitialized()) {
        return Optional.empty();
    }

    ExternalDatabase<? extends ExternalTable> exact = databases.getIfPresent(dbName);
    if (exact != null) {
        return Optional.of(exact);
    }
    String localDbName = getLocalDatabaseName(dbName, true);
    return localDbName == null
            ? Optional.empty()
            : Optional.ofNullable(databases.getIfPresent(localDbName));
}
```

`isInitialized()` 检查必须位于任何 `databases` / `databaseNames` 访问之前，因为这些字段只在 `buildMetaCache()` 中初始化。精确 key 查询必须放在大小写归一化之前。`resetMetaCacheNames()` 会失效整个 `NameCacheValue`，包括 lower-case index；此时 replay 不能触发 names reload，但日志中通常保存规范名称，仍应能够直接命中 object entry。

### 6.11 registerDatabase()

`HMSExternalCatalog.registerDatabase()` 当前调用 `metaCache.updateCache(...)`。

迁移后实现可供 catalog 子类调用的 helper：

```java
protected final void updateDatabaseCache(String remoteName, String localName,
        ExternalDatabase<? extends ExternalTable> db) {
    Objects.requireNonNull(db, "database can not be null");
    databaseNames.compute("", (ignored, oldValue) ->
            (oldValue == null ? NameCacheValue.empty() : oldValue)
                    .withName(remoteName, localName));
    databases.put(localName, db);
    dbIdToName.put(db.getId(), localName);
}
```

更新顺序必须保持为“参数校验 -> names entry -> object entry -> ID map”。`NameCacheValue.withName()` 的精确 remote-name 冲突异常发生在 names entry 的 `compute()` 内；mapping function 抛出异常时该 names entry 保持原值，且 object entry 和 ID map 尚未修改，不会留下部分更新状态。

`NameCacheValue.withName()` 内部完成 copy-on-write，并同时重建对应 lower-case index。三个独立结构之间不引入新的全局事务锁；names 更新成功后、object entry 写入前可能存在一个很短的 names-only 可见窗口。并发查询在该窗口命中名称后，如果 object entry miss，会通过现有 object loader 加载对象，该行为可接受。相比之下，不能采用“先写 object、再执行可能抛异常的 names compute”的顺序。

`HMSExternalCatalog.registerDatabase()` 当前仍直接调用 `metaCache.updateCache(...)`，因此 `HMSExternalCatalog.java` 必须纳入本次改造。迁移后改为：

```java
@Override
public void registerDatabase(long dbId, String dbName) {
    if (LOG.isDebugEnabled()) {
        LOG.debug("create database [{}]", dbName);
    }

    ExternalDatabase<? extends ExternalTable> db =
            buildDbForInit(dbName, null, dbId, logType, false);
    if (isInitialized()) {
        updateDatabaseCache(db.getRemoteName(), db.getFullName(), db);
    }
}
```

`updateDatabaseCache()` 不能声明为 `private`，否则 `HMSExternalCatalog` 无法调用；使用 `protected final` 既保持子类可见，也避免子类改变统一的 names/object/id-map 更新语义。helper 内部通过 `db.getId()` 更新 `dbIdToName`，不再由 HMS 子类重新生成 ID。

### 6.12 unregisterDatabase()

替换：

```java
metaCache.invalidate(dbName, Util.genIdByName(name, dbName));
```

为：

```java
private void invalidateDatabaseCache(String localName) {
    ExternalDatabase<? extends ExternalTable> cached = databases.getIfPresent(localName);
    databaseNames.compute("", (ignored, oldValue) ->
            (oldValue == null ? NameCacheValue.empty() : oldValue)
                    .withoutLocalName(localName));
    databases.invalidateKey(localName);
    if (cached != null) {
        dbIdToName.remove(cached.getId(), localName);
    }
    dbIdToName.entrySet().removeIf(entry -> entry.getValue().equals(localName));
}
```

注销路径不再根据名称重新生成 ID。对象仍在 cache 时优先使用实际 `getId()` 删除；对象已因 TTL/容量淘汰时，按 local cache key 清理可能残留的 id-to-name 映射。扫描属于低频元数据变更路径。

该 helper 只替换 `unregisterDatabase()` 中原有的 `metaCache.invalidate(...)` 及独立 lower-case side-map 清理职责。`unregisterDatabase()` 的日志、`isInitialized()` 判断以及后续 `ExternalMetaCacheMgr.invalidateDb(...)` 调用必须保留，不能用该 helper 替换整个方法。

### 6.13 resetMetaCacheNames()

当前 `ExternalCatalog.resetMetaCacheNames()` 仍被创建 DB 路径调用，例如：

```text
HiveMetadataOps
PaimonMetadataOps
IcebergMetadataOps
MaxComputeMetadataOps
```

其语义是“只刷新 names cache，让新建 DB 能立即被 list 到”，不能清理 object cache，也不能清理 `dbIdToName`。

迁移后必须保留等价 names-only helper：

```java
public void resetMetaCacheNames() {
    invalidateDatabaseNamesOnly();
}

private void invalidateDatabaseNamesOnly() {
    if (databaseNames != null) {
        databaseNames.invalidateAll();
    }
}
```

要求：

- 只 invalidate `databaseNames`。
- 不 invalidate `databases`。
- 不 clear `dbIdToName`。
- 保持创建 DB 后只刷新 names list 的现有行为。
- names value 失效后 replay-by-name 仍先按精确 object cache key 查询，不依赖远端重载 index。

### 6.14 refreshMetaCacheOnly() / resetToUninitialized()

替换：

```java
metaCache.invalidateAll();
```

为：

```java
private void invalidateAllDatabaseCache() {
    if (databaseNames != null) {
        databaseNames.invalidateAll();
    }
    if (databases != null) {
        databases.invalidateAll();
    }
    dbIdToName.clear();
}
```

注意：

- `invalidateAllDatabaseCache()` 是全量清理，用于 refresh/reset 场景。
- `invalidateDatabaseNamesOnly()` 是 names-only 清理，用于 `resetMetaCacheNames()` 创建路径。
- 两者语义不能混用。
- `resetToUninitialized()` 不再单独 clear lower-case map；invalidate `databaseNames` 即同时丢弃 names list 和 index。

### 6.15 测试辅助接口

以下现有测试辅助接口的方法名、参数和调用方式保持不变：

```java
ExternalCatalog.addDatabaseForTest(...)
ExternalCatalog.setInitializedForTest(...)
```

`addDatabaseForTest()` 当前只向 object cache 和 ID-to-name 映射写入测试对象，不修改 names cache。迁移后必须保持该语义：

```java
public void addDatabaseForTest(ExternalDatabase<? extends ExternalTable> db) {
    buildMetaCache();
    databases.put(db.getFullName(), db);
    dbIdToName.put(db.getId(), db.getFullName());
}
```

不得在该 helper 中额外修改 `databaseNames`，避免扩大现有测试辅助接口的行为。`setInitializedForTest(true)` 继续调用 `buildMetaCache()`，确保 `databaseNames` 和 `databases` 均已构造。

该迁移会影响现有直接调用这些 helper 的 FE UT，包括但不限于 `HmsCatalogTest`、`HmsQueryCacheTest`、`HiveTableSinkTest`、`IcebergDDLAndDMLPlanTest`、`IcebergExternalTableBranchAndTagTest` 和 `CreateIcebergTableTest`。实现后必须保证这些调用点无需修改其对外调用方式。

## 7. ExternalDatabase 详细改造

### 7.1 字段替换

删除：

```java
private MetaCache<T> metaCache;
```

新增：

```java
private MetaCacheEntry<String, NameCacheValue> tableNames;
private MetaCacheEntry<String, T> tables;
private Map<Long, String> tableIdToName = Maps.newConcurrentMap();
```

同时删除独立的 `lowerCaseToTableName` 字段，lower-case index 改由 `NameCacheValue` 持有。
同步删除 reset、register/unregister 等路径中对 `lowerCaseToTableName` 的初始化、clear、put 和 remove。

### 7.2 buildMetaCache()

当前通过 `legacyMetaCacheFactory().build(...)`。

改为：

```java
private void buildMetaCache() {
    if (tableNames != null && tables != null) {
        return;
    }

    CacheSpec namesSpec = CacheSpec.of(
            true,
            Config.external_cache_expire_time_seconds_after_access,
            1);
    tableNames = new MetaCacheEntry<>(
            name + ".table_names",
            ignored -> NameCacheValue.of(listTableNames()),
            namesSpec,
            Env.getCurrentEnv().getExtMetaCacheMgr().commonRefreshExecutor(),
            true);

    CacheSpec objSpec = CacheSpec.of(
            true,
            Config.external_cache_expire_time_seconds_after_access,
            Math.max(Config.max_meta_object_cache_num, 1));
    tables = new MetaCacheEntry<>(
            name + ".tables",
            localTableName -> buildTableForInit(
                    null, localTableName,
                    Util.genIdByName(extCatalog.getName(), name, localTableName),
                    extCatalog, this, true),
            objSpec,
            Env.getCurrentEnv().getExtMetaCacheMgr().commonRefreshExecutor(),
            false);
}
```

`listTableNames()` 改为纯粹构建并返回 remote/local pairs，不再 clear/put 独立 lower-case side map。

### 7.3 list table names

原：

```java
metaCache.listNames()
```

新 helper：

```java
private NameCacheValue getTableNamesValue() {
    return Objects.requireNonNull(tableNames.get(""));
}

private List<String> listLocalTableNamesFromCache() {
    return getTableNamesValue().names().stream()
            .map(Pair::value)
            .collect(Collectors.toList());
}
```

`getTableNamesWithLock()`：

```java
return Sets.newHashSet(listLocalTableNamesFromCache());
```

### 7.4 getRemoteTableName()

新增 private helper：

```java
private String getRemoteTableName(String localTableName) {
    return getTableNamesValue().remoteNameOfLocalName(localTableName);
}
```

必须 load-through。

### 7.5 删除独立 lowerCaseToTableName

当前 `listTableNames()` 同时返回 names list 并通过副作用更新 `lowerCaseToTableName`；`getLocalTableName()` 和 `isTableExist()` 的 miss path 还会直接调用该 loader 方法，从而可能只更新 index 而没有替换 names cache。

迁移后删除独立 `lowerCaseToTableName`。所有大小写不敏感查询统一从当前 `NameCacheValue` 读取 lower-case index，不再在 cache hit 时 clear/rebuild Map。

当前必须迁移的真实入口包括：

```text
ExternalDatabase.getLocalTableName(String tableName, boolean isReplay)
ExternalDatabase.isTableExist(String tableName)
```

`getLocalTableName()` 中当前存在：

```java
listTableNames();
```

迁移后应改成：

```java
private String getLocalTableName(String tableName, boolean isReplay) {
    String finalName = tableName;
    if (this.isStoredTableNamesLowerCase()) {
        finalName = tableName.toLowerCase();
    }
    if (!this.isTableNamesCaseInsensitive()) {
        return finalName;
    }

    NameCacheValue names = isReplay
            ? tableNames.getIfPresent("")
            : tableNames.get("");
    if (names == null) {
        return null;
    }
    return names.remoteNameForCaseInsensitiveLookup(tableName);
}
```

`isTableExist()` 当前也会在 miss 时直接调用 `listTableNames()`。迁移后应改为：

```java
public boolean isTableExist(String tableName) {
    String remoteTblName = tableName;
    if (this.isTableNamesCaseInsensitive()) {
        NameCacheValue names = tableNames.get("");
        remoteTblName = names.remoteNameForCaseInsensitiveLookup(tableName);
        if (remoteTblName == null) {
            return false;
        }
    }
    return extCatalog.tableExist(ConnectContext.get().getSessionContext(), remoteName, remoteTblName);
}
```

要求：

- replay 路径仍 cache-only，不触发远端 load。
- 非 replay 路径通过 `tableNames.get("")` 触发 names entry manual miss load。
- 不允许继续直接调用 `listTableNames()` 补 index，除非是在 table names entry loader 内部。
- 只有 table-name mode 2 才调用 `remoteNameForCaseInsensitiveLookup()`。该模式下现有 loader 将 local object-cache key 设置为 remote/original-case table name，因此 `getLocalTableName()` 返回该值是现有兼容语义；`isTableExist()` 则直接需要该 remote name 访问外部 catalog。

### 7.6 getTableNullable(String)

原：

```java
return metaCache.getMetaObj(finalName, Util.genIdByName(extCatalog.getName(), name, finalName)).orElse(null);
```

改为：

```java
T table = tables.get(finalName);
if (table != null) {
    tableIdToName.put(table.getId(), finalName);
}
return table;
```

`buildTableForInit()` 返回 `null` 时，`tables.get()` 返回 `null` 且不产生缓存条目。
成功获取对象后必须使用 `table.getId()` 更新 `tableIdToName`，不要再次调用 `Util.genIdByName()`。

### 7.7 buildTableForInit()

`buildTableForInit()` 是 `tables` object entry 的 loader。迁移时必须保留现有 Step 2 的二次存在性确认和 FE UT 旁路语义，但所有 names 访问都必须收口到 `tableNames` entry：

```java
if (checkExists
        && (!FeConstants.runningUnitTest || this instanceof TestExternalDatabase)) {
    List<String> tableNameList = listLocalTableNamesFromCache();
    if (!tableNameList.contains(localTableName)) {
        resetMetaCacheNames();
        tableNameList = listLocalTableNamesFromCache();
        if (!tableNameList.contains(localTableName)) {
            LOG.warn("Table {} does not exist in the remote database {}. Skipping initialization.",
                    localTableName, name);
            return null;
        }
    }
}
```

`resetMetaCacheNames()` 只 invalidate `tableNames`，第二次 `listLocalTableNamesFromCache()` 通过 `tableNames.get("")` 完成统一 reload。不得在该路径直接调用 `listTableNames()`。

UT 兼容语义必须保持不变：

- 普通 FE UT 中 `FeConstants.runningUnitTest=true`，且 database 不是 `TestExternalDatabase` 时，跳过远端存在性检查和 table names reload。
- `TestExternalDatabase` 即使运行在 UT 环境，仍允许在 `checkExists=true` 时进入该分支，以覆盖存在性检查逻辑。
- 非 UT 环境按照 `checkExists` 正常执行检查。
- 原有异常处理、name-conflict 传播和“不存在时返回 null”语义保持不变。

当前 Step 3 使用：

```java
remoteTableName = metaCache.getRemoteName(localTableName);
```

迁移后只替换方法调用，并完整保留 `remoteTableName == null` 和是否需要 remote-name 解析的外层条件：

```java
if (remoteTableName == null) {
    if (Boolean.parseBoolean(extCatalog.getLowerCaseMetaNames())
            || !Strings.isNullOrEmpty(extCatalog.getMetaNamesMapping())
            || this.isStoredTableNamesLowerCase()) {
        remoteTableName = getRemoteTableName(localTableName);
        if (remoteTableName == null) {
            LOG.warn("Could not resolve remote table name for local table: {}", localTableName);
            return null;
        }
    } else {
        remoteTableName = localTableName;
    }
}
```

`getRemoteTableName()` 通过当前 `NameCacheValue` 完成 local-to-remote name 解析。mode 0 且没有 meta-name mapping、也不存储 lowercase table name 时继续直接使用 `localTableName`，不得无条件触发 table names load。删除 `MetaCache` 前必须确认该方法中不再残留 `metaCache.getRemoteName()`。

### 7.8 getTableNullable(long)

原：

```java
return metaCache.getMetaObjById(tableId).orElse(null);
```

改为：

```java
String tableName = tableIdToName.get(tableId);
if (tableName == null) {
    return null;
}
return tables.get(tableName);
```

### 7.9 getTableForReplay(String/long)

两个重载都必须 cache-only，并在访问 `tables` / `tableNames` 前检查初始化状态。

String 版本：

```java
public Optional<T> getTableForReplay(String tblName) {
    if (!isInitialized()) {
        return Optional.empty();
    }

    T exact = tables.getIfPresent(tblName);
    if (exact != null) {
        return Optional.of(exact);
    }
    String localName = getLocalTableName(tblName, true);
    return localName == null
            ? Optional.empty()
            : Optional.ofNullable(tables.getIfPresent(localName));
}
```

long 版本：

```java
public Optional<T> getTableForReplay(long tableId) {
    if (!isInitialized()) {
        return Optional.empty();
    }
    String tableName = tableIdToName.get(tableId);
    if (tableName == null) {
        return Optional.empty();
    }
    return Optional.ofNullable(tables.getIfPresent(tableName));
}
```

`isInitialized()` 检查必须位于两个方法开头。与 database 侧一致，String 版本的精确 key 查询必须优先于 lower-case index。names-only invalidation 后 replay 不能加载 names，但不应因此丢失对仍在 object entry 中的规范名称对象的访问。

long 版本同样包含行为修正：当前 `metaCache.getMetaObjById(tableId)` 可能在 object cache miss 时触发远端 loader；迁移后的 `tables.getIfPresent()` 才符合 replay cache-only 契约。

### 7.10 registerTable()

替换 `metaCache.updateCache(...)`：

```java
private void updateTableCache(String remoteName, String localName, T table) {
    Objects.requireNonNull(table, "table can not be null");
    tableNames.compute("", (ignored, oldValue) ->
            (oldValue == null ? NameCacheValue.empty() : oldValue)
                    .withName(remoteName, localName));
    tables.put(localName, table);
    tableIdToName.put(table.getId(), localName);
}
```

table 侧遵循与 database 侧相同的更新顺序。remote-name 冲突导致 `tableNames.compute()` 抛异常时，`tables` 和 `tableIdToName` 必须保持调用前状态。不得先执行 `tables.put()` 再调用可能抛异常的 `withName()`。

### 7.11 unregisterTable()

替换 `metaCache.invalidate(...)`：

```java
private void invalidateTableCache(String localName) {
    T cached = tables.getIfPresent(localName);
    tableNames.compute("", (ignored, oldValue) ->
            (oldValue == null ? NameCacheValue.empty() : oldValue)
                    .withoutLocalName(localName));
    tables.invalidateKey(localName);
    if (cached != null) {
        tableIdToName.remove(cached.getId(), localName);
    }
    tableIdToName.entrySet().removeIf(entry -> entry.getValue().equals(localName));
}
```

注销路径不再根据名称重新生成 ID，也不单独维护 lower-case side map；对象已被淘汰时按 local cache key 清理残留映射。

该 helper 只替换 `unregisterTable()` 中原有的 `metaCache.invalidate(...)` 和 `lowerCaseToTableName.remove(...)`。原方法的 `makeSureInitialized()`、更新时间更新、`getTableForReplay()` 对象检查、`isInitialized()` 条件以及后续 `ExternalMetaCacheMgr.invalidateTableCache(...)` 调用必须全部保留。

### 7.12 resetMetaCacheNames()

当前 `ExternalDatabase.resetMetaCacheNames()` 仍被创建、删除、rename table 等路径调用，例如：

```text
HiveMetadataOps
PaimonMetadataOps
IcebergMetadataOps
MaxComputeMetadataOps
```

其语义是“只刷新 table names cache，让新建或变更 table 能立即被 list 到”，不能清理 object cache，也不能清理 `tableIdToName`。

迁移后必须保留等价 names-only helper：

```java
public void resetMetaCacheNames() {
    invalidateTableNamesOnly();
}

private void invalidateTableNamesOnly() {
    if (tableNames != null) {
        tableNames.invalidateAll();
    }
}
```

要求：

- 只 invalidate `tableNames`。
- 不 invalidate `tables`。
- 不 clear `tableIdToName`。
- 保持 CREATE TABLE / DROP TABLE / RENAME TABLE 后只刷新 names list 的现有行为。
- names value 失效后 replay-by-name 仍先按精确 object cache key 查询，不依赖远端重载 index。

### 7.13 resetMetaToUninitialized()

替换：

```java
metaCache.invalidateAll();
```

为：

```java
private void invalidateAllTableCache() {
    if (tableNames != null) {
        tableNames.invalidateAll();
    }
    if (tables != null) {
        tables.invalidateAll();
    }
    tableIdToName.clear();
}
```

注意：

- `invalidateAllTableCache()` 是全量清理，用于 database reset 场景。
- `invalidateTableNamesOnly()` 是 names-only 清理，用于 `resetMetaCacheNames()` 创建/变更路径。
- 两者语义不能混用。
- `resetMetaToUninitialized()` 不再创建新的 lower-case map；invalidate `tableNames` 即同时丢弃 names list 和 index。

### 7.14 测试辅助接口

现有测试辅助接口保持不变：

```java
ExternalDatabase.addTableForTest(...)
```

当前 `addTableForTest()` 只写 object cache 和 ID-to-name 映射，不修改 names cache。迁移后保持等价实现：

```java
public void addTableForTest(T table) {
    buildMetaCache();
    tables.put(table.getName(), table);
    tableIdToName.put(table.getId(), table.getName());
}
```

不得在该 helper 中额外修改 `tableNames`。已有 FE UT 继续通过原方法注入测试对象，不要求调用方了解新的 cache 字段拆分。

## 8. 删除 Legacy MetaCache

### 8.1 删除类

删除：

```text
fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/MetaCache.java
fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/LegacyMetaCacheFactory.java
```

### 8.2 修改 ExternalMetaCacheMgr

删除字段：

```java
private final LegacyMetaCacheFactory legacyMetaCacheFactory;
```

删除初始化：

```java
legacyMetaCacheFactory = new LegacyMetaCacheFactory(commonRefreshExecutor);
```

删除方法：

```java
public LegacyMetaCacheFactory legacyMetaCacheFactory()
```

如 `ExternalCatalog` / `ExternalDatabase` 需要 refresh executor，新增受控 getter：

```java
public ExecutorService commonRefreshExecutor() {
    return commonRefreshExecutor;
}
```

或者提供更窄的 factory 方法构造 `MetaCacheEntry`。为了改造简单，建议先提供 executor getter。

### 8.3 删除 Config

删除：

```java
enable_external_meta_cache_manual_miss_load
```

同步删除对应 tests、文档说明、配置校验引用。

## 9. copy-on-write 的定位与实现边界

copy-on-write 不是新方案为自身引入的并发问题增加的补丁。当前 Legacy `MetaCache` 已经在 `namesCache.asMap().compute()` 中对缓存的可变 `List` 原地执行 `add/removeIf`，而 `listNames()/getRemoteName()` 会在 cache 锁外遍历同一个 List。`compute()` 只能保证 key 到 value 引用的映射原子，不能保护 value 内部结构，因此当前实现本来就存在并发遍历不稳定风险。

新方案将 copy-on-write 收口在不可变 `NameCacheValue` 内：

```java
databaseNames.compute("", (ignored, oldValue) ->
        (oldValue == null ? NameCacheValue.empty() : oldValue)
                .withName(remoteName, localName));

tableNames.compute("", (ignored, oldValue) ->
        (oldValue == null ? NameCacheValue.empty() : oldValue)
                .withoutLocalName(localName));
```

每次 mutation 生成一个完整的新 `NameCacheValue`：

- 已持有旧 value 的读取线程继续读取稳定旧快照。
- `compute()` 完成后，新读取线程看到完整新快照。
- names list 与 lower-case index 同时切换，不存在一边新、一边旧。
- 旧 value 不再发生原地修改。

register/unregister 是低频元数据路径，O(N) 复制成本可以接受；names 查询和大小写解析是高频读路径，应优先保证无锁稳定读取和 O(1) index 查询。

## 10. 关键兼容边界

必须保持不变：

- `ExternalCatalog` 对外方法名、参数、返回值。
- `ExternalDatabase` 对外方法名、参数、返回值。
- FE image / edit log 格式。
- FE-BE 协议。
- 外表对象持久化结构。

可以改变：

- `MetaCache` 内部 API。
- `LegacyMetaCacheFactory`。
- `ExternalCatalog` / `ExternalDatabase` 内部字段和 private helper。

### 10.1 系统库与系统表兼容约束

`information_schema` 和 `mysql` 不是普通远端数据库。重构只能替换其缓存承载方式，不能把专用处理合并到普通 external catalog 加载路径中。

必须保留以下行为：

1. `ExternalCatalog.getDbNullable(String)` 在进入通用大小写和 cache key 处理前，继续通过 `equalsIgnoreCase()` 将输入规范化为 `InfoSchemaDb.DATABASE_NAME` 或 `MysqlDb.DATABASE_NAME`。
2. `getFilteredDatabaseNames()` 继续在 database names loader 中注入 `information_schema` 和 `mysql`，并确保 include/exclude database 过滤规则不删除这两个系统库。
3. `buildDbForInit()` 继续使用 `ExternalInfoSchemaDatabase` 和 `ExternalMysqlDatabase` 的专用构造分支，不通过普通 `logType` 分支创建。
4. `ExternalDatabase.listTableNames()` 对系统库继续读取 `ExternalInfoSchemaDatabase.listTableNames()` 或 `ExternalMysqlDatabase.listTableNames()` 的本地定义，不调用远端 `extCatalog.listTableNames()`。
5. 系统库和系统表 names 也封装成 `NameCacheValue`，但 `NameCacheValue` 只负责不可变 names/index 快照，不改变上述数据来源和过滤语义。

实现时不得为了统一 helper 而删除这些特殊分支，否则会导致系统库大小写查询失败，或错误地向远端 catalog 请求 Doris 系统表。

## 11. 测试开发清单

### 11.1 MetaCacheEntryTest

需要新增或修改以下测试：

1. `get()` 固定走 manual miss load。
2. cache disabled 时 `get()` 执行 loader，但不写入 cache。
3. public `put()` bump generation。
4. internal load write-back 不 bump generation。
5. load 后 put，再二次 generation check，stale 时移除自己写入的值。
6. `compute()` bump generation。
7. `invalidateKey()` 只 bump 当前 stripe。
8. `invalidateAll()` bump 所有 stripe。
9. loader 返回 `null` 时 `get()` 返回 `null`，且 `getIfPresent()` 仍为 miss。
10. loader 返回 `null` 后再次调用 `get()` 会重新执行 loader。
11. public `put()` 拒绝 null key/value。
12. `refreshAfterWrite` reload 在 generation 变化后不写回。
13. `withSyncRemovalListener()` 使用同步 listener executor，且 Caffeine policy 中不存在 refreshAfterWrite。
14. generation 变化时 `asyncReload()` 返回的最终 future 进入直接 cancelled 状态，不通过 `CompletionException` 包装 `CancellationException`。
15. contextual-only entry 拒绝非 null 默认 loader，并拒绝 `autoRefresh=true`；普通 entry 拒绝 null loader。

原 `MetaCacheTest` 中必须迁移到 `MetaCacheEntryTest` 的语义：

| 原测试语义                                                    | 新测试落点                                                   |
| -------------------------------------------------------- | ------------------------------------------------------- |
| `Optional.empty()` 不作为有效命中，后续 `getMetaObj()` 重新 load     | loader 返回 `null` 时不缓存，后续 `MetaCacheEntry.get()` 重新 load |
| 多线程同时访问不存在 key，且 loader 返回对象时只触发一次 loader              | `MetaCacheEntry.get()` 并发去重                             |
| 多线程访问首次 loader 返回对象的 cold-miss key，只产生一个有效缓存对象          | `MetaCacheEntry.get()` load lock 与写回测试                    |

不再保留“并发访问已缓存的 `Optional.empty()`”测试，因为新设计不会缓存空值。若 loader 持续返回 `null`，等待同一 stripe 的后续线程可能依次重新执行 loader，这是不启用 negative cache 的预期语义。

原 `MetaCacheDeadlockTest` 中必须迁移到 `MetaCacheEntryTest` 或新的 metacache UT 的语义：

| 原测试语义                                                                                     | 新测试落点                                       |
| ----------------------------------------------------------------------------------------- | ------------------------------------------- |
| object cache 使用 sync removal listener，listener 内部调用另一个 cache 的 `invalidateAll()` 不发生线程池死锁 | `MetaCacheEntry` sync removal listener 构造路径 |
| sync removal listener 不能与 refreshAfterWrite 组合                                            | `withSyncRemovalListener()` 固定关闭 auto refresh，并验证 Caffeine policy |

### 11.2 NameCacheValueTest

需要覆盖：

1. `of()` 同时生成不可变 names list 和正确的 lower-case remote-name index。
2. `remoteNameOfLocalName()` 保持现有 remote/local mapping 语义。
3. `remoteNameForCaseInsensitiveLookup()` 支持 mode 2 大小写不敏感查找并返回原始 remote name。
4. `withName()` 返回新对象，不修改旧 value。
5. `withoutLocalName()` 返回新对象，不修改旧 value。
6. names list 和 lower-case index 在 add/remove 后始终属于同一版本。
7. 暴露的 names list 不可由调用方修改。
8. `containsLocalName()` 按 local name 判断存在性。
9. `withName()` 对相同 remote/local pair 重复调用时保持单条记录，不产生重复项。
10. `withName()` 在同一个精确 remote name 已映射到其他 local name 时拒绝更新。

### 11.3 ExternalCatalog 相关测试

建议覆盖：

1. cold miss 下 `getRemoteDatabaseName()` 能触发 names load。
2. cache hit 下直接使用同一个 `NameCacheValue` 的 lower-case index，不重建 side map。
3. `getDbNullable(String)` 使用 `databases.get()`，loader 返回 `null` 时不缓存。
4. `getDbForReplay(String/long)` cache-only，不触发远端 load；long 版本必须覆盖 `dbIdToName` 命中但 object entry miss 的场景。
5. `registerDatabase()` 更新 object entry、names entry、`dbIdToName`。
6. `unregisterDatabase()` 清理 object entry、names entry、`dbIdToName`。
7. `invalidateAllDatabaseCache()` 清理两个 entries 和 id map。
8. `resetMetaCacheNames()` 只清理 `databaseNames`，不清理 `databases` 和 `dbIdToName`。
9. `getLocalDatabaseName(isReplay=false)` miss 时通过 `databaseNames.get("")` load-through，不直接调用 `getFilteredDatabaseNames()`。
10. `getLocalDatabaseName(isReplay=true)` 保持 cache-only。
11. `getDbNullable(String)` 使用 `db.getId()` 更新 `dbIdToName`，不重新生成 ID。
12. names-only invalidation 后，`getDbForReplay(String)` 仍可按规范名称精确命中 object entry，不触发 names load。
13. `buildDbForInit()` 第一次不存在判断失败后，通过 `databaseNames.invalidateKey("")` 和 `databaseNames.get("")` 完成二次确认，不直接调用 `getFilteredDatabaseNames()`。
14. `buildDbForInit()` Step 3 保留现有外层条件，仅在需要 remote-name 解析时调用 `getRemoteDatabaseName()`；mode 0 无 mapping 时不加载 names entry。
15. `addDatabaseForTest()` 只写 `databases` 和 `dbIdToName`；`setInitializedForTest(true)` 能初始化两个 entries。
16. `information_schema` / `mysql` 的混合大小写查询仍返回专用 database 对象。
17. database names 的 include/exclude 过滤不会删除 `information_schema` 和 `mysql`。
18. 普通 FE UT 中 `FeConstants.runningUnitTest=true` 且不是 `TestExternalCatalog` 时，`buildDbForInit(checkExists=true)` 不触发 database names load/reload。
19. `TestExternalCatalog` 在 FE UT 中调用 `buildDbForInit(checkExists=true)` 时仍执行存在性检查和二次 names reload。
20. `getDbForReplay(String/long)` 在 `isInitialized()==false` 时直接返回 empty，且不访问未构造的 cache entries。
21. `unregisterDatabase()` 替换本地缓存条目后仍调用 `ExternalMetaCacheMgr.invalidateDb(...)`。
22. `HMSExternalCatalog.registerDatabase()` 通过 `updateDatabaseCache()` 同时更新 object entry、names entry 和 `dbIdToName`，且 ID 来自 `db.getId()`。
23. `updateDatabaseCache()` 遇到精确 remote-name 冲突时抛出异常，且 `databases`、`databaseNames`、`dbIdToName` 均保持调用前状态。

### 11.4 ExternalDatabase 相关测试

建议覆盖：

1. cold miss 下 `getRemoteTableName()` 能触发 names load。
2. cache hit 下直接使用同一个 `NameCacheValue` 的 lower-case index，不重建 side map。
3. `getTableNullable(String)` 使用 `tables.get()`，loader 返回 `null` 时不缓存。
4. `getTableForReplay(String/long)` cache-only，不触发远端 load；long 版本必须覆盖 `tableIdToName` 命中但 object entry miss 的场景。
5. `registerTable()` 更新 object entry、names entry、`tableIdToName`。
6. `unregisterTable()` 清理 object entry、names entry、`tableIdToName`。
7. `invalidateAllTableCache()` 清理两个 entries 和 id map。
8. `resetMetaCacheNames()` 只清理 `tableNames`，不清理 `tables` 和 `tableIdToName`。
9. `getLocalTableName(isReplay=false)` miss 时通过 `tableNames.get("")` load-through，不直接调用 `listTableNames()`。
10. `getLocalTableName(isReplay=true)` 保持 cache-only。
11. `isTableExist()` 大小写不敏感 miss 时通过 `tableNames.get("")` load-through，不直接调用 `listTableNames()`。
12. `getTableNullable(String)` 使用 `table.getId()` 更新 `tableIdToName`，不重新生成 ID。
13. names-only invalidation 后，`getTableForReplay(String)` 仍可按规范名称精确命中 object entry，不触发 names load。
14. `buildTableForInit()` 二次存在性确认通过 `resetMetaCacheNames()` 和 `tableNames.get("")` 完成，不直接调用 `listTableNames()`。
15. `buildTableForInit()` Step 3 保留现有外层条件，仅在需要 remote-name 解析时调用 `getRemoteTableName()`；mode 0 无 mapping 时不加载 names entry。
16. `addTableForTest()` 只写 `tables` 和 `tableIdToName`。
17. `information_schema` / `mysql` 的 table names 从 Doris 本地系统表定义获取，不调用远端 catalog。
18. 普通 FE UT 中 `FeConstants.runningUnitTest=true` 且不是 `TestExternalDatabase` 时，`buildTableForInit(checkExists=true)` 不触发 table names load/reload。
19. `TestExternalDatabase` 在 FE UT 中调用 `buildTableForInit(checkExists=true)` 时仍执行存在性检查和二次 names reload。
20. `getTableForReplay(String/long)` 在 `isInitialized()==false` 时直接返回 empty，且不访问未构造的 cache entries。
21. `unregisterTable()` 替换本地缓存条目后仍保留对象检查、更新时间更新和 `ExternalMetaCacheMgr.invalidateTableCache(...)` 调用。
22. `updateTableCache()` 遇到精确 remote-name 冲突时抛出异常，且 `tables`、`tableNames`、`tableIdToName` 均保持调用前状态。

## 12. 开发顺序建议

建议按以下顺序开发，降低一次性改动复杂度：

1. 新增不可变 `NameCacheValue` 及其 UT。
2. 增强 `MetaCacheEntry`。
3. 调整 `MetaCacheEntryTest`，确保新基础能力正确。
4. 改造 `ExternalCatalog` 字段和内部 helper。
5. 改造 `ExternalDatabase` 字段和内部 helper。
6. 删除 `MetaCache` / `LegacyMetaCacheFactory`。
7. 清理 `ExternalMetaCacheMgr` 中 legacy factory。
8. 删除 `Config.enable_external_meta_cache_manual_miss_load`。
9. 修复编译错误和 imports。
10. 补充/调整 FE UT。

## 13. 自检清单

开发完成后必须检查：

- [ ] 全仓库没有 `LegacyMetaCacheFactory` 引用。
- [ ] 全仓库没有 `new MetaCache` 引用。
- [ ] 全仓库没有 `MetaCache<` 类型引用。
- [ ] 全仓库没有 `enable_external_meta_cache_manual_miss_load` 引用。
- [ ] `ExternalCatalog` / `ExternalDatabase` 对外方法签名未变化。
- [ ] 新增 cache 字段没有 `@SerializedName`。
- [ ] object cache 不配置 refreshAfterWrite。
- [ ] object cache value 直接使用对象类型，不使用 `Optional<T>`。
- [ ] object loader 返回 `null` 时不写入 cache，后续访问仍可重新 load。
- [ ] public `put()` 拒绝 null key/value。
- [ ] `withSyncRemovalListener()` 固定关闭 refreshAfterWrite，public API 不暴露两者同时开启的组合入口。
- [ ] private 底层构造器保留 contextual-only 的 loader/null 和 autoRefresh 校验。
- [ ] generation 变化时 `asyncReload()` 直接 cancel 最终返回 future，不返回 `oldValue`，也不通过 composed child future 包装 cancellation。
- [ ] names entry value 使用不可变 `NameCacheValue`。
- [ ] names list mutation 使用 copy-on-write，不修改旧 value。
- [ ] `NameCacheValue.withName()` 保证精确 remote name 不映射到多个 local name，同时保持相同 remote/local pair 重复注册幂等。
- [ ] 大小写相关 remote-name 冲突仍由具有 catalog 配置上下文的 loader/调用方判断，不在 `NameCacheValue` 中无条件处理。
- [ ] update database/table cache 的顺序为参数校验、names compute、object put、ID map put；可能抛冲突异常的 names compute 不得放在 object put 之后。
- [ ] 不再保留独立 `lowerCaseToDatabaseName/lowerCaseToTableName` side map。
- [ ] cache hit 不重建 lower-case index。
- [ ] `dbIdToName/tableIdToName` 的 key 来自实际对象 `getId()`。
- [ ] replay 方法在访问 cache entry 前检查 `isInitialized()`，未初始化时直接返回 empty。
- [ ] replay-by-id 只使用 ID map 和 object entry 的 `getIfPresent()`，不触发远端 load。
- [ ] names loader 不通过副作用修改独立 lower-case 状态。
- [ ] `getLocalDatabaseName()` miss path 不直接调用 `getFilteredDatabaseNames()`。
- [ ] `buildDbForInit()` 二次存在性确认只通过 `databaseNames` entry reload，不直接调用 `getFilteredDatabaseNames()`。
- [ ] `buildDbForInit()` 保留 `checkExists && (!FeConstants.runningUnitTest || this instanceof TestExternalCatalog)` 条件。
- [ ] `buildDbForInit()` Step 3 保留原外层条件，并在条件成立时使用 `getRemoteDatabaseName()`，不再调用 `metaCache.getRemoteName()`。
- [ ] `getLocalTableName()` / `isTableExist()` miss path 不直接调用 `listTableNames()`。
- [ ] `buildTableForInit()` 二次存在性确认只通过 `tableNames` entry reload，不直接调用 `listTableNames()`。
- [ ] `buildTableForInit()` 保留 `checkExists && (!FeConstants.runningUnitTest || this instanceof TestExternalDatabase)` 条件。
- [ ] `buildTableForInit()` Step 3 保留原外层条件，并在条件成立时使用 `getRemoteTableName()`，不再调用 `metaCache.getRemoteName()`。
- [ ] invalidateAll 路径清理 id map。
- [ ] `resetMetaCacheNames()` 只清理 names entry，不清理 object entry 和 id map。
- [ ] unregister database/table 只替换本地 cache mutation，保留原方法中的初始化、对象检查、更新时间及 `ExternalMetaCacheMgr` 失效调用。
- [ ] `addDatabaseForTest()` / `addTableForTest()` 只写 object entry 和 id map，不修改 names entry。
- [ ] `setInitializedForTest(true)` 仍构造完整 cache entries。
- [ ] `HMSExternalCatalog.registerDatabase()` 不再引用 `metaCache`，改为调用 `protected final updateDatabaseCache()`。
- [ ] `information_schema` / `mysql` 的名称规范化、专用对象构造和本地系统表枚举分支保持不变。
- [ ] `MetaCacheEntry` 不暴露 public mutable `asMap()`。

可使用：

```bash
rg -n "LegacyMetaCacheFactory|new MetaCache|MetaCache<|enable_external_meta_cache_manual_miss_load" fe
rg -n "asMap\\(\\).*compute|asMap\\(\\).*remove|asMap\\(\\).*keySet" fe/fe-core/src/main/java/org/apache/doris/datasource
rg -n "lowerCaseToDatabaseName|lowerCaseToTableName" fe/fe-core/src/main/java/org/apache/doris/datasource
rg -n "metaCache\\.getRemoteName|getFilteredDatabaseNames\\(\\)|listTableNames\\(\\)" \
  fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalCatalog.java \
  fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalDatabase.java
```

最后一条搜索允许 `getFilteredDatabaseNames()` 和 `listTableNames()` 出现在对应 names entry loader 及方法定义中；业务 miss path、`buildDbForInit()` 和 `buildTableForInit()` 中不得再直接调用。

## 14. 本次不做的事项

本次改造不做：

- 不把新拆出的 catalog/database legacy entries 纳入 `information_schema.catalog_meta_cache_statistics`。
- 不改变 FE image / edit log / FE-BE 协议。
- 不引入新的 `ExternalMetaCacheStore`。
- 不保留 `Config.enable_external_meta_cache_manual_miss_load` 回滚开关。
- 不重构 `ExternalCatalog` / `ExternalDatabase` 的 Java monitor 锁顺序。

## 15. 预期结果

完成后：

- 外表元数据缓存统一基于增强版 `MetaCacheEntry`。
- Legacy `MetaCache` 包装层被删除。
- `ExternalCatalog` / `ExternalDatabase` 对外 API 保持不变。
- Legacy names/object cache miss load 不再走 Caffeine 同步 load 路径。
- refresh / put / compute / invalidate 与 in-flight load 并发时具备 generation 保护。
- names list 与 lower-case index 通过不可变 `NameCacheValue` 形成同一缓存版本。
- copy-on-write 修复 Legacy `MetaCache` 当前共享可变 List 的并发风险。
- ID 映射以实际对象 ID 为准。
- 代码结构更直接，后续扩展和审查成本更低。
