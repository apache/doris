# Doris 数据湖元数据缓存文档索引

> 本文档集梳理了 Doris FE 中数据湖元数据缓存的完整架构和代码逻辑，提供从分析到实施的完整方案。

## 📚 文档列表

### 🆕 统一缓存架构设计

#### [统一数据湖元数据分层缓存架构](./unified_datalake_cache_architecture.md)
**推荐指数**: ⭐⭐⭐⭐⭐ **【重点推荐】**

**适合人群**: 架构师、核心开发者、需要设计通用方案的技术负责人

**内容概要**:
- ✅ 支持所有数据湖格式的统一抽象（Iceberg/Hudi/Paimon/Delta/MaxCompute）
- ✅ 四层通用缓存架构（TableMeta/Version/Schema/Partition）
- ✅ 格式适配器设计模式（易于扩展新格式）
- ✅ 完整的实施计划（14周，分5个阶段）
- ✅ 性能对比和监控方案
- ✅ 向后兼容和迁移策略

**核心亮点**:
- 🎯 一套架构支持所有数据湖格式
- 🎯 新增格式只需 3-5 天（vs 当前 2-3 周）
- 🎯 预期性能提升 50%+
- 🎯 统一监控和管理

---

### Iceberg 专项文档

#### [Iceberg 分层缓存实现方案](./iceberg_tiered_cache_implementation.md)
**推荐指数**: ⭐⭐⭐⭐⭐

**适合人群**: 需要快速实施 Iceberg 优化的开发者

**内容概要**:
- ✅ Iceberg 专属的四层缓存设计
- ✅ 详细的实现步骤（9-11周）
- ✅ 完整的代码示例和接口设计
- ✅ 测试方案和性能优化建议
- ✅ 灰度上线和回滚策略

**适用场景**:
- 只需要优化 Iceberg，不涉及其他格式
- 需要快速上线
- 作为统一架构的前期试点

---

### 1. [详细架构与代码梳理](./iceberg_metadata_cache_analysis.md)
**推荐指数**: ⭐⭐⭐⭐⭐

**适合人群**: 需要深入理解架构、准备进行重构的开发者

**内容概要**:
- ✅ 完整的组件层次结构和职责划分
- ✅ 核心类的详细解析（IcebergMetadataCache、ExternalSchemaCache 等）
- ✅ 四种缓存类型的详细说明（Table、Snapshot、Schema、View）
- ✅ 数据加载流程的完整说明
- ✅ 缓存失效机制详解
- ✅ 配置参数和线程池说明
- ✅ **潜在的优化点和重构建议** ⚡
- ✅ 所有相关代码文件清单

**关键章节**:
- 第二章: 核心类详解（重点）
- 第三章: 数据加载流程
- 第八章: 潜在的优化点（重构必读）

---

### 2. [架构图与流程图集](./iceberg_cache_diagrams.md)
**推荐指数**: ⭐⭐⭐⭐⭐

**适合人群**: 喜欢图形化理解、需要快速掌握架构的开发者

**内容概要**:
- ✅ 整体架构图（组件关系）
- ✅ Table 缓存加载时序图
- ✅ Schema 缓存加载时序图
- ✅ Snapshot 缓存加载时序图（MTMV）
- ✅ 缓存失效流程图
- ✅ 缓存键值关系类图
- ✅ 配置和线程池架构图
- ✅ 查询执行中的缓存使用流程
- ✅ 重构优化方向图
- ✅ 典型使用场景时序图（3个场景）

**使用方式**:
- 支持 Mermaid 语法
- 可在 GitHub、GitLab 直接渲染
- 可复制到 [Mermaid Live Editor](https://mermaid.live/) 查看

---

### 3. [快速参考手册](./iceberg_cache_quick_reference.md)
**推荐指数**: ⭐⭐⭐⭐

**适合人群**: 日常开发和问题排查、需要快速查找 API 的开发者

**内容概要**:
- ✅ 核心类和接口速查
- ✅ 关键配置参数表格
- ✅ 常用调试命令
- ✅ 核心数据结构速览
- ✅ 典型代码模式（4种）
- ✅ 性能优化建议（4个方面）
- ✅ 注意事项（5个重点）
- ✅ 常见问题排查（4个 Q&A）

**特色**:
- 表格化呈现，查找方便
- 代码示例完整可用
- 问题排查有明确步骤

---

## 🎯 快速导航

### 场景 0: 我要设计统一的数据湖缓存架构

**推荐阅读顺序**:
1. **必读**：[统一数据湖元数据分层缓存架构](./unified_datalake_cache_architecture.md) - 完整方案
2. 参考：[Iceberg 详细架构分析](./iceberg_metadata_cache_analysis.md) 第八章「潜在的优化点」
3. 参考：[Iceberg 分层缓存实现方案](./iceberg_tiered_cache_implementation.md) - 具体实现细节
4. 对照：[架构图集](./iceberg_cache_diagrams.md) 的第 9 个图「重构优化方向」

**预计时间**: 3-4 小时

**关键收获**:
- 理解如何抽象不同数据湖格式的共性
- 学习适配器模式在元数据缓存中的应用
- 掌握完整的实施路径和风险控制

---

### 场景 1: 我是新手，想快速了解架构

**推荐阅读顺序**:
1. 先看 [架构图集](./iceberg_cache_diagrams.md) 的第 1-3 个图
2. 再看 [详细架构文档](./iceberg_metadata_cache_analysis.md) 的第一、二章
3. 参考 [快速参考手册](./iceberg_cache_quick_reference.md) 的核心类速查

**预计时间**: 30-45 分钟

---

### 场景 2: 我要进行优化重构

**方案 A: 统一架构重构（推荐）**

**推荐阅读顺序**:
1. **首选**：[统一数据湖元数据分层缓存架构](./unified_datalake_cache_architecture.md) - 一次性解决所有格式
2. 精读第二、三、四章理解核心设计
3. 精读第七章「实施计划」制定时间表
4. 参考 [Iceberg 分层缓存实现方案](./iceberg_tiered_cache_implementation.md) 的具体实现细节

**预计时间**: 3-4 小时

**方案 B: 仅 Iceberg 重构（快速）**

**推荐阅读顺序**:
1. 阅读 [Iceberg 分层缓存实现方案](./iceberg_tiered_cache_implementation.md)
2. 精读 [详细架构文档](./iceberg_metadata_cache_analysis.md) 的第八章「潜在的优化点」
3. 对照 [架构图集](./iceberg_cache_diagrams.md) 的第 9 个图「重构优化方向」
4. 参考 [快速参考手册](./iceberg_cache_quick_reference.md) 的性能优化建议

**预计时间**: 2-3 小时

---

### 场景 3: 我在开发功能，需要使用缓存

**推荐阅读顺序**:
1. 直接查看 [快速参考手册](./iceberg_cache_quick_reference.md) 的核心接口和代码模式
2. 如果需要理解原理，参考 [架构图集](./iceberg_cache_diagrams.md) 对应的时序图
3. 遇到问题查看 [快速参考手册](./iceberg_cache_quick_reference.md) 的常见问题排查

**预计时间**: 15-30 分钟

---

### 场景 4: 我在排查问题

**推荐阅读顺序**:
1. 优先查看 [快速参考手册](./iceberg_cache_quick_reference.md) 的常见问题排查
2. 使用调试命令查看缓存统计
3. 如果需要深入理解，参考 [详细架构文档](./iceberg_metadata_cache_analysis.md) 对应章节

**预计时间**: 10-20 分钟

---

## 🔍 核心概念速查

### 四种缓存类型

| 缓存 | 存储内容 | 主要用途 | 重要性 |
|-----|---------|---------|--------|
| **tableCache** | org.apache.iceberg.Table | 存储原生 Table 对象，最核心 | ⭐⭐⭐⭐⭐ |
| **snapshotCache** | IcebergSnapshotCacheValue | MTMV 的 snapshot 和分区信息 | ⭐⭐⭐ |
| **schemaCache** | IcebergSchemaCacheValue | 表的 Schema 和分区列 | ⭐⭐⭐⭐⭐ |
| **viewCache** | org.apache.iceberg.view.View | Iceberg View 对象 | ⭐⭐ |

### 关键类速查

| 类名 | 作用 | 位置 |
|-----|------|------|
| **IcebergMetadataCache** | 核心缓存实现 | `datasource/iceberg/IcebergMetadataCache.java` |
| **ExternalSchemaCache** | Schema 缓存（独立） | `datasource/ExternalSchemaCache.java` |
| **ExternalMetaCacheMgr** | 统一缓存管理入口 | `datasource/ExternalMetaCacheMgr.java` |
| **IcebergUtils** | 工具类（大部分缓存访问） | `datasource/iceberg/IcebergUtils.java` |
| **IcebergMetadataOps** | 元数据操作（实际加载） | `datasource/iceberg/IcebergMetadataOps.java` |

### 最常用的 API

```java
// 1. 获取 Table（最常用）
Table table = Env.getCurrentEnv()
    .getExtMetaCacheMgr()
    .getIcebergMetadataCache()
    .getIcebergTable(dorisTable);

// 2. 获取 Schema
IcebergSchemaCacheValue schema = IcebergUtils.getSchemaCacheValue(dorisTable, schemaId);

// 3. 失效缓存
Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(dorisTable);
```

---

## 📊 重点关注的优化方向

### 🆕 统一架构方向（强烈推荐）

**为什么需要统一架构？**

当前 Doris 支持多种数据湖格式：
- Iceberg：有完善的缓存
- Hudi：有独立的缓存实现
- Paimon：有基础的缓存
- MaxCompute：有简单的缓存
- Delta Lake：未来可能支持

**问题**：
- 每种格式独立实现，重复造轮子
- 缓存粒度和策略不一致
- 难以统一管理和监控
- 新增格式成本高（2-3周）

**统一架构方案**: 
见 [统一数据湖元数据分层缓存架构](./unified_datalake_cache_architecture.md)

**预期收益**:
- ✅ 新增格式成本降低 70%（3-5天）
- ✅ 性能提升 50%+
- ✅ 统一的监控和运维
- ✅ 更好的扩展性

---

### Iceberg 专项优化方向

根据文档梳理，以下是 Iceberg 特定的优化方向（详见 [详细架构文档](./iceberg_metadata_cache_analysis.md) 第八章）:

### 🔥 优先级 HIGH

1. **统一 Schema 和 Table 缓存**
   - 当前 Schema 和 Table 分离，需要两次查询
   - 建议将 Schema 信息合并到 Table 缓存值中

2. **分区信息缓存优化**
   - 当前每次扫描 Partitions 元数据表，效率低
   - 建议实现 Manifest 级别的分区缓存

3. **增量更新机制**
   - 当前失效策略粗暴（全量清空）
   - 建议利用 Iceberg transaction log 实现增量更新

### 🟡 优先级 MEDIUM

4. **分层缓存架构**
   - 按元数据级别分层（Metadata/Snapshot/Schema/Partition）
   - 实现更细粒度的缓存控制

5. **按需加载策略**
   - 支持字段级懒加载
   - 区分轻量级和重量级元数据

### 🟢 优先级 LOW

6. **监控和指标增强**
   - 添加更详细的缓存命中率统计
   - 区分不同类型缓存的性能指标

---

## 🛠️ 开发工具

### 查看缓存统计
```sql
SHOW PROC '/catalog_meta_cache/{catalog_name}';
```

### 刷新缓存
```sql
-- 刷新 catalog
REFRESH CATALOG catalog_name;

-- 刷新表
REFRESH TABLE catalog_name.db_name.table_name;
```

### 启用 DEBUG 日志
```properties
# fe.conf
sys_log_level = DEBUG
```

### 关键日志搜索
```bash
# 缓存加载
grep "load iceberg table" fe.log

# 缓存失效
grep "invalidate iceberg table cache" fe.log
```

---

## 📈 文档统计

- **总字数**: ~50,000 字
- **代码示例**: 30+ 个
- **架构图**: 10 个
- **配置参数**: 4 个核心参数
- **核心类**: 10+ 个
- **优化建议**: 6 个方向

---

## 🤝 贡献与反馈

如果你在使用过程中发现文档有误或者有改进建议，请：

1. 提交 Issue 说明问题
2. 或直接提交 PR 修正
3. 或在团队群中反馈

---

## 📝 更新日志

### v1.0 (2025-11-05)
- ✅ 完成核心架构梳理
- ✅ 完成代码逻辑分析
- ✅ 完成架构图绘制
- ✅ 完成优化建议整理
- ✅ 完成快速参考手册

---

## 📖 相关资源

### 官方文档
- [Apache Iceberg 官方文档](https://iceberg.apache.org/)
- [Apache Doris 官方文档](https://doris.apache.org/)

### 代码仓库
- [Doris GitHub](https://github.com/apache/doris)

### 相关 Issue
- 可以在这里添加相关的 Issue 链接

---

**文档维护**: Doris 团队
**最后更新**: 2025-11-05
**适用版本**: Doris 主分支

---

> 💡 **提示**: 建议从适合自己的场景开始阅读，不必按顺序通读所有文档。

