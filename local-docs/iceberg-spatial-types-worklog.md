# Iceberg 空间类型工作日志

日期：2026-10-11  
开发基线：`master`  
任务 worktree：`/mnt/disk4/zhangyuze/doris-worktrees/master-67907`  
任务分支：`feature/iceberg-spatial-types`  
关联 PR：[#67907](https://github.com/apache/doris/pull/67907)

## 1. 需求重新分析

本任务的目标是在 Apache Doris `master` 分支上，以可审查、可验证、可持续维护的方式支持 Iceberg V3 的 `GEOMETRY` 和 `GEOGRAPHY` 类型。

最终需要形成一条完整链路：

```
Iceberg V3 schema
    -> FE 类型解析与元数据保留
    -> Doris/Iceberg schema 双向映射
    -> Nereids SQL 分析与写入校验
    -> BE 空间值读取、WKB 转换和函数计算
    -> 写回 Iceberg 并重新读取验证
```

需求重点不是单独增加两个类型，而是保证类型元数据、数据表示、分析校验和读写行为在整条链路上一致。

## 2. PR #67907 已有内容

根据已读取的 PR 相关聊天和评审记录，原 PR 已经完成或部分完成以下工作：

- 增加 `GEOMETRY`、`GEOGRAPHY` 类型基础设施。
- 保存 CRS、Geography Edge Algorithm 等空间类型元数据。
- 增加 Doris 与 Iceberg 空间类型的双向 schema 映射。
- 支持空间值读取、空间函数计算和写回，持久化使用标准原始 WKB。
- 修复空间函数的 NULL 传播。
- 修复投影 `GEOMETRY(EPSG:3857)` 的原始 WKB 访问。
- 增强 legacy WKB 对带 SRID 的 EWKB 兼容解析。
- 修复 Varbinary/Arrow 长度转换和空间类型校验顺序。
- 增加类型解析、转换、WKB 读写和空间函数测试。
- 已完成部分格式检查、静态检查和定向编译验证。

原 PR 还明确排除了空间索引、bounds 裁剪、谓词下推、Paimon、Lance 和 GeoArrow。

这些内容只能作为实现和评审资料，不能直接视为 `master` 分支上的验证结果。原工作主要在 4.1 分支上推进，提交较分散，且混有构建环境排查和临时验证过程。

## 3. 当前工作判断

### 已确认的基础事实

- mentor 要求后续工作基于 `master`，不再继续在 4.1 上扩展。
- 已创建干净的 master 任务线：
  - 基线：`/mnt/disk4/zhangyuze/doris-worktrees/master-base`
  - 任务 worktree：`/mnt/disk4/zhangyuze/doris-worktrees/master-67907`
  - 分支：`feature/iceberg-spatial-types`
- 任务 worktree 当前从 `master` 的 `dd456ab7a894` 创建，工作区干净。
- 旧 4.1 worktree 和 PR 保留作为历史参考，不作为新的开发基线。

### 需要重新确认的内容

由于 master 与 4.1 存在代码和依赖差异，以下内容必须在 master 上重新定位和验证：

- Iceberg 版本是否仍提供相同的 V3 `GEOMETRY`/`GEOGRAPHY` API。
- FE 类型定义、Thrift 定义和 Nereids 类型转换是否已经存在或发生变化。
- Iceberg schema 解析和 Doris schema 生成的实际入口。
- CREATE、ALTER、ADD COLUMN 的空间类型校验路径。
- BE 端空间列的物理读取、WKB 编码和列转换路径。
- 现有空间函数对 NULL、SRID、CRS 和 Geography metadata 的行为。
- master 当前回归测试环境是否具备 Iceberg V3 所需的 catalog、Parquet 和依赖。

## 4. 工作范围

### 本轮必须完成

1. FE 空间类型和元数据基础设施。
2. Iceberg V3 与 Doris 的双向 schema 映射。
3. CRS 和 Geography Edge Algorithm 的默认值、合法值和往返一致性。
4. CREATE/ALTER/ADD COLUMN 的空间类型校验。
5. 真实 Nereids SQL 分析路径中的空间类型兼容性校验。
6. 标准原始 WKB 的读取、计算和写回。
7. Geometry/Geography 的 NULL、SRID、投影 CRS 和 metadata 行为。
8. Iceberg 端到端写入、读取和空间函数回归。
9. 与 master 代码结构匹配的单元测试、BE 测试和回归测试。

### 明确不纳入本轮

- 空间索引。
- bounds 裁剪。
- 谓词下推。
- Paimon、Lance 或 GeoArrow 支持。
- 重复 WKB 解析优化和大规模 COW/filter 性能压测。
- 与本功能无关的 OIDC、认证插件或 TeamCity 基础设施修复。
- 为绕过本地构建环境而修改 Doris 或第三方依赖源码。

## 5. 分阶段工作计划

### 阶段 0：master 基线审查

- 检查 master 当前空间类型、Iceberg、Thrift 和 Nereids 相关代码。
- 对照旧 PR 变更，建立“可复用、需重写、已不需要”清单。
- 确认 Iceberg 依赖版本和 V3 类型 API。
- 输出 master 上的实际入口、缺口和文件范围。

完成标准：不修改业务代码，形成 master 适配清单。

### 阶段 1：类型与元数据基础设施

- 在 master 当前类型体系中补齐 `GEOMETRY`、`GEOGRAPHY`。
- 处理 CRS、Edge Algorithm、默认值和非法值。
- 补充 Catalog、Nereids、Thrift 的双向转换。
- 添加类型 round-trip 和参数校验测试。

完成标准：FE 类型可以创建、序列化、反序列化并保持 metadata 一致。

### 阶段 2：Iceberg schema 双向映射

- 实现 Iceberg `GEOMETRY`/`GEOGRAPHY` 到 Doris 类型的映射。
- 实现 Doris 类型到 Iceberg V3 类型的映射。
- 统一默认 CRS 的表示方式，避免 Iceberg 的 `null` 默认值与 Doris 显式默认值产生误判。
- 校验 Geography algorithm 的枚举和值域。
- 覆盖嵌套类型和不支持类型的明确失败路径。

完成标准：schema 往返不丢失类型、CRS 和 Geography metadata。

### 阶段 3：DDL 和真实分析链路

- 覆盖 CREATE TABLE、ALTER TABLE、ADD COLUMN。
- 验证 Iceberg V3 和 Parquet 前置条件。
- 增加真实 Nereids `INSERT`、`INSERT ... SELECT` 和必要的 `MERGE` 分析测试。
- 覆盖 Geometry/Geography kind、CRS、algorithm 不匹配。
- 覆盖 NULL 写入可空空间列。

完成标准：错误在正确的分析或 DDL 阶段被发现，错误信息稳定且不依赖直接调用内部 analyzer。

### 阶段 4：空间值读写和函数闭环

- 确认 Iceberg Parquet/ORC 物理值到 Doris 空间列的转换入口。
- 统一标准原始 WKB 的读取和写回格式。
- 验证投影 Geometry 的 WKB 访问。
- 验证 legacy EWKB/SRID 的兼容行为。
- 覆盖 mixed NULL/non-NULL 行。
- 验证 `ST_AsText`、`ST_AsBinary`、`ST_X`、`ST_Y`、`ST_GeometryType`。
- 验证 Geography 距离、关系和长度类函数。

完成标准：写入 Iceberg 后重新读取，值、metadata、NULL 和函数结果均一致。

### 阶段 5：测试、评审和提交整理

- FE 定向单元测试。
- BE 空间函数和 WKB 定向测试。
- Iceberg 外表回归测试。
- 生成并检查回归测试输出，不手写结果文件。
- 执行格式、静态检查和相关编译。
- 按单一主题拆分提交：类型基础设施、schema/DDL、读写闭环、测试修复。
- 做一次范围审查，确认未混入环境脚本、构建产物或无关修改。

完成标准：master 任务分支具备可审查提交历史和完整验证证据。

## 6. 验收标准

- master 上可以识别并保留 Iceberg V3 Geometry/Geography 类型。
- Geometry/Geography schema 双向转换不丢失 CRS 和 Geography algorithm。
- 默认 CRS、非法 CRS、非法 algorithm 和 metadata 不匹配行为有测试。
- CREATE、ALTER、ADD COLUMN 和真实 SQL 分析链路有覆盖。
- 标准 WKB 写入、读取和重新读取闭环通过。
- NULL、SRID、投影 CRS 和 legacy EWKB 行为有明确结果。
- 关键空间函数返回值被断言，而不是只检查执行成功。
- FE、BE 和回归测试结果可区分代码失败与环境失败。
- 不引入空间索引、bounds 裁剪、谓词下推或其他未批准范围。
- 代码提交不包含 `local-docs/`、构建产物或临时环境修改。

## 7. 风险和处理原则

- **master 与 4.1 差异**：先重新定位入口，不直接整批 cherry-pick 旧提交。
- **旧 PR 提交分散**：按功能重新组织提交，旧提交只作参考。
- **FE UT OIDC 失败**：记录为 TeamCity 环境问题，不修改认证插件代码。
- **本地 Lance/Cargo 构建失败**：使用开发机原生 SSH/Remote-SSH 执行；不修改 Doris 源码绕过。
- **Iceberg 依赖或测试环境缺失**：先记录阻塞点，区分环境准备和功能实现，不能用未验证的临时依赖替代正式配置。
- **测试范围扩大**：任何超出本日志范围的功能先单独确认，不在本任务中顺手加入。

## 8. 当前下一步

下一步执行阶段 0：只读审查 master 上的空间类型、Iceberg、Nereids、Thrift 和现有测试入口，形成与旧 PR 的迁移清单。完成后再开始阶段 1，不直接修改业务代码。



## 9. 阶段 0 执行结果（2026-10-11）

### master 当前架构证据

1. Iceberg 类型映射入口已经是 Connector SPI：

   - `fe/fe-connector/fe-connector-iceberg/.../IcebergTypeMapping.java`
   - `fe/fe-connector/fe-connector-iceberg/.../IcebergSchemaBuilder.java`
   - `fe/fe-connector/fe-connector-iceberg/.../IcebergSchemaUtils.java`
   - `fe/fe-connector/fe-connector-iceberg/.../IcebergConnectorMetadata.java`

   旧 PR 记录中的 `fe-core/.../IcebergUtils.java` 和 `DorisTypeToIcebergType.java` 在当前 master 路径中不存在，不能作为直接修改入口。

2. 当前 `IcebergTypeMapping.fromIcebergType` 对 Iceberg V3 的 `GEOMETRY`、`GEOGRAPHY` 和 `TIMESTAMP_NANO` 统一返回 `UNSUPPORTED`。现有测试明确锁定了“表可以加载，但该列不可查询”的降级行为。

3. 当前 `IcebergTypeMapping.toIcebergPrimitive` 只接受 Connector SPI 中已有的标量名称，没有 `GEOMETRY` 或 `GEOGRAPHY` 分支；`IcebergSchemaBuilder.convert` 对未知标量最终会抛出不支持异常。

4. 当前 FE `PrimitiveType` 枚举和 Doris Thrift primitive 类型没有 `GEOMETRY`、`GEOGRAPHY`。因此只修改 Iceberg connector 映射，会得到一个 ConnectorType 名称，但 `ConnectorColumnConverter` 无法把它转换成真正的 Doris 类型，最终仍会降级为 `UNSUPPORTED`。

5. 当前 BE Parquet 读取器已经识别 Parquet 的 `GEOMETRY`/`GEOGRAPHY` logical annotation，但把它们作为 `BYTE_ARRAY` 原始 payload 处理，不建立 Doris 空间类型。现有测试只验证原始 WKB 字节能够解码为字节列。

### 对原 PR 方案的修正判断

原 PR 的“增加 FE 空间类型 + Iceberg 双向映射 + WKB 读写”方向仍然有效，但实现入口和依赖关系已经过时。新的实现必须先决定空间类型在 Doris 内部的正式表示：

- 如果目标是支持 `ST_*` 函数和类型检查，必须新增真正的 FE/Thrift/BE 空间类型，并贯通列、表达式、序列化和读写链路。
- 如果只把 Iceberg 空间列映射成 VARBINARY，只能实现原始字节读写，不能满足原 PR 的空间函数、Geometry/Geography 区分、CRS 和 algorithm 校验目标。
- 因此本任务采用“正式空间类型 + Connector SPI 透传 metadata + WKB 作为物理表示”的方向；不能用 UNSUPPORTED 或普通 VARBINARY 作为最终方案。

### 阶段 0 产出

阶段 0 已完成，形成以下迁移约束：

- 不整批 cherry-pick 4.1 旧提交。
- 先在 master 建立空间类型的最小跨层契约，再接入 Iceberg connector。
- 先确认 Doris Thrift 和 BE 列类型的新增方式，再实现 connector 映射。
- 先补可验证的类型和 WKB 单测，再加入 Iceberg 端到端回归。
- 保留当前 Connector SPI 对未知外部类型的降级能力，但 Geometry/Geography 一旦进入正式支持路径，不能继续静默降级。

## 10. 调整后的后续计划

### 阶段 1 调整：正式空间类型契约

先只做类型契约和最小闭环，不接 Iceberg DDL：

- 评估 `PrimitiveType`、Doris Thrift、FE `Type/ScalarType`、Nereids `DataType` 的最小改动集合。
- 明确 Geometry 与 Geography 是否共享一个物理列类型，以及 CRS/algorithm 存放位置。
- 明确标准原始 WKB 的内存表示、NULL 表示和 metadata 传递方式。
- 明确已有 BE `GeoShape`/空间函数能否复用，避免引入第二套空间值实现。
- 为类型序列化、反序列化、Thrift round-trip 和非法 metadata 添加测试。

阶段 1 完成前，不修改 Iceberg connector 的 Geometry 映射。

### 阶段 2 调整：Connector SPI 和 Iceberg schema

在正式类型契约确定后：

- 为 ConnectorType 增加空间类型所需的最小 metadata carrier。
- 修改 `IcebergTypeMapping` 支持 Geometry/Geography，并保留 CRS/algorithm。
- 修改 `IcebergSchemaBuilder` 支持 Doris 空间类型写入 Iceberg V3 schema。
- 修改 `IcebergSchemaUtils` 生成正确的 Thrift 字段描述。
- 增加默认 CRS、非默认 CRS、不同 Edge Algorithm 和非法值测试。

### 阶段 3 调整：分析与 DDL

- 接入真实 CREATE/ALTER/ADD COLUMN 路径。
- 验证 V3、Parquet 和嵌套类型约束。
- 验证 Geometry/Geography 之间、CRS 之间、algorithm 之间的不兼容行为。
- 验证 ConnectorType 到 FE 类型的转换不会回落到 UNSUPPORTED。
- 增加真实 Nereids INSERT/INSERT SELECT 分析测试。

### 阶段 4 调整：BE 物理读写和函数

- 将 Parquet 的 GEOMETRY/GEOGRAPHY BYTE_ARRAY annotation 接入正式空间列，而不是停留在 raw byte fallback。
- 复用现有 GeoShape/WKB 解析和空间函数实现；若不能复用，先拆出共享的 WKB 层。
- 验证投影 Geometry、EWKB SRID、NULL 和 mixed rows。
- 验证写回 Iceberg 后重新读取及关键 `ST_*` 函数结果。

### 阶段 5 调整：验证和提交

- 按“空间类型契约”“connector/schema”“BE 读写和函数”“回归测试”拆分提交。
- 每个提交只包含对应模块和测试。
- 使用 master 的标准 FE/BE/回归入口验证。
- 明确区分代码缺陷、依赖缺失和 CI 环境失败。
- 在完成端到端证据前，不宣称 PR 已完成。



### 与旧 PR 的差异补充

旧 PR 的空间实现并不只是 FE 代码，至少还包含：

- Doris Thrift `TPrimitiveType.GEOMETRY/GEOGRAPHY`；
- FE `PrimitiveType`、`ScalarType`、`Type` 和 Nereids 类型；
- BE `DataTypeSpatial`、`ColumnSpatial` 和空间列序列化；
- Iceberg Parquet/Arrow schema 和 writer；
- Nereids 空间函数绑定与写入校验；
- 大量 WKB、NULL、CRS、SRID 和回归测试。

当前 master 的这些空间层文件尚不存在，但 BE 已有成熟的 `GeoShape`/WKB 函数实现以及 Iceberg Parquet 的 GEOMETRY/GEOGRAPHY annotation 识别。因此重做时应复用现有 GeoShape/WKB 能力，并把旧 PR 的“空间列、类型、writer、connector”改动分层迁移，不能只复制 FE 类型映射。


## 执行记录：阶段 1（进行中）

- 已将空间原生类型接入 master 当前的类型编号体系：保留 master 已使用的 `TIMESTAMP_NS=45`、`UUID=46`，新增 Thrift `GEOMETRY=47`、`GEOGRAPHY=48`；BE 内部枚举追加为 45/46，避免沿用旧 PR 在旧基线上的编号。
- 已补齐 `PrimitiveType` 与 BE primitive type 的 Thrift 转换和字符串转换。
- 已在 `TScalarType` / `PTypeNode` 预留 CRS 与 geography algorithm 元数据字段，并在 FE `ScalarType` 中加入默认值、构造、SQL 展示、Thrift 序列化、反序列化和相等性语义。
- 当前仍未把空间类型接入表 DDL、Connector SPI、BE 物理列、Arrow/Parquet 读写或函数绑定；这些必须在基础类型编译通过后分阶段推进。
- 风险控制：不直接 cherry-pick 旧 PR 后续提交；旧 PR 使用的 FE 路径和类型编号已与 master 发生漂移，后续只提取语义并按当前 SPI/模块边界重写。

- FE 构建验证：`./build.sh --fe` 成功；Thrift/Proto 生成和 FE 全模块编译通过。
- Connector SPI 适配：Iceberg V3 GEOMETRY/GEOGRAPHY 读方向不再统一降级为 `UNSUPPORTED`；写方向映射到 Iceberg `GeometryType.crs84()` / `GeographyType.crs84()`；FE connector 转换器可生成对应 Doris `ScalarType`。当前 ConnectorType 仍只传递类型名，CRS/algorithm 的完整保真将在后续写入请求元数据时处理，暂不宣称已完成端到端语义。

- 阶段 2 验证：Nereids 空间类型类已加入，`./build.sh --fe` 再次成功；当前只完成类型分派与元数据保留，空间字面量、函数参数绑定和表达式求值仍属于阶段 4。

## 执行记录：阶段 3（进行中）

- 已从旧 PR 提取 BE 空间物理类型的最小变更集：`ColumnSpatial`、`DataTypeSpatial`、DataTypeFactory 分派和 BE 单测；保留 master 当前 `VARBINARY` SerDe，未接受旧提交对该文件的覆盖。
- `./build.sh --be` 已进入 worktree 子模块准备，但在 `clucene` 外部下载阶段因依赖传输阻塞而中止，尚未获得 BE 编译结论；该状态不能视为通过。
- 构建过程中 datasketches 子模块的两个二进制 fixture 被工具改写，未纳入提交；后续需在依赖恢复后清理/确认子模块工作树。

## 当前决策点

- 阶段 4（WKB/Arrow/Parquet 读写与空间函数）和阶段 5（回归、兼容性、PR 收敛）尚未完成；在 BE 依赖可构建前不继续扩展 writer/reader 代码，避免形成无法验证的半成品。
- 交付前必须补充：GEOMETRY/GEOGRAPHY 的 CRS/algorithm 端到端传递、嵌套类型边界、WKB/EWKB/SRID 校验、Arrow/Parquet 双向测试、NULL/无效输入语义，以及 master 上的回归测试与 PR 说明。

## 执行记录：阶段 3/4（继续推进）

- ConnectorType 现已携带 `spatialCrs` / `spatialAlgorithm`；Iceberg 读取、Doris 转换、Iceberg 写入可保留自定义 CRS 和边插值算法，不再只使用默认 CRS。
- V1/V2 Parquet schema descriptor 已把 GEOMETRY/GEOGRAPHY annotation 转为 `DataTypeSpatial`，默认值分别为 `OGC:CRS84`、`spherical`，并保留显式 annotation 元数据；DataTypeSpatial 的 protobuf 序列化也已补齐。
- FE 构建成功；IcebergTypeMappingReadTest 13/13 通过；BE header hygiene 通过。
- BE 完整构建仍未进入 CMake 编译：`build.sh` 的 submodule 准备阶段对已解包的 apache-orc 目录反复尝试 clone/下载，clucene 也未完成初始化。该依赖问题与源码编译结论分开记录，不能把本次 BE 构建视为通过。
- clang-format 检查未执行成功，环境提供的 clang-format 不是 v16；已保留该验证缺口，不能宣称 C++ 格式检查通过。

## 执行记录：阶段 4（空间函数绑定继续推进）

- 从旧 PR 语义中移植并按 master 重写 `SpatialFunctionSignature`，使 `ST_*` 函数保留参数化 Geometry/Geography 的 CRS 与 algorithm，而不是在签名匹配时退化为普通字符串。
- 补齐 `st_geogfromwkb` 的 FE/BE 注册；Geometry WKB 构造函数和 Geography WKB 构造函数现在返回对应空间类型，BE 侧使用 `DataTypeSpatial` / `ColumnSpatial`。
- 首轮定向测试发现旧生成类仍返回 VARCHAR、`ST_ASTEXT` 未覆写空间签名；已在 `b50df6c2203` 修正。
- 第二轮验证通过：`FE_UT_PARALLEL=1 ./run-fe-ut.sh --run org.apache.doris.nereids.trees.expressions.functions.scalar.SpatialConstructorFunctionTest`，6/6 通过，Maven BUILD SUCCESS。
- `./build.sh --fe` 在修正 import 顺序后成功完成，Checkstyle 0 violations。构建输出中的 npm engine/audit 和大 chunk 为环境/既有构建告警，不影响本次 FE 编译结论。

## 当前仍未完成的工作

- BE 完整编译和 BE 空间函数单测尚未通过；当前仍被 submodule 准备阶段的 apache-orc/clucene 依赖目录与下载问题阻塞。
- Iceberg writer/Arrow 双向读写、WKB/EWKB/SRID 校验、内部表 DDL 约束、嵌套类型限制和端到端回归仍未完成。
- C++ clang-format 检查仍受环境 clang-format 版本不是 v16 影响；不能宣称格式检查通过。

## 执行记录：阶段 3/4（当前轮次）

- 已完成内部 Doris 表边界：Nereids `ColumnDefinition` 在类型校验前拒绝 Geometry、Geography 及嵌套空间类型；`ColumnDefinitionTest` 定向测试 8/8 通过。
- 已完成空间列二进制通路：Arrow/Parquet 的通用 binary SerDe 改为使用 `IColumn` 接口，使 `ColumnSpatial` 可以复用 Varbinary SerDe；ORC 专用路径仍保持原有 Varbinary 前置条件。
- 已完成 BE 原始 WKB 输入：空间函数和 Iceberg writer 使用 `ColumnSpatial` 中的原始 WKB 字节；构造函数支持十六进制文本并写入原始 WKB。
- 已完成 Iceberg Parquet writer 适配：Geometry/Geography 顶层字段写出 BYTE_ARRAY logical annotation，保留 CRS、Geography algorithm 和 field id；嵌套空间字段明确返回 NotImplemented；writer 在写入前执行空间 WKB 校验。
- 已补充严格 WKB 校验层：Geometry 输入检查字节序、类型、坐标数量、尾随字节及 Z/M/SRID 元数据；Geography 保留现有带 SRID 的兼容解析路径。该部分待 BE 编译和单测验证。
- 新增 C++ writer/schema 测试覆盖 Geometry EPSG:3857 与 Geography vincenty annotation；待 BE 依赖准备完成后执行。

## 当前验证与阻塞

- FE 构建成功；空间构造函数单测 6/6、内部表列定义单测 8/8、Iceberg 类型映射单测 13/13 通过。
- BE 构建目前仍停在 `build.sh --be` 的第三方子模块准备阶段：apache-orc 目录已存在导致 clone 重试，随后进入 clucene clone，尚未进入 CMake/ninja 编译；因此不能把 BE 源码或 C++ 新测试标记为通过。
- WKB 校验源码已提交为 `8618349f64e`；BE 编译和 C++ 单测仍待第三方依赖可用后执行。datasketches 子模块的工作树改写仍视为构建噪声，不纳入提交。

## 执行记录：阶段 4/5（写入分析与回归补齐）

- 发现通用 connector sink 在类型 coercion 前没有保护空间参数，可能把 Geometry/Geography、CRS 或 Geography edge algorithm 的不匹配隐藏到 BE。新增 `ConnectorSpatialWriteValidator`，在 sink 进入通用 coercion 前校验空间 kind/CRS/algorithm，并允许 NULL 写入。
- 新增 `ConnectorSpatialWriteValidatorTest`：匹配类型通过，Geometry/Geography kind、CRS、algorithm 和非空间源均有拒绝用例，并验证嵌套 `IF`/`CAST` 不会隐藏源类型。
- 恢复 `regression-test/suites/external_table_p0/iceberg/test_iceberg_spatial_v3.groovy`，覆盖 Iceberg V3 Geometry 建表、Doris 原始 WKB 写入、Parquet 读回和 `ST_AsText`。用例受 `enableIcebergTest` 和外部 Docker 标签控制，未在当前环境执行。
- `./build.sh --fe` 重新通过；空间写入校验单测 6/6 通过；BE header hygiene 通过。
- BE 构建再次确认在 openblas 子模块 clone 阶段无进展；apache-orc 的目标 commit 在当前远端不可取，构建脚本进入下载 fallback 后仍被 openblas 初始化阻塞。未获得 BE 编译结论。

## 执行记录：master 兼容性编译与验证收敛

- `./build.sh --be` 完整构建成功，证据：`be-build-7.log` 的 `Successfully build Doris`。修复了空间类型 storage switch、当前 master Column 接口、StringRef/Arrow offset、WKB hex offset 和 protobuf metadata 放置问题；提交 `f99217a31dd`。空间 protobuf metadata 最终位于 `PScalarType` 的 optional 9/10 字段。
- BE 定向 UT 首次发现 Parquet `LogicalType` 基类没有 `crs()`，改为 Geometry/Geography 具体类型接口。第二次源码编译完成，但 ASAN 链接失败：Doris 和工具链 glibc compatibility 重复符号；尚无 BE UT 通过结论。
- RELEASE UT 重试受到并发 FE 测试重新生成 gensrc 的影响，生成头文件暂时缺失，构建失败。后续 FE/BE 构建串行执行，并恢复默认 ASAN UT，尝试通过脚本支持的 `GLIBC_COMPATIBILITY=OFF` 隔离本机工具链冲突；不改业务代码绕过环境问题。
- clang-tidy 已执行，但当前工具链缺失 sanitizer/asan_interface.h、stddef.h，4 个文件不能完成分析；不能记为通过。C++ 格式检查仍要求 clang-format 16，当前工具不是 v16；未通过。
- 范围复查发现 Connector changelog UPDATE/MERGE 在普通 sink 校验之前返回。现将相同空间 metadata 校验接入 UPDATE 的 bindColumn 及 MERGE 各分支 coercion 前，增加真实 plan builder CRS 拒绝测试，FE 测试进行中。
- 当前阶段 5 尚未完成：仍需 BE UT、真实 SQL 分析测试覆盖、Iceberg 外部读写回归及自动输出、格式和静态检查证据。现有通过证据仅支持 FE 定向测试和完整构建，不支持端到端验收完成。
- 环境残留为 contrib/apache-orc、contrib/datasketches-cpp，未纳入源码提交。

## 执行记录：2026-10-13 BE 定向测试收敛

- 标准 `run-be-ut.sh` 最终通过 86/86，日志 `be-spatial-ut-current-4.log`，XML 位于 `be/ut_build_ASAN/gtest_output`。覆盖 DataTypeSpatial、VGeoFunctions、GeoTypes、ArrowSchemaUtil 全套和 Parquet native geospatial BYTE_ARRAY 解码。
- 本机验证显式使用 ASAN、GLIBC_COMPATIBILITY=OFF；通过临时 Git config 跳过已解包 apache-orc/faiss/openblas 的重复更新，不修改仓库脚本或提交依赖目录。默认 glibc compatibility ON 的工具链冲突仍需单独处理。
- 实际执行暴露并修复：Geography 默认 algorithm 规范化为 spherical；序列化大小多算一个 size_t；S2 Geography 函数必须拒绝 Geometry 和不支持的 CRS/algorithm；Geometry accessor 必须直接处理原始 WKB，支持投影坐标；Parquet root/group 没有 logical annotation 时必须传 nullptr；numeric sphere 函数保留原有 NULL 传播。
- 旧 PR 的兼容性补丁直接套用会删除 master 新增函数并重复已有 WKB accessor。已恢复 ST_Within、ST_IsClosed、ST_NumGeometries、ST_NumPoints/alias 和 ST_Geometries，保留当前 FE 的 typed WKB constructor 名称，并保留旧 STRING 返回 plan 的 EWKB 兼容路径。
- legacy EWKB fixture 的错误坐标预期已按独立 little-endian 解码修正。投影 Geometry 和 legacy SRID 测试现均通过。
- 本轮修改已用隔离安装的 clang-format 16.0.6 格式化；diff whitespace 与 build hygiene 检查通过。clang-tidy 改用正确 compiler resource-dir，正在分析，尚未记录通过。
- FE changelog/空间 validator 13/13 已通过。仍缺真实 SQL 分析范围补齐、Iceberg V3 外部端到端回归及自动结果、完整范围静态/格式检查和最终评审；阶段 0–5 的总体目标保持未完成。

### 最新验证补充

- `spatial-format-current-2.log`：仓库标准 `check-format.sh` 使用 clang-format 16.0.6，退出 0，无格式差异。所有 spatial feature 改动的 29 个 C++ 文件完成格式化。
- clang-tidy 可解析后发现空间反序列化 `auto` 和 geo 循环索引 signed/unsigned 告警，已修复。整文件检查另有既有 writer `std::reverse` 建议；随后改按 feature 基线 `2413c00b143^` 的完整 diff 检查改动行，日志 `clang-tidy-spatial-scope.log`，仍在运行，未宣称通过。
- `be-spatial-ut-current-5.log`：上述修改后，标准 BE UT 再次 86/86，通过。
- 个人环境 `health-check.sh` 失败 9 项：Doris FE/BE、MinIO、HDFS 等个人端口未就绪，Docker socket 查询无权限。部署 dry-run 亦无法创建只读环境目录中的 lock；未更改或重启任何个人/其他服务。后续外部验证必须先恢复健康环境，或在当前 worktree output 下用隔离端口部署再接入个人 Iceberg fixture。
