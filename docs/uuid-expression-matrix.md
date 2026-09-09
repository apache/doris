# UUID 表达式与函数参数矩阵

基线为 `4892be80d41`，该 squash 已包含此前 57 个 UUID 功能套件。本轮依据参数形态进一步增加 8 个独立套件，保留原有存储、裁剪、索引、导入导出和协议用例。范围按 UUID 原生表达式、公开泛型函数及容器入口核对；可先隐式转 STRING 再调用的字符串函数按转换通道验证，未声称逐一枚举所有字符串函数。所有套件位于 [UUID regression 目录](../regression-test/suites/datatype_p0/uuid)。

## 参数和数据如何组合

[uuid_matrix.groovy](../regression-test/plugins/uuid_matrix.groovy) 统一生成数据和组合，但每个 suite 建立自己的表。确定结果仍由标准脚本生成 `.out`，再由普通回归运行比较。

| 维度 | 实际安排 |
| --- | --- |
| UUID 特殊值 | NULL、全零、1、全 `ff`、`7fff...ffff` / `8000...0000`、高低 64 位进位相邻值 |
| UUID 正常值 | 普通 v4、v7，包含大小写不同的文本输入，最终规范化为同一 UUID |
| 全常量 | 每个参数均为 SQL 常量表达式；常量不预先在 Groovy 中求值，而使用 `CAST(CONCAT(...,'') AS UUID)` 等可折叠表达式 |
| 常量与列混合 | 二元函数覆盖 `cv`、`vc`；三元表达式覆盖 `ccv/cvc/vcc/cvv/vcv/vvc`，常量和列两侧均遍历特殊值和正常值 |
| 全列 | `v/vv/vvv` 实际扫描存储列；列数据使用保留全部值域的排列，包含相等、不同、两侧同时 NULL、大小比较和高位边界两侧 |
| nullable 分支 | 标量矩阵分别使用 UUID nullable 列和 UUID NOT NULL 列；NOT NULL 列中的缺省占位使用零 UUID，常量侧仍允许 NULL |
| 容器 | NULL/空容器、NULL 元素、重复元素、正常值和无符号边界值；MAP key/value、STRUCT 字段分别保留 UUID 身份 |
| 其他参数 | Boolean 的 true/false/NULL，数组索引的负数/0/1/2/越界/NULL，重复次数或长度的 0/正常值/NULL；布尔数组含 true/false/NULL 元素 |
| 常量执行批次 | 普通常量矩阵也从十行表投影，避免所有 ColumnConst 场景只处理一行；UUID 随机函数另用 4097 行跨批输入 |

`c` 表示常量表达式，`v` 表示存储列。每个位置组合使用三种模式：

| 模式 | 设置 | 验证含义 |
| --- | --- | --- |
| FE | `debug_skip_fold_constant=false`、`enable_fold_constant_by_be=false` | FE 有实现的常量表达式被折叠；其余保留执行 |
| BE | `debug_skip_fold_constant=false`、`enable_fold_constant_by_be=true` | 允许通过 BE 求值并折叠常量表达式 |
| runtime | `debug_skip_fold_constant=true`、`enable_fold_constant_by_be=false` | 禁止该常量折叠规则，保留常量参数在 BE 中执行 |

含有列的父表达式不能整体折叠，混合矩阵检查的是其常量子表达式折叠前后的等价性。关闭常量折叠不等于关闭所有 NULL 推导或语义重写。

标量 suite 还检查真实 `EXPLAIN VERBOSE`：对 `UUID_VERSION(CAST(CONCAT(...) AS UUID))` 和 `COALESCE(u,CAST(CONCAT(...) AS UUID))`，FE 模式消除 CONCAT，保留 UUID_VERSION；BE 模式进一步消除 UUID_VERSION；runtime 模式保留两者。三种模式均保留依赖列的 COALESCE。不能仅设置开关就声称表达式已折叠。

## 按功能组织的矩阵

| Suite | 表达式及函数 |
| --- | --- |
| [scalar_matrix](../regression-test/suites/datatype_p0/uuid/test_uuid_scalar_matrix.groovy) | `= != < <= > >= <=>`、IS NULL/IS NOT NULL、IN/NOT IN、BETWEEN/NOT BETWEEN、AND/OR/NOT、IF、搜索 CASE、简单 CASE、NULLIF、IFNULL/NVL、COALESCE、GREATEST/LEAST（二元及三元）、UUID_VERSION、TO_JSON |
| [cast_matrix](../regression-test/suites/datatype_p0/uuid/test_uuid_cast_matrix.groovy) | STRING/CHAR/VARCHAR/VARIANT→UUID、UUID→STRING/CHAR/VARCHAR/VARIANT、身份和往返 CAST、TRY_CAST、ARRAY/MAP 递归转换；严格与非严格模式、非法长度/字符/连字符/空白/花括号/零字节/非 ASCII 文本 |
| [array_matrix](../regression-test/suites/datatype_p0/uuid/test_uuid_array_matrix.groovy) | ARRAY 构造、ARRAY_SORT/ARRAY_REVERSE_SORT、ARRAY_DISTINCT/ARRAY_COMPACT、ARRAY_MIN/MAX、ARRAY_POPBACK/POPFRONT、REVERSE、SIZE/CARDINALITY、ARRAY_ENUMERATE/ENUMERATE_UNIQ、TO_JSON、ARRAY_MAP/FILTER/FIRST/LAST/FIRST_INDEX/LAST_INDEX、ARRAY_SHUFFLE 单参数及 seed、ARRAY_UNION/INTERSECT/EXCEPT/EXCEPT_ALL、ARRAYS_OVERLAP、ARRAY_CONTAINS_ALL/CONCAT、ARRAY_CONTAINS/POSITION/REMOVE/PUSHBACK/PUSHFRONT/APPEND、COUNTEQUAL、ELEMENT_AT/下标、ARRAY_SLICE 两/三参数、ARRAY_REPEAT/WITH_CONSTANT、ARRAY_SORTBY/ZIP、ARRAY_SPLIT/REVERSE_SPLIT、ARRAY_FLATTEN、ARRAY_COUNT/EXISTS/MATCH_ANY/MATCH_ALL；lambda 捕获外部 UUID 的全部常量/列组合及 NULL 谓词；EXPLODE/POSEXPLODE 及 OUTER |
| [map_matrix](../regression-test/suites/datatype_p0/uuid/test_uuid_map_matrix.groovy) | MAP_KEYS/VALUES/SIZE、DEDUPLICATE_MAP、MAP_FILTER/APPLY/EXISTS/ALL、MAP_ENTRIES/FROM_ENTRIES、lambda 捕获外部 UUID 及 NULL 谓词、EXPLODE_MAP/OUTER、MAP_CONTAINS_KEY/VALUE/ENTRY、ELEMENT_AT/下标、MAP/MAP_FROM_ARRAYS、NAMED_STRUCT、STRUCT_ELEMENT、STRUCT 的 TO_JSON/CAST |
| [hash_matrix](../regression-test/suites/datatype_p0/uuid/test_uuid_hash_matrix.groovy) | MURMUR_HASH3_32/64、XXHASH_32/64 的单/双参数，CRC32、MD5、LENGTH、HEX、IS_UUID、UUID_TO_INT→INT_TO_UUID→UUID 往返；这些签名经 FE 将 UUID 转换成规范字符串，验证的是该转换通道 |
| [aggregate_matrix](../regression-test/suites/datatype_p0/uuid/test_uuid_aggregate_matrix.groovy) | MIN/MAX、COUNT、COUNT DISTINCT 单/双参数、MULTI_DISTINCT_COUNT、NDV、COLLECT_LIST/SET、ARRAY_AGG、HISTOGRAM/HIST、TOPN_ARRAY 两/三参数、ANY_VALUE、MIN_BY/MAX_BY、MAP_AGG/MAP_AGG_V2、GROUP_ARRAY_UNION/INTERSECT、MIN/MAX/COUNT 的 STATE→MERGE、STATE→UNION→MERGE、COMBINE→MERGE、FOREACH，MIN_MAP/MAX_MAP/COUNT_MAP；单阶段/两阶段、空输入、全 NULL |
| [window_matrix](../regression-test/suites/datatype_p0/uuid/test_uuid_window_matrix.groovy) | LAG/LEAD 默认参数和三参数（offset 0/1/越界；负数/NULL 负向检查，默认值常量或列）、FIRST_VALUE/LAST_VALUE 忽略/保留 NULL、NTH_VALUE 第 1/2/越界项及 0/负数/NULL 负向检查、窗口 MIN/MAX/COUNT、RANK/DENSE_RANK/ROW_NUMBER/PERCENT_RANK/CUME_DIST/NTILE 的 UUID 排序键 |
| [generation_matrix](../regression-test/suites/datatype_p0/uuid/test_uuid_generation_matrix.groovy) | UUID_V4/UUID_V7、GENERATEUUIDV4/GENERATEUUIDV7、GENERATE_UUID_V4/GENERATE_UUID_V7；三种折叠设置下检查非 NULL、每行生成、去重数量、version/variant 位和规范文本长度 |

## 不应伪造的组合

- 一元函数只有全常量和全列两种参数形态；没有合法的“同一次调用既常量又列”的组合。
- UUID 随机生成函数没有输入参数，应验证不被折叠成全批重复的同一个值，不能为它们虚构特殊输入值。生成结果先写入 UUID 列再校验，避免投影中多次求值干扰观察。
- 聚合和窗口算子依赖输入行集或窗口，不能把“参数都是常量”解释成算子本身应被折叠。矩阵切换其参数表达式的折叠模式，保留真实聚合/窗口执行。
- HISTOGRAM 桶数、TOPN_ARRAY 数量/容量、COLLECT_LIST 限额、窗口 offset、FIRST/LAST_VALUE 的忽略 NULL 标记由现有签名要求为常量。合法常量值执行，非法列参数用 `test { sql; exception }` 验证拒绝；不标为全列执行成功。
- NAMED_STRUCT 字段名、STRUCT_ELEMENT 字段标识和 CAST 目标类型属于静态结构信息，不作为可任意替换成列的数据参数。
- ARRAY_ZIP/SORTBY、多数组 lambda、split/filter 的对应数组需要长度匹配；混合矩阵按 id 配对，使常量和列都遍历 NULL/空/正常值，同时保持长度约束。
- ANY_VALUE 和带并列键的 MIN_BY/MAX_BY 不固定某个任意返回值：分别检查结果属于输入集合，以及返回的 UUID 与最小/最大键构成真实输入对。MAP_AGG 同时输出按 key/value 编码的完整配对，不能仅靠独立排序后的两边集合验收；其混合形态按行配对，避免把重复 key 的未指定胜出值固化；全列输入仍执行多行聚合。
- FOREACH 的现有绑定入口要求输入为存储列，常量 ARRAY 被明确拒绝（数值类型也相同）；MIN/MAX/COUNT_FOREACH 执行包含全部值域的列输入，并在三种模式下逐一验证常量输入拒绝。
- COLLECT_SET 与 COLLECT_LIST 不同，允许列限额。矩阵按限额列分组，使每组限额一致；检查返回基数等于限额与去重基数的较小值，以及所有结果属于输入集合，避免固定限额内任意保留的某些 UUID。
- ARRAY_SHUFFLE 检查排序后的完整多重集，包含重复值及 NULL，不固定随机排列。

## 本轮修复及验证

新增矩阵修复 UUID 直接相关的两个 BE 注册缺口：

1. [HISTOGRAM UUID 注册](../be/src/exprs/aggregate/aggregate_function_histogram.cpp)：补入 TYPE_UUID，复用已有无符号值排序、桶统计和 UUID 格式化。
2. [MAP_AGG_V2 UUID key 注册](../be/src/exprs/aggregate/aggregate_function_map_v2.cpp)：补入 TYPE_UUID，复用已有 Field key、列和状态序列化。MAP_AGG 的当前 FE 改写会进入此入口。

### 已有通用问题：AGG_STATE 常量折叠后的 MERGE 类型

`719e70f4910` 曾同时修改 `MergeCombinator.withChildren` 并新增通用 FE 单测；这两项已撤回，`MergeCombinator.java` 恢复为 `4892be80d41` 中的实现。该问题在新增 UUID 前就存在，INT 也能复现，不属于此次 UUID 类型改动的修复范围。

复现示例（使用本套件的十行测试表）：

```sql
SET enable_agg_state = true;
SET debug_skip_fold_constant = false;
SET enable_fold_constant_by_be = false;
SELECT MAX_MERGE(MAX_STATE(CAST(CONCAT('1','') AS INT)))
FROM uuid_matrix_aggregate;
```

CAST 在折叠前可空，折叠后的 INT 字面量非空；AGG_STATE 参数类型更新，MERGE 的 nested 参数仍保留旧的可空属性。BE 期望 INT，而 FE 传入 Nullable(INT)，因此报错。将 INT 换成 UUID 也会触发同一问题。

当前 UUID 的 STATE→MERGE、STATE→UNION→MERGE、COMBINE→MERGE 用例显式使用 `NULLABLE(...)` 稳定状态参数的可空属性，子表达式仍分别进行 FE/BE 折叠和禁止折叠；NULL、边界值、正常值及列输入均保留。BE 折叠规则本身禁止折叠 NULLABLE 包装，以保留其类型属性。**这验证的是可空状态签名的 UUID 功能，不表示上述原始故障路径已经修复或覆盖通过。** 原始故障仅在此记录，留待独立修复。

[AggregateFunctionUUIDTest](../be/test/exprs/aggregate/aggregate_function_uuid_test.cpp) 新增 histogram 和 map 的工厂、NULL/边界、合并、序列化恢复、reset 检查。Histogram 检查每个桶的上下界和计数；MAP 检查完整 key/value 关联，包含 NULL 和重复 key。两者与原 Histogram/UUID 单测合计 5 项通过。

8 个新增 suite 共生成 4,397 个 `.out` 结果块。独立审计确认 FE/BE/runtime 三种模式结果一致，另用 UUID 无符号整数比较、SQL 三值逻辑、窗口位置计算、MD5/CRC32、既有 UUID_TO_INT 字节序及聚合集合/计数规则核对 93,231 项结果；HISTOGRAM 检查桶上下界、计数、NDV 和前缀计数，MAP_AGG 检查完整配对。原有 57 个 suite 的 `.out` 内容均未修改。

- BE：通过 `build.sh --be` 的 ASAN 构建；5 项相关 BE UT 通过。
- FE：`719e70f4910` 的 39 项 FE UT 结果属于撤回前的验证；其中新增的 MergeCombinatorTest 已删除，不作为当前 UUID 改动的测试证据。
- 撤回前的额外兼容性检查：3 个既有聚合状态 regression suite 普通比较通过。
- C++ 格式与 header hygiene 通过。`run-clang-tidy.sh` 已执行；三个修改文件的分析均被基线 `be/src/core/types.h:577` 的未配对 NOLINTEND 阻断，不能标为 clang-tidy 全部通过；没有遗留本轮新增代码的诊断。
- `719e70f4910` 的全部 65 个 UUID suite 普通比较曾通过；本次撤回后的针对性验证记录见下文。

撤回后的验证：`build.sh --fe` 构建和 Checkstyle 已通过；运行恢复后 FE，确认 INT/UUID 的原始故障仍可复现。显式可空签名在 FE/BE/runtime 三种模式均返回正确结果，`EXPLAIN VERBOSE` 的执行算子表达式确认 FE/BE 模式折叠 CONCAT、runtime 保留 CONCAT，同时保留 NULLABLE 包装。

针对性回归：`test_uuid_aggregate_matrix`、`test_uuid_aggregate_state`、`test_agg_state`、`test_agg_state_max`、`test_agg_state_nereids` 共 5 个套件普通比较通过，0 失败、0 fatal、0 跳过。全部 UUID `.out` 文件保持不变，本次没有重新生成期望结果。

本地验证使用当前 worktree 的 ASAN BE、FE 和三个独立端口的 BE，运行目录在 `output/uuid-completeness`。测试脚本为 `run-regression-test.sh --conf <worktree配置> --run -d datatype_p0/uuid -parallel 3`；只在首次生成新增结果时传递 `-genOut -forceGenOut`，最终验证不携带生成参数。

## 自检关键项

| 检查点 | 结论 |
| --- | --- |
| 目标与证明 | 新套件按参数位置和折叠模式枚举；计划断言验证折叠确实发生，结果由回归脚本生成并比较 |
| 修改范围 | 两个 BE 工厂复用现有 UUID 实现；FE 的通用 MERGE 修复已撤回；公共数据/枚举放在插件 |
| 并发与锁 | 不引入线程或锁；每个 suite 使用独立表，插件没有跨 suite 可变数据 |
| 生命周期 | 复用聚合状态/列的既有所有权；BE UT 明确 create/merge/reset/destroy，无新增静态初始化依赖 |
| 配置 | 不新增产品配置；矩阵只设置与折叠、聚合阶段、严格转换及状态功能有关的会话变量 |
| 兼容性 | 不改变 UUID 编码、协议、存储格式和签名；修复此前无法执行的入口 |
| 平行路径 | HIST/HISTOGRAM、MAP_AGG/MAP_AGG_V2；FE/BE/runtime、单/两阶段聚合、STATE/UNION/COMBINE 及状态列路径均覆盖 |
| 条件分支 | 不新增 FE 分支；状态用例显式固定可空属性，已有故障单独记录 |
| 测试 | 特殊/正常、常量/混合/全列、nullable/NOT NULL、容器 NULL/空/重复、合法和非法固定参数；BE UT 验证 UUID 工厂缺口；通用 FE 单测已撤回 |
| 期望结果 | `.out` 全由 runner 生成；另核对折叠模式一致性、UUID 整数比较、三值逻辑及窗口默认值；任意选择/随机函数验证性质 |
| 可观测性 | EXPLAIN 断言和既有错误信息可定位本轮问题；不新增日志/指标 |
| 事务与持久化 | 本轮不改事务、visible version、EditLog 或 delete bitmap；原有存储功能套件保留 |
| 写入原子性 | 不改变写入路径；测试用表通过既有 INSERT 写入 |
| FE/BE 参数传递 | 无新变量或字段；AGG_STATE 的通用类型一致性问题未在本次修改 |
| 性能 | BE 沿用既有模板；FE 保持原实现；测试枚举复用代码，按 suite 可单独运行 |
| 其他问题 | clang-tidy 受基线 types.h 的 NOLINT 配对错误阻断，单独记录；隐式 STRING 全部函数的穷举不在本矩阵声明范围 |
