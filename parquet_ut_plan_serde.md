# Parquet → DataTypeSerde UT 规划

## 测试范围

DataTypeSerde 是 Doris 通用序列化层。Parquet UT 只测**Parquet 层的构造环节**：验证 `append_values()` 传给 Serde 的 `DecodedColumnView` 参数正确，以及 Parquet 特有的数据准备路径（FLOAT16 转换、binary chunks 提取）。

不测 Serde 内部的值转换逻辑（那是 Serde 自己的 UT 责任）。

## 一、DecodedColumnView 传参验证

每种逻辑类型 → 验证构造的 `DecodedColumnView` 各字段与 `ParquetTypeDescriptor` 一致：

```
value_kind     ← decoded_value_kind(type_descriptor)
time_unit      ← type_descriptor.time_unit
decimal_p / s  ← type_descriptor.decimal_precision / scale
fixed_length   ← type_descriptor.fixed_length
is_adjusted_utc← type_descriptor.timestamp_is_adjusted_to_utc
timezone        ← session timezone
enable_strict   ← strict mode 开关
null_map        ← build_null_map() 产物；空 null_map 时传 nullptr
```

覆盖类型：

```
INT_8      → value_kind=INT32,  Serde 做 int32→int8 截断
INT_16     → value_kind=INT32,  Serde 做 int32→int16 截断
UINT_8     → value_kind=INT32,  Serde 做 int32→int16 转换
UINT_16    → value_kind=INT32,  Serde 做 int32→int32
UINT_32    → value_kind=UINT32
UINT_64    → value_kind=UINT64
DECIMAL + INT32   → value_kind=INT32,  decimal_precision/scale 传对
DECIMAL + INT64   → value_kind=INT64,  decimal_precision/scale 传对
DECIMAL + BYTE_ARRAY → value_kind=BINARY
DATE       → value_kind=INT32
TIME_MILLIS   → value_kind=INT32,  time_unit=MILLIS
TIME_MICROS   → value_kind=INT64,  time_unit=MICROS
TIMESTAMP_MILLIS → value_kind=INT64,  time_unit=MILLIS
TIMESTAMP_MICROS → value_kind=INT64,  time_unit=MICROS
TIMESTAMP_NANOS  → value_kind=INT64,  time_unit=NANOS
TIMESTAMP(utc=false) → value_kind=INT64, timestamp_is_adjusted_to_utc=false
INT96       → value_kind=INT96
```

**验证方式**：不能只依赖 Type UT。Type UT 只能证明 `type_descriptor` 字段正确，不能证明 `append_values()` 把字段正确传入 `DecodedColumnView`。需要用可观测结果覆盖高风险字段：

```
decimal_p_s_passed       DECIMAL INT32/INT64/BINARY 读值正确，覆盖 precision/scale
timestamp_unit_passed    MILLIS/MICROS/NANOS 读值差异正确
timestamp_utc_passed     isAdjustedToUTC=true/false 在指定 timezone 下结果不同且符合预期
timezone_passed          同一 timestamp 文件，UTC/Asia/Shanghai session timezone 结果符合预期
strict_mode_passed       strict mode 对非法值转换返回错误，非 strict 行为符合现有 Serde 约定
null_map_empty           required 列 / 无 NULL optional 列 → view.null_map=nullptr 路径
null_map_non_empty       optional 列含 NULL → view.null_map 指向 null_map
```

如果直接观察最终值无法区分某个字段是否传入，增加 file-local test serde/spy helper；
否则优先使用真实 `DataTypeSerde` 做端到端断言。至少需要 spy/helper 精确覆盖:

```
fixed_length_passed      FIXED_LEN_BYTE_ARRAY/DECIMAL_BYTE_ARRAY 的 fixed_length
strict_flag_passed       enable_strict_mode 从 factory/reader 传到 view
null_map_nullptr_passed  required 列、空 null_map 均传 nullptr
value_kind_override      FLOAT16 进入 append_values 后 view.value_kind 被覆盖为 FLOAT
```

## 二、Parquet 特有数据准备路径

这些逻辑在调用 `DataTypeSerde` **之前**，属于 Parquet 层的独有逻辑：

### 2.1 FLOAT16 转换

```
FLOAT16  →  binary chunks → build_binary_values → StringRef[]
         →  build_float16_values → half_to_float 逐值转换
         →  view.value_kind 覆盖为 FLOAT（不再是 FIXED_BINARY）

测试数据:
  +0.0    (0x0000) → 0.0f
  -0.0    (0x8000) → -0.0f  
  1.5     (0x3E00) → 1.5f
  NaN     (0x7E00) → NaN
  subnormal (0x0001) → 5.96e-8f
```

### 2.2 Binary chunks 提取

```
BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY
  → Arrow chunks → build_binary_values → StringRef[]
  
  验证: StringRef 的 data/size 与原始写入一致
        NULL 行的 StringRef 正确处理（data=nullptr, size=0）
```

### 2.3 dense nullable 固定宽度

```
RecordReader read_dense_for_nullable=true 时
  紧凑值（只含非 NULL）→ build_spaced_fixed_values 展开为间隔排列
  
  验证: null_map=[0,1,0,1,0] + compact=[v0,v1,v2] → spaced=[v0,_,v1,_,v2]
```

补充错误路径：

```
dense_value_count_mismatch
  values_written != non_null_count → 返回 Corruption

dense_binary_count_mismatch
  binary chunks 数量与 non_null_count 不一致 → 返回 Corruption
```

注意: 当前 `ParquetColumnReaderFactory::get_record_reader()` 创建 RecordReader 时
`read_dense_for_nullable=false`，普通写文件→读文件路径不会自然触发 dense nullable。
该路径需要以下二选一:

```
1. 增加 test-only helper/friend，直接构造 ParquetLeafBatch 并调用 append_values()
2. 将 RecordReader 构造参数做成可测试注入，再用真实文件触发 dense nullable
```

## 三、与 Type UT 和 Column Reader UT 的关系

```
                  Type UT          Column Reader UT       Serde UT (本文)
              ───────────────     ─────────────────     ────────────────
测试内容   type_descriptor 字段    控制流(read/select/skip)  DecodedColumnView 传参
                                                                    数据准备路径(FLOAT16等)
测试方式   构造 schema 验证        写文件→创建 Reader→读值       组装 DecodedColumnView 验证
值转换     不测                    不测                       不测(Serde 自己负责)
```

## 四、测试文件组织

```
测试放在 Column Reader 测试文件中即可（Serde 路径是 read() 的子步骤）:

parquet_column_reader_test.cpp:
  - FLOAT16 值读取验证（已有）
  - binary chunks → STRING 路径（已有）
  - dense nullable 展开（需补充，必须通过 test helper 或可注入 RecordReader 参数）
  - timestamp timezone / isAdjustedToUTC 传参（需补充）
  - strict mode 传参（需补充，如能构造非法转换输入）
  - 各逻辑类型 read 验证（与 Type UT 配合：Type UT 验证 type_descriptor, 
    这里验证 decode 链路完整）

parquet_type_test.cpp（新增）:
  - resolve_parquet_type() 所有类型的 type_descriptor 验证
```
