# Parquet Type UT 规划

## 测试范围

只测 `resolve_parquet_type()` 的**类型映射正确性**，不测值读取（那是 Serde UT 的责任）。

```
测试方式：直接构造 Arrow ColumnDescriptor → resolve_parquet_type() → 验证 ParquetTypeDescriptor 各字段

不需要写 Parquet 文件，不需要经过 ColumnReader。
```

## 一、ParquetTypeDescriptor 关键字段

```
doris_type                    → Doris 类型（TYPE_INT, TYPE_DECIMAL128, TYPE_DATETIMEV2...）
extra_type_info               → 物理编码方式（DECIMAL_INT32, UNIT_MS, IMPALA_TIMESTAMP...）
is_decimal / is_timestamp     → 类型分类标记
is_unsigned_integer            → unsigned 标记
timestamp_is_adjusted_to_utc   → UTC 标记
is_string_like                 → string-like 标记
supports_record_reader         → 是否支持 RecordReader
integer_bit_width              → INT 位宽
decimal_precision / scale      → DECIMAL 参数
time_unit                      → TIME/TIMESTAMP 精度
```

## 二、三级解析覆盖

### 第1级：logical_type

```
每种 LogicalType 一个 case：

INT(8, true)   → doris_type=TINYINT,  is_unsigned=false, bit_width=8
INT(8, false)  → doris_type=SMALLINT, is_unsigned=true,  bit_width=8
INT(16, true)  → doris_type=SMALLINT, is_unsigned=false, bit_width=16
INT(16, false) → doris_type=INT,      is_unsigned=true,  bit_width=16
INT(32, true)  → doris_type=INT,      is_unsigned=false, bit_width=32
INT(32, false) → doris_type=BIGINT,   is_unsigned=true,  bit_width=32
INT(64, true)  → doris_type=BIGINT,   is_unsigned=false, bit_width=64
INT(64, false) → doris_type=LARGEINT, is_unsigned=true,  bit_width=64

DECIMAL(9,2)   → doris_type=DECIMAL128(9,2),  is_decimal=true
DECIMAL(18,6)  → doris_type=DECIMAL128(18,6), is_decimal=true
DECIMAL(39,6)  → doris_type=DECIMAL256, is_decimal=true    [P2]

STRING         → doris_type=STRING
ENUM           → doris_type=STRING
JSON           → doris_type=STRING
BSON           → doris_type=STRING
UUID           → doris_type=STRING

DATE           → doris_type=DATEV2

TIME(MILLIS, utc=false) → doris_type=TIMEV2(3), time_unit=MILLIS, extra=UNIT_MS
TIME(MICROS, utc=false) → doris_type=TIMEV2(6), time_unit=MICROS, extra=UNIT_MICROS
TIME(MILLIS, utc=true)  → unsupported_reason 非空, supports_record_reader=false
TIME(MICROS, utc=true)  → unsupported_reason 非空, supports_record_reader=false

TIMESTAMP(MILLIS, utc=true)   → DATETIMEV2(3), is_timestamp=true, isAdjustedToUTC=true,  UNIT_MS
TIMESTAMP(MILLIS, utc=false)  → DATETIMEV2(3), is_timestamp=true, isAdjustedToUTC=false, UNIT_MS
TIMESTAMP(MICROS, utc=true)   → DATETIMEV2(6), is_timestamp=true, isAdjustedToUTC=true,  UNIT_MICROS
TIMESTAMP(MICROS, utc=false)  → DATETIMEV2(6), is_timestamp=true, isAdjustedToUTC=false, UNIT_MICROS
TIMESTAMP(NANOS,  utc=true)   → DATETIMEV2(6), is_timestamp=true, isAdjustedToUTC=true,  UNIT_NS
TIMESTAMP(NANOS,  utc=false)  → DATETIMEV2(6), is_timestamp=true, isAdjustedToUTC=false, UNIT_NS

FLOAT16        → doris_type=FLOAT, extra=FLOAT16
FLOAT16 invalid physical_type/length → 不按 FLOAT16 解析，走 converted/physical fallback
```

### 第2级：converted_type

```
每种 ConvertedType 一个 case：

INT_8     → doris_type=TINYINT,  is_unsigned=false, bit_width=8
UINT_8    → doris_type=SMALLINT, is_unsigned=true,  bit_width=8
INT_16    → doris_type=SMALLINT, is_unsigned=false, bit_width=16
UINT_16   → doris_type=INT,      is_unsigned=true,  bit_width=16
INT_32    → doris_type=INT,      is_unsigned=false, bit_width=32
UINT_32   → doris_type=BIGINT,   is_unsigned=true,  bit_width=32
INT_64    → doris_type=BIGINT,   is_unsigned=false, bit_width=64
UINT_64   → doris_type=LARGEINT, is_unsigned=true,  bit_width=64

DECIMAL + INT32              → DECIMAL128, is_decimal, extra=DECIMAL_INT32
DECIMAL + INT64              → DECIMAL128, is_decimal, extra=DECIMAL_INT64
DECIMAL + BYTE_ARRAY         → DECIMAL128, is_decimal, extra=DECIMAL_BYTE_ARRAY
DECIMAL + FIXED_LEN_BYTE_ARRAY → DECIMAL128, is_decimal, extra=DECIMAL_BYTE_ARRAY

UTF8    → STRING
ENUM    → STRING
JSON    → STRING
BSON    → STRING

DATE    → DATEV2

TIME_MILLIS  → 当前实现不支持 converted TIME, unsupported_reason 非空
TIME_MICROS  → 当前实现不支持 converted TIME, unsupported_reason 非空

TIMESTAMP_MILLIS → DATETIMEV2(3), is_timestamp, isAdjustedToUTC=true（默认）, UNIT_MS
TIMESTAMP_MICROS → DATETIMEV2(6), is_timestamp, isAdjustedToUTC=true（默认）, UNIT_MICROS
```

### 第3级：physical_type 兜底

```
BOOLEAN           → BOOL
INT32             → INT
INT64             → BIGINT
FLOAT             → FLOAT
DOUBLE            → DOUBLE
BYTE_ARRAY        → STRING
FIXED_LEN_BYTE_ARRAY → STRING
INT96             → DATETIMEV2(6), IMPALA_TIMESTAMP
```

## 三、级联降级验证

```
logical + converted 同时存在  → 按 logical_type 解析
只有 converted              → 按 converted_type 解析
都没有                      → 按 physical_type 解析

验证方式：对同一个物理类型（如 INT32），分别构造三组 ColumnDescriptor：
  - 带 logical_type=INT(8,true)    → TINYINT
  - 不带 logical + converted=INT_8 → TINYINT（与上面结果相同）
  - 都不带                          → INT（物理兜底）
```

## 四、辅助字段验证

```
is_string_like:
  BYTE_ARRAY 无 annotation        → true
  BYTE_ARRAY + DECIMAL            → false
  FIXED_LEN(2) + FLOAT16          → false
  FIXED_LEN(4) + FLOAT16 logical  → true/false 按 fallback 后结果验证

supports_record_reader:
  全部已知物理类型                  → true
  unsupported/unknown physical     → false（如 Arrow/Parquet API 可构造；不可构造则不强求）
  logical/converted TIME utc=true   → false，原因来自 unsupported_reason

decoded_value_kind:
  普通 INT32   → INT32
  UINT_32      → UINT32
  普通 INT64   → INT64
  UINT_64      → UINT64
  FLOAT16      → FIXED_BINARY（在 append_values 中被 override 为 FLOAT）
```

## 五、边界与非法组合

```
null_descriptor          resolve_parquet_type(nullptr) → 默认 descriptor，不 crash
invalid_int_bit_width    INT(bit_width 非 8/16/32/64) → logical 解析失败，fallback 到 physical
invalid_time_unit        TIME(NANOS) 当前不支持 TIMEV2(9)
                         - logical TIME(NANOS, utc=false) 无 unsupported_reason, fallback 到 physical
                         - logical TIME(NANOS, utc=true) unsupported_reason 非空
invalid_float16_carrier  FLOAT16 但 physical!=FIXED_LEN_BYTE_ARRAY 或 length!=2
                         → 不设置 extra=FLOAT16，避免 append_values 误按 half 解析
decimal_precision_boundary
                         precision=38 → DECIMAL128
                         precision=39 → DECIMAL256
nullable_flag            max_definition_level=0/1 对 doris_type nullable 属性的影响
                         注意 build_parquet_column_schema() 后会按 node optional 状态重新规范化
```

## 六、测试文件组织

```
一个测试文件：parquet_type_test.cpp
  - 每个 test case 构造一种 ColumnDescriptor
  - 调用 resolve_parquet_type(descriptor)
  - 验证 ParquetTypeDescriptor 各字段

构造方式：
  ::parquet::schema::PrimitiveNode::Make(name, repetition, logical_type, physical_type, length...)
  → 构造 GroupNode → SchemaDescriptor → build_parquet_column_schema → 验证每个 field 的 type_descriptor
```
