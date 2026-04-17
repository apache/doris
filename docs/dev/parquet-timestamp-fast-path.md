# Parquet Timestamp Convert 快路径优化方案 (v2)

## 1. 背景与动机

Parquet 读链路中，`INT64/INT96 -> DateTimeV2` 的 convert 是已知热点。当前路径：

```
parquet physical value
  -> epoch seconds
  -> DateV2Value::from_unixtime(ts, ctz)
       -> cctz::convert(t, ctz)    // 构造 civil_second，开销最大
       -> unchecked_set_time(year, month, day, hour, minute, second, 0)
  -> set_microsecond(...)
```

`cctz::convert` 内部需要：
1. 通过 transition table 查找 UTC offset（二分查找 / hint 命中）
2. 将 unix_time + offset 拆分为 civil_second（year/month/day/hour/minute/second）

对 fixed-offset 时区（UTC, +08:00 等），步骤 1 的 offset 是常量，不需要逐行查找。

### 1.1 已有基础设施

Doris 已有两个关键基础设施可以复用：

**cctz `lookup_offset` 补丁** (`thirdparty/patches/cctz-lookup-offset.patch`)：
- 在 `cctz::time_zone` 上新增 `lookup_offset()` 方法
- 仅返回 UTC offset，不构造 `civil_second`
- 已在 `be/src/exprs/function/date_time_transforms.h` 中使用

**cctz `civil-cache` 补丁** (`thirdparty/patches/cctz-civil-cache.patch`)：
- 预缓存 200 年的 `civil_day` 对象
- 优化 `LocalTime()` 中的日期拆分
- 添加 `__builtin_expect` 分支提示

**libdivide**：快速整数除法库，已在 `date_time_transforms.h` 中使用。

**Phase 1 已完成**：commit f4f653b 已新增 `convert_time`、`column_read_io_time`、`fill_columns_time` profile 计时器。

### 1.2 已有快路径模式

`date_time_transforms.h` 中的 `HourFromUnixtimeImpl::extract_field()` 已经实现了一种快路径模式：

```cpp
// 1. 用 lookup_offset 获取 offset（而非 cctz::convert）
cctz::time_point<cctz::sys_seconds> t = epoch + cctz::seconds(local_time);
int offset = ctz.lookup_offset(t).offset;
local_time += offset;

// 2. 用 libdivide 做快速整数除法拆分字段
int64_t remainder = local_time - local_time / fast_div_86400 * 86400;
return static_cast<int8_t>(remainder / fast_div_3600);
```

本方案的核心思路：将这一模式推广到 Parquet timestamp convert 场景。

## 2. 优化目标

- 对 fixed-offset 时区（UTC、`+08:00` 等），跳过逐行 `lookup_offset`，直接用常量 offset
- 对 IANA 名字时区（`Asia/Shanghai` 等），用 `lookup_offset` 替代 `cctz::convert`，避免 `civil_second` 构造开销
- 内联字段拆分逻辑到 converter 循环，使用 libdivide 加速除法

**非目标**：
- 不修改 FE 传参与 SQL 语义
- 不在本期优化 `TimestampTz` 路径
- 不对名字时区做区间型 offset 推断

## 3. 设计

### 3.1 Fixed-offset 检测

在 `ConvertParams` 初始化时判断是否为 fixed-offset 时区。

**检测方法**：检查 `cctz::time_zone::name()` 是否以 `"Fixed/"` 开头。

cctz 内部对 `cctz::fixed_time_zone()` 创建的时区统一使用 `"Fixed/UTC<+-><hours>:<mins>:<secs>"` 命名（见 `time_zone_fixed.h`）。`cctz::utc_time_zone()` 的 name 为 `"UTC"`，需单独处理。

```cpp
// timezone_utils.h 新增
static bool try_get_fixed_offset_seconds(const cctz::time_zone& tz, int32_t* offset_out) {
    const auto& name = tz.name();
    if (name == "UTC") {
        *offset_out = 0;
        return true;
    }
    if (name.starts_with("Fixed/")) {
        // Fixed/UTC+HH:MM:SS or Fixed/UTC-HH:MM:SS
        // 从 name 中解析 offset，或直接用 lookup_offset(epoch) 获取
        static const auto epoch = std::chrono::time_point_cast<cctz::sys_seconds>(
                std::chrono::system_clock::from_time_t(0));
        *offset_out = tz.lookup_offset(epoch).offset;
        return true;
    }
    return false;
}
```

**为什么不从原始时区字符串判断**：
- 原始字符串可能是 `+8:00`、`UTC+8`、`GMT-6` 等各种格式
- 经过 `find_cctz_time_zone` 解析后的 `cctz::time_zone` 对象才是可靠的判定来源
- 严禁基于原始字符串做快路径判断，避免把 IANA 名字时区误判为 fixed-offset

### 3.2 ConvertParams 扩展

```cpp
struct ConvertParams {
    // 现有字段 ...
    const cctz::time_zone* ctz = nullptr;
    int64_t second_mask = 1;
    int64_t scale_to_nano_factor = 1;
    // ...

    // 新增
    bool is_fixed_offset = false;
    int32_t fixed_offset_seconds = 0;

    void init(const FieldSchema* field_schema_, const cctz::time_zone* ctz_) {
        // ... 现有逻辑 ...

        // 在确定最终 ctz 之后，检测 fixed-offset
        if (ctz != nullptr) {
            is_fixed_offset = TimezoneUtils::try_get_fixed_offset_seconds(
                    *ctz, &fixed_offset_seconds);
        }
    }
};
```

### 3.3 快路径实现：内联字段拆分

不单独新增 `from_unixtime_fast()` 方法，而是直接在 converter 循环中内联字段拆分逻辑。原因：

1. 避免函数调用开销
2. 可以在循环外预计算 libdivide divider
3. 与 `date_time_transforms.h` 中已有模式保持一致

**Int64ToTimestamp 改造**：

```cpp
struct Int64ToTimestamp : public PhysicalToLogicalConverter {
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        // ... 前置代码不变 ...

        if (_convert_params->is_fixed_offset) {
            // 快路径：fixed-offset，offset 是常量
            const int32_t offset = _convert_params->fixed_offset_seconds;
            convert_batch_fast(src_data, data, start_idx, rows, offset,
                               _convert_params->second_mask,
                               _convert_params->scale_to_nano_factor);
        } else {
            // 通用路径：IANA 名字时区，用 lookup_offset 替代 cctz::convert
            const auto& ctz = *_convert_params->ctz;
            convert_batch_lookup(src_data, data, start_idx, rows, ctz,
                                 _convert_params->second_mask,
                                 _convert_params->scale_to_nano_factor);
        }
        return Status::OK();
    }
};
```

**快路径核心逻辑** (`convert_batch_fast`)：

```cpp
static void convert_batch_fast(
        const int64_t* src, UInt64* dst, size_t start, size_t rows,
        int32_t offset_seconds, int64_t second_mask, int64_t scale_to_nano_factor) {

    static const libdivide::divider<int64_t> fast_div_86400(86400);
    static const libdivide::divider<int64_t> fast_div_3600(3600);
    static const libdivide::divider<int64_t> fast_div_60(60);

    for (size_t i = 0; i < rows; i++) {
        int64_t x = src[i];
        int64_t epoch_seconds = x / second_mask;
        int64_t sub_seconds = x % second_mask;

        // 1. 加上固定 offset，得到本地 unix time
        int64_t local_ts = epoch_seconds + offset_seconds;

        // 2. 拆分为 day + second-of-day
        int64_t days = local_ts / fast_div_86400;
        int64_t sod = local_ts - days * 86400;
        if (sod < 0) { sod += 86400; days--; }

        // 3. 从 days 拆分 year/month/day（C++20 chrono）
        std::chrono::sys_days sd(std::chrono::days(days));
        std::chrono::year_month_day ymd(sd);

        // 4. 从 sod 拆分 hour/minute/second
        int64_t hour = sod / fast_div_3600;
        int64_t rem = sod - hour * 3600;
        int64_t minute = rem / fast_div_60;
        int64_t second = rem - minute * 60;

        // 5. 组装结果
        auto& value = reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(dst[start + i]);
        value.unchecked_set_time(
                static_cast<int>(ymd.year()),
                static_cast<unsigned>(ymd.month()),
                static_cast<unsigned>(ymd.day()),
                hour, minute, second, 0);
        value.set_microsecond(sub_seconds * (scale_to_nano_factor / 1000));
    }
}
```

**通用路径** (`convert_batch_lookup`)：

对 IANA 名字时区，用 `lookup_offset` 替代 `cctz::convert`。虽然仍需逐行查 offset（因为 DST 可能变化），但省去了 `civil_second` 构造开销：

```cpp
static void convert_batch_lookup(
        const int64_t* src, UInt64* dst, size_t start, size_t rows,
        const cctz::time_zone& ctz, int64_t second_mask, int64_t scale_to_nano_factor) {

    static const auto epoch = std::chrono::time_point_cast<cctz::sys_seconds>(
            std::chrono::system_clock::from_time_t(0));
    static const libdivide::divider<int64_t> fast_div_86400(86400);
    static const libdivide::divider<int64_t> fast_div_3600(3600);
    static const libdivide::divider<int64_t> fast_div_60(60);

    for (size_t i = 0; i < rows; i++) {
        int64_t x = src[i];
        int64_t epoch_seconds = x / second_mask;
        int64_t sub_seconds = x % second_mask;

        // 用 lookup_offset 获取 offset（跳过 civil_second 构造）
        cctz::time_point<cctz::sys_seconds> t = epoch + cctz::seconds(epoch_seconds);
        int32_t offset = ctz.lookup_offset(t).offset;

        int64_t local_ts = epoch_seconds + offset;

        // 后续拆分逻辑同 fast 路径
        int64_t days = local_ts / fast_div_86400;
        int64_t sod = local_ts - days * 86400;
        if (sod < 0) { sod += 86400; days--; }

        std::chrono::sys_days sd(std::chrono::days(days));
        std::chrono::year_month_day ymd(sd);

        int64_t hour = sod / fast_div_3600;
        int64_t rem = sod - hour * 3600;
        int64_t minute = rem / fast_div_60;
        int64_t second = rem - minute * 60;

        auto& value = reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(dst[start + i]);
        value.unchecked_set_time(
                static_cast<int>(ymd.year()),
                static_cast<unsigned>(ymd.month()),
                static_cast<unsigned>(ymd.day()),
                hour, minute, second, 0);
        value.set_microsecond(sub_seconds * (scale_to_nano_factor / 1000));
    }
}
```

### 3.4 时区输入归一扩展

当前 `parse_tz_offset_string` 的正则 `^[+-]\d{2}:\d{2}$` 覆盖面过窄。建议扩展支持：

| 输入格式 | 归一结果 | 说明 |
|----------|----------|------|
| `+08:00` | `+08:00` | 已支持 |
| `+8:00`  | `+08:00` | 小时不补零 |
| `UTC+8`  | `+08:00` | UTC 前缀 |
| `UTC+08:00` | `+08:00` | UTC 前缀 + 完整格式 |
| `GMT-6`  | `-06:00` | GMT 前缀 |
| `GMT-06:30` | `-06:30` | GMT 前缀 + 带分钟 |
| `Etc/GMT+8` | `-08:00` | **注意符号反转** |
| `Etc/GMT-8` | `+08:00` | **注意符号反转** |
| `UTC` / `Etc/UTC` / `Zulu` | `UTC` | 已在 cache 中处理 |

**不支持**的格式：
- `0800`、`+800`：无分隔符的纯数字格式，存在歧义（`0800` 是 UTC+08:00 还是军事时间？），不做猜测式兼容
- `CST`、`EST` 等缩写：全球存在多个同名缩写（CST = 中国标准时间 / 美国中部标准时间），当前特殊处理 CST=Asia/Shanghai 保持不变

**`Etc/GMT+N` 符号反转问题**：

POSIX 标准中 `Etc/GMT+N` 的含义是 **UTC-N**（符号反转），这是一个常见陷阱。例如 `Etc/GMT+8` 实际上是 UTC-08:00。cctz 正确实现了这一语义，但如果在归一层面错误处理会导致偏差达到 16 小时。

处理方式：`Etc/GMT*` 格式由 cctz 原生支持，直接通过 `cctz::load_time_zone` 加载，不在归一层做额外转换。这些时区在 `load_timezones_to_cache` 中已被加载到 cache。

### 3.5 `isAdjustedToUTC == false` 的处理

当 Parquet schema 中 `logicalType.TIMESTAMP.isAdjustedToUTC == false` 时，`ConvertParams::init()` 已将 `ctz` 设为 `&utc0`。

UTC 的 offset 为 0，必然命中 fixed-offset 快路径。这是收益最确定的场景——不依赖用户 session timezone 设置。

## 4. 代码改动清单

| 文件 | 改动 |
|------|------|
| `be/src/util/timezone_utils.h` | 新增 `try_get_fixed_offset_seconds` |
| `be/src/util/timezone_utils.cpp` | 扩展时区输入归一能力 |
| `be/src/format/parquet/parquet_column_convert.h` | `ConvertParams` 增加 `is_fixed_offset` / `fixed_offset_seconds`；改造 `Int64ToTimestamp`、`Int96toTimestamp` |
| `be/src/core/value/vdatetime_value.h` | （可选）新增 `from_unixtime_with_offset` 供非 Parquet 路径复用 |

## 5. 三条路径对比

| 路径 | 适用场景 | offset 获取 | 字段拆分 | 预期开销 |
|------|----------|------------|----------|----------|
| 原始路径 | 回退兜底 | `cctz::convert` (transition查找 + civil构造) | cctz 内部 | 最高 |
| lookup 路径 | IANA 名字时区 | `lookup_offset` (transition查找, 无civil构造) | libdivide + chrono | 中等 |
| fast 路径 | fixed-offset 时区 | 常量，循环外确定 | libdivide + chrono | 最低 |

## 6. 为什么不对 `Asia/Shanghai` 走 fixed-offset 快路径

`Asia/Shanghai` 当前实际使用 CST（UTC+8），但：

- 1927-12-31 23:54:08 之前使用 LMT（UTC+08:05:43）
- 历史上存在过 DST（1986-1991）
- IANA 数据库可能在未来更新规则

虽然现代数据（2000年后）的 offset 固定为 +08:00，但将名字时区降格为固定 offset 是语义错误。对于现代数据，`lookup_offset` 路径的 hint 命中率接近 100%（同一 transition 内），已经足够快。

## 7. 测试方案

### 7.1 `try_get_fixed_offset_seconds` 单测

```
输入                 | 预期 is_fixed | 预期 offset
UTC                  | true          | 0
+08:00               | true          | 28800
-06:00               | true          | -21600
+05:45               | true          | 20700
Asia/Shanghai        | false         | -
America/Mexico_City  | false         | -
```

### 7.2 快路径一致性测试

对每个 fixed-offset 时区，验证 `convert_batch_fast(ts, offset)` 与 `DateV2Value::from_unixtime(ts, ctz)` 输出完全一致：

- 正常值：1700000000 (+08:00, UTC)
- 负 timestamp：-86400 (1969-12-31)
- 跨天边界：epoch + 86399, epoch + 86400
- 亚秒精度：毫秒/微秒/纳秒各一组
- 极端值：INT64_MIN / 2, INT64_MAX / 2

### 7.3 lookup 路径一致性测试

对 IANA 名字时区，验证 `convert_batch_lookup` 与原始 `from_unixtime` 输出一致：

- `Asia/Shanghai`：2024-01-01 正常值 + 1988-06-01 DST 期间
- `America/New_York`：夏令时切换前后各一个值
- `Pacific/Auckland`：南半球 DST

### 7.4 时区输入归一测试

```
+8:00     -> 解析成功，offset == 28800
UTC+8     -> 解析成功，offset == 28800
GMT-06:30 -> 解析成功，offset == -23400
+25:00    -> 解析失败
UTC+8:75  -> 解析失败
```

### 7.5 回归测试

同一份 Parquet timestamp 数据，在 `UTC`、`+08:00`、`Asia/Shanghai`、`America/New_York` 下查询，结果与优化前完全一致。

## 8. 风险与规避

| 风险 | 规避措施 |
|------|----------|
| 误将 IANA 时区判定为 fixed-offset | 仅基于 `cctz::time_zone::name()` 判定，不基于原始字符串 |
| `std::chrono::year_month_day` 对极端 days 值行为 | 限制在合理范围内（-365*10000 ~ 365*10000），超出回退原始路径 |
| libdivide 对负数除法语义 | 手动处理 sod < 0 的情况（已在代码中体现） |
| `Etc/GMT+N` 符号反转 | 不在归一层处理，交给 cctz 原生加载 |
| 与 cctz civil-cache 补丁的交互 | lookup 路径不走 `cctz::convert`，与 civil-cache 补丁无冲突 |

## 9. 交付计划

Phase 1（已完成，f4f653b）：
- profile 计时器：`convert_time`、`column_read_io_time`、`fill_columns_time`

Phase 2（本期）：
1. `timezone_utils` 增加 `try_get_fixed_offset_seconds` + 时区输入归一扩展
2. `ConvertParams` 增加 `is_fixed_offset` / `fixed_offset_seconds`
3. `Int64ToTimestamp` / `Int96toTimestamp` 接入三条路径（fast / lookup / 原始回退）
4. 单测 + 回归测试

Phase 3（可选，后续）：
- 对名字时区，按批次 timestamp range 判定无 transition 切换时临时走 fast 路径
- 复杂度高，不与 Phase 2 合并
