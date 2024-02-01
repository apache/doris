// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstring>
#include <vector>

#include "cctz/civil_time.h"
#include "cctz/time_zone.h"
#include "exec/olap_common.h"
#include "gutil/endian.h"
#include "parquet_common.h"
#include "util/timezone_utils.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/parquet/schema_desc.h"

namespace doris::vectorized {

class ParquetPredicate {
#define FOR_REINTERPRET_TYPES(M)             \
    M(TYPE_BOOLEAN, tparquet::Type::BOOLEAN) \
    M(TYPE_TINYINT, tparquet::Type::INT32)   \
    M(TYPE_SMALLINT, tparquet::Type::INT32)  \
    M(TYPE_INT, tparquet::Type::INT32)       \
    M(TYPE_BIGINT, tparquet::Type::INT64)    \
    M(TYPE_FLOAT, tparquet::Type::FLOAT)     \
    M(TYPE_DOUBLE, tparquet::Type::DOUBLE)

private:
    struct ScanPredicate {
        ScanPredicate() = default;
        ~ScanPredicate() = default;
        SQLFilterOp op;
        std::vector<const void*> values;
        int scale;

        ScanPredicate(const ScanPredicate& other) {
            op = other.op;
            for (auto v : other.values) {
                values.emplace_back(v);
            }
            scale = other.scale;
        }
    };

    template <typename DecimalPrimitiveType, typename DecimalPhysicalType>
    static DecimalPrimitiveType _decode_primitive_decimal(const FieldSchema* col_schema,
                                                          const std::string& encoded_data,
                                                          int dest_scale) {
        int scale = col_schema->parquet_schema.scale;
        Int128 value = *reinterpret_cast<const DecimalPhysicalType*>(encoded_data.data());
        if (dest_scale > scale) {
            value *= DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(dest_scale - scale);
        } else if (dest_scale < scale) {
            value /= DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(scale - dest_scale);
        }
        return (DecimalPrimitiveType)value;
    }

    template <typename DecimalPrimitiveType>
    static DecimalPrimitiveType _decode_binary_decimal(const FieldSchema* col_schema,
                                                       const std::string& encoded_data,
                                                       int dest_scale) {
        int scale = col_schema->parquet_schema.scale;
        const char* buf_start = encoded_data.data();
        Int128 value = buf_start[0] & 0x80 ? -1 : 0;
        memcpy(reinterpret_cast<char*>(&value) + sizeof(Int128) - encoded_data.size(), buf_start,
               encoded_data.size());
        value = BigEndian::ToHost128(value);
        if (dest_scale > scale) {
            value *= DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(dest_scale - scale);
        } else if (dest_scale < scale) {
            value /= DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(scale - dest_scale);
        }
        return (DecimalPrimitiveType)value;
    }

    template <typename CppType>
    static bool _filter_by_min_max(const SQLFilterOp op,
                                   const std::vector<CppType>& predicate_values, CppType& min_value,
                                   CppType& max_value) {
        if (predicate_values.empty()) {
            return false;
        }
        switch (op) {
        case FILTER_IN:
            for (const CppType& in_value : predicate_values) {
                if (in_value >= min_value && in_value <= max_value) {
                    return false;
                }
            }
            return true;
        case FILTER_LESS:
            return min_value >= predicate_values[0];
        case FILTER_LESS_OR_EQUAL:
            return min_value > predicate_values[0];
        case FILTER_LARGER:
            return max_value <= predicate_values[0];
        case FILTER_LARGER_OR_EQUAL:
            return max_value < predicate_values[0];
        default:
            return false;
        }
    }

    template <PrimitiveType primitive_type>
    static bool _filter_by_min_max(const ColumnValueRange<primitive_type>& col_val_range,
                                   const ScanPredicate& predicate, const FieldSchema* col_schema,
                                   const std::string& encoded_min, const std::string& encoded_max,
                                   const cctz::time_zone& ctz, bool use_min_max_value = false) {
        using CppType = typename PrimitiveTypeTraits<primitive_type>::CppType;
        std::vector<CppType> predicate_values;
        for (const void* v : predicate.values) {
            predicate_values.emplace_back(*reinterpret_cast<const CppType*>(v));
        }

        CppType min_value;
        CppType max_value;
        tparquet::Type::type physical_type = col_schema->physical_type;
        switch (col_val_range.type()) {
#define DISPATCH(REINTERPRET_TYPE, PARQUET_TYPE)                           \
    case REINTERPRET_TYPE:                                                 \
        if (col_schema->physical_type != PARQUET_TYPE) return false;       \
        min_value = *reinterpret_cast<const CppType*>(encoded_min.data()); \
        max_value = *reinterpret_cast<const CppType*>(encoded_max.data()); \
        break;
            FOR_REINTERPRET_TYPES(DISPATCH)
#undef DISPATCH
        case TYPE_VARCHAR:
            [[fallthrough]];
        case TYPE_CHAR:
            [[fallthrough]];
        case TYPE_STRING:
            // TODO: In parquet, min and max statistics may not be able to handle UTF8 correctly.
            // Current processing method is using min_value and max_value statistics introduced by PARQUET-1025 if they are used.
            // If not, current processing method is temporarily ignored. A better way is try to read min and max statistics
            // if it contains only ASCII characters.
            if (!use_min_max_value) {
                return false;
            }
            if constexpr (std::is_same_v<CppType, StringRef>) {
                min_value = StringRef(encoded_min);
                max_value = StringRef(encoded_max);
            } else {
                return false;
            };
            break;
        case TYPE_DECIMALV2:
            if constexpr (std::is_same_v<CppType, DecimalV2Value>) {
                size_t max_precision = max_decimal_precision<Decimal128V2>();
                if (col_schema->parquet_schema.precision < 1 ||
                    col_schema->parquet_schema.precision > max_precision ||
                    col_schema->parquet_schema.scale > max_precision) {
                    return false;
                }
                int v2_scale = DecimalV2Value::SCALE;
                if (physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                    min_value = DecimalV2Value(_decode_binary_decimal<Decimal128V2>(
                            col_schema, encoded_min, v2_scale));
                    max_value = DecimalV2Value(_decode_binary_decimal<Decimal128V2>(
                            col_schema, encoded_max, v2_scale));
                } else if (physical_type == tparquet::Type::INT32) {
                    min_value = DecimalV2Value(_decode_primitive_decimal<Decimal128V2, Int32>(
                            col_schema, encoded_min, v2_scale));
                    max_value = DecimalV2Value(_decode_primitive_decimal<Decimal128V2, Int32>(
                            col_schema, encoded_max, v2_scale));
                } else if (physical_type == tparquet::Type::INT64) {
                    min_value = DecimalV2Value(_decode_primitive_decimal<Decimal128V2, Int64>(
                            col_schema, encoded_min, v2_scale));
                    max_value = DecimalV2Value(_decode_primitive_decimal<Decimal128V2, Int64>(
                            col_schema, encoded_max, v2_scale));
                } else {
                    return false;
                }
            } else {
                return false;
            }
            break;
        case TYPE_DECIMAL32:
            [[fallthrough]];
        case TYPE_DECIMAL64:
            [[fallthrough]];
        case TYPE_DECIMAL128I:
            if constexpr (std::is_same_v<CppType, Decimal32> ||
                          std::is_same_v<CppType, Decimal64> ||
                          std::is_same_v<CppType, Decimal128V3>) {
                size_t max_precision = max_decimal_precision<CppType>();
                if (col_schema->parquet_schema.precision < 1 ||
                    col_schema->parquet_schema.precision > max_precision ||
                    col_schema->parquet_schema.scale > max_precision) {
                    return false;
                }
                if (physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                    min_value = _decode_binary_decimal<CppType>(col_schema, encoded_min,
                                                                predicate.scale);
                    max_value = _decode_binary_decimal<CppType>(col_schema, encoded_max,
                                                                predicate.scale);
                } else if (physical_type == tparquet::Type::INT32) {
                    min_value = _decode_primitive_decimal<CppType, Int32>(col_schema, encoded_min,
                                                                          predicate.scale);
                    max_value = _decode_primitive_decimal<CppType, Int32>(col_schema, encoded_max,
                                                                          predicate.scale);
                } else if (physical_type == tparquet::Type::INT64) {
                    min_value = _decode_primitive_decimal<CppType, Int64>(col_schema, encoded_min,
                                                                          predicate.scale);
                    max_value = _decode_primitive_decimal<CppType, Int64>(col_schema, encoded_max,
                                                                          predicate.scale);
                } else {
                    return false;
                }
            } else {
                return false;
            }
            break;
        case TYPE_DATE:
            [[fallthrough]];
        case TYPE_DATEV2:
            if (physical_type == tparquet::Type::INT32) {
                int64_t min_date_value =
                        static_cast<int64_t>(*reinterpret_cast<const int32_t*>(encoded_min.data()));
                int64_t max_date_value =
                        static_cast<int64_t>(*reinterpret_cast<const int32_t*>(encoded_max.data()));
                if constexpr (std::is_same_v<CppType, VecDateTimeValue> ||
                              std::is_same_v<CppType, DateV2Value<DateV2ValueType>>) {
                    min_value.from_unixtime(min_date_value * 24 * 60 * 60, ctz);
                    max_value.from_unixtime(max_date_value * 24 * 60 * 60, ctz);
                } else {
                    return false;
                }
            } else {
                return false;
            }
            break;
        case TYPE_DATETIME:
            [[fallthrough]];
        case TYPE_DATETIMEV2:
            if (physical_type == tparquet::Type::INT96) {
                ParquetInt96 datetime96_min =
                        *reinterpret_cast<const ParquetInt96*>(encoded_min.data());
                int64_t micros_min = datetime96_min.to_timestamp_micros();
                ParquetInt96 datetime96_max =
                        *reinterpret_cast<const ParquetInt96*>(encoded_max.data());
                int64_t micros_max = datetime96_max.to_timestamp_micros();
                if constexpr (std::is_same_v<CppType, VecDateTimeValue> ||
                              std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
                    min_value.from_unixtime(micros_min / 1000000, ctz);
                    max_value.from_unixtime(micros_max / 1000000, ctz);
                    if constexpr (std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
                        min_value.set_microsecond(micros_min % 1000000);
                        max_value.set_microsecond(micros_max % 1000000);
                    }
                } else {
                    return false;
                }
            } else if (physical_type == tparquet::Type::INT64) {
                int64_t date_value_min = *reinterpret_cast<const int64_t*>(encoded_min.data());
                int64_t date_value_max = *reinterpret_cast<const int64_t*>(encoded_max.data());

                int64_t second_mask = 1;
                int64_t scale_to_nano_factor = 1;
                cctz::time_zone resolved_ctz = ctz;
                const auto& schema = col_schema->parquet_schema;
                if (schema.__isset.logicalType && schema.logicalType.__isset.TIMESTAMP) {
                    const auto& timestamp_info = schema.logicalType.TIMESTAMP;
                    if (!timestamp_info.isAdjustedToUTC) {
                        // should set timezone to utc+0
                        resolved_ctz = cctz::utc_time_zone();
                    }
                    const auto& time_unit = timestamp_info.unit;
                    if (time_unit.__isset.MILLIS) {
                        second_mask = 1000;
                        scale_to_nano_factor = 1000000;
                    } else if (time_unit.__isset.MICROS) {
                        second_mask = 1000000;
                        scale_to_nano_factor = 1000;
                    } else if (time_unit.__isset.NANOS) {
                        second_mask = 1000000000;
                        scale_to_nano_factor = 1;
                    }
                } else if (schema.__isset.converted_type) {
                    const auto& converted_type = schema.converted_type;
                    if (converted_type == tparquet::ConvertedType::TIMESTAMP_MILLIS) {
                        second_mask = 1000;
                        scale_to_nano_factor = 1000000;
                    } else if (converted_type == tparquet::ConvertedType::TIMESTAMP_MICROS) {
                        second_mask = 1000000;
                        scale_to_nano_factor = 1000;
                    }
                }

                if constexpr (std::is_same_v<CppType, VecDateTimeValue> ||
                              std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
                    min_value.from_unixtime(date_value_min / second_mask, resolved_ctz);
                    max_value.from_unixtime(date_value_max / second_mask, resolved_ctz);
                    if constexpr (std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
                        min_value.set_microsecond((date_value_min % second_mask) *
                                                  scale_to_nano_factor / 1000);
                        max_value.set_microsecond((date_value_max % second_mask) *
                                                  scale_to_nano_factor / 1000);
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
            break;
        default:
            return false;
        }
        return _filter_by_min_max(predicate.op, predicate_values, min_value, max_value);
    }

    template <PrimitiveType primitive_type>
    static std::vector<ScanPredicate> _value_range_to_predicate(
            const ColumnValueRange<primitive_type>& col_val_range) {
        using CppType = typename PrimitiveTypeTraits<primitive_type>::CppType;
        std::vector<ScanPredicate> predicates;

        if (col_val_range.is_fixed_value_range()) {
            ScanPredicate in_predicate;
            in_predicate.op = SQLFilterOp::FILTER_IN;
            in_predicate.scale = col_val_range.scale();
            for (const auto& value : col_val_range.get_fixed_value_set()) {
                in_predicate.values.emplace_back(&value);
            }
            if (!in_predicate.values.empty()) {
                predicates.emplace_back(in_predicate);
            }
            return predicates;
        }

        const CppType high_value = col_val_range.get_range_max_value();
        const CppType low_value = col_val_range.get_range_min_value();
        const SQLFilterOp high_op = col_val_range.get_range_high_op();
        const SQLFilterOp low_op = col_val_range.get_range_low_op();

        // orc can only push down is_null. When col_value_range._contain_null = true, only indicating that
        // value can be null, not equals null, so ignore _contain_null in col_value_range
        if (col_val_range.is_high_value_maximum() && high_op == SQLFilterOp::FILTER_LESS_OR_EQUAL &&
            col_val_range.is_low_value_mininum() && low_op == SQLFilterOp::FILTER_LARGER_OR_EQUAL) {
            return predicates;
        }

        if (low_value < high_value) {
            if (!col_val_range.is_low_value_mininum() ||
                SQLFilterOp::FILTER_LARGER_OR_EQUAL != low_op) {
                ScanPredicate low_predicate;
                low_predicate.scale = col_val_range.scale();
                low_predicate.op = low_op;
                low_predicate.values.emplace_back(col_val_range.get_range_min_value_ptr());
                predicates.emplace_back(low_predicate);
            }
            if (!col_val_range.is_high_value_maximum() ||
                SQLFilterOp::FILTER_LESS_OR_EQUAL != high_op) {
                ScanPredicate high_predicate;
                high_predicate.scale = col_val_range.scale();
                high_predicate.op = high_op;
                high_predicate.values.emplace_back(col_val_range.get_range_max_value_ptr());
                predicates.emplace_back(high_predicate);
            }
        }
        return predicates;
    }

public:
    static bool filter_by_stats(const ColumnValueRangeType& col_val_range,
                                const FieldSchema* col_schema, bool is_set_min_max,
                                const std::string& encoded_min, const std::string& encoded_max,
                                bool is_all_null, const cctz::time_zone& ctz,
                                bool use_min_max_value = false) {
        bool need_filter = false;
        std::visit(
                [&](auto&& range) {
                    std::vector<ScanPredicate> filters = _value_range_to_predicate(range);
                    // Currently, ScanPredicate doesn't include "is null" && "x = null", filters will be empty when contains these exprs.
                    // So we can handle is_all_null safely.
                    if (!filters.empty()) {
                        need_filter = is_all_null;
                        if (need_filter) {
                            return;
                        }
                    }
                    for (auto& filter : filters) {
                        need_filter |= _filter_by_min_max(range, filter, col_schema, encoded_min,
                                                          encoded_max, ctz, use_min_max_value);
                        if (need_filter) {
                            break;
                        }
                    }
                },
                col_val_range);
        return need_filter;
    }
};

} // namespace doris::vectorized
