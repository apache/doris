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

#include "vec/exec/format/column_type_convert.h"

#include "common/cast_set.h"

namespace doris::vectorized::converter {
#include "common/compile_check_begin.h"

const std::set<std::string> SafeCastString<TYPE_BOOLEAN>::FALSE_VALUES = {"false", "off", "no", "0",
                                                                          ""};

#define FOR_LOGICAL_INTEGER_TYPES(M) \
    M(TYPE_TINYINT)                  \
    M(TYPE_SMALLINT)                 \
    M(TYPE_INT)                      \
    M(TYPE_BIGINT)                   \
    M(TYPE_LARGEINT)

#define FOR_LOGICAL_NUMERIC_TYPES(M) \
    M(TYPE_TINYINT)                  \
    M(TYPE_SMALLINT)                 \
    M(TYPE_INT)                      \
    M(TYPE_BIGINT)                   \
    M(TYPE_LARGEINT)                 \
    M(TYPE_FLOAT)                    \
    M(TYPE_DOUBLE)

#define FOR_LOGICAL_DECIMAL_TYPES(M) \
    M(TYPE_DECIMAL32)                \
    M(TYPE_DECIMAL64)                \
    M(TYPE_DECIMAL128I)              \
    M(TYPE_DECIMAL256)

#define FOR_LOGICAL_TIME_TYPES(M) \
    M(TYPE_DATETIME)              \
    M(TYPE_DATE)                  \
    M(TYPE_DATETIMEV2)            \
    M(TYPE_DATEV2)

#define FOR_ALL_LOGICAL_TYPES(M) \
    M(TYPE_BOOLEAN)              \
    M(TYPE_TINYINT)              \
    M(TYPE_SMALLINT)             \
    M(TYPE_INT)                  \
    M(TYPE_BIGINT)               \
    M(TYPE_LARGEINT)             \
    M(TYPE_FLOAT)                \
    M(TYPE_DOUBLE)               \
    M(TYPE_DECIMALV2)            \
    M(TYPE_DECIMAL32)            \
    M(TYPE_DECIMAL64)            \
    M(TYPE_DECIMAL128I)          \
    M(TYPE_DECIMAL256)           \
    M(TYPE_DATETIME)             \
    M(TYPE_DATE)                 \
    M(TYPE_DATETIMEV2)           \
    M(TYPE_DATEV2)

static bool _is_numeric_type(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
        return true;
    default:
        return false;
    }
}

static bool _is_decimal_type(doris::PrimitiveType type) {
    switch (type) {
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
        return true;
    default:
        return false;
    }
}

ColumnPtr ColumnTypeConverter::get_column(const DataTypePtr& src_type, ColumnPtr& dst_column,
                                          const DataTypePtr& dst_type) {
    if (is_consistent()) {
        if (_cached_src_type == nullptr) {
            _cached_src_type = dst_type;
        }
        return dst_column;
    }

    if (!_cached_src_column) {
        _cached_src_type = dst_type->is_nullable()
                                   ? get_data_type_with_default_argument(make_nullable(src_type))
                                   : get_data_type_with_default_argument(remove_nullable(src_type));
        _cached_src_column = remove_nullable(_cached_src_type)->create_column();
    }
    // remove the old cached data
    _cached_src_column->assume_mutable()->clear();

    if (dst_type->is_nullable()) {
        // In order to share null map between parquet converted src column and dst column to avoid copying. It is very tricky that will
        // call mutable function `doris_nullable_column->get_null_map_column_ptr()` which will set `_need_update_has_null = true`.
        // Because some operations such as agg will call `has_null()` to set `_need_update_has_null = false`.
        auto* doris_nullable_column = static_cast<const ColumnNullable*>(dst_column.get());
        return ColumnNullable::create(_cached_src_column,
                                      doris_nullable_column->get_null_map_column_ptr());
    }

    return _cached_src_column;
}

static std::unique_ptr<ColumnTypeConverter> _numeric_converter(const DataTypePtr& src_type,
                                                               const DataTypePtr& dst_type) {
    PrimitiveType src_primitive_type = src_type->get_primitive_type();
    PrimitiveType dst_primitive_type = dst_type->get_primitive_type();

    switch (dst_primitive_type) {
#define DISPATCH(DST_PTYPE)                                                                 \
    case DST_PTYPE: {                                                                       \
        switch (src_primitive_type) {                                                       \
        case TYPE_TINYINT:                                                                  \
            return std::make_unique<IntegerToIntegerConverter<TYPE_TINYINT, DST_PTYPE>>();  \
        case TYPE_SMALLINT:                                                                 \
            return std::make_unique<IntegerToIntegerConverter<TYPE_SMALLINT, DST_PTYPE>>(); \
        case TYPE_INT:                                                                      \
            return std::make_unique<IntegerToIntegerConverter<TYPE_INT, DST_PTYPE>>();      \
        case TYPE_BIGINT:                                                                   \
            return std::make_unique<IntegerToIntegerConverter<TYPE_BIGINT, DST_PTYPE>>();   \
        case TYPE_LARGEINT:                                                                 \
            return std::make_unique<IntegerToIntegerConverter<TYPE_LARGEINT, DST_PTYPE>>(); \
        default:                                                                            \
            return std::make_unique<UnsupportedConverter>(src_type, dst_type);              \
        }                                                                                   \
    }
        FOR_LOGICAL_INTEGER_TYPES(DISPATCH)
#undef DISPATCH

    case TYPE_FLOAT: {
        switch (src_primitive_type) {
#define DISPATCH(SRC_PTYPE)                                                             \
    case SRC_PTYPE: {                                                                   \
        return std::make_unique<NumericToFloatPointConverter<SRC_PTYPE, TYPE_FLOAT>>(); \
    }
            FOR_LOGICAL_INTEGER_TYPES(DISPATCH)
#undef DISPATCH
        default:
            return std::make_unique<UnsupportedConverter>(src_type, dst_type);
        }
    }

    case TYPE_DOUBLE: {
        switch (src_primitive_type) {
#define DISPATCH(SRC_PTYPE)                                                              \
    case SRC_PTYPE: {                                                                    \
        return std::make_unique<NumericToFloatPointConverter<SRC_PTYPE, TYPE_DOUBLE>>(); \
    }
            FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        default:
            return std::make_unique<UnsupportedConverter>(src_type, dst_type);
        }
    }
    default:
        return std::make_unique<UnsupportedConverter>(src_type, dst_type);
    }
}

template <FileFormat fileFormat = COMMON>
static std::unique_ptr<ColumnTypeConverter> _to_string_converter(const DataTypePtr& src_type,
                                                                 const DataTypePtr& dst_type) {
    PrimitiveType src_primitive_type = src_type->get_primitive_type();
    // numeric type to string, using native std::to_string
    if (src_primitive_type == TYPE_BOOLEAN) {
        return std::make_unique<BooleanToStringConverter>();
    } else if (_is_numeric_type(src_primitive_type)) {
        switch (src_primitive_type) {
#define DISPATCH(SRC_PTYPE) \
    case SRC_PTYPE:         \
        return std::make_unique<NumericToStringConverter<SRC_PTYPE, fileFormat>>();
            FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        default:
            return std::make_unique<UnsupportedConverter>(src_type, dst_type);
        }
    } else if (_is_decimal_type(src_primitive_type)) { // decimal type to string
        switch (src_primitive_type) {
#define DISPATCH(SRC_PTYPE) \
    case SRC_PTYPE:         \
        return std::make_unique<DecimalToStringConverter<SRC_PTYPE>>(src_type->get_scale());
            FOR_LOGICAL_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
        default:
            return std::make_unique<UnsupportedConverter>(src_type, dst_type);
        }
    } else if (is_date_type(src_primitive_type)) { // date and datetime type to string
        switch (src_primitive_type) {
#define DISPATCH(SRC_PTYPE) \
    case SRC_PTYPE:         \
        return std::make_unique<TimeToStringConverter<SRC_PTYPE>>();
            FOR_LOGICAL_TIME_TYPES(DISPATCH)
#undef DISPATCH
        default:
            return std::make_unique<UnsupportedConverter>(src_type, dst_type);
        }
    }
    return std::make_unique<UnsupportedConverter>(src_type, dst_type);
}

template <FileFormat fileFormat = COMMON>
static std::unique_ptr<ColumnTypeConverter> _from_string_converter(const DataTypePtr& src_type,
                                                                   const DataTypePtr& dst_type) {
    PrimitiveType dst_primitive_type = dst_type->get_primitive_type();
    switch (dst_primitive_type) {
#define DISPATCH(DST_PTYPE)                                                  \
    case DST_PTYPE:                                                          \
        return std::make_unique<CastStringConverter<DST_PTYPE, fileFormat>>( \
                remove_nullable(dst_type));
        FOR_ALL_LOGICAL_TYPES(DISPATCH)
#undef DISPATCH
    default:
        return std::make_unique<UnsupportedConverter>(src_type, dst_type);
    }
}

static std::unique_ptr<ColumnTypeConverter> _numeric_to_decimal_converter(
        const DataTypePtr& src_type, const DataTypePtr& dst_type) {
    PrimitiveType src_primitive_type = src_type->get_primitive_type();
    PrimitiveType dst_primitive_type = dst_type->get_primitive_type();
    int scale = dst_type->get_scale();
    int precision = dst_type->get_precision();
    switch (src_primitive_type) {
#define DISPATCH(SRC_PTYPE)                                                                  \
    case SRC_PTYPE: {                                                                        \
        switch (dst_primitive_type) {                                                        \
        case TYPE_DECIMAL32:                                                                 \
            return std::make_unique<NumericToDecimalConverter<SRC_PTYPE, TYPE_DECIMAL32>>(   \
                    precision, scale);                                                       \
        case TYPE_DECIMAL64:                                                                 \
            return std::make_unique<NumericToDecimalConverter<SRC_PTYPE, TYPE_DECIMAL64>>(   \
                    precision, scale);                                                       \
        case TYPE_DECIMAL128I:                                                               \
            return std::make_unique<NumericToDecimalConverter<SRC_PTYPE, TYPE_DECIMAL128I>>( \
                    precision, scale);                                                       \
        case TYPE_DECIMAL256:                                                                \
            return std::make_unique<NumericToDecimalConverter<SRC_PTYPE, TYPE_DECIMAL256>>(  \
                    precision, scale);                                                       \
        default:                                                                             \
            return std::make_unique<UnsupportedConverter>(src_type, dst_type);               \
        }                                                                                    \
    }
        FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    default:
        return std::make_unique<UnsupportedConverter>(src_type, dst_type);
    }
}

static std::unique_ptr<ColumnTypeConverter> _datetime_to_numeric_converter(
        const DataTypePtr& src_type, const DataTypePtr& dst_type) {
    PrimitiveType dst_primitive_type = dst_type->get_primitive_type();

    switch (dst_primitive_type) {
#define DISPATCH(DST_TYPE)                                               \
    case DST_TYPE: {                                                     \
        return std::make_unique<DateTimeToNumericConverter<DST_TYPE>>(); \
    }
        FOR_LOGICAL_INTEGER_TYPES(DISPATCH)
#undef DISPATCH
    default: {
        return std::make_unique<UnsupportedConverter>(src_type, dst_type);
    }
    };
}

static std::unique_ptr<ColumnTypeConverter> _decimal_to_numeric_converter(
        const DataTypePtr& src_type, const DataTypePtr& dst_type) {
    PrimitiveType src_primitive_type = src_type->get_primitive_type();
    PrimitiveType dst_primitive_type = dst_type->get_primitive_type();
    int scale = src_type->get_scale();
    switch (dst_primitive_type) {
#define DISPATCH(DST_PTYPE)                                                                        \
    case DST_PTYPE: {                                                                              \
        switch (src_primitive_type) {                                                              \
        case TYPE_DECIMAL32:                                                                       \
            return std::make_unique<DecimalToNumericConverter<TYPE_DECIMAL32, DST_PTYPE>>(scale);  \
        case TYPE_DECIMAL64:                                                                       \
            return std::make_unique<DecimalToNumericConverter<TYPE_DECIMAL64, DST_PTYPE>>(scale);  \
        case TYPE_DECIMAL128I:                                                                     \
            return std::make_unique<DecimalToNumericConverter<TYPE_DECIMAL128I, DST_PTYPE>>(       \
                    scale);                                                                        \
        case TYPE_DECIMAL256:                                                                      \
            return std::make_unique<DecimalToNumericConverter<TYPE_DECIMAL256, DST_PTYPE>>(scale); \
        default:                                                                                   \
            return std::make_unique<UnsupportedConverter>(src_type, dst_type);                     \
        }                                                                                          \
    }
        FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    default:
        return std::make_unique<UnsupportedConverter>(src_type, dst_type);
    }
}

static std::unique_ptr<ColumnTypeConverter> _decimal_converter(const DataTypePtr& src_type,
                                                               const DataTypePtr& dst_type) {
    int from_precision = src_type->get_precision();
    int from_scale = src_type->get_scale();
    int to_precision = dst_type->get_precision();
    int to_scale = dst_type->get_scale();

    if (from_scale == to_scale && from_precision == to_precision) {
        return std::make_unique<ConsistentConverter>();
    }

    PrimitiveType src_primitive_type = src_type->get_primitive_type();
    PrimitiveType dst_primitive_type = dst_type->get_primitive_type();
    switch (dst_primitive_type) {
#define DISPATCH(DST_PTYPE)                                                                  \
    case DST_PTYPE: {                                                                        \
        switch (src_primitive_type) {                                                        \
        case TYPE_DECIMAL32:                                                                 \
            return std::make_unique<DecimalToDecimalConverter<TYPE_DECIMAL32, DST_PTYPE>>(   \
                    from_precision, from_scale, to_precision, to_scale);                     \
        case TYPE_DECIMAL64:                                                                 \
            return std::make_unique<DecimalToDecimalConverter<TYPE_DECIMAL64, DST_PTYPE>>(   \
                    from_precision, from_scale, to_precision, to_scale);                     \
        case TYPE_DECIMAL128I:                                                               \
            return std::make_unique<DecimalToDecimalConverter<TYPE_DECIMAL128I, DST_PTYPE>>( \
                    from_precision, from_scale, to_precision, to_scale);                     \
        case TYPE_DECIMAL256:                                                                \
            return std::make_unique<DecimalToDecimalConverter<TYPE_DECIMAL256, DST_PTYPE>>(  \
                    from_precision, from_scale, to_precision, to_scale);                     \
        default:                                                                             \
            return std::make_unique<UnsupportedConverter>(src_type, dst_type);               \
        }                                                                                    \
    }
        FOR_LOGICAL_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
    default:
        return std::make_unique<UnsupportedConverter>(src_type, dst_type);
    }
}

std::unique_ptr<ColumnTypeConverter> ColumnTypeConverter::get_converter(const DataTypePtr& src_type,
                                                                        const DataTypePtr& dst_type,
                                                                        FileFormat file_format) {
    PrimitiveType src_primitive_type = src_type->get_primitive_type();
    PrimitiveType dst_primitive_type = dst_type->get_primitive_type();
    //todo:  type to varchar/char.
    if (is_string_type(src_primitive_type) && is_string_type(dst_primitive_type)) {
        return std::make_unique<ConsistentConverter>();
    }

    if (_is_decimal_type(src_primitive_type) && _is_decimal_type(dst_primitive_type)) {
        return _decimal_converter(src_type, dst_type);
    }

    if (src_primitive_type == dst_primitive_type) {
        return std::make_unique<ConsistentConverter>();
    }

    // from numeric type to numeric type, use native static cast
    // example: float -> int
    if (_is_numeric_type(src_primitive_type) && _is_numeric_type(dst_primitive_type)) {
        return _numeric_converter(src_type, dst_type);
    }

    auto str_type = is_string_type(src_primitive_type)
                            ? DataTypeFactory::instance().create_data_type(
                                      PrimitiveType::TYPE_STRING, src_type->is_nullable())
                            : src_type;
    // change to string type
    // example: decimal -> string
    if (is_string_type(dst_primitive_type)) {
        if (file_format == ORC) {
            return _to_string_converter<ORC>(str_type, dst_type);
        } else {
            return _to_string_converter<COMMON>(str_type, dst_type);
        }
    }

    // string type to other type
    // example: string -> date
    if (is_string_type(src_primitive_type)) {
        if (file_format == ORC) {
            return _from_string_converter<ORC>(str_type, dst_type);
        } else {
            return _from_string_converter<COMMON>(str_type, dst_type);
        }
    }

    // date to datetime, datetime to date
    // only support date & datetime v2
    if (src_primitive_type == TYPE_DATEV2 && dst_primitive_type == TYPE_DATETIMEV2) {
        return std::make_unique<TimeV2Converter<TYPE_DATEV2, TYPE_DATETIMEV2>>();
    }
    if (src_primitive_type == TYPE_DATETIMEV2 && dst_primitive_type == TYPE_DATEV2) {
        return std::make_unique<TimeV2Converter<TYPE_DATETIMEV2, TYPE_DATEV2>>();
    }

    // datetime to bigint (ms)
    if (src_primitive_type == TYPE_DATETIMEV2 && _is_numeric_type(dst_primitive_type)) {
        return _datetime_to_numeric_converter(src_type, dst_type);
    }

    // numeric to decimal
    if (_is_numeric_type(src_primitive_type) && _is_decimal_type(dst_primitive_type)) {
        return _numeric_to_decimal_converter(src_type, dst_type);
    }

    // decimal to numeric
    if (_is_decimal_type(src_primitive_type) && _is_numeric_type(dst_primitive_type)) {
        return _decimal_to_numeric_converter(src_type, dst_type);
    }

    return std::make_unique<UnsupportedConverter>(src_type, dst_type);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized::converter
