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

#include "vec/exec/format/parquet/parquet_column_convert.h"

#include <cctz/time_zone.h>

#include "vec/columns/column_nullable.h"
namespace doris::vectorized::parquet {
const cctz::time_zone ConvertParams::utc0 = cctz::utc_time_zone();

#define FOR_LOGICAL_DECIMAL_TYPES(M) \
    M(TYPE_DECIMALV2)                \
    M(TYPE_DECIMAL32)                \
    M(TYPE_DECIMAL64)                \
    M(TYPE_DECIMAL128I)

bool PhysicalToLogicalConverter::is_parquet_native_type(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return true;
    default:
        return false;
    }
}

bool PhysicalToLogicalConverter::is_decimal_type(doris::PrimitiveType type) {
    switch (type) {
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMALV2:
        return true;
    default:
        return false;
    }
}

ColumnPtr PhysicalToLogicalConverter::get_physical_column(tparquet::Type::type src_physical_type,
                                                          TypeDescriptor src_logical_type,
                                                          ColumnPtr& dst_logical_column,
                                                          const DataTypePtr& dst_logical_type,
                                                          bool is_dict_filter) {
    if (is_dict_filter) {
        src_physical_type = tparquet::Type::INT32;
        src_logical_type = TypeDescriptor(PrimitiveType::TYPE_INT);
    }
    if (is_consistent() && _logical_converter->is_consistent()) {
        if (_cached_src_physical_type == nullptr) {
            _cached_src_physical_type = DataTypeFactory::instance().create_data_type(
                    src_logical_type, dst_logical_type->is_nullable());
        }
        return dst_logical_column;
    }

    if (_cached_src_physical_column == nullptr) {
        switch (src_physical_type) {
        case tparquet::Type::type::BOOLEAN:
            _cached_src_physical_type = std::make_shared<DataTypeUInt8>();
            break;
        case tparquet::Type::type::INT32:
            _cached_src_physical_type = std::make_shared<DataTypeInt32>();
            break;
        case tparquet::Type::type::INT64:
            _cached_src_physical_type = std::make_shared<DataTypeInt64>();
            break;
        case tparquet::Type::type::FLOAT:
            _cached_src_physical_type = std::make_shared<DataTypeFloat32>();
            break;
        case tparquet::Type::type::DOUBLE:
            _cached_src_physical_type = std::make_shared<DataTypeFloat64>();
            break;
        case tparquet::Type::type::BYTE_ARRAY:
            _cached_src_physical_type = std::make_shared<DataTypeString>();
            break;
        case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY:
            _cached_src_physical_type = std::make_shared<DataTypeUInt8>();
            break;
        case tparquet::Type::type::INT96:
            _cached_src_physical_type = std::make_shared<DataTypeInt8>();
            break;
        }
        _cached_src_physical_column = _cached_src_physical_type->create_column();
        if (dst_logical_type->is_nullable()) {
            _cached_src_physical_type = make_nullable(_cached_src_physical_type);
        }
    }
    // remove the old cached data
    _cached_src_physical_column->assume_mutable()->clear();

    if (dst_logical_type->is_nullable()) {
        // In order to share null map between parquet converted src column and dst column to avoid copying. It is very tricky that will
        // call mutable function `doris_nullable_column->get_null_map_column_ptr()` which will set `_need_update_has_null = true`.
        // Because some operations such as agg will call `has_null()` to set `_need_update_has_null = false`.
        auto doris_nullable_column = const_cast<ColumnNullable*>(
                static_cast<const ColumnNullable*>(dst_logical_column.get()));
        return ColumnNullable::create(_cached_src_physical_column,
                                      doris_nullable_column->get_null_map_column_ptr());
    }

    return _cached_src_physical_column;
}

static void get_decimal_converter(FieldSchema* field_schema, TypeDescriptor src_logical_type,
                                  const DataTypePtr& dst_logical_type,
                                  ConvertParams* convert_params,
                                  std::unique_ptr<PhysicalToLogicalConverter>& physical_converter) {
    const tparquet::SchemaElement& parquet_schema = field_schema->parquet_schema;
    if (is_decimal(remove_nullable(dst_logical_type))) {
        // using destination decimal type, avoid type and scale change
        src_logical_type = remove_nullable(dst_logical_type)->get_type_as_type_descriptor();
    }

    tparquet::Type::type src_physical_type = parquet_schema.type;
    PrimitiveType src_logical_primitive = src_logical_type.type;
    int dst_scale = src_logical_type.scale;

    if (src_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
        switch (src_logical_primitive) {
#define DISPATCH(LOGICAL_PTYPE)                                                                   \
    case LOGICAL_PTYPE: {                                                                         \
        using DECIMAL_TYPE = typename PrimitiveTypeTraits<LOGICAL_PTYPE>::ColumnType::value_type; \
        convert_params->init_decimal_converter<DECIMAL_TYPE>(dst_scale);                          \
        DecimalScaleParams& scale_params = convert_params->decimal_scale;                         \
        if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {                            \
            physical_converter.reset(                                                             \
                    new FixedSizeToDecimal<DECIMAL_TYPE, DecimalScaleParams::SCALE_UP>(           \
                            parquet_schema.type_length));                                         \
        } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {                   \
            physical_converter.reset(                                                             \
                    new FixedSizeToDecimal<DECIMAL_TYPE, DecimalScaleParams::SCALE_DOWN>(         \
                            parquet_schema.type_length));                                         \
        } else {                                                                                  \
            physical_converter.reset(                                                             \
                    new FixedSizeToDecimal<DECIMAL_TYPE, DecimalScaleParams::NO_SCALE>(           \
                            parquet_schema.type_length));                                         \
        }                                                                                         \
        break;                                                                                    \
    }
            FOR_LOGICAL_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
        default:
            physical_converter.reset(new UnsupportedConverter(src_physical_type, src_logical_type));
        }
    } else if (src_physical_type == tparquet::Type::BYTE_ARRAY) {
        switch (src_logical_primitive) {
#define DISPATCH(LOGICAL_PTYPE)                                                                   \
    case LOGICAL_PTYPE: {                                                                         \
        using DECIMAL_TYPE = typename PrimitiveTypeTraits<LOGICAL_PTYPE>::ColumnType::value_type; \
        convert_params->init_decimal_converter<DECIMAL_TYPE>(dst_scale);                          \
        DecimalScaleParams& scale_params = convert_params->decimal_scale;                         \
        if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {                            \
            physical_converter.reset(                                                             \
                    new StringToDecimal<DECIMAL_TYPE, DecimalScaleParams::SCALE_UP>());           \
        } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {                   \
            physical_converter.reset(                                                             \
                    new StringToDecimal<DECIMAL_TYPE, DecimalScaleParams::SCALE_DOWN>());         \
        } else {                                                                                  \
            physical_converter.reset(                                                             \
                    new StringToDecimal<DECIMAL_TYPE, DecimalScaleParams::NO_SCALE>());           \
        }                                                                                         \
        break;                                                                                    \
    }
            FOR_LOGICAL_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
        default:
            physical_converter.reset(new UnsupportedConverter(src_physical_type, src_logical_type));
        }
    } else if (src_physical_type == tparquet::Type::INT32 ||
               src_physical_type == tparquet::Type::INT64) {
        switch (src_logical_primitive) {
#define DISPATCH(LOGICAL_PTYPE)                                                                   \
    case LOGICAL_PTYPE: {                                                                         \
        using DECIMAL_TYPE = typename PrimitiveTypeTraits<LOGICAL_PTYPE>::ColumnType::value_type; \
        convert_params->init_decimal_converter<DECIMAL_TYPE>(dst_scale);                          \
        DecimalScaleParams& scale_params = convert_params->decimal_scale;                         \
        if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {                            \
            if (src_physical_type == tparquet::Type::INT32) {                                     \
                physical_converter.reset(new NumberToDecimal<int32_t, DECIMAL_TYPE,               \
                                                             DecimalScaleParams::SCALE_UP>());    \
            } else {                                                                              \
                physical_converter.reset(new NumberToDecimal<int64_t, DECIMAL_TYPE,               \
                                                             DecimalScaleParams::SCALE_UP>());    \
            }                                                                                     \
        } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {                   \
            if (src_physical_type == tparquet::Type::INT32) {                                     \
                physical_converter.reset(new NumberToDecimal<int32_t, DECIMAL_TYPE,               \
                                                             DecimalScaleParams::SCALE_DOWN>());  \
            } else {                                                                              \
                physical_converter.reset(new NumberToDecimal<int64_t, DECIMAL_TYPE,               \
                                                             DecimalScaleParams::SCALE_DOWN>());  \
            }                                                                                     \
        } else {                                                                                  \
            if (src_physical_type == tparquet::Type::INT32) {                                     \
                physical_converter.reset(new NumberToDecimal<int32_t, DECIMAL_TYPE,               \
                                                             DecimalScaleParams::NO_SCALE>());    \
            } else {                                                                              \
                physical_converter.reset(new NumberToDecimal<int64_t, DECIMAL_TYPE,               \
                                                             DecimalScaleParams::NO_SCALE>());    \
            }                                                                                     \
        }                                                                                         \
        break;                                                                                    \
    }
            FOR_LOGICAL_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
        default:
            physical_converter.reset(new UnsupportedConverter(src_physical_type, src_logical_type));
        }
    } else {
        physical_converter.reset(new UnsupportedConverter(src_physical_type, src_logical_type));
    }
}

std::unique_ptr<PhysicalToLogicalConverter> PhysicalToLogicalConverter::get_converter(
        FieldSchema* field_schema, TypeDescriptor src_logical_type,
        const DataTypePtr& dst_logical_type, cctz::time_zone* ctz, bool is_dict_filter) {
    std::unique_ptr<ConvertParams> convert_params = std::make_unique<ConvertParams>();
    const tparquet::SchemaElement& parquet_schema = field_schema->parquet_schema;
    convert_params->init(field_schema, ctz);
    tparquet::Type::type src_physical_type = parquet_schema.type;
    std::unique_ptr<PhysicalToLogicalConverter> physical_converter;
    if (is_dict_filter) {
        src_physical_type = tparquet::Type::INT32;
        src_logical_type = TypeDescriptor(PrimitiveType::TYPE_INT);
    }
    PrimitiveType src_logical_primitive = src_logical_type.type;

    if (is_parquet_native_type(src_logical_primitive)) {
        if (is_string_type(src_logical_primitive) &&
            src_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            // for FixedSizeBinary
            physical_converter.reset(new FixedSizeBinaryConverter(parquet_schema.type_length));
        } else {
            physical_converter.reset(new ConsistentPhysicalConverter());
        }
    } else if (src_logical_type == TYPE_TINYINT) {
        physical_converter.reset(new LittleIntPhysicalConverter<TYPE_TINYINT>());
    } else if (src_logical_type == TYPE_SMALLINT) {
        physical_converter.reset(new LittleIntPhysicalConverter<TYPE_SMALLINT>);
    } else if (is_decimal_type(src_logical_primitive)) {
        get_decimal_converter(field_schema, src_logical_type, dst_logical_type,
                              convert_params.get(), physical_converter);
    } else if (src_logical_type == TYPE_DATEV2) {
        physical_converter.reset(new Int32ToDate());
    } else if (src_logical_type == TYPE_DATETIMEV2) {
        if (src_physical_type == tparquet::Type::INT96) {
            // int96 only stores nanoseconds in standard parquet file
            convert_params->reset_time_scale_if_missing(9);
            physical_converter.reset(new Int96toTimestamp());
        } else if (src_physical_type == tparquet::Type::INT64) {
            convert_params->reset_time_scale_if_missing(
                    remove_nullable(dst_logical_type)->get_scale());
            physical_converter.reset(new Int64ToTimestamp());
        } else {
            physical_converter.reset(new UnsupportedConverter(src_physical_type, src_logical_type));
        }
    } else {
        physical_converter.reset(new UnsupportedConverter(src_physical_type, src_logical_type));
    }

    if (physical_converter->support()) {
        physical_converter->_convert_params = std::move(convert_params);
        physical_converter->_logical_converter =
                converter::ColumnTypeConverter::get_converter(src_logical_type, dst_logical_type);
        if (!physical_converter->_logical_converter->support()) {
            physical_converter.reset(new UnsupportedConverter(
                    "Unsupported type change: " +
                    physical_converter->_logical_converter->get_error_msg()));
        }
    }
    return physical_converter;
}

} // namespace doris::vectorized::parquet
