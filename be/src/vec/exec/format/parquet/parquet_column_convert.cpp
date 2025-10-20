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

#include "common/cast_set.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/column_nullable.h"

namespace doris::vectorized::parquet {
#include "common/compile_check_begin.h"
const cctz::time_zone ConvertParams::utc0 = cctz::utc_time_zone();

#define FOR_LOGICAL_DECIMAL_TYPES(M) \
    M(TYPE_DECIMAL32)                \
    M(TYPE_DECIMAL64)                \
    M(TYPE_DECIMAL128I)              \
    M(TYPE_DECIMAL256)

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
    case TYPE_DECIMAL256:
    case TYPE_DECIMALV2:
        return true;
    default:
        return false;
    }
}

ColumnPtr PhysicalToLogicalConverter::get_physical_column(tparquet::Type::type src_physical_type,
                                                          DataTypePtr src_logical_type,
                                                          ColumnPtr& dst_logical_column,
                                                          const DataTypePtr& dst_logical_type,
                                                          bool is_dict_filter) {
    if (is_dict_filter) {
        src_physical_type = tparquet::Type::INT32;
        src_logical_type = DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, dst_logical_type->is_nullable());
    }

    if (!_convert_params->is_type_compatibility && is_consistent() &&
        _logical_converter->is_consistent()) {
        if (_cached_src_physical_type == nullptr) {
            _cached_src_physical_type = dst_logical_type->is_nullable()
                                                ? make_nullable(src_logical_type)
                                                : remove_nullable(src_logical_type);
        }
        return dst_logical_column;
    }

    if (!_cached_src_physical_column) {
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
        auto* doris_nullable_column = assert_cast<const ColumnNullable*>(dst_logical_column.get());
        return ColumnNullable::create(_cached_src_physical_column,
                                      doris_nullable_column->get_null_map_column_ptr());
    }

    return _cached_src_physical_column;
}

static void get_decimal_converter(const FieldSchema* field_schema, DataTypePtr src_logical_type,
                                  const DataTypePtr& dst_logical_type,
                                  ConvertParams* convert_params,
                                  std::unique_ptr<PhysicalToLogicalConverter>& physical_converter) {
    const tparquet::SchemaElement& parquet_schema = field_schema->parquet_schema;
    if (is_decimal(dst_logical_type->get_primitive_type())) {
        src_logical_type = create_decimal(parquet_schema.precision, parquet_schema.scale, false);
    }

    tparquet::Type::type src_physical_type = parquet_schema.type;
    PrimitiveType src_logical_primitive = src_logical_type->get_primitive_type();

    if (src_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
        switch (src_logical_primitive) {
#define DISPATCH(LOGICAL_PTYPE)                                                     \
    case LOGICAL_PTYPE: {                                                           \
        physical_converter.reset(                                                   \
                new FixedSizeToDecimal<LOGICAL_PTYPE>(parquet_schema.type_length)); \
        break;                                                                      \
    }
            FOR_LOGICAL_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
        default:
            physical_converter =
                    std::make_unique<UnsupportedConverter>(src_physical_type, src_logical_type);
        }
    } else if (src_physical_type == tparquet::Type::BYTE_ARRAY) {
        switch (src_logical_primitive) {
#define DISPATCH(LOGICAL_PTYPE)                                         \
    case LOGICAL_PTYPE: {                                               \
        physical_converter.reset(new StringToDecimal<LOGICAL_PTYPE>()); \
        break;                                                          \
    }
            FOR_LOGICAL_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
        default:
            physical_converter =
                    std::make_unique<UnsupportedConverter>(src_physical_type, src_logical_type);
        }
    } else if (src_physical_type == tparquet::Type::INT32 ||
               src_physical_type == tparquet::Type::INT64) {
        switch (src_logical_primitive) {
#define DISPATCH(LOGICAL_PTYPE)                                                          \
    case LOGICAL_PTYPE: {                                                                \
        if (src_physical_type == tparquet::Type::INT32) {                                \
            physical_converter.reset(new NumberToDecimal<TYPE_INT, LOGICAL_PTYPE>());    \
        } else {                                                                         \
            physical_converter.reset(new NumberToDecimal<TYPE_BIGINT, LOGICAL_PTYPE>()); \
        }                                                                                \
        break;                                                                           \
    }
            FOR_LOGICAL_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
        default:
            physical_converter =
                    std::make_unique<UnsupportedConverter>(src_physical_type, src_logical_type);
        }
    } else {
        physical_converter =
                std::make_unique<UnsupportedConverter>(src_physical_type, src_logical_type);
    }
}

std::unique_ptr<PhysicalToLogicalConverter> PhysicalToLogicalConverter::get_converter(
        const FieldSchema* field_schema, DataTypePtr src_logical_type,
        const DataTypePtr& dst_logical_type, const cctz::time_zone* ctz, bool is_dict_filter) {
    std::unique_ptr<ConvertParams> convert_params = std::make_unique<ConvertParams>();
    const tparquet::SchemaElement& parquet_schema = field_schema->parquet_schema;
    convert_params->init(field_schema, ctz);
    tparquet::Type::type src_physical_type = parquet_schema.type;
    std::unique_ptr<PhysicalToLogicalConverter> physical_converter;
    if (is_dict_filter) {
        src_physical_type = tparquet::Type::INT32;
        src_logical_type = DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, dst_logical_type->is_nullable());
    }
    PrimitiveType src_logical_primitive = src_logical_type->get_primitive_type();

    if (field_schema->is_type_compatibility) {
        if (src_logical_primitive == TYPE_SMALLINT) {
            physical_converter = std::make_unique<UnsignedIntegerConverter<TYPE_SMALLINT>>();
        } else if (src_logical_primitive == TYPE_INT) {
            physical_converter = std::make_unique<UnsignedIntegerConverter<TYPE_INT>>();
        } else if (src_logical_primitive == TYPE_BIGINT) {
            physical_converter = std::make_unique<UnsignedIntegerConverter<TYPE_BIGINT>>();
        } else if (src_logical_primitive == TYPE_LARGEINT) {
            physical_converter = std::make_unique<UnsignedIntegerConverter<TYPE_LARGEINT>>();
        } else {
            physical_converter =
                    std::make_unique<UnsupportedConverter>(src_physical_type, src_logical_type);
        }
    } else if (is_parquet_native_type(src_logical_primitive)) {
        if (is_string_type(src_logical_primitive) &&
            src_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            // for FixedSizeBinary
            physical_converter =
                    std::make_unique<FixedSizeBinaryConverter>(parquet_schema.type_length);
        } else {
            physical_converter = std::make_unique<ConsistentPhysicalConverter>();
        }
    } else if (src_logical_primitive == TYPE_TINYINT) {
        physical_converter = std::make_unique<LittleIntPhysicalConverter<TYPE_TINYINT>>();
    } else if (src_logical_primitive == TYPE_SMALLINT) {
        physical_converter = std::make_unique<LittleIntPhysicalConverter<TYPE_SMALLINT>>();
    } else if (is_decimal_type(src_logical_primitive)) {
        get_decimal_converter(field_schema, src_logical_type, dst_logical_type,
                              convert_params.get(), physical_converter);
    } else if (src_logical_primitive == TYPE_DATEV2) {
        physical_converter = std::make_unique<Int32ToDate>();
    } else if (src_logical_primitive == TYPE_DATETIMEV2) {
        if (src_physical_type == tparquet::Type::INT96) {
            // int96 only stores nanoseconds in standard parquet file
            convert_params->reset_time_scale_if_missing(9);
            physical_converter = std::make_unique<Int96toTimestamp>();
        } else if (src_physical_type == tparquet::Type::INT64) {
            convert_params->reset_time_scale_if_missing(src_logical_type->get_scale());
            physical_converter = std::make_unique<Int64ToTimestamp>();
        } else {
            physical_converter =
                    std::make_unique<UnsupportedConverter>(src_physical_type, src_logical_type);
        }
    } else {
        physical_converter =
                std::make_unique<UnsupportedConverter>(src_physical_type, src_logical_type);
    }

    if (physical_converter->support()) {
        physical_converter->_convert_params = std::move(convert_params);
        physical_converter->_logical_converter = converter::ColumnTypeConverter::get_converter(
                src_logical_type, dst_logical_type, converter::FileFormat::PARQUET);
        if (!physical_converter->_logical_converter->support()) {
            physical_converter = std::make_unique<UnsupportedConverter>(
                    "Unsupported type change: " +
                    physical_converter->_logical_converter->get_error_msg());
        }
    }
    return physical_converter;
}
#include "common/compile_check_end.h"

} // namespace doris::vectorized::parquet
