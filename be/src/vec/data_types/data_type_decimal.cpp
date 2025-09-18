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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypesDecimal.cpp
// and modified by Doris

#include "vec/data_types/data_type_decimal.h"

#include <fmt/format.h>
#include <gen_cpp/data.pb.h>
#include <streamvbyte.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "agent/be_exec_version_manager.h"
#include "common/cast_set.h"
#include "runtime/decimalv2_value.h"
#include "util/string_parser.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/common/assert_cast.h"
#include "vec/common/int_exp.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/typeid_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/functions/cast/cast_to_string.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

DataTypePtr get_data_type_with_default_argument(DataTypePtr type) {
    auto transform = [&](DataTypePtr t) -> DataTypePtr {
        if (t->get_primitive_type() == PrimitiveType::TYPE_DECIMALV2) {
            auto res = DataTypeFactory::instance().create_data_type(
                    TYPE_DECIMALV2, t->is_nullable(), BeConsts::MAX_DECIMALV2_PRECISION,
                    BeConsts::MAX_DECIMALV2_SCALE);
            DCHECK_EQ(res->get_scale(), BeConsts::MAX_DECIMALV2_SCALE);
            return res;
        } else if (t->get_primitive_type() == PrimitiveType::TYPE_BINARY ||
                   t->get_primitive_type() == PrimitiveType::TYPE_LAMBDA_FUNCTION) {
            return DataTypeFactory::instance().create_data_type(TYPE_STRING, t->is_nullable());
        } else {
            return t;
        }
    };
    // FIXME(gabriel): DECIMALV2 should be processed in a more general way.
    if (type->get_primitive_type() == PrimitiveType::TYPE_ARRAY) {
        auto nested = std::make_shared<DataTypeArray>(get_data_type_with_default_argument(
                assert_cast<const DataTypeArray*>(remove_nullable(type).get())->get_nested_type()));
        return type->is_nullable() ? make_nullable(nested) : nested;
    } else if (type->get_primitive_type() == PrimitiveType::TYPE_MAP) {
        auto nested = std::make_shared<DataTypeMap>(
                get_data_type_with_default_argument(
                        assert_cast<const DataTypeMap*>(remove_nullable(type).get())
                                ->get_key_type()),
                get_data_type_with_default_argument(
                        assert_cast<const DataTypeMap*>(remove_nullable(type).get())
                                ->get_value_type()));
        return type->is_nullable() ? make_nullable(nested) : nested;
    } else if (type->get_primitive_type() == PrimitiveType::TYPE_STRUCT) {
        std::vector<DataTypePtr> data_types;
        std::vector<std::string> names;
        const auto* type_struct = assert_cast<const DataTypeStruct*>(remove_nullable(type).get());
        data_types.reserve(type_struct->get_elements().size());
        names.reserve(type_struct->get_elements().size());
        for (size_t i = 0; i < type_struct->get_elements().size(); i++) {
            data_types.push_back(get_data_type_with_default_argument(type_struct->get_element(i)));
            names.push_back(type_struct->get_element_name(i));
        }
        auto nested = std::make_shared<DataTypeStruct>(data_types, names);
        return type->is_nullable() ? make_nullable(nested) : nested;
    } else {
        return transform(type);
    }
}

template <PrimitiveType T>
std::string DataTypeDecimal<T>::do_get_name() const {
    std::stringstream ss;
    ss << "Decimal(" << precision << ", " << scale << ")";
    return ss.str();
}

template <PrimitiveType T>
bool DataTypeDecimal<T>::equals(const IDataType& rhs) const {
    if (auto* ptype = typeid_cast<const DataTypeDecimal<T>*>(&rhs)) {
        return precision == ptype->get_precision() && scale == ptype->get_scale();
    }
    return false;
}

template <PrimitiveType T>
std::string DataTypeDecimal<T>::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    if constexpr (T != TYPE_DECIMALV2) {
        auto value = assert_cast<const ColumnType&>(*ptr).get_element(row_num);
        return value.to_string(scale);
    } else {
        auto value = (DecimalV2Value)assert_cast<const ColumnType&>(*ptr).get_element(row_num);
        return value.to_string(get_format_scale());
    }
}

template <PrimitiveType T>
void DataTypeDecimal<T>::to_string(const IColumn& column, size_t row_num,
                                   BufferWritable& ostr) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    if constexpr (T != TYPE_DECIMALV2) {
        FieldType value = assert_cast<const ColumnType&>(*ptr).get_element(row_num);
        auto str = value.to_string(scale);
        ostr.write(str.data(), str.size());
    } else {
        auto value = (DecimalV2Value)assert_cast<const ColumnType&>(*ptr).get_element(row_num);
        auto str = value.to_string(get_format_scale());
        ostr.write(str.data(), str.size());
    }
}

template <PrimitiveType T>
void DataTypeDecimal<T>::to_string_batch(const IColumn& column, ColumnString& column_to) const {
    // column may be column const
    const auto& col_ptr = column.get_ptr();
    const auto& [column_ptr, is_const] = unpack_if_const(col_ptr);
    if (is_const) {
        to_string_batch_impl<true>(column_ptr, column_to);
    } else {
        to_string_batch_impl<false>(column_ptr, column_to);
    }
}

template <PrimitiveType T>
template <bool is_const>
void DataTypeDecimal<T>::to_string_batch_impl(const ColumnPtr& column_ptr,
                                              ColumnString& column_to) const {
    auto& col_vec = assert_cast<const ColumnType&>(*column_ptr);
    const auto size = col_vec.size();
    auto& chars = column_to.get_chars();
    auto& offsets = column_to.get_offsets();
    offsets.resize(size);
    chars.reserve(4 * sizeof(FieldType));
    const auto get_scale = get_format_scale();
    for (int row_num = 0; row_num < size; row_num++) {
        auto num = is_const ? col_vec.get_element(0) : col_vec.get_element(row_num);
        auto str = CastToString::from_decimal(num, get_scale);
        chars.insert(str.begin(), str.end());

        // cast by row, so not use cast_set for performance issue
        offsets[row_num] = static_cast<UInt32>(chars.size());
    }
}

template <PrimitiveType T>
std::string DataTypeDecimal<T>::to_string(const FieldType& value) const {
    if constexpr (T != TYPE_DECIMALV2) {
        return value.to_string(scale);
    } else {
        auto decemalv2_value = (DecimalV2Value)value;
        return decemalv2_value.to_string(get_format_scale());
    }
}

// binary: const flag | row num | real_saved_num | data
// data  : {val1 | val2| ...} or {encode_size | val1 | val2| ...}
template <PrimitiveType T>
int64_t DataTypeDecimal<T>::get_uncompressed_serialized_bytes(const IColumn& column,
                                                              int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        auto size = sizeof(bool) + sizeof(size_t) + sizeof(size_t);
        auto real_need_copy_num = is_column_const(column) ? 1 : column.size();
        auto mem_size = cast_set<UInt32>(sizeof(FieldType) * real_need_copy_num);
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            return size + mem_size;
        } else {
            return size + sizeof(size_t) +
                   std::max(cast_set<size_t>(mem_size),
                            streamvbyte_max_compressedbytes(upper_int32(mem_size)));
        }
    } else {
        auto size = sizeof(FieldType) * column.size();
        if (size <= SERIALIZED_MEM_SIZE_LIMIT) {
            return sizeof(uint32_t) + size;
        } else {
            return sizeof(uint32_t) + sizeof(size_t) +
                   std::max(size,
                            streamvbyte_max_compressedbytes(cast_set<UInt32>(upper_int32(size))));
        }
    }
}

template <PrimitiveType T>
char* DataTypeDecimal<T>::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        const auto* data_column = &column;
        size_t real_need_copy_num = 0;
        buf = serialize_const_flag_and_row_num(&data_column, buf, &real_need_copy_num);

        UInt32 mem_size = cast_set<UInt32>(real_need_copy_num * sizeof(FieldType));
        const auto* origin_data =
                assert_cast<const ColumnDecimal<T>&>(*data_column).get_data().data();

        // column data
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(buf, origin_data, mem_size);
            return buf + mem_size;
        } else {
            auto encode_size =
                    streamvbyte_encode(reinterpret_cast<const uint32_t*>(origin_data),
                                       upper_int32(mem_size), (uint8_t*)(buf + sizeof(size_t)));
            unaligned_store<size_t>(buf, encode_size);
            buf += sizeof(size_t);
            return buf + encode_size;
        }
    } else {
        // row num
        UInt32 mem_size = cast_set<UInt32>(column.size() * sizeof(FieldType));
        *reinterpret_cast<uint32_t*>(buf) = mem_size;
        buf += sizeof(uint32_t);
        // column data
        auto ptr = column.convert_to_full_column_if_const();
        const auto* origin_data =
                assert_cast<const ColumnDecimal<T>&>(*ptr.get()).get_data().data();
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(buf, origin_data, mem_size);
            return buf + mem_size;
        }

        auto encode_size =
                streamvbyte_encode(reinterpret_cast<const uint32_t*>(origin_data),
                                   upper_int32(mem_size), (uint8_t*)(buf + sizeof(size_t)));
        unaligned_store<size_t>(buf, encode_size);
        buf += sizeof(size_t);
        return buf + encode_size;
    }
}
template <PrimitiveType T>
const char* DataTypeDecimal<T>::deserialize(const char* buf, MutableColumnPtr* column,
                                            int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        auto* origin_column = column->get();
        size_t real_have_saved_num = 0;
        buf = deserialize_const_flag_and_row_num(buf, column, &real_have_saved_num);

        // column data
        UInt32 mem_size = cast_set<UInt32>(real_have_saved_num * sizeof(FieldType));
        auto& container = assert_cast<ColumnDecimal<T>*>(origin_column)->get_data();
        container.resize(real_have_saved_num);
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(container.data(), buf, mem_size);
            buf = buf + mem_size;
        } else {
            auto encode_size = unaligned_load<size_t>(buf);
            buf += sizeof(size_t);
            streamvbyte_decode((const uint8_t*)buf, (uint32_t*)(container.data()),
                               upper_int32(mem_size));
            buf = buf + encode_size;
        }
        return buf;
    } else {
        // row num
        uint32_t mem_size = *reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        // column data
        auto& container = assert_cast<ColumnDecimal<T>*>(column->get())->get_data();
        container.resize(mem_size / sizeof(FieldType));
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(container.data(), buf, mem_size);
            return buf + mem_size;
        }

        auto encode_size = unaligned_load<size_t>(buf);
        buf += sizeof(size_t);
        streamvbyte_decode((const uint8_t*)buf, (uint32_t*)(container.data()),
                           upper_int32(mem_size));
        return buf + encode_size;
    }
}

template <PrimitiveType T>
void DataTypeDecimal<T>::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    col_meta->mutable_decimal_param()->set_precision(precision);
    col_meta->mutable_decimal_param()->set_scale(scale);
}

template <PrimitiveType T>
Field DataTypeDecimal<T>::get_default() const {
    return Field::create_field<T>(DecimalField<FieldType>(FieldType(), scale));
}

template <PrimitiveType T>
MutableColumnPtr DataTypeDecimal<T>::create_column() const {
    return ColumnType::create(0, scale);
}

template <PrimitiveType T>
Status DataTypeDecimal<T>::check_column(const IColumn& column) const {
    return check_column_non_nested_type<ColumnType>(column);
}

template <PrimitiveType T>
bool DataTypeDecimal<T>::parse_from_string(const std::string& str, FieldType* res) const {
    StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
    res->value = StringParser::string_to_decimal<DataTypeDecimalSerDe<T>::get_primitive_type()>(
            str.c_str(), cast_set<Int32>(str.size()), precision, scale, &result);
    return result == StringParser::PARSE_SUCCESS || result == StringParser::PARSE_UNDERFLOW;
}

DataTypePtr create_decimal(UInt64 precision_value, UInt64 scale_value, bool use_v2) {
    auto max_precision = use_v2 ? max_decimal_precision<TYPE_DECIMALV2>()
                                : max_decimal_precision<TYPE_DECIMAL256>();
    if (precision_value < min_decimal_precision() || precision_value > max_precision) {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Wrong precision {}, min: {}, max: {}", precision_value,
                               min_decimal_precision(), max_precision);
    }

    if (static_cast<UInt64>(scale_value) > precision_value) {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Negative scales and scales larger than precision are not "
                               "supported, scale_value: {}, precision_value: {}",
                               scale_value, precision_value);
    }

    if (use_v2) {
        return std::make_shared<DataTypeDecimal<TYPE_DECIMALV2>>(precision_value, scale_value);
    }

    if (precision_value <= max_decimal_precision<TYPE_DECIMAL32>()) {
        return std::make_shared<DataTypeDecimal<TYPE_DECIMAL32>>(precision_value, scale_value);
    } else if (precision_value <= max_decimal_precision<TYPE_DECIMAL64>()) {
        return std::make_shared<DataTypeDecimal<TYPE_DECIMAL64>>(precision_value, scale_value);
    } else if (precision_value <= max_decimal_precision<TYPE_DECIMAL128I>()) {
        return std::make_shared<DataTypeDecimal<TYPE_DECIMAL128I>>(precision_value, scale_value);
    }
    return std::make_shared<DataTypeDecimal<TYPE_DECIMAL256>>(precision_value, scale_value);
}

template <PrimitiveType T>
FieldWithDataType DataTypeDecimal<T>::get_field_with_data_type(const IColumn& column,
                                                               size_t row_num) const {
    const auto& decimal_column =
            assert_cast<const ColumnDecimal<T>&, TypeCheckOnRelease::DISABLE>(column);
    Field field;
    decimal_column.get(row_num, field);
    return FieldWithDataType {.field = std::move(field),
                              .base_scalar_type_id = get_primitive_type(),
                              .precision = static_cast<int>(precision),
                              .scale = static_cast<int>(scale)};
}

/// Explicit template instantiations.
template class DataTypeDecimal<TYPE_DECIMAL32>;
template class DataTypeDecimal<TYPE_DECIMAL64>;
template class DataTypeDecimal<TYPE_DECIMALV2>;
template class DataTypeDecimal<TYPE_DECIMAL128I>;
template class DataTypeDecimal<TYPE_DECIMAL256>;

} // namespace doris::vectorized
