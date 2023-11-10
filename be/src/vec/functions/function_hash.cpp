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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionHash.cpp
// and modified by Doris

#include "vec/functions/function_hash.h"

#include "common/status.h"
#include "util/hash_util.hpp"
#include "util/murmur_hash3.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/function_variadic_arguments.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/template_helpers.hpp"

namespace doris::vectorized {
constexpr uint64_t emtpy_value = 0xe28dbde7fe22e41c;

struct MurmurHash2Impl64 {
    static constexpr auto name = "murmurHash2_64";
    using ReturnType = UInt64;

    static Status empty_apply(IColumn& icolumn, size_t input_rows_count) {
        auto& vec_to = assert_cast<ColumnVector<ReturnType>&>(icolumn);
        vec_to.get_data().assign(input_rows_count, static_cast<ReturnType>(emtpy_value));
        return Status::OK();
    }

    static Status first_apply(const IDataType* type, const IColumn* column, size_t input_rows_count,
                              IColumn& icolumn) {
        static_cast<void>(execute_any<true>(type, column, icolumn, input_rows_count));
        return Status::OK();
    }

    static Status combine_apply(const IDataType* type, const IColumn* column,
                                size_t input_rows_count, IColumn& icolumn) {
        static_cast<void>(execute_any<false>(type, column, icolumn, input_rows_count));
        return Status::OK();
    }

    template <typename FromType, bool first>
    static Status execute_int_type(const IColumn* column, IColumn& col_to,
                                   size_t input_rows_count) {
        if (const auto* col_from = check_and_get_column<ColumnVector<FromType>>(column)) {
            const typename ColumnVector<FromType>::Container& vec_from = col_from->get_data();
            size_t size = vec_from.size();
            for (size_t i = 0; i < size; ++i) {
                ReturnType val = HashUtil::murmur_hash2_64(
                        reinterpret_cast<const char*>(reinterpret_cast<const char*>(&vec_from[i])),
                        sizeof(vec_from[i]), 0);
                if (first) {
                    col_to.insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)), 0);
                } else {
                    assert_cast<ColumnVector<ReturnType>&>(col_to).get_data()[i] =
                            IntHash64Impl::apply(
                                    assert_cast<ColumnVector<ReturnType>&>(col_to).get_data()[i]) ^
                            val;
                }
            }
        } else if (auto col_from_const =
                           check_and_get_column_const<ColumnVector<FromType>>(column)) {
            ReturnType value = col_from_const->template get_value<FromType>();
            ReturnType val;
            val = IntHash64Impl::apply(value);
            for (size_t i = 0; i < input_rows_count; ++i) {
                if (first) {
                    col_to.insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)), 0);
                } else {
                    assert_cast<ColumnVector<ReturnType>&>(col_to).get_data()[i] =
                            IntHash64Impl::apply(
                                    assert_cast<ColumnVector<ReturnType>&>(col_to).get_data()[i]) ^
                            val;
                }
            }
        } else {
            DCHECK(false);
            return Status::NotSupported("Illegal column {} of argument of function {}",
                                        column->get_name(), name);
        }
        return Status::OK();
    }

    template <bool first>
    static Status execute_string(const IColumn* column, IColumn& col_to, size_t input_rows_count) {
        if (const auto* col_from = check_and_get_column<ColumnString>(column)) {
            const typename ColumnString::Chars& data = col_from->get_chars();
            const typename ColumnString::Offsets& offsets = col_from->get_offsets();
            size_t size = offsets.size();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i) {
                const ReturnType val = HashUtil::murmur_hash2_64(
                        reinterpret_cast<const char*>(&data[current_offset]),
                        offsets[i] - current_offset, 0);

                if (first) {
                    col_to.insert_data(reinterpret_cast<const char*>(&val), 0);
                } else {
                    assert_cast<ColumnVector<ReturnType>&>(col_to).get_data()[i] =
                            IntHash64Impl::apply(
                                    assert_cast<ColumnVector<ReturnType>&>(col_to).get_data()[i]) ^
                            val;
                }

                current_offset = offsets[i];
            }
        } else if (const ColumnConst* col_from_const =
                           check_and_get_column_const_string_or_fixedstring(column)) {
            auto value = col_from_const->get_value<String>();
            const ReturnType val = HashUtil::murmur_hash2_64(value.data(), value.size(), 0);

            for (size_t i = 0; i < input_rows_count; ++i) {
                if (first) {
                    col_to.insert_data(reinterpret_cast<const char*>(&val), 0);
                } else {
                    assert_cast<ColumnVector<ReturnType>&>(col_to).get_data()[i] =
                            IntHash64Impl::apply(
                                    assert_cast<ColumnVector<ReturnType>&>(col_to).get_data()[i]) ^
                            val;
                }
            }
        } else {
            DCHECK(false);
            return Status::NotSupported("Illegal column {} of argument of function {}",
                                        column->get_name(), name);
        }
        return Status::OK();
    }

    template <bool first>
    static Status execute_any(const IDataType* from_type, const IColumn* icolumn, IColumn& col_to,
                              size_t input_rows_count) {
        WhichDataType which(from_type);
        if (which.is_string()) {
            return execute_string<first>(icolumn, col_to, input_rows_count);
        }

#define DISPATCH(TYPE, COLUMN_TYPE)   \
    if (which.idx == TypeIndex::TYPE) \
        return execute_int_type<TYPE, first>(icolumn, col_to, input_rows_count);
        NUMERIC_TYPE_TO_COLUMN_TYPE(DISPATCH)
#undef DISPATCH
        return Status::NotSupported("argument_type {} not supported", from_type->get_name());
    }
};
using FunctionMurmurHash2_64 = FunctionVariadicArgumentsBase<DataTypeUInt64, MurmurHash2Impl64>;

template <typename ReturnType>
struct MurmurHash3ImplName {};

template <>
struct MurmurHash3ImplName<Int32> {
    static constexpr auto name = "murmur_hash3_32";
};

template <>
struct MurmurHash3ImplName<Int64> {
    static constexpr auto name = "murmur_hash3_64";
};

template <typename ReturnType>
struct MurmurHash3Impl {
    static constexpr auto name = MurmurHash3ImplName<ReturnType>::name;

    static Status empty_apply(IColumn& icolumn, size_t input_rows_count) {
        ColumnVector<ReturnType>& vec_to = assert_cast<ColumnVector<ReturnType>&>(icolumn);
        vec_to.get_data().assign(input_rows_count, static_cast<ReturnType>(emtpy_value));
        return Status::OK();
    }

    static Status first_apply(const IDataType* type, const IColumn* column, size_t input_rows_count,
                              IColumn& icolumn) {
        return execute<true>(type, column, input_rows_count, icolumn);
    }

    static Status combine_apply(const IDataType* type, const IColumn* column,
                                size_t input_rows_count, IColumn& icolumn) {
        return execute<false>(type, column, input_rows_count, icolumn);
    }

    template <bool first>
    static Status execute(const IDataType* type, const IColumn* column, size_t input_rows_count,
                          IColumn& col_to) {
        auto* col_to_data = assert_cast<ColumnVector<ReturnType>&>(col_to).get_data().data();
        if (const auto* col_from = check_and_get_column<ColumnString>(column)) {
            const typename ColumnString::Chars& data = col_from->get_chars();
            const typename ColumnString::Offsets& offsets = col_from->get_offsets();
            size_t size = offsets.size();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i) {
                if (first) {
                    if constexpr (std::is_same_v<ReturnType, Int32>) {
                        UInt32 val = HashUtil::murmur_hash3_32(
                                reinterpret_cast<const char*>(&data[current_offset]),
                                offsets[i] - current_offset, HashUtil::MURMUR3_32_SEED);
                        col_to.insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)),
                                           0);
                    } else {
                        UInt64 val = 0;
                        murmur_hash3_x64_64(reinterpret_cast<const char*>(&data[current_offset]),
                                            offsets[i] - current_offset, 0, &val);
                        col_to.insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)),
                                           0);
                    }
                } else {
                    if constexpr (std::is_same_v<ReturnType, Int32>) {
                        col_to_data[i] = HashUtil::murmur_hash3_32(
                                reinterpret_cast<const char*>(&data[current_offset]),
                                offsets[i] - current_offset,
                                assert_cast<ColumnInt32&>(col_to).get_data()[i]);
                    } else {
                        murmur_hash3_x64_64(reinterpret_cast<const char*>(&data[current_offset]),
                                            offsets[i] - current_offset,
                                            assert_cast<ColumnInt64&>(col_to).get_data()[i],
                                            col_to_data + i);
                    }
                }
                current_offset = offsets[i];
            }
        } else if (const ColumnConst* col_from_const =
                           check_and_get_column_const_string_or_fixedstring(column)) {
            auto value = col_from_const->get_value<String>();
            for (size_t i = 0; i < input_rows_count; ++i) {
                if (first) {
                    if constexpr (std::is_same_v<ReturnType, Int32>) {
                        UInt32 val = HashUtil::murmur_hash3_32(value.data(), value.size(),
                                                               HashUtil::MURMUR3_32_SEED);
                        col_to.insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)),
                                           0);
                    } else {
                        UInt64 val = 0;
                        murmur_hash3_x64_64(value.data(), value.size(), 0, &val);
                        col_to.insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)),
                                           0);
                    }
                } else {
                    if constexpr (std::is_same_v<ReturnType, Int32>) {
                        col_to_data[i] = HashUtil::murmur_hash3_32(
                                value.data(), value.size(),
                                assert_cast<ColumnInt32&>(col_to).get_data()[i]);
                    } else {
                        murmur_hash3_x64_64(value.data(), value.size(),
                                            assert_cast<ColumnInt64&>(col_to).get_data()[i],
                                            col_to_data + i);
                    }
                }
            }
        } else {
            DCHECK(false);
            return Status::NotSupported("Illegal column {} of argument of function {}",
                                        column->get_name(), name);
        }
        return Status::OK();
    }
};
using FunctionMurmurHash3_32 = FunctionVariadicArgumentsBase<DataTypeInt32, MurmurHash3Impl<Int32>>;
using FunctionMurmurHash3_64 = FunctionVariadicArgumentsBase<DataTypeInt64, MurmurHash3Impl<Int64>>;

void register_function_hash(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMurmurHash2_64>();
    factory.register_function<FunctionMurmurHash3_32>();
    factory.register_function<FunctionMurmurHash3_64>();
}
} // namespace doris::vectorized