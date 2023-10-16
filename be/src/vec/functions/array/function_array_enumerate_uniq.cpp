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

#include <fmt/format.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <new>
#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_map.h"
#include "vec/common/hash_table/hash_map_context.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/common/hash_table/hash_table_allocator.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/common/uint128.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris
template <typename, typename>
struct DefaultHash;

namespace doris::vectorized {

class FunctionArrayEnumerateUniq : public IFunction {
private:
    static constexpr size_t INITIAL_SIZE_DEGREE = 5;

public:
    using NullMapType = PaddedPODArray<UInt8>;
    static constexpr auto name = "array_enumerate_uniq";
    static FunctionPtr create() { return std::make_shared<FunctionArrayEnumerateUniq>(); }
    String get_name() const override { return name; }
    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 1; }
    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments.empty()) {
            LOG(FATAL) << "Incorrect number of arguments for array_enumerate_uniq function";
        }
        bool is_nested_nullable = false;
        for (size_t i = 0; i < arguments.size(); ++i) {
            const DataTypeArray* array_type =
                    check_and_get_data_type<DataTypeArray>(remove_nullable(arguments[i]).get());
            if (!array_type) {
                LOG(FATAL) << "The " << i
                           << "-th argument for function " + get_name() +
                                      " must be an array but it has type " +
                                      arguments[i]->get_name() + ".";
            }
            if (i == 0) {
                is_nested_nullable = array_type->get_nested_type()->is_nullable();
            }
        }

        auto return_nested_type = std::make_shared<DataTypeInt64>();
        DataTypePtr return_type = std::make_shared<DataTypeArray>(
                is_nested_nullable ? make_nullable(return_nested_type) : return_nested_type);
        if (arguments.size() == 1 && arguments[0]->is_nullable()) {
            return_type = make_nullable(return_type);
        }
        return return_type;
    }

// When compiling `FunctionArrayEnumerateUniq::_execute_by_hash`, `AllocatorWithStackMemory::free(buf)`
// will be called when `delete HashMapContainer`. the gcc compiler will think that `size > N` and `buf` is not heap memory,
// and report an error `' void free(void*)' called on unallocated object 'hash_map'`
// This only fails on doris docker + gcc 11.1, no problem on doris docker + clang 16.0.1,
// no problem on ldb_toolchanin gcc 11.1 and clang 16.0.1.
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
#endif // __GNUC__

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnRawPtrs data_columns(arguments.size());
        const ColumnArray::Offsets64* offsets = nullptr;
        ColumnPtr src_offsets;
        Columns src_columns; // to keep ownership

        const ColumnArray* first_column_array = nullptr;

        for (size_t i = 0; i < arguments.size(); i++) {
            src_columns.emplace_back(
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const());
            ColumnPtr& cur_column = src_columns[i];
            const ColumnArray* array =
                    check_and_get_column<ColumnArray>(remove_nullable(cur_column->get_ptr()));
            if (!array) {
                return Status::RuntimeError(
                        fmt::format("Illegal column {}, of first argument of function {}",
                                    cur_column->get_name(), get_name()));
            }

            const ColumnArray::Offsets64& cur_offsets = array->get_offsets();
            if (i == 0) {
                first_column_array = array;
                offsets = &cur_offsets;
                src_offsets = array->get_offsets_ptr();
            } else if (*offsets != cur_offsets) {
                return Status::RuntimeError(fmt::format(
                        "lengths of all arrays of fucntion {} must be equal.", get_name()));
            }
            const auto* array_data = &array->get_data();
            data_columns[i] = array_data;
        }

        const NullMapType* null_map = nullptr;
        if (arguments.size() == 1 && data_columns[0]->is_nullable()) {
            const ColumnNullable* nullable = check_and_get_column<ColumnNullable>(*data_columns[0]);
            data_columns[0] = nullable->get_nested_column_ptr();
            null_map = &nullable->get_null_map_column().get_data();
        }

        auto dst_nested_column = ColumnInt64::create();
        ColumnInt64::Container& dst_values = dst_nested_column->get_data();
        dst_values.resize(offsets->back());

        if (arguments.size() == 1) {
            DataTypePtr src_column_type = block.get_by_position(arguments[0]).type;
            if (src_column_type->is_nullable()) {
                src_column_type =
                        assert_cast<const DataTypeNullable&>(*src_column_type).get_nested_type();
            }
            auto nested_type =
                    assert_cast<const DataTypeArray&>(*src_column_type).get_nested_type();
            WhichDataType which(remove_nullable(nested_type));
            if (which.is_uint8()) {
                _execute_number<ColumnUInt8>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_int8()) {
                _execute_number<ColumnInt8>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_int16()) {
                _execute_number<ColumnInt16>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_int32()) {
                _execute_number<ColumnInt32>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_int64()) {
                _execute_number<ColumnInt64>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_int128()) {
                _execute_number<ColumnInt128>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_float32()) {
                _execute_number<ColumnFloat32>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_float64()) {
                _execute_number<ColumnFloat64>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_date()) {
                _execute_number<ColumnDate>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_date_time()) {
                _execute_number<ColumnDateTime>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_date_v2()) {
                _execute_number<ColumnDateV2>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_decimal32()) {
                _execute_number<ColumnDecimal32>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_decimal64()) {
                _execute_number<ColumnDecimal64>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_decimal128i()) {
                _execute_number<ColumnDecimal128I>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_date_time_v2()) {
                _execute_number<ColumnDateTimeV2>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_decimal128()) {
                _execute_number<ColumnDecimal128>(data_columns, *offsets, null_map, dst_values);
            } else if (which.is_string()) {
                _execute_string(data_columns, *offsets, null_map, dst_values);
            }
        } else {
            _execute_by_hash<MethodSerialized<PHHashMap<StringRef, Int64>>, false>(
                    data_columns, *offsets, nullptr, dst_values);
        }

        ColumnPtr nested_column = dst_nested_column->get_ptr();
        if (first_column_array->get_data().is_nullable()) {
            nested_column = ColumnNullable::create(nested_column,
                                                   ColumnUInt8::create(nested_column->size(), 0));
        }
        ColumnPtr res_column = ColumnArray::create(std::move(nested_column), src_offsets);
        if (arguments.size() == 1 && block.get_by_position(arguments[0]).column->is_nullable()) {
            auto left_column =
                    block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
            const ColumnNullable* nullable = check_and_get_column<ColumnNullable>(left_column);
            res_column = ColumnNullable::create(
                    res_column, nullable->get_null_map_column().clone_resized(nullable->size()));
        }

        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }

private:
    template <typename HashTableContext, bool is_nullable>
    void _execute_by_hash(const ColumnRawPtrs& columns, const ColumnArray::Offsets64& offsets,
                          [[maybe_unused]] const NullMap* null_map,
                          ColumnInt64::Container& dst_values) const {
        HashTableContext ctx;
        ctx.init_serialized_keys(columns, columns[0]->size(),
                                 null_map ? null_map->data() : nullptr);

        using KeyGetter = typename HashTableContext::State;
        KeyGetter key_getter(columns);

        auto creator = [&](const auto& ctor, auto& key, auto& origin) { ctor(key, 0); };
        auto creator_for_null_key = [&](auto& mapped) { mapped = 0; };

        ColumnArray::Offset64 prev_off = 0;
        for (size_t off : offsets) {
            ctx.hash_table->clear_and_shrink();
            Int64 null_count = 0;
            for (ColumnArray::Offset64 j = prev_off; j < off; ++j) {
                if constexpr (is_nullable) {
                    if ((*null_map)[j]) {
                        dst_values[j] = ++null_count;
                        continue;
                    }
                }
                auto& mapped = ctx.lazy_emplace(key_getter, j, creator, creator_for_null_key);
                mapped++;
                dst_values[j] = mapped;
            }
            prev_off = off;
        }
    }

    template <typename ColumnType>
    void _execute_number(const ColumnRawPtrs& columns, const ColumnArray::Offsets64& offsets,
                         const NullMapType* null_map, ColumnInt64::Container& dst_values) const {
        using NestType = typename ColumnType::value_type;
        using ElementNativeType = typename NativeType<NestType>::Type;
        using HashMethod =
                MethodOneNumber<ElementNativeType,
                                PHHashMap<ElementNativeType, Int64, HashCRC32<ElementNativeType>>>;
        if (null_map != nullptr) {
            _execute_by_hash<HashMethod, true>(columns, offsets, null_map, dst_values);
        } else {
            _execute_by_hash<HashMethod, false>(columns, offsets, nullptr, dst_values);
        }
    }

    void _execute_string(const ColumnRawPtrs& columns, const ColumnArray::Offsets64& offsets,
                         const NullMapType* null_map, ColumnInt64::Container& dst_values) const {
        using HashMethod = MethodStringNoCache<PHHashMap<StringRef, Int64>>;
        if (null_map != nullptr) {
            _execute_by_hash<HashMethod, true>(columns, offsets, null_map, dst_values);
        } else {
            _execute_by_hash<HashMethod, false>(columns, offsets, nullptr, dst_values);
        }
    }
};

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif // __GNUC__

void register_function_array_enumerate_uniq(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayEnumerateUniq>();
}

} // namespace doris::vectorized
