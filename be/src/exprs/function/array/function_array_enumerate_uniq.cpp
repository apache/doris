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
#include <vector>

#include "common/status.h"
#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/call_on_type_index.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/define_primitive_type.h"
#include "core/pod_array_fwd.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/uint128.h"
#include "exec/common/columns_hashing.h"
#include "exec/common/hash_table/hash.h"
#include "exec/common/hash_table/hash_map_context.h"
#include "exec/common/util.hpp"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris
template <typename, typename>
struct DefaultHash;

namespace doris {

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
            throw doris::Exception(
                    ErrorCode::INVALID_ARGUMENT,
                    "Incorrect number of arguments for array_enumerate_uniq function");
        }
        bool is_nested_nullable = false;
        for (size_t i = 0; i < arguments.size(); ++i) {
            if (arguments[i]->is_null_literal()) {
                return make_nullable(std::make_shared<DataTypeNothing>());
            }
            const DataTypeArray* array_type =
                    check_and_get_data_type<DataTypeArray>(remove_nullable(arguments[i]).get());
            if (!array_type) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "The {} -th argument for function: {} .must be an array but it type is {}",
                        i, get_name(), arguments[i]->get_name());
            }
            is_nested_nullable = is_nested_nullable || array_type->get_nested_type()->is_nullable();
        }

        auto return_nested_type = std::make_shared<DataTypeInt64>();
        DataTypePtr return_type = std::make_shared<DataTypeArray>(
                is_nested_nullable ? make_nullable(return_nested_type) : return_nested_type);
        return have_nullable(arguments) ? make_nullable(return_type) : return_type;
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
                        uint32_t result, size_t input_rows_count) const override {
        ColumnUInt8::MutablePtr result_null_map;
        ColumnUInt8::Container* result_null_map_data = nullptr;
        if (block.get_by_position(result).type->is_nullable()) {
            result_null_map = ColumnUInt8::create(input_rows_count, 0);
            result_null_map_data = &result_null_map->get_data();
        }

        std::vector<const ColumnArray*> array_columns(arguments.size());
        Columns src_columns;
        src_columns.reserve(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i) {
            auto cur_column =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (cur_column->only_null()) {
                auto& result_column = block.get_by_position(result);
                result_column.column =
                        result_column.type->create_column_const(input_rows_count, Field());
                return Status::OK();
            }
            if (const auto* nullable = check_and_get_column<ColumnNullable>(cur_column.get())) {
                VectorizedUtils::update_null_map(*result_null_map_data,
                                                 nullable->get_null_map_data());
                cur_column = nullable->get_nested_column_ptr();
            }
            src_columns.emplace_back(std::move(cur_column));
            const auto* array = check_and_get_column<ColumnArray>(src_columns.back().get());
            if (!array) {
                return Status::RuntimeError(
                        fmt::format("Illegal column {}, of first argument of function {}",
                                    src_columns.back()->get_name(), get_name()));
            }
            array_columns[i] = array;
        }

        bool has_hidden_nested_data = false;
        if (result_null_map_data != nullptr) {
            for (size_t row = 0; row < input_rows_count && !has_hidden_nested_data; ++row) {
                if (!(*result_null_map_data)[row]) {
                    continue;
                }
                for (const auto* array : array_columns) {
                    const auto& current_offsets = array->get_offsets();
                    if (current_offsets[row] != current_offsets[row - 1]) {
                        has_hidden_nested_data = true;
                        break;
                    }
                }
            }
        }

        ColumnRawPtrs data_columns(arguments.size());
        const ColumnArray::Offsets64* offsets = nullptr;
        ColumnPtr result_offsets;
        MutableColumns compacted_data_columns;
        if (!has_hidden_nested_data) {
            offsets = &array_columns[0]->get_offsets();
            result_offsets = array_columns[0]->get_offsets_ptr();
            for (size_t i = 0; i < arguments.size(); ++i) {
                if (i > 0 && *offsets != array_columns[i]->get_offsets()) {
                    return Status::RuntimeError(fmt::format(
                            "lengths of all arrays of function {} must be equal.", get_name()));
                }
                data_columns[i] = &array_columns[i]->get_data();
            }
        } else {
            // Outer-NULL rows may retain payload and shift each input's physical offsets.
            // Compact visible rows so the hash key getter can keep using one element index.
            compacted_data_columns.resize(arguments.size());
            for (size_t i = 0; i < arguments.size(); ++i) {
                compacted_data_columns[i] = array_columns[i]->get_data().clone_empty();
            }

            auto compacted_offsets = ColumnArray::ColumnOffsets::create();
            auto& compacted_offsets_data = compacted_offsets->get_data();
            compacted_offsets_data.reserve(input_rows_count);
            size_t compacted_offset = 0;
            for (size_t row = 0; row < input_rows_count; ++row) {
                if ((*result_null_map_data)[row]) {
                    compacted_offsets_data.push_back(compacted_offset);
                    continue;
                }

                size_t row_size = 0;
                for (size_t i = 0; i < arguments.size(); ++i) {
                    const auto& current_offsets = array_columns[i]->get_offsets();
                    const size_t row_begin = current_offsets[row - 1];
                    const size_t current_row_size = current_offsets[row] - row_begin;
                    if (i == 0) {
                        row_size = current_row_size;
                    } else if (current_row_size != row_size) {
                        return Status::RuntimeError(fmt::format(
                                "lengths of all arrays of function {} must be equal.", get_name()));
                    }
                    compacted_data_columns[i]->insert_range_from(array_columns[i]->get_data(),
                                                                 row_begin, row_size);
                }
                compacted_offset += row_size;
                compacted_offsets_data.push_back(compacted_offset);
            }

            for (size_t i = 0; i < arguments.size(); ++i) {
                data_columns[i] = compacted_data_columns[i].get();
            }
            offsets = &compacted_offsets_data;
            result_offsets = std::move(compacted_offsets);
        }

        const NullMapType* null_map = nullptr;
        if (arguments.size() == 1 &&
            (nullptr != check_and_get_column<ColumnNullable>(data_columns[0]))) {
            const auto* nullable = check_and_get_column<ColumnNullable>(data_columns[0]);
            data_columns[0] = nullable->get_nested_column_ptr().get();
            null_map = &nullable->get_null_map_column().get_data();
        }

        auto dst_nested_column = ColumnInt64::create();
        ColumnInt64::Container& dst_values = dst_nested_column->get_data();
        dst_values.resize(offsets->empty() ? 0 : offsets->back());

        if (arguments.size() == 1) {
            DataTypePtr src_column_type = block.get_by_position(arguments[0]).type;
            if (src_column_type->is_nullable()) {
                src_column_type =
                        assert_cast<const DataTypeNullable&>(*src_column_type).get_nested_type();
            }
            auto nested_type =
                    assert_cast<const DataTypeArray&>(*src_column_type).get_nested_type();

            auto call = [&](const auto& type) -> bool {
                using DispatchType = std::decay_t<decltype(type)>;
                _execute_number<typename DispatchType::ColumnType>(data_columns, *offsets, null_map,
                                                                   dst_values);
                return true;
            };

            if (is_string_type(nested_type->get_primitive_type())) {
                _execute_string(data_columns, *offsets, null_map, dst_values);
            } else if (!dispatch_switch_scalar(nested_type->get_primitive_type(), call)) {
                return Status::RuntimeError(fmt::format(
                        "execute failed or unsupported types for function {}({})", get_name(),
                        block.get_by_position(arguments[0]).type->get_name()));
            }
        } else {
            _execute_by_hash<MethodSerialized<PHHashMap<StringRef, Int64>>, false>(
                    data_columns, *offsets, nullptr, dst_values);
        }

        ColumnPtr nested_column = dst_nested_column->get_ptr();
        const auto& result_array_type = assert_cast<const DataTypeArray&>(
                *remove_nullable(block.get_by_position(result).type));
        if (result_array_type.get_nested_type()->is_nullable()) {
            nested_column = ColumnNullable::create(nested_column,
                                                   ColumnUInt8::create(nested_column->size(), 0));
        }
        ColumnPtr res_column =
                ColumnArray::create(std::move(nested_column), std::move(result_offsets));
        if (block.get_by_position(result).type->is_nullable()) {
            res_column = ColumnNullable::create(std::move(res_column), std::move(result_null_map));
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
        ctx.init_serialized_keys(columns, static_cast<uint32_t>(columns[0]->size()),
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
                auto& mapped = *ctx.lazy_emplace(key_getter, j, creator, creator_for_null_key);
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
} // namespace doris
