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

#include <rapidjson/encodings.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cstdint>
#include <string>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_multi_top.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/space_saving.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

inline constexpr UInt64 TOP_K_MAX_SIZE = 0xFFFFFF;

struct AggregateFunctionTopKGenericData {
    using Set = SpaceSaving<StringRef, StringRefHash>;

    Set value;
};

class AggregateFunctionMultiTopN final
        : public IAggregateFunctionDataHelper<AggregateFunctionTopKGenericData,
                                              AggregateFunctionMultiTopN>,
          AggregateFunctionMultiTop {
private:
    using State = AggregateFunctionTopKGenericData;

public:
    AggregateFunctionMultiTopN(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<AggregateFunctionTopKGenericData,
                                           AggregateFunctionMultiTopN>(argument_types_),
              column_size(argument_types_.size() - 2) {}

    String get_name() const override { return "multi_topn"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    bool allocates_memory_in_arena() const override { return true; }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).value.write(buf);

        write_var_uint(threshold, buf);
        write_var_uint(reserved, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        auto readStringBinaryInto = [](Arena& arena, BufferReadable& buf) {
            size_t size = 0;
            read_var_uint(size, buf);

            if (UNLIKELY(size > DEFAULT_MAX_STRING_SIZE)) {
                throw Exception(ErrorCode::INTERNAL_ERROR, "Too large string size.");
            }

            char* data = arena.alloc(size);
            buf.read(data, size);

            return StringRef(data, size);
        };

        auto& set = this->data(place).value;
        set.clear();

        size_t size = 0;
        read_var_uint(size, buf);
        if (UNLIKELY(size > TOP_K_MAX_SIZE)) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Too large size ({}) for aggregate function '{}' state (maximum is {})",
                            size, get_name(), TOP_K_MAX_SIZE);
        }

        set.resize(size);
        for (size_t i = 0; i < size; ++i) {
            auto ref = readStringBinaryInto(*arena, buf);
            uint64_t count = 0;
            uint64_t error = 0;
            read_var_uint(count, buf);
            read_var_uint(error, buf);
            set.insert(ref, count, error);
            arena->rollback(ref.size);
        }

        set.read_alpha_map(buf);

        read_var_uint(threshold, buf);
        read_var_uint(reserved, buf);
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena* arena) const override {
        if (!init_flag) {
            lazy_init(columns, row_num);
        }

        auto& set = this->data(place).value;
        if (set.capacity() != reserved) {
            set.resize(reserved);
        }

        auto all_serialize_value_into_arena =
                [](size_t i, size_t keys_size, const IColumn** columns, Arena* arena) -> StringRef {
            const char* begin = nullptr;

            size_t sum_size = 0;
            for (size_t j = 0; j < keys_size; ++j) {
                sum_size += columns[j]->serialize_value_into_arena(i, *arena, begin).size;
            }

            return {begin, sum_size};
        };

        StringRef str_serialized =
                all_serialize_value_into_arena(row_num, column_size, columns, arena);
        set.insert(str_serialized);
        arena->rollback(str_serialized.size);
    }

    void add_many(AggregateDataPtr __restrict place, const IColumn** columns,
                  std::vector<int>& rows, Arena* arena) const override {
        for (auto row : rows) {
            add(place, columns, row, arena);
        }
    }

    void add_range(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t min,
                   ssize_t max, Arena* arena) const {
        for (ssize_t row_num = min; row_num < max; ++row_num) {
            add(place, columns, row_num, arena);
        }
    }

    void reset(AggregateDataPtr __restrict place) const override {
        this->data(place).value.clear();
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        auto& rhs_set = this->data(rhs).value;
        if (!rhs_set.size()) {
            return;
        }

        auto& set = this->data(place).value;
        if (set.capacity() != reserved) {
            set.resize(reserved);
        }
        set.merge(rhs_set);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& data_to = assert_cast<ColumnString&, TypeCheckOnRelease::DISABLE>(to);

        const typename State::Set& set = this->data(place).value;
        auto result_vec = set.top_k(threshold);

        rapidjson::StringBuffer buffer;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        writer.StartObject();
        for (auto& result : result_vec) {
            auto argument_types = this->get_argument_types();
            MutableColumns argument_columns(column_size);
            for (size_t i = 0; i < column_size; ++i) {
                argument_columns[i] = argument_types[i]->create_column();
            }

            std::string row_str;
            const char* begin = result.key.data;
            for (size_t i = 0; i < argument_columns.size(); i++) {
                begin = argument_columns[i]->deserialize_and_insert_from_arena(begin);
                row_str += argument_types[i]->to_string(*argument_columns[i], 0);
                row_str += "_";
            }
            row_str.pop_back();

            writer.Key(row_str.data(), row_str.size());
            writer.Uint64(result.count);
        }
        writer.EndObject();

        std::string res = buffer.GetString();
        data_to.insert_data(res.data(), res.size());
    }

private:
    void lazy_init(const IColumn** columns, ssize_t row_num) const {
        auto get_param = [](size_t idx, const DataTypes& data_types,
                            const IColumn** columns) -> uint64_t {
            const auto& data_type = data_types.at(idx);
            const IColumn* column = columns[idx];

            const auto* type = data_type.get();
            if (type->is_nullable()) {
                type = assert_cast<const DataTypeNullable*, TypeCheckOnRelease::DISABLE>(type)
                               ->get_nested_type()
                               .get();
            }
            WhichDataType which(type);
            if (which.idx == TypeIndex::Int8) {
                return assert_cast<const ColumnInt8*, TypeCheckOnRelease::DISABLE>(column)
                        ->get_element(0);
            } else if (which.idx == TypeIndex::Int16) {
                return assert_cast<const ColumnInt16*, TypeCheckOnRelease::DISABLE>(column)
                        ->get_element(0);
            } else if (which.idx == TypeIndex::Int32) {
                return assert_cast<const ColumnInt32*, TypeCheckOnRelease::DISABLE>(column)
                        ->get_element(0);
            }
            return 0;
        };

        const auto& data_types = this->get_argument_types();
        threshold = std::min(get_param(column_size, data_types, columns), (uint64_t)1000);
        reserved = std::min(std::max(get_param(column_size + 1, data_types, columns), threshold),
                            (uint64_t)1000);

        if (threshold == 0 || reserved == 0 || threshold > 1000 || reserved > 1000) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "multi_topn param error, threshold: {}, reserved: {}", threshold,
                            reserved);
        }

        init_flag = true;
    }

    size_t column_size = 0;
    mutable bool init_flag = false;
    mutable uint64_t threshold = 0;
    mutable uint64_t reserved = 0;
};

} // namespace doris::vectorized