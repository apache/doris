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
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cstdint>
#include <string>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_approx_top.h"
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

struct AggregateFunctionTopKGenericData {
    using Set = SpaceSaving<StringRef, StringRefHash>;

    Set value;
};

class AggregateFunctionApproxTopK final
        : public IAggregateFunctionDataHelper<AggregateFunctionTopKGenericData,
                                              AggregateFunctionApproxTopK>,
          AggregateFunctionApproxTop {
private:
    using State = AggregateFunctionTopKGenericData;

public:
    AggregateFunctionApproxTopK(const std::vector<std::string>& column_names,
                                const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<AggregateFunctionTopKGenericData,
                                           AggregateFunctionApproxTopK>(argument_types_),
              AggregateFunctionApproxTop(column_names) {}

    String get_name() const override { return "approx_top_k"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    // Serializes the aggregate function's state (including the SpaceSaving structure and threshold) into a buffer.
    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).value.write(buf);

        write_var_uint(_column_names.size(), buf);
        for (const auto& column_name : _column_names) {
            write_string_binary(column_name, buf);
        }
        write_var_uint(_threshold, buf);
        write_var_uint(_reserved, buf);
    }

    // Deserializes the aggregate function's state from a buffer (including the SpaceSaving structure and threshold).
    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        auto readStringBinaryInto = [](Arena& arena, BufferReadable& buf) {
            uint64_t size = 0;
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

        uint64_t size = 0;
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

        uint64_t column_size = 0;
        read_var_uint(column_size, buf);
        _column_names.clear();
        for (uint64_t i = 0; i < column_size; i++) {
            std::string column_name;
            read_string_binary(column_name, buf);
            _column_names.emplace_back(std::move(column_name));
        }
        read_var_uint(_threshold, buf);
        read_var_uint(_reserved, buf);
    }

    // Adds a new row of data to the aggregate function (inserts a new value into the SpaceSaving structure).
    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena* arena) const override {
        if (!_init_flag) {
            lazy_init(columns, row_num, this->get_argument_types());
        }

        auto& set = this->data(place).value;
        if (set.capacity() != _reserved) {
            set.resize(_reserved);
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
                all_serialize_value_into_arena(row_num, _column_names.size(), columns, arena);
        set.insert(str_serialized);
        arena->rollback(str_serialized.size);
    }

    void add_many(AggregateDataPtr __restrict place, const IColumn** columns,
                  std::vector<int>& rows, Arena* arena) const override {
        for (auto row : rows) {
            add(place, columns, row, arena);
        }
    }

    void reset(AggregateDataPtr __restrict place) const override {
        this->data(place).value.clear();
    }

    // Merges the state of another aggregate function into the current one (merges two SpaceSaving sets).
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        auto& rhs_set = this->data(rhs).value;
        if (!rhs_set.size()) {
            return;
        }

        auto& set = this->data(place).value;
        if (set.capacity() != _reserved) {
            set.resize(_reserved);
        }
        set.merge(rhs_set);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& data_to = assert_cast<ColumnString&, TypeCheckOnRelease::DISABLE>(to);

        const typename State::Set& set = this->data(place).value;
        auto result_vec = set.top_k(_threshold);

        rapidjson::StringBuffer buffer;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        writer.StartArray();
        for (auto& result : result_vec) {
            auto argument_types = this->get_argument_types();
            MutableColumns argument_columns(_column_names.size());
            for (size_t i = 0; i < _column_names.size(); ++i) {
                argument_columns[i] = argument_types[i]->create_column();
            }
            rapidjson::StringBuffer sub_buffer;
            rapidjson::Writer<rapidjson::StringBuffer> sub_writer(sub_buffer);
            sub_writer.StartObject();
            const char* begin = result.key.data;
            for (size_t i = 0; i < _column_names.size(); i++) {
                begin = argument_columns[i]->deserialize_and_insert_from_arena(begin);
                std::string row_str = argument_types[i]->to_string(*argument_columns[i], 0);
                sub_writer.Key(_column_names[i].data(), _column_names[i].size());
                sub_writer.String(row_str.data(), row_str.size());
            }
            sub_writer.Key("count");
            sub_writer.String(std::to_string(result.count).c_str());
            sub_writer.EndObject();
            writer.RawValue(sub_buffer.GetString(), sub_buffer.GetSize(), rapidjson::kObjectType);
        }
        writer.EndArray();
        std::string res = buffer.GetString();
        data_to.insert_data(res.data(), res.size());
    }
};

} // namespace doris::vectorized