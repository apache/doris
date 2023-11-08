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

#include <fmt/format.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>
#include <stddef.h>

#include <memory>
#include <new>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "runtime/runtime_state.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/sort_block.h"
#include "vec/core/sort_description.h"
#include "vec/core/types.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

struct AggregateFunctionSortData {
    const SortDescription sort_desc;
    Block block;

    // The construct only support the template compiler, useless
    AggregateFunctionSortData() : sort_desc() {};
    AggregateFunctionSortData(SortDescription sort_desc, const Block& block)
            : sort_desc(std::move(sort_desc)), block(block.clone_empty()) {}

    void merge(const AggregateFunctionSortData& rhs) {
        if (block.rows() == 0) {
            block = rhs.block;
        } else {
            for (size_t i = 0; i < block.columns(); i++) {
                auto column = block.get_by_position(i).column->assume_mutable();
                auto column_rhs = rhs.block.get_by_position(i).column;
                column->insert_range_from(*column_rhs, 0, rhs.block.rows());
            }
        }
    }

    void serialize(const RuntimeState* state, BufferWritable& buf) const {
        PBlock pblock;
        size_t uncompressed_bytes = 0;
        size_t compressed_bytes = 0;
        static_cast<void>(block.serialize(state->be_exec_version(), &pblock, &uncompressed_bytes,
                                          &compressed_bytes,
                                          segment_v2::CompressionTypePB::SNAPPY));

        write_string_binary(pblock.SerializeAsString(), buf);
    }

    void deserialize(BufferReadable& buf) {
        std::string data;
        read_binary(data, buf);

        PBlock pblock;
        pblock.ParseFromString(data);
        auto st = block.deserialize(pblock);
        CHECK(st.ok());
    }

    void add(const IColumn** columns, size_t columns_num, size_t row_num) {
        DCHECK(block.columns() == columns_num)
                << fmt::format("block.columns()!=columns_num, block.columns()={}, columns_num={}",
                               block.columns(), columns_num);

        for (size_t i = 0; i < columns_num; ++i) {
            auto column = block.get_by_position(i).column->assume_mutable();
            column->insert_from(*columns[i], row_num);
        }
    }

    void sort() { sort_block(block, block, sort_desc, block.rows()); }
};

template <typename Data>
class AggregateFunctionSort
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionSort<Data>> {
private:
    static constexpr auto prefix_size = sizeof(Data);
    AggregateFunctionPtr _nested_func;
    DataTypes _arguments;
    const SortDescription& _sort_desc;
    Block _block;
    const RuntimeState* _state;

    AggregateDataPtr get_nested_place(AggregateDataPtr __restrict place) const noexcept {
        return place + prefix_size;
    }

    ConstAggregateDataPtr get_nested_place(ConstAggregateDataPtr __restrict place) const noexcept {
        return place + prefix_size;
    }

public:
    AggregateFunctionSort(const AggregateFunctionPtr& nested_func, const DataTypes& arguments,
                          const SortDescription& sort_desc, const RuntimeState* state)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionSort>(arguments),
              _nested_func(nested_func),
              _arguments(arguments),
              _sort_desc(sort_desc),
              _state(state) {
        for (const auto& type : _arguments) {
            _block.insert({type, ""});
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        this->data(place).add(columns, _arguments.size(), row_num);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).serialize(_state, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        this->data(place).deserialize(buf);
    }

    void insert_result_into(ConstAggregateDataPtr targetplace, IColumn& to) const override {
        auto place = const_cast<AggregateDataPtr>(targetplace);
        if (!this->data(place).block.is_empty_column()) {
            this->data(place).sort();

            ColumnRawPtrs arguments_nested;
            for (int i = 0; i < _arguments.size() - _sort_desc.size(); i++) {
                arguments_nested.emplace_back(
                        this->data(place).block.get_by_position(i).column.get());
            }
            _nested_func->add_batch_single_place(arguments_nested[0]->size(),
                                                 get_nested_place(place), arguments_nested.data(),
                                                 nullptr);
        }

        _nested_func->insert_result_into(get_nested_place(place), to);
    }

    size_t size_of_data() const override { return prefix_size + _nested_func->size_of_data(); }

    size_t align_of_data() const override { return _nested_func->align_of_data(); }

    void create(AggregateDataPtr __restrict place) const override {
        new (place) Data(_sort_desc, _block);
        SAFE_CREATE(_nested_func->create(get_nested_place(place)), this->data(place).~Data());
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        this->data(place).~Data();
        _nested_func->destroy(get_nested_place(place));
    }

    String get_name() const override { return _nested_func->get_name() + "Sort"; }

    DataTypePtr get_return_type() const override { return _nested_func->get_return_type(); }
};

AggregateFunctionPtr transform_to_sort_agg_function(const AggregateFunctionPtr& nested_function,
                                                    const DataTypes& arguments,
                                                    const SortDescription& sort_desc,
                                                    RuntimeState* state);
} // namespace doris::vectorized
