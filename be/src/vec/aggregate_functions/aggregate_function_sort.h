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

#include <string>
#include <utility>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/key_holder_helpers.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/common/aggregation_common.h"
#include "vec/common/assert_cast.h"
#include "vec/common/field_visitors.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/common/sip_hash.h"
#include "vec/core/sort_block.h"
#include "vec/core/sort_description.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

struct AggregateFunctionSortData {
    const SortDescription _sort_desc;
    Block _block;

    // The construct only support the template compiler, useless
    AggregateFunctionSortData() {};
    AggregateFunctionSortData(SortDescription sort_desc, const Block& block)
            : _sort_desc(std::move(sort_desc)), _block(block.clone_empty()) {}

    void merge(const AggregateFunctionSortData& rhs) {
        if (_block.rows() == 0) {
            _block = rhs._block;
        } else {
            for (size_t i = 0; i < _block.columns(); i++) {
                auto column = _block.get_by_position(i).column->assume_mutable();
                auto column_rhs = rhs._block.get_by_position(i).column;
                column->insert_many_from(*column_rhs, 0, rhs._block.rows());
            }
        }
    }

    void serialize(BufferWritable& buf) const {
        PBlock pblock;
        size_t uncompressed_bytes = 0;
        size_t compressed_bytes = 0;
        _block.serialize(&pblock, &uncompressed_bytes, &compressed_bytes);

        write_string_binary(pblock.SerializeAsString(), buf);
    }

    void deserialize(BufferReadable& buf) {
        std::string data;
        read_binary(data, buf);

        PBlock pblock;
        pblock.ParseFromString(data);
        new (&_block) Block(pblock);
    }

    void add(const IColumn** columns, size_t columns_num, size_t row_num) {
        DCHECK(_block.columns() == columns_num)
                << fmt::format("block.columns()!=columns_num, block.columns()={}, columns_num={}",
                               _block.columns(), columns_num);

        for (size_t i = 0; i < columns_num; ++i) {
            auto column = _block.get_by_position(i).column->assume_mutable();
            column->insert_from(*columns[i], row_num);
        }
    }

    void sort() { sort_block(_block, _sort_desc, _block.rows()); }
};

template <typename Data>
class AggregateFunctionSort
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionSort<Data>> {
    using DataReal = Data;

private:
    static constexpr auto prefix_size = sizeof(DataReal);
    AggregateFunctionPtr _nested_func;
    DataTypes _arguments;
    const SortDescription& _sort_desc;
    Block _block;

    AggregateDataPtr get_nested_place(AggregateDataPtr __restrict place) const noexcept {
        return place + prefix_size;
    }

    ConstAggregateDataPtr get_nested_place(ConstAggregateDataPtr __restrict place) const noexcept {
        return place + prefix_size;
    }

public:
    AggregateFunctionSort(const AggregateFunctionPtr& nested_func, const DataTypes& arguments,
                          const SortDescription& sort_desc)
            : IAggregateFunctionDataHelper<DataReal, AggregateFunctionSort>(
                      arguments, nested_func->get_parameters()),
              _nested_func(nested_func),
              _arguments(arguments),
              _sort_desc(sort_desc) {
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
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        this->data(place).deserialize(buf);
    }

    void insert_result_into(ConstAggregateDataPtr targetplace, IColumn& to) const override {
        auto place = const_cast<AggregateDataPtr>(targetplace);
        if (!this->data(place)._block.is_empty_column()) {
            this->data(place).sort();

            ColumnRawPtrs arguments_nested;
            for (int i = 0; i < _arguments.size() - _sort_desc.size(); i++) {
                arguments_nested.emplace_back(
                        this->data(place)._block.get_by_position(i).column.get());
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
        new (place) DataReal(_sort_desc, _block);
        _nested_func->create(get_nested_place(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        this->data(place).~DataReal();
        _nested_func->destroy(get_nested_place(place));
    }

    String get_name() const override { return _nested_func->get_name() + "Sort"; }

    DataTypePtr get_return_type() const override { return _nested_func->get_return_type(); }
};

AggregateFunctionPtr transform_to_sort_agg_function(const AggregateFunctionPtr& nested_function,
                                                    const DataTypes& arguments,
                                                    const SortDescription& sort_desc);
} // namespace doris::vectorized
