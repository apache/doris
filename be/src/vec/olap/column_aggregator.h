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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column.h"

namespace doris::vectorized {

class ColumnAggregator {
public:
    ColumnAggregator() : _source_column(nullptr) {}

    virtual ~ColumnAggregator() = default;

    // update input column
    virtual void update_source(const ColumnPtr& col) { _source_column = col; }

    // update output aggregate column
    virtual void update_aggregate(IColumn* target) { _agg_column = target; }

    virtual void aggregate_keys(int size, const uint32* agg_index) {}

    virtual void aggregate_values(int start, int size, const uint32* agg_range, bool previous_neq) {
    }

    virtual void finalize() { _agg_column = nullptr; }

public:
    ColumnPtr _source_column;
    IColumn* _agg_column;
};

class KeyColumnAggregator final : public ColumnAggregator {
    void aggregate_keys(int size, const uint32* agg_index) override {
        auto origin_size = _agg_column->size();
        _agg_column->reserve(origin_size + size);
        for (int i = 0; i < size; ++i) {
            _agg_column->insert_from(*_source_column, agg_index[i]);
        }
    }
};

class ValueColumnAggregator : public ColumnAggregator {
public:
    ValueColumnAggregator(const TabletColumn& col, const DataTypePtr data_type) {
        // create aggregate function
        FieldAggregationMethod agg_method = col.aggregation();
        std::string agg_name =
                TabletColumn::get_string_by_aggregation_type(agg_method) + AGG_READER_SUFFIX;
        std::transform(agg_name.begin(), agg_name.end(), agg_name.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        Array params;
        DataTypes argument_types;
        argument_types.push_back(data_type);
        _agg_function = AggregateFunctionSimpleFactory::instance().get(
                agg_name, argument_types, params, data_type->is_nullable());
        DCHECK(_agg_function != nullptr);

        // create aggregate data
        _agg_place = new char[_agg_function->size_of_data()];
        _agg_function->create(_agg_place);
    }

    void update_aggregate(IColumn* col) override {
        _agg_column = col;
        reset();
    }

    void aggregate_values(int start, int size, const uint32* agg_range,
                          bool neq_previous) override {
        if (size <= 0) {
            return;
        }

        // not equal to the last row of the previous block
        if (neq_previous) {
            append_data(_agg_column);
            reset();
        }

        for (int i = 0; i < size - 1; ++i) {
            aggregate_batch_impl(start, start + agg_range[i] - 1, _source_column);
            append_data(_agg_column);

            start += agg_range[i];
            reset();
        }

        // last row just aggregate, not append
        aggregate_batch_impl(start, start + agg_range[size - 1] - 1, _source_column);
    }

    void finalize() override {
        append_data(_agg_column);
        _agg_column = nullptr;
    }

    void reset() {
        _agg_function->destroy(_agg_place);
        _agg_function->create(_agg_place);
    }

    void append_data(IColumn* col) { _agg_function->insert_result_into(_agg_place, *col); }

    void aggregate_batch_impl(int begin, int end, const ColumnPtr& src) {
        auto column_ptr = src.get();
        if (begin <= end) {
            // todo(zeno) add has_null hint
            _agg_function->add_batch_range(begin, end, _agg_place,
                                           const_cast<const IColumn**>(&column_ptr), nullptr);
        }
    }

private:
    AggregateFunctionPtr _agg_function;
    AggregateDataPtr _agg_place;
};

using ColumnAggregatorPtr = std::unique_ptr<ColumnAggregator>;

} // namespace doris::vectorized