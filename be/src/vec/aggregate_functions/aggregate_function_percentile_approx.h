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

#include "util/counts.h"
#include "util/tdigest.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

struct PercentileApproxState {
    static constexpr double INIT_QUANTILE = -1.0;
    PercentileApproxState() = default;
    ~PercentileApproxState() = default;

    void init(double compression = 10000) {
        if (!init_flag) {
            //https://doris.apache.org/zh-CN/sql-reference/sql-functions/aggregate-functions/percentile_approx.html#description
            //The compression parameter setting range is [2048, 10000].
            //If the value of compression parameter is not specified set, or is outside the range of [2048, 10000],
            //will use the default value of 10000
            if (compression < 2048 || compression > 10000) {
                compression = 10000;
            }
            digest.reset(new TDigest(compression));
            compressions = compression;
            init_flag = true;
        }
    }

    void write(BufferWritable& buf) const {
        write_binary(init_flag, buf);
        if (!init_flag) {
            return;
        }

        write_binary(target_quantile, buf);
        write_binary(compressions, buf);
        uint32_t serialize_size = digest->serialized_size();
        std::string result(serialize_size, '0');
        DCHECK(digest.get() != nullptr);
        digest->serialize((uint8_t*)result.c_str());

        write_binary(result, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(init_flag, buf);
        if (!init_flag) {
            return;
        }

        read_binary(target_quantile, buf);
        read_binary(compressions, buf);
        std::string str;
        read_binary(str, buf);
        digest.reset(new TDigest(compressions));
        digest->unserialize((uint8_t*)str.c_str());
    }

    double get() const {
        if (init_flag) {
            return digest->quantile(target_quantile);
        } else {
            return std::nan("");
        }
    }

    void merge(const PercentileApproxState& rhs) {
        if (!rhs.init_flag) {
            return;
        }
        if (init_flag) {
            DCHECK(digest.get() != nullptr);
            digest->merge(rhs.digest.get());
        } else {
            digest.reset(new TDigest(compressions));
            digest->merge(rhs.digest.get());
            init_flag = true;
        }
        if (target_quantile == PercentileApproxState::INIT_QUANTILE) {
            target_quantile = rhs.target_quantile;
        }
    }

    void add(double source, double quantile) {
        digest->add(source);
        target_quantile = quantile;
    }

    void reset() {
        target_quantile = INIT_QUANTILE;
        init_flag = false;
        digest.reset(new TDigest(compressions));
    }

    bool init_flag = false;
    std::unique_ptr<TDigest> digest = nullptr;
    double target_quantile = INIT_QUANTILE;
    double compressions = 10000;
};

class AggregateFunctionPercentileApprox
        : public IAggregateFunctionDataHelper<PercentileApproxState,
                                              AggregateFunctionPercentileApprox> {
public:
    AggregateFunctionPercentileApprox(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<PercentileApproxState,
                                           AggregateFunctionPercentileApprox>(argument_types_, {}) {
    }

    String get_name() const override { return "percentile_approx"; }

    DataTypePtr get_return_type() const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    void reset(AggregateDataPtr __restrict place) const override {
        AggregateFunctionPercentileApprox::data(place).reset();
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        AggregateFunctionPercentileApprox::data(place).merge(
                AggregateFunctionPercentileApprox::data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        AggregateFunctionPercentileApprox::data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        AggregateFunctionPercentileApprox::data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        ColumnNullable& nullable_column = assert_cast<ColumnNullable&>(to);
        double result = AggregateFunctionPercentileApprox::data(place).get();

        if (std::isnan(result)) {
            nullable_column.insert_default();
        } else {
            auto& col = static_cast<ColumnVector<Float64>&>(nullable_column.get_nested_column());
            col.get_data().push_back(result);
            nullable_column.get_null_map_data().push_back(0);
        }
    }
};

// only for merge
template <bool is_nullable>
class AggregateFunctionPercentileApproxMerge : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxMerge(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}
    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        LOG(FATAL) << "AggregateFunctionPercentileApproxMerge do not support add()";
    }
};

template <bool is_nullable>
class AggregateFunctionPercentileApproxTwoParams : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxTwoParams(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}
    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        if constexpr (is_nullable) {
            double column_data[2] = {0, 0};

            for (int i = 0; i < 2; ++i) {
                const auto* nullable_column = check_and_get_column<ColumnNullable>(columns[i]);
                if (nullable_column == nullptr) { //Not Nullable column
                    const auto& column = static_cast<const ColumnVector<Float64>&>(*columns[i]);
                    column_data[i] = column.get_float64(row_num);
                } else if (!nullable_column->is_null_at(
                                   row_num)) { // Nullable column && Not null data
                    const auto& column = static_cast<const ColumnVector<Float64>&>(
                            nullable_column->get_nested_column());
                    column_data[i] = column.get_float64(row_num);
                } else { // Nullable column && null data
                    if (i == 0) {
                        return;
                    }
                }
            }

            this->data(place).init();
            this->data(place).add(column_data[0], column_data[1]);

        } else {
            const auto& sources = static_cast<const ColumnVector<Float64>&>(*columns[0]);
            const auto& quantile = static_cast<const ColumnVector<Float64>&>(*columns[1]);

            this->data(place).init();
            this->data(place).add(sources.get_float64(row_num), quantile.get_float64(row_num));
        }
    }
};

template <bool is_nullable>
class AggregateFunctionPercentileApproxThreeParams : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxThreeParams(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}
    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        if constexpr (is_nullable) {
            double column_data[3] = {0, 0, 0};

            for (int i = 0; i < 3; ++i) {
                const auto* nullable_column = check_and_get_column<ColumnNullable>(columns[i]);
                if (nullable_column == nullptr) { //Not Nullable column
                    const auto& column = static_cast<const ColumnVector<Float64>&>(*columns[i]);
                    column_data[i] = column.get_float64(row_num);
                } else if (!nullable_column->is_null_at(
                                   row_num)) { // Nullable column && Not null data
                    const auto& column = static_cast<const ColumnVector<Float64>&>(
                            nullable_column->get_nested_column());
                    column_data[i] = column.get_float64(row_num);
                } else { // Nullable column && null data
                    if (i == 0) {
                        return;
                    }
                }
            }

            this->data(place).init(column_data[2]);
            this->data(place).add(column_data[0], column_data[1]);

        } else {
            const auto& sources = static_cast<const ColumnVector<Float64>&>(*columns[0]);
            const auto& quantile = static_cast<const ColumnVector<Float64>&>(*columns[1]);
            const auto& compression = static_cast<const ColumnVector<Float64>&>(*columns[2]);

            this->data(place).init(compression.get_float64(row_num));
            this->data(place).add(sources.get_float64(row_num), quantile.get_float64(row_num));
        }
    }
};

struct PercentileState {
    std::vector<Counts> vec_counts;
    std::vector<double> vec_quantile {-1};
    bool inited_flag = false;

    void write(BufferWritable& buf) const {
        write_binary(inited_flag, buf);
        int size_num = vec_quantile.size();
        write_binary(size_num, buf);
        for (const auto& quantile : vec_quantile) {
            write_binary(quantile, buf);
        }
        std::string serialize_str;
        for (const auto& counts : vec_counts) {
            serialize_str.resize(counts.serialized_size(), '0');
            counts.serialize((uint8_t*)serialize_str.c_str());
            write_binary(serialize_str, buf);
        }
    }

    void read(BufferReadable& buf) {
        read_binary(inited_flag, buf);
        int size_num = 0;
        read_binary(size_num, buf);
        double data = 0.0;
        vec_quantile.clear();
        for (int i = 0; i < size_num; ++i) {
            read_binary(data, buf);
            vec_quantile.emplace_back(data);
        }
        StringRef ref;
        vec_counts.clear();
        vec_counts.resize(size_num);
        for (int i = 0; i < size_num; ++i) {
            read_binary(ref, buf);
            vec_counts[i].unserialize((uint8_t*)ref.data);
        }
    }

    void add(int64_t source, const PaddedPODArray<Float64>& quantiles, int arg_size) {
        if (!inited_flag) {
            vec_counts.resize(arg_size);
            vec_quantile.resize(arg_size, -1);
            inited_flag = true;
            for (int i = 0; i < arg_size; ++i) {
                vec_quantile[i] = quantiles[i];
            }
        }
        for (int i = 0; i < arg_size; ++i) {
            vec_counts[i].increment(source, 1);
        }
    }

    void merge(const PercentileState& rhs) {
        if (!rhs.inited_flag) {
            return;
        }
        int size_num = rhs.vec_quantile.size();
        if (!inited_flag) {
            vec_counts.resize(size_num);
            vec_quantile.resize(size_num, -1);
            inited_flag = true;
        }

        for (int i = 0; i < size_num; ++i) {
            if (vec_quantile[i] == -1.0) {
                vec_quantile[i] = rhs.vec_quantile[i];
            }
            vec_counts[i].merge(&(rhs.vec_counts[i]));
        }
    }

    void reset() {
        vec_counts.clear();
        vec_quantile.clear();
        inited_flag = false;
    }

    double get() const {
        auto result = vec_counts[0].terminate(vec_quantile[0]); //DoubleVal
        return result.val;
    }

    void insert_result_into(IColumn& to) const {
        auto& column_data = static_cast<ColumnVector<Float64>&>(to).get_data();
        for (int i = 0; i < vec_counts.size(); ++i) {
            column_data.push_back(vec_counts[i].terminate(vec_quantile[i]).val);
        }
    }
};

class AggregateFunctionPercentile final
        : public IAggregateFunctionDataHelper<PercentileState, AggregateFunctionPercentile> {
public:
    AggregateFunctionPercentile(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<PercentileState, AggregateFunctionPercentile>(
                      argument_types_, {}) {}

    String get_name() const override { return "percentile"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& sources = static_cast<const ColumnVector<Int64>&>(*columns[0]);
        const auto& quantile = static_cast<const ColumnVector<Float64>&>(*columns[1]);
        AggregateFunctionPercentile::data(place).add(sources.get_int(row_num), quantile.get_data(),
                                                     1);
    }

    void reset(AggregateDataPtr __restrict place) const override {
        AggregateFunctionPercentile::data(place).reset();
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        AggregateFunctionPercentile::data(place).merge(AggregateFunctionPercentile::data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        AggregateFunctionPercentile::data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        AggregateFunctionPercentile::data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& col = assert_cast<ColumnVector<Float64>&>(to);
        col.insert_value(AggregateFunctionPercentile::data(place).get());
    }
};

class AggregateFunctionPercentileArray final
        : public IAggregateFunctionDataHelper<PercentileState, AggregateFunctionPercentileArray> {
public:
    AggregateFunctionPercentileArray(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<PercentileState, AggregateFunctionPercentileArray>(
                      argument_types_, {}) {}

    String get_name() const override { return "percentile_array"; }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat64>()));
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& sources = static_cast<const ColumnVector<Int64>&>(*columns[0]);
        const auto& quantile_array = static_cast<const ColumnArray&>(*columns[1]);
        const auto& offset_column_data = quantile_array.get_offsets();
        const auto& nested_column =
                static_cast<const ColumnNullable&>(quantile_array.get_data()).get_nested_column();
        const auto& nested_column_data = static_cast<const ColumnVector<Float64>&>(nested_column);

        AggregateFunctionPercentileArray::data(place).add(
                sources.get_int(row_num), nested_column_data.get_data(),
                offset_column_data.data()[row_num] - offset_column_data.data()[row_num - 1]);
    }

    void reset(AggregateDataPtr __restrict place) const override {
        AggregateFunctionPercentileArray::data(place).reset();
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        AggregateFunctionPercentileArray::data(place).merge(
                AggregateFunctionPercentileArray::data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        AggregateFunctionPercentileArray::data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        AggregateFunctionPercentileArray::data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        if (to_nested_col.is_nullable()) {
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            AggregateFunctionPercentileArray::data(place).insert_result_into(
                    col_null->get_nested_column());
            col_null->get_null_map_data().resize_fill(col_null->get_nested_column().size(), 0);
        } else {
            AggregateFunctionPercentileArray::data(place).insert_result_into(to_nested_col);
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }
};

} // namespace doris::vectorized