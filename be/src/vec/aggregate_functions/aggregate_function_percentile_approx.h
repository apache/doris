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

#include "common/status.h"
#include "util/counts.h"
#include "util/tdigest.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

struct PercentileApproxState {
    static constexpr double INIT_QUANTILE = -1.0;
    PercentileApproxState() = default;
    ~PercentileApproxState() = default;

    void init(double compression = 10000) {
        if (!init_flag) {
            digest.reset(new TDigest(compression));
            init_flag = true;
        }
    }

    void write(BufferWritable& buf) const {
        write_binary(init_flag, buf);
        write_binary(target_quantile, buf);

        uint32_t serialize_size = digest->serialized_size();
        std::string result(serialize_size, '0');
        DCHECK(digest.get() != nullptr);
        digest->serialize((uint8_t*)result.c_str());

        write_binary(result, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(init_flag, buf);
        read_binary(target_quantile, buf);

        std::string str;
        read_binary(str, buf);
        digest.reset(new TDigest());
        digest->unserialize((uint8_t*)str.c_str());
    }

    double get() const { return digest->quantile(target_quantile); }

    void merge(const PercentileApproxState& rhs) {
        if (init_flag) {
            DCHECK(digest.get() != nullptr);
            digest->merge(rhs.digest.get());
        } else {
            digest.reset(new TDigest());
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
        digest.reset();
    }

    bool init_flag = false;
    std::unique_ptr<TDigest> digest;
    double target_quantile = INIT_QUANTILE;
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

    bool insert_to_null_default() const override { return false; }

    DataTypePtr get_return_type() const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(AggregateDataPtr __restrict place, IColumn& to) const override {
        ColumnNullable& nullable_column = assert_cast<ColumnNullable&>(to);
        double result = this->data(place).get();

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
class AggregateFunctionPercentileApproxMerge : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxMerge(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}
    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        LOG(FATAL) << "AggregateFunctionPercentileApproxMerge do not support add()";
    }
};

class AggregateFunctionPercentileApproxTwoParams : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxTwoParams(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}
    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& sources = static_cast<const ColumnVector<Float64>&>(*columns[0]);
        const auto& quantile = static_cast<const ColumnVector<Float64>&>(*columns[1]);

        this->data(place).init();
        this->data(place).add(sources.get_float64(row_num), quantile.get_float64(row_num));
    }
};

class AggregateFunctionPercentileApproxThreeParams : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxThreeParams(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}
    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& sources = static_cast<const ColumnVector<Float64>&>(*columns[0]);
        const auto& quantile = static_cast<const ColumnVector<Float64>&>(*columns[1]);
        const auto& compression = static_cast<const ColumnVector<Float64>&>(*columns[2]);

        this->data(place).init(compression.get_float64(row_num));
        this->data(place).add(sources.get_float64(row_num), quantile.get_float64(row_num));
    }
};

struct PercentileState {
    Counts counts;
    double quantile = -1.0;

    void write(BufferWritable& buf) const {
        uint32_t serialize_size = counts.serialized_size();
        std::string result(serialize_size + sizeof(double), '0');
        memcpy(result.data(), &quantile, sizeof(double));
        counts.serialize((uint8_t*)result.c_str() + sizeof(double));
        write_binary(result, buf);
    }

    void read(BufferReadable& buf) {
        StringRef ref;
        read_binary(ref, buf);
        memcpy(&quantile, ref.data, sizeof(double));
        counts.unserialize((uint8_t*)ref.data + sizeof(double));
    }

    void add(int64_t source, double quantiles) {
        counts.increment(source, 1);
        quantile = quantiles;
    }

    void merge(const PercentileState& rhs) {
        counts.merge(&(rhs.counts));
        if (quantile == -1.0) {
            quantile = rhs.quantile;
        }
    }

    void reset() {
        counts = {};
        quantile = -1.0;
    }

    double get() const {
        auto result = counts.terminate(quantile); //DoubleVal
        return result.val;
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

        this->data(place).add(sources.get_int(row_num), quantile.get_float64(row_num));
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(AggregateDataPtr __restrict place, IColumn& to) const override {
        auto& col = assert_cast<ColumnVector<Float64>&>(to);
        col.insert_value(this->data(place).get());
    }
};

} // namespace doris::vectorized
