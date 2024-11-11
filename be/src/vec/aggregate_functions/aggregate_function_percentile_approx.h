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

#include <glog/logging.h>

#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <cstdint>
#include <memory>
#include <string>

#include "util/tdigest.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
struct PercentileApproxState {
    // Since TDigest internally performs calculations using float32, but the function definitions use double,
    // there is an additional type conversion overhead. To ensure compatibility with both up- and down-casting,
    // the original code is appended with the suffix "Old". The BE will support both float32 and float64 implementations.
    // To ensure the result remains unchanged, the return value is still float64.
    static constexpr Value INIT_QUANTILE = -1.0;
    PercentileApproxState() = default;
    ~PercentileApproxState() = default;

    void init(Value compression = 10000) {
        if (!init_flag) {
            //https://doris.apache.org/zh-CN/sql-reference/sql-functions/aggregate-functions/percentile_approx.html#description
            //The compression parameter setting range is [2048, 10000].
            //If the value of compression parameter is not specified set, or is outside the range of [2048, 10000],
            //will use the default value of 10000
            if (compression < 2048 || compression > 10000) {
                compression = 10000;
            }
            digest = TDigest::create_unique(compression);
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
        digest = TDigest::create_unique(compressions);
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
            digest = TDigest::create_unique(compressions);
            digest->merge(rhs.digest.get());
            init_flag = true;
        }
        if (target_quantile == PercentileApproxState::INIT_QUANTILE) {
            target_quantile = rhs.target_quantile;
        }
    }

    void add(Value source, Value quantile) {
        digest->add(source);
        target_quantile = quantile;
    }

    void add_with_weight(Value source, Value weight, Value quantile) {
        // the weight should be positive num, as have check the value valid use DCHECK_GT(c._weight, 0);
        if (weight <= 0) {
            return;
        }
        digest->add(source, weight);
        target_quantile = quantile;
    }

    void reset() {
        target_quantile = INIT_QUANTILE;
        init_flag = false;
        digest = TDigest::create_unique(compressions);
    }

    bool init_flag = false;
    std::unique_ptr<TDigest> digest;
    Value target_quantile = INIT_QUANTILE;
    Value compressions = 10000;
};

class AggregateFunctionPercentileApprox
        : public IAggregateFunctionDataHelper<PercentileApproxState,
                                              AggregateFunctionPercentileApprox> {
public:
    AggregateFunctionPercentileApprox(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<PercentileApproxState,
                                           AggregateFunctionPercentileApprox>(argument_types_) {}

    String get_name() const override { return "percentile_approx"; }

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
};

class AggregateFunctionPercentileApproxTwoParams : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxTwoParams(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}
    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        const auto& sources =
                assert_cast<const ColumnFloat32&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& quantile =
                assert_cast<const ColumnFloat32&, TypeCheckOnRelease::DISABLE>(*columns[1]);
        this->data(place).init();
        this->data(place).add(sources.get_element(row_num), quantile.get_element(row_num));
    }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& col = assert_cast<ColumnFloat64&>(to);
        double result = AggregateFunctionPercentileApprox::data(place).get();

        if (std::isnan(result)) {
            col.insert_default();
        } else {
            col.get_data().push_back(result);
        }
    }
};

class AggregateFunctionPercentileApproxThreeParams : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxThreeParams(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}
    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        const auto& sources =
                assert_cast<const ColumnFloat32&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& quantile =
                assert_cast<const ColumnFloat32&, TypeCheckOnRelease::DISABLE>(*columns[1]);
        const auto& compression =
                assert_cast<const ColumnFloat32&, TypeCheckOnRelease::DISABLE>(*columns[2]);

        this->data(place).init(compression.get_element(row_num));
        this->data(place).add(sources.get_element(row_num), quantile.get_element(row_num));
    }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& col = assert_cast<ColumnFloat64&>(to);
        double result = AggregateFunctionPercentileApprox::data(place).get();

        if (std::isnan(result)) {
            col.insert_default();
        } else {
            col.get_data().push_back(result);
        }
    }
};

class AggregateFunctionPercentileApproxWeightedThreeParams
        : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxWeightedThreeParams(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        const auto& sources =
                assert_cast<const ColumnVector<Float32>&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& weight =
                assert_cast<const ColumnVector<Float32>&, TypeCheckOnRelease::DISABLE>(*columns[1]);
        const auto& quantile =
                assert_cast<const ColumnVector<Float32>&, TypeCheckOnRelease::DISABLE>(*columns[2]);

        this->data(place).init();
        this->data(place).add_with_weight(sources.get_element(row_num), weight.get_element(row_num),
                                          quantile.get_element(row_num));
    }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& col = assert_cast<ColumnFloat64&>(to);
        double result = AggregateFunctionPercentileApprox::data(place).get();

        if (std::isnan(result)) {
            col.insert_default();
        } else {
            col.get_data().push_back(result);
        }
    }
};

class AggregateFunctionPercentileApproxWeightedFourParams
        : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxWeightedFourParams(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}
    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        const auto& sources =
                assert_cast<const ColumnVector<Float32>&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& weight =
                assert_cast<const ColumnVector<Float32>&, TypeCheckOnRelease::DISABLE>(*columns[1]);
        const auto& quantile =
                assert_cast<const ColumnVector<Float32>&, TypeCheckOnRelease::DISABLE>(*columns[2]);
        const auto& compression =
                assert_cast<const ColumnVector<Float32>&, TypeCheckOnRelease::DISABLE>(*columns[3]);

        this->data(place).init(compression.get_element(row_num));
        this->data(place).add_with_weight(sources.get_element(row_num), weight.get_element(row_num),
                                          quantile.get_element(row_num));
    }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& col = assert_cast<ColumnFloat64&>(to);
        double result = AggregateFunctionPercentileApprox::data(place).get();

        if (std::isnan(result)) {
            col.insert_default();
        } else {
            col.get_data().push_back(result);
        }
    }
};

} // namespace doris::vectorized
#include "common/compile_check_end.h"