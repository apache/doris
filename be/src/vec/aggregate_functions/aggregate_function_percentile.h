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
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "util/counts.h"
#include "util/tdigest.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

class Arena;
class BufferReadable;

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

    void add(double source, double quantile) {
        digest->add(source);
        target_quantile = quantile;
    }

    void add_with_weight(double source, double weight, double quantile) {
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
    double target_quantile = INIT_QUANTILE;
    double compressions = 10000;
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

template <bool is_nullable>
class AggregateFunctionPercentileApproxTwoParams_OLDER : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxTwoParams_OLDER(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}
    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        if constexpr (is_nullable) {
            double column_data[2] = {0, 0};

            for (int i = 0; i < 2; ++i) {
                const auto* nullable_column = check_and_get_column<ColumnNullable>(columns[i]);
                if (nullable_column == nullptr) { //Not Nullable column
                    const auto& column =
                            assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(
                                    *columns[i]);
                    column_data[i] = column.get_element(row_num);
                } else if (!nullable_column->is_null_at(
                                   row_num)) { // Nullable column && Not null data
                    const auto& column =
                            assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(
                                    nullable_column->get_nested_column());
                    column_data[i] = column.get_element(row_num);
                } else { // Nullable column && null data
                    if (i == 0) {
                        return;
                    }
                }
            }

            this->data(place).init();
            this->data(place).add(column_data[0], column_data[1]);

        } else {
            const auto& sources =
                    assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[0]);
            const auto& quantile =
                    assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[1]);

            this->data(place).init();
            this->data(place).add(sources.get_element(row_num), quantile.get_element(row_num));
        }
    }

    DataTypePtr get_return_type() const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& nullable_column = assert_cast<ColumnNullable&>(to);
        double result = AggregateFunctionPercentileApprox::data(place).get();

        if (std::isnan(result)) {
            nullable_column.insert_default();
        } else {
            auto& col = assert_cast<ColumnFloat64&>(nullable_column.get_nested_column());
            col.get_data().push_back(result);
            nullable_column.get_null_map_data().push_back(0);
        }
    }
};

class AggregateFunctionPercentileApproxTwoParams : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxTwoParams(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}
    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        const auto& sources =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& quantile =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[1]);
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

template <bool is_nullable>
class AggregateFunctionPercentileApproxThreeParams_OLDER
        : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxThreeParams_OLDER(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}
    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        if constexpr (is_nullable) {
            double column_data[3] = {0, 0, 0};

            for (int i = 0; i < 3; ++i) {
                const auto* nullable_column = check_and_get_column<ColumnNullable>(columns[i]);
                if (nullable_column == nullptr) { //Not Nullable column
                    const auto& column =
                            assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(
                                    *columns[i]);
                    column_data[i] = column.get_element(row_num);
                } else if (!nullable_column->is_null_at(
                                   row_num)) { // Nullable column && Not null data
                    const auto& column =
                            assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(
                                    nullable_column->get_nested_column());
                    column_data[i] = column.get_element(row_num);
                } else { // Nullable column && null data
                    if (i == 0) {
                        return;
                    }
                }
            }

            this->data(place).init(column_data[2]);
            this->data(place).add(column_data[0], column_data[1]);

        } else {
            const auto& sources =
                    assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[0]);
            const auto& quantile =
                    assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[1]);
            const auto& compression =
                    assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[2]);

            this->data(place).init(compression.get_element(row_num));
            this->data(place).add(sources.get_element(row_num), quantile.get_element(row_num));
        }
    }

    DataTypePtr get_return_type() const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& nullable_column = assert_cast<ColumnNullable&>(to);
        double result = AggregateFunctionPercentileApprox::data(place).get();

        if (std::isnan(result)) {
            nullable_column.insert_default();
        } else {
            auto& col = assert_cast<ColumnFloat64&>(nullable_column.get_nested_column());
            col.get_data().push_back(result);
            nullable_column.get_null_map_data().push_back(0);
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
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& quantile =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[1]);
        const auto& compression =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[2]);

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

template <bool is_nullable>
class AggregateFunctionPercentileApproxWeightedThreeParams_OLDER
        : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxWeightedThreeParams_OLDER(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        if constexpr (is_nullable) {
            // sources quantile weight
            double column_data[3] = {0, 0, 0};
            for (int i = 0; i < 3; ++i) {
                const auto* nullable_column = check_and_get_column<ColumnNullable>(columns[i]);
                if (nullable_column == nullptr) { //Not Nullable column
                    const auto& column =
                            assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(
                                    *columns[i]);
                    column_data[i] = column.get_element(row_num);
                } else if (!nullable_column->is_null_at(
                                   row_num)) { // Nullable column && Not null data
                    const auto& column =
                            assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(
                                    nullable_column->get_nested_column());
                    column_data[i] = column.get_element(row_num);
                } else { // Nullable column && null data
                    if (i == 0) {
                        return;
                    }
                }
            }
            this->data(place).init();
            this->data(place).add_with_weight(column_data[0], column_data[1], column_data[2]);

        } else {
            const auto& sources =
                    assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(
                            *columns[0]);
            const auto& weight =
                    assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(
                            *columns[1]);
            const auto& quantile =
                    assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(
                            *columns[2]);

            this->data(place).init();
            this->data(place).add_with_weight(sources.get_element(row_num),
                                              weight.get_element(row_num),
                                              quantile.get_element(row_num));
        }
    }

    DataTypePtr get_return_type() const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& nullable_column = assert_cast<ColumnNullable&>(to);
        double result = AggregateFunctionPercentileApprox::data(place).get();

        if (std::isnan(result)) {
            nullable_column.insert_default();
        } else {
            auto& col = assert_cast<ColumnFloat64&>(nullable_column.get_nested_column());
            col.get_data().push_back(result);
            nullable_column.get_null_map_data().push_back(0);
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
                assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& weight =
                assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(*columns[1]);
        const auto& quantile =
                assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(*columns[2]);

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

template <bool is_nullable>
class AggregateFunctionPercentileApproxWeightedFourParams_OLDER
        : public AggregateFunctionPercentileApprox {
public:
    AggregateFunctionPercentileApproxWeightedFourParams_OLDER(const DataTypes& argument_types_)
            : AggregateFunctionPercentileApprox(argument_types_) {}
    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        if constexpr (is_nullable) {
            double column_data[4] = {0, 0, 0, 0};

            for (int i = 0; i < 4; ++i) {
                const auto* nullable_column = check_and_get_column<ColumnNullable>(columns[i]);
                if (nullable_column == nullptr) { //Not Nullable column
                    const auto& column =
                            assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(
                                    *columns[i]);
                    column_data[i] = column.get_element(row_num);
                } else if (!nullable_column->is_null_at(
                                   row_num)) { // Nullable column && Not null data
                    const auto& column =
                            assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(
                                    nullable_column->get_nested_column());
                    column_data[i] = column.get_element(row_num);
                } else { // Nullable column && null data
                    if (i == 0) {
                        return;
                    }
                }
            }

            this->data(place).init(column_data[3]);
            this->data(place).add_with_weight(column_data[0], column_data[1], column_data[2]);

        } else {
            const auto& sources =
                    assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(
                            *columns[0]);
            const auto& weight =
                    assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(
                            *columns[1]);
            const auto& quantile =
                    assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(
                            *columns[2]);
            const auto& compression =
                    assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(
                            *columns[3]);

            this->data(place).init(compression.get_element(row_num));
            this->data(place).add_with_weight(sources.get_element(row_num),
                                              weight.get_element(row_num),
                                              quantile.get_element(row_num));
        }
    }

    DataTypePtr get_return_type() const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& nullable_column = assert_cast<ColumnNullable&>(to);
        double result = AggregateFunctionPercentileApprox::data(place).get();

        if (std::isnan(result)) {
            nullable_column.insert_default();
        } else {
            auto& col = assert_cast<ColumnFloat64&>(nullable_column.get_nested_column());
            col.get_data().push_back(result);
            nullable_column.get_null_map_data().push_back(0);
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
                assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& weight =
                assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(*columns[1]);
        const auto& quantile =
                assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(*columns[2]);
        const auto& compression =
                assert_cast<const ColumnVector<Float64>&, TypeCheckOnRelease::DISABLE>(*columns[3]);

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

template <typename T>
struct PercentileState {
    mutable std::vector<Counts<T>> vec_counts;
    std::vector<double> vec_quantile;
    bool inited_flag = false;

    void write(BufferWritable& buf) const {
        write_binary(inited_flag, buf);
        int size_num = vec_quantile.size();
        write_binary(size_num, buf);
        for (const auto& quantile : vec_quantile) {
            write_binary(quantile, buf);
        }
        for (auto& counts : vec_counts) {
            counts.serialize(buf);
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
        vec_counts.clear();
        vec_counts.resize(size_num);
        for (int i = 0; i < size_num; ++i) {
            vec_counts[i].unserialize(buf);
        }
    }

    void add(T source, const PaddedPODArray<Float64>& quantiles, int arg_size) {
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
            vec_counts[i].merge(const_cast<Counts<T>*>(&(rhs.vec_counts[i])));
        }
    }

    void reset() {
        vec_counts.clear();
        vec_quantile.clear();
        inited_flag = false;
    }

    double get() const { return vec_counts.empty() ? 0 : vec_counts[0].terminate(vec_quantile[0]); }

    void insert_result_into(IColumn& to) const {
        auto& column_data = assert_cast<ColumnFloat64&>(to).get_data();
        for (int i = 0; i < vec_counts.size(); ++i) {
            column_data.push_back(vec_counts[i].terminate(vec_quantile[i]));
        }
    }
};

template <typename T>
class AggregateFunctionPercentile final
        : public IAggregateFunctionDataHelper<PercentileState<T>, AggregateFunctionPercentile<T>> {
public:
    using ColVecType = ColumnVector<T>;
    using Base = IAggregateFunctionDataHelper<PercentileState<T>, AggregateFunctionPercentile<T>>;
    AggregateFunctionPercentile(const DataTypes& argument_types_) : Base(argument_types_) {}

    String get_name() const override { return "percentile"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        const auto& sources =
                assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& quantile =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[1]);
        AggregateFunctionPercentile::data(place).add(sources.get_data()[row_num],
                                                     quantile.get_data(), 1);
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
        auto& col = assert_cast<ColumnFloat64&>(to);
        col.insert_value(AggregateFunctionPercentile::data(place).get());
    }
};

template <typename T>
class AggregateFunctionPercentileArray final
        : public IAggregateFunctionDataHelper<PercentileState<T>,
                                              AggregateFunctionPercentileArray<T>> {
public:
    using ColVecType = ColumnVector<T>;
    using Base =
            IAggregateFunctionDataHelper<PercentileState<T>, AggregateFunctionPercentileArray<T>>;
    AggregateFunctionPercentileArray(const DataTypes& argument_types_) : Base(argument_types_) {}

    String get_name() const override { return "percentile_array"; }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat64>()));
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        const auto& sources =
                assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& quantile_array =
                assert_cast<const ColumnArray&, TypeCheckOnRelease::DISABLE>(*columns[1]);
        const auto& offset_column_data = quantile_array.get_offsets();
        const auto& nested_column = assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(
                                            quantile_array.get_data())
                                            .get_nested_column();
        const auto& nested_column_data =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(nested_column);

        AggregateFunctionPercentileArray::data(place).add(
                sources.get_int(row_num), nested_column_data.get_data(),
                offset_column_data.data()[row_num] - offset_column_data[(ssize_t)row_num - 1]);
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