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

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <unordered_map>
#include <vector>

#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_decimal.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

// TODO: optimize count=0
// TODO: support datetime
// TODO: support foreach

namespace doris {

template <PrimitiveType T>
struct AggregateFunctionLinearHistogramData {
    // bucket key limits
    const static int32_t MIN_BUCKET_KEY = std::numeric_limits<int32_t>::min();
    const static int32_t MAX_BUCKET_KEY = std::numeric_limits<int32_t>::max();
    const static int32_t NO_BOUNDARY_SCALE = -1;
    const static int32_t MAX_BOUNDARY_SCALE = 12;

private:
    // influxdb use double
    double interval = 0;
    double offset = 0;
    int32_t boundary_decimal_scale = NO_BOUNDARY_SCALE;
    double lower; // not used yet
    double upper; // not used yet
    std::unordered_map<int32_t, size_t,
                       decltype([](int32_t key) { return static_cast<size_t>(key); })>
            buckets;

public:
    // reset
    void reset() {
        offset = 0;
        interval = 0;
        boundary_decimal_scale = NO_BOUNDARY_SCALE;
        buckets.clear();
    }

    void set_parameters(double input_interval, double input_offset) {
        if (interval == input_interval && offset == input_offset) {
            return;
        }
        interval = input_interval;
        offset = input_offset;
        boundary_decimal_scale = infer_boundary_scale();
    }

    // add
    void add(const typename PrimitiveTypeTraits<T>::CppType& value, UInt32 scale) {
        double val = 0;
        if constexpr (is_decimal(T)) {
            using NativeType = typename PrimitiveTypeTraits<T>::CppType::NativeType;
            val = static_cast<double>(value.value) /
                  static_cast<double>(decimal_scale_multiplier<NativeType>(scale));
        } else {
            val = static_cast<double>(value);
        }
        double key = bucket_key(val);
        if (key <= MIN_BUCKET_KEY || key >= MAX_BUCKET_KEY) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "{} exceeds the bucket range limit",
                                   val);
        }
        buckets[static_cast<int32_t>(key)]++;
    }

    // merge
    void merge(const AggregateFunctionLinearHistogramData& rhs) {
        if (rhs.interval == 0) {
            return;
        }

        interval = rhs.interval;
        offset = rhs.offset;
        boundary_decimal_scale = rhs.boundary_decimal_scale;

        for (const auto& [key, count] : rhs.buckets) {
            buckets[key] += count;
        }
    }

    // write
    void write(BufferWritable& buf) const {
        buf.write_binary(offset);
        buf.write_binary(interval);
        buf.write_binary(lower);
        buf.write_binary(upper);
        buf.write_binary(buckets.size());
        for (const auto& [key, count] : buckets) {
            buf.write_binary(key);
            buf.write_binary(count);
        }
    }

    // read
    void read(BufferReadable& buf) {
        buf.read_binary(offset);
        buf.read_binary(interval);
        boundary_decimal_scale = infer_boundary_scale();
        buf.read_binary(lower);
        buf.read_binary(upper);
        size_t size;
        buf.read_binary(size);
        for (size_t i = 0; i < size; i++) {
            int32_t key;
            size_t count;
            buf.read_binary(key);
            buf.read_binary(count);
            buckets[key] = count;
        }
    }

    // insert_result_into
    void insert_result_into(IColumn& to) const {
        std::vector<std::pair<int32_t, size_t>> bucket_vector(buckets.begin(), buckets.end());
        std::sort(bucket_vector.begin(), bucket_vector.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

        rapidjson::Document doc;
        doc.SetObject();
        rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

        unsigned num_buckets = bucket_vector.empty() ? 0
                                                     : bucket_vector.rbegin()->first -
                                                               bucket_vector.begin()->first + 1;
        doc.AddMember("num_buckets", num_buckets, allocator);

        rapidjson::Value bucket_arr(rapidjson::kArrayType);
        bucket_arr.Reserve(num_buckets, allocator);

        if (num_buckets > 0) {
            int32_t idx = bucket_vector.begin()->first;
            size_t count = 0;
            size_t acc_count = 0;

            for (const auto& [key, count_] : bucket_vector) {
                for (; idx <= key; ++idx) {
                    rapidjson::Value bucket_json(rapidjson::kObjectType);
                    double left = bucket_boundary(idx, boundary_decimal_scale);
                    bucket_json.AddMember("lower", left, allocator);
                    bucket_json.AddMember("upper", bucket_boundary(idx + 1, boundary_decimal_scale),
                                          allocator);
                    count = (idx == key) ? count_ : 0;
                    bucket_json.AddMember("count", static_cast<uint64_t>(count), allocator);
                    acc_count += count;
                    bucket_json.AddMember("acc_count", static_cast<uint64_t>(acc_count), allocator);
                    bucket_arr.PushBack(bucket_json, allocator);
                }
            }
        }

        doc.AddMember("buckets", bucket_arr, allocator);

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);

        auto& column = assert_cast<ColumnString&, TypeCheckOnRelease::DISABLE>(to);
        column.insert_data(buffer.GetString(), buffer.GetSize());
    }

private:
    double bucket_key(double value) const {
        double key = (value - offset) / interval;
        double rounded_key = std::round(key);
        if (boundary_decimal_scale != NO_BOUNDARY_SCALE) {
            double raw_boundary = rounded_key * interval + offset;
            double boundary = normalize_boundary(raw_boundary, boundary_decimal_scale);
            if (value == boundary) {
                return rounded_key;
            }
        } else if (value == rounded_key * interval + offset) {
            return rounded_key;
        }
        return std::floor(key);
    }

    int32_t infer_boundary_scale() const {
        int32_t interval_scale = decimal_scale(interval);
        int32_t offset_scale = decimal_scale(offset);
        if (interval_scale == NO_BOUNDARY_SCALE || offset_scale == NO_BOUNDARY_SCALE) {
            return NO_BOUNDARY_SCALE;
        }
        return std::max(interval_scale, offset_scale);
    }

    int32_t decimal_scale(double value) const {
        double multiplier = 1;
        for (int32_t scale = 0; scale <= MAX_BOUNDARY_SCALE; ++scale, multiplier *= 10) {
            double scaled_value = value * multiplier;
            double rounded_value = std::round(scaled_value);
            if (scaled_value == rounded_value) {
                if (rounded_value == 0 && value != 0) {
                    continue;
                }
                return scale;
            }
        }
        return NO_BOUNDARY_SCALE;
    }

    double bucket_boundary(int32_t key, int32_t boundary_scale) const {
        double boundary = key * interval + offset;
        if (boundary_scale == NO_BOUNDARY_SCALE) {
            return boundary;
        }
        return normalize_boundary(boundary, boundary_scale);
    }

    double normalize_boundary(double boundary, int32_t boundary_scale) const {
        double multiplier = std::pow(10, boundary_scale);
        return std::round(boundary * multiplier) / multiplier;
    }
};

class AggregateFunctionLinearHistogramConsts {
public:
    const static std::string NAME;
};

template <PrimitiveType T, typename Data, bool has_offset>
class AggregateFunctionLinearHistogram final
        : public IAggregateFunctionDataHelper<
                  Data, AggregateFunctionLinearHistogram<T, Data, has_offset>>,
          MultiExpression,
          NotNullableAggregateFunction {
public:
    using ColVecType = typename PrimitiveTypeTraits<T>::ColumnType;

    AggregateFunctionLinearHistogram(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data,
                                           AggregateFunctionLinearHistogram<T, Data, has_offset>>(
                      argument_types_),
              scale(get_decimal_scale(*argument_types_[0])) {}

    std::string get_name() const override { return AggregateFunctionLinearHistogramConsts::NAME; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        double interval =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[1])
                        .get_data()[row_num];
        if (interval <= 0) {
            throw doris::Exception(
                    ErrorCode::INVALID_ARGUMENT,
                    "Invalid interval {}, row_num {}, interval should be larger than 0", interval,
                    row_num);
        }

        double offset = 0;
        if constexpr (has_offset) {
            offset = assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[2])
                             .get_data()[row_num];
            if (offset < 0 || offset >= interval) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Invalid offset {}, row_num {}, offset should be in [0, interval)", offset,
                        row_num);
            }
        }

        this->data(place).set_parameters(interval, offset);

        this->data(place).add(
                assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0])
                        .get_data()[row_num],
                scale);
    }

    void check_input_columns_type(const IColumn** columns) const override {
        this->template check_argument_column_type<ColVecType>(columns[0]);
        this->template check_argument_column_type<ColumnFloat64>(columns[1]);
        if constexpr (has_offset) {
            this->template check_argument_column_type<ColumnFloat64>(columns[2]);
        }
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void check_result_column_type(const IColumn& to) const override {
        IAggregateFunction::check_result_column_type(to);
        this->template check_result_column_type_as<ColumnString>(to);
    }

private:
    UInt32 scale;
};

} // namespace doris
