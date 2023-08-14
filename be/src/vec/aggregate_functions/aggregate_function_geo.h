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
#include <string.h>

#include <memory>
#include <string>

#include "geo/util/GeoCollection.h"
#include "geo/util/GeoMultiPoint.h"
#include "geo/util/GeoPoint.h"
#include "geo/util/GeoShape.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
template <typename T>
class ColumnDecimal;
template <typename T>
class DataTypeNumber;
template <typename>
class ColumnVector;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

struct AggregateFunctionGeoData {
    std::unique_ptr<GeoCollection> collection = GeoCollection::create_unique();

    void add(const StringRef& shape_value) {
        std::unique_ptr<GeoShape> shape;
        size_t size = shape_value.size;
        shape.reset(GeoShape::from_encoded(shape_value.data, size));
        collection->add_one_geometry(shape.release());
    }

    void merge(const AggregateFunctionGeoData& rhs) {
        for (int i = 0; i < rhs.collection->get_num_geometries(); ++i) {
            collection->add_one_geometry(rhs.collection->get_geometries_n(i));
        }
    }

    void write(BufferWritable& buf) const {
        std::string data;
        collection->encode_to(&data);
        write_binary(data, buf);
    }

    void read(BufferReadable& buf) {
        std::string data;
        read_binary(data, buf);
        size_t size = data.size();
        collection->from_encoded(data.data(), size);
    }

    const std::string get() const {
        std::string data;
        //should to_homogenize
        //collection->to_homogenize()->encode_to(&data);
        collection->encode_to(&data);
        return data;
    }

    void reset() { collection.reset(GeoCollection::create_unique().release()); }
};

struct AggregateFunctionGeoImplStr {
    static void add(AggregateFunctionGeoData& __restrict place, const IColumn** columns,
                    size_t row_num) {
        place.add(assert_cast<const ColumnString&>(*columns[0]).get_data_at(row_num));
    }
};

template <typename Impl>
class AggregateFunctionGeo final : public IAggregateFunctionDataHelper<AggregateFunctionGeoData,
                                                                       AggregateFunctionGeo<Impl>> {
public:
    AggregateFunctionGeo(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<AggregateFunctionGeoData, AggregateFunctionGeo<Impl>>(
                      argument_types_) {}

    String get_name() const override { return "st_collect"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        Impl::add(this->data(place), columns, row_num);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

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

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        const std::string result = this->data(place).get();
        assert_cast<ColumnString&>(to).insert_data(result.c_str(), result.length());
    }
};

} // namespace doris::vectorized
