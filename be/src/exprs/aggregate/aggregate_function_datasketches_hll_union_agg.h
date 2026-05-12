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
#include <stddef.h>

#include <DataSketches/hll.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <type_traits>
#include <vector>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/uint128.h"
#include "exec/common/hash_table/hash.h"
#include "exec/common/hash_table/phmap_fwd_decl.h"
#include "exprs/aggregate/aggregate_function.h"
#include "util/var_int.h"
template <typename T>
struct HashCRC32;
namespace doris {
class Arena;
class BufferReadable;
class BufferWritable;
template <PrimitiveType T>
class ColumnDecimal;
/// datasketches_hll_union_agg
template <PrimitiveType T>
struct AggregateFunctionHllSketchData {
    /** We set the default LgK to 12,
      * as this value is used as a performance baseline in the relevant documentation.
      * (https://datasketches.apache.org/docs/HLL/HllPerformance.html)
      */
    static const uint8_t DEFAULT_LOG_K = 12;
    std::unique_ptr<datasketches::hll_union> hll_union_data;
    static String get_name() { return "datasketches_hll_union_agg"; }
    void merge(const datasketches::hll_sketch & sketch_data) {
        if (hll_union_data == nullptr) {
            hll_union_data =
                    std::make_unique<datasketches::hll_union>(sketch_data.get_lg_config_k());
        }
        try {
            hll_union_data->update(sketch_data);
        } catch (...) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Internal error happened when update HLL sketch.");
        }
    }
    void reset() {
        if (hll_union_data != nullptr) {
            hll_union_data->reset();
        }
        hll_union_data = nullptr;
    }
    void write_sketch(BufferWritable& buf, const datasketches::hll_sketch& sk) const {
        auto serialized_bytes = sk.serialize_compact();
        StringRef d(serialized_bytes.data(), serialized_bytes.size());
        buf.write_binary(d);
    }
    void write(BufferWritable& buf) const {
        if (hll_union_data == nullptr) {
            /** Using DEFAULT_LOG_K(12) here is surely sufficient,
              * because in this case the union that actually needs to be serialized should contain no data.
              */
            datasketches::hll_union u(DEFAULT_LOG_K);
            write_sketch(buf, u.get_result());
        }
        try {
            auto cache = hll_union_data->get_result();
            write_sketch(buf, cache);
        } catch (...) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Internal error happened when serialize HLL sketch.");
        }
    }
    void read(BufferReadable& buf) {
        StringRef d;
        buf.read_binary(d);
        try {
            auto cache = datasketches::hll_sketch::deserialize(d.data, d.size);
            merge(cache);
        } catch (...) {
            throw Exception(ErrorCode::CORRUPTION, "HLL sketch data corrupted when read.");
        }
    }
    int64_t get_result() const {
        if (hll_union_data != nullptr) {
            try {
                return static_cast<int64_t>(hll_union_data->get_estimate());
            } catch (...) {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "Internal error happened when get HLL sketch estimate.");
            }
        }
        return 0;
    }
};
namespace detail {
/** The structure for the delegation work to add one element to the `datasketches_hll_union_agg` aggregate functions.
  * Used for partial specialization to add strings.
  */
template <PrimitiveType T, typename Data>
struct OneAdder {
    static void ALWAYS_INLINE add(Data& data, const IColumn& column, size_t row_num) {
        if constexpr (is_string_type(T) || is_varbinary(T)) {
            StringRef value = column.get_data_at(row_num);
            if (value.empty()) {
                return;
            }
            try {
                datasketches::hll_sketch sketch_data =
                        datasketches::hll_sketch::deserialize(value.begin(), value.size);
                data.merge(sketch_data);
            } catch (...) {
                throw Exception(ErrorCode::CORRUPTION, "HLL sketch data corrupted when add.");
            }
        }
    }
};
} // namespace detail
/// Calculates the number of different values approximately using hll sketch.
template <PrimitiveType T, typename Data>
class AggregateFunctionDataSketchesHllUnionAgg final
        : public IAggregateFunctionDataHelper<Data,
                                              AggregateFunctionDataSketchesHllUnionAgg<T, Data>>,
          VarargsExpression,
          NotNullableAggregateFunction {
public:
    AggregateFunctionDataSketchesHllUnionAgg(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionDataSketchesHllUnionAgg<T, Data>>(
                      argument_types_) {}
    String get_name() const override { return Data::get_name(); }
    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }
    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }
    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        detail::OneAdder<T, Data>::add(this->data(place), *columns[0], row_num);
    }
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(this->data(rhs).hll_union_data->get_result(datasketches::HLL_8));
    }
    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }
    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }
    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(this->data(place).get_result());
    }
};
} // namespace doris