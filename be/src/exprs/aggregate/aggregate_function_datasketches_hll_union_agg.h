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
#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/custom_allocator.h"
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
    static constexpr uint8_t DEFAULT_LOG_K = 12;
    using Alloc = CustomStdAllocator<uint8_t>;
    using Sketch = datasketches::hll_sketch_alloc<Alloc>;
    using Union = datasketches::hll_union_alloc<Alloc>;

    std::optional<Union> hll_union_data;

    static String get_name() { return "datasketches_hll_union_agg"; }

    void merge(const Sketch& sketch_data) {
        if (!hll_union_data.has_value()) {
            /** We clamp max lg_k to [7, 21],
              * considering that the code comment requires 7 to 21.
              * See: datasketches-cpp/hll/include/hll.hpp:451
              */
            constexpr uint8_t MIN_UNION_LOG_K = 7;
            const uint8_t union_lg_k =
                    std::clamp<uint8_t>(sketch_data.get_lg_config_k(), MIN_UNION_LOG_K,
                                        datasketches::hll_constants::MAX_LOG_K);
            hll_union_data.emplace(union_lg_k, Alloc());
        }
        try {
            hll_union_data->update(sketch_data);
        } catch (const doris::Exception& e) {
            throw Exception(e.code(), "Internal error happened when update HLL sketch: {}",
                            e.to_string());
        } catch (const std::exception& e) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Internal error happened when update HLL sketch: {}", e.what());
        } catch (...) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Internal error happened when update HLL sketch: unknown exception.");
        }
    }
    void reset() {
        if (hll_union_data.has_value()) {
            hll_union_data->reset();
        }
        hll_union_data.reset();
    }

    void write_sketch(BufferWritable& buf, const Sketch& sk) const {
        auto serialized_bytes = sk.serialize_compact();
        StringRef d(serialized_bytes.data(), serialized_bytes.size());
        buf.write_binary(d);
    }
    void write(BufferWritable& buf) const {
        if (!hll_union_data.has_value()) {
            /** Using DEFAULT_LOG_K(12) here is surely sufficient,
              * because in this case the union that actually needs to be serialized should contain no data.
              */
            Union u(DEFAULT_LOG_K, Alloc());
            write_sketch(buf, u.get_result());
            return;
        }
        try {
            auto cache = hll_union_data->get_result();
            write_sketch(buf, cache);
        } catch (const doris::Exception& e) {
            throw Exception(e.code(), "Internal error happened when serialize HLL sketch: {}",
                            e.to_string());
        } catch (const std::exception& e) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Internal error happened when serialize HLL sketch: {}", e.what());
        } catch (...) {
            throw Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "Internal error happened when serialize HLL sketch: unknown exception.");
        }
    }
    void read(BufferReadable& buf) {
        StringRef d;
        buf.read_binary(d);
        try {
            auto cache = Sketch::deserialize(d.data, d.size, Alloc());
            merge(cache);
        } catch (const doris::Exception& e) {
            throw Exception(ErrorCode::CORRUPTION, "HLL sketch data corrupted when read: {}",
                            e.to_string());
        } catch (const std::exception& e) {
            throw Exception(ErrorCode::CORRUPTION, "HLL sketch data corrupted when read: {}",
                            e.what());
        } catch (...) {
            throw Exception(ErrorCode::CORRUPTION,
                            "HLL sketch data corrupted when read: unknown exception.");
        }
    }
    double get_result() const {
        if (hll_union_data.has_value()) {
            try {
                return hll_union_data->get_estimate();
            } catch (const doris::Exception& e) {
                throw Exception(e.code(),
                                "Internal error happened when get HLL sketch estimate: {}",
                                e.to_string());
            } catch (const std::exception& e) {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "Internal error happened when get HLL sketch estimate: {}",
                                e.what());
            } catch (...) {
                throw Exception(
                        ErrorCode::INTERNAL_ERROR,
                        "Internal error happened when get HLL sketch estimate: unknown exception.");
            }
        }
        return 0.0;
    }
};

/// Calculates the number of different values approximately using hll sketch.
template <PrimitiveType T, typename Data>
class AggregateFunctionDataSketchesHllUnionAgg final
        : public IAggregateFunctionDataHelper<Data,
                                              AggregateFunctionDataSketchesHllUnionAgg<T, Data>>,
          UnaryExpression,
          NotNullableAggregateFunction {
public:
    AggregateFunctionDataSketchesHllUnionAgg(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionDataSketchesHllUnionAgg<T, Data>>(
                      argument_types_) {}
    String get_name() const override { return Data::get_name(); }
    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }
    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }
    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        add_one(this->data(place), *columns[0], row_num);
    }
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        const auto& rhs_data = this->data(rhs);
        if (!rhs_data.hll_union_data.has_value()) {
            return;
        }
        this->data(place).merge(rhs_data.hll_union_data->get_result(datasketches::HLL_8));
    }
    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }
    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }
    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        assert_cast<ColumnFloat64&>(to).get_data().push_back(this->data(place).get_result());
    }

private:
    static void ALWAYS_INLINE add_one(Data& data, const IColumn& column, ssize_t row_num) {
        if constexpr (is_string_type(T) || is_varbinary(T)) {
            const auto& src_column = assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                                 TypeCheckOnRelease::DISABLE>(column);
            StringRef value = src_column.get_data_at(static_cast<size_t>(row_num));
            if (value.empty()) {
                throw Exception(ErrorCode::CORRUPTION,
                                "HLL sketch data corrupted when add: empty input.");
            }
            try {
                using Sketch = typename Data::Sketch;
                using Alloc = typename Data::Alloc;
                Sketch sketch_data = Sketch::deserialize(value.begin(), value.size, Alloc());
                data.merge(sketch_data);
            } catch (const doris::Exception& e) {
                throw Exception(ErrorCode::CORRUPTION, "HLL sketch data corrupted when add: {}",
                                e.to_string());
            } catch (const std::exception& e) {
                throw Exception(ErrorCode::CORRUPTION, "HLL sketch data corrupted when add: {}",
                                e.what());
            } catch (...) {
                throw Exception(ErrorCode::CORRUPTION,
                                "HLL sketch data corrupted when add: unknown exception.");
            }
        }
    }
};
} // namespace doris
