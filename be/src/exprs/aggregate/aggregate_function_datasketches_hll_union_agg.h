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
    static constexpr uint8_t EMPTY_STATE_LOG_K = 12;
    static constexpr uint8_t MIN_UNION_LOG_K = 7;
    static constexpr uint8_t DEFAULT_UNION_LOG_K = 12;
    using Alloc = CustomStdAllocator<uint8_t>;
    using Sketch = datasketches::hll_sketch_alloc<Alloc>;
    using Union = datasketches::hll_union_alloc<Alloc>;

    std::optional<Union> hll_union_data;

    static String get_name() { return "datasketches_hll_union_agg"; }

    void merge(const Sketch& sketch_data, uint8_t lg_max_k) {
        if (sketch_data.is_empty()) {
            return;
        }
        if (!hll_union_data.has_value()) {
            hll_union_data.emplace(lg_max_k, Alloc());
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
    void merge(const Sketch& sketch_data) {
        merge(sketch_data, std::max<uint8_t>(sketch_data.get_lg_config_k(), MIN_UNION_LOG_K));
    }
    void reset() { hll_union_data.reset(); }

    void write_sketch(BufferWritable& buf, const Sketch& sk) const {
        auto serialized_bytes = sk.serialize_compact();
        StringRef d(serialized_bytes.data(), serialized_bytes.size());
        buf.write_binary(d);
    }
    void write(BufferWritable& buf) const {
        if (!hll_union_data.has_value()) {
            Union u(EMPTY_STATE_LOG_K, Alloc());
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

        auto cache = [&]() -> Sketch {
            try {
                return Sketch::deserialize(d.data, d.size, Alloc());
            } catch (const doris::Exception& e) {
                throw Exception(e.code(), "Failed to deserialize HLL sketch when read: {}",
                                e.to_string());
            } catch (const std::exception& e) {
                throw Exception(ErrorCode::CORRUPTION, "HLL sketch data corrupted when read: {}",
                                e.what());
            } catch (...) {
                throw Exception(ErrorCode::CORRUPTION,
                                "HLL sketch data corrupted when read: unknown exception.");
            }
        }();

        merge(cache);
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
          VarargsExpression,
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
        uint8_t lg_max_k = Data::DEFAULT_UNION_LOG_K;
        if (this->argument_types.size() == 2) {
            const auto value =
                    assert_cast<const ColumnInt32&, TypeCheckOnRelease::DISABLE>(*columns[1])
                            .get_element(row_num);
            if (value < Data::MIN_UNION_LOG_K || value > datasketches::hll_constants::MAX_LOG_K) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "lg_max_k must be between {} and {}, but was {}",
                                Data::MIN_UNION_LOG_K, datasketches::hll_constants::MAX_LOG_K,
                                value);
            }
            lg_max_k = static_cast<uint8_t>(value);
        }
        add_one(this->data(place), *columns[0], row_num, lg_max_k);
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
        assert_cast<ColumnFloat64&, TypeCheckOnRelease::DISABLE>(to).get_data().push_back(
                this->data(place).get_result());
    }

    void check_input_columns_type(const IColumn** columns) const override {
        IAggregateFunction::check_input_columns_type(columns);
        if constexpr (is_string_type(T) || is_varbinary(T)) {
            this->template check_argument_column_type<typename PrimitiveTypeTraits<T>::ColumnType>(
                    columns[0]);
        }
        if (this->argument_types.size() == 2) {
            this->template check_argument_column_type<ColumnInt32>(columns[1]);
        }
    }

private:
    static void ALWAYS_INLINE add_one(Data& data, const IColumn& column, ssize_t row_num,
                                      uint8_t lg_max_k) {
        if constexpr (is_string_type(T) || is_varbinary(T)) {
            const auto& src_column = assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                                 TypeCheckOnRelease::DISABLE>(column);
            StringRef value = src_column.get_data_at(static_cast<size_t>(row_num));
            if (value.empty()) {
                throw Exception(ErrorCode::CORRUPTION,
                                "HLL sketch data corrupted when add: empty input.");
            }

            using Sketch = typename Data::Sketch;
            using Alloc = typename Data::Alloc;

            auto sketch_data = [&]() -> Sketch {
                try {
                    return Sketch::deserialize(value.begin(), value.size, Alloc());
                } catch (const doris::Exception& e) {
                    throw Exception(e.code(), "Failed to deserialize HLL sketch when add: {}",
                                    e.to_string());
                } catch (const std::exception& e) {
                    throw Exception(ErrorCode::CORRUPTION, "HLL sketch data corrupted when add: {}",
                                    e.what());
                } catch (...) {
                    throw Exception(ErrorCode::CORRUPTION,
                                    "HLL sketch data corrupted when add: unknown exception.");
                }
            }();

            data.merge(sketch_data, lg_max_k);
        }
    }
};
} // namespace doris
