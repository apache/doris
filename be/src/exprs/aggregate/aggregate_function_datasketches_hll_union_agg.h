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

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <hll.hpp>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_string.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/uint128.h"
#include "exec/common/hash_table/hash.h"
#include "exec/common/hash_table/phmap_fwd_decl.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/datasketches_hll_util.h"
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
    static constexpr uint8_t DEFAULT_UNION_LOG_K = datasketches::hll_constants::MAX_LOG_K;
    using Alloc = datasketches_hll::Alloc;
    using Sketch = datasketches_hll::Sketch;
    using Union = datasketches_hll::Union;

    std::optional<Union> hll_union_data;

    static String get_name() { return "datasketches_hll_union_agg"; }

    void merge(const Sketch& sketch_data, uint8_t lg_max_k) {
        if (sketch_data.is_empty()) {
            return;
        }
        try {
            if (!hll_union_data.has_value()) {
                hll_union_data.emplace(lg_max_k, Alloc());
            }
            hll_union_data->update(sketch_data);
        } catch (const doris::Exception& e) {
            // A failed library update cannot be reused; the wrapper reclaims its allocations
            // without dereferencing the possibly dangling library gadget.
            hll_union_data.reset();
            throw Exception(e.code(), "Internal error happened when update HLL sketch: {}",
                            e.to_string());
        } catch (const std::exception& e) {
            hll_union_data.reset();
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Internal error happened when update HLL sketch: {}", e.what());
        } catch (...) {
            hll_union_data.reset();
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Internal error happened when update HLL sketch: unknown exception.");
        }
    }
    void merge(const Sketch& sketch_data) {
        if (sketch_data.is_empty()) {
            return;
        }
        const auto lg_max_k = std::max<uint8_t>(sketch_data.get_lg_config_k(), MIN_UNION_LOG_K);
        if (hll_union_data.has_value() && lg_max_k < hll_union_data->get_lg_config_k()) {
            try {
                // Sparse sketch updates do not lower an existing union's lgK.
                auto current = hll_union_data->get_result(datasketches::HLL_8);
                hll_union_data.emplace(lg_max_k, Alloc());
                hll_union_data->update(current);
            } catch (const doris::Exception& e) {
                hll_union_data.reset();
                throw Exception(e.code(), "Internal error happened when update HLL sketch: {}",
                                e.to_string());
            } catch (const std::exception& e) {
                hll_union_data.reset();
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "Internal error happened when update HLL sketch: {}", e.what());
            } catch (...) {
                hll_union_data.reset();
                throw Exception(
                        ErrorCode::INTERNAL_ERROR,
                        "Internal error happened when update HLL sketch: unknown exception.");
            }
        }
        merge(sketch_data, lg_max_k);
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
        merge(datasketches_hll::deserialize(d, "read"));
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
            data.merge(datasketches_hll::deserialize(value, "add"), lg_max_k);
        }
    }
};

/// State for datasketches_hll_union, extending AggregateFunctionHllSketchData with target output
/// parameters so empty-sketch-only groups preserve explicit lg_max_k and tgt_type across
/// serialize/deserialize/merge.
template <PrimitiveType T>
struct DataSketchesHllUnionData : AggregateFunctionHllSketchData<T> {
    using Base = AggregateFunctionHllSketchData<T>;
    using Alloc = typename Base::Alloc;
    using Sketch = typename Base::Sketch;
    using Union = typename Base::Union;

    bool params_set = false;
    uint8_t lg_max_k = Base::DEFAULT_UNION_LOG_K;
    uint8_t empty_lg_k = Base::EMPTY_STATE_LOG_K;
    datasketches::target_hll_type tgt_type = datasketches::HLL_4;

    static String get_name() { return "datasketches_hll_union"; }

    void set_params(uint8_t max_k, uint8_t empty_k, datasketches::target_hll_type type) {
        params_set = true;
        lg_max_k = max_k;
        empty_lg_k = empty_k;
        tgt_type = type;
    }

    void merge_params(const DataSketchesHllUnionData& rhs) {
        if (!params_set && rhs.params_set) {
            params_set = true;
            lg_max_k = rhs.lg_max_k;
            empty_lg_k = rhs.empty_lg_k;
            tgt_type = rhs.tgt_type;
        }
    }

    void reset() {
        Base::reset();
        params_set = false;
        lg_max_k = Base::DEFAULT_UNION_LOG_K;
        empty_lg_k = Base::EMPTY_STATE_LOG_K;
        tgt_type = datasketches::HLL_4;
    }

    void write(BufferWritable& buf) const {
        Base::write(buf);
        buf.write_binary(params_set);
        buf.write_binary(lg_max_k);
        buf.write_binary(empty_lg_k);
        buf.write_binary(static_cast<uint8_t>(tgt_type));
    }

    void read(BufferReadable& buf) {
        Base::read(buf);
        bool rhs_params_set = false;
        uint8_t rhs_lg_max_k = Base::DEFAULT_UNION_LOG_K;
        uint8_t rhs_empty_lg_k = Base::EMPTY_STATE_LOG_K;
        uint8_t raw_tgt_type = 0;
        buf.read_binary(rhs_params_set);
        buf.read_binary(rhs_lg_max_k);
        buf.read_binary(rhs_empty_lg_k);
        buf.read_binary(raw_tgt_type);
        datasketches::target_hll_type parsed_tgt_type = datasketches::HLL_4;
        switch (raw_tgt_type) {
        case datasketches::HLL_4:
        case datasketches::HLL_6:
        case datasketches::HLL_8:
            parsed_tgt_type = static_cast<datasketches::target_hll_type>(raw_tgt_type);
            break;
        default:
            throw Exception(ErrorCode::CORRUPTION,
                            "HLL sketch state corrupted when read: invalid tgt_type {}",
                            raw_tgt_type);
        }
        if (!params_set && rhs_params_set) {
            set_params(rhs_lg_max_k, rhs_empty_lg_k, parsed_tgt_type);
        }
    }

    auto get_sketch_bytes() const {
        try {
            if (this->hll_union_data.has_value()) {
                return this->hll_union_data->get_result(tgt_type).serialize_compact();
            }
            Union u(empty_lg_k, Alloc());
            return u.get_result(tgt_type).serialize_compact();
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
};

/// Unions serialized DataSketches HLL sketches and returns a serialized compact sketch whose type
/// matches the first argument (STRING/VARCHAR -> DataTypeString, VARBINARY -> DataTypeVarbinary).
template <PrimitiveType T, typename Data>
class AggregateFunctionDataSketchesHllUnion final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionDataSketchesHllUnion<T, Data>>,
          VarargsExpression,
          NullableAggregateFunction {
public:
    explicit AggregateFunctionDataSketchesHllUnion(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionDataSketchesHllUnion<T, Data>>(
                      argument_types_) {}

    String get_name() const override { return Data::get_name(); }

    DataTypePtr get_return_type() const override {
        if constexpr (is_varbinary(T)) {
            return std::make_shared<DataTypeVarbinary>();
        }
        return std::make_shared<DataTypeString>();
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        auto& state = this->data(place);
        uint8_t lg_max_k = state.lg_max_k;
        uint8_t empty_lg_k = state.empty_lg_k;
        datasketches::target_hll_type tgt_type = state.tgt_type;
        if (!state.params_set) {
            lg_max_k = Data::DEFAULT_UNION_LOG_K;
            empty_lg_k = Data::EMPTY_STATE_LOG_K;
            tgt_type = datasketches::HLL_4;
            if (this->argument_types.size() >= 2) {
                const auto value =
                        assert_cast<const ColumnInt32&, TypeCheckOnRelease::DISABLE>(*columns[1])
                                .get_element(row_num);
                if (value < Data::MIN_UNION_LOG_K ||
                    std::cmp_greater(value, datasketches::hll_constants::MAX_LOG_K)) {
                    throw Exception(ErrorCode::INVALID_ARGUMENT,
                                    "lg_max_k must be between {} and {}, but was {}",
                                    Data::MIN_UNION_LOG_K, datasketches::hll_constants::MAX_LOG_K,
                                    value);
                }
                lg_max_k = static_cast<uint8_t>(value);
                empty_lg_k = lg_max_k;
            }
            if (this->argument_types.size() == 3) {
                const StringRef raw_tgt_type =
                        assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(*columns[2])
                                .get_data_at(static_cast<size_t>(row_num));
                if (!datasketches_hll::parse_tgt_type(
                            std::string_view(raw_tgt_type.data, raw_tgt_type.size), tgt_type)) {
                    throw Exception(ErrorCode::INVALID_ARGUMENT,
                                    "tgt_type must be one of HLL_4, HLL_6, HLL_8, but was '{}'",
                                    raw_tgt_type.to_string());
                }
            }
        }
        if constexpr (is_string_type(T) || is_varbinary(T)) {
            const auto& src_column = assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                                 TypeCheckOnRelease::DISABLE>(*columns[0]);
            StringRef value = src_column.get_data_at(static_cast<size_t>(row_num));
            state.merge(datasketches_hll::deserialize(value, "add"), lg_max_k);
        }
        if (!state.params_set) {
            state.set_params(lg_max_k, empty_lg_k, tgt_type);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        auto& dst_data = this->data(place);
        const auto& rhs_data = this->data(rhs);
        if (rhs_data.hll_union_data.has_value()) {
            dst_data.merge(rhs_data.hll_union_data->get_result(datasketches::HLL_8));
        }
        dst_data.merge_params(rhs_data);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        const auto serialized_bytes = this->data(place).get_sketch_bytes();
        using ColumnType = typename PrimitiveTypeTraits<T>::ColumnType;
        assert_cast<ColumnType&, TypeCheckOnRelease::DISABLE>(to).insert_data(
                reinterpret_cast<const char*>(serialized_bytes.data()), serialized_bytes.size());
    }

    void check_input_columns_type(const IColumn** columns) const override {
        IAggregateFunction::check_input_columns_type(columns);
        if constexpr (is_string_type(T) || is_varbinary(T)) {
            this->template check_argument_column_type<typename PrimitiveTypeTraits<T>::ColumnType>(
                    columns[0]);
        }
        if (this->argument_types.size() >= 2) {
            this->template check_argument_column_type<ColumnInt32>(columns[1]);
        }
        if (this->argument_types.size() == 3) {
            this->template check_argument_column_type<ColumnString>(columns[2]);
        }
    }
};
} // namespace doris
