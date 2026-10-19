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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionNull.h
// and modified by Doris

#pragma once

#include <glog/logging.h>

#include <algorithm>
#include <array>
#include <memory>

#include "agent/be_exec_version_manager.h"
#include "common/logging.h"
#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_nullable.h"
#include "core/string_buffer.hpp"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_distinct.h"
#include "exprs/aggregate/aggregate_function_legacy_nullable.h"

namespace doris {

template <typename NestFunction, bool result_is_nullable, typename Derived>
class AggregateFunctionNullBaseInlineV2 : public IAggregateFunctionHelper<Derived> {
protected:
    std::unique_ptr<NestFunction> nested_function;
    size_t prefix_size;
    bool is_window_function = false;

    AggregateDataPtr nested_place(AggregateDataPtr __restrict place) const noexcept {
        return place + prefix_size;
    }

    ConstAggregateDataPtr nested_place(ConstAggregateDataPtr __restrict place) const noexcept {
        return place + prefix_size;
    }

    static void init(AggregateDataPtr __restrict place, bool is_window_function) noexcept {
        init_flag(place);
        init_null_count(place, is_window_function);
    }

    static constexpr bool supports_empty_state_byte_marker = [] {
        if constexpr (requires { NestFunction::supports_empty_state_byte_marker; }) {
            return !result_is_nullable && NestFunction::supports_empty_state_byte_marker;
        }
        return false;
    }();

    static void init_flag(AggregateDataPtr __restrict place) noexcept { place[0] = false; }

    static void set_flag(AggregateDataPtr __restrict place) noexcept { place[0] = true; }

    static bool get_flag(ConstAggregateDataPtr __restrict place) noexcept { return place[0] == 1; }

    static void init_null_count(AggregateDataPtr __restrict place,
                                bool is_window_function) noexcept {
        if (is_window_function && result_is_nullable) {
            unaligned_store<int32_t>(place + 1, 0);
        }
    }

    static void update_null_count(AggregateDataPtr __restrict place, bool incremental,
                                  bool is_window_function) noexcept {
        if (is_window_function && result_is_nullable) {
            auto null_count = unaligned_load<int32_t>(place + 1);
            incremental ? null_count++ : null_count--;
            unaligned_store<int32_t>(place + 1, null_count);
        }
    }

    static int32_t get_null_count(ConstAggregateDataPtr __restrict place,
                                  bool is_window_function) noexcept {
        int32_t num = 0;
        if (is_window_function && result_is_nullable) {
            num = unaligned_load<int32_t>(place + 1);
        }
        return num;
    }

public:
    AggregateFunctionNullBaseInlineV2(IAggregateFunction* nested_function_,
                                      const DataTypes& arguments, bool is_window_function_,
                                      int be_exec_version)
            : IAggregateFunctionHelper<Derived>(arguments),
              nested_function {assert_cast<NestFunction*>(nested_function_)},
              is_window_function(is_window_function_) {
        DCHECK(nested_function_ != nullptr);
        if constexpr (result_is_nullable) {
            if (this->is_window_function) {
                // flag|---null_count----|-------padding-------|--nested_data----|
                size_t nested_align = nested_function->align_of_data();
                prefix_size = 1 + sizeof(int32_t);
                if (prefix_size % nested_align != 0) {
                    prefix_size += (nested_align - (prefix_size % nested_align));
                }
            } else {
                prefix_size = nested_function->align_of_data();
            }
        } else {
            // BITMAP_INTERSECT returns a non-NULL empty bitmap for no input, but its
            // empty partial must still be distinguishable from an actual empty bitmap.
            prefix_size = nested_function->align_of_data();
        }
        // The serialized type is queried by non-pipeline callers too (AGG_STATE and UT).
        // Initialize this wrapper from its creator's negotiated version immediately.
        AggregateFunctionNullBaseInlineV2::set_version(be_exec_version);
    }

    bool native_state_is_nullable() const {
        return result_is_nullable || this->version >= USE_UNIFIED_NULLABLE_AGGREGATION_VERSION;
    }

    MutableColumnPtr create_serialize_column() const override {
        if (native_state_is_nullable()) {
            return ColumnNullable::create(nested_function->create_serialize_column(),
                                          ColumnUInt8::create());
        }
        return nested_function->create_serialize_column();
    }

    DataTypePtr get_serialized_type() const override {
        if (native_state_is_nullable()) {
            return make_nullable(nested_function->get_serialized_type());
        }
        return nested_function->get_serialized_type();
    }

    void set_query_context(QueryContext* ctx) override {
        return nested_function->set_query_context(ctx);
    }

    bool is_blockable() const override { return nested_function->is_blockable(); }

    void set_version(const int version_) override {
        IAggregateFunctionHelper<Derived>::set_version(version_);
        nested_function->set_version(version_);
    }

    String get_name() const override { return "NullableV2(" + nested_function->get_name() + ")"; }

    DataTypePtr get_return_type() const override {
        return result_is_nullable ? make_nullable(nested_function->get_return_type())
                                  : nested_function->get_return_type();
    }

    void create(AggregateDataPtr __restrict place) const override {
        init(place, this->is_window_function);
        nested_function->create(nested_place(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        nested_function->destroy(nested_place(place));
    }
    void reset(AggregateDataPtr place) const override {
        init(place, this->is_window_function);
        nested_function->reset(nested_place(place));
    }

    bool is_trivial() const override { return false; }

    bool has_legacy_nullable_state_format() const override { return true; }

    AggregateFunctionPtr create_legacy_nullable_codec(
            AggregateFunctionPtr function) const override {
        return std::make_shared<AggregateFunctionLegacyNullableState<Derived>>(
                std::static_pointer_cast<Derived>(std::move(function)));
    }

    size_t size_of_data() const override { return prefix_size + nested_function->size_of_data(); }

    size_t align_of_data() const override {
        if (this->is_window_function && result_is_nullable) {
            return std::max(nested_function->align_of_data(), alignof(int32_t));
        } else {
            return nested_function->align_of_data();
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena& arena) const override {
        if (get_flag(rhs)) {
            set_flag(place);
            nested_function->merge(nested_place(place), nested_place(rhs), arena);
        } else if constexpr (supports_empty_state_byte_marker) {
            // State 2 is an empty contribution decoded from the new persistent marker.
            // Preserve it through old-schema compaction, whose function version can be 19.
            if (rhs[0] == 2 && !get_flag(place)) {
                place[0] = 2;
            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        bool flag = get_flag(place);
        if constexpr (supports_empty_state_byte_marker) {
            if (!flag &&
                (this->version >= USE_UNIFIED_NULLABLE_AGGREGATION_VERSION || place[0] == 2)) {
                // Legacy non-NULL bitmap payloads begin with a varint-length byte in [1,9].
                // Zero is a self-describing no-input state; a real empty bitmap stays [1,1,0].
                buf.write_char(0);
                return;
            }
        }
        if constexpr (result_is_nullable) {
            buf.write_binary(flag);
        }
        if (flag || !result_is_nullable) {
            nested_function->serialize(nested_place(place), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena& arena) const override {
        if constexpr (supports_empty_state_byte_marker) {
            // Accept the marker even with old metadata: new rows in an existing table and
            // compaction must retain the distinction without changing its physical String type.
            if (*buf.data() == 0) {
                buf.add_offset(1);
                place[0] = 2;
                return;
            }
        }
        bool flag = true;
        if constexpr (result_is_nullable) {
            buf.read_binary(flag);
        }
        if (flag) {
            set_flag(place);
            nested_function->deserialize(nested_place(place), buf, arena);
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        if (native_state_is_nullable()) {
            auto& nullable_col = assert_cast<ColumnNullable&>(*dst);
            auto& nested_col = nullable_col.get_nested_column();
            auto& null_map = nullable_col.get_null_map_data();
            MutableColumnPtr nested_col_ptr = nested_col.assert_mutable();

            null_map.resize(num_rows);
            uint8_t* __restrict null_map_data = null_map.data();
            for (size_t i = 0; i < num_rows; ++i) {
                null_map_data[i] = !get_flag(places[i] + offset);
            }
            nested_function->serialize_to_column(places, offset + prefix_size, nested_col_ptr,
                                                 num_rows);
        } else {
            nested_function->serialize_to_column(places, offset + prefix_size, dst, num_rows);
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena& arena) const override {
        const auto& column = assert_cast<const ColumnNullable&>(*columns[0]);
        const IColumn* nested_column = &column.get_nested_column();
        streaming_serialize(&nested_column, column.get_null_map_data().data(), dst, num_rows,
                            arena);
    }

protected:
    void streaming_serialize(const IColumn** columns, const UInt8* null_map, MutableColumnPtr& dst,
                             size_t num_rows, Arena& arena) const {
        if (num_rows == 0) {
            return;
        }
        MutableColumnPtr nested_dst;
        ColumnNullable* nullable_dst = nullptr;
        if (native_state_is_nullable()) {
            nullable_dst = &assert_cast<ColumnNullable&>(*dst);
            nested_dst = nullable_dst->get_nested_column().assert_mutable();
            auto& dst_null_map = nullable_dst->get_null_map_data();
            dst_null_map.assign(null_map, null_map + num_rows);
        } else {
            nested_dst = std::move(dst);
        }
        DEFER({
            if (!native_state_is_nullable()) {
                dst = std::move(nested_dst);
            }
        });
        if constexpr (requires(const NestFunction& function) {
                          function.streaming_agg_serialize_to_column_with_null_map(
                                  columns, nested_dst, num_rows, null_map);
                      }) {
            nested_function->streaming_agg_serialize_to_column_with_null_map(columns, nested_dst,
                                                                             num_rows, null_map);
            return;
        }
        const bool has_null =
                std::any_of(null_map, null_map + num_rows, [](UInt8 value) { return value != 0; });
        if (!has_null) {
            nested_function->streaming_agg_serialize_to_column(columns, nested_dst, num_rows,
                                                               arena);
            return;
        }
        // A NULL row has no contribution (not even an empty bitmap intersection).
        // Keep only one aligned state alive: wide/string/set states must not grow with batch size.
        Arena state_arena;
        Arena row_arena;
        AggregateDataPtr place = nullptr;
        for (size_t row = 0; row < num_rows; ++row) {
            if (native_state_is_nullable()) {
                if (null_map[row]) {
                    nested_dst->insert_default();
                    continue;
                }
            }
            if (place == nullptr) {
                // An entirely NULL native batch does not need even one temporary state.
                place = state_arena.aligned_alloc(nested_function->size_of_data(),
                                                  nested_function->align_of_data());
            }
            row_arena.clear();
            nested_function->create(place);
            DEFER({ nested_function->destroy(place); });
            if (!null_map[row]) {
                nested_function->add(place, columns, row, row_arena);
            }
            nested_function->serialize_without_key_to_column(place, *nested_dst);
        }
    }

public:
    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        if (native_state_is_nullable()) {
            auto& nullable_col = assert_cast<ColumnNullable&>(to);
            auto& nested_col = nullable_col.get_nested_column();
            auto& null_map = nullable_col.get_null_map_data();

            bool flag = get_flag(place);
            if (flag) {
                nested_function->serialize_without_key_to_column(nested_place(place), nested_col);
                null_map.push_back(0);
            } else {
                nested_col.insert_default();
                null_map.push_back(1);
            }
        } else {
            nested_function->serialize_without_key_to_column(nested_place(place), to);
        }
    }

    bool needs_deserialize_and_merge_scratch() const override {
        return nested_function->needs_deserialize_and_merge_scratch();
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   size_t num_rows) const override {
        merge_serialized<false>(places, offset, rhs, column, arena, num_rows);
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena& arena, size_t num_rows) const override {
        merge_serialized<true>(places, offset, rhs, column, arena, num_rows);
    }

private:
    template <bool selected>
    bool merge_native_serialized(const AggregateDataPtr* places, size_t offset,
                                 const ColumnNullable& nullable, Arena& arena, size_t rows) const {
        const auto& column = nullable.get_nested_column();
        const auto* null_map = nullable.get_null_map_data().data();
        const auto mark_valid = [places, offset](size_t row) { set_flag(places[row] + offset); };
        if constexpr (requires(const NestFunction& function) {
                          function.template deserialize_and_merge_vec_with_null_map<selected>(
                                  places, offset, column, null_map, arena, rows, mark_valid);
                      }) {
            // Update validity and native payload together, avoiding a second NULL check
            // and a separate validity pass over the destination states.
            nested_function->template deserialize_and_merge_vec_with_null_map<selected>(
                    places, offset + prefix_size, column, null_map, arena, rows, mark_valid);
            return true;
        }
        return false;
    }

    template <bool selected>
    void merge_null_serialized(const AggregateDataPtr* places, size_t offset, AggregateDataPtr rhs,
                               const ColumnNullable& nullable, Arena& arena,
                               size_t num_rows) const {
        const auto& nested_column = nullable.get_nested_column();
        const auto& null_map = nullable.get_null_map_data();
        // Merging an empty bitmap is not equivalent to ignoring a NULL-only partial:
        // INTERSECT({1,2}, empty) loses the intersection. Never pass such rows to nested.
        DorisVector<AggregateDataPtr> valid_places(num_rows, nullptr);
        bool has_selected = false;
        for (size_t i = 0; i < num_rows; ++i) {
            if (!null_map[i]) {
                if constexpr (selected) {
                    if (!places[i]) {
                        continue;
                    }
                }
                set_flag(places[i] + offset);
                valid_places[i] = places[i];
                has_selected = true;
            }
        }
        if (has_selected) {
            nested_function->deserialize_and_merge_vec_selected(valid_places.data(),
                                                                offset + prefix_size, rhs,
                                                                &nested_column, arena, num_rows);
        }
    }

    template <bool selected>
    void merge_serialized(const AggregateDataPtr* places, size_t offset, AggregateDataPtr rhs,
                          const IColumn* column, Arena& arena, size_t num_rows) const {
        if (num_rows == 0) {
            return;
        }
        if (native_state_is_nullable()) {
            const auto& nullable = assert_cast<const ColumnNullable&>(*column);
            const auto& nested_column = nullable.get_nested_column();
            if (merge_native_serialized<selected>(places, offset, nullable, arena, num_rows)) {
                return;
            }
            if (nullable.has_null()) {
                merge_null_serialized<selected>(places, offset, rhs, nullable, arena, num_rows);
                return;
            }
            column = &nested_column;
        }
        // Unmarked legacy columns represent every row as a valid state, too.
        for (size_t i = 0; i < num_rows; ++i) {
            if constexpr (selected) {
                if (!places[i]) {
                    continue;
                }
            }
            set_flag(places[i] + offset);
        }
        if constexpr (selected) {
            nested_function->deserialize_and_merge_vec_selected(places, offset + prefix_size, rhs,
                                                                column, arena, num_rows);
        } else {
            nested_function->deserialize_and_merge_vec(places, offset + prefix_size, rhs, column,
                                                       arena, num_rows);
        }
    }

public:
    void deserialize_from_column_row(AggregateDataPtr __restrict place, const IColumn& column,
                                     size_t row, Arena& arena) const override {
        DCHECK_LT(row, column.size());
        if (native_state_is_nullable()) {
            const auto& nullable_col = assert_cast<const ColumnNullable&>(column);
            if (!nullable_col.is_null_at(row)) {
                set_flag(place);
                nested_function->deserialize_from_column_row(
                        nested_place(place), nullable_col.get_nested_column(), row, arena);
            }
        } else {
            set_flag(place);
            nested_function->deserialize_from_column_row(nested_place(place), column, row, arena);
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena& arena) const override {
        DCHECK(end <= column.size() && begin <= end)
                << ", begin:" << begin << ", end:" << end << ", column.size():" << column.size();

        if (native_state_is_nullable()) {
            const auto& nullable_col = assert_cast<const ColumnNullable&>(column);
            const auto& nested_col = nullable_col.get_nested_column();
            const auto& null_map = nullable_col.get_null_map_data();

            for (size_t i = begin; i <= end; ++i) {
                if (!null_map[i]) {
                    set_flag(place);
                    nested_function->deserialize_and_merge_from_column_range(
                            nested_place(place), nested_col, i, i, arena);
                }
            }
        } else {
            set_flag(place);
            nested_function->deserialize_and_merge_from_column_range(nested_place(place), column,
                                                                     begin, end, arena);
        }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        if constexpr (result_is_nullable) {
            auto& to_concrete = assert_cast<ColumnNullable&, TypeCheckOnRelease::DISABLE>(to);
            if (get_flag(place)) {
                nested_function->insert_result_into(nested_place(place),
                                                    to_concrete.get_nested_column());
                to_concrete.get_null_map_data().push_back(0);
            } else {
                to_concrete.insert_default();
            }
        } else {
            nested_function->insert_result_into(nested_place(place), to);
        }
    }

    void check_result_column_type(const IColumn& to) const override {
        IAggregateFunction::check_result_column_type(to);
        if constexpr (result_is_nullable) {
            const auto* to_concrete = check_and_get_column<ColumnNullable>(to);
            if (UNLIKELY(to_concrete == nullptr)) {
                throw doris::Exception(Status::InternalError(
                        "Aggregate function {} result type check failed: Column type {} is not "
                        "ColumnNullable",
                        this->get_name(), to.get_name()));
            }
            nested_function->check_result_column_type(to_concrete->get_nested_column());
        } else {
            nested_function->check_result_column_type(to);
        }
    }
};

template <typename NestFuction, bool result_is_nullable>
class AggregateFunctionNullUnaryInlineV2 final
        : public AggregateFunctionNullBaseInlineV2<
                  NestFuction, result_is_nullable,
                  AggregateFunctionNullUnaryInlineV2<NestFuction, result_is_nullable>> {
public:
    AggregateFunctionNullUnaryInlineV2(
            IAggregateFunction* nested_function_, const DataTypes& arguments,
            bool is_window_function_,
            int be_exec_version = BeExecVersionManager::get_newest_version())
            : AggregateFunctionNullBaseInlineV2<
                      NestFuction, result_is_nullable,
                      AggregateFunctionNullUnaryInlineV2<NestFuction, result_is_nullable>>(
                      nested_function_, arguments, is_window_function_, be_exec_version) {}

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        const auto* column =
                assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(columns[0]);
        if (!column->is_null_at(row_num)) {
            this->set_flag(place);
            const IColumn* nested_column = &column->get_nested_column();
            this->nested_function->add(this->nested_place(place), &nested_column, row_num, arena);
        } else {
            this->update_null_count(place, true, this->is_window_function);
        }
    }

    void check_input_columns_type(const IColumn** columns) const override {
        IAggregateFunction::check_input_columns_type(columns);
        const auto* column = check_and_get_column<ColumnNullable>(*columns[0]);
        if (UNLIKELY(column == nullptr)) {
            throw doris::Exception(Status::InternalError(
                    "Aggregate function {} argument 0 type check failed: Column type {} is not "
                    "ColumnNullable",
                    this->get_name(), columns[0]->get_name()));
        }
        const IColumn* nested_column = &column->get_nested_column();
        this->nested_function->check_input_columns_type(&nested_column);
    }

    IAggregateFunction* transmit_to_stable() override {
        auto f = AggregateFunctionNullBaseInlineV2<
                         NestFuction, result_is_nullable,
                         AggregateFunctionNullUnaryInlineV2<NestFuction, result_is_nullable>>::
                         nested_function->transmit_to_stable();
        if (!f) {
            return nullptr;
        }
        return new AggregateFunctionNullUnaryInlineV2<
                typename FunctionStableTransfer<NestFuction>::FunctionStable, result_is_nullable>(
                f, IAggregateFunction::argument_types, this->is_window_function, this->version);
    }

    void add_batch(size_t batch_size, AggregateDataPtr* __restrict places, size_t place_offset,
                   const IColumn** columns, Arena& arena, bool agg_many) const override {
        const auto* column =
                assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(columns[0]);
        const IColumn* nested_column = &column->get_nested_column();
        if (column->has_null()) {
            const auto* __restrict null_map_data = column->get_null_map_data().data();
            for (int i = 0; i < batch_size; ++i) {
                if (!null_map_data[i]) {
                    AggregateDataPtr __restrict place = places[i] + place_offset;
                    this->set_flag(place);
                    this->nested_function->add(this->nested_place(place), &nested_column, i, arena);
                }
            }
        } else {
            if constexpr (result_is_nullable) {
                for (int i = 0; i < batch_size; ++i) {
                    AggregateDataPtr __restrict place = places[i] + place_offset;
                    place[0] |= 1;
                    this->nested_function->add(this->nested_place(place), &nested_column, i, arena);
                }
            } else {
                for (size_t i = 0; i < batch_size; ++i) {
                    this->set_flag(places[i] + place_offset);
                }
                this->nested_function->add_batch(batch_size, places,
                                                 place_offset + this->prefix_size, &nested_column,
                                                 arena, agg_many);
            }
        }
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena) const override {
        const auto* column =
                assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(columns[0]);
        bool has_null = column->has_null();

        if (has_null) {
            for (size_t i = 0; i < batch_size; ++i) {
                this->add(place, columns, i, arena);
            }
        } else if (batch_size > 0) {
            this->set_flag(place);
            const IColumn* nested_column = &column->get_nested_column();
            this->nested_function->add_batch_single_place(batch_size, this->nested_place(place),
                                                          &nested_column, arena);
        }
    }

    void add_batch_range(size_t batch_begin, size_t batch_end, AggregateDataPtr place,
                         const IColumn** columns, Arena& arena, bool has_null) override {
        const auto* column =
                assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(columns[0]);

        if (has_null) {
            for (size_t i = batch_begin; i <= batch_end; ++i) {
                this->add(place, columns, i, arena);
            }
        } else if (batch_begin <= batch_end) {
            this->set_flag(place);
            const IColumn* nested_column = &column->get_nested_column();
            this->nested_function->add_batch_range(batch_begin, batch_end,
                                                   this->nested_place(place), &nested_column, arena,
                                                   false);
        }
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena, UInt8* use_null_result,
                                UInt8* could_use_previous_result) const override {
        auto current_frame_start = std::max<int64_t>(frame_start, partition_start);
        auto current_frame_end = std::min<int64_t>(frame_end, partition_end);
        if (current_frame_start >= current_frame_end) {
            if (!*could_use_previous_result) {
                this->init_flag(place);
                *use_null_result = true;
                return;
            }
        } else {
            *use_null_result = false;
            *could_use_previous_result = true;
        }
        const auto* column =
                assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(columns[0]);
        bool has_null = column->has_null();
        if (has_null) {
            for (size_t i = current_frame_start; i < current_frame_end; ++i) {
                this->add(place, columns, i, arena);
            }
        } else {
            const IColumn* nested_column = &(column->get_nested_column());
            this->set_flag(place);
            this->nested_function->add_range_single_place(
                    partition_start, partition_end, frame_start, frame_end,
                    this->nested_place(place), &nested_column, arena, use_null_result,
                    could_use_previous_result);
        }
    }

    bool supported_incremental_mode() const override {
        return this->nested_function->supported_incremental_mode();
    }

    void execute_function_with_incremental(int64_t partition_start, int64_t partition_end,
                                           int64_t frame_start, int64_t frame_end,
                                           AggregateDataPtr place, const IColumn** columns,
                                           Arena& arena, bool previous_is_nul, bool end_is_nul,
                                           bool has_null, UInt8* use_null_result,
                                           UInt8* could_use_previous_result) const override {
        int64_t current_frame_start = std::max<int64_t>(frame_start, partition_start);
        int64_t current_frame_end = std::min<int64_t>(frame_end, partition_end);
        if (current_frame_start >= current_frame_end) {
            *use_null_result = true;
            this->init_flag(place);
            return;
        }

        DCHECK(is_column_nullable(*columns[0])) << columns[0]->get_name();
        const auto* column =
                assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(columns[0]);
        const IColumn* nested_column = &column->get_nested_column();

        if (!column->has_null()) {
            if (*could_use_previous_result) {
                this->nested_function->execute_function_with_incremental(
                        partition_start, partition_end, frame_start, frame_end,
                        this->nested_place(place), &nested_column, arena, previous_is_nul,
                        end_is_nul, false, use_null_result, could_use_previous_result);
            } else {
                this->nested_function->add_range_single_place(
                        partition_start, partition_end, frame_start, frame_end,
                        this->nested_place(place), &nested_column, arena, use_null_result,
                        could_use_previous_result);
            }
            this->set_flag(place);
            return;
        }

        const auto* __restrict null_map_data = column->get_null_map_data().data();
        if (*could_use_previous_result) {
            auto outcoming_pos = frame_start - 1;
            auto incoming_pos = frame_end - 1;
            bool is_previous_frame_start_null = false;
            if (outcoming_pos >= partition_start && outcoming_pos < partition_end &&
                null_map_data[outcoming_pos] == 1) {
                is_previous_frame_start_null = true;
                DCHECK_EQ(result_is_nullable, true);
                DCHECK_EQ(this->is_window_function, true);
                this->update_null_count(place, false, this->is_window_function);
            }
            bool is_current_frame_end_null = false;
            if (incoming_pos >= partition_start && incoming_pos < partition_end &&
                null_map_data[incoming_pos] == 1) {
                is_current_frame_end_null = true;
                DCHECK_EQ(result_is_nullable, true);
                DCHECK_EQ(this->is_window_function, true);
                this->update_null_count(place, true, this->is_window_function);
            }
            const IColumn* columns_tmp[2] {nested_column, &(*column->get_null_map_column_ptr())};
            this->nested_function->execute_function_with_incremental(
                    partition_start, partition_end, frame_start, frame_end,
                    this->nested_place(place), columns_tmp, arena, is_previous_frame_start_null,
                    is_current_frame_end_null, true, use_null_result, could_use_previous_result);
            DCHECK_EQ(result_is_nullable, true);
            DCHECK_EQ(this->is_window_function, true);
            if (current_frame_end - current_frame_start ==
                this->get_null_count(place, this->is_window_function)) {
                this->init_flag(place);
            } else {
                this->set_flag(place);
            }
        } else {
            this->add_range_single_place(partition_start, partition_end, frame_start, frame_end,
                                         place, columns, arena, use_null_result,
                                         could_use_previous_result);
        }
    }
};

template <typename NestFuction, bool result_is_nullable>
class AggregateFunctionNullVariadicInlineV2 final
        : public AggregateFunctionNullBaseInlineV2<
                  NestFuction, result_is_nullable,
                  AggregateFunctionNullVariadicInlineV2<NestFuction, result_is_nullable>> {
public:
    AggregateFunctionNullVariadicInlineV2(
            IAggregateFunction* nested_function_, const DataTypes& arguments,
            bool is_window_function_,
            int be_exec_version = BeExecVersionManager::get_newest_version())
            : AggregateFunctionNullBaseInlineV2<
                      NestFuction, result_is_nullable,
                      AggregateFunctionNullVariadicInlineV2<NestFuction, result_is_nullable>>(
                      nested_function_, arguments, is_window_function_, be_exec_version),
              number_of_arguments(arguments.size()) {
        if (number_of_arguments == 1) {
            throw Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "Logical error: single argument is passed to AggregateFunctionNullVariadic");
        }

        if (number_of_arguments > MAX_ARGS) {
            throw Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "Maximum number of arguments for aggregate function with Nullable types is {}",
                    size_t(MAX_ARGS));
        }

        for (size_t i = 0; i < number_of_arguments; ++i) {
            is_nullable[i] = arguments[i]->is_nullable();
        }
    }

    IAggregateFunction* transmit_to_stable() override {
        auto f = AggregateFunctionNullBaseInlineV2<
                         NestFuction, result_is_nullable,
                         AggregateFunctionNullVariadicInlineV2<NestFuction, result_is_nullable>>::
                         nested_function->transmit_to_stable();
        if (!f) {
            return nullptr;
        }
        return new AggregateFunctionNullVariadicInlineV2<
                typename FunctionStableTransfer<NestFuction>::FunctionStable, result_is_nullable>(
                f, IAggregateFunction::argument_types, this->is_window_function, this->version);
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        /// This container stores the columns we really pass to the nested function.
        std::vector<const IColumn*> nested_columns(number_of_arguments);

        for (size_t i = 0; i < number_of_arguments; ++i) {
            if (is_nullable[i]) {
                const auto& nullable_col =
                        assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(
                                *columns[i]);
                if (nullable_col.is_null_at(row_num)) {
                    /// If at least one column has a null value in the current row,
                    /// we don't process this row.
                    return;
                }
                nested_columns[i] = &nullable_col.get_nested_column();
            } else {
                nested_columns[i] = columns[i];
            }
        }

        this->set_flag(place);
        this->nested_function->add(this->nested_place(place), nested_columns.data(), row_num,
                                   arena);
    }

    void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                   const IColumn** columns, Arena& arena, bool /*agg_many*/) const override {
        add_batch_selected_impl(batch_size, columns, arena,
                                [&](size_t row) { return places[row] + place_offset; });
    }

    void add_batch_selected(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                            const IColumn** columns, Arena& arena) const override {
        add_batch_selected_impl(batch_size, columns, arena, [&](size_t row) {
            return places[row] == nullptr ? nullptr : places[row] + place_offset;
        });
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena) const override {
        add_batch_selected_impl(batch_size, columns, arena, [&](size_t) { return place; });
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           size_t num_rows, Arena& arena) const override {
        std::vector<const IColumn*> nested_columns(number_of_arguments);
        std::array<const ColumnNullable*, MAX_ARGS> nullable_columns {};
        NullMap null_map(num_rows, 0);
        for (size_t i = 0; i < number_of_arguments; ++i) {
            if (is_nullable[i]) {
                nullable_columns[i] = &assert_cast<const ColumnNullable&>(*columns[i]);
                nested_columns[i] = &nullable_columns[i]->get_nested_column();
                const auto& input_null_map = nullable_columns[i]->get_null_map_data();
                for (size_t row = 0; row < num_rows; ++row) {
                    null_map[row] |= input_null_map[row];
                }
            } else {
                nested_columns[i] = columns[i];
            }
        }
        Columns prepared_columns;
        if constexpr (requires(const NestFuction& function) {
                          function.requires_batch_add_for_streaming();
                      }) {
            prepared_columns = this->nested_function->prepare_batch_columns_for_streaming(
                    num_rows, nested_columns.data(), [&](size_t row) { return !null_map[row]; });
            if (!prepared_columns.empty()) {
                for (size_t i = 0; i < number_of_arguments; ++i) {
                    nested_columns[i] = prepared_columns[i].get();
                }
            }
        }
        this->streaming_serialize(nested_columns.data(), null_map.data(), dst, num_rows, arena);
    }

    void check_input_columns_type(const IColumn** columns) const override {
        IAggregateFunction::check_input_columns_type(columns);
        if constexpr (requires(NestFuction& function, const IColumn** input_columns) {
                          function.check_nullable_input_columns(input_columns);
                      }) {
            this->nested_function->check_nullable_input_columns(columns);
        }
        std::vector<const IColumn*> nested_columns(number_of_arguments);
        for (size_t i = 0; i < number_of_arguments; ++i) {
            if (is_nullable[i]) {
                const auto* nullable_col = check_and_get_column<ColumnNullable>(*columns[i]);
                if (UNLIKELY(nullable_col == nullptr)) {
                    throw doris::Exception(Status::InternalError(
                            "Aggregate function {} argument {} type check failed: Column type {} "
                            "is not ColumnNullable",
                            this->get_name(), i, columns[i]->get_name()));
                }
                nested_columns[i] = &nullable_col->get_nested_column();
            } else {
                nested_columns[i] = columns[i];
            }
        }
        this->nested_function->check_input_columns_type(nested_columns.data());
    }

private:
    template <typename GetPlace>
    void add_batch_selected_impl(size_t batch_size, const IColumn** columns, Arena& arena,
                                 GetPlace&& get_place) const {
        std::vector<const IColumn*> nested_columns(number_of_arguments);
        std::array<const ColumnNullable*, MAX_ARGS> nullable_columns {};
        for (size_t i = 0; i < number_of_arguments; ++i) {
            if (is_nullable[i]) {
                nullable_columns[i] =
                        &assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(
                                *columns[i]);
                nested_columns[i] = &nullable_columns[i]->get_nested_column();
            } else {
                nested_columns[i] = columns[i];
            }
        }

        DorisVector<AggregateDataPtr> nested_places(batch_size, nullptr);
        for (size_t row = 0; row < batch_size; ++row) {
            AggregateDataPtr place = get_place(row);
            if (place == nullptr) {
                continue;
            }
            bool has_null = false;
            for (size_t i = 0; i < number_of_arguments; ++i) {
                has_null |= nullable_columns[i] != nullptr && nullable_columns[i]->is_null_at(row);
            }
            if (!has_null) {
                this->set_flag(place);
                nested_places[row] = this->nested_place(place);
            }
        }
        this->nested_function->add_batch_selected(batch_size, nested_places.data(), 0,
                                                  nested_columns.data(), arena);
    }

    // The array length is fixed in the implementation of some aggregate functions.
    // Therefore we choose 256 as the appropriate maximum length limit.
    static const size_t MAX_ARGS = 256;
    size_t number_of_arguments = 0;
    std::array<char, MAX_ARGS>
            is_nullable; /// Plain array is better than std::vector due to one indirection less.
};
// Stable transfer changes the nested DISTINCT type while preserving the nullable
// wrapper. The typed legacy codec uses the same mapping as the nullable creator.
template <typename Nested, bool result_is_nullable>
struct FunctionStableTransfer<AggregateFunctionNullUnaryInlineV2<Nested, result_is_nullable>> {
    using FunctionStable = AggregateFunctionNullUnaryInlineV2<
            typename FunctionStableTransfer<Nested>::FunctionStable, result_is_nullable>;
};

template <typename Nested, bool result_is_nullable>
struct FunctionStableTransfer<AggregateFunctionNullVariadicInlineV2<Nested, result_is_nullable>> {
    using FunctionStable = AggregateFunctionNullVariadicInlineV2<
            typename FunctionStableTransfer<Nested>::FunctionStable, result_is_nullable>;
};
} // namespace doris
