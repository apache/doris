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

#include <memory>

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_distinct.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

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

    static void init_flag(AggregateDataPtr __restrict place) noexcept {
        if constexpr (result_is_nullable) {
            place[0] = false;
        }
    }

    static void set_flag(AggregateDataPtr __restrict place) noexcept {
        if constexpr (result_is_nullable) {
            place[0] = true;
        }
    }

    static bool get_flag(ConstAggregateDataPtr __restrict place) noexcept {
        return result_is_nullable ? place[0] : true;
    }

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
                                      const DataTypes& arguments, bool is_window_function_)
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
            prefix_size = 0;
        }
    }

    MutableColumnPtr create_serialize_column() const override {
        if constexpr (result_is_nullable) {
            return ColumnNullable::create(nested_function->create_serialize_column(),
                                          ColumnUInt8::create());
        }
        return nested_function->create_serialize_column();
    }

    DataTypePtr get_serialized_type() const override {
        if constexpr (result_is_nullable) {
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
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        bool flag = get_flag(place);
        if constexpr (result_is_nullable) {
            buf.write_binary(flag);
        }
        if (flag) {
            nested_function->serialize(nested_place(place), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena& arena) const override {
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
        if constexpr (result_is_nullable) {
            auto& nullable_col = assert_cast<ColumnNullable&>(*dst);
            auto& nested_col = nullable_col.get_nested_column();
            auto& null_map = nullable_col.get_null_map_data();
            MutableColumnPtr nested_col_ptr = nested_col.assume_mutable();

            null_map.resize(num_rows);
            uint8_t* __restrict null_map_data = null_map.data();
            for (size_t i = 0; i < num_rows; ++i) {
                null_map_data[i] = !get_flag(places[i] + offset);
            }
            nested_function->serialize_to_column(places, offset + prefix_size, nested_col_ptr,
                                                 num_rows);
        } else {
            nested_function->serialize_to_column(places, offset, dst, num_rows);
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena& arena) const override {
        const auto* src_nullable_col = assert_cast<const ColumnNullable*>(columns[0]);
        const auto* __restrict src_null_map_data = src_nullable_col->get_null_map_data().data();

        size_t nested_size = nested_function->size_of_data();
        std::vector<AggregateDataPtr> nested_places(num_rows);
        std::vector<char> places_data(num_rows * nested_size);
        for (size_t i = 0; i < num_rows; ++i) {
            nested_places[i] = places_data.data() + i * nested_size;
        }

        if (!nested_function->is_trivial()) {
            for (int i = 0; i < num_rows; ++i) {
                try {
                    nested_function->create(nested_places[i]);
                } catch (...) {
                    for (int j = 0; j < i; ++j) {
                        nested_function->destroy(nested_places[j]);
                    }
                    throw;
                }
            }
        }
        Defer destroy_places = {[&]() {
            if (!nested_function->is_trivial()) {
                for (int i = 0; i < num_rows; ++i) {
                    nested_function->destroy(nested_places[i]);
                }
            }
        }};
        const IColumn* src_nested_column =
                src_nullable_col->get_nested_column().assume_mutable().get();
        if (src_nullable_col->has_null()) {
            for (size_t i = 0; i < num_rows; ++i) {
                if (!src_null_map_data[i]) {
                    nested_function->add(nested_places[i], &src_nested_column, i, arena);
                }
            }
        } else {
            nested_function->add_batch(num_rows, nested_places.data(), 0, &src_nested_column, arena,
                                       false);
        }

        if constexpr (result_is_nullable) {
            auto& dst_nullable_col = assert_cast<ColumnNullable&>(*dst);
            MutableColumnPtr nested_col_ptr = dst_nullable_col.get_nested_column().assume_mutable();
            dst_nullable_col.get_null_map_column().insert_range_from(
                    src_nullable_col->get_null_map_column(), 0, num_rows);
            nested_function->serialize_to_column(nested_places, 0, nested_col_ptr, num_rows);
        } else {
            nested_function->serialize_to_column(nested_places, 0, dst, num_rows);
        }
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        if constexpr (result_is_nullable) {
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

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   const size_t num_rows) const override {
        if constexpr (result_is_nullable) {
            const auto& nullable_col = assert_cast<const ColumnNullable&>(*column);
            const auto& nested_col = nullable_col.get_nested_column();
            const auto* __restrict null_map_data = nullable_col.get_null_map_data().data();

            for (size_t i = 0; i < num_rows; ++i) {
                *(places[i] + offset) |= (!null_map_data[i]);
            }
            nested_function->deserialize_and_merge_vec(places, offset + prefix_size, rhs,
                                                       &nested_col, arena, num_rows);
        } else {
            this->nested_function->deserialize_and_merge_vec(places, offset, rhs, column, arena,
                                                             num_rows);
        }
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena& arena, const size_t num_rows) const override {
        if constexpr (result_is_nullable) {
            const auto& nullable_col = assert_cast<const ColumnNullable&>(*column);
            const auto& nested_col = nullable_col.get_nested_column();
            const auto* __restrict null_map_data = nullable_col.get_null_map_data().data();

            for (size_t i = 0; i < num_rows; ++i) {
                if (places[i]) {
                    *(places[i] + offset) |= (!null_map_data[i]);
                }
            }
            nested_function->deserialize_and_merge_vec_selected(places, offset + prefix_size, rhs,
                                                                &nested_col, arena, num_rows);
        } else {
            this->nested_function->deserialize_and_merge_vec_selected(places, offset, rhs, column,
                                                                      arena, num_rows);
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena& arena) const override {
        DCHECK(end <= column.size() && begin <= end)
                << ", begin:" << begin << ", end:" << end << ", column.size():" << column.size();

        if constexpr (result_is_nullable) {
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
            nested_function->deserialize_and_merge_from_column_range(place, column, begin, end,
                                                                     arena);
        }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        if constexpr (result_is_nullable) {
            auto& to_concrete = assert_cast<ColumnNullable&>(to);
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
};

template <typename NestFuction, bool result_is_nullable>
class AggregateFunctionNullUnaryInlineV2 final
        : public AggregateFunctionNullBaseInlineV2<
                  NestFuction, result_is_nullable,
                  AggregateFunctionNullUnaryInlineV2<NestFuction, result_is_nullable>> {
public:
    AggregateFunctionNullUnaryInlineV2(IAggregateFunction* nested_function_,
                                       const DataTypes& arguments, bool is_window_function_)
            : AggregateFunctionNullBaseInlineV2<
                      NestFuction, result_is_nullable,
                      AggregateFunctionNullUnaryInlineV2<NestFuction, result_is_nullable>>(
                      nested_function_, arguments, is_window_function_) {}

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
                f, IAggregateFunction::argument_types, this->is_window_function);
    }

    void add_batch(size_t batch_size, AggregateDataPtr* __restrict places, size_t place_offset,
                   const IColumn** columns, Arena& arena, bool agg_many) const override {
        const auto* column = assert_cast<const ColumnNullable*>(columns[0]);
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
                this->nested_function->add_batch(batch_size, places, place_offset, &nested_column,
                                                 arena, agg_many);
            }
        }
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena) const override {
        const auto* column = assert_cast<const ColumnNullable*>(columns[0]);
        bool has_null = column->has_null();

        if (has_null) {
            for (size_t i = 0; i < batch_size; ++i) {
                this->add(place, columns, i, arena);
            }
        } else {
            this->set_flag(place);
            const IColumn* nested_column = &column->get_nested_column();
            this->nested_function->add_batch_single_place(batch_size, this->nested_place(place),
                                                          &nested_column, arena);
        }
    }

    void add_batch_range(size_t batch_begin, size_t batch_end, AggregateDataPtr place,
                         const IColumn** columns, Arena& arena, bool has_null) override {
        const auto* column = assert_cast<const ColumnNullable*>(columns[0]);

        if (has_null) {
            for (size_t i = batch_begin; i <= batch_end; ++i) {
                this->add(place, columns, i, arena);
            }
        } else {
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
        const auto* column = assert_cast<const ColumnNullable*>(columns[0]);
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

        DCHECK(columns[0]->is_nullable()) << columns[0]->get_name();
        const auto* column = assert_cast<const ColumnNullable*>(columns[0]);
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

} // namespace doris::vectorized

#include "common/compile_check_end.h"
