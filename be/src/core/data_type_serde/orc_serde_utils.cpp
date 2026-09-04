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

#include "core/data_type_serde/orc_serde_utils.h"

#include "common/cast_set.h"
#include "common/check.h"
#include "core/column/column_array.h"

namespace doris::orc_serde_utils {

size_t orc_decode_row_count(size_t rows, const std::vector<size_t>* selected_rows) {
    if (selected_rows == nullptr) {
        return rows;
    }
    return selected_rows->size();
}

size_t orc_source_row_at(size_t row, const std::vector<size_t>* selected_rows) {
    if (selected_rows == nullptr) {
        return row;
    }
    return (*selected_rows)[row];
}

bool orc_row_is_null(const ::orc::ColumnVectorBatch& batch, size_t row) {
    return batch.hasNulls && !batch.notNull[row];
}

Status round_orc_timestamp_to_microseconds(int64_t seconds, int64_t nanoseconds,
                                           RoundedOrcTimestamp* result) {
    constexpr int64_t NANOS_PER_SECOND = 1000000000;
    constexpr int64_t NANOS_PER_MICROSECOND = 1000;
    constexpr int64_t MICROS_PER_SECOND = 1000000;
    DORIS_CHECK(result != nullptr);
    DORIS_CHECK(nanoseconds >= 0 && nanoseconds < NANOS_PER_SECOND);
    // Doris stores six fractional digits, so use half-up rounding and carry 999999500ns into the
    // next second instead of silently truncating the ORC value.
    const auto rounded_microseconds =
            (nanoseconds + NANOS_PER_MICROSECOND / 2) / NANOS_PER_MICROSECOND;
    // Validate the carry here, but validate Doris' calendar range after timezone conversion:
    // a valid year-zero local timestamp may have a UTC epoch before year zero.
    if (__builtin_add_overflow(seconds, rounded_microseconds / MICROS_PER_SECOND,
                               &result->seconds)) {
        return Status::DataQualityError("ORC timestamp overflows after microsecond rounding");
    }
    result->microseconds = cast_set<uint64_t>(rounded_microseconds % MICROS_PER_SECOND);
    result->carry = rounded_microseconds >= MICROS_PER_SECOND;
    return Status::OK();
}

DecodedColumnView make_orc_decoded_view(const OrcDecodedColumnView& orc_view,
                                        DecodedValueKind value_kind) {
    DecodedColumnView view;
    view.value_kind = value_kind;
    view.row_count = cast_set<int64_t>(orc_decode_row_count(orc_view.rows, orc_view.selected_rows));
    view.timezone = orc_view.timezone;
    return view;
}

Status read_decoded_values(const DataTypeSerDe& serde, IColumn& column, DecodedColumnView* view) {
    DORIS_CHECK(view != nullptr);
    RETURN_IF_ERROR(serde.read_column_from_decoded_values(column, *view));
    return Status::OK();
}

void fill_orc_decoded_null_map(const ::orc::ColumnVectorBatch& batch, size_t rows,
                               const std::vector<size_t>* selected_rows, NullMap* null_map) {
    DORIS_CHECK(null_map != nullptr);
    if (!batch.hasNulls) {
        return;
    }
    const auto output_rows = orc_decode_row_count(rows, selected_rows);
    null_map->resize(output_rows);
    for (size_t row = 0; row < output_rows; ++row) {
        (*null_map)[row] = !batch.notNull[orc_source_row_at(row, selected_rows)];
    }
}

Status append_orc_offsets(ColumnArray::Offsets64& doris_offsets,
                          const ::orc::DataBuffer<int64_t>& orc_offsets, size_t rows,
                          size_t* element_size, const std::vector<size_t>* selected_rows,
                          std::vector<size_t>* element_selection) {
    DORIS_CHECK(element_size != nullptr);
    if (selected_rows != nullptr) {
        DORIS_CHECK(element_selection != nullptr);
        const auto prev_offset = doris_offsets.empty() ? 0 : doris_offsets.back();
        ColumnArray::Offset64 current_offset = prev_offset;
        element_selection->clear();
        for (size_t row = 0; row < selected_rows->size(); ++row) {
            const auto source_row = (*selected_rows)[row];
            DORIS_CHECK(source_row < rows);
            const auto begin_offset = orc_offsets[source_row];
            const auto end_offset = orc_offsets[source_row + 1];
            if (end_offset < begin_offset) {
                return Status::Corruption("Invalid ORC offsets");
            }
            const auto delta = static_cast<size_t>(end_offset - begin_offset);
            for (size_t element_idx = 0; element_idx < delta; ++element_idx) {
                element_selection->push_back(static_cast<size_t>(begin_offset) + element_idx);
            }
            current_offset += static_cast<ColumnArray::Offset64>(delta);
            doris_offsets.push_back(current_offset);
        }
        *element_size = element_selection->size();
        return Status::OK();
    }

    const auto prev_offset = doris_offsets.empty() ? 0 : doris_offsets.back();
    const auto base_offset = orc_offsets[0];
    for (size_t idx = 1; idx <= rows; ++idx) {
        const auto delta = orc_offsets[idx] - base_offset;
        if (delta < 0) {
            return Status::Corruption("Invalid ORC offsets");
        }
        doris_offsets.push_back(prev_offset + static_cast<ColumnArray::Offset64>(delta));
    }
    const auto total_delta = orc_offsets[rows] - base_offset;
    if (total_delta < 0) {
        return Status::Corruption("Invalid ORC offsets");
    }
    *element_size = static_cast<size_t>(total_delta);
    return Status::OK();
}

OrcDecodedColumnView make_child_orc_view(const OrcDecodedColumnView& parent_view,
                                         const ::orc::Type* file_type,
                                         const ::orc::Type* selected_type,
                                         const ::orc::ColumnVectorBatch* batch, size_t rows,
                                         const std::vector<size_t>* selected_rows) {
    OrcDecodedColumnView child_view = parent_view;
    child_view.file_type = file_type;
    child_view.selected_type = selected_type;
    child_view.batch = batch;
    child_view.rows = rows;
    child_view.selected_rows = selected_rows;
    return child_view;
}

Status read_orc_child_column(const DataTypeSerDeSPtr& child_serde, MutableColumnPtr& child_column,
                             const OrcDecodedColumnView& child_view) {
    DORIS_CHECK(child_serde != nullptr);
    RETURN_IF_ERROR(child_serde->read_column_from_orc(*child_column, child_view));
    return Status::OK();
}

} // namespace doris::orc_serde_utils
