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

#include "storage/index/index_probe_recorder.h"

#include <algorithm>
#include <optional>
#include <string>
#include <utility>

#include "common/check.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

IndexProbeRecorder::IndexProbeRecorder(OlapReaderStatistics* stats,
                                       const TabletSchema* tablet_schema, int32_t segment_id)
        : _stats(stats), _tablet_schema(tablet_schema), _segment_id(segment_id) {}

size_t IndexProbeRecorder::probe_count(const IndexQueryContextPtr& context) {
    return context == nullptr ? 0 : context->index_read_probes.size();
}

IndexFallbackReason IndexProbeRecorder::fallback_reason(const Status& status, bool need_remaining) {
    if (status.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>()) {
        return IndexFallbackReason::MISSING_INDEX;
    }
    if (status.is<ErrorCode::INVERTED_INDEX_BYPASS>()) {
        return IndexFallbackReason::BYPASS;
    }
    if (status.is<ErrorCode::INVERTED_INDEX_NO_TERMS>() && need_remaining) {
        return IndexFallbackReason::NO_TERMS;
    }
    if (status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) {
        return IndexFallbackReason::CORRUPTED;
    }
    if (status.is<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>()) {
        return IndexFallbackReason::EVALUATE_SKIPPED;
    }
    return IndexFallbackReason::NOT_SUPPORTED;
}

void IndexProbeRecorder::record(ColumnId column_id, IndexProbeSource source, IndexProbeState state,
                                IndexFallbackReason reason, int64_t input_rows, int64_t output_rows,
                                int64_t index_id) const {
    if (!enabled()) {
        return;
    }
    DORIS_CHECK(_tablet_schema != nullptr);
    DORIS_CHECK_LT(column_id, _tablet_schema->num_columns());

    const auto& column = _tablet_schema->column(column_id);
    int32_t column_uid = column.unique_id();
    if (column_uid < 0) {
        column_uid = column.parent_unique_id();
    }

    std::optional<std::string> variant_path;
    if (column.has_path_info()) {
        variant_path = column.path_info_ptr()->copy_pop_front().get_path();
    }

    _stats->index_probe_events.push_back(IndexProbeEvent {
            .column_uid = column_uid,
            .variant_path = std::move(variant_path),
            .index_id = index_id,
            .segment_id = _segment_id,
            .storage_format = _tablet_schema->get_inverted_index_storage_format(),
            .source = source,
            .state = state,
            .reason = reason,
            .input_rows = input_rows,
            .output_rows = output_rows,
            .filtered_rows = std::max<int64_t>(0, input_rows - output_rows),
    });
}

bool IndexProbeRecorder::record_probes_since(const IndexQueryContextPtr& context,
                                             size_t first_probe, IndexProbeSource source,
                                             IndexProbeState state, IndexFallbackReason reason,
                                             int64_t input_rows, int64_t output_rows) const {
    if (!enabled() || context == nullptr || first_probe >= context->index_read_probes.size()) {
        return false;
    }

    size_t result_probe = first_probe;
    for (size_t probe_idx = first_probe; probe_idx < context->index_read_probes.size();
         ++probe_idx) {
        if (!context->index_read_probes[probe_idx].is_null_bitmap) {
            result_probe = probe_idx;
            break;
        }
    }

    for (size_t probe_idx = first_probe; probe_idx < context->index_read_probes.size();
         ++probe_idx) {
        const auto& probe = context->index_read_probes[probe_idx];
        const auto event_input_rows = probe_idx == result_probe ? input_rows : 0;
        const auto event_output_rows = probe_idx == result_probe ? output_rows : 0;
        record(probe.column_id, source, state, reason, event_input_rows, event_output_rows,
               probe.index_id);
    }
    return true;
}

} // namespace doris::segment_v2
