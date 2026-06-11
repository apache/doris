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

#include <cstddef>
#include <cstdint>

#include "common/status.h"
#include "storage/index/index_query_context.h"
#include "storage/olap_common.h"

namespace doris {

class TabletSchema;

namespace segment_v2 {

class IndexProbeRecorder {
public:
    IndexProbeRecorder(OlapReaderStatistics* stats, const TabletSchema* tablet_schema,
                       int32_t segment_id);

    [[nodiscard]] bool enabled() const {
        return _stats != nullptr && _stats->collect_index_probe_events;
    }

    [[nodiscard]] static size_t probe_count(const IndexQueryContextPtr& context);
    [[nodiscard]] static IndexFallbackReason fallback_reason(const Status& status,
                                                             bool need_remaining);

    void record(ColumnId column_id, IndexProbeSource source, IndexProbeState state,
                IndexFallbackReason reason, int64_t input_rows, int64_t output_rows,
                int64_t index_id = -1, bool counts_toward_filter_stats = true) const;

    bool record_probes_since(const IndexQueryContextPtr& context, size_t first_probe,
                             IndexProbeSource source, IndexProbeState state,
                             IndexFallbackReason reason, int64_t input_rows,
                             int64_t output_rows) const;

private:
    OlapReaderStatistics* _stats = nullptr;
    const TabletSchema* _tablet_schema = nullptr;
    int32_t _segment_id = -1;
};

} // namespace segment_v2
} // namespace doris
