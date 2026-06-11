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

#include <gtest/gtest.h>

#include <memory>

#include "testutil/index_storage_test_util.h"

namespace doris::segment_v2 {
namespace {

TEST(IndexProbeRecorderTest, RecordsNullBitmapAndDuplicateReadProbes) {
    index_storage_test::IndexTabletOptions options;
    options.text_columns = {index_storage_test::TextColumnSpec {.unique_id = 2, .name = "title"}};
    auto tablet_schema = index_storage_test::build_tablet_schema(options);

    OlapReaderStatistics stats;
    stats.collect_index_probe_events = true;

    auto query_context = std::make_shared<IndexQueryContext>();
    query_context->index_read_probes = {
            IndexReadProbe {.column_id = 1, .index_id = 1001, .is_null_bitmap = false},
            IndexReadProbe {.column_id = 1, .index_id = 2002, .is_null_bitmap = true},
            IndexReadProbe {.column_id = 1, .index_id = 1001, .is_null_bitmap = false},
    };

    const IndexProbeRecorder recorder(&stats, tablet_schema.get(), 7);
    ASSERT_TRUE(recorder.record_probes_since(query_context, 0, IndexProbeSource::EXPR_PUSHDOWN,
                                             IndexProbeState::APPLIED, IndexFallbackReason::NONE,
                                             10, 4));

    ASSERT_EQ(stats.index_probe_events.size(), 3);
    EXPECT_EQ(stats.index_probe_events[0].index_id, 1001);
    EXPECT_EQ(stats.index_probe_events[1].index_id, 2002);
    EXPECT_EQ(stats.index_probe_events[2].index_id, 1001);
    EXPECT_EQ(stats.index_probe_events[0].filtered_rows, 6);
    EXPECT_EQ(stats.index_probe_events[1].filtered_rows, 0);
    EXPECT_EQ(stats.index_probe_events[2].filtered_rows, 0);

    for (const auto& event : stats.index_probe_events) {
        EXPECT_EQ(event.column_uid, 2);
        EXPECT_FALSE(event.variant_path.has_value());
        EXPECT_EQ(event.segment_id, 7);
        EXPECT_EQ(event.source, IndexProbeSource::EXPR_PUSHDOWN);
        EXPECT_EQ(event.state, IndexProbeState::APPLIED);
        EXPECT_EQ(event.reason, IndexFallbackReason::NONE);
    }
}

} // namespace
} // namespace doris::segment_v2
