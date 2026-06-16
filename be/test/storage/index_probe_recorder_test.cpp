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
#include <string>

#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/index_storage_test_util.h"

namespace doris::segment_v2 {
namespace {

class ProbeMockInvertedIndexReader final : public InvertedIndexReader {
public:
    explicit ProbeMockInvertedIndexReader(const std::shared_ptr<TabletIndex>& index_meta)
            : InvertedIndexReader(index_meta.get(), nullptr) {}

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override {
        *iterator = std::make_unique<InvertedIndexIterator>();
        return Status::OK();
    }

    Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                 const Field& query_value, InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override {
        static_cast<void>(context);
        static_cast<void>(column_name);
        static_cast<void>(query_value);
        static_cast<void>(query_type);
        static_cast<void>(bit_map);
        static_cast<void>(analyzer_ctx);
        return Status::OK();
    }

    Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                     const Field& query_value, InvertedIndexQueryType query_type,
                     size_t* count) override {
        static_cast<void>(context);
        static_cast<void>(column_name);
        static_cast<void>(query_value);
        static_cast<void>(query_type);
        *count = 0;
        return Status::OK();
    }

    InvertedIndexReaderType type() override { return InvertedIndexReaderType::STRING_TYPE; }
};

InvertedIndexReaderPtr make_probe_reader(int64_t index_id) {
    auto index_meta = std::make_shared<TabletIndex>();
    TabletIndexPB index_pb;
    index_pb.set_index_id(index_id);
    index_pb.set_index_name("probe_index_" + std::to_string(index_id));
    index_pb.set_index_type(IndexType::INVERTED);
    index_meta->init_from_pb(index_pb);
    return std::make_shared<ProbeMockInvertedIndexReader>(index_meta);
}

TEST(IndexProbeRecorderTest, IteratorReadProbeUsesBoundColumnId) {
    OlapReaderStatistics stats;
    stats.collect_index_probe_events = true;

    auto query_context = std::make_shared<IndexQueryContext>();
    query_context->stats = &stats;

    InvertedIndexIterator iterator;
    iterator.bind_context(query_context, 3);
    auto reader = make_probe_reader(3003);
    iterator.record_read_probe(reader, false);
    iterator.record_read_probe(reader, true);

    ASSERT_EQ(query_context->index_read_probes.size(), 2);
    EXPECT_EQ(query_context->index_read_probes[0].column_id, 3);
    EXPECT_EQ(query_context->index_read_probes[0].index_id, 3003);
    EXPECT_FALSE(query_context->index_read_probes[0].is_null_bitmap);
    EXPECT_EQ(query_context->index_read_probes[1].column_id, 3);
    EXPECT_EQ(query_context->index_read_probes[1].index_id, 3003);
    EXPECT_TRUE(query_context->index_read_probes[1].is_null_bitmap);
}

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
    EXPECT_TRUE(stats.index_probe_events[0].counts_toward_filter_stats);
    EXPECT_FALSE(stats.index_probe_events[1].counts_toward_filter_stats);
    EXPECT_FALSE(stats.index_probe_events[2].counts_toward_filter_stats);
    EXPECT_EQ(stats.index_probe_events[1].input_rows, 10);
    EXPECT_EQ(stats.index_probe_events[1].output_rows, 4);
    EXPECT_EQ(stats.index_probe_events[2].input_rows, 10);
    EXPECT_EQ(stats.index_probe_events[2].output_rows, 4);

    for (const auto& event : stats.index_probe_events) {
        EXPECT_EQ(event.column_uid, 2);
        EXPECT_FALSE(event.variant_path.has_value());
        EXPECT_EQ(event.segment_id, 7);
        EXPECT_EQ(event.source, IndexProbeSource::EXPR_PUSHDOWN);
        EXPECT_EQ(event.state, IndexProbeState::APPLIED);
        EXPECT_EQ(event.reason, IndexFallbackReason::NONE);
    }

    index_storage_test::IndexReadResult result;
    result.stats = stats;
    index_storage_test::expect_index_probe_count(result,
                                                 index_storage_test::IndexProbeExpectation {
                                                         .source = IndexProbeSource::EXPR_PUSHDOWN,
                                                         .state = IndexProbeState::APPLIED,
                                                         .reason = IndexFallbackReason::NONE,
                                                         .column_uid = 2,
                                                         .variant_path = std::nullopt,
                                                         .index_id = 1001,
                                                         .segment_id = 7,
                                                         .counts_toward_filter_stats = true,
                                                         .input_rows = 10,
                                                         .output_rows = 4,
                                                         .filtered_rows = 6,
                                                 },
                                                 1);
    index_storage_test::expect_index_probe_count(result,
                                                 index_storage_test::IndexProbeExpectation {
                                                         .source = IndexProbeSource::EXPR_PUSHDOWN,
                                                         .state = IndexProbeState::APPLIED,
                                                         .reason = IndexFallbackReason::NONE,
                                                         .column_uid = 2,
                                                         .variant_path = std::nullopt,
                                                         .index_id = 1001,
                                                         .segment_id = 8,
                                                         .counts_toward_filter_stats = true,
                                                 },
                                                 0);
}

TEST(IndexProbeRecorderTest, FallbackReasonMapsStorageStatus) {
    EXPECT_EQ(IndexProbeRecorder::fallback_reason(
                      Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>("missing"), true),
              IndexFallbackReason::MISSING_INDEX);
    EXPECT_EQ(IndexProbeRecorder::fallback_reason(
                      Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>("bypass"), true),
              IndexFallbackReason::BYPASS);
    EXPECT_EQ(IndexProbeRecorder::fallback_reason(
                      Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>("no terms"), true),
              IndexFallbackReason::NO_TERMS);
    EXPECT_EQ(IndexProbeRecorder::fallback_reason(
                      Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>("no terms"), false),
              IndexFallbackReason::NOT_SUPPORTED);
    EXPECT_EQ(IndexProbeRecorder::fallback_reason(
                      Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>("corrupted"), true),
              IndexFallbackReason::CORRUPTED);
    EXPECT_EQ(IndexProbeRecorder::fallback_reason(
                      Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>("skipped"), true),
              IndexFallbackReason::EVALUATE_SKIPPED);
    EXPECT_EQ(IndexProbeRecorder::fallback_reason(
                      Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>("unsupported"), true),
              IndexFallbackReason::NOT_SUPPORTED);
}

TEST(IndexProbeRecorderTest, RecordsFallbackProbeReasonWithoutFilterStats) {
    index_storage_test::IndexTabletOptions options;
    options.text_columns = {index_storage_test::TextColumnSpec {.unique_id = 2, .name = "title"}};
    auto tablet_schema = index_storage_test::build_tablet_schema(options);

    OlapReaderStatistics stats;
    stats.collect_index_probe_events = true;

    auto query_context = std::make_shared<IndexQueryContext>();
    query_context->index_read_probes = {
            IndexReadProbe {.column_id = 1, .index_id = 1001, .is_null_bitmap = false},
            IndexReadProbe {.column_id = 1, .index_id = 2002, .is_null_bitmap = true},
    };

    const IndexProbeRecorder recorder(&stats, tablet_schema.get(), 9);
    ASSERT_TRUE(recorder.record_probes_since(query_context, 0, IndexProbeSource::COLUMN_PREDICATE,
                                             IndexProbeState::FALLBACK, IndexFallbackReason::BYPASS,
                                             12, 5));

    ASSERT_EQ(stats.index_probe_events.size(), 2);
    for (const auto& event : stats.index_probe_events) {
        EXPECT_EQ(event.column_uid, 2);
        EXPECT_EQ(event.segment_id, 9);
        EXPECT_EQ(event.source, IndexProbeSource::COLUMN_PREDICATE);
        EXPECT_EQ(event.state, IndexProbeState::FALLBACK);
        EXPECT_EQ(event.reason, IndexFallbackReason::BYPASS);
        EXPECT_FALSE(event.counts_toward_filter_stats);
        EXPECT_EQ(event.input_rows, 12);
        EXPECT_EQ(event.output_rows, 5);
        EXPECT_EQ(event.filtered_rows, 0);
    }
    EXPECT_EQ(stats.index_probe_events[0].index_id, 1001);
    EXPECT_EQ(stats.index_probe_events[1].index_id, 2002);

    index_storage_test::IndexReadResult result;
    result.stats = stats;
    index_storage_test::expect_index_probe_count(
            result,
            index_storage_test::IndexProbeExpectation {
                    .source = IndexProbeSource::COLUMN_PREDICATE,
                    .state = IndexProbeState::FALLBACK,
                    .reason = IndexFallbackReason::BYPASS,
                    .column_uid = 2,
                    .index_id = 1001,
                    .segment_id = 9,
                    .counts_toward_filter_stats = false,
                    .input_rows = 12,
                    .output_rows = 5,
                    .filtered_rows = 0,
            },
            1);
}

TEST(IndexProbeRecorderTest, RecordsSearchFunctionAndQueryDisabledEvents) {
    index_storage_test::IndexTabletOptions options;
    options.text_columns = {index_storage_test::TextColumnSpec {.unique_id = 2, .name = "title"}};
    auto tablet_schema = index_storage_test::build_tablet_schema(options);

    OlapReaderStatistics stats;
    stats.collect_index_probe_events = true;

    const IndexProbeRecorder recorder(&stats, tablet_schema.get(), 11);
    recorder.record(1, IndexProbeSource::SEARCH_FUNCTION, IndexProbeState::APPLIED,
                    IndexFallbackReason::NONE, 20, 8, 3003);
    recorder.record(1, IndexProbeSource::SEARCH_FUNCTION, IndexProbeState::NOT_ATTEMPTED,
                    IndexFallbackReason::QUERY_DISABLED, 20, 20, 3003);

    ASSERT_EQ(stats.index_probe_events.size(), 2);
    EXPECT_EQ(stats.index_probe_events[0].source, IndexProbeSource::SEARCH_FUNCTION);
    EXPECT_EQ(stats.index_probe_events[0].state, IndexProbeState::APPLIED);
    EXPECT_EQ(stats.index_probe_events[0].reason, IndexFallbackReason::NONE);
    EXPECT_TRUE(stats.index_probe_events[0].counts_toward_filter_stats);
    EXPECT_EQ(stats.index_probe_events[0].filtered_rows, 12);

    EXPECT_EQ(stats.index_probe_events[1].source, IndexProbeSource::SEARCH_FUNCTION);
    EXPECT_EQ(stats.index_probe_events[1].state, IndexProbeState::NOT_ATTEMPTED);
    EXPECT_EQ(stats.index_probe_events[1].reason, IndexFallbackReason::QUERY_DISABLED);
    EXPECT_FALSE(stats.index_probe_events[1].counts_toward_filter_stats);
    EXPECT_EQ(stats.index_probe_events[1].filtered_rows, 0);

    index_storage_test::IndexReadResult result;
    result.stats = stats;
    index_storage_test::expect_index_probe_count(
            result,
            index_storage_test::IndexProbeExpectation {
                    .source = IndexProbeSource::SEARCH_FUNCTION,
                    .state = IndexProbeState::APPLIED,
                    .reason = IndexFallbackReason::NONE,
                    .column_uid = 2,
                    .index_id = 3003,
                    .segment_id = 11,
                    .counts_toward_filter_stats = true,
                    .input_rows = 20,
                    .output_rows = 8,
                    .filtered_rows = 12,
            },
            1);
    index_storage_test::expect_index_probe_count(
            result,
            index_storage_test::IndexProbeExpectation {
                    .source = IndexProbeSource::SEARCH_FUNCTION,
                    .state = IndexProbeState::NOT_ATTEMPTED,
                    .reason = IndexFallbackReason::QUERY_DISABLED,
                    .column_uid = 2,
                    .index_id = 3003,
                    .segment_id = 11,
                    .counts_toward_filter_stats = false,
                    .filtered_rows = 0,
            },
            1);
}

} // namespace
} // namespace doris::segment_v2
