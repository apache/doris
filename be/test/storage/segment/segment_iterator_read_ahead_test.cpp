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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/config.h"
#include "common/status.h"
#include "storage/olap_common.h"
#include "storage/segment/column_read_ahead.h"
#include "storage/segment/column_read_ahead_resources.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/mock/mock_segment.h"
#include "storage/tablet/tablet_schema.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#define protected public
#include "storage/segment/segment_iterator.h"
#undef private
#undef protected
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace doris::segment_v2 {
namespace {

struct RecordedReadAheadCall {
    roaring::Roaring rowids;
    ColumnReadAheadRole role {ColumnReadAheadRole::EAGER};
    ColumnIterator::ReadPhase phase {ColumnIterator::ReadPhase::NORMAL};
    bool reverse {false};
    bool page_driven {false};
};

class RecordingColumnIterator final : public ColumnIterator {
public:
    Status seek_to_ordinal(ordinal_t) override { return Status::OK(); }
    ordinal_t get_current_ordinal() const override { return 0; }

    Status init_prefetcher(const SegmentPrefetchParams&) override {
        ++prefetcher_init_calls;
        return Status::OK();
    }

    void collect_prefetchers(std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>&,
                             PrefetcherInitMethod) override {
        ++prefetcher_collect_calls;
    }

    Status prepare_read_ahead(const ColumnReadAheadHint& hint) override {
        const auto& request = std::get<ScanReadAheadHint>(hint.request);
        RecordedReadAheadCall call {.rowids = {},
                                    .role = request.role,
                                    .phase = _read_phase,
                                    .reverse = request.reverse,
                                    .page_driven = true};
        call.rowids = *request.candidate_rows;
        calls.push_back(std::move(call));
        if (!_prepare_status.ok()) {
            return _prepare_status;
        }
        return Status::OK();
    }

    void set_prepare_status(Status status) { _prepare_status = std::move(status); }
    ReadPhase phase() const { return _read_phase; }

    std::vector<RecordedReadAheadCall> calls;
    size_t prefetcher_init_calls {0};
    size_t prefetcher_collect_calls {0};

private:
    Status _prepare_status;
};

TabletSchemaSPtr make_tablet_schema(size_t column_count) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    for (size_t index = 0; index < column_count; ++index) {
        auto* column = schema_pb.add_column();
        column->set_unique_id(cast_set<int32_t>(index));
        column->set_name("c" + std::to_string(index));
        column->set_type("INT");
        column->set_is_key(index == 0);
        column->set_is_nullable(false);
    }
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    return tablet_schema;
}

class HintOnlyFileReader final : public io::FileReader {
public:
    Status close() override { return Status::OK(); }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return 100; }
    bool closed() const override { return false; }
    int64_t mtime() const override { return 0; }

protected:
    Status read_at_impl(size_t, Slice, size_t*, const io::IOContext*) override {
        return Status::NotSupported("hint-only test reader");
    }

private:
    const io::Path _path {"hint-only"};
};

class SegmentIteratorReadAheadTest : public testing::Test {
protected:
    void SetUp() override {
        _tablet_schema = make_tablet_schema(3);
        _read_schema = std::make_shared<ReadSchema>(_tablet_schema->columns());
        ASSERT_TRUE(ThreadPoolBuilder("ColumnHintTest")
                            .set_min_threads(1)
                            .set_max_threads(1)
                            .build(&_pool)
                            .ok());
        ASSERT_TRUE(io::FileRangeReadScheduler::create(
                            {.max_bytes_per_query = 1024, .max_bytes_per_be = 1024}, _pool.get(),
                            &_scheduler)
                            .ok());
    }

    std::unique_ptr<SegmentIterator> make_iterator() {
        auto iterator =
                std::make_unique<SegmentIterator>(std::make_shared<MockSegment>(_tablet_schema),
                                                  _read_schema, StorageReadOptions(_stats));
        iterator->_read_ahead_resources = std::make_shared<ColumnReadAheadResources>(
                std::make_shared<HintOnlyFileReader>(), _scheduler.get(),
                _scheduler->create_context(), io::FileRangeReadIOContext {},
                ColumnReadAheadIOOptions {});
        iterator->_row_bitmap.addRange(0, 100);
        return iterator;
    }

    RecordingColumnIterator* set_column(SegmentIterator* iterator, ColumnId ordinal) {
        auto column = std::make_unique<RecordingColumnIterator>();
        auto* result = column.get();
        iterator->_column_iterators[ordinal] = std::move(column);
        return result;
    }

    TabletSchemaSPtr _tablet_schema;
    ReadSchemaSPtr _read_schema;
    OlapReaderStatistics _stats;
    std::unique_ptr<ThreadPool> _pool;
    std::unique_ptr<io::FileRangeReadScheduler> _scheduler;
};

TEST_F(SegmentIteratorReadAheadTest, SubmitsAllPlannableRolesBeforeDecoding) {
    auto iterator = make_iterator();
    auto* predicate = set_column(iterator.get(), 0);
    auto* common_expr = set_column(iterator.get(), 1);
    auto* output = set_column(iterator.get(), 2);
    iterator->_is_need_short_eval = true;
    iterator->_is_need_expr_eval = true;
    iterator->_predicate_ordinals = {0};
    iterator->_common_expr_ordinals = {1};
    iterator->_output_ordinals = {2};
    iterator->_opts.read_orderby_key_reverse = true;

    iterator->_prepare_scan_read_ahead();

    ASSERT_EQ(predicate->calls.size(), 1);
    EXPECT_EQ(predicate->calls[0].role, ColumnReadAheadRole::EAGER);
    EXPECT_EQ(predicate->calls[0].phase, ColumnIterator::ReadPhase::NORMAL);
    EXPECT_TRUE(predicate->calls[0].reverse);
    EXPECT_EQ(predicate->calls[0].rowids, iterator->_row_bitmap);
    EXPECT_TRUE(predicate->calls[0].page_driven);

    ASSERT_EQ(common_expr->calls.size(), 1);
    EXPECT_EQ(common_expr->calls[0].role, ColumnReadAheadRole::LAZY);
    EXPECT_EQ(common_expr->calls[0].phase, ColumnIterator::ReadPhase::NORMAL);

    ASSERT_EQ(output->calls.size(), 1);
    EXPECT_EQ(output->calls[0].role, ColumnReadAheadRole::LAZY);
    EXPECT_EQ(output->calls[0].phase, ColumnIterator::ReadPhase::NORMAL);
}

TEST_F(SegmentIteratorReadAheadTest, LeavesPrefetchMechanismSelectionToColumns) {
    const auto old_cloud_unique_id = config::cloud_unique_id;
    const auto old_enable_prefetch = config::enable_query_segment_file_cache_prefetch;
    Defer restore_config {[&] {
        config::cloud_unique_id = old_cloud_unique_id;
        config::enable_query_segment_file_cache_prefetch = old_enable_prefetch;
    }};
    config::cloud_unique_id = "SegmentIteratorReadAheadTest";
    config::enable_query_segment_file_cache_prefetch = true;
    auto iterator = make_iterator();
    auto* scalar = set_column(iterator.get(), 0);
    auto* complex = set_column(iterator.get(), 1);
    set_column(iterator.get(), 2);
    _tablet_schema->columns()[1]->set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    iterator->_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    iterator->_init_segment_prefetchers();
    EXPECT_EQ(scalar->prefetcher_init_calls, 1);
    EXPECT_EQ(complex->prefetcher_init_calls, 1);
    EXPECT_EQ(scalar->prefetcher_collect_calls, 1);
    EXPECT_EQ(complex->prefetcher_collect_calls, 1);
}

TEST_F(SegmentIteratorReadAheadTest, UsesEagerWindowForFirstCommonExpressionStage) {
    auto iterator = make_iterator();
    auto* common_expr = set_column(iterator.get(), 0);
    auto* output = set_column(iterator.get(), 1);
    set_column(iterator.get(), 2);
    iterator->_is_need_expr_eval = true;
    iterator->_common_expr_ordinals = {0};
    iterator->_output_ordinals = {1};

    iterator->_prepare_scan_read_ahead();

    ASSERT_EQ(common_expr->calls.size(), 1);
    ASSERT_EQ(output->calls.size(), 1);
    EXPECT_EQ(common_expr->calls[0].role, ColumnReadAheadRole::EAGER);
    EXPECT_EQ(output->calls[0].role, ColumnReadAheadRole::LAZY);
}

TEST_F(SegmentIteratorReadAheadTest, UsesEagerWindowWhenOutputHasNoDependency) {
    auto iterator = make_iterator();
    auto* first = set_column(iterator.get(), 0);
    auto* second = set_column(iterator.get(), 1);
    set_column(iterator.get(), 2);
    iterator->_output_ordinals = {0, 1};

    iterator->_prepare_scan_read_ahead();

    ASSERT_EQ(first->calls.size(), 1);
    ASSERT_EQ(second->calls.size(), 1);
    EXPECT_EQ(first->calls[0].role, ColumnReadAheadRole::EAGER);
    EXPECT_EQ(second->calls[0].role, ColumnReadAheadRole::EAGER);
}

TEST_F(SegmentIteratorReadAheadTest, PreparationFailureFallsBackAndKeepsOtherColumns) {
    auto iterator = make_iterator();
    auto* failed = set_column(iterator.get(), 0);
    auto* healthy = set_column(iterator.get(), 1);
    set_column(iterator.get(), 2);
    failed->set_prepare_status(Status::IOError("injected planning failure"));
    iterator->_output_ordinals = {0, 1};

    iterator->_prepare_scan_read_ahead();

    EXPECT_EQ(failed->calls.size(), 1);
    EXPECT_EQ(healthy->calls.size(), 1);
    EXPECT_EQ(failed->phase(), ColumnIterator::ReadPhase::NORMAL);
}

TEST_F(SegmentIteratorReadAheadTest, EmptySelectionAndDisabledReadAheadSkipPlanning) {
    auto iterator = make_iterator();
    auto* column = set_column(iterator.get(), 0);
    iterator->_output_ordinals = {0};
    iterator->_row_bitmap = roaring::Roaring {};
    iterator->_prepare_scan_read_ahead();
    EXPECT_TRUE(column->calls.empty());

    iterator->_row_bitmap.add(20);
    iterator->_read_ahead_resources.reset();
    iterator->_prepare_scan_read_ahead();
    EXPECT_TRUE(column->calls.empty());
}

} // namespace
} // namespace doris::segment_v2
