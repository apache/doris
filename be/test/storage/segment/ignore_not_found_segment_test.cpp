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

#include "common/config.h"
#include "common/status.h"
#include "json2pb/json_to_pb.h"
#include "runtime/exec_env.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/segment/lazy_init_segment_iterator.h"
#include "storage/segment/segment_loader.h"
#include "util/debug_points.h"

namespace doris {

class IgnoreNotFoundSegmentTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_ignore = config::ignore_not_found_segment;
        _saved_debug_points = config::enable_debug_points;
        config::enable_debug_points = true;

        // Set up a SegmentLoader for LazyInitSegmentIterator tests
        _saved_segment_loader = ExecEnv::GetInstance()->segment_loader();
        _segment_loader = new SegmentLoader(1024 * 1024, 100);
        ExecEnv::GetInstance()->set_segment_loader(_segment_loader);
    }

    void TearDown() override {
        DebugPoints::instance()->remove("BetaRowset::load_segment.return_not_found");
        DebugPoints::instance()->remove("BetaRowset::load_segment.return_io_error");
        config::ignore_not_found_segment = _saved_ignore;
        config::enable_debug_points = _saved_debug_points;

        ExecEnv::GetInstance()->set_segment_loader(_saved_segment_loader);
        delete _segment_loader;
        _segment_loader = nullptr;
    }

    BetaRowsetSharedPtr create_rowset(int num_segments) {
        auto schema = std::make_shared<TabletSchema>();
        TabletColumn col;
        col.set_name("c1");
        col.set_unique_id(0);
        col.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        col.set_length(4);
        col.set_is_key(true);
        col.set_is_nullable(false);
        schema->append_column(col);
        schema->_keys_type = DUP_KEYS;

        auto rsm = std::make_shared<RowsetMeta>();
        std::string json = R"({
            "rowset_id": 540081,
            "tablet_id": 10001,
            "partition_id": 10000,
            "tablet_schema_hash": 567997577,
            "rowset_type": "BETA_ROWSET",
            "rowset_state": "VISIBLE",
            "empty": false
        })";
        RowsetMetaPB pb;
        EXPECT_TRUE(json2pb::JsonToProtoMessage(json, &pb));
        pb.set_start_version(0);
        pb.set_end_version(1);
        pb.set_num_segments(num_segments);
        rsm->init_from_pb(pb);
        rsm->set_tablet_schema(schema);

        return std::make_shared<BetaRowset>(schema, rsm, "");
    }

    bool _saved_ignore = true;
    bool _saved_debug_points = false;
    SegmentLoader* _saved_segment_loader = nullptr;
    SegmentLoader* _segment_loader = nullptr;
};

// Test: BetaRowset::load_segments skips NOT_FOUND segments when config enabled
TEST_F(IgnoreNotFoundSegmentTest, BetaRowsetLoadSegmentsSkipsNotFound) {
    config::ignore_not_found_segment = true;
    auto rowset = create_rowset(3);

    DebugPoints::instance()->add("BetaRowset::load_segment.return_not_found");

    std::vector<segment_v2::SegmentSharedPtr> segments;
    auto st = rowset->load_segments(&segments);
    // All segments are "not found" but should be skipped, resulting in OK with empty segments
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(0, segments.size());
}

// Test: BetaRowset::load_segments fails on NOT_FOUND when config disabled
TEST_F(IgnoreNotFoundSegmentTest, BetaRowsetLoadSegmentsFailsWhenConfigDisabled) {
    config::ignore_not_found_segment = false;
    auto rowset = create_rowset(3);

    DebugPoints::instance()->add("BetaRowset::load_segment.return_not_found");

    std::vector<segment_v2::SegmentSharedPtr> segments;
    auto st = rowset->load_segments(&segments);
    ASSERT_TRUE(st.is<ErrorCode::NOT_FOUND>()) << st;
    ASSERT_EQ(0, segments.size());
}

// Test: BetaRowset::load_segments with range skips NOT_FOUND
TEST_F(IgnoreNotFoundSegmentTest, BetaRowsetLoadSegmentsRangeSkipsNotFound) {
    config::ignore_not_found_segment = true;
    auto rowset = create_rowset(5);

    DebugPoints::instance()->add("BetaRowset::load_segment.return_not_found");

    std::vector<segment_v2::SegmentSharedPtr> segments;
    auto st = rowset->load_segments(1, 4, &segments);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(0, segments.size());
}

// Test: SegmentLoader::load_segments skips NOT_FOUND segments when config enabled
TEST_F(IgnoreNotFoundSegmentTest, SegmentLoaderLoadSegmentsSkipsNotFound) {
    config::ignore_not_found_segment = true;
    auto rowset = create_rowset(3);

    DebugPoints::instance()->add("BetaRowset::load_segment.return_not_found");

    // Create a SegmentLoader with a small cache
    SegmentLoader loader(1024 * 1024, 100);
    SegmentCacheHandle handle;
    auto st = loader.load_segments(rowset, &handle, false);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(0, handle.get_segments().size());
    ASSERT_TRUE(handle.is_inited());
}

// Test: SegmentLoader::load_segments fails when config disabled
TEST_F(IgnoreNotFoundSegmentTest, SegmentLoaderLoadSegmentsFailsWhenConfigDisabled) {
    config::ignore_not_found_segment = false;
    auto rowset = create_rowset(3);

    DebugPoints::instance()->add("BetaRowset::load_segment.return_not_found");

    SegmentLoader loader(1024 * 1024, 100);
    SegmentCacheHandle handle;
    auto st = loader.load_segments(rowset, &handle, false);
    ASSERT_TRUE(st.is<ErrorCode::NOT_FOUND>()) << st;
}

// Test: SegmentLoader::load_segment (single) returns NOT_FOUND directly
// (single-segment load does not skip; it's the caller's responsibility)
TEST_F(IgnoreNotFoundSegmentTest, SegmentLoaderLoadSingleSegmentReturnsNotFound) {
    auto rowset = create_rowset(1);

    DebugPoints::instance()->add("BetaRowset::load_segment.return_not_found");

    SegmentLoader loader(1024 * 1024, 100);
    SegmentCacheHandle handle;
    auto st = loader.load_segment(rowset, 0, &handle, false);
    ASSERT_TRUE(st.is<ErrorCode::NOT_FOUND>()) << st;
}

// Test: LazyInitSegmentIterator returns EOF when segment not found
// (explicit init path - simulates VUnionIterator calling init() before next_batch())
TEST_F(IgnoreNotFoundSegmentTest, LazyInitIteratorReturnsEofOnNotFound) {
    config::ignore_not_found_segment = true;
    auto rowset = create_rowset(1);

    DebugPoints::instance()->add("BetaRowset::load_segment.return_not_found");

    auto schema = std::make_shared<Schema>(rowset->tablet_schema());
    StorageReadOptions opts;
    opts.tablet_schema = rowset->tablet_schema();

    auto iter =
            std::make_unique<segment_v2::LazyInitSegmentIterator>(rowset, 0, false, schema, opts);

    // Explicit init should succeed (segment skipped, inner iterator is null)
    auto st = iter->init(opts);
    ASSERT_TRUE(st.ok()) << st;

    // next_batch should return EOF since inner iterator is null.
    // This is the critical path: _need_lazy_init is already false because init() was called,
    // so the null check must be outside the UNLIKELY branch.
    Block block;
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.is<ErrorCode::END_OF_FILE>()) << st;
}

// Test: LazyInitSegmentIterator fails when config disabled
TEST_F(IgnoreNotFoundSegmentTest, LazyInitIteratorFailsWhenConfigDisabled) {
    config::ignore_not_found_segment = false;
    auto rowset = create_rowset(1);

    DebugPoints::instance()->add("BetaRowset::load_segment.return_not_found");

    auto schema = std::make_shared<Schema>(rowset->tablet_schema());
    StorageReadOptions opts;
    opts.tablet_schema = rowset->tablet_schema();

    auto iter =
            std::make_unique<segment_v2::LazyInitSegmentIterator>(rowset, 0, false, schema, opts);

    // init should fail with NOT_FOUND
    auto st = iter->init(opts);
    ASSERT_TRUE(st.is<ErrorCode::NOT_FOUND>()) << st;
}

// Test: LazyInitSegmentIterator next_batch path with lazy init (not pre-inited)
// (lazy path - simulates the case where next_batch() triggers init internally)
TEST_F(IgnoreNotFoundSegmentTest, LazyInitIteratorNextBatchLazyPath) {
    config::ignore_not_found_segment = true;
    auto rowset = create_rowset(1);

    DebugPoints::instance()->add("BetaRowset::load_segment.return_not_found");

    auto schema = std::make_shared<Schema>(rowset->tablet_schema());
    StorageReadOptions opts;
    opts.tablet_schema = rowset->tablet_schema();

    auto iter =
            std::make_unique<segment_v2::LazyInitSegmentIterator>(rowset, 0, false, schema, opts);

    // Don't call init() explicitly - let next_batch trigger lazy init
    Block block;
    auto st = iter->next_batch(&block);
    ASSERT_TRUE(st.is<ErrorCode::END_OF_FILE>()) << st;
}

// ==================== IO_ERROR tests ====================

// Test: BetaRowset::load_segments skips IO_ERROR segments when config enabled
TEST_F(IgnoreNotFoundSegmentTest, BetaRowsetLoadSegmentsSkipsIOError) {
    config::ignore_not_found_segment = true;
    auto rowset = create_rowset(3);

    DebugPoints::instance()->add("BetaRowset::load_segment.return_io_error");

    std::vector<segment_v2::SegmentSharedPtr> segments;
    auto st = rowset->load_segments(&segments);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(0, segments.size());
}

// Test: BetaRowset::load_segments fails on IO_ERROR when config disabled
TEST_F(IgnoreNotFoundSegmentTest, BetaRowsetLoadSegmentsFailsIOErrorWhenConfigDisabled) {
    config::ignore_not_found_segment = false;
    auto rowset = create_rowset(3);

    DebugPoints::instance()->add("BetaRowset::load_segment.return_io_error");

    std::vector<segment_v2::SegmentSharedPtr> segments;
    auto st = rowset->load_segments(&segments);
    ASSERT_TRUE(st.is<ErrorCode::IO_ERROR>()) << st;
}

// Test: SegmentLoader::load_segments skips IO_ERROR segments
TEST_F(IgnoreNotFoundSegmentTest, SegmentLoaderLoadSegmentsSkipsIOError) {
    config::ignore_not_found_segment = true;
    auto rowset = create_rowset(3);

    DebugPoints::instance()->add("BetaRowset::load_segment.return_io_error");

    SegmentLoader loader(1024 * 1024, 100);
    SegmentCacheHandle handle;
    auto st = loader.load_segments(rowset, &handle, false);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(0, handle.get_segments().size());
    ASSERT_TRUE(handle.is_inited());
}

// Test: LazyInitSegmentIterator returns EOF on IO_ERROR
TEST_F(IgnoreNotFoundSegmentTest, LazyInitIteratorReturnsEofOnIOError) {
    config::ignore_not_found_segment = true;
    auto rowset = create_rowset(1);

    DebugPoints::instance()->add("BetaRowset::load_segment.return_io_error");

    auto schema = std::make_shared<Schema>(rowset->tablet_schema());
    StorageReadOptions opts;
    opts.tablet_schema = rowset->tablet_schema();

    auto iter =
            std::make_unique<segment_v2::LazyInitSegmentIterator>(rowset, 0, false, schema, opts);

    auto st = iter->init(opts);
    ASSERT_TRUE(st.ok()) << st;

    Block block;
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.is<ErrorCode::END_OF_FILE>()) << st;
}

} // namespace doris
