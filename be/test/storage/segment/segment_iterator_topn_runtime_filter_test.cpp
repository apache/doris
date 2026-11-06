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

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exec/pipeline/thrift_builder.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_predicate.h"
#include "runtime/thread_context.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/row_cursor.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"
#include "storage/segment/test_segment_writer.h"
#include "storage/tablet/tablet_schema_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris::segment_v2 {
namespace {
constexpr auto kTestDir = "./ut_dir/topn_runtime_ranges";
constexpr int kRows = 131072;
constexpr int kBound = kRows / 2 - 1;

struct ScanResult {
    std::vector<int32_t> ids;
    int64_t raw_rows = 0;
    int64_t pages = 0;
    int64_t range_filtered = 0;
};
} // namespace

class SegmentIteratorTopNRuntimeFilterTest : public testing::Test {
protected:
    void SetUp() override {
        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->delete_directory(kTestDir).ok());
        ASSERT_TRUE(fs->create_directory(kTestDir).ok());
        _tablet_schema = std::make_shared<TabletSchema>();
        _tablet_schema->append_column(*create_int_key(0, false));
        _tablet_schema->set_storage_page_size(4096);
        _schema = std::make_shared<ReadSchema>(_tablet_schema->columns());
        _slot._id = 0;
        _slot._col_name = _tablet_schema->column(0).name();
        _slot._type = std::make_shared<DataTypeInt32>();
        ASSERT_NO_FATAL_FAILURE(write_segment());
    }

    void TearDown() override {
        _reader.reset();
        _segment.reset();
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    void write_segment() {
        const auto path = std::string(kTestDir) + "/segment_7.dat";
        auto fs = io::global_local_filesystem();
        io::FileWriterPtr file;
        ASSERT_TRUE(fs->create_file(path, &file).ok());
        VerticalSegmentWriterOptions options;
        options.num_rows_per_block = 1024;
        TestVerticalSegmentWriter writer(file.get(), 7, _tablet_schema, nullptr, nullptr, options,
                                         nullptr);
        ASSERT_TRUE(writer.init().ok());
        RowCursor row;
        ASSERT_TRUE(row.init_scan_key(_tablet_schema, {Field::create_field<TYPE_INT>(0)}).ok());
        for (int rid = 0; rid < kRows; ++rid) {
            row.mutable_field(0) = Field::create_field<TYPE_INT>(rid);
            ASSERT_TRUE(writer.append_row(row).ok());
        }
        uint64_t index_size = 0;
        uint64_t file_size = 0;
        ASSERT_TRUE(writer.finalize_columns(&index_size).ok());
        ASSERT_TRUE(writer.finalize_footer(&file_size).ok());
        ASSERT_TRUE(file->close().ok());
        ASSERT_TRUE(Segment::open(fs, path, 100, 7, RowsetId {.version = 1}, _tablet_schema,
                                  io::FileReaderOptions {}, &_segment)
                            .ok());
    }

    void start(bool reverse = false, const Field& initial = Field(), bool with_hole = false,
               int batch_rows = 1024) {
        _reader.reset();
        _stats = OlapReaderStatistics {};
        auto target = TRuntimeFilterDescBuilder::get_default_expr();
        target.nodes[0].__set_type(create_type_desc(TYPE_INT));
        TTopnFilterDesc desc;
        desc.__set_source_node_id(10);
        desc.__set_is_asc(!reverse);
        desc.__set_null_first(false);
        desc.__set_target_node_id_to_target_expr({{20, target}});
        _predicate = std::make_unique<RuntimePredicate>(desc);
        ASSERT_TRUE(_predicate->init_target(20, {{0, &_slot}}, 0).ok());
        if (!initial.is_null()) {
            ASSERT_TRUE(_predicate->update(initial).ok());
        }
        StorageReadOptions options(_stats);
        options.runtime_state = &_state;
        options.io_ctx.reader_type = ReaderType::READER_QUERY;
        options.block_row_max = batch_rows;
        options.read_orderby_key_reverse = reverse;
        if (with_hole) {
            options.row_ranges.add({0, 2048});
            options.row_ranges.add({4096, kRows});
        }
        options.column_predicates = {_predicate->get_predicate(20)};
        auto block_predicate = AndBlockColumnPredicate::create_shared();
        block_predicate->add_column_predicate(
                SingleColumnBlockPredicate::create_unique(options.column_predicates.front()));
        options.col_id_to_predicates.emplace(0, std::move(block_predicate));
        ASSERT_TRUE(_segment->new_iterator(_schema, options, &_reader).ok());
        _block = _schema->create_read_block();
    }

    Status read(std::vector<int32_t>* ids) {
        _block.clear_column_data();
        RETURN_IF_ERROR(_reader->next_batch(&_block));
        const auto& column = assert_cast<const ColumnInt32&>(*_block.get_by_position(0).column);
        ids->insert(ids->end(), column.get_data().begin(), column.get_data().end());
        return Status::OK();
    }

    void drain(std::vector<int32_t>* ids) {
        while (true) {
            const auto status = read(ids);
            if (status.is<ErrorCode::END_OF_FILE>()) {
                return;
            }
            ASSERT_TRUE(status.ok()) << status;
        }
    }

    void late_scan(bool reverse, bool dynamic, ScanResult* result, int batch_rows = 1024) {
        ASSERT_NO_FATAL_FAILURE(start(reverse, Field(), false, batch_rows));
        ASSERT_TRUE(read(&result->ids).ok());
        ASSERT_EQ(batch_rows, result->ids.size());
        auto* iterator = assert_cast<SegmentIterator*>(_reader.get());
        if (!dynamic) {
            // Preserve the old behavior as a control: current row predicates still update.
            iterator->_topn_runtime_filters.clear();
        }
        ASSERT_TRUE(_predicate->update(Field::create_field<TYPE_INT>(reverse ? kBound + 1 : kBound))
                            .ok());
        ASSERT_NO_FATAL_FAILURE(drain(&result->ids));
        result->raw_rows = _stats.raw_rows_read;
        result->pages = _stats.total_pages_num;
        result->range_filtered = _stats.rows_stats_rp_filtered;
    }

    void expect_same_scan_result(const ScanResult& baseline, const ScanResult& dynamic) {
        EXPECT_EQ(dynamic.ids, baseline.ids);
        EXPECT_EQ(kRows / 2, dynamic.ids.size());
        EXPECT_EQ(kRows, baseline.raw_rows);
        EXPECT_LT(dynamic.raw_rows, baseline.raw_rows);
        EXPECT_LT(dynamic.pages, baseline.pages);
        EXPECT_EQ(kRows - dynamic.raw_rows, dynamic.range_filtered);
        const std::set<int32_t> unique(dynamic.ids.begin(), dynamic.ids.end());
        EXPECT_EQ(unique.size(), dynamic.ids.size());
    }

    void check_late_scan(bool reverse) {
        ScanResult baseline;
        ScanResult dynamic;
        ASSERT_NO_FATAL_FAILURE(late_scan(reverse, false, &baseline));
        ASSERT_NO_FATAL_FAILURE(late_scan(reverse, true, &dynamic));
        expect_same_scan_result(baseline, dynamic);
        RecordProperty(reverse ? "reverse_raw_rows" : "forward_raw_rows",
                       std::to_string(dynamic.raw_rows));
        RecordProperty(reverse ? "reverse_pages" : "forward_pages", std::to_string(dynamic.pages));
        RecordProperty("baseline_raw_rows", std::to_string(baseline.raw_rows));
        RecordProperty("baseline_pages", std::to_string(baseline.pages));
    }

    TabletSchemaSPtr _tablet_schema;
    ReadSchemaSPtr _schema;
    std::shared_ptr<Segment> _segment;
    std::unique_ptr<RowwiseIterator> _reader;
    std::unique_ptr<RuntimePredicate> _predicate;
    SlotDescriptor _slot;
    OlapReaderStatistics _stats;
    MockRuntimeState _state;
    Block _block;
};

TEST_F(SegmentIteratorTopNRuntimeFilterTest, LateArrivalSkipsPagesAndMatchesRowFiltering) {
    ASSERT_NO_FATAL_FAILURE(check_late_scan(false));
    ASSERT_NO_FATAL_FAILURE(check_late_scan(true));
}

TEST_F(SegmentIteratorTopNRuntimeFilterTest, RebuildingRefreshesCachedBitmapCardinality) {
    for (const bool reverse : {false, true}) {
        SCOPED_TRACE(reverse);
        ASSERT_NO_FATAL_FAILURE(start(reverse));
        std::vector<int32_t> ids;
        ASSERT_TRUE(read(&ids).ok());
        const auto consumed = ids.size();
        auto* iterator = assert_cast<SegmentIterator*>(_reader.get());
        EXPECT_EQ(kRows, iterator->_row_bitmap_cardinality);

        ASSERT_TRUE(_predicate->update(Field::create_field<TYPE_INT>(reverse ? kBound + 1 : kBound))
                            .ok());
        ASSERT_TRUE(iterator->_update_topn_runtime_filter_ranges().ok());
        EXPECT_EQ(kRows / 2 - consumed, iterator->_row_bitmap_cardinality);
        EXPECT_EQ(iterator->_row_bitmap.cardinality(), iterator->_row_bitmap_cardinality);
        ASSERT_TRUE(read(&ids).ok());
        EXPECT_EQ(2 * consumed, ids.size());
    }
}

TEST_F(SegmentIteratorTopNRuntimeFilterTest, RebuildingInsideAPageKeepsOnlyItsUnreadRows) {
    for (const bool reverse : {false, true}) {
        ScanResult result;
        ASSERT_NO_FATAL_FAILURE(late_scan(reverse, true, &result, 127));
        const std::set<int32_t> unique(result.ids.begin(), result.ids.end());
        EXPECT_EQ(kRows / 2, result.ids.size());
        EXPECT_EQ(unique.size(), result.ids.size());
        EXPECT_EQ(reverse ? kBound + 1 : 0, *unique.begin());
        EXPECT_EQ(reverse ? kRows - 1 : kBound, *unique.rbegin());
    }
}

TEST_F(SegmentIteratorTopNRuntimeFilterTest, TighteningWaitsUntilRawRowThresholdIsExceeded) {
    ASSERT_NO_FATAL_FAILURE(start(false, Field::create_field<TYPE_INT>(kRows)));
    std::vector<int32_t> ids;
    for (int batch = 0; batch < 40; ++batch) {
        ASSERT_TRUE(read(&ids).ok());
    }
    ASSERT_EQ(40960, _stats.raw_rows_read);
    ASSERT_TRUE(_predicate->update(Field::create_field<TYPE_INT>(kBound)).ok());
    ASSERT_TRUE(read(&ids).ok());
    EXPECT_EQ(0, _stats.rows_stats_rp_filtered);
    ASSERT_TRUE(read(&ids).ok());
    EXPECT_GT(_stats.rows_stats_rp_filtered, 0);
    ASSERT_NO_FATAL_FAILURE(drain(&ids));
    EXPECT_EQ(kRows / 2, ids.size());
    EXPECT_EQ(std::set<int32_t>(ids.begin(), ids.end()).size(), ids.size());
}

TEST_F(SegmentIteratorTopNRuntimeFilterTest, UpdatesAtEofDoNotRewindOrCountConsumedRows) {
    ScanResult result;
    ASSERT_NO_FATAL_FAILURE(late_scan(false, true, &result));
    const auto before = _stats.rows_stats_rp_filtered;
    ASSERT_TRUE(_predicate->update(Field::create_field<TYPE_INT>(1)).ok());
    auto* iterator = assert_cast<SegmentIterator*>(_reader.get());
    ASSERT_TRUE(iterator->_update_topn_runtime_filter_ranges().ok());
    EXPECT_EQ(before, _stats.rows_stats_rp_filtered);
    EXPECT_EQ(kRows / 2, result.ids.size());
}

TEST_F(SegmentIteratorTopNRuntimeFilterTest, RebuildingPreservesHolesFromInitialRestrictions) {
    ASSERT_NO_FATAL_FAILURE(start(false, Field(), true));
    std::vector<int32_t> ids;
    ASSERT_TRUE(read(&ids).ok());
    ASSERT_TRUE(_predicate->update(Field::create_field<TYPE_INT>(8191)).ok());
    ASSERT_NO_FATAL_FAILURE(drain(&ids));
    EXPECT_EQ(6144, ids.size());
    EXPECT_EQ(std::set<int32_t>(ids.begin(), ids.end()).size(), ids.size());
    EXPECT_TRUE(std::ranges::all_of(
            ids, [](int id) { return (id < 2048 || id >= 4096) && id <= 8191; }));
}

TEST_F(SegmentIteratorTopNRuntimeFilterTest, SnapshotKeepsItsBoundAfterPublicationOfANewerVersion) {
    ASSERT_NO_FATAL_FAILURE(start(false, Field::create_field<TYPE_INT>(100)));
    auto shared = std::dynamic_pointer_cast<SharedPredicate>(_predicate->get_predicate(20));
    ASSERT_NE(nullptr, shared);
    const auto snapshot = shared->snapshot();
    ASSERT_EQ(1, snapshot.version);
    ASSERT_TRUE(_predicate->update(Field::create_field<TYPE_INT>(100)).ok());
    ASSERT_TRUE(_predicate->update(Field::create_field<TYPE_INT>(200)).ok());
    EXPECT_EQ(1, shared->version());
    ASSERT_TRUE(_predicate->update(Field::create_field<TYPE_INT>(50)).ok());
    EXPECT_EQ(2, shared->version());
    ZoneMap zone_map;
    zone_map.has_not_null = true;
    zone_map.min_value = Field::create_field<TYPE_INT>(75);
    zone_map.max_value = zone_map.min_value;
    EXPECT_TRUE(snapshot.predicate->evaluate_and(zone_map));
    EXPECT_FALSE(shared->evaluate_and(zone_map));
}

TEST_F(SegmentIteratorTopNRuntimeFilterTest, ConcurrentSnapshotsPairEachVersionWithItsBound) {
    ASSERT_NO_FATAL_FAILURE(start(false, Field::create_field<TYPE_INT>(kRows)));
    auto shared = std::dynamic_pointer_cast<SharedPredicate>(_predicate->get_predicate(20));
    ASSERT_NE(nullptr, shared);
    std::jthread writer([&] {
        SCOPED_INIT_THREAD_CONTEXT();
        for (int i = 1; i <= 1000; ++i) {
            EXPECT_TRUE(_predicate->update(Field::create_field<TYPE_INT>(kRows - i)).ok());
        }
    });
    for (int i = 0; i < 1000; ++i) {
        const auto snapshot = shared->snapshot();
        const int boundary = kRows - static_cast<int>(snapshot.version - 1);
        ZoneMap zone_map;
        zone_map.has_not_null = true;
        zone_map.min_value = Field::create_field<TYPE_INT>(boundary);
        zone_map.max_value = zone_map.min_value;
        EXPECT_TRUE(snapshot.predicate->evaluate_and(zone_map));
        zone_map.min_value = Field::create_field<TYPE_INT>(boundary + 1);
        zone_map.max_value = zone_map.min_value;
        EXPECT_FALSE(snapshot.predicate->evaluate_and(zone_map));
    }
    writer.join();
    EXPECT_EQ(1001, shared->version());
}

} // namespace doris::segment_v2
