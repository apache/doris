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

#include "core/block/block.h"
#include "core/field.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/olap_tuple.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/comparison_predicate.h"
#include "storage/row_cursor.h"
#include "storage/segment/segment.h"
#include "storage/segment/test_segment_writer.h"
#include "storage/tablet/tablet_schema_helper.h"

namespace doris::segment_v2 {
namespace {

constexpr auto kTestDir = "./ut_dir/segment_iterator_key_range_stats_test";
constexpr int kNumRows = 8192;
constexpr int kTargetKey = 5000;
const RowsetId kRowsetId {.version = 1};

Field int_field(int32_t value) {
    return Field::create_field<TYPE_INT>(value);
}

TabletSchemaSPtr make_tablet_schema() {
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*create_int_key(0, false));
    tablet_schema->append_column(
            *create_int_value(1, FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, false));
    // Small pages so the value column has many zone maps to prune.
    tablet_schema->set_storage_page_size(4096);
    return tablet_schema;
}

std::shared_ptr<AndBlockColumnPredicate> make_int_eq_predicate(uint32_t column_id,
                                                               const std::string& column_name,
                                                               int32_t value) {
    auto predicates = AndBlockColumnPredicate::create_shared();
    std::shared_ptr<ColumnPredicate> pred(new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(
            column_id, column_name, int_field(value)));
    predicates->add_column_predicate(SingleColumnBlockPredicate::create_unique(pred));
    return predicates;
}

} // namespace

class SegmentIteratorKeyRangeStatsTest : public testing::Test {
protected:
    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        _tablet_schema = make_tablet_schema();
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    // Both columns hold the row id, so a key range and a value predicate can select
    // the same single row.
    void build_segment(std::shared_ptr<Segment>* segment) {
        const auto path = std::string(kTestDir) + "/key_range_stats_segment.dat";
        auto fs = io::global_local_filesystem();
        io::FileWriterPtr file_writer;
        auto st = fs->create_file(path, &file_writer);
        ASSERT_TRUE(st.ok()) << st;

        VerticalSegmentWriterOptions opts;
        opts.num_rows_per_block = 1024;
        TestVerticalSegmentWriter writer(file_writer.get(), 0, _tablet_schema, nullptr, nullptr,
                                         opts, nullptr);
        st = writer.init();
        ASSERT_TRUE(st.ok()) << st;

        RowCursor row;
        std::vector<Field> fields(_tablet_schema->num_columns(), Field(PrimitiveType::TYPE_NULL));
        st = row.init_scan_key(_tablet_schema, std::move(fields));
        ASSERT_TRUE(st.ok()) << st;
        for (int rid = 0; rid < kNumRows; ++rid) {
            row.mutable_field(0) = int_field(rid);
            row.mutable_field(1) = int_field(rid);
            st = writer.append_row(row);
            ASSERT_TRUE(st.ok()) << st;
        }

        uint64_t file_size = 0;
        uint64_t index_size = 0;
        st = writer.finalize_columns(&index_size);
        ASSERT_TRUE(st.ok()) << st;
        st = writer.finalize_footer(&file_size);
        ASSERT_TRUE(st.ok()) << st;
        st = file_writer->close();
        ASSERT_TRUE(st.ok()) << st;

        st = Segment::open(fs, path, 100, 0, kRowsetId, _tablet_schema, io::FileReaderOptions {},
                           segment);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(kNumRows, (*segment)->num_rows());
    }

    size_t read_all(const std::shared_ptr<Segment>& segment,
                    const StorageReadOptions& read_options) {
        auto read_schema = std::make_shared<ReadSchema>(_tablet_schema->columns());
        std::unique_ptr<RowwiseIterator> iter;
        auto st = segment->new_iterator(read_schema, read_options, &iter);
        EXPECT_TRUE(st.ok()) << st;
        size_t rows = 0;
        while (true) {
            Block block = _tablet_schema->create_storage_block();
            st = iter->next_batch(&block);
            if (st.is<ErrorCode::END_OF_FILE>()) {
                break;
            }
            EXPECT_TRUE(st.ok()) << st;
            rows += block.rows();
        }
        return rows;
    }

    TabletSchemaSPtr _tablet_schema;
    OlapReaderStatistics _stats;
};

// A point lookup that the key range already narrowed down to one row must not
// report the rest of the segment as zone-map filtered.
TEST_F(SegmentIteratorKeyRangeStatsTest, KeyRangeHitDoesNotInflateStatsFiltered) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));

    OlapTuple key_tuple;
    key_tuple.add_field(int_field(kTargetKey));
    RowCursor key;
    ASSERT_TRUE(key.init(_tablet_schema, key_tuple).ok());

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_options.key_ranges.emplace_back(&key, true, &key, true);
    read_options.col_id_to_predicates.emplace(1, make_int_eq_predicate(1, "1", kTargetKey));

    EXPECT_EQ(1, read_all(segment, read_options));
    EXPECT_EQ(kNumRows - 1, _stats.rows_key_range_filtered);
    EXPECT_EQ(0, _stats.rows_stats_filtered);
}

// Without a key range the very same predicate still prunes pages by zone map.
TEST_F(SegmentIteratorKeyRangeStatsTest, ZoneMapStillFiltersWithoutKeyRange) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_options.col_id_to_predicates.emplace(1, make_int_eq_predicate(1, "1", kTargetKey));

    // col_id_to_predicates only drives index pruning, so the rows that survive are
    // exactly the rows of the pages the zone map kept.
    const size_t rows = read_all(segment, read_options);
    EXPECT_EQ(0, _stats.rows_key_range_filtered);
    EXPECT_GT(_stats.rows_stats_filtered, 0);
    EXPECT_LT(_stats.rows_stats_filtered, kNumRows);
    EXPECT_EQ(kNumRows - _stats.rows_stats_filtered, rows);
}

} // namespace doris::segment_v2
