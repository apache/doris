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
#include <random>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "core/block/block.h"
#include "core/field.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/index/ordinal_page_index.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/olap_tuple.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/comparison_predicate.h"
#include "storage/row_cursor.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/row_ranges.h"
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

TabletSchemaSPtr make_tablet_schema(bool bloom_filter = false) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*create_int_key(0, false));
    tablet_schema->append_column(*create_int_value(
            1, FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, false, "", bloom_filter));
    // Small pages so the value column has many zone maps to prune.
    tablet_schema->set_storage_page_size(4096);
    return tablet_schema;
}

class CountingAndBlockColumnPredicate : public AndBlockColumnPredicate {
public:
    using AndBlockColumnPredicate::evaluate_and;

    bool evaluate_and(const ZoneMap& zone_map) const override {
        // Segment pruning evaluates the same predicate once before page pruning.
        if (zone_map.min_value != int_field(0) || zone_map.max_value != int_field(kNumRows - 1)) {
            ++page_evaluations;
        }
        return AndBlockColumnPredicate::evaluate_and(zone_map);
    }

    bool evaluate_and(const BloomFilter* bloom_filter) const override {
        ++bloom_filter_evaluations;
        return AndBlockColumnPredicate::evaluate_and(bloom_filter);
    }

    mutable size_t page_evaluations = 0;
    mutable size_t bloom_filter_evaluations = 0;
};

std::shared_ptr<CountingAndBlockColumnPredicate> make_int_eq_predicate(
        uint32_t column_id, const std::string& column_name, int32_t value) {
    auto predicates = std::make_shared<CountingAndBlockColumnPredicate>();
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
            for (size_t cid = 0; cid < _tablet_schema->num_columns(); ++cid) {
                row.mutable_field(cid) = int_field(rid);
            }
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
    auto predicates = make_int_eq_predicate(1, "1", kTargetKey);
    read_options.col_id_to_predicates.emplace(1, predicates);

    EXPECT_EQ(1, read_all(segment, read_options));
    EXPECT_EQ(kNumRows - 1, _stats.rows_key_range_filtered);
    EXPECT_EQ(0, _stats.rows_stats_filtered);
    EXPECT_EQ(1, predicates->page_evaluations);
}

// Disjoint key ranges leave a sparse bitmap. The zone map may only be charged for the
// alive rows it removes, not for the rows between the key ranges.
TEST_F(SegmentIteratorKeyRangeStatsTest, DisjointKeyRangesOnlyCountAliveRows) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));

    constexpr int kOtherKey = 1;
    OlapTuple other_key_tuple;
    other_key_tuple.add_field(int_field(kOtherKey));
    RowCursor other_key;
    ASSERT_TRUE(other_key.init(_tablet_schema, other_key_tuple).ok());
    OlapTuple target_key_tuple;
    target_key_tuple.add_field(int_field(kTargetKey));
    RowCursor target_key;
    ASSERT_TRUE(target_key.init(_tablet_schema, target_key_tuple).ok());

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_options.key_ranges.emplace_back(&other_key, true, &other_key, true);
    read_options.key_ranges.emplace_back(&target_key, true, &target_key, true);
    auto predicates = make_int_eq_predicate(1, "1", kTargetKey);
    read_options.col_id_to_predicates.emplace(1, predicates);

    // Only the row of kOtherKey sits on a page the zone map prunes.
    EXPECT_EQ(1, read_all(segment, read_options));
    EXPECT_EQ(kNumRows - 2, _stats.rows_key_range_filtered);
    EXPECT_EQ(1, _stats.rows_stats_filtered);
    EXPECT_EQ(1, _stats.rows_conditions_filtered);
    EXPECT_EQ(2, predicates->page_evaluations);
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

TEST_F(SegmentIteratorKeyRangeStatsTest, SamePageKeyRangesAreEvaluatedOnce) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    OlapTuple first_tuple;
    first_tuple.add_field(int_field(1));
    RowCursor first;
    ASSERT_TRUE(first.init(_tablet_schema, first_tuple).ok());
    OlapTuple second_tuple;
    second_tuple.add_field(int_field(2));
    RowCursor second;
    ASSERT_TRUE(second.init(_tablet_schema, second_tuple).ok());
    auto predicates = make_int_eq_predicate(1, "1", 2);
    StorageReadOptions opts;
    opts.stats = &_stats;
    opts.key_ranges.emplace_back(&first, true, &first, true);
    opts.key_ranges.emplace_back(&second, true, &second, true);
    opts.col_id_to_predicates.emplace(1, predicates);
    // This fixture applies index predicates only, so both candidates on the matching page survive.
    EXPECT_EQ(2, read_all(segment, opts));
    EXPECT_EQ(1, predicates->page_evaluations);
    EXPECT_EQ(0, _stats.rows_stats_filtered);
}

TEST_F(SegmentIteratorKeyRangeStatsTest, ZoneMapRejectsTheOnlyCandidatePage) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    OlapTuple tuple;
    tuple.add_field(int_field(1));
    RowCursor key;
    ASSERT_TRUE(key.init(_tablet_schema, tuple).ok());
    auto predicates = make_int_eq_predicate(1, "1", kTargetKey);
    StorageReadOptions opts;
    opts.stats = &_stats;
    opts.key_ranges.emplace_back(&key, true, &key, true);
    opts.col_id_to_predicates.emplace(1, predicates);
    EXPECT_EQ(0, read_all(segment, opts));
    EXPECT_EQ(1, predicates->page_evaluations);
    EXPECT_EQ(1, _stats.rows_stats_filtered);
}

TEST_F(SegmentIteratorKeyRangeStatsTest, EmptyKeyRangeSkipsPagePredicates) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    OlapTuple tuple;
    tuple.add_field(int_field(kNumRows));
    RowCursor key;
    ASSERT_TRUE(key.init(_tablet_schema, tuple).ok());
    auto predicates = make_int_eq_predicate(1, "1", kTargetKey);
    StorageReadOptions opts;
    opts.stats = &_stats;
    opts.key_ranges.emplace_back(&key, true, &key, true);
    opts.col_id_to_predicates.emplace(1, predicates);
    EXPECT_EQ(0, read_all(segment, opts));
    EXPECT_EQ(0, predicates->page_evaluations);
    EXPECT_EQ(0, _stats.rows_stats_filtered);
}

TEST_F(SegmentIteratorKeyRangeStatsTest, BloomFilterVisitsOnlyCandidatePages) {
    _tablet_schema = make_tablet_schema(true);
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    OlapTuple tuple;
    tuple.add_field(int_field(kTargetKey));
    RowCursor key;
    ASSERT_TRUE(key.init(_tablet_schema, tuple).ok());
    auto predicates = make_int_eq_predicate(1, "1", kTargetKey);
    StorageReadOptions opts;
    opts.stats = &_stats;
    opts.key_ranges.emplace_back(&key, true, &key, true);
    opts.col_id_to_predicates.emplace(1, predicates);
    EXPECT_EQ(1, read_all(segment, opts));
    EXPECT_EQ(1, predicates->bloom_filter_evaluations);
    EXPECT_EQ(1, predicates->page_evaluations);
    EXPECT_EQ(0, _stats.rows_bf_filtered);
    EXPECT_EQ(0, _stats.rows_stats_filtered);
}

TEST_F(SegmentIteratorKeyRangeStatsTest, LaterColumnsUseEarlierPruning) {
    _tablet_schema->append_column(
            *create_int_value(2, FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, false));
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto first = make_int_eq_predicate(1, "1", kTargetKey);
    auto second = make_int_eq_predicate(2, "2", kTargetKey);
    StorageReadOptions opts;
    opts.stats = &_stats;
    opts.col_id_to_predicates.emplace(1, first);
    opts.col_id_to_predicates.emplace(2, second);
    EXPECT_GT(read_all(segment, opts), 0);
    EXPECT_GT(first->page_evaluations, 1);
    EXPECT_EQ(1, second->page_evaluations);
    EXPECT_EQ(first->page_evaluations + second->page_evaluations,
              _stats.zonemap_index_pages_evaluated);
}

TEST_F(SegmentIteratorKeyRangeStatsTest, RejectsMismatchedZoneMapPageCount) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    std::shared_ptr<ColumnReader> reader;
    ASSERT_TRUE(segment->get_column_reader(1, &reader, &_stats).ok());
    ColumnIteratorOptions opts;
    opts.file_reader = segment->file_reader().get();
    opts.stats = &_stats;
    const std::vector<ZoneMapPB>* zone_maps = nullptr;
    ASSERT_TRUE(reader->get_page_zone_maps(opts, &zone_maps).ok());
    ASSERT_GT(zone_maps->size(), 1);
    reader->_zone_map_index->_page_zone_maps.pop_back();
    roaring::Roaring bitmap;
    bitmap.add(kNumRows - 1);
    auto ranges = RowRanges::create_single(kNumRows);
    auto predicates = make_int_eq_predicate(1, "1", kNumRows - 1);
    auto status =
            reader->get_row_ranges_by_zone_map(predicates.get(), nullptr, bitmap, &ranges, opts);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
}

TEST_F(SegmentIteratorKeyRangeStatsTest, CandidatePageSelectionMatchesBitmapAndRanges) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    std::shared_ptr<ColumnReader> reader;
    ASSERT_TRUE(segment->get_column_reader(1, &reader, &_stats).ok());
    OrdinalIndexReader* ordinal_index = nullptr;
    ASSERT_TRUE(reader->get_ordinal_index_reader(ordinal_index, &_stats).ok());
    ASSERT_GT(ordinal_index->num_data_pages(), 2);
    ColumnIteratorOptions opts;
    opts.file_reader = segment->file_reader().get();
    opts.stats = &_stats;
    auto verify = [&](const roaring::Roaring& bitmap, const RowRanges& ranges) {
        // Deliberately use a full page walk as an independent oracle for the seeking helper.
        std::vector<uint32_t> expected;
        for (int page = 0; page < ordinal_index->num_data_pages(); ++page) {
            for (size_t range = 0; range < ranges.range_size(); ++range) {
                auto from = std::max<int64_t>(ordinal_index->get_first_ordinal(page),
                                              ranges.get_range_from(range));
                auto to = std::min<int64_t>(ordinal_index->get_last_ordinal(page) + 1,
                                            ranges.get_range_to(range));
                if (from < to &&
                    roaring::api::roaring_bitmap_range_cardinality(&bitmap.roaring, from, to) > 0) {
                    expected.push_back(page);
                    break;
                }
            }
        }
        std::vector<uint32_t> actual {999};
        ASSERT_TRUE(reader->get_candidate_page_indexes(bitmap, ranges, opts, &actual).ok());
        EXPECT_EQ(expected, actual);
    };
    roaring::Roaring all_rows;
    all_rows.addRange(0, kNumRows);
    const auto full_range = RowRanges::create_single(kNumRows);
    verify(all_rows, full_range);
    verify({}, full_range);
    verify(all_rows, {});
    RowRanges same_page_ranges;
    same_page_ranges.add(RowRange(0, 1));
    same_page_ranges.add(RowRange(2, 3));
    verify(all_rows, same_page_ranges);
    const auto boundary = ordinal_index->get_first_ordinal(1);
    verify(all_rows, RowRanges::create_single(boundary - 1, boundary + 1));
    roaring::Roaring endpoints;
    endpoints.add(0);
    endpoints.add(kNumRows - 1);
    verify(endpoints, full_range);
    verify(endpoints, RowRanges::create_single(1, kNumRows - 1));
    verify(endpoints, RowRanges::create_single(kNumRows - 1, kNumRows));
    std::mt19937 random(42);
    for (int trial = 0; trial < 100; ++trial) {
        SCOPED_TRACE(trial);
        roaring::Roaring sparse;
        RowRanges ranges;
        for (int i = 0; i < 64; ++i) {
            sparse.add(random() % kNumRows);
        }
        for (int from = 0; from < kNumRows; from += 256) {
            ranges.add(RowRange(from, from + random() % 256));
        }
        verify(sparse, ranges);
    }
}

} // namespace doris::segment_v2
