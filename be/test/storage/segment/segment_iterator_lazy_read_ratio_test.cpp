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
#include <string>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/runtime_state.h"
#include "storage/olap_common.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/comparison_predicate.h"
#include "storage/row_cursor.h"
#include "storage/rowset_id.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"
#include "storage/segment/test_segment_writer.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {

constexpr auto kTestDir = "./ut_dir/segment_iterator_lazy_read_ratio_test";
constexpr int kRows = 1000;
const RowsetId kRowsetId {.version = 1};

class RecordingOutputIterator final : public ColumnIterator {
public:
    explicit RecordingOutputIterator(std::unique_ptr<ColumnIterator> delegate)
            : _delegate(std::move(delegate)) {}

    Status seek_to_ordinal(ordinal_t ord) override { return _delegate->seek_to_ordinal(ord); }

    Status next_batch(size_t* rows, MutableColumnPtr& dst, bool* has_null) override {
        ++batch_calls;
        auto status = _delegate->next_batch(rows, dst, has_null);
        if (status.ok()) {
            batch_rows += *rows;
        }
        return status;
    }

    Status read_by_rowids(const rowid_t* rowids, size_t count, MutableColumnPtr& dst) override {
        ++sparse_calls;
        sparse_rows += count;
        return _delegate->read_by_rowids(rowids, count, dst);
    }

    ordinal_t get_current_ordinal() const override { return _delegate->get_current_ordinal(); }

    size_t batch_calls = 0;
    size_t batch_rows = 0;
    size_t sparse_calls = 0;
    size_t sparse_rows = 0;

private:
    std::unique_ptr<ColumnIterator> _delegate;
};

class SegmentIteratorLazyReadRatioTest : public testing::Test {
protected:
    void SetUp() override {
        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->delete_directory(kTestDir).ok());
        ASSERT_TRUE(fs->create_directory(kTestDir).ok());

        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        constexpr const char* names[] = {"k", "p", "payload"};
        for (int cid = 0; cid < 3; ++cid) {
            auto* col = schema_pb.add_column();
            col->set_unique_id(cid);
            col->set_name(names[cid]);
            col->set_type("INT");
            col->set_is_key(cid == 0);
            col->set_is_nullable(false);
            col->set_aggregation("NONE");
        }
        _tablet_schema = std::make_shared<TabletSchema>();
        _tablet_schema->init_from_pb(schema_pb);
        _read_schema = std::make_shared<ReadSchema>(_tablet_schema->columns());

        const auto path = std::string(kTestDir) + "/segment.dat";
        io::FileWriterPtr writer_file;
        ASSERT_TRUE(fs->create_file(path, &writer_file).ok());
        VerticalSegmentWriterOptions writer_options;
        writer_options.num_rows_per_block = 1024;
        TestVerticalSegmentWriter writer(writer_file.get(), 0, _tablet_schema, nullptr, nullptr,
                                         writer_options, nullptr);
        ASSERT_TRUE(writer.init().ok());
        RowCursor row;
        std::vector<Field> fields(3, Field(PrimitiveType::TYPE_NULL));
        ASSERT_TRUE(row.init_scan_key(_tablet_schema, std::move(fields)).ok());
        for (int rid = 0; rid < kRows; ++rid) {
            row.mutable_field(0) = Field::create_field<TYPE_INT>(rid);
            row.mutable_field(1) = Field::create_field<TYPE_INT>(rid % 100);
            row.mutable_field(2) = Field::create_field<TYPE_INT>(rid * 10 + 7);
            ASSERT_TRUE(writer.append_row(row).ok());
        }
        uint64_t index_size = 0;
        uint64_t file_size = 0;
        ASSERT_TRUE(writer.finalize_columns(&index_size).ok());
        ASSERT_TRUE(writer.finalize_footer(&file_size).ok());
        ASSERT_TRUE(writer_file->close().ok());
        ASSERT_TRUE(Segment::open(fs, path, 100, 0, kRowsetId, _tablet_schema,
                                  io::FileReaderOptions {}, &_segment)
                            .ok());
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    // GTest assertions expand into several control-flow branches in clang-tidy.
    // NOLINTNEXTLINE(readability-function-cognitive-complexity)
    void check_read(int predicate_bound, int ratio, bool expected_lazy, size_t read_limit = 0,
                    bool reverse = false) {
        RuntimeState runtime_state;
        TQueryOptions query_options;
        if (ratio >= 0) {
            query_options.__set_lazy_read_ratio(ratio);
        }
        runtime_state.set_query_options(query_options);

        OlapReaderStatistics stats;
        StorageReadOptions options(stats);
        options.runtime_state = &runtime_state;
        options.io_ctx.reader_type = ReaderType::READER_QUERY;
        options.block_row_max = kRows;
        options.read_limit = read_limit;
        options.read_orderby_key_reverse = reverse;
        auto predicate = std::shared_ptr<ColumnPredicate>(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::LT>(
                        1, "p", Field::create_field<TYPE_INT>(predicate_bound)));
        options.column_predicates.push_back(predicate);
        auto block_predicate = AndBlockColumnPredicate::create_shared();
        block_predicate->add_column_predicate(SingleColumnBlockPredicate::create_unique(predicate));
        options.col_id_to_predicates.emplace(1, std::move(block_predicate));

        std::unique_ptr<RowwiseIterator> iterator;
        auto status = _segment->new_iterator(_read_schema, options, &iterator);
        ASSERT_TRUE(status.ok()) << status;
        auto* segment_iterator = dynamic_cast<SegmentIterator*>(iterator.get());
        ASSERT_NE(nullptr, segment_iterator);
        Block block = _read_schema->create_read_block();
        status = segment_iterator->_lazy_init(&block);
        ASSERT_TRUE(status.ok()) << status;
        segment_iterator->_block_size_predictor.reset();
        auto recorder = std::make_unique<RecordingOutputIterator>(
                std::move(segment_iterator->_column_iterators[2]));
        auto* recording = recorder.get();
        segment_iterator->_column_iterators[2] = std::move(recorder);

        status = iterator->next_batch(&block);
        ASSERT_TRUE(status.ok()) << status;
        const size_t expected_rows = predicate_bound * (kRows / 100);
        const size_t returned_rows =
                read_limit == 0 ? expected_rows : std::min(read_limit, expected_rows);
        ASSERT_EQ(returned_rows, block.rows());
        const auto* keys = check_and_get_column<ColumnInt32>(block.get_by_position(0).column.get());
        const auto* payloads =
                check_and_get_column<ColumnInt32>(block.get_by_position(2).column.get());
        ASSERT_NE(nullptr, keys);
        ASSERT_NE(nullptr, payloads);
        for (size_t i = 0; i < returned_rows; ++i) {
            int rid = keys->get_element(i);
            EXPECT_LT(rid % 100, predicate_bound);
            EXPECT_EQ(rid * 10 + 7, payloads->get_element(i));
        }
        if (expected_lazy) {
            EXPECT_EQ(0, recording->batch_calls);
            EXPECT_EQ(returned_rows, recording->sparse_rows);
        } else {
            EXPECT_GT(recording->batch_calls, 0);
            EXPECT_EQ(kRows, recording->batch_rows);
            EXPECT_EQ(0, recording->sparse_calls);
        }
    }

    TabletSchemaSPtr _tablet_schema;
    ReadSchemaSPtr _read_schema;
    std::shared_ptr<Segment> _segment;
};

TEST_F(SegmentIteratorLazyReadRatioTest, LowFilteringUsesBatchReadByDefault) {
    check_read(99, -1, false);
}

TEST_F(SegmentIteratorLazyReadRatioTest, StrictThresholdAndLimits) {
    check_read(51, 50, false);
    check_read(50, 50, false);
    check_read(49, 50, true);
    check_read(99, 0, true);
    check_read(1, 100, false);
    check_read(80, 50, false, 2);
}

TEST_F(SegmentIteratorLazyReadRatioTest, MissingQueryOptionDefaultsToFifty) {
    RuntimeState state;
    EXPECT_EQ(50, state.lazy_read_ratio());
    TQueryOptions options;
    options.__set_lazy_read_ratio(0);
    state.set_query_options(options);
    EXPECT_EQ(0, state.lazy_read_ratio());
    options.__set_lazy_read_ratio(100);
    state.set_query_options(options);
    EXPECT_EQ(100, state.lazy_read_ratio());
}

} // namespace
} // namespace doris::segment_v2
