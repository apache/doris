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

// TopNBlockIterator returns the first `limit` rows by the sort columns of rowsets whose rows come
// as sorted streams, reads at most `limit` rows of a stream, and passes the first sort column of
// the last of the first `limit` rows to the TopN filter each time that row changes.

#include "storage/iterator/topn_block_iterator.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/consts.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_nothing.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

using Row = std::vector<int64_t>;
using Batch = std::vector<Row>;
// The batches of one stream.
using Batches = std::vector<Batch>;

// BIGINT columns: the key k and the value v, then with `virtual_column` the virtual column x.
ReadSchemaSPtr make_schema(bool virtual_column = false) {
    std::vector<TabletColumnPtr> columns;
    auto add = [&](const std::string& name, bool is_key) {
        auto column = std::make_shared<TabletColumn>(
                FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                FieldType::OLAP_FIELD_TYPE_BIGINT, false, static_cast<int32_t>(columns.size()),
                sizeof(int64_t));
        column->set_name(name);
        column->set_is_key(is_key);
        columns.push_back(column);
    };
    add("k", true);
    add("v", false);
    if (virtual_column) {
        add(BeConsts::VIRTUAL_COLUMN_PREFIX + "x", false);
    }
    return std::make_shared<ReadSchema>(std::move(columns));
}

// Hands out `batches`, one per next_batch(), then `end`, and counts the batches it hands out in
// `*batches_read`. Like a segment, it gives the empty placeholder of a virtual column in the Block
// it fills a column of its own.
class SegmentUtIterator final : public RowwiseIterator {
public:
    SegmentUtIterator(const ReadSchema& schema, Batches batches, Status end, int* batches_read)
            : _schema(schema),
              _batches(std::move(batches)),
              _end(std::move(end)),
              _batches_read(batches_read) {}

    Status next_batch(Block* block) override {
        if (_next == _batches.size()) {
            return _end;
        }
        for (size_t c = 0; c < block->columns(); ++c) {
            ColumnPtr& column = block->get_by_position(c).column;
            if (check_and_get_column<ColumnNothing>(*column) != nullptr) {
                column = ColumnInt64::create();
            }
        }
        auto columns_guard = block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        for (const Row& row : _batches[_next]) {
            for (size_t c = 0; c < columns.size(); ++c) {
                assert_cast<ColumnInt64&>(*columns[c]).insert_value(row[c]);
            }
        }
        ++_next;
        if (_batches_read != nullptr) {
            ++*_batches_read;
        }
        return Status::OK();
    }

    const ReadSchema& schema() const override { return _schema; }

private:
    const ReadSchema& _schema;
    Batches _batches;
    const Status _end;
    int* _batches_read;
    size_t _next = 0;
};

// A rowset of one stream: a SegmentUtIterator of `batches`.
TopNBlockIterator::RowsetOpener open_rowset(const ReadSchemaSPtr& schema, Batches batches,
                                            Status end = Status::EndOfFile("no more batches"),
                                            int* batches_read = nullptr) {
    return [schema, batches = std::move(batches), end,
            batches_read](std::vector<RowwiseIteratorUPtr>* streams) {
        streams->push_back(
                std::make_unique<SegmentUtIterator>(*schema, batches, end, batches_read));
        return Status::OK();
    };
}

// A rowset whose segments overlap: a SegmentUtIterator for the batches of each stream.
TopNBlockIterator::RowsetOpener open_overlapping_rowset(const ReadSchemaSPtr& schema,
                                                        std::vector<Batches> segments) {
    return [schema, segments = std::move(segments)](std::vector<RowwiseIteratorUPtr>* streams) {
        for (const Batches& batches : segments) {
            streams->push_back(std::make_unique<SegmentUtIterator>(
                    *schema, batches, Status::EndOfFile("no more batches"), nullptr));
        }
        return Status::OK();
    };
}

std::vector<Row> rows_of(const Block& block) {
    std::vector<Row> rows(block.rows());
    for (size_t c = 0; c < block.columns(); ++c) {
        const auto* column = check_and_get_column<ColumnInt64>(*block.get_by_position(c).column);
        if (column == nullptr) {
            ADD_FAILURE() << "column " << c << " is "
                          << block.get_by_position(c).column->get_name();
            return {};
        }
        for (size_t r = 0; r < block.rows(); ++r) {
            rows[r].push_back(column->get_element(r));
        }
    }
    return rows;
}

// Read `topn` the way BlockReader does: one Block with the rows, then the end.
std::vector<Row> read(TopNBlockIterator& topn, const ReadSchema& schema) {
    Block block = schema.create_read_block();
    bool eof = false;
    Status st = topn.next_block(&block, &eof);
    EXPECT_TRUE(st.ok()) << st;
    std::vector<Row> rows = rows_of(block);
    EXPECT_EQ(eof, rows.empty());

    Block next = schema.create_read_block();
    eof = false;
    st = topn.next_block(&next, &eof);
    EXPECT_TRUE(st.ok()) << st;
    EXPECT_TRUE(eof);
    EXPECT_EQ(next.rows(), 0);
    return rows;
}

// The first `limit` rows of `rowsets` by `sort_columns`; `filter_values` gets the values the TopN
// filter is tightened to.
std::vector<Row> read_topn(const ReadSchemaSPtr& schema, const std::vector<Batches>& rowsets,
                           size_t limit, std::vector<uint32_t> sort_columns, bool descending,
                           std::vector<int64_t>* filter_values = nullptr) {
    std::vector<TopNBlockIterator::RowsetOpener> openers;
    for (const Batches& rowset : rowsets) {
        openers.push_back(open_rowset(schema, rowset));
    }
    TopNBlockIterator::FilterUpdater update_filter;
    if (filter_values != nullptr) {
        update_filter = [filter_values](const Field& value) {
            filter_values->push_back(value.get<TYPE_BIGINT>());
            return Status::OK();
        };
    }
    TopNBlockIterator topn(std::move(openers), limit, std::move(sort_columns), descending, {},
                           std::move(update_filter));
    return read(topn, *schema);
}

} // namespace

// Rowsets 1 4 7, then 2 3 | 9, then 0 5 with a limit of 3: the first rowset fills the rows, 2 and
// 3 of the second replace 4 and 7 and its 9 is not copied, and 0 of the third replaces 3. The
// filter goes to the last of the first 3 rows each time it changes: 7, 3, 2.
TEST(TopNBlockIteratorTest, KeepsTheFirstRowsInAscendingOrder) {
    auto schema = make_schema();
    std::vector<int64_t> filter_values;
    EXPECT_EQ(read_topn(schema,
                        {{{{1, 10}, {4, 40}, {7, 70}}},
                         {{{2, 20}, {3, 30}}, {{9, 90}}},
                         {{{0, 0}, {5, 50}}}},
                        3, {0}, false, &filter_values),
              (std::vector<Row> {{0, 0}, {1, 10}, {2, 20}}));
    EXPECT_EQ(filter_values, (std::vector<int64_t> {7, 3, 2}));
}

// The same rowsets, descending: 9 6 3, then 8 7 | 1, then 10 5.
TEST(TopNBlockIteratorTest, KeepsTheFirstRowsInDescendingOrder) {
    auto schema = make_schema();
    std::vector<int64_t> filter_values;
    EXPECT_EQ(read_topn(schema,
                        {{{{9, 90}, {6, 60}, {3, 30}}},
                         {{{8, 80}, {7, 70}}, {{1, 10}}},
                         {{{10, 100}, {5, 50}}}},
                        3, {0}, true, &filter_values),
              (std::vector<Row> {{10, 100}, {9, 90}, {8, 80}}));
    EXPECT_EQ(filter_values, (std::vector<int64_t> {3, 7, 8}));
}

// With fewer rows than the limit, every row comes back, sorted, and the filter stays as it is.
TEST(TopNBlockIteratorTest, ReturnsEveryRowWhenThereAreFewerThanTheLimit) {
    auto schema = make_schema();
    std::vector<int64_t> filter_values;
    EXPECT_EQ(
            read_topn(schema, {{{{3, 30}, {5, 50}}}, {{{1, 10}}}}, 10, {0}, false, &filter_values),
            (std::vector<Row> {{1, 10}, {3, 30}, {5, 50}}));
    EXPECT_TRUE(filter_values.empty());
}

// No rowsets, a rowset without batches and a rowset of an empty batch, like that of a segment
// whose predicates filtered out every row: the read ends at once.
TEST(TopNBlockIteratorTest, EndsAtOnceWithoutRows) {
    auto schema = make_schema();
    EXPECT_TRUE(read_topn(schema, {}, 3, {0}, false).empty());
    EXPECT_TRUE(read_topn(schema, {Batches {}, Batches {Batch {}}}, 3, {0}, false).empty());
}

// A row equal on the sort columns to the last of the first rows does not replace it, and the
// sort columns are compared in turn.
TEST(TopNBlockIteratorTest, ComparesTheSortColumnsInTurn) {
    auto schema = make_schema();
    const std::vector<Batches> rowsets = {{{{1, 10}, {2, 20}}}, {{{2, 15}, {2, 25}, {3, 30}}}};
    EXPECT_EQ(read_topn(schema, rowsets, 2, {0}, false), (std::vector<Row> {{1, 10}, {2, 20}}));
    EXPECT_EQ(read_topn(schema, rowsets, 2, {0, 1}, false), (std::vector<Row> {{1, 10}, {2, 15}}));
    EXPECT_EQ(read_topn(schema, {{{{2, 25}, {2, 15}}}, {{{2, 20}, {1, 10}}}}, 2, {0, 1}, true),
              (std::vector<Row> {{2, 25}, {2, 20}}));
}

// A stream gives at most `limit` rows: once that many are read, its later batches are not read.
TEST(TopNBlockIteratorTest, ReadsAtMostTheLimitOfRowsOfAStream) {
    auto schema = make_schema();
    int batches_read = 0;
    std::vector<TopNBlockIterator::RowsetOpener> openers;
    openers.push_back(open_rowset(schema, {{{5, 50}}, {{6, 60}, {7, 70}}, {{8, 80}}},
                                  Status::EndOfFile("no more batches"), &batches_read));
    openers.push_back(open_rowset(schema, {{{1, 10}, {2, 20}, {3, 30}, {4, 40}}}));
    TopNBlockIterator topn(std::move(openers), 2, {0}, false, {}, {});
    EXPECT_EQ(read(topn, *schema), (std::vector<Row> {{1, 10}, {2, 20}}));
    EXPECT_EQ(batches_read, 2);
}

// With a limit of 2, the rows dropped from the first 2 are removed once there are more than 2 of
// them: after the third rowset the copied rows are 5 6 3 4 1 2, of which only 1 2 are kept, and
// the fourth rowset goes on from those.
TEST(TopNBlockIteratorTest, RemovesTheDroppedRowsOnceTheyOutnumberTheKeptOnes) {
    auto schema = make_schema();
    std::vector<int64_t> filter_values;
    EXPECT_EQ(read_topn(schema,
                        {{{{5, 50}, {6, 60}}},
                         {{{3, 30}, {4, 40}}},
                         {{{1, 10}, {2, 20}}},
                         {{{0, 0}, {9, 90}}}},
                        2, {0}, false, &filter_values),
              (std::vector<Row> {{0, 0}, {1, 10}}));
    EXPECT_EQ(filter_values, (std::vector<int64_t> {6, 4, 2, 1}));
}

// The segments of an overlapping rowset are streams read in turn, with no merge: with a limit of
// 3, the streams 1 4 7 and 2 5 8 of the first rowset keep 1 2 4, and the second rowset 0 3 keeps
// 0 1 2. The filter goes to 7, 4, 2.
TEST(TopNBlockIteratorTest, ReadsTheSegmentsOfAnOverlappingRowsetInTurn) {
    auto schema = make_schema();
    std::vector<int64_t> filter_values;
    std::vector<TopNBlockIterator::RowsetOpener> openers;
    openers.push_back(open_overlapping_rowset(
            schema, {{{{1, 10}, {4, 40}, {7, 70}}}, {{{2, 20}, {5, 50}, {8, 80}}}}));
    openers.push_back(open_rowset(schema, {{{0, 0}, {3, 30}}}));
    TopNBlockIterator topn(std::move(openers), 3, {0}, false, {}, [&](const Field& value) {
        filter_values.push_back(value.get<TYPE_BIGINT>());
        return Status::OK();
    });
    EXPECT_EQ(read(topn, *schema), (std::vector<Row> {{0, 0}, {1, 10}, {2, 20}}));
    EXPECT_EQ(filter_values, (std::vector<int64_t> {7, 4, 2}));
}

// The segments fill the empty placeholder of a virtual column in the Block passed in with a
// column of their own; the rows kept get a column of the virtual column's type, so that its values
// are kept too.
TEST(TopNBlockIteratorTest, KeepsTheValuesOfVirtualColumns) {
    auto schema = make_schema(/*virtual_column=*/true);
    std::vector<TopNBlockIterator::RowsetOpener> openers;
    openers.push_back(open_overlapping_rowset(
            schema, {{{{1, 10, 100}, {3, 30, 300}}}, {{{2, 20, 200}, {4, 40, 400}}}}));
    TopNBlockIterator topn(std::move(openers), 3, {0}, false,
                           {{2, std::make_shared<DataTypeInt64>()}}, {});
    EXPECT_EQ(read(topn, *schema), (std::vector<Row> {{1, 10, 100}, {2, 20, 200}, {3, 30, 300}}));
}

// The errors of opening a rowset, of reading it and of tightening the filter end the read.
TEST(TopNBlockIteratorTest, PassesOnTheErrors) {
    auto schema = make_schema();
    auto next_block = [&](std::vector<TopNBlockIterator::RowsetOpener> openers,
                          TopNBlockIterator::FilterUpdater update_filter) {
        TopNBlockIterator topn(std::move(openers), 1, {0}, false, {}, std::move(update_filter));
        Block block = schema->create_read_block();
        bool eof = false;
        return topn.next_block(&block, &eof);
    };

    std::vector<TopNBlockIterator::RowsetOpener> openers;
    openers.push_back(open_rowset(schema, {{{1, 10}}}));
    openers.push_back(
            [](std::vector<RowwiseIteratorUPtr>*) { return Status::InternalError("open failed"); });
    Status st = next_block(std::move(openers), {});
    EXPECT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>()) << st;
    EXPECT_NE(st.to_string().find("open failed"), std::string::npos) << st;

    openers.clear();
    openers.push_back(open_rowset(schema, Batches {}, Status::IOError("read failed")));
    st = next_block(std::move(openers), {});
    EXPECT_TRUE(st.is<ErrorCode::IO_ERROR>()) << st;

    openers.clear();
    openers.push_back(open_rowset(schema, {{{1, 10}}}));
    st = next_block(std::move(openers),
                    [](const Field&) { return Status::InternalError("filter failed"); });
    EXPECT_NE(st.to_string().find("filter failed"), std::string::npos) << st;
}

} // namespace doris
