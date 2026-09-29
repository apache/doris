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

#include "storage/iterator/vgeneric_iterators.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/field.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/runtime_state.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/olap_tuple.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/null_predicate.h"
#include "storage/row_cursor.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/segment/test_segment_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet/tablet_schema_helper.h"

namespace doris {
using namespace ErrorCode;

class VGenericIteratorsTest : public testing::Test {
public:
    VGenericIteratorsTest() {}
    virtual ~VGenericIteratorsTest() {}
};

static Schema create_schema() {
    std::vector<TabletColumnPtr> col_schemas;
    auto c1 = std::make_shared<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                             FieldType::OLAP_FIELD_TYPE_SMALLINT, true);
    c1->set_is_key(true);
    col_schemas.emplace_back(c1);
    // c2: int
    auto c2 = std::make_shared<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                             FieldType::OLAP_FIELD_TYPE_INT, true);
    c2->set_is_key(true);
    col_schemas.emplace_back(c2);
    // c3: big int
    col_schemas.emplace_back(
            std::make_shared<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM,
                                           FieldType::OLAP_FIELD_TYPE_BIGINT, true));

    std::vector<ColumnId> column_ids(col_schemas.size());
    for (uint32_t cid = 0; cid < column_ids.size(); ++cid) {
        column_ids[cid] = cid;
    }

    Schema schema(col_schemas, column_ids);
    return schema;
}

static void create_block(Schema& schema, Block& block) {
    for (auto& column_desc : schema.columns()) {
        EXPECT_TRUE(column_desc);
        auto data_type = Schema::get_data_type_ptr(*column_desc);
        EXPECT_NE(data_type, nullptr);
        auto column = data_type->create_column();
        ColumnWithTypeAndName ctn(std::move(column), data_type, column_desc->name());
        block.insert(ctn);
    }
}

TEST(VGenericIteratorsTest, AutoIncrement) {
    auto schema = create_schema();
    auto iter = new_auto_increment_iterator(schema, 10);

    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    create_block(schema, block);

    auto ret = iter->next_batch(&block);
    EXPECT_TRUE(ret.ok());
    EXPECT_EQ(block.rows(), 10);

    auto c0 = block.get_by_position(0).column;
    auto c1 = block.get_by_position(1).column;
    auto c2 = block.get_by_position(2).column;

    int row_count = 0;
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i) {
        EXPECT_EQ(row_count, (*c0)[i].get<TYPE_SMALLINT>());
        EXPECT_EQ(row_count + 1, (*c1)[i].get<TYPE_INT>());
        EXPECT_EQ(row_count + 2, (*c2)[i].get<TYPE_BIGINT>());
        row_count++;
    }
}

TEST(VGenericIteratorsTest, StatisticsIteratorPreservesNullForNullableChar) {
    constexpr auto test_dir = "./ut_dir/vgeneric_iterators_test";
    constexpr auto segment_path = "./ut_dir/vgeneric_iterators_test/nullable_char_segment.dat";
    constexpr auto row_count = 3;

    auto fs = io::global_local_filesystem();
    ASSERT_TRUE(fs->delete_directory(test_dir).ok());
    ASSERT_TRUE(fs->create_directory(test_dir).ok());

    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*create_int_key(0, false));
    auto nullable_char = std::make_shared<TabletColumn>();
    nullable_char->set_unique_id(1);
    nullable_char->set_name("1");
    nullable_char->set_type(FieldType::OLAP_FIELD_TYPE_CHAR);
    nullable_char->set_is_nullable(true);
    nullable_char->set_length(8);
    nullable_char->set_index_length(8);
    nullable_char->set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    tablet_schema->append_column(*nullable_char);
    tablet_schema->set_storage_page_size(4096);

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(fs->create_file(segment_path, &file_writer).ok());
    SegmentWriterOptions writer_options;
    writer_options.num_rows_per_block = 1024;
    TestSegmentWriter writer(file_writer.get(), 0, tablet_schema, nullptr, nullptr, writer_options,
                             nullptr);
    ASSERT_TRUE(writer.init().ok());

    RowCursor row;
    std::vector<Field> fields(tablet_schema->num_columns(), Field(PrimitiveType::TYPE_NULL));
    ASSERT_TRUE(row.init_scan_key(tablet_schema, std::move(fields)).ok());
    for (int i = 0; i < row_count; ++i) {
        row.mutable_field(0) = Field::create_field<TYPE_INT>(i);
        ASSERT_TRUE(writer.append_row(row).ok());
    }
    uint64_t file_size = 0;
    uint64_t index_size = 0;
    ASSERT_TRUE(writer.finalize(&file_size, &index_size).ok());
    ASSERT_TRUE(file_writer->close().ok());

    std::shared_ptr<segment_v2::Segment> segment;
    ASSERT_TRUE(segment_v2::Segment::open(fs, segment_path, 100, 0, RowsetId {.version = 1},
                                          tablet_schema, io::FileReaderOptions {}, &segment)
                        .ok());

    std::vector<ColumnId> column_ids {0, 1};
    Schema schema(tablet_schema->columns(), column_ids);
    VStatisticsIterator iterator(segment, schema);
    StorageReadOptions read_options;
    OlapReaderStatistics stats;
    read_options.push_down_agg_type_opt = TPushAggOp::MINMAX;
    read_options.stats = &stats;
    read_options.tablet_schema = tablet_schema;
    ASSERT_TRUE(iterator.init(read_options).ok());

    Block block;
    create_block(schema, block);
    ASSERT_TRUE(iterator.next_batch(&block).ok());
    ASSERT_EQ(2, block.rows());

    const auto& nullable_column =
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    ASSERT_TRUE(iterator.next_batch(&block).is<ErrorCode::END_OF_FILE>());

    ASSERT_TRUE(fs->delete_directory(test_dir).ok());
}

// A string zone map bound is cut to 512 bytes, and a cut bound is not a value the column holds:
// the min is a prefix of the smallest value and the max was raised past the largest one. FE pushes
// MIN/MAX down for every string column, so the segment is the one that has to notice and hand the
// query back to a normal read.
class StatisticsIteratorStringBoundsTest : public testing::Test {
protected:
    static constexpr auto kTestDir = "./ut_dir/statistics_string_bounds_test";

    void SetUp() override {
        _fs = io::global_local_filesystem();
        ASSERT_TRUE(_fs->delete_directory(kTestDir).ok());
        ASSERT_TRUE(_fs->create_directory(kTestDir).ok());
    }
    void TearDown() override { EXPECT_TRUE(_fs->delete_directory(kTestDir).ok()); }

    static TabletSchemaSPtr make_schema() {
        auto tablet_schema = std::make_shared<TabletSchema>();
        TabletColumn key;
        key.set_name("c1");
        key.set_unique_id(0);
        key.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        key.set_length(4);
        key.set_index_length(4);
        key.set_is_key(true);
        key.set_is_nullable(false);
        tablet_schema->append_column(key);

        TabletColumn value;
        value.set_name("c2");
        value.set_unique_id(1);
        value.set_type(FieldType::OLAP_FIELD_TYPE_VARCHAR);
        value.set_length(65535);
        value.set_is_key(false);
        value.set_is_nullable(false);
        value.set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
        tablet_schema->append_column(value);
        tablet_schema->set_storage_page_size(4096);
        return tablet_schema;
    }

    // Writes one segment holding `values` in the VARCHAR column and returns the iterator that the
    // pushed-down `agg` would run on. `accept_cut_bound` is what statistics collection sets:
    // it takes an inexact min/max as an approximation instead of reading the data.
    // `with_delete` adds a delete predicate, which leaves the zone map covering removed rows.
    std::unique_ptr<RowwiseIterator> pushdown_iterator_for(
            const std::string& name, const std::vector<std::string>& values,
            bool accept_cut_bound = false, bool with_delete = false,
            TPushAggOp::type agg = TPushAggOp::MINMAX) {
        auto tablet_schema = make_schema();
        const std::string segment_path = std::string(kTestDir) + "/" + name + ".dat";

        io::FileWriterPtr file_writer;
        EXPECT_TRUE(_fs->create_file(segment_path, &file_writer).ok());
        SegmentWriterOptions writer_options;
        writer_options.num_rows_per_block = 1024;
        TestSegmentWriter writer(file_writer.get(), 0, tablet_schema, nullptr, nullptr,
                                 writer_options, nullptr);
        EXPECT_TRUE(writer.init().ok());

        RowCursor row;
        OlapTuple tuple;
        for (size_t i = 0; i < tablet_schema->num_columns(); ++i) {
            tuple.add_null();
        }
        EXPECT_EQ(Status::OK(), row.init(tablet_schema, tuple));
        for (size_t i = 0; i < values.size(); ++i) {
            row.mutable_field(0) = Field::create_field<TYPE_INT>(static_cast<int32_t>(i));
            row.mutable_field(1) = Field::create_field<TYPE_STRING>(String(values[i]));
            EXPECT_TRUE(writer.append_row(row).ok());
        }
        uint64_t file_size = 0;
        uint64_t index_size = 0;
        EXPECT_TRUE(writer.finalize(&file_size, &index_size).ok());
        EXPECT_TRUE(file_writer->close().ok());

        std::shared_ptr<segment_v2::Segment> segment;
        EXPECT_TRUE(segment_v2::Segment::open(_fs, segment_path, 100, 0, RowsetId {.version = 1},
                                              tablet_schema, io::FileReaderOptions {}, &segment)
                            .ok());

        std::vector<ColumnId> column_ids {0, 1};
        // VStatisticsIterator keeps a reference to the schema, so it has to outlive the iterator.
        auto schema = std::make_shared<Schema>(tablet_schema->columns(), column_ids);
        StorageReadOptions read_options;
        read_options.push_down_agg_type_opt = agg;
        read_options.stats = &_stats;
        read_options.tablet_schema = tablet_schema;

        if (with_delete) {
            auto del_pred = NullPredicate::create_shared(0, "c1", true, PrimitiveType::TYPE_INT);
            read_options.delete_condition_predicates->add_column_predicate(
                    SingleColumnBlockPredicate::create_unique(del_pred));
        }

        auto state = std::make_unique<RuntimeState>();
        TQueryOptions query_options;
        query_options.__set_force_pushdown_zonemap_minmax(accept_cut_bound);
        state->set_query_options(query_options);
        read_options.runtime_state = state.get();
        // The iterator keeps a copy of read_options, so the state has to outlive it.
        _states.push_back(std::move(state));
        _schemas.push_back(schema);

        std::unique_ptr<RowwiseIterator> iter;
        EXPECT_TRUE(segment->new_iterator(schema, read_options, &iter).ok());
        return iter;
    }

    std::shared_ptr<io::FileSystem> _fs;
    OlapReaderStatistics _stats;
    std::vector<std::unique_ptr<RuntimeState>> _states;
    std::vector<SchemaSPtr> _schemas;
};

TEST_F(StatisticsIteratorStringBoundsTest, ShortBoundsAnswerFromTheZoneMap) {
    // Every value fits well inside the 512-byte bound, so the stored min/max are the real ones.
    auto iter = pushdown_iterator_for("short", {"aaa", "bbb", "ccc"});
    EXPECT_NE(dynamic_cast<VStatisticsIterator*>(iter.get()), nullptr)
            << "exact bounds can answer MIN/MAX without reading the data";
}

TEST_F(StatisticsIteratorStringBoundsTest, CutBoundsFallBackToReadingTheData) {
    // The longest value runs past the 512-byte cut, so the stored max is a raised prefix and not a
    // value in the column. Answering MIN/MAX from it would return a string the table never held.
    auto iter = pushdown_iterator_for("cut", {"aaa", "bbb", std::string(600, 'c')});
    EXPECT_EQ(dynamic_cast<VStatisticsIterator*>(iter.get()), nullptr)
            << "a cut bound is not a value from the data, so the query has to read the rows";
}

// A VARCHAR(512) column full to its declared length was cut too, and FE used to push MIN/MAX down
// for it because the length is not over 512.
TEST_F(StatisticsIteratorStringBoundsTest, BoundsCutExactlyAtTheLimitFallBack) {
    auto iter = pushdown_iterator_for("exact", {"aaa", std::string(MAX_ZONE_MAP_INDEX_SIZE, 'z')});
    EXPECT_EQ(dynamic_cast<VStatisticsIterator*>(iter.get()), nullptr);
}

// Statistics collection only needs an approximation, and reading the data instead would scan the
// whole table. It keeps the statistics iterator even when the stored bounds were cut.
TEST_F(StatisticsIteratorStringBoundsTest, CutBoundsAnswerWhenTheCallerTakesAnApproximation) {
    auto iter = pushdown_iterator_for("cut_approx", {"aaa", "bbb", std::string(600, 'c')},
                                      /*accept_cut_bound=*/true);
    EXPECT_NE(dynamic_cast<VStatisticsIterator*>(iter.get()), nullptr)
            << "statistics collection reads the cut bound rather than scanning the rows";
}

// A max raised from 0xff wraps to 0x00, so the read side turns pass_all on for that zone. The
// bounds were parsed before that happened, so statistics collection still reads them.
TEST_F(StatisticsIteratorStringBoundsTest, PassAllZoneMapAnswersWhenApproximationIsAccepted) {
    std::string wrapping(MAX_ZONE_MAP_INDEX_SIZE - 1, 'a');
    wrapping.push_back(static_cast<char>(0xff));
    auto iter = pushdown_iterator_for("pass_all_approx", {"aaa", wrapping},
                                      /*accept_cut_bound=*/true);
    EXPECT_NE(dynamic_cast<VStatisticsIterator*>(iter.get()), nullptr)
            << "a zone map that gave up its range on read still carries the bounds it parsed";

    Block block;
    for (const auto& column : iter->schema().columns()) {
        auto data_type = column->get_vec_type();
        block.insert(ColumnWithTypeAndName(data_type->create_column(), data_type, column->name()));
    }
    EXPECT_TRUE(iter->next_batch(&block).ok()) << "reading the bounds must not trip an assertion";
}

// With the switch off the same zone map sends the query back to the rows.
TEST_F(StatisticsIteratorStringBoundsTest, PassAllZoneMapFallsBackToReadingTheData) {
    std::string wrapping(MAX_ZONE_MAP_INDEX_SIZE - 1, 'a');
    wrapping.push_back(static_cast<char>(0xff));
    auto iter = pushdown_iterator_for("pass_all_exact", {"aaa", wrapping});
    EXPECT_EQ(dynamic_cast<VStatisticsIterator*>(iter.get()), nullptr);
}

// A delete predicate leaves the zone map covering rows that are gone, so its min/max may name a
// value the table no longer holds. That is a real answer for every query but statistics
// collection, which takes the approximation to avoid scanning the table.
TEST_F(StatisticsIteratorStringBoundsTest, DeletePredicateFallsBackToReadingTheData) {
    auto iter = pushdown_iterator_for("del_exact", {"aaa", "bbb"}, /*accept_cut_bound=*/false,
                                      /*with_delete=*/true);
    EXPECT_EQ(dynamic_cast<VStatisticsIterator*>(iter.get()), nullptr)
            << "a deleted row may still sit inside the zone map bounds";
}

TEST_F(StatisticsIteratorStringBoundsTest, DeletePredicateAnswersWhenApproximationIsAccepted) {
    auto iter = pushdown_iterator_for("del_approx", {"aaa", "bbb"}, /*accept_cut_bound=*/true,
                                      /*with_delete=*/true);
    EXPECT_NE(dynamic_cast<VStatisticsIterator*>(iter.get()), nullptr)
            << "statistics collection keeps the zone map even with a delete predicate";
}

TEST_F(StatisticsIteratorStringBoundsTest, CountKeepsTheDeletePredicateGuardWhenForced) {
    auto iter = pushdown_iterator_for("count_del", {"aaa", "bbb"}, /*accept_cut_bound=*/true,
                                      /*with_delete=*/true, TPushAggOp::COUNT);
    EXPECT_EQ(dynamic_cast<VStatisticsIterator*>(iter.get()), nullptr)
            << "COUNT reports the segment row count, which still counts the deleted rows";
}

TEST(VGenericIteratorsTest, Union) {
    auto schema = create_schema();
    auto output_schema = std::make_shared<Schema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;

    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));

    auto iter = new_union_iterator(std::move(inputs), output_schema);
    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    create_block(schema, block);

    do {
        st = iter->next_batch(&block);
    } while (st.ok());

    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(block.rows(), 600);

    auto c0 = block.get_by_position(0).column;
    auto c1 = block.get_by_position(1).column;
    auto c2 = block.get_by_position(2).column;

    size_t row_count = 0;
    for (int i = 0; i < block.rows(); ++i) {
        size_t base_value = row_count;
        if (row_count >= 300) {
            base_value -= 300;
        } else if (i >= 100) {
            base_value -= 100;
        }

        EXPECT_EQ(base_value, (*c0)[i].get<TYPE_SMALLINT>());
        EXPECT_EQ(base_value + 1, (*c1)[i].get<TYPE_INT>());
        EXPECT_EQ(base_value + 2, (*c2)[i].get<TYPE_BIGINT>());
        row_count++;
    }
}

TEST(VGenericIteratorsTest, MergeAgg) {
    EXPECT_TRUE(1);
    auto schema = create_schema();
    auto output_schema = std::make_shared<Schema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;

    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));

    auto iter = new_merge_iterator(std::move(inputs), -1, false, false, nullptr, output_schema);
    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    std::vector<bool> row_is_same;
    BlockWithSameBit block_with_same_bit {.block = &block, .same_bit = row_is_same};
    create_block(schema, block);

    do {
        st = iter->next_batch(&block_with_same_bit);
    } while (st.ok());

    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(block.rows(), 600);

    auto c0 = block.get_by_position(0).column;
    auto c1 = block.get_by_position(1).column;
    auto c2 = block.get_by_position(2).column;

    size_t row_count = 0;
    for (size_t i = 0; i < block.rows(); ++i) {
        size_t base_value = row_count;
        // 100 * 3, 200 * 2, 300
        if (row_count < 300) {
            base_value = row_count / 3;
        } else if (row_count < 500) {
            base_value = (row_count - 300) / 2 + 100;
        } else {
            base_value = row_count - 300;
        }

        EXPECT_EQ(base_value, (*c0)[i].get<TYPE_SMALLINT>());
        EXPECT_EQ(base_value + 1, (*c1)[i].get<TYPE_INT>());
        EXPECT_EQ(base_value + 2, (*c2)[i].get<TYPE_BIGINT>());
        row_count++;
    }
}

TEST(VGenericIteratorsTest, MergeUnique) {
    EXPECT_TRUE(1);
    auto schema = create_schema();
    auto output_schema = std::make_shared<Schema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;

    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));

    auto iter = new_merge_iterator(std::move(inputs), -1, true, false, nullptr, output_schema);
    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    std::vector<bool> row_is_same;
    BlockWithSameBit block_with_same_bit {.block = &block, .same_bit = row_is_same};
    create_block(schema, block);

    do {
        st = iter->next_batch(&block_with_same_bit);
    } while (st.ok());

    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(block.rows(), 300);

    auto c0 = block.get_by_position(0).column;
    auto c1 = block.get_by_position(1).column;
    auto c2 = block.get_by_position(2).column;

    size_t row_count = 0;
    for (size_t i = 0; i < block.rows(); ++i) {
        size_t base_value = row_count;

        EXPECT_EQ(base_value, (*c0)[i].get<TYPE_SMALLINT>());
        EXPECT_EQ(base_value + 1, (*c1)[i].get<TYPE_INT>());
        EXPECT_EQ(base_value + 2, (*c2)[i].get<TYPE_BIGINT>());
        row_count++;
    }
}

// only used for Seq Column UT
class SeqColumnUtIterator : public RowwiseIterator {
public:
    // Will generate num_rows rows in total
    SeqColumnUtIterator(const Schema& schema, size_t num_rows, size_t rows_returned,
                        size_t seq_col_idx, size_t seq_col_rows_returned)
            : _schema(schema),
              _num_rows(num_rows),
              _rows_returned(rows_returned),
              _seq_col_idx(seq_col_idx),
              _seq_col_rows_returned(seq_col_rows_returned) {}
    ~SeqColumnUtIterator() override {}

    // NOTE: Currently, this function will ignore StorageReadOptions
    Status init(const StorageReadOptions& opts) override { return Status::OK(); }

    Status next_batch(Block* block) override {
        int row_idx = 0;
        while (_rows_returned < _num_rows) {
            for (int j = 0; j < _schema.num_columns(); ++j) {
                ColumnWithTypeAndName& vc = block->get_by_position(j);
                IColumn& vi = (IColumn&)(*vc.column);

                char data[16] = {};
                size_t data_len = 0;
                const auto* col_schema = _schema.column(j);
                switch (col_schema->type()) {
                case FieldType::OLAP_FIELD_TYPE_SMALLINT:
                    *(int16_t*)data = j == _seq_col_idx ? _seq_col_rows_returned : 1;
                    data_len = sizeof(int16_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_INT:
                    *(int32_t*)data = j == _seq_col_idx ? _seq_col_rows_returned : 1;
                    data_len = sizeof(int32_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_BIGINT:
                    *(int64_t*)data = j == _seq_col_idx ? _seq_col_rows_returned : 1;
                    data_len = sizeof(int64_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_FLOAT:
                    *(float*)data = j == _seq_col_idx ? _seq_col_rows_returned : 1;
                    data_len = sizeof(float);
                    break;
                case FieldType::OLAP_FIELD_TYPE_DOUBLE:
                    *(double*)data = j == _seq_col_idx ? _seq_col_rows_returned : 1;
                    data_len = sizeof(double);
                    break;
                default:
                    break;
                }

                vi.insert_data(data, data_len);
            }

            ++_rows_returned;
            _seq_col_rows_returned++;
            row_idx++;
        }

        if (row_idx > 0) return Status::OK();
        return Status::EndOfFile("End of VAutoIncrementIterator");
    }

    const Schema& schema() const override { return _schema; }

    const Schema& _schema;
    size_t _num_rows;
    size_t _rows_returned;
    int _seq_col_idx = -1;
    int _seq_col_rows_returned = -1;
};

TEST(VGenericIteratorsTest, MergeWithSeqColumn) {
    EXPECT_TRUE(1);
    auto schema = create_schema();
    auto output_schema = std::make_shared<Schema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;

    int seq_column_id = 2;
    int seg_iter_num = 10;
    int num_rows = 1;
    int rows_begin = 0;
    // The same key in each file will only keep one with the largest seq id
    // keep the key columns all the same, but seq column value different
    // input seg file in Ascending,  expect output seq column in Descending
    for (int i = 0; i < seg_iter_num; i++) {
        int seq_id_in_every_file = i;
        inputs.push_back(std::make_unique<SeqColumnUtIterator>(
                schema, num_rows, rows_begin, seq_column_id, seq_id_in_every_file));
    }

    auto iter = new_merge_iterator(std::move(inputs), seq_column_id, true, false, nullptr,
                                   output_schema);
    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    std::vector<bool> row_is_same;
    BlockWithSameBit block_with_same_bit {.block = &block, .same_bit = row_is_same};
    create_block(schema, block);

    do {
        st = iter->next_batch(&block_with_same_bit);
    } while (st.ok());

    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(block.rows(), 1);

    auto col0 = block.get_by_position(0).column;
    auto col1 = block.get_by_position(1).column;
    auto seq_col = block.get_by_position(seq_column_id).column;
    size_t actual_value = (*seq_col)[0].get<TYPE_BIGINT>();
    EXPECT_EQ(seg_iter_num - 1, actual_value);
}

} // namespace doris
