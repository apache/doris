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

#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/cast_set.h"
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

namespace {

// Generates `num_rows` ordered rows in the layout of `schema`: row r holds r + j in column j, so
// (int, int) gives (0, 1), (1, 2), (2, 3), ... It hands every row out in its first next_batch()
// and ignores the StorageReadOptions; the tests below rely on both.
class AutoIncrementUtIterator : public RowwiseIterator {
public:
    AutoIncrementUtIterator(const ReadSchema& schema, size_t num_rows)
            : _schema(schema), _num_rows(num_rows) {}
    ~AutoIncrementUtIterator() override = default;

    Status init(const StorageReadOptions& opts) override { return Status::OK(); }

    Status next_batch(Block* block) override {
        if (_rows_returned == _num_rows) {
            return Status::EndOfFile("End of AutoIncrementUtIterator");
        }
        auto columns_guard = block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        for (; _rows_returned < _num_rows; ++_rows_returned) {
            for (size_t j = 0; j < _schema.num_block_columns(); ++j) {
                char data[16] = {};
                size_t data_len = 0;
                switch (_schema.column(j)->type()) {
                case FieldType::OLAP_FIELD_TYPE_SMALLINT:
                    *(int16_t*)data = cast_set<int16_t>(_rows_returned + j);
                    data_len = sizeof(int16_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_INT:
                    *(int32_t*)data = cast_set<int32_t>(_rows_returned + j);
                    data_len = sizeof(int32_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_BIGINT:
                    *(int64_t*)data = cast_set<int64_t>(_rows_returned + j);
                    data_len = sizeof(int64_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_FLOAT:
                    *(float*)data = cast_set<float>(_rows_returned + j);
                    data_len = sizeof(float);
                    break;
                case FieldType::OLAP_FIELD_TYPE_DOUBLE:
                    *(double*)data = cast_set<double>(_rows_returned + j);
                    data_len = sizeof(double);
                    break;
                default:
                    break;
                }
                columns[j]->insert_data(data, data_len);
            }
        }
        return Status::OK();
    }

    const ReadSchema& schema() const override { return _schema; }

private:
    const ReadSchema& _schema;
    size_t _num_rows;
    size_t _rows_returned = 0;
};

RowwiseIteratorUPtr new_auto_increment_iterator(const ReadSchema& schema, size_t num_rows) {
    return std::make_unique<AutoIncrementUtIterator>(schema, num_rows);
}

} // namespace

class VGenericIteratorsTest : public testing::Test {
public:
    VGenericIteratorsTest() {}
    virtual ~VGenericIteratorsTest() {}
};

static std::vector<TabletColumnPtr> create_col_schemas() {
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
    return col_schemas;
}

static ReadSchema create_schema() {
    return ReadSchema(create_col_schemas());
}

// The MergeDecision TabletReader would make for a read compared by the whole key of `schema`:
// `sequence_ordinal` orders the rows of one key (-1 without one), `small_sequence_first` the
// smaller value first, `reverse` the keys descending, `drop_older_versions` keeps the newest
// row of a key only.
static MergeDecision merge_decision(const ReadSchema& schema, int sequence_ordinal,
                                    bool small_sequence_first, bool reverse,
                                    bool drop_older_versions) {
    MergeDecision decision;
    decision.key_ordered = true;
    for (size_t ordinal = 0; ordinal < schema.num_key_columns(); ++ordinal) {
        decision.compare_ordinals.push_back(static_cast<uint32_t>(ordinal));
    }
    decision.sequence_ordinal = sequence_ordinal;
    decision.small_sequence_first = small_sequence_first;
    decision.reverse = reverse;
    decision.drop_older_versions = drop_older_versions;
    return decision;
}

static void create_block(ReadSchema& schema, Block& block) {
    for (auto& column_desc : schema.columns()) {
        EXPECT_TRUE(column_desc);
        auto data_type = column_desc->get_vec_type();
        EXPECT_NE(data_type, nullptr);
        auto column = data_type->create_column();
        ColumnWithTypeAndName ctn(std::move(column), data_type, column_desc->name());
        block.insert(ctn);
    }
}

// The key of row `pos` of `block`: the first column of the test schema, a nullable SMALLINT.
static int64_t key_at(const Block& block, size_t pos) {
    return (*block.get_by_position(0).column)[pos].get<TYPE_SMALLINT>();
}

TEST(VGenericIteratorsTest, AutoIncrement) {
    auto schema = create_schema();
    auto iter = new_auto_increment_iterator(schema, 10);

    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
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

// Writes one segment of an INT key and a nullable CHAR(8) column holding `values`, std::nullopt
// for NULL, and opens it. The writer pads every CHAR value to the declared 8 bytes, which is what
// the segment's zone map keeps as its bounds.
static std::shared_ptr<segment_v2::Segment> write_char_segment(
        const std::string& segment_path, const std::vector<std::optional<std::string>>& values,
        TabletSchemaSPtr* tablet_schema) {
    auto fs = io::global_local_filesystem();
    *tablet_schema = std::make_shared<TabletSchema>();
    (*tablet_schema)->append_column(*create_int_key(0, false));
    auto nullable_char = std::make_shared<TabletColumn>();
    nullable_char->set_unique_id(1);
    nullable_char->set_name("1");
    nullable_char->set_type(FieldType::OLAP_FIELD_TYPE_CHAR);
    nullable_char->set_is_nullable(true);
    nullable_char->set_length(8);
    nullable_char->set_index_length(8);
    nullable_char->set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    (*tablet_schema)->append_column(*nullable_char);
    (*tablet_schema)->set_storage_page_size(4096);

    io::FileWriterPtr file_writer;
    EXPECT_TRUE(fs->create_file(segment_path, &file_writer).ok());
    VerticalSegmentWriterOptions writer_options;
    writer_options.num_rows_per_block = 1024;
    TestVerticalSegmentWriter writer(file_writer.get(), 0, *tablet_schema, nullptr, nullptr,
                                     writer_options, nullptr);
    EXPECT_TRUE(writer.init().ok());

    RowCursor row;
    std::vector<Field> fields((*tablet_schema)->num_columns(), Field(PrimitiveType::TYPE_NULL));
    EXPECT_TRUE(row.init_scan_key(*tablet_schema, std::move(fields)).ok());
    for (size_t i = 0; i < values.size(); ++i) {
        row.mutable_field(0) = Field::create_field<TYPE_INT>(static_cast<int32_t>(i));
        row.mutable_field(1) = values[i].has_value()
                                       ? Field::create_field<TYPE_STRING>(String(*values[i]))
                                       : Field(PrimitiveType::TYPE_NULL);
        EXPECT_TRUE(writer.append_row(row).ok());
    }
    uint64_t file_size = 0;
    uint64_t index_size = 0;
    EXPECT_TRUE(writer.finalize_columns(&index_size).ok());
    EXPECT_TRUE(writer.finalize_footer(&file_size).ok());
    EXPECT_TRUE(file_writer->close().ok());

    std::shared_ptr<segment_v2::Segment> segment;
    EXPECT_TRUE(segment_v2::Segment::open(fs, segment_path, 100, 0, RowsetId {.version = 1},
                                          *tablet_schema, io::FileReaderOptions {}, &segment)
                        .ok());
    return segment;
}

class StatisticsIteratorCharTest : public testing::Test {
protected:
    static constexpr auto kTestDir = "./ut_dir/vgeneric_iterators_test";

    void SetUp() override {
        _fs = io::global_local_filesystem();
        ASSERT_TRUE(_fs->delete_directory(kTestDir).ok());
        ASSERT_TRUE(_fs->create_directory(kTestDir).ok());
    }
    void TearDown() override { EXPECT_TRUE(_fs->delete_directory(kTestDir).ok()); }

    // Answers the pushed-down `agg` over a segment holding `values` in the CHAR column: the
    // first Block it hands out goes to `block`, and the read must be over after it.
    void read_statistics(const std::vector<std::optional<std::string>>& values,
                         TPushAggOp::type agg, Block* block) {
        TabletSchemaSPtr tablet_schema;
        auto segment = write_char_segment(std::string(kTestDir) + "/char_segment.dat", values,
                                          &tablet_schema);
        ASSERT_NE(segment, nullptr);
        std::vector<ColumnId> column_ids {0, 1};
        ReadSchema schema(project_columns_by_ordinal(tablet_schema->columns(), column_ids));
        std::unique_ptr<RowwiseIterator> iterator =
                std::make_unique<VStatisticsIterator>(segment, schema);
        OlapReaderStatistics stats;
        StorageReadOptions read_options(stats);
        read_options.push_down_agg_type_opt = agg;
        ASSERT_TRUE(iterator->init(read_options).ok());

        create_block(schema, *block);
        ASSERT_TRUE(iterator->next_batch(block).ok());
        Block after_last;
        create_block(schema, after_last);
        ASSERT_TRUE(iterator->next_batch(&after_last).is<ErrorCode::END_OF_FILE>());
    }

    std::shared_ptr<io::FileSystem> _fs;
};

TEST_F(StatisticsIteratorCharTest, PreservesNullForNullableChar) {
    Block block;
    read_statistics({std::nullopt, std::nullopt, std::nullopt}, TPushAggOp::MINMAX, &block);
    ASSERT_EQ(2, block.rows());

    const auto& nullable_column =
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
}

// CHAR(8) stores "a" as "a" plus seven '\0'. MIN/MAX answered from the zone map must come out as
// the rows of a normal read do: without the padding.
TEST_F(StatisticsIteratorCharTest, AnswersCharBoundsWithoutPadding) {
    Block block;
    read_statistics({"bb", "a", "ccc"}, TPushAggOp::MINMAX, &block);
    ASSERT_EQ(2, block.rows());

    // next_batch_of_zone_map hands out the max first, then the min.
    const auto& char_column = *block.get_by_position(1).column;
    EXPECT_EQ("ccc", char_column.get_data_at(0).to_string());
    EXPECT_EQ("a", char_column.get_data_at(1).to_string());
}

// COUNT reports the segment's row count and reads no zone map, so every row is a default.
TEST_F(StatisticsIteratorCharTest, CountFillsTheRowCount) {
    Block block;
    read_statistics({"bb", std::nullopt, "ccc"}, TPushAggOp::COUNT, &block);
    EXPECT_EQ(3, block.rows());
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
        VerticalSegmentWriterOptions writer_options;
        writer_options.num_rows_per_block = 1024;
        TestVerticalSegmentWriter writer(file_writer.get(), 0, tablet_schema, nullptr, nullptr,
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
        EXPECT_TRUE(writer.finalize_columns(&index_size).ok());
        EXPECT_TRUE(writer.finalize_footer(&file_size).ok());
        EXPECT_TRUE(file_writer->close().ok());

        std::shared_ptr<segment_v2::Segment> segment;
        EXPECT_TRUE(segment_v2::Segment::open(_fs, segment_path, 100, 0, RowsetId {.version = 1},
                                              tablet_schema, io::FileReaderOptions {}, &segment)
                            .ok());

        std::vector<ColumnId> column_ids {0, 1};
        // VStatisticsIterator keeps a reference to the schema, so it has to outlive the iterator.
        auto schema = std::make_shared<ReadSchema>(
                project_columns_by_ordinal(tablet_schema->columns(), column_ids));
        StorageReadOptions read_options(_stats);
        read_options.push_down_agg_type_opt = agg;

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
    std::vector<ReadSchemaSPtr> _schemas;
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
    auto output_schema = std::make_shared<ReadSchema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;

    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));

    auto iter = new_union_iterator(std::move(inputs), output_schema);
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
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
    auto output_schema = std::make_shared<ReadSchema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;

    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));

    auto iter = new_merge_iterator(std::move(inputs), output_schema,
                                   merge_decision(*output_schema, -1, false, false, false));
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
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
    auto output_schema = std::make_shared<ReadSchema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;

    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));

    auto iter = new_merge_iterator(
            std::move(inputs), output_schema,
            merge_decision(*output_schema, -1, false, false, /*skip_same=*/true));
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    create_block(schema, block);

    do {
        st = iter->next_batch(&block);
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
    SeqColumnUtIterator(const ReadSchema& schema, size_t num_rows, size_t rows_returned,
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
            for (int j = 0; j < _schema.num_block_columns(); ++j) {
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
                case FieldType::OLAP_FIELD_TYPE_STRING:
                    std::memcpy(data, &_seq_col_rows_returned, sizeof(_seq_col_rows_returned));
                    data_len = sizeof(_seq_col_rows_returned);
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

    const ReadSchema& schema() const override { return _schema; }

    const ReadSchema& _schema;
    size_t _num_rows;
    size_t _rows_returned;
    int _seq_col_idx = -1;
    int _seq_col_rows_returned = -1;
};

TEST(VGenericIteratorsTest, MergeWithSeqColumn) {
    EXPECT_TRUE(1);
    auto schema = create_schema();
    schema.append_compaction_rowid_column();
    auto output_schema = std::make_shared<ReadSchema>(schema);
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

    auto iter = new_merge_iterator(
            std::move(inputs), output_schema,
            merge_decision(*output_schema, seq_column_id, false, false, /*skip_same=*/true));
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    create_block(schema, block);

    do {
        st = iter->next_batch(&block);
    } while (st.ok());

    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(block.rows(), 1);

    auto col0 = block.get_by_position(0).column;
    auto col1 = block.get_by_position(1).column;
    auto seq_col = block.get_by_position(seq_column_id).column;
    size_t actual_value = (*seq_col)[0].get<TYPE_BIGINT>();
    EXPECT_EQ(seg_iter_num - 1, actual_value);

    const auto rowid = block.get_by_position(schema.rowid_ordinal()).column->get_data_at(0);
    ASSERT_EQ(sizeof(int), rowid.size);
    int rowid_value = 0;
    std::memcpy(&rowid_value, rowid.data, sizeof(rowid_value));
    EXPECT_EQ(seg_iter_num - 1, rowid_value);
}

// Mirror of MergeWithSeqColumn but with small_seq_first=true.
// Same key across all segments, seq values are 0..seg_iter_num-1; the merge
// iterator should keep exactly one row whose seq value is the smallest (0).
TEST(VGenericIteratorsTest, MergeWithSeqColumnSmallSeqFirst) {
    auto schema = create_schema();
    auto output_schema = std::make_shared<ReadSchema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;

    int seq_column_id = 2;
    int seg_iter_num = 10;
    int num_rows = 1;
    int rows_begin = 0;
    for (int i = 0; i < seg_iter_num; i++) {
        int seq_id_in_every_file = i;
        inputs.push_back(std::make_unique<SeqColumnUtIterator>(
                schema, num_rows, rows_begin, seq_column_id, seq_id_in_every_file));
    }

    // small_seq_first = true => smaller seq value sorts first / wins on tie.
    auto iter = new_merge_iterator(
            std::move(inputs), output_schema,
            merge_decision(*output_schema, seq_column_id, /*small_sequence_first=*/true,
                           /*is_reverse=*/false, /*skip_same=*/true));
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    create_block(schema, block);

    do {
        st = iter->next_batch(&block);
    } while (st.ok());

    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(block.rows(), 1);

    auto seq_col = block.get_by_position(seq_column_id).column;
    size_t actual_value = (*seq_col)[0].get<TYPE_BIGINT>();
    EXPECT_EQ(0, actual_value);
}

// Models the changelog quick-merge read path: the merge output rowset holds the
// same user key across multiple segments, each carrying a distinct changelog event
// (distinct TSO). A changelog scan reads it with is_unique=false and the TSO as
// the sequence column with small_seq_first=true. Unlike the is_unique=true cases
// above (which collapse same-key rows to one), the merge iterator must keep EVERY
// event and emit them ordered by ascending TSO, so a table-stream query over a
// quick-merge rowset returns the complete, ordered change chain without dropping
// any event.
TEST(VGenericIteratorsTest, MergeKeepsAllChangelogEventsOrderedByTso) {
    auto schema = create_schema();
    auto output_schema = std::make_shared<ReadSchema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;

    int seq_column_id = 2; // BIGINT column stands in for __DORIS_CHANGELOG_TSO__.
    int num_rows = 1;
    int rows_begin = 0;
    // Same key in every segment, each carrying a distinct TSO. The children are fed in a
    // deterministic SHUFFLED TSO order (not ascending), so the test only passes if the
    // merge actually orders by the TSO sequence column; a comparator that ignored the
    // sequence and fell back to child/data-id arrival order would emit this shuffled
    // sequence unchanged and fail the ascending-order assertion below.
    const std::vector<int> tso_feed_order = {7, 1, 9, 0, 5, 8, 2, 6, 4, 3};
    int seg_iter_num = static_cast<int>(tso_feed_order.size());
    for (int tso : tso_feed_order) {
        inputs.push_back(std::make_unique<SeqColumnUtIterator>(schema, num_rows, rows_begin,
                                                               seq_column_id, tso));
    }

    // is_unique=false keeps every same-key event; small_seq_first=true orders by
    // ascending TSO. This mirrors beta_rowset_reader's changelog merge setup.
    auto iter = new_merge_iterator(
            std::move(inputs), output_schema,
            merge_decision(*output_schema, seq_column_id, /*small_sequence_first=*/true,
                           /*is_reverse=*/false, /*skip_same=*/false));
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    create_block(schema, block);

    do {
        st = iter->next_batch(&block);
    } while (st.ok());

    EXPECT_TRUE(st.is<END_OF_FILE>());
    // Every event survives: no per-key dedup.
    EXPECT_EQ(block.rows(), seg_iter_num);

    // Despite the shuffled input order, events are emitted in ascending TSO order.
    auto seq_col = block.get_by_position(seq_column_id).column;
    for (int i = 0; i < seg_iter_num; ++i) {
        EXPECT_EQ(i, (*seq_col)[i].get<TYPE_BIGINT>());
    }
}

// Emits num_rows rows for a schema whose projection (col_ids) may be narrower than the
// full tablet schema. The filled block layout follows the projection, matching how
// VMergeIteratorContext::block_reset builds its block from the output schema.
class ProjectedColumnsUtIterator : public RowwiseIterator {
public:
    ProjectedColumnsUtIterator(ReadSchema schema, size_t num_rows)
            : _schema(std::move(schema)), _num_rows(num_rows) {}
    ~ProjectedColumnsUtIterator() override = default;

    Status init(const StorageReadOptions& opts) override { return Status::OK(); }

    Status next_batch(Block* block) override {
        if (_rows_returned >= _num_rows) {
            return Status::EndOfFile("End of ProjectedColumnsUtIterator");
        }
        while (_rows_returned < _num_rows) {
            for (size_t j = 0; j < _schema.num_block_columns(); ++j) {
                ColumnWithTypeAndName& vc = block->get_by_position(j);
                IColumn& vi = (IColumn&)(*vc.column);

                char data[16] = {};
                size_t data_len = 0;
                const auto* col_schema = _schema.column(j);
                switch (col_schema->type()) {
                case FieldType::OLAP_FIELD_TYPE_SMALLINT:
                    *(int16_t*)data = static_cast<int16_t>(_rows_returned);
                    data_len = sizeof(int16_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_INT:
                    *(int32_t*)data = static_cast<int32_t>(_rows_returned);
                    data_len = sizeof(int32_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_BIGINT:
                    *(int64_t*)data = static_cast<int64_t>(_rows_returned);
                    data_len = sizeof(int64_t);
                    break;
                default:
                    break;
                }

                vi.insert_data(data, data_len);
            }
            ++_rows_returned;
        }
        return Status::OK();
    }

    const ReadSchema& schema() const override { return _schema; }

private:
    ReadSchema _schema;
    size_t _num_rows;
    size_t _rows_returned = 0;
};

// The merge-heap comparator compares the leading num_key_columns() block positions when no
// explicit compare columns are given. The read schema must therefore place its key columns as
// the leading prefix; if a non-key column precedes a key column, init() must reject the merge
// with a "compare contract violated" error instead of silently comparing a value column as a key
// (issue #66390: a changelog APPEND_ONLY scan projected value columns ahead of a key).
TEST(VGenericIteratorsTest, MergeRejectsProjectionMissingKeyPrefix) {
    // Full tablet schema: k0(smallint), k1(int), v2(bigint). Project {v2, k0}: the schema has
    // one key column (k0) but it is not the leading column, so ordinal 0 is a non-key column.
    ReadSchema projected(
            project_columns_by_ordinal(create_col_schemas(), std::vector<ColumnId> {2, 0}));
    auto output_schema = std::make_shared<ReadSchema>(projected);

    std::vector<RowwiseIteratorUPtr> inputs;
    inputs.push_back(std::make_unique<ProjectedColumnsUtIterator>(projected, 10));
    inputs.push_back(std::make_unique<ProjectedColumnsUtIterator>(projected, 10));

    auto iter = new_merge_iterator(std::move(inputs), output_schema,
                                   merge_decision(*output_schema, -1, false, false, false));
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    auto st = iter->init(opts);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("compare contract violated") != std::string::npos)
            << st.to_string();
}

// Same contract, a different non-leading-key layout: project {v2, k1}. num_key_columns() is 1
// (k1), but position 0 is the value column v2, so the merge comparator would compare v2 as if it
// were the leading key. init() must reject it.
TEST(VGenericIteratorsTest, MergeRejectsProjectionWithoutLeadingKey) {
    // Full tablet schema: k0(smallint), k1(int), v2(bigint); project {v2, k1}.
    ReadSchema projected(
            project_columns_by_ordinal(create_col_schemas(), std::vector<ColumnId> {2, 1}));
    auto output_schema = std::make_shared<ReadSchema>(projected);

    std::vector<RowwiseIteratorUPtr> inputs;
    inputs.push_back(std::make_unique<ProjectedColumnsUtIterator>(projected, 10));
    inputs.push_back(std::make_unique<ProjectedColumnsUtIterator>(projected, 10));

    auto iter = new_merge_iterator(std::move(inputs), output_schema,
                                   merge_decision(*output_schema, -1, false, false, false));
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    auto st = iter->init(opts);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("compare contract violated") != std::string::npos)
            << st.to_string();
}

// next_row() hands the merged rows out one at a time, flagging every row whose key repeats
// the key of the row before it. Three children with the keys 0..99, 0..199 and 0..299: key 0
// comes three times (is_same false, true, true), key 150 twice, key 250 once.
TEST(VGenericIteratorsTest, MergeNextRowFlagsRepeatedKeys) {
    auto schema = create_schema();
    auto output_schema = std::make_shared<ReadSchema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;
    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));
    auto iter = new_merge_iterator(std::move(inputs), output_schema,
                                   merge_decision(*output_schema, -1, false, false, false));
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    ASSERT_TRUE(iter->init(opts).ok());

    std::vector<std::pair<int64_t, bool>> rows;
    IteratorRowRef ref;
    Status st;
    while ((st = iter->next_row(&ref)).ok()) {
        rows.emplace_back(key_at(*ref.block, ref.row_pos), ref.is_same);
    }
    EXPECT_TRUE(st.is<END_OF_FILE>());
    ASSERT_EQ(rows.size(), 600);
    size_t i = 0;
    for (int64_t key = 0; key < 300; ++key) {
        const int copies = (key < 100 ? 1 : 0) + (key < 200 ? 1 : 0) + 1;
        for (int copy = 0; copy < copies; ++copy, ++i) {
            EXPECT_EQ(rows[i].first, key) << "row " << i;
            EXPECT_EQ(rows[i].second, copy > 0) << "row " << i;
        }
    }
    EXPECT_EQ(iter->merged_rows(), 0);
}

// With skip_same the repeated keys are dropped before they are handed out: one row per key,
// none flagged, and merged_rows() counts the dropped ones.
TEST(VGenericIteratorsTest, MergeNextRowSkipSameKeepsOneRowPerKey) {
    auto schema = create_schema();
    auto output_schema = std::make_shared<ReadSchema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;
    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));
    auto iter = new_merge_iterator(
            std::move(inputs), output_schema,
            merge_decision(*output_schema, -1, false, false, /*skip_same=*/true));
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    ASSERT_TRUE(iter->init(opts).ok());

    int64_t expected_key = 0;
    IteratorRowRef ref;
    Status st;
    while ((st = iter->next_row(&ref)).ok()) {
        EXPECT_EQ(key_at(*ref.block, ref.row_pos), expected_key);
        EXPECT_FALSE(ref.is_same);
        expected_key++;
    }
    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(expected_key, 300);
    EXPECT_EQ(iter->merged_rows(), 300);
}

// The merge copies runs of rows: with the Block limited to 7 rows, every batch holds exactly
// 7 rows until the last one, and the rows come out in the same order as next_row() yields them.
TEST(VGenericIteratorsTest, MergeNextBatchHonorsTheRowLimit) {
    auto schema = create_schema();
    auto output_schema = std::make_shared<ReadSchema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;
    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));
    auto iter = new_merge_iterator(std::move(inputs), output_schema,
                                   merge_decision(*output_schema, -1, false, false, false));
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    opts.block_row_max = 7;
    ASSERT_TRUE(iter->init(opts).ok());

    std::vector<int64_t> keys;
    Block block;
    create_block(schema, block);
    Status st;
    while (true) {
        block.clear_column_data();
        st = iter->next_batch(&block);
        if (!st.ok()) {
            break;
        }
        EXPECT_TRUE(block.rows() == 7 || keys.size() + block.rows() == 600) << block.rows();
        for (size_t i = 0; i < block.rows(); ++i) {
            keys.push_back(key_at(block, i));
        }
    }
    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(block.rows(), 0);
    ASSERT_EQ(keys.size(), 600);
    EXPECT_TRUE(std::ranges::is_sorted(keys));
}

// One row read through next_batch(Block*, same_key): its key and its same-key flag.
struct FlaggedRow {
    int64_t key;
    bool same;
};

// Read every row of `iter` through next_batch(Block*, same_key) into `rows`; returns the status
// that ended the read, EndOfFile when every row was read. The value columns of the test schema
// must stay with their key (column 1 = key + 1, column 2 = key + 2), and there must be one flag
// per row.
static Status read_flagged_rows(RowwiseIterator* iter, ReadSchema& schema,
                                std::vector<FlaggedRow>* rows) {
    Block block;
    create_block(schema, block);
    std::vector<uint8_t> same_key;
    while (true) {
        block.clear_column_data();
        Status st = iter->next_batch(&block, &same_key);
        if (!st.ok()) {
            return st;
        }
        if (same_key.size() != block.rows()) {
            return Status::InternalError("{} same-key flags for {} rows", same_key.size(),
                                         block.rows());
        }
        for (size_t i = 0; i < block.rows(); ++i) {
            const int64_t key = key_at(block, i);
            if ((*block.get_by_position(1).column)[i].get<TYPE_INT>() != key + 1 ||
                (*block.get_by_position(2).column)[i].get<TYPE_BIGINT>() != key + 2) {
                return Status::InternalError("the value columns of key {} moved", key);
            }
            rows->push_back({.key = key, .same = same_key[i] != 0});
        }
    }
}

// next_batch(Block*, same_key) copies the rows in merge order and flags every row whose key
// repeats the key of the row before it, also when the two rows land in different Blocks: with
// 7-row Blocks the three copies of a key straddle Block boundaries. Key 0 comes three times
// (flags 0, 1, 1), key 150 twice, key 250 once.
TEST(VGenericIteratorsTest, MergeNextBatchFlagsRepeatedKeysAcrossBlocks) {
    auto schema = create_schema();
    auto output_schema = std::make_shared<ReadSchema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;
    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));
    auto iter = new_merge_iterator(std::move(inputs), output_schema,
                                   merge_decision(*output_schema, -1, false, false, false));
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    opts.block_row_max = 7;
    ASSERT_TRUE(iter->init(opts).ok());

    std::vector<FlaggedRow> rows;
    Status st = read_flagged_rows(iter.get(), schema, &rows);
    EXPECT_TRUE(st.is<END_OF_FILE>()) << st;
    ASSERT_EQ(rows.size(), 600);
    size_t i = 0;
    for (int64_t key = 0; key < 300; ++key) {
        const int copies = (key < 100 ? 1 : 0) + (key < 200 ? 1 : 0) + 1;
        for (int copy = 0; copy < copies; ++copy, ++i) {
            EXPECT_EQ(rows[i].key, key) << "row " << i;
            EXPECT_EQ(rows[i].same, copy > 0) << "row " << i;
        }
    }
}

// A union does not compare keys: next_batch(Block*, same_key) flags no row.
TEST(VGenericIteratorsTest, UnionNextBatchFlagsNoRow) {
    auto schema = create_schema();
    auto output_schema = std::make_shared<ReadSchema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;
    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    auto iter = new_union_iterator(std::move(inputs), output_schema);
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    ASSERT_TRUE(iter->init(opts).ok());

    std::vector<FlaggedRow> rows;
    Status st = read_flagged_rows(iter.get(), schema, &rows);
    EXPECT_TRUE(st.is<END_OF_FILE>()) << st;
    EXPECT_EQ(rows.size(), 300);
    EXPECT_TRUE(std::ranges::none_of(rows, [](const FlaggedRow& row) { return row.same; }));
}

// A union hands its rows out through next_row() too, child after child, never flagged.
TEST(VGenericIteratorsTest, UnionNextRowWalksEveryRow) {
    auto schema = create_schema();
    auto output_schema = std::make_shared<ReadSchema>(schema);
    std::vector<RowwiseIteratorUPtr> inputs;
    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    auto iter = new_union_iterator(std::move(inputs), output_schema);
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    ASSERT_TRUE(iter->init(opts).ok());

    size_t rows = 0;
    IteratorRowRef ref;
    Status st;
    while ((st = iter->next_row(&ref)).ok()) {
        const int64_t expected = rows < 100 ? rows : rows - 100;
        EXPECT_EQ(key_at(*ref.block, ref.row_pos), expected);
        EXPECT_FALSE(ref.is_same);
        rows++;
    }
    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(rows, 300);
}

} // namespace doris
