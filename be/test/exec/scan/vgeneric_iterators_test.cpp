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

#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "core/field.h"
#include "gtest/gtest_pred_impl.h"
#include "storage/olap_common.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"
#include "storage/tablet/tablet_schema.h"

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

// Schema with a smallint key c1 and a struct column c2 = struct<f1:int, f2:int, f3:int>.
static Schema create_struct_schema(int32_t struct_unique_id) {
    std::vector<TabletColumnPtr> col_schemas;
    auto c1 = std::make_shared<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                             FieldType::OLAP_FIELD_TYPE_SMALLINT, true);
    c1->set_is_key(true);
    col_schemas.emplace_back(c1);

    // The (agg, type, nullable) constructor does not support complex types, so
    // build the struct column with the default constructor plus setters.
    auto c2 = std::make_shared<TabletColumn>();
    c2->set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    c2->set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    c2->set_is_nullable(true);
    c2->set_name("c2");
    c2->set_unique_id(struct_unique_id);
    for (const auto* sub_name : {"f1", "f2", "f3"}) {
        TabletColumn sub(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                         FieldType::OLAP_FIELD_TYPE_INT, true);
        sub.set_name(sub_name);
        c2->add_sub_column(sub);
    }
    col_schemas.emplace_back(c2);

    std::vector<ColumnId> column_ids(col_schemas.size());
    for (uint32_t cid = 0; cid < column_ids.size(); ++cid) {
        column_ids[cid] = cid;
    }

    Schema schema(col_schemas, column_ids);
    return schema;
}

// Fills whatever block it is given: ascending keys (key_start + i * key_stride) so
// several inputs interleave under merge, and default values for the struct column.
// Also checks that the merge context built its block with the expected struct type.
class PrunedStructUtIterator : public RowwiseIterator {
public:
    PrunedStructUtIterator(const Schema& schema, size_t num_rows, int16_t key_start,
                           int16_t key_stride, DataTypePtr expected_struct_type)
            : _schema(schema),
              _num_rows(num_rows),
              _key(key_start),
              _key_stride(key_stride),
              _expected_struct_type(std::move(expected_struct_type)) {}
    ~PrunedStructUtIterator() override = default;

    Status init(const StorageReadOptions& opts) override { return Status::OK(); }

    Status next_batch(Block* block) override {
        if (_rows_returned >= _num_rows) {
            return Status::EndOfFile("End of PrunedStructUtIterator");
        }
        // The merge block must carry the FE-pruned struct type (not the full storage
        // type) so that its struct children line up with the pruned sub-column readers.
        EXPECT_TRUE(block->get_by_position(1).type->equals(*_expected_struct_type));
        while (_rows_returned < _num_rows) {
            ColumnWithTypeAndName& key_col = block->get_by_position(0);
            ((IColumn&)(*key_col.column)).insert_data((const char*)&_key, sizeof(_key));
            ColumnWithTypeAndName& struct_col = block->get_by_position(1);
            ((IColumn&)(*struct_col.column)).insert_default();
            _key = (int16_t)(_key + _key_stride);
            ++_rows_returned;
        }
        return Status::OK();
    }

    const Schema& schema() const override { return _schema; }

private:
    const Schema& _schema;
    size_t _num_rows;
    size_t _rows_returned = 0;
    int16_t _key;
    int16_t _key_stride;
    DataTypePtr _expected_struct_type;
};

// When the query prunes struct sub-columns, StorageReadOptions carries a tablet schema
// whose pruned-columns map records the FE-pruned type for the column's unique id. The
// merge iterator must build its internal blocks with that pruned type (see
// VMergeIteratorContext::_data_type_maybe_pruned); otherwise the block's full struct
// children would be paired with the pruned sub-column iterators by bare index.
TEST(VGenericIteratorsTest, MergeWithPrunedStructColumn) {
    constexpr int32_t struct_unique_id = 10;
    auto schema = create_struct_schema(struct_unique_id);
    auto output_schema = std::make_shared<Schema>(schema);

    // The FE-pruned type of c2 keeps only {f1, f3} out of {f1, f2, f3}.
    TabletColumn pruned_c2;
    pruned_c2.set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    pruned_c2.set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    pruned_c2.set_is_nullable(true);
    pruned_c2.set_name("c2");
    pruned_c2.set_unique_id(struct_unique_id);
    for (const auto* sub_name : {"f1", "f3"}) {
        TabletColumn sub(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                         FieldType::OLAP_FIELD_TYPE_INT, true);
        sub.set_name(sub_name);
        pruned_c2.add_sub_column(sub);
    }
    auto pruned_type = Schema::get_data_type_ptr(pruned_c2);
    auto full_type = Schema::get_data_type_ptr(*schema.column(1));
    EXPECT_FALSE(pruned_type->equals(*full_type));

    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->add_pruned_columns_data_type(struct_unique_id, pruned_type);
    EXPECT_TRUE(tablet_schema->has_pruned_columns());
    EXPECT_EQ(tablet_schema->pruned_columns_data_type().size(), 1);
    EXPECT_TRUE(
            tablet_schema->pruned_columns_data_type().at(struct_unique_id)->equals(*pruned_type));

    std::vector<RowwiseIteratorUPtr> inputs;
    inputs.push_back(std::make_unique<PrunedStructUtIterator>(schema, 100, 0, 2, pruned_type));
    inputs.push_back(std::make_unique<PrunedStructUtIterator>(schema, 100, 1, 2, pruned_type));

    auto iter = new_merge_iterator(std::move(inputs), -1, false, false, nullptr, output_schema);
    StorageReadOptions opts;
    opts.tablet_schema = tablet_schema;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    // The destination block mirrors a query-level block: the struct column is created
    // with the pruned type, matching what the merge context copies into it.
    Block block;
    auto key_type = Schema::get_data_type_ptr(*schema.column(0));
    auto key_column = key_type->create_column();
    block.insert(ColumnWithTypeAndName(std::move(key_column), key_type, "c1"));
    auto struct_column = pruned_type->create_column();
    block.insert(ColumnWithTypeAndName(std::move(struct_column), pruned_type, "c2"));
    std::vector<bool> row_is_same;
    BlockWithSameBit block_with_same_bit {.block = &block, .same_bit = row_is_same};

    do {
        st = iter->next_batch(&block_with_same_bit);
    } while (st.ok());

    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(block.rows(), 200);

    // Keys 0..199 in order: the two inputs (0,2,4,... and 1,3,5,...) interleaved.
    auto c0 = block.get_by_position(0).column;
    for (size_t i = 0; i < block.rows(); ++i) {
        EXPECT_EQ(i, (*c0)[i].get<TYPE_SMALLINT>());
    }
    // The struct column kept the pruned 2-child layout end to end.
    EXPECT_TRUE(block.get_by_position(1).type->equals(*pruned_type));
    EXPECT_EQ(block.get_by_position(1).column->size(), 200);
}

// Mirror of MergeWithSeqColumn but with small_seq_first=true.
// Same key across all segments, seq values are 0..seg_iter_num-1; the merge
// iterator should keep exactly one row whose seq value is the smallest (0).
TEST(VGenericIteratorsTest, MergeWithSeqColumnSmallSeqFirst) {
    auto schema = create_schema();
    auto output_schema = std::make_shared<Schema>(schema);
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
    auto iter = new_merge_iterator(std::move(inputs), seq_column_id, /*is_unique=*/true,
                                   /*is_reverse=*/false, /*merged_rows=*/nullptr, output_schema,
                                   /*small_seq_first=*/true);
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

    auto seq_col = block.get_by_position(seq_column_id).column;
    size_t actual_value = (*seq_col)[0].get<TYPE_BIGINT>();
    EXPECT_EQ(0, actual_value);
}

} // namespace doris
