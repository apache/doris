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

#include "vec/olap/vgeneric_iterators.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/schema.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"

namespace doris {
using namespace ErrorCode;

namespace vectorized {

class VGenericIteratorsTest : public testing::Test {
public:
    VGenericIteratorsTest() {}
    virtual ~VGenericIteratorsTest() {}
};

Schema create_schema() {
    std::vector<TabletColumnPtr> col_schemas;
    col_schemas.emplace_back(
            std::make_shared<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                           FieldType::OLAP_FIELD_TYPE_SMALLINT, true));
    // c2: int
    col_schemas.emplace_back(
            std::make_shared<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                           FieldType::OLAP_FIELD_TYPE_INT, true));
    // c3: big int
    col_schemas.emplace_back(
            std::make_shared<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM,
                                           FieldType::OLAP_FIELD_TYPE_BIGINT, true));

    Schema schema(col_schemas, 2);
    return schema;
}

static void create_block(Schema& schema, vectorized::Block& block) {
    for (auto& column_desc : schema.columns()) {
        EXPECT_TRUE(column_desc);
        auto data_type = Schema::get_data_type_ptr(*column_desc);
        EXPECT_NE(data_type, nullptr);
        auto column = data_type->create_column();
        vectorized::ColumnWithTypeAndName ctn(std::move(column), data_type, column_desc->name());
        block.insert(ctn);
    }
}

TEST(VGenericIteratorsTest, AutoIncrement) {
    auto schema = create_schema();
    auto iter = vectorized::new_auto_increment_iterator(schema, 10);

    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    vectorized::Block block;
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
        EXPECT_EQ(row_count, (*c0)[i].get<int>());
        EXPECT_EQ(row_count + 1, (*c1)[i].get<int>());
        EXPECT_EQ(row_count + 2, (*c2)[i].get<int>());
        row_count++;
    }
}

TEST(VGenericIteratorsTest, Union) {
    auto schema = create_schema();
    std::vector<RowwiseIteratorUPtr> inputs;

    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 100));
    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 200));
    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 300));

    auto iter = vectorized::new_union_iterator(std::move(inputs));
    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    vectorized::Block block;
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

        EXPECT_EQ(base_value, (*c0)[i].get<int>());
        EXPECT_EQ(base_value + 1, (*c1)[i].get<int>());
        EXPECT_EQ(base_value + 2, (*c2)[i].get<int>());
        row_count++;
    }
}

TEST(VGenericIteratorsTest, MergeAgg) {
    EXPECT_TRUE(1);
    auto schema = create_schema();
    std::vector<RowwiseIteratorUPtr> inputs;

    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 100));
    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 200));
    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 300));

    auto iter = vectorized::new_merge_iterator(std::move(inputs), -1, false, false, nullptr);
    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    vectorized::Block block;
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

        EXPECT_EQ(base_value, (*c0)[i].get<int>());
        EXPECT_EQ(base_value + 1, (*c1)[i].get<int>());
        EXPECT_EQ(base_value + 2, (*c2)[i].get<int>());
        row_count++;
    }
}

TEST(VGenericIteratorsTest, MergeUnique) {
    EXPECT_TRUE(1);
    auto schema = create_schema();
    std::vector<RowwiseIteratorUPtr> inputs;

    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 100));
    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 200));
    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 300));

    auto iter = vectorized::new_merge_iterator(std::move(inputs), -1, true, false, nullptr);
    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    vectorized::Block block;
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

        EXPECT_EQ(base_value, (*c0)[i].get<int>());
        EXPECT_EQ(base_value + 1, (*c1)[i].get<int>());
        EXPECT_EQ(base_value + 2, (*c2)[i].get<int>());
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

    Status next_batch(vectorized::Block* block) override {
        int row_idx = 0;
        while (_rows_returned < _num_rows) {
            for (int j = 0; j < _schema.num_columns(); ++j) {
                vectorized::ColumnWithTypeAndName& vc = block->get_by_position(j);
                vectorized::IColumn& vi = (vectorized::IColumn&)(*vc.column);

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

    auto iter =
            vectorized::new_merge_iterator(std::move(inputs), seq_column_id, true, false, nullptr);
    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    vectorized::Block block;
    create_block(schema, block);

    do {
        st = iter->next_batch(&block);
    } while (st.ok());

    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(block.rows(), 1);

    auto col0 = block.get_by_position(0).column;
    auto col1 = block.get_by_position(1).column;
    auto seq_col = block.get_by_position(seq_column_id).column;
    size_t actual_value = (*seq_col)[0].get<int>();
    EXPECT_EQ(seg_iter_num - 1, actual_value);
}

} // namespace vectorized

} // namespace doris
