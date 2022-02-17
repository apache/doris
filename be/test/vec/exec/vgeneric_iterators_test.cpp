
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

#include <gtest/gtest.h>

#include <vector>

#include "olap/olap_common.h"
#include "olap/row_block2.h"
#include "olap/schema.h"
#include "util/slice.h"

namespace doris {

namespace vectorized {

class VGenericIteratorsTest : public testing::Test {
public:
    VGenericIteratorsTest() {}
    virtual ~VGenericIteratorsTest() {}
};

Schema create_schema() {
    std::vector<TabletColumn> col_schemas;
    col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_SMALLINT, true);
    // c2: int
    col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_INT, true);
    // c3: big int
    col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_SUM, OLAP_FIELD_TYPE_BIGINT, true);

    Schema schema(col_schemas, 2);
    return schema;
}

static void create_block(Schema& schema, vectorized::Block& block)
{
    for (auto &column_desc : schema.columns()) {
        ASSERT_TRUE(column_desc);
        auto data_type = Schema::get_data_type_ptr(column_desc->type());
        ASSERT_NE(data_type, nullptr);
        if (column_desc->is_nullable()) {
            data_type = std::make_shared<vectorized::DataTypeNullable>(std::move(data_type));
        }
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
    ASSERT_TRUE(st.ok());

    vectorized::Block block;
    create_block(schema, block);

    auto ret = iter->next_batch(&block);
    ASSERT_TRUE(ret.ok());
    ASSERT_EQ(block.rows(), 10);

    auto c0 = block.get_by_position(0).column;
    auto c1 = block.get_by_position(1).column;
    auto c2 = block.get_by_position(2).column;

    int row_count = 0;
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i) {
        ASSERT_EQ(row_count,     (*c0)[i].get<int>());
        ASSERT_EQ(row_count + 1, (*c1)[i].get<int>());
        ASSERT_EQ(row_count + 2, (*c2)[i].get<int>());
        row_count++;
    }

    delete iter;
}

TEST(VGenericIteratorsTest, Union) {
    auto schema = create_schema();
    std::vector<RowwiseIterator*> inputs;

    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 100));
    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 200));
    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 300));

    auto iter = vectorized::new_union_iterator(inputs, MemTracker::CreateTracker(-1, "VUnionIterator", nullptr, false));
    StorageReadOptions opts;
    auto st = iter->init(opts);
    ASSERT_TRUE(st.ok());

    vectorized::Block block;
    create_block(schema, block);

    do {
        st = iter->next_batch(&block);
    } while (st.ok());

    ASSERT_TRUE(st.is_end_of_file());
    ASSERT_EQ(block.rows(), 600);

    auto c0 = block.get_by_position(0).column;
    auto c1 = block.get_by_position(1).column;
    auto c2 = block.get_by_position(2).column;

    size_t row_count = 0;
    for (size_t i = 0; i < block.rows(); ++i) {
        size_t base_value = row_count;
        if (row_count >= 300) {
            base_value -= 300;
        } else if (row_count >= 100) {
            base_value -= 100;
        }

        ASSERT_EQ(base_value,     (*c0)[i].get<int>());
        ASSERT_EQ(base_value + 1, (*c1)[i].get<int>());
        ASSERT_EQ(base_value + 2, (*c2)[i].get<int>());
        row_count++;
    }

    delete iter;
}

TEST(VGenericIteratorsTest, Merge) {
    ASSERT_TRUE(1);
    auto schema = create_schema();
    std::vector<RowwiseIterator*> inputs;

    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 100));
    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 200));
    inputs.push_back(vectorized::new_auto_increment_iterator(schema, 300));

    auto iter = vectorized::new_merge_iterator(inputs, MemTracker::CreateTracker(-1, "VMergeIterator", nullptr, false));
    StorageReadOptions opts;
    auto st = iter->init(opts);
    ASSERT_TRUE(st.ok());

    vectorized::Block block;
    create_block(schema, block);

    do {
        st = iter->next_batch(&block);
    } while (st.ok());

    ASSERT_TRUE(st.is_end_of_file());
    ASSERT_EQ(block.rows(), 600);

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

        ASSERT_EQ(base_value,     (*c0)[i].get<int>());
        ASSERT_EQ(base_value + 1, (*c1)[i].get<int>());
        ASSERT_EQ(base_value + 2, (*c2)[i].get<int>());
        row_count++;
    }

    delete iter;
}

} // namespace vectorized

} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
