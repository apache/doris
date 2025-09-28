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

#include "olap/tablet_schema.h"
#include "vec/common/string_ref.h"

namespace doris {

class TabletSchemaMultiIndexTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Common setup for all tests
        schema = std::make_shared<TabletSchema>();

        auto col1 = std::make_shared<TabletColumn>();
        col1->_unique_id = 0;
        col1->_col_name = "c0";
        schema->_cols.push_back(col1);

        auto col2 = std::make_shared<TabletColumn>();
        col2->_unique_id = 1;
        col2->_col_name = "c1";
        schema->_cols.push_back(col2);

        auto col3 = std::make_shared<TabletColumn>();
        col3->_unique_id = 2;
        col3->_col_name = "c2";
        schema->_cols.push_back(col3);

        schema->_field_name_to_index[StringRef(col1->_col_name)] = 0;
        schema->_field_name_to_index[StringRef(col2->_col_name)] = 1;
        schema->_field_name_to_index[StringRef(col3->_col_name)] = 2;

        schema->_num_columns = 3;
    }

    void TearDown() override {
        // Clean up if needed
    }

    TabletSchemaSPtr schema;
};

TEST_F(TabletSchemaMultiIndexTest, AppendIndex) {
    TabletIndex index;
    index._index_id = 100;
    index._index_type = IndexType::INVERTED;
    index._col_unique_ids = {1, 2};
    index._escaped_index_suffix_path = "suffix1";

    // Append first index
    schema->append_index(std::move(index));

    // Verify the index was added
    ASSERT_EQ(schema->_indexes.size(), 1);
    ASSERT_EQ(schema->_indexes[0]->_index_id, 100);

    // Verify the mapping was created for both columns
    TabletSchema::IndexKey key1(IndexType::INVERTED, 1, "suffix1");
    TabletSchema::IndexKey key2(IndexType::INVERTED, 2, "suffix1");

    ASSERT_EQ(schema->_col_id_suffix_to_index[key1].size(), 1);
    ASSERT_EQ(schema->_col_id_suffix_to_index[key2].size(), 1);
    ASSERT_EQ(schema->_col_id_suffix_to_index[key1][0], 0);
    ASSERT_EQ(schema->_col_id_suffix_to_index[key2][0], 0);

    // Append second index with same columns
    TabletIndex index2;
    index2._index_id = 101;
    index2._index_type = IndexType::INVERTED;
    index2._col_unique_ids = {1, 2};
    index2._escaped_index_suffix_path = "suffix1";

    schema->append_index(std::move(index2));

    // Verify both indexes are present
    ASSERT_EQ(schema->_indexes.size(), 2);

    // Verify mapping now contains both indexes for the columns
    ASSERT_EQ(schema->_col_id_suffix_to_index[key1].size(), 2);
    ASSERT_EQ(schema->_col_id_suffix_to_index[key2].size(), 2);
    ASSERT_EQ(schema->_col_id_suffix_to_index[key1][0], 0);
    ASSERT_EQ(schema->_col_id_suffix_to_index[key1][1], 1);
}

TEST_F(TabletSchemaMultiIndexTest, AppendIndexWithExtractedColumn) {
    TabletIndex index;
    index._index_id = 100;
    index._index_type = IndexType::INVERTED;
    index._col_unique_ids = {3}; // extracted column unique id
    index._escaped_index_suffix_path = "suffix1";

    schema->append_index(std::move(index));

    // Should map to parent unique id (1)
    TabletSchema::IndexKey key(IndexType::INVERTED, 3, "suffix1");
    ASSERT_EQ(schema->_col_id_suffix_to_index[key].size(), 1);
    ASSERT_EQ(schema->_col_id_suffix_to_index[key][0], 0);
}

TEST_F(TabletSchemaMultiIndexTest, RemoveIndex) {
    // Setup two indexes
    TabletIndex index1;
    index1._index_id = 100;
    index1._index_type = IndexType::INVERTED;
    index1._col_unique_ids = {1};
    index1._escaped_index_suffix_path = "";
    schema->append_index(std::move(index1));

    TabletIndex index2;
    index2._index_id = 101;
    index2._index_type = IndexType::INVERTED;
    index2._col_unique_ids = {1};
    index2._escaped_index_suffix_path = "suffix1";
    schema->append_index(std::move(index2));

    // Remove first index
    schema->remove_index(100);

    // Verify only second index remains
    ASSERT_EQ(schema->_indexes.size(), 1);
    ASSERT_EQ(schema->_indexes[0]->_index_id, 101);

    // Verify mapping was updated
    TabletSchema::IndexKey key(IndexType::INVERTED, 1, "suffix1");
    ASSERT_EQ(schema->_col_id_suffix_to_index[key].size(), 1);
    ASSERT_EQ(schema->_col_id_suffix_to_index[key][0], 0); // Now points to position 0
}

TEST_F(TabletSchemaMultiIndexTest, RemoveNonExistentIndex) {
    // Setup one index
    TabletIndex index;
    index._index_id = 100;
    index._index_type = IndexType::INVERTED;
    index._col_unique_ids = {1};
    index._escaped_index_suffix_path = "suffix1";
    schema->append_index(std::move(index));

    // Try to remove non-existent index
    schema->remove_index(999);

    // Verify original index still exists
    ASSERT_EQ(schema->_indexes.size(), 1);
    ASSERT_EQ(schema->_indexes[0]->_index_id, 100);
}

TEST_F(TabletSchemaMultiIndexTest, UpdateIndexesFromThrift) {
    std::vector<doris::TOlapTableIndex> tindexes;

    // Create first thrift index
    doris::TOlapTableIndex tindex1;
    tindex1.__set_index_id(100);
    tindex1.__set_index_type(TIndexType::type::INVERTED);
    tindex1.columns.push_back("c1");
    tindexes.push_back(tindex1);

    // Create second thrift index
    doris::TOlapTableIndex tindex2;
    tindex2.__set_index_id(101);
    tindex2.__set_index_type(TIndexType::type::INVERTED);
    tindex2.columns.push_back("c1");
    tindex2.columns.push_back("c2");
    tindexes.push_back(tindex2);

    // Update from thrift
    schema->update_indexes_from_thrift(tindexes);

    // Verify indexes were created
    ASSERT_EQ(schema->_indexes.size(), 2);
    ASSERT_EQ(schema->_indexes[0]->_index_id, 100);
    ASSERT_EQ(schema->_indexes[1]->_index_id, 101);

    // Verify mappings were created
    TabletSchema::IndexKey key1(IndexType::INVERTED, 1, "");
    TabletSchema::IndexKey key2(IndexType::INVERTED, 2, "");

    ASSERT_EQ(schema->_col_id_suffix_to_index[key1].size(), 2); // Both indexes reference col1
    ASSERT_EQ(schema->_col_id_suffix_to_index[key2].size(), 1); // Only second index references col2
}

TEST_F(TabletSchemaMultiIndexTest, InvertedIndexesLookup) {
    // Setup two inverted indexes for col1
    TabletIndex index1;
    index1._index_id = 100;
    index1._index_type = IndexType::INVERTED;
    index1._col_unique_ids = {1};
    index1._escaped_index_suffix_path = "suffix1";
    schema->append_index(std::move(index1));

    TabletIndex index2;
    index2._index_id = 101;
    index2._index_type = IndexType::INVERTED;
    index2._col_unique_ids = {1};
    index2._escaped_index_suffix_path = "suffix1";
    schema->append_index(std::move(index2));

    // Setup one inverted index for col2
    TabletIndex index3;
    index3._index_id = 102;
    index3._index_type = IndexType::INVERTED;
    index3._col_unique_ids = {2};
    index3._escaped_index_suffix_path = "suffix1";
    schema->append_index(std::move(index3));

    // Lookup indexes for col1
    auto indexes = schema->inverted_indexs(1, "suffix1");
    ASSERT_EQ(indexes.size(), 2);
    ASSERT_EQ(indexes[0]->_index_id, 100);
    ASSERT_EQ(indexes[1]->_index_id, 101);

    // Lookup indexes for col2
    indexes = schema->inverted_indexs(2, "suffix1");
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0]->_index_id, 102);

    // Lookup non-existent column
    indexes = schema->inverted_indexs(999, "suffix1");
    ASSERT_TRUE(indexes.empty());
}

TEST_F(TabletSchemaMultiIndexTest, InvertedIndexesLookupWithColumnObject) {
    // Setup index for extracted column
    TabletIndex index;
    index._index_id = 100;
    index._index_type = IndexType::INVERTED;
    index._col_unique_ids = {3}; // extracted column unique id
    schema->append_index(std::move(index));

    TabletColumn col1;
    col1._unique_id = 3;

    // Lookup using the extracted column
    auto indexes = schema->inverted_indexs(col1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0]->_index_id, 100);
}

TEST_F(TabletSchemaMultiIndexTest, InvertedIndexesUnsupportedColumn) {
    // Create an unsupported column type
    TabletColumn unsupported_col;
    unsupported_col._unique_id = 4;

    // Should return empty vector for unsupported column
    auto indexes = schema->inverted_indexs(unsupported_col);
    ASSERT_TRUE(indexes.empty());
}

} // namespace doris