// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.
//
// #include <gtest/gtest.h>
//
// #include "olap/tablet_schema.h"
//
// namespace doris {
//
// class TabletSchemaIndexTest : public testing::Test {
// protected:
//     void SetUp() override {
//         // Setup common test data
//         _tablet_schema = std::make_shared<TabletSchema>();
//     }
//
//     TabletIndex create_test_index(int64_t index_id, IndexType type,
//                                   const std::vector<int32_t>& col_uids,
//                                   const std::string& suffix = "") {
//         TabletIndex index;
//         index._index_id = index_id;
//         index._index_type = type;
//         index._col_unique_ids = col_uids;
//         index.set_escaped_escaped_index_suffix_path(suffix);
//         return index;
//     }
//
//     std::shared_ptr<TabletSchema> _tablet_schema;
// };
//
// TEST_F(TabletSchemaIndexTest, TestAddInvertedIndex) {
//     // Add inverted index with suffix
//     TabletIndex index = create_test_index(1, IndexType::INVERTED, {100}, "suffix1");
//     _tablet_schema->append_index(std::move(index));
//
//     // Verify index mapping
// <<<<<<< HEAD
//     auto* found_index = _tablet_schema->inverted_index(100, "suffix1");
//     ASSERT_NE(found_index, nullptr);
//     EXPECT_EQ(found_index->index_id(), 1);
//     EXPECT_EQ(found_index->get_index_suffix(), "suffix1");
// =======
//     auto found_indexs = _tablet_schema->inverted_indexs(100, "suffix1");
//     ASSERT_NE(found_indexs.size(), 0);
//     EXPECT_EQ(found_indexs[0]->index_id(), 1);
//     EXPECT_EQ(found_indexs[0]->get_index_suffix(), "suffix1");
// >>>>>>> a9e096e8f4f ([enhance](inverted index)multi index on one column (#50447))
// }
//
// TEST_F(TabletSchemaIndexTest, TestRemoveIndex) {
//     // Add multiple indexes
//     _tablet_schema->append_index(create_test_index(1, IndexType::INVERTED, {100}, "suffix1"));
//     _tablet_schema->append_index(create_test_index(2, IndexType::INVERTED, {200}, "suffix2"));
//
//     // Remove index 1
//     _tablet_schema->remove_index(1);
//
//     // Verify index 1 removed
// <<<<<<< HEAD
//     EXPECT_EQ(_tablet_schema->inverted_index(100, "suffix1"), nullptr);
//
//     // Verify index 2 still exists
//     auto* found_index = _tablet_schema->inverted_index(200, "suffix2");
//     ASSERT_NE(found_index, nullptr);
//     EXPECT_EQ(found_index->index_id(), 2);
// =======
//     EXPECT_EQ(_tablet_schema->inverted_indexs(100, "suffix1").size(), 0);
//
//     // Verify index 2 still exists
//     auto found_indexs = _tablet_schema->inverted_indexs(200, "suffix2");
//     ASSERT_NE(found_indexs.size(), 0);
//     EXPECT_EQ(found_indexs[0]->index_id(), 2);
// >>>>>>> a9e096e8f4f ([enhance](inverted index)multi index on one column (#50447))
// }
//
// TEST_F(TabletSchemaIndexTest, TestUpdateIndex) {
//     // Add initial index
//     _tablet_schema->append_index(create_test_index(1, IndexType::INVERTED, {100}, "old_suffix"));
// <<<<<<< HEAD
//     ASSERT_NE(_tablet_schema->inverted_index(100, "old_suffix"), nullptr);
// =======
//     ASSERT_NE(_tablet_schema->inverted_indexs(100, "old_suffix").size(), 0);
// >>>>>>> a9e096e8f4f ([enhance](inverted index)multi index on one column (#50447))
//
//     // Update index with new suffix
//     _tablet_schema->remove_index(1);
//     _tablet_schema->append_index(create_test_index(1, IndexType::INVERTED, {100}, "new_suffix"));
//
//     // Verify update
// <<<<<<< HEAD
//     EXPECT_EQ(_tablet_schema->inverted_index(100, "old_suffix"), nullptr);
//     auto* found_index = _tablet_schema->inverted_index(100, "new_suffix");
//     ASSERT_NE(found_index, nullptr);
//     EXPECT_EQ(found_index->get_index_suffix(), "new%5Fsuffix");
// =======
//     EXPECT_EQ(_tablet_schema->inverted_indexs(100, "old_suffix").size(), 0);
//     auto found_indexs = _tablet_schema->inverted_indexs(100, "new_suffix");
//     ASSERT_NE(found_indexs.size(), 0);
//     EXPECT_EQ(found_indexs[0]->get_index_suffix(), "new%5Fsuffix");
// >>>>>>> a9e096e8f4f ([enhance](inverted index)multi index on one column (#50447))
// }
//
// TEST_F(TabletSchemaIndexTest, TestMultipleColumnsIndex) {
//     // Add index with multiple columns
//     TabletIndex index = create_test_index(1, IndexType::INVERTED, {100, 200}, "multi_col");
//     _tablet_schema->append_index(std::move(index));
//
//     // Verify both columns mapped
// <<<<<<< HEAD
//     auto* index1 = _tablet_schema->inverted_index(100, "multi_col");
//     auto* index2 = _tablet_schema->inverted_index(200, "multi_col");
//     ASSERT_NE(index1, nullptr);
//     ASSERT_EQ(index1, index2); // Should point to same index
// =======
//     auto indexs1 = _tablet_schema->inverted_indexs(100, "multi_col");
//     auto indexs2 = _tablet_schema->inverted_indexs(200, "multi_col");
//     ASSERT_NE(indexs1.size(), 0);
//     ASSERT_EQ(indexs1[0], indexs2[0]); // Should point to same index
// >>>>>>> a9e096e8f4f ([enhance](inverted index)multi index on one column (#50447))
// }
//
// TEST_F(TabletSchemaIndexTest, TestDuplicateIndexKey) {
//     // Add two indexes with same (type,col,suffix)
//     _tablet_schema->append_index(create_test_index(1, IndexType::INVERTED, {100}, "suffix"));
//     _tablet_schema->append_index(create_test_index(2, IndexType::INVERTED, {100}, "suffix"));
//
//     // The last added should override
// <<<<<<< HEAD
//     auto* found_index = _tablet_schema->inverted_index(100, "suffix");
//     ASSERT_NE(found_index, nullptr);
//     EXPECT_EQ(found_index->index_id(), 1);
// =======
//     auto found_indexs = _tablet_schema->inverted_indexs(100, "suffix");
//     ASSERT_NE(found_indexs.size(), 0);
//     EXPECT_EQ(found_indexs[0]->index_id(), 1);
// >>>>>>> a9e096e8f4f ([enhance](inverted index)multi index on one column (#50447))
// }
//
// TEST_F(TabletSchemaIndexTest, TestClearIndexes) {
//     _tablet_schema->append_index(create_test_index(1, IndexType::INVERTED, {100}));
//     _tablet_schema->clear_index();
//
// <<<<<<< HEAD
//     EXPECT_EQ(_tablet_schema->inverted_index(100, ""), nullptr);
// =======
//     EXPECT_EQ(_tablet_schema->inverted_indexs(100, "").size(), 0);
// >>>>>>> a9e096e8f4f ([enhance](inverted index)multi index on one column (#50447))
//     EXPECT_TRUE(_tablet_schema->inverted_indexes().empty());
// }
//
// TEST_F(TabletSchemaIndexTest, TestUpdateIndexMethod) {
//     TabletColumn col;
//     col.set_parent_unique_id(100);
//     col.set_path_info(vectorized::PathInData("v2"));
//     _tablet_schema->append_column(col);
//
//     TabletIndex old_index = create_test_index(1, IndexType::INVERTED, {100}, "v2");
//     _tablet_schema->append_index(std::move(old_index));
//
// <<<<<<< HEAD
//     TabletIndex new_index = create_test_index(1, IndexType::INVERTED, {100}, "v2");
//     new_index._properties["new_prop"] = "value";
//
//     _tablet_schema->update_index(col, IndexType::INVERTED, std::move(new_index));
//
//     const TabletIndex* updated_index = _tablet_schema->inverted_index(100, "v2");
//     ASSERT_NE(updated_index, nullptr);
//     EXPECT_EQ(updated_index->index_id(), 1);
//     EXPECT_EQ(updated_index->properties().at("new_prop"), "value");
// =======
//     std::vector<TabletIndex> new_indexs;
//     TabletIndex new_index = create_test_index(1, IndexType::INVERTED, {100}, "v2");
//     new_index._properties["new_prop"] = "value";
//     new_indexs.emplace_back(std::move(new_index));
//
//     _tablet_schema->update_index(col, IndexType::INVERTED, std::move(new_indexs));
//
//     auto updated_indexs = _tablet_schema->inverted_indexs(100, "v2");
//     ASSERT_NE(updated_indexs.size(), 0);
//     EXPECT_EQ(updated_indexs[0]->index_id(), 1);
//     EXPECT_EQ(updated_indexs[0]->properties().at("new_prop"), "value");
// >>>>>>> a9e096e8f4f ([enhance](inverted index)multi index on one column (#50447))
//
//     auto key = std::make_tuple(IndexType::INVERTED, 100, "v2");
//     EXPECT_NE(_tablet_schema->_col_id_suffix_to_index.find(key),
//               _tablet_schema->_col_id_suffix_to_index.end());
// }
//
// TEST_F(TabletSchemaIndexTest, TestUpdateIndexAddNewWhenNotExist) {
//     // Not exist, return nullptr
//     TabletColumn col;
//     col.set_unique_id(200);
//
// <<<<<<< HEAD
//     TabletIndex new_index = create_test_index(2, IndexType::INVERTED, {200}, "v3");
//     _tablet_schema->update_index(col, IndexType::INVERTED, std::move(new_index));
//
//     const TabletIndex* index = _tablet_schema->inverted_index(200, "v3");
//     ASSERT_EQ(index, nullptr);
// =======
//     std::vector<TabletIndex> new_indexs;
//     new_indexs.emplace_back(create_test_index(2, IndexType::INVERTED, {200}, "v3"));
//     _tablet_schema->update_index(col, IndexType::INVERTED, std::move(new_indexs));
//
//     auto indexs = _tablet_schema->inverted_indexs(200, "v3");
//     ASSERT_EQ(indexs.size(), 0);
// >>>>>>> a9e096e8f4f ([enhance](inverted index)multi index on one column (#50447))
// }
//
// TEST_F(TabletSchemaIndexTest, TestUpdateIndexWithMultipleColumns) {
//     TabletColumn col1, col2;
//     col1.set_unique_id(300);
//     col2.set_unique_id(400);
//     _tablet_schema->append_column(col1);
//     _tablet_schema->append_column(col2);
//
//     TabletIndex old_multi_index = create_test_index(3, IndexType::INVERTED, {300, 400}, "multi");
//     _tablet_schema->append_index(std::move(old_multi_index));
//
//     TabletIndex new_multi_index = create_test_index(3, IndexType::NGRAM_BF, {300, 400});
//     _tablet_schema->append_index(std::move(new_multi_index));
//
// <<<<<<< HEAD
//     ASSERT_NE(_tablet_schema->inverted_index(300, "multi"), nullptr);
// =======
//     ASSERT_NE(_tablet_schema->inverted_indexs(300, "multi").size(), 0);
// >>>>>>> a9e096e8f4f ([enhance](inverted index)multi index on one column (#50447))
//     EXPECT_NE(_tablet_schema->get_ngram_bf_index(400), nullptr);
// }
//
// } // namespace doris