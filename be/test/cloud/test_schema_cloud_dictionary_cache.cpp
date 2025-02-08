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

#include "cloud/schema_cloud_dictionary_cache.h"
#include "gen_cpp/olap_file.pb.h"
#include "gtest/gtest.h"

namespace doris {

using SchemaCloudDictionarySPtr = std::shared_ptr<SchemaCloudDictionary>;

/*
 * FakeSchemaCloudDictionaryCache is a test subclass which allows injection of dictionary entries
 * and overrides refresh_dict to simulate RPC refresh.
 */
class FakeSchemaCloudDictionaryCache : public SchemaCloudDictionaryCache {
public:
    FakeSchemaCloudDictionaryCache(size_t capacity) : SchemaCloudDictionaryCache(capacity) {}

    // For unit testing, we override refresh_dict to simulate different scenarios.
    // (Assume the base class method is declared virtual for testing or we hide it in our subclass)
    Status refresh_dict(int64_t index_id, SchemaCloudDictionarySPtr* new_dict = nullptr) override {
        if (simulate_refresh_success) {
            // Simulate a successful refresh by creating a valid dictionary.
            SchemaCloudDictionarySPtr valid_dict = createValidDictionary();
            // Inject the dictionary into cache.
            TestInsert(index_id, valid_dict);
            if (new_dict) {
                *new_dict = valid_dict;
            }
            return Status::OK();
        } else {
            return Status::InternalError("Simulated refresh failure");
        }
    }

    // Public wrapper for injection (assume _insert is accessible, e.g. changed to protected for unit test)
    void TestInsert(int64_t index_id, const SchemaCloudDictionarySPtr& dict) {
        _insert(index_id, dict);
    }

    // Flag to control refresh_dict to simulate refresh results.
    bool simulate_refresh_success = true;

    // Create a valid SchemaCloudDictionary with expected keys.
    static SchemaCloudDictionarySPtr createValidDictionary() {
        auto* dict = new SchemaCloudDictionary();
        // Populate valid column entry with key 101.
        auto& col_dict = *dict->mutable_column_dict();
        ColumnPB* col_pb = &(col_dict)[101];
        col_pb->set_unique_id(101);
        // Populate valid index entry with key 201. Set index_suffix_name to empty.
        auto& idx_dict = *dict->mutable_index_dict();
        TabletIndexPB* idx_pb = &(idx_dict)[201];
        idx_pb->set_index_suffix_name("");
        return SchemaCloudDictionarySPtr(dict);
    }

    // Create an invalid SchemaCloudDictionary (missing column key 101)
    static SchemaCloudDictionarySPtr createInvalidDictionary() {
        auto* dict = new SchemaCloudDictionary();
        // Insert a column with a wrong key example 999 rather than 101.
        auto& col_dict = *dict->mutable_column_dict();
        ColumnPB* col_pb = &(col_dict)[999];
        col_pb->set_unique_id(999);
        // 正常的 index 数据.
        auto& idx_dict = *dict->mutable_index_dict();
        TabletIndexPB* idx_pb = &(idx_dict)[201];
        idx_pb->set_index_suffix_name("");
        return SchemaCloudDictionarySPtr(dict);
    }
};

// Test case 1: Cached dictionary valid, _try_fill_schema returns OK.
TEST(SchemaCloudDictionaryCacheTest, ReplaceDictKeysToSchema_ValidCache) {
    int64_t index_id = 100;
    FakeSchemaCloudDictionaryCache cache(10);
    // Inject a valid dictionary into cache.
    SchemaCloudDictionarySPtr valid_dict = FakeSchemaCloudDictionaryCache::createValidDictionary();
    cache.TestInsert(index_id, valid_dict);

    // Create a RowsetMetaCloudPB with schema dictionary key list.
    RowsetMetaCloudPB rs_meta;
    // For testing, add expected column key (101) and index key (201).
    SchemaDictKeyList* dict_keys = rs_meta.mutable_schema_dict_key_list();
    dict_keys->add_column_dict_key_list(101);
    dict_keys->add_index_info_dict_key_list(201);
    // Ensure tablet schema message is created.
    rs_meta.mutable_tablet_schema();

    // Call replace_dict_keys_to_schema.
    Status st = cache.replace_dict_keys_to_schema(index_id, &rs_meta);
    EXPECT_TRUE(st.ok());

    // Check that the tablet schema was filled.
    const TabletSchemaCloudPB& schema = rs_meta.tablet_schema();
    EXPECT_EQ(schema.column_size(), 1);
    EXPECT_EQ(schema.index_size(), 1);
}

// Test case 2: Cached dictionary invalid, triggers refresh then succeeds.
TEST(SchemaCloudDictionaryCacheTest, ReplaceDictKeysToSchema_InvalidCache_ThenRefresh) {
    int64_t index_id = 200;
    FakeSchemaCloudDictionaryCache cache(10);
    // Inject an invalid dictionary (missing required column key 101).
    SchemaCloudDictionarySPtr invalid_dict =
            FakeSchemaCloudDictionaryCache::createInvalidDictionary();
    cache.TestInsert(index_id, invalid_dict);

    // Create rowset meta with keys expecting valid dictionary.
    RowsetMetaCloudPB rs_meta;
    SchemaDictKeyList* dict_keys = rs_meta.mutable_schema_dict_key_list();
    dict_keys->add_column_dict_key_list(101); // invalid dict does not contain 101.
    dict_keys->add_index_info_dict_key_list(201);
    rs_meta.mutable_tablet_schema();

    cache.simulate_refresh_success = true;
    Status st = cache.replace_dict_keys_to_schema(index_id, &rs_meta);
    EXPECT_TRUE(st.ok());

    // After refresh, the valid dictionary should be used.
    const TabletSchemaCloudPB& schema = rs_meta.tablet_schema();
    EXPECT_EQ(schema.column_size(), 1);
    EXPECT_EQ(schema.index_size(), 1);
}

// Test case 3: No dictionary in cache, refresh is triggered and succeeds.
TEST(SchemaCloudDictionaryCacheTest, ReplaceDictKeysToSchema_NoCache_ThenRefresh) {
    int64_t index_id = 300;
    FakeSchemaCloudDictionaryCache cache(10);
    // Not injecting any dictionary so that _lookup returns null.
    RowsetMetaCloudPB rs_meta;
    SchemaDictKeyList* dict_keys = rs_meta.mutable_schema_dict_key_list();
    dict_keys->add_column_dict_key_list(101);
    dict_keys->add_index_info_dict_key_list(201);
    rs_meta.mutable_tablet_schema();

    // Refresh should be triggered.
    cache.simulate_refresh_success = true;
    Status st = cache.replace_dict_keys_to_schema(index_id, &rs_meta);
    EXPECT_TRUE(st.ok());

    const TabletSchemaCloudPB& schema = rs_meta.tablet_schema();
    EXPECT_EQ(schema.column_size(), 1);
    EXPECT_EQ(schema.index_size(), 1);
}

// Test case 4: Refresh fails, replace_dict_keys_to_schema returns error.
TEST(SchemaCloudDictionaryCacheTest, ReplaceDictKeysToSchema_RefreshFailure) {
    int64_t index_id = 400;
    FakeSchemaCloudDictionaryCache cache(10);
    // Ensure no valid dictionary in cache.
    RowsetMetaCloudPB rs_meta;
    SchemaDictKeyList* dict_keys = rs_meta.mutable_schema_dict_key_list();
    dict_keys->add_column_dict_key_list(101);
    dict_keys->add_index_info_dict_key_list(201);
    rs_meta.mutable_tablet_schema();

    cache.simulate_refresh_success = false;
    Status st = cache.replace_dict_keys_to_schema(index_id, &rs_meta);
    EXPECT_FALSE(st.ok());
}

} // namespace doris