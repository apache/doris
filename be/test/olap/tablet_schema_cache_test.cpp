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

#include "olap/tablet_schema_cache.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "exec/tablet_info.h"
#include "olap/tablet_schema.h"

namespace doris {

static TabletColumn create_test_column(int32_t unique_id, const std::string& name,
                                       bool is_nullable = false) {
    TabletColumn column;
    column.set_unique_id(unique_id);
    column.set_name(name);
    column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    column.set_is_key(true);
    column.set_is_nullable(is_nullable);
    column.set_length(4);
    column.set_index_length(4);
    return column;
}

static void set_schema_param(OlapTableSchemaParam* param, int64_t table_id = 20, int64_t db_id = 10,
                             int64_t version = 3) {
    POlapTableSchemaParam pschema;
    pschema.set_db_id(db_id);
    pschema.set_table_id(table_id);
    pschema.set_version(version);
    Status st = param->init(pschema);
    ASSERT_TRUE(st.ok()) << st;
}

TEST(TabletSchemaCacheTest, LoadSchemaCacheKeyUsesFullIndexSchema) {
    TabletSchema ori_schema;
    ori_schema.set_schema_version(1);
    ori_schema.append_column(create_test_column(1, "k1"));

    OlapTableSchemaParam param;
    set_schema_param(&param);
    TabletColumn column1 = create_test_column(1, "k1");
    TabletColumn column2 = create_test_column(1, "k1_changed");

    OlapTableIndexSchema index_schema1;
    index_schema1.index_id = 100;
    index_schema1.schema_hash = 200;
    index_schema1.columns.push_back(&column1);

    OlapTableIndexSchema index_schema2;
    index_schema2.index_id = 100;
    index_schema2.schema_hash = 200;
    index_schema2.columns.push_back(&column2);

    auto key1 =
            TabletSchemaCache::build_load_schema_cache_key(100, &param, ori_schema, &index_schema1);
    auto key2 =
            TabletSchemaCache::build_load_schema_cache_key(100, &param, ori_schema, &index_schema2);
    EXPECT_NE(key1, key2);

    auto same_key =
            TabletSchemaCache::build_load_schema_cache_key(100, &param, ori_schema, &index_schema1);
    EXPECT_EQ(key1, same_key);

    auto fallback_key =
            TabletSchemaCache::build_load_schema_cache_key(100, &param, ori_schema, nullptr);
    EXPECT_NE(key1, fallback_key);
    auto same_fallback_key =
            TabletSchemaCache::build_load_schema_cache_key(100, &param, ori_schema, nullptr);
    EXPECT_EQ(fallback_key, same_fallback_key);

    OlapTableSchemaParam other_table_param;
    set_schema_param(&other_table_param, 21);
    auto other_table_key = TabletSchemaCache::build_load_schema_cache_key(
            100, &other_table_param, ori_schema, &index_schema1);
    EXPECT_NE(key1, other_table_key);
}

TEST(TabletSchemaCacheTest, InsertAndLookupLoadSchema) {
    TabletSchema ori_schema;
    ori_schema.set_schema_version(1);
    ori_schema.append_column(create_test_column(1, "k1"));

    OlapTableSchemaParam param;
    set_schema_param(&param, 30);
    TabletColumn column = create_test_column(1, "k1");
    OlapTableIndexSchema index_schema;
    index_schema.index_id = 100;
    index_schema.schema_hash = 200;
    index_schema.columns.push_back(&column);

    auto cache_key =
            TabletSchemaCache::build_load_schema_cache_key(100, &param, ori_schema, &index_schema);
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->copy_from(ori_schema);

    auto inserted = TabletSchemaCache::instance()->insert(cache_key, tablet_schema);
    EXPECT_EQ(tablet_schema.get(), inserted.second.get());
    TabletSchemaCache::instance()->release(inserted.first);

    auto cached = TabletSchemaCache::instance()->lookup_schema(cache_key);
    ASSERT_NE(nullptr, cached.first);
    EXPECT_EQ(tablet_schema.get(), cached.second.get());
    TabletSchemaCache::instance()->release(cached.first);
}

} // namespace doris
