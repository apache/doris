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

#include "storage/tablet/tablet_reader.h"

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/delete/delete_handler.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

class TabletReaderTest : public testing::Test {
protected:
    static TabletSchemaSPtr create_schema(
            const std::vector<std::pair<std::string, int32_t>>& name_and_uid) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        bool first = true;
        for (const auto& [name, uid] : name_and_uid) {
            auto* col = schema_pb.add_column();
            col->set_unique_id(uid);
            col->set_name(name);
            col->set_type("INT");
            col->set_is_key(first);
            col->set_is_nullable(!first);
            first = false;
        }
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(schema_pb);
        return schema;
    }

    // Build a DeleteHandler initialized with a single delete-predicate rowset.
    static void init_delete_handler(DeleteHandler& handler, const TabletSchemaSPtr& schema,
                                    const DeletePredicatePB& delete_predicate) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_tablet_schema(schema);
        rs_meta->set_version(Version(2, 2));
        rs_meta->set_delete_predicate(delete_predicate);
        ASSERT_TRUE(handler.init(schema, {rs_meta}, /*version=*/100).ok());
    }
};

// The delete columns (resolved by the delete handler to field ids) are mapped back to their
// column unique ids and stripped from all_access_paths; unrelated columns keep their paths.
TEST_F(TabletReaderTest, remove_delete_columns_from_access_paths) {
    auto schema = create_schema({{"k1", 10}, {"k2", 11}, {"v1", 12}, {"v2", 13}, {"v4", 15}});

    DeletePredicatePB delete_predicate;
    auto* p1 = delete_predicate.add_sub_predicates_v2(); // k1 -> erase 10
    p1->set_column_name("k1");
    p1->set_column_unique_id(10);
    p1->set_op("=");
    p1->set_cond_value("1");
    auto* p2 = delete_predicate.add_sub_predicates_v2(); // v1 -> erase 12
    p2->set_column_name("v1");
    p2->set_column_unique_id(12);
    p2->set_op("=");
    p2->set_cond_value("2");
    auto* in1 = delete_predicate.add_in_predicates(); // k2 IN (...) -> erase 11
    in1->set_column_name("k2");
    in1->set_column_unique_id(11);
    in1->set_is_not_in(false);
    in1->add_values("3");
    in1->add_values("4");

    DeleteHandler handler;
    init_delete_handler(handler, schema, delete_predicate);

    std::map<int32_t, TColumnAccessPaths> access_paths;
    for (int32_t uid : {10, 11, 12, 13, 15}) {
        access_paths[uid] = TColumnAccessPaths {};
    }

    TabletReader::remove_delete_columns_from_access_paths(handler, *schema, access_paths);

    EXPECT_EQ(size_t(2), access_paths.size());
    EXPECT_EQ(size_t(1), access_paths.count(13)) << "non-delete column must keep its access path";
    EXPECT_EQ(size_t(1), access_paths.count(15)) << "non-delete column must keep its access path";
    for (int32_t uid : {10, 11, 12}) {
        EXPECT_EQ(size_t(0), access_paths.count(uid)) << "delete column " << uid << " not erased";
    }
}

// A delete condition whose column has no access path leaves the map untouched.
TEST_F(TabletReaderTest, remove_delete_columns_keeps_unrelated_paths) {
    auto schema = create_schema({{"k1", 10}, {"v1", 12}});

    DeletePredicatePB delete_predicate;
    auto* p1 = delete_predicate.add_sub_predicates_v2();
    p1->set_column_name("k1");
    p1->set_column_unique_id(10);
    p1->set_op("=");
    p1->set_cond_value("1");

    DeleteHandler handler;
    init_delete_handler(handler, schema, delete_predicate);

    std::map<int32_t, TColumnAccessPaths> access_paths;
    access_paths[12] = TColumnAccessPaths {};
    access_paths[15] = TColumnAccessPaths {};

    TabletReader::remove_delete_columns_from_access_paths(handler, *schema, access_paths);

    EXPECT_EQ(size_t(2), access_paths.size());
}

} // namespace doris
