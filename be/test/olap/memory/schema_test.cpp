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

#include "olap/memory/schema.h"

#include <gtest/gtest.h>

#include <vector>

namespace doris {
namespace memory {

TEST(ColumnSchema, create) {
    ColumnSchema cs(1, "uid", ColumnType::OLAP_FIELD_TYPE_TINYINT, false, true);
    EXPECT_EQ(1, cs.cid());
    EXPECT_EQ(std::string("uid"), cs.name());
    EXPECT_FALSE(cs.is_nullable());
    EXPECT_TRUE(cs.is_key());
}

TEST(Schema, desc_create) {
    scoped_refptr<Schema> sc;
    ASSERT_TRUE(Schema::create("id int,uv int,pv int,city tinyint null", &sc).ok());
    ASSERT_EQ(sc->num_columns(), 4);
    ASSERT_EQ(sc->num_key_columns(), 1);
    ASSERT_EQ(sc->get(0)->cid(), 1);
    ASSERT_EQ(sc->get(1)->cid(), 2);
    ASSERT_EQ(sc->get(2)->cid(), 3);
    ASSERT_EQ(sc->get(3)->cid(), 4);
    ASSERT_EQ(sc->get_by_name("city")->is_nullable(), true);
    ASSERT_EQ(sc->get_by_name("pv")->is_nullable(), false);
    ASSERT_EQ(sc->get_by_name("uv")->type(), ColumnType::OLAP_FIELD_TYPE_INT);
}

TEST(Schema, create) {
    TabletSchemaPB tspb;
    auto cpb = tspb.add_column();
    cpb->set_unique_id(1);
    cpb->set_name("uid");
    cpb->set_type(TabletColumn::get_string_by_field_type(FieldType::OLAP_FIELD_TYPE_INT));
    cpb->set_is_nullable(false);
    cpb->set_is_key(true);
    auto cpb2 = tspb.add_column();
    cpb2->set_unique_id(2);
    cpb2->set_type(TabletColumn::get_string_by_field_type(FieldType::OLAP_FIELD_TYPE_INT));
    cpb2->set_name("city");
    cpb2->set_is_nullable(true);
    cpb2->set_is_key(false);
    tspb.set_keys_type(KeysType::UNIQUE_KEYS);
    tspb.set_next_column_unique_id(3);
    tspb.set_num_short_key_columns(1);
    tspb.set_is_in_memory(false);
    TabletSchema ts;
    ts.init_from_pb(tspb);
    scoped_refptr<Schema> schema(new Schema(ts));
    EXPECT_EQ(schema->cid_size(), 3);
    EXPECT_EQ(schema->get_by_name("uid")->name(), std::string("uid"));
    EXPECT_EQ(schema->get_by_cid(1)->name(), std::string("uid"));
}

} // namespace memory
} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
