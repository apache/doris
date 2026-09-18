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
#include <string>
#include <vector>

#include "core/field.h"
#include "storage/mow/mow_transform_test_base.h"

namespace doris {

class CharColumnReadTest : public MowTransformTestBase {};

// A physical read must preserve the row-store value and reproduce the same padded primary key.
TEST_F(CharColumnReadTest, EmbeddedNulPreservesPrimaryKey) {
    TabletSchemaPB pb;
    create_row_store_schema()->to_schema_pb(&pb);
    pb.mutable_column(0)->set_type("CHAR");
    pb.mutable_column(0)->set_length(4);
    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(pb);
    const std::string value("a\0b", 3);
    TabletSharedPtr tablet;
    auto rowset = write_rowset_block(
            schema, 8500, 2,
            [&](Block& block) {
                auto guard = block.mutate_columns_scoped();
                auto& columns = guard.mutable_columns();
                columns[0]->insert(Field::create_field<TYPE_STRING>(value));
                columns[1]->insert(Field::create_field<TYPE_INT>(10));
                columns[2]->insert_default();
                columns[3]->insert_default();
            },
            &tablet);

    Block physical;
    ASSERT_TRUE(read_rowset(rowset, schema, &physical).ok());
    ASSERT_EQ(physical.rows(), 1);
    EXPECT_EQ(physical.get_by_position(0).column->get_data_at(0).to_string(), value);
    auto gathered = physical.get_by_position(0).column->clone_empty();
    ASSERT_TRUE(
            BaseTablet::fetch_value_by_rowids(rowset, 0, {0}, schema->column(0), gathered).ok());
    ASSERT_EQ(gathered->size(), 1);
    EXPECT_EQ(gathered->get_data_at(0).to_string(), value);

    auto row_store = schema->create_storage_block({0});
    ASSERT_TRUE(BaseTablet::fetch_value_through_row_column(rowset, *schema, 0, {0}, {0}, row_store)
                        .ok());
    ASSERT_EQ(row_store.rows(), 1);
    EXPECT_EQ(row_store.get_by_position(0).column->get_data_at(0).to_string(), value);

    OlapBlockDataConvertor convertor(schema.get());
    convertor.set_source_content(&physical, 0, 1);
    auto [status, key_column] = convertor.convert_column_data(0);
    ASSERT_TRUE(status.ok()) << status;
    RowKeyEncoder encoder(*schema, /*mow=*/true);
    const auto key = encoder.full_encode_primary_keys({key_column}, 0);
    RowLocation location;
    std::vector<std::unique_ptr<SegmentCacheHandle>> caches(1);
    const auto lookup_status =
            tablet->lookup_row_key(Slice(key), schema.get(), false, {rowset}, &location, 2, caches);
    EXPECT_TRUE(lookup_status.ok()) << lookup_status;
}

} // namespace doris
