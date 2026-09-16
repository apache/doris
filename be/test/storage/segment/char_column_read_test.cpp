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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "storage/mow/mow_transform_test_base.h"

namespace doris {

struct CharReadCase {
    const char* name;
    uint32_t cid;
    TPrimitiveType::type type;
};

class CharColumnReadTest : public MowTransformTestBase,
                           public testing::WithParamInterface<CharReadCase> {
protected:
    TabletSchemaSPtr create_schema() {
        TabletSchemaPB pb;
        create_row_store_schema()->to_schema_pb(&pb);
        const auto& param = GetParam();
        auto* column = pb.mutable_column(param.cid);
        column->set_type(type_to_string(thrift_to_type(param.type)));
        column->set_length(TabletColumn::get_field_length_by_type(param.type, 4));
        column->clear_default_value();
        if (param.type != TPrimitiveType::CHAR) {
            ColumnPB child;
            child.set_unique_id(-1);
            child.set_name("item");
            child.set_type("CHAR");
            child.set_length(4);
            child.set_index_length(4);
            child.set_is_key(false);
            child.set_is_nullable(true);
            child.set_aggregation("NONE");
            column->add_children_columns()->CopyFrom(child);
            if (param.type == TPrimitiveType::MAP) {
                column->mutable_children_columns(0)->set_name("key");
                child.set_name("value");
                column->add_children_columns()->CopyFrom(child);
            }
        }
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    Field char_value() const {
        auto value = Field::create_field<TYPE_STRING>(std::string("a\0b", 3));
        switch (GetParam().type) {
        case TPrimitiveType::ARRAY:
            return Field::create_field<TYPE_ARRAY>(Array {value});
        case TPrimitiveType::MAP: {
            auto entries = Field::create_field<TYPE_ARRAY>(Array {value});
            return Field::create_field<TYPE_MAP>(Map {entries, entries});
        }
        case TPrimitiveType::STRUCT:
            return Field::create_field<TYPE_STRUCT>(Struct {value});
        default:
            return value;
        }
    }
};

// Real segment writes add CHAR padding. Both sequential reads and rowid gathers must
// preserve embedded NULs, including CHAR children, and reproduce the original primary key.
TEST_P(CharColumnReadTest, EmbeddedNulRoundTrip) {
    auto schema = create_schema();
    const auto cid = GetParam().cid;
    const auto value = char_value();
    TabletSharedPtr tablet;
    auto rowset = write_rowset_block(
            schema, 8500 + GetParam().type * 2 + GetParam().cid, 2,
            [&](Block& block) {
                auto guard = block.mutate_columns_scoped();
                auto& columns = guard.mutable_columns();
                columns[0]->insert(cid == 0 ? value : Field::create_field<TYPE_INT>(1));
                columns[1]->insert(cid == 1 ? value : Field::create_field<TYPE_INT>(10));
                columns[2]->insert_default();
                columns[3]->insert_default();
            },
            &tablet);

    Block physical;
    ASSERT_TRUE(read_rowset(rowset, schema, &physical).ok());
    ASSERT_EQ(physical.rows(), 1);
    const auto& actual = physical.get_by_position(cid);
    auto expected = actual.column->clone_empty();
    expected->insert(value);
    // Field equality does not compare ARRAY/MAP/STRUCT payloads; compare actual columns.
    EXPECT_EQ(actual.column->compare_at(0, 0, *expected, 1), 0);
    auto gathered = expected->clone_empty();
    ASSERT_TRUE(
            BaseTablet::fetch_value_by_rowids(rowset, 0, {0}, schema->column(cid), gathered).ok());
    ASSERT_EQ(gathered->size(), 1);
    EXPECT_EQ(gathered->compare_at(0, 0, *expected, 1), 0);

    if (cid != 0) {
        return;
    }
    // Check row-store agreement and primary-key identity once, with a CHAR key.
    auto row_store = schema->create_storage_block({0});
    ASSERT_TRUE(BaseTablet::fetch_value_through_row_column(rowset, *schema, 0, {0}, {0}, row_store)
                        .ok());
    ASSERT_EQ(row_store.rows(), 1);
    EXPECT_EQ(row_store.get_by_position(0).column->compare_at(0, 0, *expected, 1), 0);

    OlapBlockDataConvertor convertor(schema.get());
    convertor.set_source_content(&physical, 0, 1);
    auto [status, key_column] = convertor.convert_column_data(0);
    ASSERT_TRUE(status.ok()) << status;
    RowKeyEncoder encoder(*schema, /*mow=*/true);
    const auto encoded_key = encoder.full_encode_primary_keys({key_column}, 0);
    RowLocation location;
    std::vector<std::unique_ptr<SegmentCacheHandle>> caches(1);
    auto lookup_status = tablet->lookup_row_key(Slice(encoded_key), schema.get(), false, {rowset},
                                                &location, 2, caches);
    EXPECT_TRUE(lookup_status.ok()) << lookup_status;
}

INSTANTIATE_TEST_SUITE_P(CharProjection, CharColumnReadTest,
                         testing::Values(CharReadCase {"Key", 0, TPrimitiveType::CHAR},
                                         CharReadCase {"Value", 1, TPrimitiveType::CHAR},
                                         CharReadCase {"Array", 1, TPrimitiveType::ARRAY},
                                         CharReadCase {"Map", 1, TPrimitiveType::MAP},
                                         CharReadCase {"Struct", 1, TPrimitiveType::STRUCT}),
                         [](const testing::TestParamInfo<CharReadCase>& info) {
                             return info.param.name;
                         });

} // namespace doris
