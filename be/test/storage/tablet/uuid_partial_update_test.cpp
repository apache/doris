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

#include <set>

#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_uuid.h"
#include "core/value/uuid_value.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/partial_update_info.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

TEST(UuidPartialUpdateTest, DefaultsAreRowAlignedAndLiteralDefaultsBroadcast) {
    for (bool nullable : {false, true}) {
        for (const std::string& name : {"uuid_v4()", "UUID_V7()"}) {
            TabletSchema schema;
            schema.set_table_id(123);
            TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                FieldType::OLAP_FIELD_TYPE_UUID, nullable);
            column.set_name("u");
            column.set_default_value(name);
            column.set_unique_id(1);
            TabletColumn key(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                             FieldType::OLAP_FIELD_TYPE_INT, true);
            key.set_name("k");
            key.set_is_key(true);
            key.set_unique_id(0);
            schema.append_column(key);
            schema.append_column(column);
            DataTypePtr type = std::make_shared<DataTypeUUID>();
            if (nullable) {
                type = make_nullable(type);
            }
            Block ref {{type->create_column(), type, "u"}};
            auto keys = ColumnInt32::create();
            for (int32_t row = 0; row < 128; ++row) {
                keys->insert_value(row);
            }
            auto key_type = make_nullable(std::make_shared<DataTypeInt32>());
            auto nullable_keys =
                    ColumnNullable::create(std::move(keys), ColumnUInt8::create(128, 0));
            nullable_keys->get_null_map_data()[127] = 1;
            Block rows {{std::move(nullable_keys), key_type, "k"}};
            PUniqueId load_id;
            load_id.set_hi(1234);
            load_id.set_lo(5678);
            PartialUpdateInfo info;
            ASSERT_TRUE(info.init(1, 2, schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                                  PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 1750000000000,
                                  0, "UTC", "", -1, -1, &load_id)
                                .ok());
            auto defaults = ref.clone_empty();
            ASSERT_TRUE(BaseTablet::generate_default_value_block(schema, {1}, info, rows, defaults)
                                .ok());
            const auto& output = *defaults.get_by_position(0).column;
            ASSERT_EQ(output.size(), 128);
            const auto& values = assert_cast<const ColumnUUID&>(
                    nullable ? assert_cast<const ColumnNullable&>(output).get_nested_column()
                             : output);
            std::set<UUIDValueType> distinct;
            for (size_t row = 0; row < output.size(); ++row) {
                EXPECT_EQ(UUIDValue::version(values.get_element(row)), name == "uuid_v4()" ? 4 : 7);
                EXPECT_EQ(static_cast<uint8_t>((values.get_element(row) >> 62) & 3), 2);
                if (name == "UUID_V7()") {
                    EXPECT_EQ(static_cast<uint64_t>(values.get_element(row) >> 80),
                              info.timestamp_ms);
                }
                distinct.insert(values.get_element(row));
                if (nullable) {
                    EXPECT_FALSE(assert_cast<const ColumnNullable&>(output).is_null_at(row));
                }
            }
            EXPECT_EQ(distinct.size(), 128);

            // Simulate another replica recovering the metadata and reading the same keys in
            // a different publish order. No row ordinal or process-local random state is shared.
            PartialUpdateInfoPB pb;
            info.to_pb(&pb);
            PartialUpdateInfo restored;
            restored.from_pb(&pb);
            TabletSchemaPB schema_pb;
            schema.to_schema_pb(&schema_pb);
            TabletSchema restored_schema;
            restored_schema.init_from_pb(schema_pb);
            auto replica_defaults = ref.clone_empty();
            const std::map<uint32_t, uint32_t> reordered {{0, 127}, {1, 73}, {2, 0}};
            ASSERT_TRUE(BaseTablet::generate_default_value_block(restored_schema, {1}, restored,
                                                                 rows, replica_defaults, &reordered)
                                .ok());
            for (auto [target, source] : reordered) {
                EXPECT_EQ(type->to_string(*replica_defaults.get_by_position(0).column, target),
                          type->to_string(output, source));
            }
            restored.load_id_lo++;
            auto other_load = ref.clone_empty();
            ASSERT_TRUE(BaseTablet::generate_default_value_block(schema, {1}, restored, rows,
                                                                 other_load)
                                .ok());
            EXPECT_NE(type->to_string(*other_load.get_by_position(0).column, 0),
                      type->to_string(output, 0));
            schema.mutable_column(1).set_name("other_uuid");
            auto other_column = ref.clone_empty();
            ASSERT_TRUE(
                    BaseTablet::generate_default_value_block(schema, {1}, info, rows, other_column)
                            .ok());
            EXPECT_NE(type->to_string(*other_column.get_by_position(0).column, 0),
                      type->to_string(output, 0));
            restored.has_load_id = false;
            auto legacy_defaults = ref.clone_empty();
            EXPECT_FALSE(BaseTablet::generate_default_value_block(schema, {1}, restored, rows,
                                                                  legacy_defaults)
                                 .ok());
            defaults = ref.clone_empty();
            const std::string literal = "00112233-4455-6677-8899-aabbccddeeff";
            info.default_values = {literal};
            info.has_load_id = false; // Existing literal defaults need no new metadata.
            ASSERT_TRUE(BaseTablet::generate_default_value_block(schema, {1}, info, rows, defaults)
                                .ok());
            EXPECT_EQ(defaults.rows(), 1);
            EXPECT_EQ(type->to_string(*defaults.get_by_position(0).column, 0), literal);
        }
    }
}

TEST(UuidPartialUpdateTest, DefaultsUseLogicalColumnsAcrossIndexSchemas) {
    for (const std::string& function : {"uuid_v4()", "uuid_v7()"}) {
        auto key_type = std::make_shared<DataTypeInt32>();
        auto uuid_type = std::make_shared<DataTypeUUID>();
        auto first_key = ColumnInt32::create();
        auto second_key = ColumnInt32::create();
        for (int32_t row = 0; row < 16; ++row) {
            first_key->insert_value(row);
            second_key->insert_value(row + 100);
        }
        Block base_rows {{first_key->get_ptr(), key_type, "a"},
                         {second_key->get_ptr(), key_type, "b"}};
        Block rollup_rows {{second_key->get_ptr(), key_type, "b"},
                           {first_key->get_ptr(), key_type, "a"}};
        Block ref {{uuid_type->create_column(), uuid_type, "u"},
                   {uuid_type->create_column(), uuid_type, "v"}};
        TabletSchema base;
        for (const std::string& name : {"a", "b", "u", "v"}) {
            const bool is_key = name == "a" || name == "b";
            TabletColumn column(
                    FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                    is_key ? FieldType::OLAP_FIELD_TYPE_INT : FieldType::OLAP_FIELD_TYPE_UUID,
                    false);
            column.set_name(name);
            column.set_is_key(is_key);
            column.set_unique_id(-1); // light_schema_change=false
            if (!is_key) {
                column.set_default_value(function);
            }
            base.append_column(column);
        }
        PartialUpdateInfo info;
        info.has_load_id = true;
        info.load_id_hi = 1234;
        info.load_id_lo = 5678;
        info.timestamp_ms = 1750000000000;
        info.default_values = {function, function};
        auto expected = ref.clone_empty();
        ASSERT_TRUE(
                BaseTablet::generate_default_value_block(base, {2, 3}, info, base_rows, expected)
                        .ok());
        for (size_t row = 0; row < base_rows.rows(); ++row) {
            EXPECT_NE(uuid_type->to_string(*expected.get_by_position(0).column, row),
                      uuid_type->to_string(*expected.get_by_position(1).column, row));
        }
        // A rollup has the same logical names but reordered keys and unrelated IDs. Persist
        // both metadata objects before generating defaults in a different publish row order.
        TabletSchema rollup;
        for (int pos : {1, 0, 2, 3}) {
            auto column = base.column(pos);
            column.set_unique_id(20 + pos);
            rollup.append_column(column);
        }
        TabletSchemaPB schema_pb;
        rollup.to_schema_pb(&schema_pb);
        TabletSchema restored_schema;
        restored_schema.init_from_pb(schema_pb);
        PartialUpdateInfoPB info_pb;
        info.to_pb(&info_pb);
        PartialUpdateInfo restored_info;
        restored_info.from_pb(&info_pb);
        const std::map<uint32_t, uint32_t> row_indices {{0, 15}, {1, 7}, {2, 0}};
        auto actual = ref.clone_empty();
        ASSERT_TRUE(BaseTablet::generate_default_value_block(restored_schema, {2, 3}, restored_info,
                                                             rollup_rows, actual, &row_indices)
                            .ok());
        for (const auto& [target, source] : row_indices) {
            for (size_t column = 0; column < 2; ++column) {
                EXPECT_EQ(uuid_type->to_string(*actual.get_by_position(column).column, target),
                          uuid_type->to_string(*expected.get_by_position(column).column, source));
            }
        }
        // Swapping key values without swapping their identities must still identify another row.
        auto other_key = ref.clone_empty();
        ASSERT_TRUE(
                BaseTablet::generate_default_value_block(base, {2, 3}, info, rollup_rows, other_key)
                        .ok());
        EXPECT_NE(uuid_type->to_string(*other_key.get_by_position(0).column, 0),
                  uuid_type->to_string(*expected.get_by_position(0).column, 0));
    }
}

} // namespace doris
