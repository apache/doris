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

#include "format/table/table_schema_change_helper.h"

#include <gtest/gtest.h>

#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_factory.hpp"
#include "format/table/iceberg_reader.h"
#include "format/table/partition_column_filler.h"
#include "testutil/desc_tbl_builder.h"

namespace doris {
class MockTableSchemaChangeHelper : public TableSchemaChangeHelper {};

namespace {

schema::external::TStructField partial_name_mapping_root_field() {
    TColumnType int_type;
    int_type.type = TPrimitiveType::INT;

    schema::external::TStructField root_field;
    for (const auto& [name, id, aliases] :
         std::vector<std::tuple<std::string, int32_t, std::vector<std::string>>> {{"a", 1, {"a"}},
                                                                                  {"b", 2, {}}}) {
        auto field = std::make_shared<schema::external::TField>();
        field->__set_name(name);
        field->__set_id(id);
        field->__set_type(int_type);
        field->__set_name_mapping(aliases);
        field->__set_name_mapping_is_authoritative(true);
        schema::external::TFieldPtr field_ptr;
        field_ptr.__set_field_ptr(field);
        root_field.fields.emplace_back(std::move(field_ptr));
    }
    return root_field;
}

schema::external::TStructField nested_partial_name_mapping_root_field() {
    TColumnType struct_type;
    struct_type.type = TPrimitiveType::STRUCT;

    auto nested_fields = partial_name_mapping_root_field();
    nested_fields.fields[0].field_ptr->__set_name_mapping({});

    auto field = std::make_shared<schema::external::TField>();
    field->__set_name("s");
    field->__set_id(10);
    field->__set_type(struct_type);
    field->__set_name_mapping({});
    field->__set_name_mapping_is_authoritative(true);
    field->nestedField.__set_struct_field(std::move(nested_fields));
    field->__isset.nestedField = true;

    schema::external::TFieldPtr field_ptr;
    field_ptr.__set_field_ptr(field);
    schema::external::TStructField root_field;
    root_field.fields.emplace_back(std::move(field_ptr));
    return root_field;
}

} // namespace

TEST(PartitionColumnFillerTest, FillNullableStringPartitionValue) {
    SlotDescriptor slot;
    slot._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, true);
    slot._col_name = "dt";
    auto column = slot._type->create_column();

    ASSERT_TRUE(fill_partition_column_from_path_value(*column, slot, "2026-05-26", 3, false).ok());

    auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
    ASSERT_EQ(3, nullable_column->size());
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_FALSE(nullable_column->is_null_at(i));
        EXPECT_EQ("2026-05-26", nullable_column->get_data_at(i).to_string());
    }
}

TEST(PartitionColumnFillerTest, FillNullableIntPartitionValue) {
    SlotDescriptor slot;
    slot._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    slot._col_name = "pt";
    auto column = slot._type->create_column();

    ASSERT_TRUE(fill_partition_column_from_path_value(*column, slot, "42", 4, false).ok());

    auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
    auto& nested_column = assert_cast<ColumnInt32&>(nullable_column->get_nested_column());
    ASSERT_EQ(4, nullable_column->size());
    for (size_t i = 0; i < 4; ++i) {
        EXPECT_FALSE(nullable_column->is_null_at(i));
        EXPECT_EQ(42, nested_column.get_data()[i]);
    }
}

TEST(MockTableSchemaChangeHelper, OrcNameNoSchemaChange) {
    std::vector<DataTypePtr> data_types;
    std::vector<std::string> column_names;

    SlotDescriptor slot1;
    slot1._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, true);
    slot1._col_name = "col1";

    SlotDescriptor slot2;
    slot2._type = std::make_shared<DataTypeStruct>(
            std::vector<DataTypePtr> {
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, true),
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, true)},
            Strings {"a", "b"});
    slot2._col_name = "col2";

    SlotDescriptor slot3;
    slot3._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    slot3._col_name = "col3";

    TupleDescriptor tuple_desc;
    tuple_desc.add_slot(&slot1);
    tuple_desc.add_slot(&slot2);
    tuple_desc.add_slot(&slot3);

    std::cout << tuple_desc.debug_string() << "\n";

    std::unique_ptr<orc::Type> orc_type(
            orc::Type::buildTypeFromString("struct<col1:int,col2:struct<a:int,b:int>,col3:int>"));

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_name(&tuple_desc,
                                                                         orc_type.get(), ans_node)
                        .ok());
    std::cout << TableSchemaChangeHelper::debug(ans_node) << "\n";

    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  col1 (file: col1)\n"
              "    ScalarNode\n"
              "  col2 (file: col2)\n"
              "    StructNode\n"
              "      a (file: a)\n"
              "        ScalarNode\n"
              "      b (file: b)\n"
              "        ScalarNode\n"
              "  col3 (file: col3)\n"
              "    ScalarNode\n");
}

TEST(MockTableSchemaChangeHelper, HasChildrenColumnGuardsAbsentProjectedColumn) {
    // Build a top-level StructNode with two known columns via the public by_orc_name path.
    SlotDescriptor slot1;
    slot1._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    slot1._col_name = "col1";

    SlotDescriptor slot2;
    slot2._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    slot2._col_name = "col2";

    TupleDescriptor tuple_desc;
    tuple_desc.add_slot(&slot1);
    tuple_desc.add_slot(&slot2);

    std::unique_ptr<orc::Type> orc_type(
            orc::Type::buildTypeFromString("struct<col1:int,col2:int>"));
    std::shared_ptr<TableSchemaChangeHelper::Node> node = nullptr;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_name(&tuple_desc,
                                                                         orc_type.get(), node)
                        .ok());

    // A projected column that IS in the table schema tree -> present.
    EXPECT_TRUE(node->has_children_column("col1"));
    EXPECT_TRUE(node->has_children_column("col2"));
    // A projected column that is NOT in the tree at all models an FE/BE schema-contract mismatch
    // (e.g. a paimon renamed column missing from a stale history_schema_info entry). has_children_column
    // must report it absent WITHOUT the DCHECK/abort that children_column_exists would trigger — this is
    // the presence check the parquet reader's missing-cols guard relies on to fail the query instead of
    // aborting the whole BE. MUTATION: having has_children_column call children.at() -> abort/red.
    EXPECT_FALSE(node->has_children_column("not_a_projected_column"));

    // ConstNode (the no-schema-change path) reports every column present, matching its
    // children_column_exists.
    EXPECT_TRUE(
            TableSchemaChangeHelper::ConstNode::get_instance()->has_children_column("anything"));
}

TEST(MockTableSchemaChangeHelper, OrcNameSchemaChange1) {
    std::vector<DataTypePtr> data_types;
    std::vector<std::string> column_names;

    SlotDescriptor slot1;
    slot1._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, true);
    slot1._col_name = "col1";

    SlotDescriptor slot2;
    slot2._type = std::make_shared<DataTypeStruct>(
            std::vector<DataTypePtr> {
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, true),
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, true)},
            Strings {"a", "b"});
    slot2._col_name = "col2";

    SlotDescriptor slot3;
    slot3._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    slot3._col_name = "col3";

    TupleDescriptor tuple_desc;
    tuple_desc.add_slot(&slot1);
    tuple_desc.add_slot(&slot2);
    tuple_desc.add_slot(&slot3);

    std::cout << tuple_desc.debug_string() << "\n";
    {
        std::unique_ptr<orc::Type> orc_type(
                orc::Type::buildTypeFromString("struct<col1:int,col2:struct<a:int>,col3:int>"));

        std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
        ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_name(
                            &tuple_desc, orc_type.get(), ans_node)
                            .ok());
        std::cout << TableSchemaChangeHelper::debug(ans_node) << "\n";
        ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
                  "StructNode\n"
                  "  col1 (file: col1)\n"
                  "    ScalarNode\n"
                  "  col2 (file: col2)\n"
                  "    StructNode\n"
                  "      a (file: a)\n"
                  "        ScalarNode\n"
                  "      b (not exists)\n"
                  "  col3 (file: col3)\n"
                  "    ScalarNode\n");
    }
    {
        std::unique_ptr<orc::Type> orc_type(
                orc::Type::buildTypeFromString("struct<col111:int,COL2:struct<A:int>,Col3:int>"));

        std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
        ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_name(
                            &tuple_desc, orc_type.get(), ans_node)
                            .ok());
        std::cout << TableSchemaChangeHelper::debug(ans_node) << "\n";
        ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
                  "StructNode\n"
                  "  col1 (not exists)\n"
                  "  col2 (file: COL2)\n"
                  "    StructNode\n"
                  "      a (file: A)\n"
                  "        ScalarNode\n"
                  "      b (not exists)\n"
                  "  col3 (file: Col3)\n"
                  "    ScalarNode\n");
    }

    {
        std::unique_ptr<orc::Type> orc_type(orc::Type::buildTypeFromString(
                "struct<col111:int,col1:int,CoL3:int,COL2:struct<A:int>>"));
        std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
        ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_name(
                            &tuple_desc, orc_type.get(), ans_node)
                            .ok());
        std::cout << TableSchemaChangeHelper::debug(ans_node) << "\n";
        ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
                  "StructNode\n"
                  "  col1 (file: col1)\n"
                  "    ScalarNode\n"
                  "  col2 (file: COL2)\n"
                  "    StructNode\n"
                  "      a (file: A)\n"
                  "        ScalarNode\n"
                  "      b (not exists)\n"
                  "  col3 (file: CoL3)\n"
                  "    ScalarNode\n");
    }

    {
        std::unique_ptr<orc::Type> orc_type(orc::Type::buildTypeFromString("struct<col111:int>"));
        std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
        ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_name(
                            &tuple_desc, orc_type.get(), ans_node)
                            .ok());
        std::cout << TableSchemaChangeHelper::debug(ans_node) << "\n";
        ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
                  "StructNode\n"
                  "  col1 (not exists)\n"
                  "  col2 (not exists)\n"
                  "  col3 (not exists)\n");
    }
}

TEST(MockTableSchemaChangeHelper, ParquetNameSchemaChange) {
    std::vector<DataTypePtr> data_types;
    std::vector<std::string> column_names;

    SlotDescriptor slot1;
    slot1._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, true);
    slot1._col_name = "col1";

    SlotDescriptor slot2;
    slot2._type = std::make_shared<DataTypeStruct>(
            std::vector<DataTypePtr> {
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, true),
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, true)},
            Strings {"a", "b"});
    slot2._col_name = "col2";

    SlotDescriptor slot3;
    slot3._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    slot3._col_name = "col3";

    TupleDescriptor tuple_desc;
    tuple_desc.add_slot(&slot1);
    tuple_desc.add_slot(&slot2);
    tuple_desc.add_slot(&slot3);

    FieldDescriptor parquet_field;

    FieldSchema parquet_field_col1;
    {
        parquet_field_col1.name = "col1";
        parquet_field_col1.data_type =
                DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, true);
        parquet_field_col1.field_id = -1;
        parquet_field._fields.emplace_back(parquet_field_col1);
    }
    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_name(
                        &tuple_desc, parquet_field, ans_node)
                        .ok());
    std::cout << TableSchemaChangeHelper::debug(ans_node) << "\n";

    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  col1 (file: col1)\n"
              "    ScalarNode\n"
              "  col2 (not exists)\n"
              "  col3 (not exists)\n");
}

TEST(MockTableSchemaChangeHelper, IcebergParquetSchemaChange) {
    schema::external::TField test_field;
    TColumnType struct_type;
    struct_type.type = TPrimitiveType::STRUCT;
    test_field.type = struct_type;
    schema::external::TStructField root_field;
    {
        TColumnType int_type;
        int_type.type = TPrimitiveType::INT;

        {
            auto col1_field = std::make_shared<schema::external::TField>();
            col1_field->name = "col1";
            col1_field->id = 1;
            col1_field->type = int_type;

            schema::external::TFieldPtr col1_ptr;
            col1_ptr.field_ptr = col1_field;
            root_field.fields.emplace_back(col1_ptr);
        }

        {
            auto col2_field = std::make_shared<schema::external::TField>();
            col2_field->name = "col2";
            col2_field->id = 2;
            col2_field->type = struct_type;

            schema::external::TStructField struct_field;
            {
                auto a_field = std::make_shared<schema::external::TField>();
                a_field->name = "a";
                a_field->id = 3;
                a_field->type = int_type;
                schema::external::TFieldPtr a_ptr;
                a_ptr.field_ptr = a_field;
                struct_field.fields.emplace_back(a_ptr);
            }

            col2_field->nestedField.struct_field = struct_field;
            schema::external::TFieldPtr col2_ptr;
            col2_ptr.field_ptr = col2_field;
            root_field.fields.emplace_back(col2_ptr);
        }
    }

    FieldSchema parquet_field;
    std::vector<DataTypePtr> table_data_type;
    Strings table_names;
    {
        {
            FieldSchema parquet_field_col1;
            parquet_field_col1.name = "col1";
            parquet_field_col1.data_type =
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, true);
            parquet_field_col1.field_id = 1;
            parquet_field.children.emplace_back(parquet_field_col1);

            table_data_type.emplace_back(parquet_field_col1.data_type);
            table_names.emplace_back(parquet_field_col1.name);
        }

        {
            FieldSchema parquet_field_col2;
            parquet_field_col2.name = "coL1";

            std::vector<DataTypePtr> sub_data_type;
            Strings sub_names;

            {
                FieldSchema b_field;
                b_field.name = "b5555555";
                b_field.field_id = 4;
                b_field.data_type = DataTypeFactory::instance().create_data_type(
                        PrimitiveType::TYPE_BIGINT, true);
                sub_data_type.emplace_back(b_field.data_type);
                sub_names.emplace_back(b_field.name);
                parquet_field_col2.children.emplace_back(b_field);
            }
            {
                FieldSchema a_field;
                a_field.name = "a33333333";
                a_field.field_id = 3;
                a_field.data_type = DataTypeFactory::instance().create_data_type(
                        PrimitiveType::TYPE_BIGINT, true);
                sub_data_type.emplace_back(a_field.data_type);
                sub_names.emplace_back(a_field.name);

                parquet_field_col2.children.emplace_back(a_field);
            }
            parquet_field_col2.data_type =
                    std::make_shared<DataTypeStruct>(sub_data_type, sub_names);
            parquet_field_col2.field_id = 2;

            parquet_field.children.emplace_back(parquet_field_col2);

            table_data_type.emplace_back(parquet_field_col2.data_type);
            table_names.emplace_back(parquet_field_col2.name);
        }
    }
    parquet_field.data_type = std::make_shared<DataTypeStruct>(table_data_type, table_names);
    test_field.nestedField.struct_field = root_field;
    bool exist_field_id = true;
    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_field_id(
                        test_field, parquet_field, ans_node, exist_field_id)
                        .ok());
    ASSERT_TRUE(exist_field_id);
    std::cout << TableSchemaChangeHelper::debug(ans_node) << "\n";

    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  col1 (file: col1)\n"
              "    ScalarNode\n"
              "  col2 (file: coL1)\n"
              "    StructNode\n"
              "      a (file: a33333333)\n"
              "        ScalarNode\n"

    );
}

TEST(MockTableSchemaChangeHelper, IcebergParquetNameMappingFallback) {
    TColumnType int_type;
    int_type.type = TPrimitiveType::INT;

    schema::external::TStructField root_field;
    auto renamed_field = std::make_shared<schema::external::TField>();
    renamed_field->name = "col1_new";
    renamed_field->id = 1;
    renamed_field->type = int_type;
    renamed_field->__set_name_mapping(std::vector<std::string> {"col1_old"});
    schema::external::TFieldPtr renamed_ptr;
    renamed_ptr.field_ptr = renamed_field;
    root_field.fields.emplace_back(renamed_ptr);

    FieldDescriptor parquet_field;
    FieldSchema parquet_field_col1;
    parquet_field_col1.name = "col1_old";
    parquet_field_col1.data_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, true);
    parquet_field_col1.field_id = -1;
    parquet_field._fields.emplace_back(parquet_field_col1);

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_field_id_with_name_mapping(
                        root_field, parquet_field, ans_node)
                        .ok());

    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  col1_new (file: col1_old)\n"
              "    ScalarNode\n");
}

TEST(MockTableSchemaChangeHelper, IcebergParquetLegacyEmptyNameMappingFallsBack) {
    auto root_field = partial_name_mapping_root_field();
    root_field.fields.resize(1);
    root_field.fields[0].field_ptr->__set_name_mapping({});
    root_field.fields[0].field_ptr->__isset.name_mapping_is_authoritative = false;

    FieldDescriptor parquet_field;
    FieldSchema file_field;
    file_field.name = "a";
    file_field.field_id = -1;
    file_field.data_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    parquet_field._fields.emplace_back(std::move(file_field));

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_field_id_with_name_mapping(
                        root_field, parquet_field, ans_node)
                        .ok());
    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  a (file: a)\n"
              "    ScalarNode\n");
}

TEST(MockTableSchemaChangeHelper, IcebergParquetPartialNameMappingIsStrict) {
    auto root_field = partial_name_mapping_root_field();

    FieldDescriptor parquet_field;
    for (const auto& name : {"a", "b"}) {
        FieldSchema file_field;
        file_field.name = name;
        file_field.field_id = -1;
        file_field.data_type =
                DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
        parquet_field._fields.emplace_back(std::move(file_field));
    }

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_field_id_with_name_mapping(
                        root_field, parquet_field, ans_node)
                        .ok());
    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  a (file: a)\n"
              "    ScalarNode\n"
              "  b (not exists)\n");
}

TEST(MockTableSchemaChangeHelper, IcebergParquetMixedFieldIdsPreferExistingIds) {
    auto root_field = partial_name_mapping_root_field();
    root_field.fields[0].field_ptr->__set_name_mapping({});

    FieldDescriptor parquet_field;
    for (const auto& [name, field_id] :
         std::vector<std::pair<std::string, int32_t>> {{"a", 1}, {"b", -1}}) {
        FieldSchema file_field;
        file_field.name = name;
        file_field.field_id = field_id;
        file_field.data_type =
                DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
        parquet_field._fields.emplace_back(std::move(file_field));
    }

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_field_id_with_name_mapping(
                        root_field, parquet_field, ans_node, true)
                        .ok());
    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  a (file: a)\n"
              "    ScalarNode\n"
              "  b (not exists)\n");
}

TEST(MockTableSchemaChangeHelper, IcebergParquetLegacyPlanFallsBackForMixedFieldIds) {
    auto root_field = partial_name_mapping_root_field();
    for (auto& field : root_field.fields) {
        field.field_ptr->__isset.name_mapping_is_authoritative = false;
    }

    FieldDescriptor parquet_field;
    for (const auto& [name, field_id] :
         std::vector<std::pair<std::string, int32_t>> {{"a", 1}, {"b", -1}}) {
        FieldSchema file_field;
        file_field.name = name;
        file_field.field_id = field_id;
        file_field.data_type =
                DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
        parquet_field._fields.emplace_back(std::move(file_field));
    }

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_field_id_with_name_mapping(
                        root_field, parquet_field, ans_node)
                        .ok());
    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  a (file: a)\n"
              "    ScalarNode\n"
              "  b (file: b)\n"
              "    ScalarNode\n");
}

TEST(MockTableSchemaChangeHelper, IcebergParquetNestedMixedFieldIdsPreferExistingIds) {
    auto root_field = nested_partial_name_mapping_root_field();
    root_field.fields[0]
            .field_ptr->nestedField.struct_field.fields[1]
            .field_ptr->__set_initial_default_value("AAEC/w==");
    root_field.fields[0]
            .field_ptr->nestedField.struct_field.fields[1]
            .field_ptr->__set_initial_default_value_is_base64(true);

    FieldSchema struct_field;
    struct_field.name = "s";
    struct_field.field_id = 10;
    std::vector<DataTypePtr> child_types;
    Strings child_names;
    for (const auto& [name, field_id] :
         std::vector<std::pair<std::string, int32_t>> {{"a", 1}, {"b", -1}}) {
        FieldSchema child;
        child.name = name;
        child.field_id = field_id;
        child.data_type =
                DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
        child_types.emplace_back(child.data_type);
        child_names.emplace_back(child.name);
        struct_field.children.emplace_back(std::move(child));
    }
    struct_field.data_type = std::make_shared<DataTypeStruct>(child_types, child_names);

    FieldDescriptor parquet_field;
    parquet_field._fields.emplace_back(std::move(struct_field));
    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_field_id_with_name_mapping(
                        root_field, parquet_field, ans_node, true)
                        .ok());
    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  s (file: s)\n"
              "    StructNode\n"
              "      a (file: a)\n"
              "        ScalarNode\n"
              "      b (not exists)\n");
    const auto nested_node = ans_node->get_children_node("s");
    const auto default_value = nested_node->children_initial_default_value("b");
    ASSERT_TRUE(default_value.has_value());
    EXPECT_EQ(default_value->value, "AAEC/w==");
    EXPECT_TRUE(default_value->is_base64);
}

TEST(MockTableSchemaChangeHelper, IcebergParquetLegacyPlanFallsBackForNestedMixedFieldIds) {
    auto root_field = nested_partial_name_mapping_root_field();
    root_field.fields[0].field_ptr->__isset.name_mapping_is_authoritative = false;
    for (auto& child : root_field.fields[0].field_ptr->nestedField.struct_field.fields) {
        child.field_ptr->__isset.name_mapping_is_authoritative = false;
    }

    FieldSchema struct_field;
    struct_field.name = "s";
    struct_field.field_id = 10;
    std::vector<DataTypePtr> child_types;
    Strings child_names;
    for (const auto& [name, field_id] :
         std::vector<std::pair<std::string, int32_t>> {{"a", 1}, {"b", -1}}) {
        FieldSchema child;
        child.name = name;
        child.field_id = field_id;
        child.data_type =
                DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
        child_types.emplace_back(child.data_type);
        child_names.emplace_back(child.name);
        struct_field.children.emplace_back(std::move(child));
    }
    struct_field.data_type = std::make_shared<DataTypeStruct>(child_types, child_names);

    FieldDescriptor parquet_field;
    parquet_field._fields.emplace_back(std::move(struct_field));
    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_field_id_with_name_mapping(
                        root_field, parquet_field, ans_node)
                        .ok());
    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  s (file: s)\n"
              "    StructNode\n"
              "      a (file: a)\n"
              "        ScalarNode\n"
              "      b (file: b)\n"
              "        ScalarNode\n");
}

TEST(MockTableSchemaChangeHelper,
     IcebergParquetDescendantIdRetainsWrapperWithAuthoritativeEmptyMapping) {
    TColumnType int_type;
    int_type.__set_type(TPrimitiveType::INT);
    TColumnType struct_type;
    struct_type.__set_type(TPrimitiveType::STRUCT);

    auto id_field = std::make_shared<schema::external::TField>();
    id_field->__set_name("id");
    id_field->__set_id(1);
    id_field->__set_type(int_type);

    auto child_field = std::make_shared<schema::external::TField>();
    child_field->__set_name("a");
    child_field->__set_id(2);
    child_field->__set_type(int_type);
    schema::external::TFieldPtr child_ptr;
    child_ptr.__set_field_ptr(child_field);
    schema::external::TStructField struct_fields;
    struct_fields.__set_fields({child_ptr});

    auto struct_field = std::make_shared<schema::external::TField>();
    struct_field->__set_name("s");
    struct_field->__set_id(10);
    struct_field->__set_type(struct_type);
    struct_field->__set_name_mapping({});
    struct_field->__set_name_mapping_is_authoritative(true);
    struct_field->nestedField.__set_struct_field(struct_fields);
    struct_field->__isset.nestedField = true;

    schema::external::TFieldPtr id_ptr;
    id_ptr.__set_field_ptr(id_field);
    schema::external::TFieldPtr struct_ptr;
    struct_ptr.__set_field_ptr(struct_field);
    schema::external::TStructField table_root;
    table_root.__set_fields({id_ptr, struct_ptr});

    FieldSchema file_id;
    file_id.name = "id";
    file_id.field_id = 1;
    file_id.data_type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    FieldSchema file_child;
    file_child.name = "a";
    file_child.field_id = 2;
    file_child.data_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    FieldSchema file_struct;
    file_struct.name = "s";
    file_struct.field_id = -1;
    file_struct.children = {file_child};
    file_struct.data_type = std::make_shared<DataTypeStruct>(DataTypes {file_child.data_type},
                                                             Strings {file_child.name});
    FieldDescriptor parquet_field;
    parquet_field._fields = {file_id, file_struct};

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_field_id_with_name_mapping(
                        table_root, parquet_field, ans_node, true)
                        .ok());
    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  id (file: id)\n"
              "    ScalarNode\n"
              "  s (file: s)\n"
              "    StructNode\n"
              "      a (file: a)\n"
              "        ScalarNode\n");
}

TEST(MockTableSchemaChangeHelper, IcebergParquetKeepsWholeFileIdModeInNestedStruct) {
    TColumnType int_type;
    int_type.__set_type(TPrimitiveType::INT);
    TColumnType struct_type;
    struct_type.__set_type(TPrimitiveType::STRUCT);

    auto id_field = std::make_shared<schema::external::TField>();
    id_field->__set_name("id");
    id_field->__set_id(1);
    id_field->__set_type(int_type);
    auto child_field = std::make_shared<schema::external::TField>();
    child_field->__set_name("a");
    child_field->__set_id(2);
    child_field->__set_type(int_type);
    child_field->__set_name_mapping({"legacy_a"});
    schema::external::TFieldPtr child_ptr;
    child_ptr.__set_field_ptr(child_field);
    schema::external::TStructField struct_children;
    struct_children.__set_fields({child_ptr});
    auto struct_field = std::make_shared<schema::external::TField>();
    struct_field->__set_name("s");
    struct_field->__set_id(10);
    struct_field->__set_type(struct_type);
    struct_field->nestedField.__set_struct_field(struct_children);
    struct_field->__isset.nestedField = true;

    schema::external::TFieldPtr id_ptr;
    id_ptr.__set_field_ptr(id_field);
    schema::external::TFieldPtr struct_ptr;
    struct_ptr.__set_field_ptr(struct_field);
    schema::external::TStructField table_root;
    table_root.__set_fields({id_ptr, struct_ptr});

    FieldSchema file_id;
    file_id.name = "id";
    file_id.field_id = 1;
    file_id.data_type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    FieldSchema file_child;
    file_child.name = "legacy_a";
    file_child.field_id = -1;
    file_child.data_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    FieldSchema file_struct;
    file_struct.name = "s";
    file_struct.field_id = 10;
    file_struct.children = {file_child};
    file_struct.data_type = std::make_shared<DataTypeStruct>(DataTypes {file_child.data_type},
                                                             Strings {file_child.name});
    FieldDescriptor parquet_field;
    parquet_field._fields = {file_id, file_struct};

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_field_id_with_name_mapping(
                        table_root, parquet_field, ans_node, true)
                        .ok());
    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  id (file: id)\n"
              "    ScalarNode\n"
              "  s (file: s)\n"
              "    StructNode\n"
              "      a (not exists)\n");
}

TEST(MockTableSchemaChangeHelper, IcebergOrcSchemaChange) {
    schema::external::TField test_field;
    TColumnType struct_type;
    struct_type.type = TPrimitiveType::STRUCT;
    test_field.type = struct_type;
    schema::external::TStructField root_field;
    {
        TColumnType int_type;
        int_type.type = TPrimitiveType::INT;

        {
            auto col1_field = std::make_shared<schema::external::TField>();
            col1_field->name = "col1";
            col1_field->id = 1;
            col1_field->type = int_type;
            schema::external::TFieldPtr col1_ptr;
            col1_ptr.field_ptr = col1_field;
            root_field.fields.emplace_back(col1_ptr);
        }

        {
            auto col2_field = std::make_shared<schema::external::TField>();
            col2_field->name = "col2";
            col2_field->id = 2;
            col2_field->type = struct_type;

            schema::external::TStructField struct_field;
            {
                auto a_field = std::make_shared<schema::external::TField>();
                a_field->name = "a";
                a_field->id = 3;
                a_field->type = int_type;
                schema::external::TFieldPtr a_ptr;
                a_ptr.field_ptr = a_field;
                struct_field.fields.emplace_back(a_ptr);
            }

            {
                auto b_field = std::make_shared<schema::external::TField>();
                b_field->name = "b";
                b_field->id = 4;
                b_field->type = int_type;
                schema::external::TFieldPtr b_ptr;
                b_ptr.field_ptr = b_field;
                struct_field.fields.emplace_back(b_ptr);
            }

            col2_field->nestedField.struct_field = struct_field;
            schema::external::TFieldPtr col2_ptr;
            col2_ptr.field_ptr = col2_field;
            root_field.fields.emplace_back(col2_ptr);
        }
    }
    test_field.nestedField.struct_field = root_field;
    std::unique_ptr<orc::Type> orc_type(orc::Type::buildTypeFromString(
            "struct<col1:int,col1122:struct<a:int,aa:int>,COL369:int>"));
    const auto& attribute = IcebergOrcReader::ICEBERG_ORC_ATTRIBUTE;
    orc_type->getSubtype(0)->setAttribute(attribute, "1");
    orc_type->getSubtype(1)->setAttribute(attribute, "2");
    orc_type->getSubtype(1)->getSubtype(0)->setAttribute(attribute, "3");
    orc_type->getSubtype(1)->getSubtype(1)->setAttribute(attribute, "4");
    orc_type->getSubtype(2)->setAttribute(attribute, "5");

    bool exist_field_id = true;
    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_field_id(
                        test_field, orc_type.get(), attribute, ans_node, exist_field_id)
                        .ok());
    ASSERT_TRUE(exist_field_id);

    std::cout << TableSchemaChangeHelper::debug(ans_node) << "\n";
    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  col1 (file: col1)\n"
              "    ScalarNode\n"
              "  col2 (file: col1122)\n"
              "    StructNode\n"
              "      a (file: a)\n"
              "        ScalarNode\n"
              "      b (file: aa)\n"
              "        ScalarNode\n");
}

TEST(MockTableSchemaChangeHelper, IcebergOrcNameMappingFallback) {
    TColumnType int_type;
    int_type.type = TPrimitiveType::INT;

    schema::external::TStructField root_field;
    auto renamed_field = std::make_shared<schema::external::TField>();
    renamed_field->name = "col1_new";
    renamed_field->id = 1;
    renamed_field->type = int_type;
    renamed_field->__set_name_mapping(std::vector<std::string> {"col1_old"});
    schema::external::TFieldPtr renamed_ptr;
    renamed_ptr.field_ptr = renamed_field;
    root_field.fields.emplace_back(renamed_ptr);

    std::unique_ptr<orc::Type> orc_type(orc::Type::buildTypeFromString("struct<col1_old:int>"));

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
    ASSERT_TRUE(
            TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_field_id_with_name_mapping(
                    root_field, orc_type.get(), IcebergOrcReader::ICEBERG_ORC_ATTRIBUTE, ans_node)
                    .ok());

    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  col1_new (file: col1_old)\n"
              "    ScalarNode\n");
}

TEST(MockTableSchemaChangeHelper, IcebergOrcPartialNameMappingIsStrict) {
    auto root_field = partial_name_mapping_root_field();

    std::unique_ptr<orc::Type> orc_type(orc::Type::buildTypeFromString("struct<a:int,b:int>"));
    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node;
    ASSERT_TRUE(
            TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_field_id_with_name_mapping(
                    root_field, orc_type.get(), IcebergOrcReader::ICEBERG_ORC_ATTRIBUTE, ans_node)
                    .ok());
    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  a (file: a)\n"
              "    ScalarNode\n"
              "  b (not exists)\n");
}

TEST(MockTableSchemaChangeHelper, IcebergOrcMixedFieldIdsPreferExistingIds) {
    auto root_field = partial_name_mapping_root_field();
    root_field.fields[0].field_ptr->__set_name_mapping({});

    std::unique_ptr<orc::Type> orc_type(orc::Type::buildTypeFromString("struct<a:int,b:int>"));
    orc_type->getSubtype(0)->setAttribute(IcebergOrcReader::ICEBERG_ORC_ATTRIBUTE, "1");

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_field_id_with_name_mapping(
                        root_field, orc_type.get(), IcebergOrcReader::ICEBERG_ORC_ATTRIBUTE,
                        ans_node, true)
                        .ok());
    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  a (file: a)\n"
              "    ScalarNode\n"
              "  b (not exists)\n");
}

TEST(MockTableSchemaChangeHelper, IcebergOrcNestedMixedFieldIdsPreferExistingIds) {
    auto root_field = nested_partial_name_mapping_root_field();
    root_field.fields[0]
            .field_ptr->nestedField.struct_field.fields[1]
            .field_ptr->__set_initial_default_value("AAEC/w==");
    root_field.fields[0]
            .field_ptr->nestedField.struct_field.fields[1]
            .field_ptr->__set_initial_default_value_is_base64(true);

    std::unique_ptr<orc::Type> orc_type(
            orc::Type::buildTypeFromString("struct<s:struct<a:int,b:int>>"));
    const auto& attribute = IcebergOrcReader::ICEBERG_ORC_ATTRIBUTE;
    orc_type->getSubtype(0)->setAttribute(attribute, "10");
    orc_type->getSubtype(0)->getSubtype(0)->setAttribute(attribute, "1");

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_field_id_with_name_mapping(
                        root_field, orc_type.get(), attribute, ans_node, true)
                        .ok());
    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  s (file: s)\n"
              "    StructNode\n"
              "      a (file: a)\n"
              "        ScalarNode\n"
              "      b (not exists)\n");
    const auto nested_node = ans_node->get_children_node("s");
    const auto default_value = nested_node->children_initial_default_value("b");
    ASSERT_TRUE(default_value.has_value());
    EXPECT_EQ(default_value->value, "AAEC/w==");
    EXPECT_TRUE(default_value->is_base64);
}

TEST(MockTableSchemaChangeHelper, IcebergOrcDoesNotBindIdlessWrapperByName) {
    auto root_field = nested_partial_name_mapping_root_field();
    std::unique_ptr<orc::Type> orc_type(orc::Type::buildTypeFromString("struct<s:struct<a:int>>"));
    const auto& attribute = IcebergOrcReader::ICEBERG_ORC_ATTRIBUTE;
    orc_type->getSubtype(0)->getSubtype(0)->setAttribute(attribute, "1");

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_field_id_with_name_mapping(
                        root_field, orc_type.get(), attribute, ans_node, true)
                        .ok());
    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  s (not exists)\n");
}

TEST(MockTableSchemaChangeHelper, NestedMapArrayStruct) {
    // struct<col1:map<array<int>, struct<a:int, b:int>>>
    SlotDescriptor slot1;
    slot1._type = std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeArray>(
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true)),
            std::make_shared<DataTypeStruct>(
                    std::vector<DataTypePtr> {DataTypeFactory::instance().create_data_type(
                                                      PrimitiveType::TYPE_INT, true),
                                              DataTypeFactory::instance().create_data_type(
                                                      PrimitiveType::TYPE_INT, true)},
                    Strings {"a", "b"}));
    slot1._col_name = "col1";

    TupleDescriptor tuple_desc;
    tuple_desc.add_slot(&slot1);

    std::unique_ptr<orc::Type> orc_type(
            orc::Type::buildTypeFromString("struct<COl1:map<array<int>,struct<A:int,B:int>>>"));

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_name(&tuple_desc,
                                                                         orc_type.get(), ans_node)
                        .ok());

    std::cout << TableSchemaChangeHelper::debug(ans_node) << "\n";

    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  col1 (file: COl1)\n"
              "    MapNode\n"
              "      Key:\n"
              "        ArrayNode\n"
              "          Element:\n"
              "            ScalarNode\n"
              "      Value:\n"
              "        StructNode\n"
              "          a (file: A)\n"
              "            ScalarNode\n"
              "          b (file: B)\n"
              "            ScalarNode\n");
}

TEST(MockTableSchemaChangeHelper, NestedArrayStruct) {
    //  struct<col1:array<struct<a:int, b:array<int>>>>
    SlotDescriptor slot1;
    slot1._type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeStruct>(
            std::vector<DataTypePtr> {
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true),
                    std::make_shared<DataTypeArray>(DataTypeFactory::instance().create_data_type(
                            PrimitiveType::TYPE_INT, true))},
            Strings {"a", "b"}));
    slot1._col_name = "col1";

    TupleDescriptor tuple_desc;
    tuple_desc.add_slot(&slot1);

    std::unique_ptr<orc::Type> orc_type(
            orc::Type::buildTypeFromString("struct<coL1:array<struct<a:int,B:array<int>>>>"));

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_name(&tuple_desc,
                                                                         orc_type.get(), ans_node)
                        .ok());

    std::cout << TableSchemaChangeHelper::debug(ans_node) << "\n";

    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  col1 (file: coL1)\n"
              "    ArrayNode\n"
              "      Element:\n"
              "        StructNode\n"
              "          a (file: a)\n"
              "            ScalarNode\n"
              "          b (file: B)\n"
              "            ArrayNode\n"
              "              Element:\n"
              "                ScalarNode\n");
}

TEST(MockTableSchemaChangeHelper, NestedMapStruct) {
    //  struct<col1:map<int, struct<a:int, b:map<int, int>>>>
    SlotDescriptor slot1;
    slot1._type = std::make_shared<DataTypeMap>(
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true),
            std::make_shared<DataTypeStruct>(
                    std::vector<DataTypePtr> {DataTypeFactory::instance().create_data_type(
                                                      PrimitiveType::TYPE_INT, true),
                                              std::make_shared<DataTypeMap>(
                                                      DataTypeFactory::instance().create_data_type(
                                                              PrimitiveType::TYPE_INT, true),
                                                      DataTypeFactory::instance().create_data_type(
                                                              PrimitiveType::TYPE_INT, true))},
                    Strings {"a", "b"}));
    slot1._col_name = "col1";

    TupleDescriptor tuple_desc;
    tuple_desc.add_slot(&slot1);

    std::unique_ptr<orc::Type> orc_type(
            orc::Type::buildTypeFromString("struct<col1:map<int,struct<AA:int,b:map<int,int>>>>"));

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_name(&tuple_desc,
                                                                         orc_type.get(), ans_node)
                        .ok());

    std::cout << TableSchemaChangeHelper::debug(ans_node) << "\n";

    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  col1 (file: col1)\n"
              "    MapNode\n"
              "      Key:\n"
              "        ScalarNode\n"
              "      Value:\n"
              "        StructNode\n"
              "          a (not exists)\n"
              "          b (file: b)\n"
              "            MapNode\n"
              "              Key:\n"
              "                ScalarNode\n"
              "              Value:\n"
              "                ScalarNode\n");
}

TEST(MockTableSchemaChangeHelper, ParquetNestedArrayStruct) {
    //  struct<col1:array<struct<a:int, b:array<int>>>>
    SlotDescriptor slot1;
    slot1._type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeStruct>(
            std::vector<DataTypePtr> {
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true),
                    std::make_shared<DataTypeArray>(DataTypeFactory::instance().create_data_type(
                            PrimitiveType::TYPE_INT, true))},
            Strings {"a", "b"}));
    slot1._col_name = "col1";

    TupleDescriptor tuple_desc;
    tuple_desc.add_slot(&slot1);

    FieldDescriptor parquet_field;
    {
        FieldSchema col1_field;
        col1_field.name = "col1";

        {
            FieldSchema col1_element;
            col1_element.data_type = std::make_shared<DataTypeStruct>(
                    std::vector<DataTypePtr> {DataTypeFactory::instance().create_data_type(
                                                      PrimitiveType::TYPE_INT, true),
                                              std::make_shared<DataTypeArray>(
                                                      DataTypeFactory::instance().create_data_type(
                                                              PrimitiveType::TYPE_INT, true))},
                    Strings {"a", "B"});
            {
                FieldSchema a_field;
                a_field.name = "a";
                a_field.data_type =
                        DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
                col1_element.children.emplace_back(a_field);
            }

            {
                FieldSchema b_field;
                b_field.name = "B";
                b_field.data_type = std::make_shared<DataTypeArray>(
                        DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT,
                                                                     true));
                {
                    FieldSchema b_element_field;
                    b_element_field.data_type = DataTypeFactory::instance().create_data_type(
                            PrimitiveType::TYPE_INT, true);

                    b_field.children.emplace_back(b_element_field);
                }
                col1_element.children.emplace_back(b_field);
            }

            col1_field.children.emplace_back(col1_element);
        }

        col1_field.data_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeStruct>(
                std::vector<DataTypePtr> {
                        DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true),
                        std::make_shared<DataTypeArray>(
                                DataTypeFactory::instance().create_data_type(
                                        PrimitiveType::TYPE_INT, true))},
                Strings {"a", "B"}));
        parquet_field._fields.emplace_back(col1_field);
    }

    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_name(
                        &tuple_desc, parquet_field, ans_node)
                        .ok());

    std::cout << TableSchemaChangeHelper::debug(ans_node) << "\n";

    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  col1 (file: col1)\n"
              "    ArrayNode\n"
              "      Element:\n"
              "        StructNode\n"
              "          a (file: a)\n"
              "            ScalarNode\n"
              "          b (file: B)\n"
              "            ArrayNode\n"
              "              Element:\n"
              "                ScalarNode\n");
}

TEST(MockTableSchemaChangeHelper, TableFieldIdNestedArrayStruct) {
    //  struct<col1:array<struct<a:int, b:array<int>>>>
    schema::external::TStructField table_schema;
    {
        auto col1_field = std::make_shared<schema::external::TField>();
        col1_field->name = "col1";
        col1_field->id = 1;
        col1_field->type.type = TPrimitiveType::ARRAY;

        auto item_field = std::make_shared<schema::external::TField>();
        item_field->type.type = TPrimitiveType::STRUCT;
        schema::external::TStructField struct_field;
        {
            auto a_field = std::make_shared<schema::external::TField>();
            a_field->name = "a";
            a_field->id = 2;
            a_field->type.type = TPrimitiveType::INT;
            schema::external::TFieldPtr a_ptr;
            a_ptr.field_ptr = a_field;
            struct_field.fields.emplace_back(a_ptr);
        }
        {
            auto b_field = std::make_shared<schema::external::TField>();
            b_field->name = "b";
            b_field->id = 3;
            b_field->type.type = TPrimitiveType::ARRAY;

            {
                auto b_element_filed = std::make_shared<schema::external::TField>();
                b_field->nestedField.array_field.item_field.field_ptr = b_element_filed;
            }
            schema::external::TFieldPtr b_ptr;
            b_ptr.field_ptr = b_field;
            struct_field.fields.emplace_back(b_ptr);
        }
        item_field->nestedField.struct_field = struct_field;
        col1_field->nestedField.array_field.item_field.field_ptr = item_field;
        schema::external::TFieldPtr col1_ptr;
        col1_ptr.field_ptr = col1_field;
        table_schema.fields.emplace_back(col1_ptr);
    }

    schema::external::TStructField file_schema = table_schema;
    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_table_field_id(
                        table_schema, file_schema, ans_node)
                        .ok());

    std::cout << TableSchemaChangeHelper::debug(ans_node) << "\n";

    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  col1 (file: col1)\n"
              "    ArrayNode\n"
              "      Element:\n"
              "        StructNode\n"
              "          a (file: a)\n"
              "            ScalarNode\n"
              "          b (file: b)\n"
              "            ArrayNode\n"
              "              Element:\n"
              "                ScalarNode\n");
}

TEST(MockTableSchemaChangeHelper, OrcFieldIdNestedStructMap) {
    //  struct<col1:struct<a:map<int, int>, b:struct<c:int, d:map<int, int>>>>
    schema::external::TField test_field;
    TColumnType struct_type;
    struct_type.type = TPrimitiveType::STRUCT;
    test_field.type = struct_type;
    schema::external::TStructField table_schema;
    {
        auto col1_field = std::make_shared<schema::external::TField>();
        col1_field->name = "col111111";
        col1_field->id = 1;
        col1_field->type.type = TPrimitiveType::STRUCT;
        schema::external::TStructField struct_field;
        {
            auto a_field = std::make_shared<schema::external::TField>();
            a_field->name = "xxxxxx";
            a_field->id = 2;
            a_field->type.type = TPrimitiveType::MAP;
            {
                schema::external::TMapField map_field;

                auto key_field = std::make_shared<schema::external::TField>();
                key_field->type.type = TPrimitiveType::INT;
                auto value_field = std::make_shared<schema::external::TField>();
                value_field->type.type = TPrimitiveType::INT;
                map_field.key_field.field_ptr = key_field;
                map_field.value_field.field_ptr = value_field;
                a_field->nestedField.map_field = map_field;
            }
            schema::external::TFieldPtr a_ptr;
            a_ptr.field_ptr = a_field;
            struct_field.fields.emplace_back(a_ptr);
        }
        {
            auto b_field = std::make_shared<schema::external::TField>();
            b_field->name = "AAAAAAA";
            b_field->id = 3;
            b_field->type.type = TPrimitiveType::STRUCT;

            schema::external::TStructField nested_struct_field;
            {
                auto c_field = std::make_shared<schema::external::TField>();
                c_field->name = "d";
                c_field->id = 4;
                c_field->type.type = TPrimitiveType::INT;
                schema::external::TFieldPtr c_ptr;
                c_ptr.field_ptr = c_field;
                nested_struct_field.fields.emplace_back(c_ptr);
            }
            {
                auto d_field = std::make_shared<schema::external::TField>();
                d_field->name = "CCCCCCCCC";
                d_field->id = 5;
                d_field->type.type = TPrimitiveType::MAP;
                {
                    schema::external::TMapField map_field;

                    auto key_field = std::make_shared<schema::external::TField>();
                    key_field->type.type = TPrimitiveType::INT;
                    auto value_field = std::make_shared<schema::external::TField>();
                    value_field->type.type = TPrimitiveType::INT;
                    map_field.key_field.field_ptr = key_field;
                    map_field.value_field.field_ptr = value_field;
                    d_field->nestedField.map_field = map_field;
                }
                schema::external::TFieldPtr d_ptr;
                d_ptr.field_ptr = d_field;
                nested_struct_field.fields.emplace_back(d_ptr);
            }
            b_field->nestedField.struct_field = nested_struct_field;
            schema::external::TFieldPtr b_ptr;
            b_ptr.field_ptr = b_field;
            struct_field.fields.emplace_back(b_ptr);
        }
        col1_field->nestedField.struct_field = struct_field;
        schema::external::TFieldPtr col1_ptr;
        col1_ptr.field_ptr = col1_field;
        table_schema.fields.emplace_back(col1_ptr);
    }
    test_field.nestedField.struct_field = table_schema;
    std::unique_ptr<orc::Type> orc_type(orc::Type::buildTypeFromString(
            "struct<col1:struct<a:map<int,int>,b:struct<c:int,d:map<int,int>>>>"));
    const auto& attribute = IcebergOrcReader::ICEBERG_ORC_ATTRIBUTE;
    orc_type->getSubtype(0)->setAttribute(attribute, "1");
    orc_type->getSubtype(0)->getSubtype(0)->setAttribute(attribute, "2");
    orc_type->getSubtype(0)->getSubtype(1)->setAttribute(attribute, "3");
    orc_type->getSubtype(0)->getSubtype(1)->getSubtype(0)->setAttribute(attribute, "4");
    orc_type->getSubtype(0)->getSubtype(1)->getSubtype(1)->setAttribute(attribute, "5");

    bool exist_field_id = true;
    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_field_id(
                        test_field, orc_type.get(), attribute, ans_node, exist_field_id)
                        .ok());

    ASSERT_TRUE(exist_field_id);
    std::cout << TableSchemaChangeHelper::debug(ans_node) << "\n";

    ASSERT_EQ(TableSchemaChangeHelper::debug(ans_node),
              "StructNode\n"
              "  col111111 (file: col1)\n"
              "    StructNode\n"
              "      AAAAAAA (file: b)\n"
              "        StructNode\n"
              "          CCCCCCCCC (file: d)\n"
              "            MapNode\n"
              "              Key:\n"
              "                ScalarNode\n"
              "              Value:\n"
              "                ScalarNode\n"
              "          d (file: c)\n"
              "            ScalarNode\n"
              "      xxxxxx (file: a)\n"
              "        MapNode\n"
              "          Key:\n"
              "            ScalarNode\n"
              "          Value:\n"
              "            ScalarNode\n");
}
} // namespace doris
