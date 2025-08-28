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

#include <string>
#include <unordered_map>
#include <vector>

#include "testutil/desc_tbl_builder.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/table/iceberg_reader.h"
#include "vec/exec/format/table/table_format_reader.h"

namespace doris::vectorized {
class MockTableSchemaChangeHelper : public TableSchemaChangeHelper {};

TEST(MockTableSchemaChangeHelper, OrcNameNoSchemaChange) {
    std::vector<DataTypePtr> data_types;
    std::vector<std::string> column_names;

    SlotDescriptor slot1;
    slot1._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, true);
    slot1._col_name = "col1";

    SlotDescriptor slot2;
    slot2._type = std::make_shared<DataTypeStruct>(
            std::vector<DataTypePtr> {vectorized::DataTypeFactory::instance().create_data_type(
                                              PrimitiveType::TYPE_BIGINT, true),
                                      vectorized::DataTypeFactory::instance().create_data_type(
                                              PrimitiveType::TYPE_BIGINT, true)},
            Strings {"a", "b"});
    slot2._col_name = "col2";

    SlotDescriptor slot3;
    slot3._type =
            vectorized::DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
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

TEST(MockTableSchemaChangeHelper, OrcNameSchemaChange1) {
    std::vector<DataTypePtr> data_types;
    std::vector<std::string> column_names;

    SlotDescriptor slot1;
    slot1._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, true);
    slot1._col_name = "col1";

    SlotDescriptor slot2;
    slot2._type = std::make_shared<DataTypeStruct>(
            std::vector<DataTypePtr> {vectorized::DataTypeFactory::instance().create_data_type(
                                              PrimitiveType::TYPE_BIGINT, true),
                                      vectorized::DataTypeFactory::instance().create_data_type(
                                              PrimitiveType::TYPE_BIGINT, true)},
            Strings {"a", "b"});
    slot2._col_name = "col2";

    SlotDescriptor slot3;
    slot3._type =
            vectorized::DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
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
    slot1._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, true);
    slot1._col_name = "col1";

    SlotDescriptor slot2;
    slot2._type = std::make_shared<DataTypeStruct>(
            std::vector<DataTypePtr> {vectorized::DataTypeFactory::instance().create_data_type(
                                              PrimitiveType::TYPE_BIGINT, true),
                                      vectorized::DataTypeFactory::instance().create_data_type(
                                              PrimitiveType::TYPE_BIGINT, true)},
            Strings {"a", "b"});
    slot2._col_name = "col2";

    SlotDescriptor slot3;
    slot3._type =
            vectorized::DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    slot3._col_name = "col3";

    TupleDescriptor tuple_desc;
    tuple_desc.add_slot(&slot1);
    tuple_desc.add_slot(&slot2);
    tuple_desc.add_slot(&slot3);

    FieldDescriptor parquet_field;

    FieldSchema parquet_field_col1;
    {
        parquet_field_col1.name = "col1";
        parquet_field_col1.data_type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_BIGINT, true);
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
    schema::external::TStructField root_field;
    {
        TColumnType int_type;
        int_type.type = TPrimitiveType::INT;

        TColumnType struct_type;
        struct_type.type = TPrimitiveType::STRUCT;

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

    FieldDescriptor parquet_field;
    {
        {
            FieldSchema parquet_field_col1;
            parquet_field_col1.name = "col1";
            parquet_field_col1.data_type = vectorized::DataTypeFactory::instance().create_data_type(
                    PrimitiveType::TYPE_BIGINT, true);
            parquet_field_col1.field_id = 1;
            parquet_field._fields.emplace_back(parquet_field_col1);
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
                b_field.data_type = vectorized::DataTypeFactory::instance().create_data_type(
                        PrimitiveType::TYPE_BIGINT, true);
                sub_data_type.emplace_back(b_field.data_type);
                sub_names.emplace_back(b_field.name);
                parquet_field_col2.children.emplace_back(b_field);
            }
            {
                FieldSchema a_field;
                a_field.name = "a33333333";
                a_field.field_id = 3;
                a_field.data_type = vectorized::DataTypeFactory::instance().create_data_type(
                        PrimitiveType::TYPE_BIGINT, true);
                sub_data_type.emplace_back(a_field.data_type);
                sub_names.emplace_back(a_field.name);

                parquet_field_col2.children.emplace_back(a_field);
            }
            parquet_field_col2.data_type =
                    std::make_shared<DataTypeStruct>(sub_data_type, sub_names);
            parquet_field_col2.field_id = 2;

            parquet_field._fields.emplace_back(parquet_field_col2);
        }
    }
    bool exist_field_id = true;
    std::shared_ptr<TableSchemaChangeHelper::Node> ans_node = nullptr;
    ASSERT_TRUE(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_field_id(
                        root_field, parquet_field, ans_node, exist_field_id)
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

TEST(MockTableSchemaChangeHelper, IcebergOrcSchemaChange) {
    schema::external::TStructField root_field;
    {
        TColumnType int_type;
        int_type.type = TPrimitiveType::INT;

        TColumnType struct_type;
        struct_type.type = TPrimitiveType::STRUCT;

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
                        root_field, orc_type.get(), attribute, ans_node, exist_field_id)
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

TEST(MockTableSchemaChangeHelper, NestedMapArrayStruct) {
    // struct<col1:map<array<int>, struct<a:int, b:int>>>
    SlotDescriptor slot1;
    slot1._type = std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeArray>(
                    vectorized::DataTypeFactory::instance().create_data_type(
                            PrimitiveType::TYPE_INT, true)),
            std::make_shared<DataTypeStruct>(
                    std::vector<DataTypePtr> {
                            vectorized::DataTypeFactory::instance().create_data_type(
                                    PrimitiveType::TYPE_INT, true),
                            vectorized::DataTypeFactory::instance().create_data_type(
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
                    vectorized::DataTypeFactory::instance().create_data_type(
                            PrimitiveType::TYPE_INT, true),
                    std::make_shared<DataTypeArray>(
                            vectorized::DataTypeFactory::instance().create_data_type(
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
            vectorized::DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true),
            std::make_shared<DataTypeStruct>(
                    std::vector<DataTypePtr> {
                            vectorized::DataTypeFactory::instance().create_data_type(
                                    PrimitiveType::TYPE_INT, true),
                            std::make_shared<DataTypeMap>(
                                    vectorized::DataTypeFactory::instance().create_data_type(
                                            PrimitiveType::TYPE_INT, true),
                                    vectorized::DataTypeFactory::instance().create_data_type(
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
                    vectorized::DataTypeFactory::instance().create_data_type(
                            PrimitiveType::TYPE_INT, true),
                    std::make_shared<DataTypeArray>(
                            vectorized::DataTypeFactory::instance().create_data_type(
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
                    std::vector<DataTypePtr> {
                            vectorized::DataTypeFactory::instance().create_data_type(
                                    PrimitiveType::TYPE_INT, true),
                            std::make_shared<DataTypeArray>(
                                    vectorized::DataTypeFactory::instance().create_data_type(
                                            PrimitiveType::TYPE_INT, true))},
                    Strings {"a", "B"});
            {
                FieldSchema a_field;
                a_field.name = "a";
                a_field.data_type = vectorized::DataTypeFactory::instance().create_data_type(
                        PrimitiveType::TYPE_INT, true);
                col1_element.children.emplace_back(a_field);
            }

            {
                FieldSchema b_field;
                b_field.name = "B";
                b_field.data_type = std::make_shared<DataTypeArray>(
                        vectorized::DataTypeFactory::instance().create_data_type(
                                PrimitiveType::TYPE_INT, true));
                {
                    FieldSchema b_element_field;
                    b_element_field.data_type =
                            vectorized::DataTypeFactory::instance().create_data_type(
                                    PrimitiveType::TYPE_INT, true);

                    b_field.children.emplace_back(b_element_field);
                }
                col1_element.children.emplace_back(b_field);
            }

            col1_field.children.emplace_back(col1_element);
        }

        col1_field.data_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeStruct>(
                std::vector<DataTypePtr> {
                        vectorized::DataTypeFactory::instance().create_data_type(
                                PrimitiveType::TYPE_INT, true),
                        std::make_shared<DataTypeArray>(
                                vectorized::DataTypeFactory::instance().create_data_type(
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
                        table_schema, orc_type.get(), attribute, ans_node, exist_field_id)
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
} // namespace doris::vectorized
