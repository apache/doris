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

#include "vec/exec/format/table/iceberg/arrow_schema_util.h"

#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <gtest/gtest.h>

#include "vec/exec/format/table/iceberg/schema.h"
#include "vec/exec/format/table/iceberg/schema_parser.h"

namespace doris {
namespace iceberg {

class ArrowSchemaUtilTest : public testing::Test {
public:
    ArrowSchemaUtilTest() = default;
    virtual ~ArrowSchemaUtilTest() = default;
};

const std::string_view pfid = "PARQUET:field_id";

TEST(ArrowSchemaUtilTest, test_simple_field) {
    std::vector<NestedField> nested_fields;
    nested_fields.reserve(2);
    NestedField field1(false, 1, "field1", std::make_unique<IntegerType>(), std::nullopt);
    NestedField field2(false, 2, "field2", std::make_unique<StringType>(), std::nullopt);
    nested_fields.emplace_back(std::move(field1));
    nested_fields.emplace_back(std::move(field2));

    Schema schema(1, std::move(nested_fields));

    std::vector<std::shared_ptr<arrow::Field>> fields;
    Status st;
    st = ArrowSchemaUtil::Convert(&schema, "utc", fields);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(2, fields.size());
    EXPECT_EQ("field1", fields[0]->name());
    EXPECT_EQ("field2", fields[1]->name());
    EXPECT_TRUE(fields[0]->HasMetadata());
    EXPECT_TRUE(fields[1]->HasMetadata());
    EXPECT_EQ("1", fields[0]->metadata()->Get(pfid).ValueUnsafe());
    EXPECT_EQ("2", fields[1]->metadata()->Get(pfid).ValueUnsafe());
}

TEST(ArrowSchemaUtilTest, test_stuct_field) {
    // struct_json comes from :
    //     Schema schema = new Schema(
    //     Types.NestedField.optional(
    //         21, "st_col", Types.StructType.of(
    //             Types.NestedField.optional(32, "st_col_c1", Types.IntegerType.get()),
    //             Types.NestedField.optional(43, "st_col_c2", Types.StringType.get())
    //         )
    //     )
    // );
    // StringWriter writer = new StringWriter();
    // JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
    // SchemaParser.toJson(schema.asStruct(), generator);
    // generator.flush();
    // System.out.println(writer.toString());

    const std::string struct_json = R"({
        "type": "struct",
        "fields": [
            {
                "id": 21,
                "name": "st_col",
                "required": false,
                "type": {
                    "type": "struct",
                    "fields": [
                        {
                            "id": 32,
                            "name": "st_col_c1",
                            "required": false,
                            "type": "int"
                        },
                        {
                            "id": 43,
                            "name": "st_col_c2",
                            "required": false,
                            "type": "string"
                        }
                    ]
                }
            }
        ]
    })";
    std::unique_ptr<Schema> schema = SchemaParser::from_json(struct_json);

    std::vector<std::shared_ptr<arrow::Field>> fields;
    Status st;
    st = ArrowSchemaUtil::Convert(schema.get(), "utc", fields);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(1, fields.size());
    EXPECT_EQ("st_col", fields[0]->name());
    EXPECT_EQ("21", fields[0]->metadata()->Get(pfid).ValueUnsafe());

    arrow::StructType* arrow_struct = dynamic_cast<arrow::StructType*>(fields[0]->type().get());
    auto map_fields = arrow_struct->fields();
    EXPECT_EQ(2, arrow_struct->fields().size());
    EXPECT_EQ("st_col_c1", map_fields.at(0).get()->name());
    EXPECT_EQ("st_col_c2", map_fields.at(1).get()->name());
    EXPECT_EQ("32", map_fields.at(0).get()->metadata()->Get(pfid).ValueUnsafe());
    EXPECT_EQ("43", map_fields.at(1).get()->metadata()->Get(pfid).ValueUnsafe());
}

TEST(ArrowSchemaUtilTest, test_map_field) {
    // map_json comes from :
    // Schema schema = new Schema(
    //     Types.NestedField.optional(
    //         21, "map_col", Types.MapType.ofOptional(
    //             32, 43, Types.IntegerType.get(), Types.StringType.get()
    //         )
    //     )
    // );
    // StringWriter writer = new StringWriter();
    // JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
    // SchemaParser.toJson(schema.asStruct(), generator);
    // generator.flush();
    // System.out.println(writer.toString());

    const std::string map_json = R"({
        "type": "struct",
        "fields": [
            {
                "id": 21,
                "name": "map_col",
                "required": false,
                "type": {
                    "type": "map",
                    "key-id": 32,
                    "key": "int",
                    "value-id": 43,
                    "value": "string",
                    "value-required": false
                }
            }
        ]
    })";
    std::unique_ptr<Schema> schema = SchemaParser::from_json(map_json);

    std::vector<std::shared_ptr<arrow::Field>> fields;
    Status st;
    st = ArrowSchemaUtil::Convert(schema.get(), "utc", fields);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(1, fields.size());
    EXPECT_EQ("map_col", fields[0]->name());
    EXPECT_EQ("21", fields[0]->metadata()->Get(pfid).ValueUnsafe());

    arrow::MapType* arrow_map = dynamic_cast<arrow::MapType*>(fields[0]->type().get());
    auto map_fields = arrow_map->fields();
    EXPECT_EQ(1, arrow_map->fields().size());
    EXPECT_EQ("key", arrow_map->key_field()->name());
    EXPECT_EQ("value", arrow_map->item_field()->name());
    EXPECT_EQ("32", arrow_map->key_field()->metadata()->Get(pfid).ValueUnsafe());
    EXPECT_EQ("43", arrow_map->item_field()->metadata()->Get(pfid).ValueUnsafe());
}

TEST(ArrowSchemaUtilTest, test_list_field) {
    // list_json comes from :
    // Schema schema = new Schema(
    //     Types.NestedField.optional(
    //         21, "list_col", Types.ListType.ofOptional(
    //             32, Types.IntegerType.get())));
    // StringWriter writer = new StringWriter();
    // JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
    // SchemaParser.toJson(schema.asStruct(), generator);
    // generator.flush();
    // System.out.println(writer.toString());

    const std::string list_json = R"({
        "type": "struct",
        "fields": [
            {
                "id": 21,
                "name": "list_col",
                "required": false,
                "type": {
                    "type": "list",
                    "element-id": 32,
                    "element": "int",
                    "element-required": false
                }
            }
        ]
    })";
    std::unique_ptr<Schema> schema = SchemaParser::from_json(list_json);

    std::vector<std::shared_ptr<arrow::Field>> fields;
    Status st;
    st = ArrowSchemaUtil::Convert(schema.get(), "utc", fields);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(1, fields.size());
    EXPECT_EQ("list_col", fields[0]->name());
    EXPECT_EQ("21", fields[0]->metadata()->Get(pfid).ValueUnsafe());

    arrow::ListType* arrow_list = dynamic_cast<arrow::ListType*>(fields[0]->type().get());
    auto map_fields = arrow_list->fields();
    EXPECT_EQ(1, arrow_list->fields().size());
    EXPECT_EQ("element", arrow_list->value_field()->name());
    EXPECT_EQ("32", arrow_list->value_field()->metadata()->Get(pfid).ValueUnsafe());
}

} // namespace iceberg
} // namespace doris
