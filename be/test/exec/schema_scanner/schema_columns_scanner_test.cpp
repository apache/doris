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

#include "information_schema/schema_columns_scanner.h"

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <string>

namespace doris {

namespace {

TColumnDesc make_desc(const std::string& name, TPrimitiveType::type type) {
    TColumnDesc desc;
    desc.__set_columnName(name);
    desc.__set_columnType(type);
    return desc;
}

} // namespace

// COLUMN_TYPE is the text information_schema clients read a schema back from, so a
// STRUCT has to name its fields there. The names arrive lower cased from the FE, while
// SHOW CREATE TABLE keeps the spelling the user wrote.
class SchemaColumnsScannerTest : public testing::Test {
protected:
    std::string type_to_string(TColumnDesc& desc) { return _scanner._type_to_string(desc); }

private:
    SchemaColumnsScanner _scanner;
};

TEST_F(SchemaColumnsScannerTest, struct_type_string_carries_field_names) {
    TColumnDesc desc = make_desc("st", TPrimitiveType::STRUCT);
    desc.__set_children(
            {make_desc("f1", TPrimitiveType::INT), make_desc("f2", TPrimitiveType::STRING)});

    EXPECT_EQ("struct<f1:int(11),f2:string>", type_to_string(desc));
}

TEST_F(SchemaColumnsScannerTest, nested_struct_type_string_carries_field_names) {
    TColumnDesc element = make_desc("item", TPrimitiveType::STRUCT);
    element.__set_children({make_desc("deep", TPrimitiveType::INT)});

    // An array element has no name of its own, only the struct fields are named.
    TColumnDesc arr = make_desc("arr", TPrimitiveType::ARRAY);
    arr.__set_children({element});

    TColumnDesc outer = make_desc("outer", TPrimitiveType::STRUCT);
    outer.__set_children({arr});

    EXPECT_EQ("struct<arr:array<struct<deep:int(11)>>>", type_to_string(outer));
}

TEST_F(SchemaColumnsScannerTest, empty_struct_type_string) {
    TColumnDesc desc = make_desc("st", TPrimitiveType::STRUCT);
    EXPECT_EQ("struct<>", type_to_string(desc));
}

// Array and map keep printing types only, they have no field names to carry.
TEST_F(SchemaColumnsScannerTest, array_and_map_type_string_unchanged) {
    TColumnDesc arr = make_desc("arr", TPrimitiveType::ARRAY);
    arr.__set_children({make_desc("item", TPrimitiveType::INT)});
    EXPECT_EQ("array<int(11)>", type_to_string(arr));

    TColumnDesc mp = make_desc("mp", TPrimitiveType::MAP);
    mp.__set_children(
            {make_desc("key", TPrimitiveType::STRING), make_desc("value", TPrimitiveType::INT)});
    EXPECT_EQ("map<string,int(11)>", type_to_string(mp));
}

} // namespace doris
