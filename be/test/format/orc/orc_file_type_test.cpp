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

#include "format/orc/orc_file_type.h"

#include <gtest/gtest.h>

#include <orc/Type.hh>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format/orc/vorc_reader.h"
#include "orc_memory_stream_test.h"

namespace doris {

TEST(OrcFileTypeTest, CanonicalSchemaHasOneMarkerAndSixChildren) {
    auto type = create_orc_file_type();
    EXPECT_EQ(type->toString(),
              "struct<uri:string,offset:bigint,size:bigint,content_type:string,checksum:string,"
              "inline:binary>");
    EXPECT_EQ(type->getAttributeKeys(), std::vector<std::string>({"doris.struct-type"}));
    EXPECT_TRUE(is_orc_file_type(*type));
    EXPECT_TRUE(validate_orc_file_type(*type).ok());
}

TEST(OrcFileTypeTest, MarkerIsRequiredAndDoesNotIdentifyOrdinaryStructs) {
    auto type = create_orc_file_type();
    type->removeAttribute("doris.struct-type");
    EXPECT_FALSE(is_orc_file_type(*type));
    EXPECT_FALSE(validate_orc_file_type(*type).ok());
    type->setAttribute("doris.struct-type", "file");
    EXPECT_FALSE(is_orc_file_type(*type));

    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto reader = OrcReader::create_unique(params, range, 1024, "UTC", nullptr, nullptr, true);
    EXPECT_EQ(reader->convert_to_doris_type(type.get())->get_primitive_type(), TYPE_STRUCT);
    type->setAttribute("doris.struct-type", "FILE");
    EXPECT_EQ(reader->convert_to_doris_type(type.get())->get_primitive_type(), TYPE_FILE);
}

TEST(OrcFileTypeTest, RejectsWrongPhysicalChildType) {
    auto type = orc::Type::buildTypeFromString(
            "struct<uri:string,offset:int,size:bigint,content_type:string,checksum:string,"
            "inline:binary>");
    type->setAttribute("doris.struct-type", "FILE");
    EXPECT_FALSE(validate_orc_file_type(*type).ok());
}

TEST(OrcFileTypeTest, AcceptsMissingOptionalChildrenInCanonicalOrder) {
    for (const auto* schema : {"struct<uri:string>", "struct<uri:string,inline:binary>",
                               "struct<uri:string,size:bigint,checksum:string>"}) {
        auto type = orc::Type::buildTypeFromString(schema);
        type->setAttribute("doris.struct-type", "FILE");
        EXPECT_TRUE(validate_orc_file_type(*type).ok()) << schema;
        TFileScanRangeParams params;
        TFileRangeDesc range;
        auto reader = OrcReader::create_unique(params, range, 1024, "UTC", nullptr, nullptr, true);
        EXPECT_EQ(reader->convert_to_doris_type(type.get())->get_primitive_type(), TYPE_FILE);
    }
}

TEST(OrcFileTypeTest, SparseSchemaRetainsNameOrderAndPhysicalTypeChecks) {
    for (const auto* schema :
         {"struct<size:bigint>", "struct<uri:string,extra:string>", "struct<uri:string,size:int>",
          "struct<uri:string,inline:string>", "struct<uri:string,checksum:string,size:bigint>",
          "struct<uri:string,size:bigint,size:bigint>"}) {
        auto type = orc::Type::buildTypeFromString(schema);
        type->setAttribute("doris.struct-type", "FILE");
        EXPECT_FALSE(validate_orc_file_type(*type).ok()) << schema;
    }
}

TEST(OrcFileTypeTest, InvalidSchemaReturnsStatusFromLegacySchemaEntryPoints) {
    auto schema = orc::Type::buildTypeFromString("struct<f:struct<uri:bigint>>");
    schema->getSubtype(0)->setAttribute("doris.struct-type", "FILE");
    MemoryOutputStream output(1024 * 1024);
    auto writer = orc::createWriter(*schema, &output, orc::WriterOptions());
    writer->close();
    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto reader = OrcReader::create_unique(params, range, 1024, "UTC", nullptr, nullptr, true);
    reader->_reader = orc::createReader(
            std::make_unique<MemoryInputStream>(output.getData(), output.getLength()),
            orc::ReaderOptions());
    std::vector<std::string> names;
    std::vector<DataTypePtr> types;
    const auto status = reader->get_parsed_schema(&names, &types);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Invalid FILE ORC child uri type"), std::string::npos);
    std::unordered_map<std::string, DataTypePtr> columns;
    EXPECT_FALSE(reader->get_columns(&columns).ok());
}

TEST(OrcFileTypeTest, ExplicitSchemaAndReaderPreserveNestedIdentity) {
    const auto file = make_nullable(std::make_shared<DataTypeFile>());
    const auto map = make_nullable(
            std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeString>()), file));
    const auto array = make_nullable(std::make_shared<DataTypeArray>(map));
    const auto type =
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {array}, Strings {"assets"}));
    auto schema = orc::Type::buildTypeFromString(
            "struct<assets:array<map<string,struct<uri:string,offset:bigint,size:bigint,"
            "content_type:string,checksum:string,inline:binary>>>>");
    ASSERT_TRUE(annotate_orc_file_types(type, *schema).ok());
    EXPECT_FALSE(is_orc_file_type(*schema));
    const auto* nested = schema->getSubtype(0)->getSubtype(0)->getSubtype(1);
    EXPECT_TRUE(is_orc_file_type(*nested));

    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto reader = OrcReader::create_unique(params, range, 1024, "UTC", nullptr, nullptr, true);
    const auto converted = reader->convert_to_doris_type(schema.get());
    EXPECT_TRUE(type->equals(*converted));
}

} // namespace doris
