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

#include "core/data_type/data_type_file.h"

#include <gen_cpp/data.pb.h>
#include <gen_cpp/types.pb.h>
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cstdlib>
#include <limits>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_file.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_varbinary.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"

namespace doris {

TEST(DataTypeFileTest, CanonicalIdentityAndMetadata) {
    DataTypeFile type;
    EXPECT_EQ(type.get_primitive_type(), TYPE_FILE);
    EXPECT_EQ(type.get_element_names(),
              (Strings {"uri", "offset", "size", "content_type", "checksum", "inline"}));
    ASSERT_EQ(type.get_elements().size(), 6);
    for (const auto& child : type.get_elements()) {
        EXPECT_TRUE(child->is_nullable());
    }
    EXPECT_EQ(remove_nullable(type.get_element(5))->get_primitive_type(), TYPE_VARBINARY);
    EXPECT_FALSE(type.equals(DataTypeStruct(type.get_elements(), type.get_element_names())));
    EXPECT_EQ(type.try_get_position_by_name("URI"), 0);

    PColumnMeta meta;
    type.to_pb_column_meta(&meta);
    EXPECT_EQ(meta.type(), PGenericType::FILE);
    EXPECT_EQ(meta.children_size(), 6);
    EXPECT_TRUE(type.equals(*DataTypeFactory::instance().create_data_type(meta)));
    TTypeDesc thrift;
    type.to_thrift(thrift);
    ASSERT_EQ(thrift.types.size(), 7);
    EXPECT_EQ(thrift.types[0].type, TTypeNodeType::FILE);
    EXPECT_TRUE(type.equals(*DataTypeFactory::instance().create_data_type(thrift, false)));
    PTypeDesc proto;
    static_cast<const IDataType&>(type).to_protobuf(&proto);
    EXPECT_TRUE(type.equals(*DataTypeFactory::instance().create_data_type(proto, false)));
    // FILE metadata must agree with FE's canonical inline VARBINARY length.
    EXPECT_EQ(proto.types(6).scalar_type().len(), std::numeric_limits<int32_t>::max());
}

TEST(DataTypeFileTest, RejectMalformedTypeDescriptors) {
    DataTypeFile type;
    TTypeDesc original;
    type.to_thrift(original);
    auto malformed = original;
    malformed.types.pop_back();
    EXPECT_THROW(DataTypeFactory::instance().create_data_type(malformed, false), Exception);
    malformed = original;
    malformed.types[1].type = TTypeNodeType::ARRAY;
    EXPECT_THROW(DataTypeFactory::instance().create_data_type(malformed, false), Exception);
    malformed = original;
    malformed.types[1].scalar_type.__set_len(1024);
    EXPECT_THROW(DataTypeFactory::instance().create_data_type(malformed, false), Exception);
    malformed = original;
    malformed.types[0].struct_fields[0].__set_contains_null(false);
    EXPECT_THROW(DataTypeFactory::instance().create_data_type(malformed, false), Exception);
    malformed = original;
    malformed.types[0].struct_fields[5].__set_name("payload");
    EXPECT_THROW(DataTypeFactory::instance().create_data_type(malformed, false), Exception);

    PTypeDesc proto;
    static_cast<const IDataType&>(type).to_protobuf(&proto);
    proto.mutable_types(6)->mutable_scalar_type()->set_type(TPrimitiveType::VARCHAR);
    EXPECT_THROW(DataTypeFactory::instance().create_data_type(proto, false), Exception);
}

TEST(DataTypeFileTest, ValidationRespectsFileNulls) {
    const auto type = make_nullable(std::make_shared<DataTypeFile>());
    auto column = type->create_column();
    column->insert_default();
    EXPECT_TRUE(validate_file_column(*column, type).ok());
    auto& nullable = assert_cast<ColumnNullable&>(*column);
    nullable.get_null_map_data()[0] = 0;
    EXPECT_FALSE(validate_file_column(*column, type).ok());
    nullable.get_null_map_data()[0] = 1;
    auto constant = ColumnConst::create(std::move(column), 8);
    EXPECT_TRUE(validate_file_column(*constant, type).ok());
}

namespace {
void validate_protected_inline() {
    const auto page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    auto* bytes = static_cast<char*>(
            mmap(nullptr, page_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (bytes == MAP_FAILED) {
        std::_Exit(1);
    }
    const auto type = std::make_shared<DataTypeFile>();
    auto column = type->create_column();
    column->insert_default();
    column->insert(
            Field::create_field<TYPE_FILE>(File {Field::create_field<TYPE_STRING>("urn:valid"),
                                                 Field(), Field(), Field(), Field(), Field()}));
    column->insert_default();
    auto& inline_column =
            assert_cast<ColumnNullable&>(assert_cast<ColumnFile&>(*column).get_column(5));
    auto& payloads = assert_cast<ColumnVarbinary&>(inline_column.get_nested_column()).get_data();
    payloads[0] = StringView(bytes, static_cast<uint32_t>(page_size));
    payloads[1] = StringView(bytes, static_cast<uint32_t>(page_size));
    inline_column.get_null_map_data().assign(size_t {3}, UInt8 {0});
    if (mprotect(bytes, page_size, PROT_NONE) != 0 || !validate_file_column(*column, type).ok()) {
        std::_Exit(2);
    }
    // Empty inline is still non-NULL; NULL uri + NULL inline is invalid.
    inline_column.get_null_map_data()[2] = 1;
    if (validate_file_column(*column, type).ok()) {
        std::_Exit(3);
    }
    column->resize(1);
    auto constant = ColumnConst::create(std::move(column), 8);
    if (!validate_file_column(*constant, type).ok()) {
        std::_Exit(4);
    }
    if (munmap(bytes, page_size) != 0) {
        std::_Exit(5);
    }
    std::_Exit(0);
}
} // namespace

TEST(DataTypeFileDeathTest, BoundaryValidationNeverReadsInlineBytes) {
    // A large VARBINARY view points to a protected page; reading or copying
    // its bytes during validation makes the child process fail.
    ASSERT_EXIT(validate_protected_inline(), ::testing::ExitedWithCode(0), "");
}

TEST(DataTypeFileTest, BoundaryValidationChecksMetadataWithInline) {
    const auto type = std::make_shared<DataTypeFile>();
    const std::string bytes(4096, '\xff');
    File valid {Field(), Field(), Field(),
                Field(), Field(), Field::create_field<TYPE_VARBINARY>(StringView(bytes))};
    for (const auto& [index, bad_value] :
         std::vector<std::pair<size_t, Field>> {{1, Field::create_field<TYPE_BIGINT>(Int64 {-1})},
                                                {1, Field::create_field<TYPE_BIGINT>(Int64 {0})},
                                                {2, Field::create_field<TYPE_BIGINT>(Int64 {-1})},
                                                {3, Field::create_field<TYPE_STRING>("text")},
                                                {4, Field::create_field<TYPE_STRING>("MD5:abc")}}) {
        auto fields = valid;
        fields[index] = bad_value;
        auto column = type->create_column();
        column->insert(Field::create_field<TYPE_FILE>(std::move(fields)));
        EXPECT_FALSE(validate_file_column(*column, type).ok()) << index;
    }
    valid[1] = Field::create_field<TYPE_BIGINT>(std::numeric_limits<Int64>::max());
    valid[2] = Field::create_field<TYPE_BIGINT>(Int64 {1});
    auto column = type->create_column();
    column->insert(Field::create_field<TYPE_FILE>(valid));
    EXPECT_FALSE(validate_file_column(*column, type).ok());
    valid[1] = Field::create_field<TYPE_BIGINT>(Int64 {0});
    column->clear();
    column->insert(Field::create_field<TYPE_FILE>(std::move(valid)));
    EXPECT_TRUE(validate_file_column(*column, type).ok());
}

TEST(DataTypeFileTest, SerializationRetainsBinaryInlineAndNulls) {
    DataTypeFile type;
    auto column = type.create_column();
    const std::string bytes("\0\xff\0", 3);
    column->insert(Field::create_field<TYPE_FILE>(
            File {Field::create_field<TYPE_STRING>(String {"s3://bucket/object?versionId=v1"}),
                  Field(), Field::create_field<TYPE_BIGINT>(Int64 {0}), Field(), Field(),
                  Field::create_field<TYPE_VARBINARY>(StringView(bytes))}));
    column->insert(Field::create_field<TYPE_FILE>(
            File {Field::create_field<TYPE_STRING>(String {"hdfs://host/a"}), Field(), Field(),
                  Field(), Field(), Field()}));
    column->insert(Field::create_field<TYPE_FILE>(
            File {Field(), Field(), Field(), Field(), Field(),
                  Field::create_field<TYPE_VARBINARY>(StringView(bytes))}));
    const auto size = type.get_uncompressed_serialized_bytes(*column, SUPPORT_FILE_VERSION);
    std::string buffer(size, '\0');
    const char* end = type.serialize(*column, buffer.data(), SUPPORT_FILE_VERSION);
    auto restored = type.create_column();
    EXPECT_EQ(type.deserialize(buffer.data(), &restored, SUPPORT_FILE_VERSION), end);
    ASSERT_EQ(restored->size(), 3);
    auto value = (*restored)[0];
    const auto& inline_value = value.get<TYPE_FILE>()[5].get<TYPE_VARBINARY>();
    EXPECT_EQ(std::string(inline_value.data(), inline_value.size()), bytes);
    EXPECT_TRUE((*restored)[1].get<TYPE_FILE>()[5].is_null());
    EXPECT_TRUE((*restored)[2].get<TYPE_FILE>()[0].is_null());
    EXPECT_EQ((*restored)[2].get<TYPE_FILE>()[5].as_string_view(), bytes);
    EXPECT_TRUE(validate_file_column(*restored, std::make_shared<DataTypeFile>()).ok());
    EXPECT_THROW(type.get_uncompressed_serialized_bytes(*column, SUPPORT_FILE_VERSION - 1),
                 Exception);
}

TEST(DataTypeFileTest, BlockRoundTripPreservesNestedAndConstantFileValues) {
    const auto file = make_nullable(std::make_shared<DataTypeFile>());
    const auto array = std::make_shared<DataTypeArray>(file);
    const std::string bytes(4096, '\xff');
    const auto value = [&bytes](bool empty_inline) {
        File fields(6);
        fields[0] = Field::create_field<TYPE_STRING>("s3://bucket/object?versionId=v1");
        fields[1] = Field::create_field<TYPE_BIGINT>(Int64 {7});
        fields[2] = Field::create_field<TYPE_BIGINT>(Int64 {11});
        fields[3] = Field::create_field<TYPE_STRING>("application/octet-stream");
        fields[4] = Field::create_field<TYPE_STRING>("ETAG:opaque");
        fields[5] = Field::create_field<TYPE_VARBINARY>(empty_inline ? StringView("", 0)
                                                                     : StringView(bytes));
        return Field::create_field<TYPE_FILE>(std::move(fields));
    };

    auto files = file->create_column();
    files->insert_default();
    files->insert(value(true));
    files->insert(value(false));
    auto arrays = array->create_column();
    arrays->insert(Field::create_field<TYPE_ARRAY>(Array {value(false), Field()}));
    arrays->insert_default();
    arrays->insert(Field::create_field<TYPE_ARRAY>(Array {value(true), value(false)}));
    auto constant_data = file->create_column();
    constant_data->insert(value(false));
    auto constant = ColumnConst::create(std::move(constant_data), 3);
    Block source {{std::move(files), file, "file"},
                  {std::move(arrays), array, "nested"},
                  {std::move(constant), file, "constant"}};

    for (const auto compression : {segment_v2::NO_COMPRESSION, segment_v2::SNAPPY}) {
        PBlock wire;
        size_t uncompressed_bytes = 0;
        size_t compressed_bytes = 0;
        int64_t codec_time = 0;
        ASSERT_TRUE(source.serialize(SUPPORT_FILE_VERSION, &wire, &uncompressed_bytes,
                                     &compressed_bytes, &codec_time, compression)
                            .ok());
        Block restored;
        ASSERT_TRUE(restored.deserialize(wire, &uncompressed_bytes, &codec_time).ok());
        ASSERT_EQ(restored.rows(), 3);
        ASSERT_EQ(restored.columns(), 3);
        EXPECT_TRUE(restored.get_by_position(0).type->equals(*file));
        EXPECT_TRUE(restored.get_by_position(1).type->equals(*array));
        EXPECT_TRUE(is_column_const(*restored.get_by_position(2).column));

        const auto& restored_files = *restored.get_by_position(0).column;
        EXPECT_TRUE(restored_files.is_null_at(0));
        const auto empty = restored_files[1];
        EXPECT_FALSE(empty.get<TYPE_FILE>()[5].is_null());
        EXPECT_EQ(empty.get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().size(), 0);
        const auto populated = restored_files[2];
        const auto& inline_value = populated.get<TYPE_FILE>()[5].get<TYPE_VARBINARY>();
        EXPECT_EQ(std::string(inline_value.data(), inline_value.size()), bytes);

        PBlock second_wire;
        ASSERT_TRUE(restored.serialize(SUPPORT_FILE_VERSION, &second_wire, &uncompressed_bytes,
                                       &compressed_bytes, &codec_time, compression)
                            .ok());
        EXPECT_EQ(wire.SerializeAsString(), second_wire.SerializeAsString());

        wire.set_be_exec_version(SUPPORT_FILE_VERSION - 1);
        Block old_version;
        EXPECT_FALSE(old_version.deserialize(wire, &uncompressed_bytes, &codec_time).ok());
    }
}

TEST(DataTypeFileTest, BlockDeserializePreservesValuesWithoutRevalidating) {
    const auto file = std::make_shared<DataTypeFile>();
    const auto parent =
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {file}, Strings {"file"}));
    auto column = parent->create_column();
    column->insert(
            Field::create_field<TYPE_STRUCT>(Struct {Field::create_field<TYPE_FILE>(File(6))}));
    auto& null_map = assert_cast<ColumnNullable&>(*column).get_null_map_data();
    for (const bool parent_is_null : {true, false}) {
        null_map[0] = parent_is_null;
        Block source {{column->get_ptr(), parent, "parent"}};
        PBlock wire;
        size_t uncompressed_bytes = 0;
        size_t compressed_bytes = 0;
        int64_t codec_time = 0;
        ASSERT_TRUE(source.serialize(SUPPORT_FILE_VERSION, &wire, &uncompressed_bytes,
                                     &compressed_bytes, &codec_time, segment_v2::SNAPPY)
                            .ok());
        Block restored;
        const auto status = restored.deserialize(wire, &uncompressed_bytes, &codec_time);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(restored.get_by_position(0).column->is_null_at(0), parent_is_null);
    }
}

} // namespace doris
