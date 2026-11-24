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

#include "format_v2/lance/lance_blob_column_reader.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <gtest/gtest.h>

#include "core/column/column_file.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/field.h"
#include "exprs/function/simple_function_factory.h"
#include "service/backend_options.h"

namespace doris::format::lance {

class LanceBlobColumnReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _old_backend_id = BackendOptions::get_backend_id();
        BackendOptions::set_backend_id(919);
        source.query_id_hi = 1;
        source.query_id_lo = 2;
        source.source_node_id = 7;
        source.dataset_uri = "s3://user:secret@bucket/assets.lance?token=private";
        source.dataset_version = 42;
        column.name = "image /一";
        column.type = make_nullable(std::make_shared<DataTypeFile>());
    }
    void TearDown() override { BackendOptions::set_backend_id(_old_backend_id); }

    std::shared_ptr<arrow::Array> descriptors(const std::vector<uint8_t>& kinds,
                                              const std::vector<uint64_t>& sizes,
                                              const std::vector<std::string>& uris,
                                              const std::vector<uint32_t>& bases,
                                              int null_kind = -1, int null_struct = -1) {
        arrow::UInt8Builder kind;
        arrow::UInt64Builder size;
        arrow::UInt32Builder base;
        arrow::StringBuilder uri;
        arrow::BooleanBuilder validity;
        for (size_t row = 0; row < kinds.size(); ++row) {
            if (static_cast<int>(row) == null_kind) {
                EXPECT_TRUE(kind.AppendNull().ok());
            } else {
                EXPECT_TRUE(kind.Append(kinds[row]).ok());
            }
            EXPECT_TRUE(size.Append(sizes[row]).ok());
            EXPECT_TRUE(base.Append(bases[row]).ok());
            EXPECT_TRUE(uri.Append(uris[row]).ok());
            EXPECT_TRUE(validity.Append(static_cast<int>(row) != null_struct).ok());
        }
        auto kind_array = kind.Finish().ValueOrDie();
        auto size_array = size.Finish().ValueOrDie();
        auto base_array = base.Finish().ValueOrDie();
        auto uri_array = uri.Finish().ValueOrDie();
        auto valid_array =
                std::static_pointer_cast<arrow::BooleanArray>(validity.Finish().ValueOrDie());
        return arrow::StructArray::Make({kind_array, size_array, base_array, uri_array},
                                        {"kind", "size", "blob_id", "blob_uri"},
                                        valid_array->values())
                .ValueOrDie();
    }

    FileReadBinding source;
    ColumnDefinition column;
    int64_t _old_backend_id;
};

TEST_F(LanceBlobColumnReaderTest, BuildsLogicalReferencesWithoutPhysicalPositions) {
    // Includes an empty inline Blob and an external length marker. Input is sliced and row IDs
    // are deliberately repeated/out of order; neither physical positions nor SDK handles exist.
    auto input = descriptors({0, 0, 1, 2, 3, 3}, {99, 0, 12, 23, 0, 8},
                             {"", "", "", "", "s3://external/whole", "s3://external/range"},
                             {0, 0, 4, 5, 0, 0});
    MutableColumnPtr output;
    const auto status = build_lance_blob_descriptors(input->Slice(1, 5), {9, 3, 9, 7, 8}, column,
                                                     source, &output);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(output->size(), 5);
    for (size_t row = 0; row < output->size(); ++row) {
        const auto file = (*output)[row].get<TYPE_FILE>();
        const auto& uri = file[DataTypeFile::URI_FIELD_INDEX].get<TYPE_STRING>();
        EXPECT_TRUE(uri.starts_with("lance-blob://"));
        EXPECT_EQ(uri.find("secret"), std::string::npos);
        EXPECT_EQ(uri.find("private"), std::string::npos);
        EXPECT_NE(uri.find("version=42&column=image+%2F"), std::string::npos);
        EXPECT_TRUE(file[DataTypeFile::OFFSET_FIELD_INDEX].is_null());
        EXPECT_TRUE(file[DataTypeFile::INLINE_FIELD_INDEX].is_null());
        ASSERT_NE(file.read_binding, nullptr);
        EXPECT_EQ(file.read_binding->dataset_uri, source.dataset_uri);
        EXPECT_EQ(file.read_binding->local_backend_id, 0);
    }
    EXPECT_EQ((*output)[0].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 0);
    EXPECT_EQ((*output)[1].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 12);
    EXPECT_EQ((*output)[2].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 23);
    EXPECT_TRUE((*output)[3].get<TYPE_FILE>()[2].is_null());
    EXPECT_EQ((*output)[4].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 8);
    EXPECT_EQ((*output)[2].get<TYPE_FILE>().read_row_id, 9);
}

TEST_F(LanceBlobColumnReaderTest, PreservesStructAndKindNulls) {
    auto input = descriptors({0, 3, 0}, {0, 0, 0}, {"", "file:///missing", ""}, {0, 0, 0}, 1, 0);
    MutableColumnPtr output;
    ASSERT_TRUE(build_lance_blob_descriptors(input, {1, 2, 3}, column, source, &output).ok());
    EXPECT_TRUE(output->is_null_at(0));
    EXPECT_TRUE(output->is_null_at(1));
    EXPECT_FALSE(output->is_null_at(2));
    column.type = std::make_shared<DataTypeFile>();
    EXPECT_FALSE(build_lance_blob_descriptors(input, {1, 2, 3}, column, source, &output).ok());
}

TEST_F(LanceBlobColumnReaderTest, KeepsLocalObjectsOnTheirOriginatingBackend) {
    auto input = descriptors({3, 3}, {0, 5}, {"file:///tmp/blob", "s3://bucket/blob"}, {0, 0});
    MutableColumnPtr output;
    ASSERT_TRUE(build_lance_blob_descriptors(input, {1, 2}, column, source, &output).ok());
    EXPECT_EQ((*output)[0].get<TYPE_FILE>().read_binding->local_backend_id, 919);
    EXPECT_EQ((*output)[1].get<TYPE_FILE>().read_binding->local_backend_id, 0);
    source.local_backend_id = 919; // The dataset has a registered local base.
    ASSERT_TRUE(build_lance_blob_descriptors(input, {1, 2}, column, source, &output).ok());
    EXPECT_EQ((*output)[1].get<TYPE_FILE>().read_binding->local_backend_id, 919);
}

TEST_F(LanceBlobColumnReaderTest, ValidatesDescriptorAlignmentAndStorageKind) {
    auto input = descriptors({4}, {0}, {""}, {0});
    MutableColumnPtr output;
    EXPECT_FALSE(build_lance_blob_descriptors(input, {}, column, source, &output).ok());
    EXPECT_FALSE(build_lance_blob_descriptors(input, {1}, column, source, &output).ok());
    EXPECT_FALSE(build_lance_blob_descriptors(nullptr, {1}, column, source, &output).ok());
}

TEST_F(LanceBlobColumnReaderTest, UnknownLengthReachesContentReadContextValidation) {
    auto input = descriptors({3}, {0}, {"s3://bucket/blob"}, {0});
    MutableColumnPtr output;
    ASSERT_TRUE(build_lance_blob_descriptors(input, {1}, column, source, &output).ok());
    const auto return_type = make_nullable(std::make_shared<DataTypeVarbinary>());
    Block block {{std::move(output), column.type, "blob"}, {nullptr, return_type, "content"}};
    auto function = SimpleFunctionFactory::instance().get_function(
            "fl_get_binary", {block.get_by_position(0)}, return_type);
    ASSERT_NE(function, nullptr);
    // A missing size is valid. Without a RuntimeState, content consumption must instead fail
    // at the query-context boundary, before any SDK handle or payload can be accessed.
    const auto status = function->execute(nullptr, block, {0}, 1, 1);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("requires a query context"), std::string::npos);
}

TEST_F(LanceBlobColumnReaderTest, InspectsRegisteredBasesWithoutPerRowResolution) {
    bool local = true;
    ASSERT_TRUE(lance_blob_base_paths_have_local_storage("{}", &local).ok());
    EXPECT_FALSE(local);
    ASSERT_TRUE(
            lance_blob_base_paths_have_local_storage(R"({"1":{"path":"s3://bucket/base"}})", &local)
                    .ok());
    EXPECT_FALSE(local);
    ASSERT_TRUE(lance_blob_base_paths_have_local_storage(
                        R"({"1":{"path":"s3://bucket/base"},"2":{"path":"/tmp/base"}})", &local)
                        .ok());
    EXPECT_TRUE(local);
    EXPECT_FALSE(lance_blob_base_paths_have_local_storage(R"({"1":{}})", &local).ok());
    EXPECT_FALSE(lance_blob_base_paths_have_local_storage("[]", &local).ok());
}

} // namespace doris::format::lance
