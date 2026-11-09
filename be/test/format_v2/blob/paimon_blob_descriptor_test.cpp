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

#include <fstream>

#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "format_v2/blob/paimon_blob_columns_materializer.h"
#include "format_v2/blob/paimon_blob_test_util.h"
#include "format_v2/table_reader.h"
#include "util/coding.h"

namespace doris::format::paimon {
namespace {

std::string serialize_descriptor(const std::string& uri, int64_t offset, int64_t length,
                                 uint8_t version = 2) {
    std::string bytes(1, static_cast<char>(version));
    if (version == 2) {
        put_fixed64_le(&bytes, 0x424C4F4244455343ULL);
    }
    put_fixed32_le(&bytes, static_cast<uint32_t>(uri.size()));
    bytes += uri;
    put_fixed64_le(&bytes, offset);
    put_fixed64_le(&bytes, length);
    return bytes;
}

class PaimonBlobDescriptorTest : public blob_test::PaimonBlobTest {
protected:
    static std::vector<ColumnDefinition> descriptor_columns(const DataTypePtr& result_column_type) {
        ColumnDefinition id_column_definition;
        id_column_definition.name = "id";
        id_column_definition.type = std::make_shared<DataTypeInt32>();
        auto result_column_definition = blob_column("renamed_payload", 7);
        result_column_definition.type = result_column_type;
        return {id_column_definition, result_column_definition};
    }

    static Status materialize_descriptors(PaimonBlobColumnsMaterializer& materializer,
                                          const ColumnPtr& input_column,
                                          const DataTypePtr& input_column_type,
                                          const DataTypePtr& result_column_type,
                                          const PaimonBlobReadContext& read_context,
                                          ColumnPtr* result_column) {
        // Exercise the real mapper contract with field-ID matching after rename and a different
        // output position. A FILE's logical struct children must not become physical projections.
        const auto projected_columns = descriptor_columns(result_column_type);
        const auto& id_column_definition = projected_columns[0];
        const auto& result_column_definition = projected_columns[1];
        auto file_column_definition = result_column_definition;
        file_column_definition.name = "old_payload";
        file_column_definition.local_id = 3;
        file_column_definition.type = input_column_type;
        TPaimonFileDesc split_params;
        split_params.__set_blob_descriptor_field_ids({7});
        RETURN_IF_ERROR(materializer.prepare_split(split_params));
        TableColumnMapperOptions mapper_options;
        materializer.configure_mapper_options(&mapper_options);
        TableColumnMapper mapper(mapper_options);
        RETURN_IF_ERROR(mapper.create_mapping({id_column_definition, result_column_definition}, {},
                                              {file_column_definition}));
        FileScanRequest file_scan_request;
        RETURN_IF_ERROR(mapper.create_scan_request(
                {}, {id_column_definition, result_column_definition}, &file_scan_request));
        RETURN_IF_ERROR(materializer.prepare_file_inputs(mapper.mappings(), file_scan_request));
        Block file_block;
        file_block.insert({input_column, input_column_type, file_column_definition.name});
        auto table_block = output_block({id_column_definition, result_column_definition},
                                        input_column->size());
        RETURN_IF_ERROR(materializer.materialize(file_block, &table_block, read_context));
        *result_column = table_block.get_by_position(1).column;
        return Status::OK();
    }

    std::string pack() {
        const auto path = (_temp_directory / "payload.managed.blob").string();
        std::ofstream file(path, std::ios::binary);
        file << "header"
             << "hello"
             << "world";
        file.close();
        return "file:" + path;
    }
    static DataTypePtr bytes_type() { return make_nullable(std::make_shared<DataTypeString>()); }
    static DataTypePtr file_type() { return make_nullable(std::make_shared<DataTypeFile>()); }
    static ColumnPtr descriptors(const std::vector<std::optional<std::string>>& values) {
        auto column = bytes_type()->create_column();
        for (const auto& value : values) {
            if (value) {
                column->insert(Field::create_field<TYPE_STRING>(*value));
            } else {
                column->insert_default();
            }
        }
        return column;
    }
};

TEST_F(PaimonBlobDescriptorTest, DecodesSdkVersionOneAndTwo) {
    for (const uint8_t version : {uint8_t(1), uint8_t(2)}) {
        PaimonBlobReference descriptor;
        const auto bytes = serialize_descriptor("file:/pack.managed.blob", 17, 21, version);
        ASSERT_TRUE(PaimonBlobReference::decode(bytes, &descriptor).ok());
        EXPECT_EQ(descriptor.uri, "file:/pack.managed.blob");
        EXPECT_EQ(descriptor.offset, 17);
        EXPECT_EQ(descriptor.length, 21);
    }
}

TEST_F(PaimonBlobDescriptorTest, RejectsMalformedDescriptors) {
    PaimonBlobReference decoded;
    const auto valid = serialize_descriptor("file:/pack.managed.blob", 17, 21);
    for (size_t size = 0; size < valid.size(); ++size) {
        EXPECT_FALSE(
                PaimonBlobReference::decode(std::string_view(valid.data(), size), &decoded).ok());
    }
    auto bad = valid;
    bad[0] = 3;
    EXPECT_FALSE(PaimonBlobReference::decode(bad, &decoded).ok());
    bad = valid;
    bad[1] ^= 1;
    EXPECT_FALSE(PaimonBlobReference::decode(bad, &decoded).ok());
    EXPECT_FALSE(PaimonBlobReference::decode(valid + "extra", &decoded).ok());
    EXPECT_FALSE(
            PaimonBlobReference::decode(serialize_descriptor("file:/x", -1, 2), &decoded).ok());
    EXPECT_FALSE(
            PaimonBlobReference::decode(serialize_descriptor("file:/x", 0, -1), &decoded).ok());
    EXPECT_FALSE(
            PaimonBlobReference::decode(serialize_descriptor("file:/x", INT64_MAX, 1), &decoded)
                    .ok());
    EXPECT_FALSE(PaimonBlobReference::decode(serialize_descriptor("", 0, 0), &decoded).ok());
}

TEST_F(PaimonBlobDescriptorTest, ReadsRangesAndPreservesNullAndEmptyAcrossBatches) {
    const auto uri = pack();
    const auto source =
            descriptors({serialize_descriptor(uri, 11, 5), std::nullopt,
                         serialize_descriptor(uri, 6, 0), serialize_descriptor(uri, 6, 5)});
    PaimonBlobColumnsMaterializer materializer;
    ASSERT_TRUE(materializer.init(descriptor_columns(file_type()), {}).ok());
    ColumnPtr result;
    for (int batch = 0; batch < 2; ++batch) {
        ASSERT_TRUE(materialize_descriptors(materializer, source, bytes_type(), file_type(),
                                            read_context(), &result)
                            .ok());
        expect_inline(*result, 0, "world");
        expect_inline(*result, 1, std::nullopt);
        expect_inline(*result, 2, "");
        expect_inline(*result, 3, "hello");
    }
    materializer.reset();
    ASSERT_TRUE(materialize_descriptors(materializer, source, bytes_type(), file_type(),
                                        read_context(), &result)
                        .ok());
    expect_inline(*result, 0, "world");
}

TEST_F(PaimonBlobDescriptorTest, DescriptorOutputDoesNotOpenPayloadOrApplyInlineLimit) {
    const std::string uri = "file:/does-not-exist.managed.blob";
    PaimonBlobColumnsMaterializer materializer;
    ASSERT_TRUE(materializer.init(descriptor_columns(file_type()), {{"blob-as-descriptor", "true"}})
                        .ok());
    ColumnPtr result;
    ASSERT_TRUE(materialize_descriptors(materializer,
                                        descriptors({serialize_descriptor(uri, 22, 100)}),
                                        bytes_type(), file_type(), read_context(1), &result)
                        .ok());
    expect_descriptor(*result, 0, uri, 22, 100);
}

TEST_F(PaimonBlobDescriptorTest, ChecksSizeBeforeOpeningAndRejectsOutOfBounds) {
    PaimonBlobColumnsMaterializer materializer;
    ASSERT_TRUE(materializer.init(descriptor_columns(file_type()), {}).ok());
    ColumnPtr result;
    auto status = materialize_descriptors(
            materializer,
            descriptors({serialize_descriptor("file:/does-not-exist.managed.blob", 0, 2)}),
            bytes_type(), file_type(), read_context(1), &result);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("paimon_blob_max_size=1"), std::string::npos);
    EXPECT_FALSE(materialize_descriptors(materializer,
                                         descriptors({serialize_descriptor(pack(), 15, 2)}),
                                         bytes_type(), file_type(), read_context(), &result)
                         .ok());
    auto context = read_context();
    _io_context.should_stop = true;
    EXPECT_FALSE(materialize_descriptors(materializer,
                                         descriptors({serialize_descriptor(pack(), 6, 5)}),
                                         bytes_type(), file_type(), context, &result)
                         .ok());
}

TEST_F(PaimonBlobDescriptorTest, MaterializesArrayElementsAndKeepsOffsets) {
    const auto uri = pack();
    ColumnPtr offsets = ColumnArray::ColumnOffsets::create();
    auto mutable_offsets = IColumn::mutate(offsets);
    auto& values = assert_cast<ColumnArray::ColumnOffsets&>(*mutable_offsets);
    values.insert_value(3);
    values.insert_value(3); // Empty array is distinct from NULL.
    offsets = std::move(mutable_offsets);
    ColumnPtr source =
            ColumnArray::create(descriptors({serialize_descriptor(uri, 6, 5), std::nullopt,
                                             serialize_descriptor(uri, 11, 0)}),
                                offsets);
    const auto source_type = std::make_shared<DataTypeArray>(bytes_type());
    const auto target_type = std::make_shared<DataTypeArray>(file_type());
    PaimonBlobColumnsMaterializer materializer;
    ASSERT_TRUE(materializer.init(descriptor_columns(target_type), {}).ok());
    ColumnPtr result;
    ASSERT_TRUE(materialize_descriptors(materializer, source, source_type, target_type,
                                        read_context(), &result)
                        .ok());
    const auto& array = assert_cast<const ColumnArray&>(*result);
    EXPECT_EQ(array.get_offsets_ptr().get(), offsets.get());
    expect_inline(array.get_data(), 0, "hello");
    expect_inline(array.get_data(), 1, std::nullopt);
    expect_inline(array.get_data(), 2, "");
    EXPECT_EQ(array.size(), 2);
}

TEST_F(PaimonBlobDescriptorTest, MaterializesMapValuesWithoutCopyingKeys) {
    const auto uri = pack();
    ColumnPtr keys = descriptors({std::string("main"), std::string("aux")});
    auto offset_values = ColumnArray::ColumnOffsets::create();
    offset_values->insert_value(2);
    ColumnPtr offsets = std::move(offset_values);
    ColumnPtr source = ColumnMap::create(
            keys, descriptors({serialize_descriptor(uri, 6, 5), std::nullopt}), offsets);
    const auto source_type = std::make_shared<DataTypeMap>(bytes_type(), bytes_type());
    const auto target_type = std::make_shared<DataTypeMap>(bytes_type(), file_type());
    PaimonBlobColumnsMaterializer materializer;
    ASSERT_TRUE(materializer.init(descriptor_columns(target_type), {}).ok());
    ColumnPtr result;
    ASSERT_TRUE(materialize_descriptors(materializer, source, source_type, target_type,
                                        read_context(), &result)
                        .ok());
    const auto& map = assert_cast<const ColumnMap&>(*result);
    EXPECT_EQ(map.get_keys_ptr().get(), keys.get());
    expect_inline(map.get_values(), 0, "hello");
    expect_inline(map.get_values(), 1, std::nullopt);
}

TEST_F(PaimonBlobDescriptorTest, RejectsNullInRequiredFile) {
    PaimonBlobColumnsMaterializer materializer;
    ASSERT_TRUE(materializer.init(descriptor_columns(std::make_shared<DataTypeFile>()), {}).ok());
    ColumnPtr result;
    EXPECT_FALSE(materialize_descriptors(materializer, descriptors({std::nullopt}), bytes_type(),
                                         std::make_shared<DataTypeFile>(), read_context(), &result)
                         .ok());
}

TEST_F(PaimonBlobDescriptorTest, ChecksInlineCapacityBeforeOpeningPayload) {
    PaimonBlobColumnsMaterializer materializer;
    ASSERT_TRUE(materializer.init(descriptor_columns(file_type()), {}).ok());
    ColumnPtr result;
    const int64_t length = int64_t(INT32_MAX) + 1;
    const auto source = descriptors({serialize_descriptor("file:/absent.managed.blob", 0, length)});
    auto status = materialize_descriptors(materializer, source, bytes_type(), file_type(),
                                          read_context(length), &result);
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
}

} // namespace
} // namespace doris::format::paimon
