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

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/encryption/encryption.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/data_type_number.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_crypto.h"
#include "format_v2/parquet/parquet_file_context.h"
#include "format_v2/parquet/parquet_reader.h"
#include "format_v2/parquet/parquet_statistics.h"
#include "format_v2/parquet/reader/count_column_reader.h"
#include "format_v2/parquet/reader/native_column_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/file_factory.h"
#include "io/fs/file_meta_cache.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/scoped_temp_dir.h"
#include "util/coding.h"
#include "util/defer_op.h"
#include "util/thrift_util.h"

namespace doris::format::parquet {
namespace {

const std::string KEY = "synthetic-key123";
const std::string AAD = "synthetic-file-aad";

#if __has_include(<parquet/encryption/encryption_internal.h>)
std::vector<uint8_t> encrypted_file(
        bool dictionary = true,
        ::parquet::ParquetDataPageVersion page_version = ::parquet::ParquetDataPageVersion::V1,
        const std::string& key = KEY) {
    auto sink = ::arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto schema = ::parquet::schema::GroupNode::Make(
            "schema", ::parquet::Repetition::REQUIRED,
            {::parquet::schema::PrimitiveNode::Make("id", ::parquet::Repetition::REQUIRED,
                                                    ::parquet::Type::INT32)});
    ::parquet::FileEncryptionProperties::Builder encryption {
            ::arrow::util::SecureString(std::string(key))};
    encryption.aad_prefix(AAD)->disable_aad_prefix_storage();
    ::parquet::WriterProperties::Builder properties;
    properties.encryption(encryption.build())
            ->compression(::parquet::Compression::SNAPPY)
            ->data_page_version(page_version)
            ->enable_write_page_index()
            ->data_pagesize(1)
            ->write_batch_size(1);
    if (dictionary) {
        properties.enable_dictionary();
    } else {
        properties.disable_dictionary();
    }
    auto writer = ::parquet::ParquetFileWriter::Open(
            sink, std::static_pointer_cast<::parquet::schema::GroupNode>(schema),
            properties.build());
    for (int32_t first : {1, 5}) {
        auto* group = writer->AppendRowGroup();
        auto* column = static_cast<::parquet::Int32Writer*>(group->NextColumn());
        const int32_t values[] = {first, first + 1, first + 2, first + 3};
        column->WriteBatch(4, nullptr, nullptr, values);
        group->Close();
    }
    writer->Close();
    auto bytes = sink->Finish().ValueOrDie();
    return {bytes->data(), bytes->data() + bytes->size()};
}

#endif

class EncryptionMemoryReader final : public io::FileReader {
public:
    explicit EncryptionMemoryReader(std::vector<uint8_t> bytes) : _bytes(std::move(bytes)) {}
    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return _bytes.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 1; }
    [[maybe_unused]] size_t read_count() const { return _read_count; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext*) override {
        if (offset > _bytes.size() || result.size > _bytes.size() - offset) {
            return Status::IOError("Synthetic encryption fixture read exceeds file");
        }
        ++_read_count;
        memcpy(result.data, _bytes.data() + offset, result.size);
        *bytes_read = result.size;
        return Status::OK();
    }

private:
    std::vector<uint8_t> _bytes;
    io::Path _path {"synthetic-encrypted.parquet"};
    bool _closed = false;
    size_t _read_count = 0;
};

std::vector<uint8_t> java_fixture() {
    const char* root = std::getenv("ROOT");
    DORIS_CHECK(root != nullptr);
    std::ifstream input(
            std::string(root) +
                    "/be/test/format_v2/parquet/test_data/encryption/iceberg_java.parquet",
            std::ios::binary);
    DORIS_CHECK(input.good());
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

TFileEncryptionInfo parameters(const std::string& key = KEY, const std::string& aad = AAD) {
    TFileEncryptionInfo info;
    info.__set_version(1);
    info.__set_key(key);
    info.__set_aad_prefix(aad);
    return info;
}

TEST(ParquetEncryptionTest, EncryptedFooterWithoutKeyHasExplicitFailure) {
    auto file = std::make_shared<EncryptionMemoryReader>(java_fixture());
    ParquetFileContext context;
    io::FileDescription description;
    auto status = context.open(file, nullptr, false, description);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("[MISSING_KEY]"), std::string::npos) << status;
}

TEST(ParquetEncryptionTest, AcceptsOnlyAuthenticatedZeroPaddingAfterMetadata) {
    EXPECT_TRUE(validate_crypto_metadata_padding(std::vector<uint8_t> {1, 0, 0}, 1).ok());
    EXPECT_TRUE(validate_crypto_metadata_padding(std::vector<uint8_t> {1}, 1).ok());
    EXPECT_FALSE(validate_crypto_metadata_padding(std::vector<uint8_t> {1, 0, 2}, 1).ok());
}

TEST(ParquetEncryptionTest, RejectsAmplifiedNativeContainersBeforeResize) {
    using namespace apache::thrift::protocol;
    auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    TCompactProtocol writer(transport);
    writer.writeStructBegin("FileMetaData");
    writer.writeFieldBegin("version", T_I32, 1);
    writer.writeI32(1);
    writer.writeFieldEnd();
    writer.writeFieldBegin("schema", T_LIST, 2);
    writer.writeListBegin(T_STRUCT, 65537);
    for (int i = 0; i < 65537; ++i) {
        writer.writeStructBegin("SchemaElement");
        writer.writeFieldStop();
        writer.writeStructEnd();
    }
    writer.writeListEnd();
    writer.writeFieldEnd();
    writer.writeFieldStop();
    writer.writeStructEnd();
    uint8_t* bytes = nullptr;
    uint32_t size = 0;
    transport->getBuffer(&bytes, &size);
    EXPECT_LT(size, 100000);
    tparquet::FileMetaData parsed;
    uint32_t consumed = 0;
    auto status = deserialize_crypto_metadata(std::span(bytes, size), &consumed, &parsed);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(parsed.schema.empty());
}

#if __has_include(<parquet/encryption/encryption_internal.h>)
Status native_rows(const std::vector<uint8_t>& bytes, const TFileEncryptionInfo& info,
                   std::vector<std::vector<Field>>* rows) {
    auto file = std::make_shared<EncryptionMemoryReader>(bytes);
    ParquetFileContext context;
    io::FileDescription description;
    description.is_immutable = true;
    RETURN_IF_ERROR(context.open(file, nullptr, true, description, false, false, false, &info));
    EXPECT_FALSE(context.native_page_cache_enabled);
    EXPECT_EQ(context.native_footer_cache_hits, 0);
    std::vector<std::unique_ptr<ParquetColumnSchema>> schema;
    RETURN_IF_ERROR(build_parquet_column_schema(context.native_metadata->schema(), &schema));
    rows->resize(schema.size());
    const auto& groups = context.native_metadata->to_thrift().row_groups;
    for (int group = 0; std::cmp_less(group, groups.size()); ++group) {
        for (size_t column = 0; column < schema.size(); ++column) {
            std::unique_ptr<ParquetColumnReader> reader;
            RETURN_IF_ERROR(NativeColumnReader::create(
                    *schema[column], nullptr, file, context.native_metadata, group,
                    {{0, groups[group].num_rows}}, {}, nullptr, std::nullopt, nullptr, nullptr,
                    true, "synthetic-cache-key", true, {}, &reader));
            auto output = reader->type()->create_column();
            int64_t offset = 0;
            while (offset < groups[group].num_rows) {
                int64_t read = 0;
                int64_t requested =
                        std::min<int64_t>(offset == 0 ? 1 : 3, groups[group].num_rows - offset);
                RETURN_IF_ERROR(reader->read(requested, output, &read));
                if (read != requested) {
                    return Status::Corruption(
                            "Synthetic native reader returned an incomplete batch");
                }
                offset += read;
            }
            for (size_t row = 0; row < output->size(); ++row) {
                (*rows)[column].push_back((*output)[row]);
            }
        }
    }
    return context.close();
}

bool equal_reference(const Field& value, const std::shared_ptr<::arrow::Scalar>& reference) {
    if (!reference->is_valid) {
        return value.is_null();
    }
    if (value.is_null()) {
        return false;
    }
    switch (reference->type->id()) {
    case ::arrow::Type::INT32:
        return value.get<TYPE_INT>() == static_cast<const ::arrow::Int32Scalar&>(*reference).value;
    case ::arrow::Type::STRING:
        return value.get<TYPE_STRING>() ==
               static_cast<const ::arrow::StringScalar&>(*reference).view();
    case ::arrow::Type::LIST: {
        const auto& items = value.get<TYPE_ARRAY>();
        const auto& expected = static_cast<const ::arrow::ListScalar&>(*reference).value;
        if (std::cmp_not_equal(items.size(), expected->length())) {
            return false;
        }
        for (size_t i = 0; i < items.size(); ++i) {
            if (!equal_reference(items[i], expected->GetScalar(i).ValueOrDie())) {
                return false;
            }
        }
        return true;
    }
    case ::arrow::Type::STRUCT: {
        const auto& fields = value.get<TYPE_STRUCT>();
        const auto& expected = static_cast<const ::arrow::StructScalar&>(*reference).value;
        if (fields.size() != expected.size()) {
            return false;
        }
        for (size_t i = 0; i < fields.size(); ++i) {
            if (!equal_reference(fields[i], expected[i])) {
                return false;
            }
        }
        return true;
    }
    default:
        ADD_FAILURE() << "Unsupported reference fixture type";
        return false;
    }
}

// Assertion macros inflate complexity in this small encoding/page-version/shape matrix.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(ParquetEncryptionTest, EncryptedNestedPageVersionsPreserveNullAndEmptyShapes) {
    for (bool dictionary : {false, true}) {
        for (auto version :
             {::parquet::ParquetDataPageVersion::V1, ::parquet::ParquetDataPageVersion::V2}) {
            auto values = std::make_shared<::arrow::Int32Builder>();
            ::arrow::ListBuilder lists(::arrow::default_memory_pool(), values);
            for (int id = 1; id <= 8; ++id) {
                int shape = (id - 1) % 4;
                if (shape == 1) {
                    ASSERT_TRUE(lists.AppendNull().ok());
                } else {
                    ASSERT_TRUE(lists.Append().ok());
                    if (shape == 0) {
                        ASSERT_TRUE(values->Append(id).ok());
                        ASSERT_TRUE(values->Append(id + 1).ok());
                    } else if (shape == 3) {
                        ASSERT_TRUE(values->AppendNull().ok());
                        ASSERT_TRUE(values->Append(id).ok());
                    }
                }
            }
            auto array = lists.Finish().ValueOrDie();
            auto table = ::arrow::Table::Make(
                    ::arrow::schema({::arrow::field("items", array->type())}), {array});
            auto sink = ::arrow::io::BufferOutputStream::Create().ValueOrDie();
            ::parquet::FileEncryptionProperties::Builder encryption {
                    ::arrow::util::SecureString(std::string(KEY))};
            encryption.aad_prefix(AAD)->disable_aad_prefix_storage();
            ::parquet::WriterProperties::Builder properties;
            properties.encryption(encryption.build())
                    ->data_page_version(version)
                    ->compression(::parquet::Compression::SNAPPY)
                    ->data_pagesize(1)
                    ->write_batch_size(1);
            if (!dictionary) {
                properties.disable_dictionary();
            }
            ASSERT_TRUE(::parquet::arrow::WriteTable(*table, ::arrow::default_memory_pool(), sink,
                                                     4, properties.build())
                                .ok());
            auto buffer = sink->Finish().ValueOrDie();
            std::vector<uint8_t> bytes(buffer->data(), buffer->data() + buffer->size());
            std::vector<std::vector<Field>> rows;
            auto status = native_rows(bytes, parameters(), &rows);
            ASSERT_TRUE(status.ok()) << status;
            ASSERT_EQ(rows.size(), 1);
            ASSERT_EQ(rows[0].size(), 8);
            for (int row = 0; row < 8; ++row) {
                EXPECT_TRUE(equal_reference(rows[0][row], array->GetScalar(row).ValueOrDie()));
            }
        }
    }
}

// Assertion macros inflate complexity in this dictionary/page-version/key-size matrix.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(ParquetEncryptionTest, NativePagesMatchReferenceAcrossKeysEncodingsAndRowGroups) {
    for (bool dictionary : {false, true}) {
        for (auto version :
             {::parquet::ParquetDataPageVersion::V1, ::parquet::ParquetDataPageVersion::V2}) {
            for (size_t key_size : {16, 24, 32}) {
                SCOPED_TRACE(::testing::Message() << dictionary << " key=" << key_size
                                                  << " page=" << static_cast<int>(version));
                std::string key(key_size, 'k');
                auto bytes = encrypted_file(dictionary, version, key);
                std::vector<std::vector<Field>> rows;
                ASSERT_TRUE(native_rows(bytes, parameters(key), &rows).ok());
                ASSERT_EQ(rows.size(), 1);
                ASSERT_EQ(rows[0].size(), 8);
                for (int i = 0; i < 8; ++i) {
                    EXPECT_EQ(rows[0][i].get<TYPE_INT>(), i + 1);
                }
            }
        }
    }
}

TEST(ParquetEncryptionTest, IcebergJavaNestedFileMatchesIndependentReferenceReader) {
    auto bytes = java_fixture();
    const std::string aad = "0123456789abcdef";
    std::vector<std::vector<Field>> rows;
    auto status = native_rows(bytes, parameters(KEY, aad), &rows);
    ASSERT_TRUE(status.ok()) << status;
    ::parquet::FileDecryptionProperties::Builder decryption;
    decryption.footer_key(::arrow::util::SecureString(std::string(KEY)));
    ::parquet::ReaderProperties properties;
    properties.file_decryption_properties(decryption.aad_prefix(aad)->build());
    ::parquet::arrow::FileReaderBuilder builder;
    ASSERT_TRUE(
            builder.Open(std::make_shared<::arrow::io::BufferReader>(::arrow::Buffer::Wrap(bytes)),
                         properties)
                    .ok());
    auto reference_reader = builder.Build().ValueOrDie();
    auto table = reference_reader->ReadTable().ValueOrDie();
    ASSERT_EQ(table->num_rows(), 8);
    ASSERT_EQ(rows.size(), table->num_columns());
    for (size_t col = 0; col < rows.size(); ++col) {
        ASSERT_EQ(rows[col].size(), 8);
        for (int row = 0; row < 8; ++row) {
            SCOPED_TRACE(::testing::Message() << "column=" << col << " row=" << row);
            EXPECT_TRUE(equal_reference(rows[col][row],
                                        table->column(col)->GetScalar(row).ValueOrDie()));
        }
    }
}

TEST(ParquetEncryptionTest, WrongKeyMissingAadCorruptFooterAndHeaderFailWithoutLeakingKeys) {
    auto bytes = encrypted_file();
    auto info = parameters();
    for (int failure = 0; failure < 6; ++failure) {
        auto corrupt = bytes;
        auto bad_info = info;
        if (failure == 0) {
            bad_info.key[0] ^= 1;
        }
        if (failure == 1) {
            bad_info.aad_prefix.clear();
        }
        if (failure == 2) {
            corrupt[corrupt.size() - 12] ^= 1;
        }
        if (failure == 3) {
            corrupt.resize(corrupt.size() - 1);
        }
        if (failure == 4) {
            bad_info.__isset.aad_prefix = false;
        }
        if (failure == 5) {
            bad_info.key.resize(15);
        }
        ParquetFileContext context;
        io::FileDescription description;
        auto status = context.open(std::make_shared<EncryptionMemoryReader>(corrupt), nullptr, true,
                                   description, false, false, false, &bad_info);
        ASSERT_FALSE(status.ok()) << failure;
        EXPECT_EQ(status.to_string().find(KEY), std::string::npos);
        EXPECT_EQ(status.to_string().find(AAD), std::string::npos);
    }
    // Corrupt an authenticated header and its length separately; neither may reach decoding.
    for (int position : {4, 12}) {
        auto corrupt = bytes;
        corrupt[position] ^= 0x7f;
        std::vector<std::vector<Field>> rows;
        auto status = native_rows(corrupt, info, &rows);
        EXPECT_FALSE(status.ok()) << position;
        EXPECT_EQ(status.to_string().find(KEY), std::string::npos);
    }
}

TEST(ParquetEncryptionTest, AuthenticatesDictionaryAndDataPayloadsBeforeDecompression) {
    auto bytes = encrypted_file();
    auto info = parameters();
    ParquetFileContext context;
    io::FileDescription description;
    ASSERT_TRUE(context.open(std::make_shared<EncryptionMemoryReader>(bytes), nullptr, true,
                             description, false, false, false, &info)
                        .ok());
    const auto& column = context.native_metadata->to_thrift().row_groups[0].columns[0].meta_data;
    for (int64_t header : {column.dictionary_page_offset, column.data_page_offset}) {
        auto corrupt = bytes;
        const auto header_size = decode_fixed32_le(corrupt.data() + header) + 4;
        const auto body = header + header_size;
        ASSERT_LT(body + 8, corrupt.size());
        corrupt[body + 8] ^= 1;
        std::vector<std::vector<Field>> rows;
        auto status = native_rows(corrupt, info, &rows);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("[DECRYPTION_FAILED]"), std::string::npos) << status;
    }
    ASSERT_TRUE(context.close().ok());
}

TEST(ParquetEncryptionTest, SharedMetadataCacheCannotAuthorizeAnotherKey) {
    FileMetaCache cache(64);
    auto* environment = ExecEnv::GetInstance();
    auto* previous = environment->_file_meta_cache;
    environment->_file_meta_cache = &cache;
    Defer restore {[&] { environment->_file_meta_cache = previous; }};
    auto bytes = encrypted_file();
    std::vector<std::vector<Field>> rows;
    ASSERT_TRUE(native_rows(bytes, parameters(), &rows).ok());
    auto wrong = parameters();
    wrong.key[0] ^= 1;
    EXPECT_FALSE(native_rows(bytes, wrong, &rows).ok());
    EXPECT_TRUE(native_rows(bytes, parameters(), &rows).ok());
}

TEST(ParquetEncryptionTest, EncryptedIndexBytesAreNotReadBySequentialStage) {
    auto file = std::make_shared<EncryptionMemoryReader>(encrypted_file());
    auto info = parameters();
    ParquetFileContext context;
    io::FileDescription description;
    ASSERT_TRUE(context.open(file, nullptr, true, description, false, false, false, &info).ok());
    const auto& chunk = context.native_metadata->to_thrift().row_groups[0].columns[0];
    ASSERT_TRUE(chunk.__isset.column_index_offset);
    ASSERT_TRUE(chunk.__isset.offset_index_offset);
    auto read_count = file->read_count();
    std::unordered_map<int, tparquet::OffsetIndex> offsets;
    std::unordered_map<int, NativeParquetPageIndex> pages;
    ASSERT_TRUE(context.load_native_offset_indexes(0, {0}, &offsets).ok());
    ASSERT_TRUE(context.load_native_page_indexes(0, {0}, &pages).ok());
    EXPECT_TRUE(offsets.empty());
    EXPECT_TRUE(pages.empty());
    EXPECT_EQ(file->read_count(), read_count);
    ASSERT_TRUE(context.close().ok());
}

TEST(ParquetEncryptionTest, LevelsOnlyReaderDecryptsNullableNestedShape) {
    auto file = std::make_shared<EncryptionMemoryReader>(java_fixture());
    auto info = parameters(KEY, "0123456789abcdef");
    ParquetFileContext context;
    io::FileDescription description;
    ASSERT_TRUE(context.open(file, nullptr, false, description, false, false, false, &info).ok());
    std::vector<std::unique_ptr<ParquetColumnSchema>> schema;
    ASSERT_TRUE(build_parquet_column_schema(context.native_metadata->schema(), &schema).ok());
    std::unique_ptr<CountColumnReader> reader;
    ASSERT_TRUE(CountColumnReader::create(file, context.native_metadata, 0, *schema[2], nullptr,
                                          nullptr, true, "synthetic-cache-key", {}, &reader)
                        .ok());
    int64_t read = 0;
    auto status = reader->read_levels(8, &read);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(read, 8);
    EXPECT_EQ(reader->definition_levels(),
              (std::vector<int16_t> {3, 3, 0, 1, 2, 3, 3, 3, 0, 1, 2, 3}));
    reader.reset();
    ASSERT_TRUE(context.close().ok());
}

TEST(ParquetEncryptionTest, PhysicalReaderAcceptsParametersWithoutOpeningSqlAdmission) {
    auto bytes = encrypted_file();
    test::ScopedTempDirectory directory("parquet-encryption");
    auto path = directory.path() / "data.parquet";
    {
        std::ofstream out(path, std::ios::binary);
        out.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }
    auto properties = std::make_shared<io::FileSystemProperties>();
    properties->system_type = TFileType::FILE_LOCAL;
    auto description = std::make_unique<io::FileDescription>();
    description->path = path;
    description->file_size = bytes.size();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ParquetReader reader(properties, description, nullptr, nullptr, std::nullopt, false, false,
                         std::nullopt, false, nullptr, parameters());
    ASSERT_TRUE(reader.init(&state).ok());
    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    ASSERT_TRUE(reader.open(request).ok());
    bool eof = false;
    int64_t total = 0;
    while (!eof) {
        Block block;
        block.insert({std::make_shared<DataTypeInt32>()->create_column(),
                      std::make_shared<DataTypeInt32>(), "id"});
        size_t read = 0;
        auto status = reader.get_block(&block, &read, &eof);
        ASSERT_TRUE(status.ok()) << status;
        for (size_t row = 0; row < read; ++row) {
            EXPECT_EQ((*block.get_by_position(0).column)[row].get<TYPE_INT>(), ++total);
        }
    }
    EXPECT_EQ(total, 8);
    ASSERT_TRUE(reader.close().ok());
}
#else
TEST(ParquetEncryptionTest, DependencyBundleMustBeRebuiltForDecryption) {
    auto file = std::make_shared<EncryptionMemoryReader>(java_fixture());
    ParquetFileContext context;
    io::FileDescription description;
    auto info = parameters(KEY, "0123456789abcdef");
    auto status = context.open(file, nullptr, false, description, false, false, false, &info);
    EXPECT_NE(status.to_string().find("[PME_DEPENDENCY_UNAVAILABLE]"), std::string::npos) << status;
}
#endif

} // namespace
} // namespace doris::format::parquet
