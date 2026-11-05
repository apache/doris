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
#include "runtime/runtime_profile.h"
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
        const std::string& key = KEY, bool plaintext_footer = false) {
    auto sink = ::arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto schema = ::parquet::schema::GroupNode::Make(
            "schema", ::parquet::Repetition::REQUIRED,
            {::parquet::schema::PrimitiveNode::Make("id", ::parquet::Repetition::REQUIRED,
                                                    ::parquet::Type::INT32)});
    ::parquet::FileEncryptionProperties::Builder encryption {
            ::arrow::util::SecureString(std::string(key))};
    encryption.aad_prefix(AAD)->disable_aad_prefix_storage();
    if (plaintext_footer) {
        encryption.set_plaintext_footer();
    }
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

std::vector<uint8_t> java_fixture(const std::string& name = "iceberg_java.parquet") {
    const char* root = std::getenv("ROOT");
    DORIS_CHECK(root != nullptr);
    std::ifstream input(
            std::string(root) + "/be/test/format_v2/parquet/test_data/encryption/" + name,
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

void expect_cache_counters(const ParquetColumnReaderProfile& counters, bool enabled, int repeat) {
    if (!enabled) {
        EXPECT_EQ(counters.page_cache_write_count->value(), 0);
        EXPECT_EQ(counters.page_cache_hit_count->value(), 0);
    } else if (repeat == 0) {
        EXPECT_GT(counters.page_cache_write_count->value(), 0);
    } else {
        EXPECT_GT(counters.page_cache_hit_count->value(), 0);
    }
}

void verify_explicit_cache_policy(const io::FileReaderSPtr& file,
                                  const NativeParquetMetadata* metadata,
                                  const ParquetColumnSchema& schema, RuntimeState* state,
                                  bool enabled, const std::string& cache_key) {
    const std::unordered_map<int, tparquet::OffsetIndex> indexes;
    const auto rows = metadata->to_thrift().row_groups[0].num_rows;
    for (int repeat : {0, 1}) {
        RuntimeProfile profile("native_cache_policy");
        ParquetColumnReaderProfile counters;
        counters.page_cache_write_count = profile.add_counter("Writes", TUnit::UNIT);
        counters.page_cache_hit_count = profile.add_counter("Hits", TUnit::UNIT);
        std::unique_ptr<ParquetColumnReader> reader;
        ASSERT_TRUE(NativeColumnReader::create(schema, nullptr, file, metadata, 0, {{0, rows}},
                                               indexes, nullptr, std::nullopt, nullptr, state,
                                               enabled, cache_key, false, counters, &reader)
                            .ok());
        auto output = reader->type()->create_column();
        int64_t read = 0;
        ASSERT_TRUE(reader->read(rows, output, &read).ok());
        ASSERT_EQ(read, rows);
        for (int64_t row = 0; row < rows; ++row) {
            EXPECT_EQ((*output)[row].get<TYPE_INT>(), row + 1);
        }
        reader->flush_profile();
        expect_cache_counters(counters, enabled, repeat);
    }
}

TEST(ParquetColumnReaderContextTest, ExplicitCachePolicyOverridesRuntimeOptions) {
    test::ScopedTempDirectory cache_identity("native-context-cache");
    auto file =
            std::make_shared<EncryptionMemoryReader>(java_fixture("iceberg_java_plain.parquet"));
    ParquetFileContext context;
    io::FileDescription description;
    ASSERT_TRUE(context.open(file, nullptr, false, description).ok());
    std::vector<std::unique_ptr<ParquetColumnSchema>> schema;
    ASSERT_TRUE(build_parquet_column_schema(context.native_metadata->schema(), &schema).ok());
    TQueryOptions disabled_options;
    disabled_options.__set_enable_parquet_file_page_cache(false);
    RuntimeState disabled_state {disabled_options, TQueryGlobals()};
    TQueryOptions enabled_options;
    enabled_options.__set_enable_parquet_file_page_cache(true);
    RuntimeState enabled_state {enabled_options, TQueryGlobals()};
    for (bool enabled : {false, true}) {
        SCOPED_TRACE(enabled);
        const auto key = (cache_identity.path() / std::to_string(enabled)).string();
        verify_explicit_cache_policy(file, context.native_metadata, *schema[0], nullptr, enabled,
                                     key + "-no-state");
        verify_explicit_cache_policy(file, context.native_metadata, *schema[0], &disabled_state,
                                     enabled, key + "-session-off");
        verify_explicit_cache_policy(file, context.native_metadata, *schema[0], &enabled_state,
                                     enabled, key + "-session-on");
    }
    ASSERT_TRUE(context.close().ok());
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

TEST(ParquetEncryptionTest, NativeContainerBudgetDoesNotTrustWireElementTypes) {
    using namespace apache::thrift::protocol;
    constexpr uint32_t values_per_container = 4096;
    for (const auto container_type : {T_LIST, T_SET, T_MAP}) {
        SCOPED_TRACE(container_type);
        const size_t elements_per_entry = container_type == T_MAP ? 2 : 1;
        const size_t container_count =
                MAX_ENCRYPTED_PARQUET_MODULE_BYTES / (values_per_container * elements_per_entry *
                                                      sizeof(tparquet::ColumnChunk)) +
                1;
        auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
        TCompactProtocol writer(transport);
        writer.writeStructBegin("FileMetaData");
        writer.writeFieldBegin("version", T_I32, 1);
        writer.writeI32(1);
        writer.writeFieldEnd();
        writer.writeFieldBegin("schema", T_LIST, 2);
        writer.writeListBegin(T_STRUCT, 0);
        writer.writeListEnd();
        writer.writeFieldEnd();
        writer.writeFieldBegin("num_rows", T_I64, 3);
        writer.writeI64(0);
        writer.writeFieldEnd();
        writer.writeFieldBegin("row_groups", T_LIST, 4);
        writer.writeListBegin(T_STRUCT, 0);
        writer.writeListEnd();
        writer.writeFieldEnd();
        // Skippable fields exercise cumulative accounting without allocating large native vectors.
        // Generated readers can resize struct vectors regardless of the advertised element type.
        for (size_t field = 0; field < container_count; ++field) {
            writer.writeFieldBegin("unknown", container_type, static_cast<int16_t>(100 + field));
            if (container_type == T_LIST) {
                writer.writeListBegin(T_BYTE, values_per_container);
            } else if (container_type == T_SET) {
                writer.writeSetBegin(T_BYTE, values_per_container);
            } else {
                writer.writeMapBegin(T_BYTE, T_BYTE, values_per_container);
            }
            for (uint32_t value = 0; value < values_per_container; ++value) {
                writer.writeByte(0);
                if (container_type == T_MAP) {
                    writer.writeByte(0);
                }
            }
            if (container_type == T_LIST) {
                writer.writeListEnd();
            } else if (container_type == T_SET) {
                writer.writeSetEnd();
            } else {
                writer.writeMapEnd();
            }
            writer.writeFieldEnd();
        }
        writer.writeFieldStop();
        writer.writeStructEnd();
        uint8_t* bytes = nullptr;
        uint32_t size = 0;
        transport->getBuffer(&bytes, &size);
        ASSERT_LT(size, 1 << 20);
        tparquet::FileMetaData parsed;
        uint32_t consumed = 0;
        auto status = deserialize_crypto_metadata(std::span(bytes, size), &consumed, &parsed);
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(consumed, 0);
    }
}

tparquet::FileCryptoMetaData crypto_metadata() {
    tparquet::AesGcmV1 aes;
    aes.__set_aad_file_unique("12345678");
    aes.__set_supply_aad_prefix(true);
    tparquet::EncryptionAlgorithm algorithm;
    algorithm.__set_AES_GCM_V1(aes);
    tparquet::FileCryptoMetaData metadata;
    metadata.__set_encryption_algorithm(algorithm);
    return metadata;
}

TEST(ParquetEncryptionTest, DescriptorFieldsAreValidatedBeforeCryptoSetup) {
    const auto metadata = crypto_metadata();
    for (int failure = 0; failure < 6; ++failure) {
        auto info = parameters();
        switch (failure) {
        case 0:
            info.__isset.version = false;
            break;
        case 1:
            info.version = 9;
            break;
        case 2:
            info.__isset.key = false;
            break;
        case 3:
            info.key.resize(33);
            break;
        case 4:
            info.__isset.aad_prefix = false;
            break;
        case 5:
            info.aad_prefix.resize(65537);
            break;
        }
        std::shared_ptr<ParquetFileCrypto> crypto;
        auto status = ParquetFileCrypto::create(info, metadata, &crypto);
        EXPECT_NE(status.to_string().find("[INVALID_DESCRIPTOR]"), std::string::npos) << status;
        EXPECT_EQ(crypto, nullptr);
    }
}

TEST(ParquetEncryptionTest, CipherAndFileAadPoliciesRejectUnsupportedMetadata) {
    for (int failure = 0; failure < 5; ++failure) {
        auto metadata = crypto_metadata();
        auto info = parameters();
        const char* expected = "[INVALID_FILE_AAD]";
        switch (failure) {
        case 0:
            metadata.encryption_algorithm.__isset.AES_GCM_V1 = false;
            expected = "[UNSUPPORTED_CIPHER]";
            break;
        case 1:
            metadata.encryption_algorithm.__set_AES_GCM_CTR_V1(tparquet::AesGcmCtrV1());
            expected = "[UNSUPPORTED_CIPHER]";
            break;
        case 2:
            metadata.encryption_algorithm.AES_GCM_V1.__isset.aad_file_unique = false;
            break;
        case 3:
            metadata.encryption_algorithm.AES_GCM_V1.aad_file_unique.resize(7);
            break;
        case 4:
            info.aad_prefix.clear();
            expected = "[MISSING_AAD]";
            break;
        }
        std::shared_ptr<ParquetFileCrypto> crypto;
        auto status = ParquetFileCrypto::create(info, metadata, &crypto);
        EXPECT_NE(status.to_string().find(expected), std::string::npos) << status;
        EXPECT_EQ(crypto, nullptr);
    }
}

TEST(ParquetEncryptionTest, StoredAndExternalAadPrefixesCannotDisagree) {
    auto metadata = crypto_metadata();
    metadata.encryption_algorithm.AES_GCM_V1.__set_aad_prefix("different-file-aad");
    std::shared_ptr<ParquetFileCrypto> crypto;
    auto status = ParquetFileCrypto::create(parameters(), metadata, &crypto);
    EXPECT_NE(status.to_string().find("[AAD_MISMATCH]"), std::string::npos) << status;
    metadata.encryption_algorithm.AES_GCM_V1.__set_aad_prefix("");
    metadata.encryption_algorithm.AES_GCM_V1.__set_supply_aad_prefix(false);
    status = ParquetFileCrypto::create(parameters(), metadata, &crypto);
    EXPECT_NE(status.to_string().find("[AAD_MISMATCH]"), std::string::npos) << status;
}

TEST(ParquetEncryptionTest, PlaintextFilesRejectEncryptionParameters) {
    auto bytes = java_fixture("iceberg_java_plain.parquet");
    auto info = parameters();
    ParquetFileContext context;
    io::FileDescription description;
    auto status = context.open(std::make_shared<EncryptionMemoryReader>(bytes), nullptr, true,
                               description, false, false, false, &info);
    EXPECT_NE(status.to_string().find("[ENCRYPTED_FOOTER_REQUIRED]"), std::string::npos) << status;
    ParquetFileContext plain;
    ASSERT_TRUE(
            plain.open(std::make_shared<EncryptionMemoryReader>(bytes), nullptr, true, description)
                    .ok());
    EXPECT_EQ(plain.native_metadata->to_thrift().num_rows, 8);
    EXPECT_EQ(plain.native_metadata->encryption(), nullptr);
    ASSERT_TRUE(plain.close().ok());
}

TEST(ParquetEncryptionTest, SeparateColumnKeysAndOrdinalBoundsAreRejected) {
    tparquet::ColumnChunk column;
    EXPECT_TRUE(ParquetFileCrypto::validate_column(column, 0, 0).ok());
    tparquet::ColumnCryptoMetaData crypto;
    crypto.__set_ENCRYPTION_WITH_FOOTER_KEY(tparquet::EncryptionWithFooterKey());
    column.__set_crypto_metadata(crypto);
    EXPECT_TRUE(ParquetFileCrypto::validate_column(column, 32767, 32767).ok());
    for (auto [row, col] :
         std::vector<std::pair<int, int>> {{-1, 0}, {0, -1}, {32768, 0}, {0, 32768}}) {
        auto status = ParquetFileCrypto::validate_column(column, row, col);
        EXPECT_NE(status.to_string().find("[ORDINAL_OUT_OF_RANGE]"), std::string::npos) << status;
    }
    column.crypto_metadata.__set_ENCRYPTION_WITH_COLUMN_KEY(tparquet::EncryptionWithColumnKey());
    auto status = ParquetFileCrypto::validate_column(column, 0, 0);
    EXPECT_NE(status.to_string().find("[COLUMN_KEY_UNSUPPORTED]"), std::string::npos) << status;
    column.__isset.crypto_metadata = false;
    column.__set_encrypted_column_metadata("opaque-encrypted-column-metadata");
    status = ParquetFileCrypto::validate_column(column, 0, 0);
    EXPECT_NE(status.to_string().find("[COLUMN_KEY_UNSUPPORTED]"), std::string::npos) << status;
}

TEST(ParquetEncryptionTest, OversizedFooterLengthIsRejectedBeforeAnAdditionalRead) {
    auto bytes = java_fixture();
    std::fill(bytes.end() - 8, bytes.end() - 4, 0xff);
    auto file = std::make_shared<EncryptionMemoryReader>(std::move(bytes));
    auto info = parameters();
    ParquetFileContext context;
    io::FileDescription description;
    EXPECT_FALSE(context.open(file, nullptr, false, description, false, false, false, &info).ok());
    EXPECT_EQ(file->read_count(), 1);
}

#if __has_include(<parquet/encryption/encryption_internal.h>)
std::vector<uint8_t> encrypted_scratch_file(::parquet::ParquetDataPageVersion version,
                                            ::parquet::Compression::type compression,
                                            const std::string& large, int small_rows) {
    auto sink = ::arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto schema = ::parquet::schema::GroupNode::Make(
            "schema", ::parquet::Repetition::REQUIRED,
            {::parquet::schema::PrimitiveNode::Make("value", ::parquet::Repetition::OPTIONAL,
                                                    ::parquet::Type::BYTE_ARRAY)});
    ::parquet::FileEncryptionProperties::Builder encryption {
            ::arrow::util::SecureString(std::string(KEY))};
    encryption.aad_prefix(AAD)->disable_aad_prefix_storage();
    ::parquet::WriterProperties::Builder properties;
    properties.encryption(encryption.build())
            ->compression(compression)
            ->data_page_version(version)
            ->disable_dictionary()
            ->data_pagesize(4096)
            ->write_batch_size(2);
    auto writer = ::parquet::ParquetFileWriter::Open(
            sink, std::static_pointer_cast<::parquet::schema::GroupNode>(schema),
            properties.build());
    auto* group = writer->AppendRowGroup();
    auto* column = static_cast<::parquet::ByteArrayWriter*>(group->NextColumn());
    const std::string first_small(256, 'a');
    const ::parquet::ByteArray first_values[] = {
            {static_cast<uint32_t>(large.size()), reinterpret_cast<const uint8_t*>(large.data())},
            {static_cast<uint32_t>(first_small.size()),
             reinterpret_cast<const uint8_t*>(first_small.data())}};
    const int16_t first_levels[] = {1, 1};
    column->WriteBatch(2, first_levels, nullptr, first_values);
    for (int row = 1; row < small_rows; ++row) {
        const std::string small(256, static_cast<char>('a' + row % 26));
        const ::parquet::ByteArray value(small.size(),
                                         reinterpret_cast<const uint8_t*>(small.data()));
        const int16_t defined = row % 5 == 4 ? 0 : 1;
        column->WriteBatch(1, &defined, nullptr, &value);
    }
    group->Close();
    writer->Close();
    auto bytes = sink->Finish().ValueOrDie();
    return {bytes->data(), bytes->data() + bytes->size()};
}

// Assertion macros expand the page-lifetime checks into many branches.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
void verify_decrypted_page_scratch(::parquet::ParquetDataPageVersion version,
                                   ::parquet::Compression::type compression,
                                   const std::string& large) {
    const size_t large_bytes = large.size();
    constexpr size_t retained_limit = 4UL << 20;
    constexpr int small_rows = 64;
    auto file = std::make_shared<EncryptionMemoryReader>(
            encrypted_scratch_file(version, compression, large, small_rows));
    ParquetFileContext context;
    io::FileDescription description;
    const auto info = parameters();
    ASSERT_TRUE(context.open(file, nullptr, false, description, false, false, false, &info).ok());
    auto field = *context.native_metadata->schema().get_column(0);
    const auto& chunk = context.native_metadata->to_thrift().row_groups[0].columns[0];
    std::shared_ptr<ParquetColumnCrypto> crypto;
    ASSERT_TRUE(context.native_metadata->encryption()->column_crypto(chunk, 0, 0, &crypto).ok());
    const auto ranges = segment_v2::RowRanges::create_single(0, small_rows + 1);
    native::ScalarColumnReader<false, false> reader(ranges, small_rows + 1, chunk, nullptr, nullptr,
                                                    nullptr);
    ASSERT_TRUE(reader.init(file, &field, {}, std::move(crypto)).ok());
    const auto& page_reader = *reader._chunk_reader->_page_reader;
    native::FilterMap filter;
    ASSERT_TRUE(filter.init(nullptr, 1, false).ok());
    for (int row = -1; row < small_rows; ++row) {
        ColumnPtr output = field.data_type->create_column();
        size_t read = 0;
        bool eof = false;
        auto status = reader.read_column_data(output, field.data_type, nullptr, filter, 1, &read,
                                              &eof, false);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(read, 1);
        ASSERT_EQ(output->size(), 1);
        if (row == -1) {
            EXPECT_EQ((*output)[0].get<TYPE_STRING>(), large);
            EXPECT_EQ(page_reader.active_scratch_bytes(), page_reader._decrypted_page.size());
            // The next value still uses the large page; active bytes must prevent idle probes
            // from scheduling a release that would defeat steady-state page reuse.
            for (int probe = 0; probe < 3; ++probe) {
                reader.release_batch_scratch(retained_limit);
            }
            EXPECT_GE(reader.retained_batch_scratch_bytes_for_test(), large_bytes);
        } else if (row % 5 == 4) {
            EXPECT_TRUE((*output)[0].is_null());
        } else {
            EXPECT_EQ((*output)[0].get<TYPE_STRING>(),
                      std::string(256, static_cast<char>('a' + row % 26)));
        }
        if (row == 0) {
            // Retiring a page must not force value-initialization of its reusable bytes next time.
            EXPECT_GE(page_reader._decrypted_page.size(), large_bytes);
            EXPECT_EQ(page_reader.active_scratch_bytes(), 0);
        }
        if (row >= 0 && row <= 2) {
            reader.release_batch_scratch(retained_limit);
            // V2 levels and uncompressed values still borrow this small page after the third
            // idle probe; the old large capacity can be freed only when this page is exhausted.
            EXPECT_GE(reader.retained_batch_scratch_bytes_for_test(), large_bytes);
        }
    }
    EXPECT_LE(reader.retained_batch_scratch_bytes_for_test(), retained_limit);
    if (compression == ::parquet::Compression::SNAPPY) {
        EXPECT_GT(reader.column_statistics().decompress_cnt, 0);
    }
    ASSERT_TRUE(context.close().ok());
}

TEST(ParquetEncryptionTest, DecryptedPageScratchFollowsScalarReclamation) {
    std::string large(32UL << 20, '\0');
    uint32_t state = 0x12345678;
    for (char& value : large) {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        value = static_cast<char>(state);
    }
    // Keep the compressed body large as well, so both codecs exercise decrypted capacity.
    for (auto version :
         {::parquet::ParquetDataPageVersion::V1, ::parquet::ParquetDataPageVersion::V2}) {
        for (auto compression :
             {::parquet::Compression::UNCOMPRESSED, ::parquet::Compression::SNAPPY}) {
            SCOPED_TRACE(testing::Message() << static_cast<int>(version) << "/" << compression);
            verify_decrypted_page_scratch(version, compression, large);
        }
    }
}

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
TEST(ParquetEncryptionTest, PlaintextFooterEncryptionIsExplicitlyUnsupported) {
    auto bytes = encrypted_file(true, ::parquet::ParquetDataPageVersion::V1, KEY, true);
    ParquetFileContext context;
    io::FileDescription description;
    auto status = context.open(std::make_shared<EncryptionMemoryReader>(bytes), nullptr, true,
                               description);
    EXPECT_NE(status.to_string().find("[PLAINTEXT_FOOTER_UNSUPPORTED]"), std::string::npos)
            << status;
}

TEST(ParquetEncryptionTest, WrongNonemptyFileAadFailsAuthentication) {
    std::vector<std::vector<Field>> rows;
    auto status = native_rows(encrypted_file(), parameters(KEY, "different-file-aad"), &rows);
    EXPECT_NE(status.to_string().find("[DECRYPTION_FAILED]"), std::string::npos) << status;
    EXPECT_EQ(status.to_string().find("different-file-aad"), std::string::npos);
}

TEST(ParquetEncryptionTest, EncryptedModuleBoundsAreCheckedBeforeAllocation) {
    auto info = parameters();
    std::shared_ptr<ParquetFileCrypto> file;
    ASSERT_TRUE(ParquetFileCrypto::create(info, crypto_metadata(), &file).ok());
    tparquet::ColumnChunk column;
    tparquet::ColumnCryptoMetaData metadata;
    metadata.__set_ENCRYPTION_WITH_FOOTER_KEY(tparquet::EncryptionWithFooterKey());
    column.__set_crypto_metadata(metadata);
    std::shared_ptr<ParquetColumnCrypto> crypto;
    ASSERT_TRUE(file->column_crypto(column, 0, 0, &crypto).ok());
    for (int32_t length :
         {-1, 0, 31, static_cast<int32_t>(MAX_ENCRYPTED_PARQUET_MODULE_BYTES) + 1}) {
        int32_t plaintext = -1;
        auto status = crypto->plaintext_size(length, &plaintext);
        EXPECT_NE(status.to_string().find("[INVALID_MODULE_SIZE]"), std::string::npos) << status;
    }
    int32_t plaintext = 0;
    EXPECT_TRUE(crypto->plaintext_size(MAX_ENCRYPTED_PARQUET_MODULE_BYTES, &plaintext).ok());
    EXPECT_EQ(plaintext, MAX_ENCRYPTED_PARQUET_MODULE_BYTES - 32);
    DorisVector<uint8_t> output;
    auto status = crypto->decrypt_header(std::vector<uint8_t> {0xff, 0xff, 0xff, 0xff}, false, 0,
                                         &output);
    EXPECT_NE(status.to_string().find("[INVALID_MODULE_SIZE]"), std::string::npos) << status;
    EXPECT_TRUE(output.empty());
    for (int32_t page : {-1, 32768}) {
        status = crypto->decrypt_header({}, false, page, &output);
        EXPECT_NE(status.to_string().find("[ORDINAL_OUT_OF_RANGE]"), std::string::npos) << status;
    }
}

// Assertion macros expand the module/row/column/page authentication checks into many branches.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(ParquetEncryptionTest, AuthenticationBindsHeadersToModulesAndPhysicalOrdinals) {
    auto bytes = encrypted_file();
    auto info = parameters();
    ParquetFileContext context;
    io::FileDescription description;
    ASSERT_TRUE(context.open(std::make_shared<EncryptionMemoryReader>(bytes), nullptr, false,
                             description, false, false, false, &info)
                        .ok());
    const auto& column = context.native_metadata->to_thrift().row_groups[0].columns[0];
    size_t offset = column.meta_data.data_page_offset;
    size_t length = decode_fixed32_le(bytes.data() + offset) + 4;
    const auto header = std::span(bytes).subspan(offset, length);
    auto file = context.native_metadata->encryption();
    std::shared_ptr<ParquetColumnCrypto> first;
    std::shared_ptr<ParquetColumnCrypto> second;
    ASSERT_TRUE(file->column_crypto(column, 0, 0, &first).ok());
    ASSERT_TRUE(file->column_crypto(column, 0, 0, &second).ok());
    EXPECT_NE(first.get(), second.get());
    DorisVector<uint8_t> expected;
    ASSERT_TRUE(first->decrypt_header(header, false, 0, &expected).ok());
    DorisVector<uint8_t> output;
    EXPECT_FALSE(first->decrypt_header(header, true, 0, &output).ok());
    EXPECT_FALSE(first->decrypt_header(header, false, 1, &output).ok());
    EXPECT_FALSE(first->decrypt_page(header, false, 0, &output).ok());
    for (auto [row, col] : std::vector<std::pair<int, int>> {{1, 0}, {0, 1}}) {
        std::shared_ptr<ParquetColumnCrypto> other;
        ASSERT_TRUE(file->column_crypto(column, row, col, &other).ok());
        EXPECT_FALSE(other->decrypt_header(header, false, 0, &output).ok());
    }
    // A failed or interleaved decryptor must not contaminate another reader sharing the file key.
    ASSERT_TRUE(second->decrypt_header(header, false, 0, &output).ok());
    EXPECT_EQ(output, expected);
    ASSERT_TRUE(first->decrypt_header(header, false, 0, &output).ok());
    EXPECT_EQ(output, expected);
    ASSERT_TRUE(context.close().ok());
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
