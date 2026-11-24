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
#include <parquet/arrow/reader.h>
#include <parquet/bloom_filter.h>
#include <parquet/bloom_filter_reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <tuple>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "core/block/block.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "format/transformer/vparquet_writer.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/timezone_utils.h"
#include "util/uid_util.h"

namespace doris {
namespace {

// -1 means an old FE that omits the optional field; dictionary mode 1 forces fallback.
class ParquetBloomWriterFixture : public testing::Test {
protected:
    static constexpr int ROWS = 16384;
    static void SetUpTestSuite() { TimezoneUtils::load_timezones_to_cache(); }

    void SetUp() override {
        _path = "./parquet_bloom_" + UniqueId::gen_uid().to_string() + ".parquet";
        _old_pool = ExecEnv::GetInstance()->_arrow_memory_pool;
        _pool = std::make_unique<arrow::ProxyMemoryPool>(arrow::default_memory_pool());
        ExecEnv::GetInstance()->_arrow_memory_pool = _pool.get();
        _old_row_group_size = config::min_row_group_size;
    }

    void TearDown() override {
        config::min_row_group_size = _old_row_group_size;
        ExecEnv::GetInstance()->_arrow_memory_pool = _old_pool;
        EXPECT_EQ(0, _pool->bytes_allocated());
        EXPECT_TRUE(io::global_local_filesystem()->delete_file(_path).ok());
    }

    static std::string key(int index, int dictionary_mode) {
        if (index % 37 == 0) {
            return {"x\0y", 3};
        }
        return dictionary_mode == 0 ? std::to_string(index % 8)
                                    : std::to_string(index) + std::string(128, 'k');
    }

    static double floating_value(int index) {
        switch (index % 16) {
        case 0:
            return -0.0;
        case 1:
            return 0.0;
        case 2:
            return std::bit_cast<double>(uint64_t {0x7ff8000000000001});
        case 3:
            return std::bit_cast<double>(uint64_t {0x7ff8000000000002});
        case 4:
            return std::numeric_limits<double>::infinity();
        case 5:
            return -std::numeric_limits<double>::infinity();
        default:
            return index + 0.5;
        }
    }

    static Block make_block(int offset, int rows, int dictionary_mode) {
        DataTypes types {std::make_shared<DataTypeInt32>(),
                         std::make_shared<DataTypeFloat32>(),
                         std::make_shared<DataTypeFloat64>(),
                         make_nullable(std::make_shared<DataTypeString>()),
                         std::make_shared<DataTypeUInt8>(),
                         make_nullable(std::make_shared<DataTypeString>()),
                         std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>())};
        std::vector<std::string> names {"id", "f", "d", "key", "flag", "all_null", "items"};
        MutableColumns columns;
        for (const auto& type : types) {
            columns.push_back(type->create_column());
        }
        for (int row = 0; row < rows; ++row) {
            const int index = offset + row;
            columns[0]->insert(Field::create_field<TYPE_INT>(2 * index));
            columns[1]->insert(
                    Field::create_field<TYPE_FLOAT>(static_cast<float>(floating_value(index))));
            columns[2]->insert(Field::create_field<TYPE_DOUBLE>(floating_value(index)));
            if (index % 29 == 0) {
                columns[3]->insert_default();
            } else {
                columns[3]->insert(Field::create_field<TYPE_STRING>(key(index, dictionary_mode)));
            }
            columns[4]->insert(Field::create_field<TYPE_BOOLEAN>(index % 2));
            columns[5]->insert_default();
            Array item {Field::create_field<TYPE_INT>(index)};
            columns[6]->insert(Field::create_field<TYPE_ARRAY>(item));
        }
        Block block;
        for (size_t c = 0; c < types.size(); ++c) {
            block.insert({std::move(columns[c]), types[c], names[c]});
        }
        return block;
    }

    void check_file(int rows, int groups, bool enabled, int dictionary_mode) {
        auto input = arrow::io::ReadableFile::Open(_path).ValueOrDie();
        auto reader = ::parquet::ParquetFileReader::Open(input);
        ASSERT_EQ(groups, reader->metadata()->num_row_groups());
        for (int group = 0; group < groups; ++group) {
            check_group(*reader, group, rows, enabled, dictionary_mode);
        }
        check_round_trip(input, rows * groups, dictionary_mode);
    }

    static void check_group(::parquet::ParquetFileReader& reader, int group, int rows, bool enabled,
                            int dictionary_mode) {
        auto metadata = reader.metadata()->RowGroup(group);
        auto filters = reader.GetBloomFilterReader().RowGroup(group);
        for (int c = 0; c < metadata->num_columns(); ++c) {
            auto chunk = metadata->ColumnChunk(c);
            auto bloom = filters->GetColumnBloomFilter(c);
            check_column(*chunk, bloom.get(), c, group * rows, rows, enabled, dictionary_mode);
        }
        auto key_column = metadata->ColumnChunk(3);
        EXPECT_EQ(dictionary_mode != 2, key_column->has_dictionary_page());
        if (dictionary_mode == 1) {
            EXPECT_TRUE(std::ranges::any_of(key_column->encoding_stats(), [](const auto& stat) {
                return stat.page_type == ::parquet::PageType::DATA_PAGE &&
                       stat.encoding == ::parquet::Encoding::PLAIN && stat.count > 0;
            }));
        }
    }

    static void check_column(const ::parquet::ColumnChunkMetaData& chunk,
                             const ::parquet::BloomFilter* bloom, int column, int offset, int rows,
                             bool enabled, int dictionary_mode) {
        const bool expected = enabled && column != 4;
        EXPECT_EQ(expected, chunk.bloom_filter_offset().has_value());
        EXPECT_EQ(expected, chunk.bloom_filter_length().has_value());
        ASSERT_EQ(expected, bloom != nullptr);
        if (expected) {
            EXPECT_GT(*chunk.bloom_filter_offset(), 0);
            EXPECT_GT(*chunk.bloom_filter_length(), 32);
            check_membership(*bloom, column, offset, rows, dictionary_mode);
        }
    }

    static void check_round_trip(const std::shared_ptr<arrow::io::ReadableFile>& input, int rows,
                                 int dictionary_mode) {
        auto reader = ::parquet::arrow::OpenFile(input, arrow::default_memory_pool()).ValueOrDie();
        auto table = reader->ReadTable().ValueOrDie()->CombineChunks().ValueOrDie();
        ASSERT_EQ(rows, table->num_rows());
        const auto& ids = static_cast<const arrow::Int32Array&>(*table->column(0)->chunk(0));
        const auto& keys = static_cast<const arrow::BinaryArray&>(*table->column(3)->chunk(0));
        for (int index = 0; index < rows; ++index) {
            EXPECT_EQ(2 * index, ids.Value(index));
            EXPECT_EQ(index % 29 == 0, keys.IsNull(index));
            if (!keys.IsNull(index)) {
                EXPECT_EQ(key(index, dictionary_mode), keys.GetView(index));
            }
        }
    }

    static void check_membership(const ::parquet::BloomFilter& bloom, int column, int offset,
                                 int rows, int dictionary_mode) {
        for (int index = offset; index < offset + rows; ++index) {
            uint64_t hash = 0;
            switch (column) {
            case 0:
                hash = bloom.Hash(static_cast<int32_t>(2 * index));
                break;
            case 1:
                hash = bloom.Hash(static_cast<float>(floating_value(index)));
                break;
            case 2:
                hash = bloom.Hash(floating_value(index));
                break;
            case 3: {
                if (index % 29 == 0) {
                    continue;
                }
                const auto value = key(index, dictionary_mode);
                const ::parquet::ByteArray bytes(static_cast<uint32_t>(value.size()),
                                                 reinterpret_cast<const uint8_t*>(value.data()));
                hash = bloom.Hash(bytes);
                break;
            }
            case 5:
                EXPECT_FALSE(bloom.FindHash(bloom.Hash(static_cast<int32_t>(index))));
                continue;
            case 6:
                hash = bloom.Hash(static_cast<int32_t>(index));
                break;
            default:
                FAIL() << "Unexpected column " << column;
            }
            ASSERT_TRUE(bloom.FindHash(hash)) << "column=" << column << ", index=" << index;
        }
    }

    void check_timestamp_encoding(bool int96) {
        TQueryOptions query_options;
        query_options.__set_enable_parquet_write_bloom_filter(true);
        RuntimeState state(query_options, TQueryGlobals());
        state.set_timezone("UTC");
        auto type = DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false, 0, 6);
        auto column = type->create_column();
        DateV2Value<DateTimeV2ValueType> value;
        value.unchecked_set_time(2023, 4, 20, 0, 0, 0, 123456);
        column->insert(Field::create_field<TYPE_DATETIMEV2>(value.to_date_int_val()));
        Block block {{std::move(column), type, "value"}};
        auto exprs = MockSlotRef::create_mock_contexts(block.get_data_types());
        io::FileWriterPtr file;
        ASSERT_TRUE(io::global_local_filesystem()->create_file(_path, &file).ok());
        ParquetFileOptions options {.compression_type = TParquetCompressionType::SNAPPY,
                                    .parquet_version = TParquetVersion::PARQUET_1_0,
                                    .enable_int96_timestamps = int96};
        {
            VParquetWriter writer(&state, file.get(), exprs, std::vector<std::string> {"value"},
                                  false, options);
            ASSERT_TRUE(writer.open().ok());
            ASSERT_TRUE(writer.write(block).ok());
            ASSERT_TRUE(writer.close().ok());
        }
        auto reader = ::parquet::ParquetFileReader::OpenFile(_path, false);
        auto chunk = reader->metadata()->RowGroup(0)->ColumnChunk(0);
        EXPECT_EQ(int96 ? ::parquet::Type::INT96 : ::parquet::Type::INT64, chunk->type());
        EXPECT_EQ(!int96, chunk->bloom_filter_offset().has_value());
        if (!int96) {
            auto filter = reader->GetBloomFilterReader().RowGroup(0)->GetColumnBloomFilter(0);
            ASSERT_NE(nullptr, filter);
            EXPECT_TRUE(filter->FindHash(filter->Hash(int64_t {1681948800123456LL})));
        }
    }

    arrow::MemoryPool* _old_pool = nullptr;
    std::unique_ptr<arrow::ProxyMemoryPool> _pool;
    int64_t _old_row_group_size = 0;
    std::string _path;
};

class ParquetBloomWriterTest
        : public ParquetBloomWriterFixture,
          public testing::WithParamInterface<std::tuple<int, int, bool, bool>> {};

class BloomFailureFileWriter final : public io::FileWriter {
public:
    Status close(bool = false) override {
        _state = State::CLOSED;
        return Status::OK();
    }
    Status appendv(const Slice* data, size_t count) override {
        const ::parquet::BloomFilterOptions options;
        const auto bloom_size =
                ::parquet::BlockSplitBloomFilter::OptimalNumOfBytes(options.ndv, options.fpp);
        for (size_t i = 0; i < count; ++i) {
            // The test's tiny data pages are smaller than the default Bloom payload.
            if (data[i].size == bloom_size) {
                failed_on_bloom = true;
                return Status::IOError("injected Bloom write failure");
            }
            _bytes += data[i].size;
        }
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t bytes_appended() const override { return _bytes; }
    State state() const override { return _state; }
    bool failed_on_bloom = false;

private:
    io::Path _path {"bloom_failure.parquet"};
    size_t _bytes = 0;
    State _state = State::OPENED;
};

class BloomFailureMemoryPool final : public arrow::ProxyMemoryPool {
public:
    BloomFailureMemoryPool() : arrow::ProxyMemoryPool(arrow::default_memory_pool()) {}

    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
        const ::parquet::BloomFilterOptions options;
        const auto bloom_size =
                ::parquet::BlockSplitBloomFilter::OptimalNumOfBytes(options.ndv, options.fpp);
        // Reject the Bloom bitset rather than the tiny test's ordinary encoding buffers.
        if (std::cmp_equal(size, bloom_size)) {
            rejected_bloom = true;
            return arrow::Status::OutOfMemory("injected Bloom allocation failure");
        }
        return arrow::ProxyMemoryPool::Allocate(size, alignment, out);
    }

    bool rejected_bloom = false;
};

class BloomThrowingMemoryPool final : public arrow::ProxyMemoryPool {
public:
    BloomThrowingMemoryPool() : arrow::ProxyMemoryPool(arrow::default_memory_pool()) {}

    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
        const ::parquet::BloomFilterOptions options;
        const auto bloom_size =
                ::parquet::BlockSplitBloomFilter::OptimalNumOfBytes(options.ndv, options.fpp);
        if (std::cmp_equal(size, bloom_size) && !rejected_bloom) {
            rejected_bloom = true;
            observed_catch_scope = enable_thread_catch_bad_alloc > 0;
            throw Exception(ErrorCode::MEM_ALLOC_FAILED, "injected Doris Bloom allocation failure");
        }
        return arrow::ProxyMemoryPool::Allocate(size, alignment, out);
    }

    bool rejected_bloom = false;
    bool observed_catch_scope = false;
};

TEST_P(ParquetBloomWriterTest, MetadataMembershipAndRoundTrip) {
    const auto [enabled, dictionary_mode, latest, multiple_groups] = GetParam();
    TQueryOptions query_options;
    if (enabled >= 0) {
        query_options.__set_enable_parquet_write_bloom_filter(enabled != 0);
    }
    RuntimeState state(query_options, TQueryGlobals());
    state.set_timezone("UTC");
    auto sample = make_block(0, 0, dictionary_mode);
    auto exprs = MockSlotRef::create_mock_contexts(sample.get_data_types());
    std::vector<std::string> names {"id", "f", "d", "key", "flag", "all_null", "items"};
    io::FileWriterPtr file;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(_path, &file).ok());
    ParquetFileOptions options {.compression_type = TParquetCompressionType::SNAPPY,
                                .parquet_version = latest ? TParquetVersion::PARQUET_2_LATEST
                                                          : TParquetVersion::PARQUET_1_0,
                                .parquet_disable_dictionary = dictionary_mode == 2};
    {
        VParquetWriter writer(&state, file.get(), exprs, names, false, options);
        ASSERT_TRUE(writer.open().ok());
        const int groups = multiple_groups ? 2 : 1;
        const int batches = multiple_groups ? 1 : 4;
        config::min_row_group_size = multiple_groups ? 0 : std::numeric_limits<int64_t>::max();
        for (int group = 0; group < groups; ++group) {
            for (int batch = 0; batch < batches; ++batch) {
                auto block = make_block(group * ROWS + batch * (ROWS / batches), ROWS / batches,
                                        dictionary_mode);
                ASSERT_TRUE(writer.write(block).ok());
            }
        }
        ASSERT_TRUE(writer.close().ok());
        EXPECT_EQ(io::FileWriter::State::CLOSED, file->state());
    }
    EXPECT_EQ(0, _pool->bytes_allocated());
    check_file(ROWS, multiple_groups ? 2 : 1, enabled == 1, dictionary_mode);
}

TEST_F(ParquetBloomWriterFixture, EmptyFileWithBloomEnabled) {
    TQueryOptions query_options;
    query_options.__set_enable_parquet_write_bloom_filter(true);
    RuntimeState state(query_options, TQueryGlobals());
    state.set_timezone("UTC");
    auto block = make_block(0, 0, 0);
    auto exprs = MockSlotRef::create_mock_contexts(block.get_data_types());
    io::FileWriterPtr file;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(_path, &file).ok());
    ParquetFileOptions options {.compression_type = TParquetCompressionType::SNAPPY,
                                .parquet_version = TParquetVersion::PARQUET_1_0};
    {
        VParquetWriter writer(
                &state, file.get(), exprs,
                std::vector<std::string> {"id", "f", "d", "key", "flag", "all_null", "items"},
                false, options);
        ASSERT_TRUE(writer.open().ok());
        ASSERT_TRUE(writer.write(block).ok());
        ASSERT_TRUE(writer.close().ok());
    }
    auto reader = ::parquet::ParquetFileReader::OpenFile(_path, false);
    EXPECT_EQ(0, reader->metadata()->num_rows());
    EXPECT_EQ(0, reader->metadata()->num_row_groups());
}

TEST_F(ParquetBloomWriterFixture, BooleanOnlyFileDoesNotEnableUnsupportedFilter) {
    TQueryOptions query_options;
    query_options.__set_enable_parquet_write_bloom_filter(true);
    RuntimeState state(query_options, TQueryGlobals());
    state.set_timezone("UTC");
    auto type = std::make_shared<DataTypeUInt8>();
    auto column = type->create_column();
    column->insert(Field::create_field<TYPE_BOOLEAN>(true));
    column->insert(Field::create_field<TYPE_BOOLEAN>(false));
    Block block {{std::move(column), type, "flag"}};
    auto exprs = MockSlotRef::create_mock_contexts(block.get_data_types());
    io::FileWriterPtr file;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(_path, &file).ok());
    ParquetFileOptions options {.compression_type = TParquetCompressionType::SNAPPY,
                                .parquet_version = TParquetVersion::PARQUET_1_0};
    {
        VParquetWriter writer(&state, file.get(), exprs, std::vector<std::string> {"flag"}, false,
                              options);
        ASSERT_TRUE(writer.open().ok());
        EXPECT_FALSE(writer._parquet_writer_properties->bloom_filter_enabled());
        ASSERT_TRUE(writer.write(block).ok());
        ASSERT_TRUE(writer.close().ok());
    }
    auto reader = ::parquet::ParquetFileReader::OpenFile(_path, false);
    EXPECT_FALSE(
            reader->metadata()->RowGroup(0)->ColumnChunk(0)->bloom_filter_offset().has_value());
}

TEST_F(ParquetBloomWriterFixture, BloomWriteFailurePropagatesAndReleasesMemory) {
    TQueryOptions query_options;
    query_options.__set_enable_parquet_write_bloom_filter(true);
    RuntimeState state(query_options, TQueryGlobals());
    state.set_timezone("UTC");
    auto block = make_block(0, 32, 0);
    auto exprs = MockSlotRef::create_mock_contexts(block.get_data_types());
    BloomFailureFileWriter file;
    ParquetFileOptions options {.compression_type = TParquetCompressionType::SNAPPY,
                                .parquet_version = TParquetVersion::PARQUET_1_0};
    {
        VParquetWriter writer(
                &state, &file, exprs,
                std::vector<std::string> {"id", "f", "d", "key", "flag", "all_null", "items"},
                false, options);
        ASSERT_TRUE(writer.open().ok());
        ASSERT_TRUE(writer.write(block).ok());
        auto status = writer.close();
        EXPECT_FALSE(status.ok());
        EXPECT_NE(std::string::npos, status.to_string().find("injected Bloom write failure"));
        EXPECT_TRUE(file.failed_on_bloom);
    }
    EXPECT_EQ(0, _pool->bytes_allocated());
}

TEST_F(ParquetBloomWriterFixture, BloomAllocationFailureReleasesMemory) {
    auto pool = std::make_unique<BloomFailureMemoryPool>();
    auto* failure_pool = pool.get();
    _pool = std::move(pool);
    ExecEnv::GetInstance()->_arrow_memory_pool = _pool.get();
    TQueryOptions query_options;
    query_options.__set_enable_parquet_write_bloom_filter(true);
    RuntimeState state(query_options, TQueryGlobals());
    state.set_timezone("UTC");
    auto block = make_block(0, 32, 0);
    auto exprs = MockSlotRef::create_mock_contexts(block.get_data_types());
    io::FileWriterPtr file;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(_path, &file).ok());
    ParquetFileOptions options {.compression_type = TParquetCompressionType::SNAPPY,
                                .parquet_version = TParquetVersion::PARQUET_1_0};
    {
        VParquetWriter writer(
                &state, file.get(), exprs,
                std::vector<std::string> {"id", "f", "d", "key", "flag", "all_null", "items"},
                false, options);
        ASSERT_TRUE(writer.open().ok());
        auto status = writer.write(block);
        EXPECT_FALSE(status.ok());
        EXPECT_TRUE(failure_pool->rejected_bloom);
        EXPECT_NE(std::string::npos, status.to_string().find("injected Bloom allocation failure"));
    }
    EXPECT_EQ(0, _pool->bytes_allocated());
}

TEST_F(ParquetBloomWriterFixture, DorisAllocationExceptionBecomesStatus) {
    auto pool = std::make_unique<BloomThrowingMemoryPool>();
    auto* failure_pool = pool.get();
    _pool = std::move(pool);
    ExecEnv::GetInstance()->_arrow_memory_pool = _pool.get();
    TQueryOptions query_options;
    query_options.__set_enable_parquet_write_bloom_filter(true);
    RuntimeState state(query_options, TQueryGlobals());
    state.set_timezone("UTC");
    auto block = make_block(0, 32, 0);
    auto exprs = MockSlotRef::create_mock_contexts(block.get_data_types());
    io::FileWriterPtr file;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(_path, &file).ok());
    ParquetFileOptions options {.compression_type = TParquetCompressionType::SNAPPY,
                                .parquet_version = TParquetVersion::PARQUET_1_0};
    const int previous_catch_scope = enable_thread_catch_bad_alloc;
    {
        VParquetWriter writer(
                &state, file.get(), exprs,
                std::vector<std::string> {"id", "f", "d", "key", "flag", "all_null", "items"},
                false, options);
        ASSERT_TRUE(writer.open().ok());
        Status status;
        EXPECT_NO_THROW(status = writer.write(block));
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(ErrorCode::MEM_LIMIT_EXCEEDED, status.code());
        EXPECT_NE(std::string::npos,
                  status.to_string().find("injected Doris Bloom allocation failure"));
        EXPECT_TRUE(failure_pool->observed_catch_scope);
    }
    EXPECT_EQ(previous_catch_scope, enable_thread_catch_bad_alloc);
    EXPECT_EQ(0, _pool->bytes_allocated());
}

TEST_F(ParquetBloomWriterFixture, Int64TimestampWritesBloom) {
    check_timestamp_encoding(false);
}

TEST_F(ParquetBloomWriterFixture, Int96TimestampOmitsBloom) {
    check_timestamp_encoding(true);
}

TEST_F(ParquetBloomWriterFixture, DecimalBloomUsesBigEndianPhysicalBytes) {
    TQueryOptions query_options;
    query_options.__set_enable_parquet_write_bloom_filter(true);
    RuntimeState state(query_options, TQueryGlobals());
    state.set_timezone("UTC");
    auto type = create_decimal(38, 2, false);
    auto column = type->create_column();
    for (int64_t value : {-257, 0, 257}) {
        column->insert(Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(value)));
    }
    Block block {{std::move(column), type, "value"}};
    auto exprs = MockSlotRef::create_mock_contexts(block.get_data_types());
    io::FileWriterPtr file;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(_path, &file).ok());
    ParquetFileOptions options {.compression_type = TParquetCompressionType::SNAPPY,
                                .parquet_version = TParquetVersion::PARQUET_1_0};
    {
        VParquetWriter writer(&state, file.get(), exprs, std::vector<std::string> {"value"}, false,
                              options);
        ASSERT_TRUE(writer.open().ok());
        ASSERT_TRUE(writer.write(block).ok());
        ASSERT_TRUE(writer.close().ok());
    }
    auto reader = ::parquet::ParquetFileReader::OpenFile(_path, false);
    ASSERT_EQ(::parquet::Type::FIXED_LEN_BYTE_ARRAY,
              reader->metadata()->schema()->Column(0)->physical_type());
    ASSERT_EQ(16, reader->metadata()->schema()->Column(0)->type_length());
    auto bloom = reader->GetBloomFilterReader().RowGroup(0)->GetColumnBloomFilter(0);
    ASSERT_NE(nullptr, bloom);
    for (int value : {-257, 0, 257}) {
        std::array<uint8_t, 16> bytes;
        bytes.fill(value < 0 ? 0xff : 0);
        bytes[14] = static_cast<uint8_t>(static_cast<uint16_t>(value) >> 8);
        bytes[15] = static_cast<uint8_t>(value);
        EXPECT_TRUE(bloom->FindHash(
                bloom->Hash(::parquet::FixedLenByteArray(bytes.data()), bytes.size())));
    }
}

INSTANTIATE_TEST_SUITE_P(SwitchDictionaryVersionAndRowGroups, ParquetBloomWriterTest,
                         testing::Combine(testing::Values(-1, 0, 1), testing::Values(0, 1, 2),
                                          testing::Bool(), testing::Bool()));

} // namespace
} // namespace doris
