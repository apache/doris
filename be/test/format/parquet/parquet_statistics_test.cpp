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

#include <algorithm>
#include <cstring>
#include <memory>
#include <regex>
#include <vector>

#include "format/parquet/parquet_predicate.h"
#include "util/thrift_util.h"

namespace doris {
namespace {

class BloomFilterFileReader final : public io::FileReader {
public:
    explicit BloomFilterFileReader(std::vector<uint8_t> data, size_t logical_size = 0)
            : _data(std::move(data)),
              _logical_size(logical_size == 0 ? _data.size() : logical_size) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    const io::Path& path() const override { return _path; }
    size_t size() const override { return _logical_size; }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }
    bool returned_short_nonzero_offset_read() const { return _returned_short_nonzero_offset_read; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        if (offset > _data.size()) {
            return Status::IOError("Out of bounds");
        }
        *bytes_read = std::min(result.size, _data.size() - offset);
        memcpy(result.data, _data.data() + offset, *bytes_read);
        _returned_short_nonzero_offset_read |= offset > 0 && *bytes_read != result.size;
        return Status::OK();
    }

private:
    std::vector<uint8_t> _data;
    size_t _logical_size;
    io::Path _path = "parquet_bloom_filter_test";
    bool _closed = false;
    bool _returned_short_nonzero_offset_read = false;
};

Status read_test_bloom_filter(int32_t header_payload_size, size_t actual_payload_size,
                              int32_t declared_length_adjustment = 0,
                              size_t logical_payload_size = 0, bool* returned_short_read = nullptr,
                              bool* installed_bloom_filter = nullptr) {
    tparquet::BloomFilterAlgorithm algorithm;
    algorithm.__set_BLOCK(tparquet::SplitBlockAlgorithm());
    tparquet::BloomFilterHash hash;
    hash.__set_XXHASH(tparquet::XxHash());
    tparquet::BloomFilterCompression compression;
    compression.__set_UNCOMPRESSED(tparquet::Uncompressed());
    tparquet::BloomFilterHeader header;
    header.__set_numBytes(header_payload_size);
    header.__set_algorithm(algorithm);
    header.__set_hash(hash);
    header.__set_compression(compression);

    std::vector<uint8_t> file_bytes;
    ThriftSerializer serializer(/*compact=*/true, /*initial_buffer_size=*/64);
    RETURN_IF_ERROR(serializer.serialize(&header, &file_bytes));
    const size_t header_size = file_bytes.size();
    file_bytes.resize(header_size + actual_payload_size);

    tparquet::ColumnMetaData metadata;
    metadata.__set_bloom_filter_offset(0);
    metadata.__set_bloom_filter_length(static_cast<int32_t>(file_bytes.size()) +
                                       declared_length_adjustment);
    const size_t logical_size =
            logical_payload_size == 0 ? file_bytes.size() : header_size + logical_payload_size;
    auto reader = std::make_shared<BloomFilterFileReader>(std::move(file_bytes), logical_size);
    ParquetPredicate::ColumnStat stat;
    Status status = ParquetPredicate::read_bloom_filter(metadata, reader, nullptr, &stat);
    if (returned_short_read != nullptr) {
        *returned_short_read = reader->returned_short_nonzero_offset_read();
    }
    if (installed_bloom_filter != nullptr) {
        *installed_bloom_filter = stat.bloom_filter != nullptr;
    }
    return status;
}

} // namespace

class ParquetStatisticsTest : public testing::Test {
public:
    ParquetStatisticsTest() = default;
};

TEST_F(ParquetStatisticsTest, reject_truncated_bloom_filter_payload) {
    // The reader may legally return a short read at EOF, so accepting it would initialize a
    // Bloom filter whose missing bytes came from zero-filled process memory.
    bool returned_short_read = false;
    bool installed_bloom_filter = true;
    EXPECT_FALSE(read_test_bloom_filter(/*header_payload_size=*/64, /*actual_payload_size=*/32,
                                        /*declared_length_adjustment=*/32,
                                        /*logical_payload_size=*/64, &returned_short_read,
                                        &installed_bloom_filter)
                         .ok());
    EXPECT_TRUE(returned_short_read);
    EXPECT_FALSE(installed_bloom_filter);
}

TEST_F(ParquetStatisticsTest, reject_bloom_filter_range_beyond_file) {
    bool returned_short_read = false;
    EXPECT_FALSE(read_test_bloom_filter(/*header_payload_size=*/64, /*actual_payload_size=*/32,
                                        /*declared_length_adjustment=*/0,
                                        /*logical_payload_size=*/0, &returned_short_read)
                         .ok());
    EXPECT_FALSE(returned_short_read);
}

TEST_F(ParquetStatisticsTest, reject_declared_bloom_filter_length_mismatch) {
    // A present length describes exactly one header and payload. Treating it as an upper bound can
    // reinterpret a multi-block filter as a smaller filter and cause false-negative pruning.
    EXPECT_FALSE(
            read_test_bloom_filter(/*header_payload_size=*/32, /*actual_payload_size=*/64).ok());
}

TEST_F(ParquetStatisticsTest, reject_invalid_bloom_filter_block_sizes) {
    EXPECT_FALSE(
            read_test_bloom_filter(/*header_payload_size=*/16, /*actual_payload_size=*/16).ok());
    EXPECT_FALSE(
            read_test_bloom_filter(/*header_payload_size=*/33, /*actual_payload_size=*/33).ok());
}

TEST_F(ParquetStatisticsTest, reject_nonpositive_bloom_filter_declared_length) {
    const int32_t declared_length_adjustment = -1000;
    EXPECT_FALSE(read_test_bloom_filter(/*header_payload_size=*/32, /*actual_payload_size=*/32,
                                        declared_length_adjustment)
                         .ok());
}

TEST_F(ParquetStatisticsTest, accept_valid_bloom_filter_layout) {
    EXPECT_TRUE(
            read_test_bloom_filter(/*header_payload_size=*/32, /*actual_payload_size=*/32).ok());
}

TEST_F(ParquetStatisticsTest, accept_bloom_filter_without_declared_length_before_trailing_bytes) {
    constexpr int32_t present_value = 1;
    ParquetBlockSplitBloomFilter source;
    ASSERT_TRUE(source.init(segment_v2::BloomFilter::MINIMUM_BYTES,
                            segment_v2::HashStrategyPB::XX_HASH_64)
                        .ok());
    source.add_bytes(reinterpret_cast<const char*>(&present_value), sizeof(present_value));
    int32_t absent_value = 2;
    while (source.test_bytes(reinterpret_cast<const char*>(&absent_value), sizeof(absent_value))) {
        ++absent_value;
    }

    tparquet::BloomFilterAlgorithm algorithm;
    algorithm.__set_BLOCK(tparquet::SplitBlockAlgorithm());
    tparquet::BloomFilterHash hash;
    hash.__set_XXHASH(tparquet::XxHash());
    tparquet::BloomFilterCompression compression;
    compression.__set_UNCOMPRESSED(tparquet::Uncompressed());
    tparquet::BloomFilterHeader header;
    header.__set_numBytes(static_cast<int32_t>(source.size()));
    header.__set_algorithm(algorithm);
    header.__set_hash(hash);
    header.__set_compression(compression);
    std::vector<uint8_t> file_bytes;
    ThriftSerializer serializer(/*compact=*/true, /*initial_buffer_size=*/64);
    ASSERT_TRUE(serializer.serialize(&header, &file_bytes).ok());
    file_bytes.insert(file_bytes.end(), source.data(), source.data() + source.size());
    file_bytes.resize(file_bytes.size() + 64);

    tparquet::ColumnMetaData metadata;
    metadata.__set_bloom_filter_offset(0);
    auto reader = std::make_shared<BloomFilterFileReader>(std::move(file_bytes));
    ParquetPredicate::ColumnStat stat;
    ASSERT_TRUE(ParquetPredicate::read_bloom_filter(metadata, reader, nullptr, &stat).ok());
    ASSERT_NE(stat.bloom_filter, nullptr);
    EXPECT_TRUE(stat.bloom_filter->test_bytes(reinterpret_cast<const char*>(&present_value),
                                              sizeof(present_value)));
    EXPECT_FALSE(stat.bloom_filter->test_bytes(reinterpret_cast<const char*>(&absent_value),
                                               sizeof(absent_value)));
}

TEST_F(ParquetStatisticsTest, test_try_read_old_utf8_stats) {
    // [, bcé]: min is empty, max starts with ASCII
    {
        std::string encoding_min("");
        std::string encoding_max("bcé");
        EXPECT_FALSE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
    }

    //    // [, ébc]: min is empty, max starts with non-ASCII
    {
        std::string encoding_min("");
        std::string encoding_max("ébc");
        EXPECT_FALSE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
    }

    // [aa, bé]: no common prefix, first different are both ASCII, min is all ASCII
    {
        std::string encoding_min("aa");
        std::string encoding_max("bé");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "aa");
        EXPECT_EQ(encoding_max, "c");
    }

    // [abcd, abcdN]: common prefix, not only ASCII, one prefix of the other, last common ASCII
    {
        std::string encoding_min("abcd");
        std::string encoding_max("abcdN");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "abcd");
        EXPECT_EQ(encoding_max, "abce");
    }

    // [abcé, abcéN]: common prefix, not only ASCII, one prefix of the other, last common non ASCII
    {
        std::string encoding_min("abcé");
        std::string encoding_max("abcéN");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "abcé");
        EXPECT_EQ(encoding_max, "abd");
    }

    // [abcéM, abcéN]: common prefix, not only ASCII, first different are both ASCII
    {
        std::string encoding_min("abcéM");
        std::string encoding_max("abcéN");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "abcéM");
        EXPECT_EQ(encoding_max, "abcéO");
    }

    // [abcéMab, abcéNxy]: common prefix, not only ASCII, first different are both ASCII, more characters afterwards
    {
        std::string encoding_min("abcéMab");
        std::string encoding_max("abcéNxy");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "abcéMab");
        EXPECT_EQ(encoding_max, "abcéO");
    }

    // [abcéM, abcé\u00f7]: common prefix, not only ASCII, first different are both ASCII, but need to be chopped off (127)
    {
        std::string encoding_min("abcéM");
        std::string encoding_max("abcé\u00f7");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        EXPECT_EQ(encoding_min, "abcéM");
        EXPECT_EQ(encoding_max, "abd");
    }

    // [abc\u007fé, bcd\u007fé]: no common prefix, first different are both ASCII
    {
        std::string encoding_min("abc\u007fé");
        std::string encoding_max("bcd\u007fé");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "abc\u007f");
        EXPECT_EQ(encoding_max, "c");
    }

    // [é, a]: no common prefix, first different are not both ASCII
    {
        std::string encoding_min("é");
        std::string encoding_max("a");
        EXPECT_FALSE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
    }

    // [é, ê]: no common prefix, first different are both not ASCII
    {
        std::string encoding_min("é");
        std::string encoding_max("ê");
        EXPECT_FALSE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
    }

    // [aé, aé]: min = max (common prefix, first different are both not ASCII)
    {
        std::string encoding_min("aé");
        std::string encoding_max("aé");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "aé");
        EXPECT_EQ(encoding_max, "aé");
    }

    // [aé, bé]: no common prefix, first different are both ASCII
    {
        std::string encoding_min("aé");
        std::string encoding_max("bé");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "a");
        EXPECT_EQ(encoding_max, "c");
    }
}

} // namespace doris
