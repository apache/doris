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

#include "util/block_compression.h"

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdlib.h>

#include <random>
#include <string>

#include "gtest/gtest_pred_impl.h"
#include "util/faststring.h"

namespace doris {
class BlockCompressionTest : public testing::Test {
public:
    BlockCompressionTest() {}
    virtual ~BlockCompressionTest() {}
};

static std::string generate_str(size_t len) {
    static char charset[] =
            "0123456789"
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::string result;
    result.resize(len);
    for (int i = 0; i < len; ++i) {
        result[i] = charset[rand() % sizeof(charset)];
    }
    return result;
}

void test_single_slice(segment_v2::CompressionTypePB type) {
    BlockCompressionCodec* codec;
    auto st = get_block_compression_codec(type, &codec);
    EXPECT_TRUE(st.ok());

    size_t test_sizes[] = {0, 1, 10, 1000, 1000000};
    for (auto size : test_sizes) {
        auto orig = generate_str(size);
        faststring compressed_str;
        {
            st = codec->compress(orig, &compressed_str);
            EXPECT_TRUE(st.ok());

            Slice compressed_slice(compressed_str);
            std::string uncompressed;
            uncompressed.resize(size);
            {
                Slice uncompressed_slice(uncompressed);
                st = codec->decompress(compressed_slice, &uncompressed_slice);
                EXPECT_TRUE(st.ok());

                EXPECT_EQ(orig, uncompressed);
            }
            // buffer not enough for decompress
            // Snappy's capacity validation is covered in snappy_invalid_input.
            // NOTE: For ZLIB, we even get OK with a insufficient output
            // when uncompressed size is 1
            if ((type == segment_v2::CompressionTypePB::ZLIB && uncompressed.size() > 1) &&
                type != segment_v2::CompressionTypePB::SNAPPY && uncompressed.size() > 0) {
                Slice uncompressed_slice(uncompressed);
                uncompressed_slice.size -= 1;
                st = codec->decompress(compressed_slice, &uncompressed_slice);
                EXPECT_FALSE(st.ok());
            }
            // corrupt compressed data
            if (type != segment_v2::CompressionTypePB::SNAPPY) {
                Slice uncompressed_slice(uncompressed);
                compressed_slice.size -= 1;
                st = codec->decompress(compressed_slice, &uncompressed_slice);
                EXPECT_FALSE(st.ok());
                compressed_slice.size += 1;
            }
        }
    }
}

TEST_F(BlockCompressionTest, single) {
    test_single_slice(segment_v2::CompressionTypePB::SNAPPY);
    test_single_slice(segment_v2::CompressionTypePB::ZLIB);
    test_single_slice(segment_v2::CompressionTypePB::LZ4);
    test_single_slice(segment_v2::CompressionTypePB::LZ4F);
    test_single_slice(segment_v2::CompressionTypePB::LZ4HC);
    test_single_slice(segment_v2::CompressionTypePB::ZSTD);
}

void test_multi_slices(segment_v2::CompressionTypePB type) {
    BlockCompressionCodec* codec;
    auto st = get_block_compression_codec(type, &codec);
    EXPECT_TRUE(st.ok());

    size_t test_sizes[] = {0, 1, 10, 1000, 1000000};
    std::vector<std::string> orig_strs;
    for (auto size : test_sizes) {
        orig_strs.emplace_back(generate_str(size));
    }
    std::vector<Slice> orig_slices;
    std::string orig;
    for (auto& str : orig_strs) {
        orig_slices.emplace_back(str);
        orig.append(str);
    }

    size_t total_size = orig.size();
    faststring compressed;
    {
        st = codec->compress(orig_slices, total_size, &compressed);
        EXPECT_TRUE(st.ok());

        Slice compressed_slice(compressed);
        std::string uncompressed;
        uncompressed.resize(total_size);
        // normal case
        {
            Slice uncompressed_slice(uncompressed);
            st = codec->decompress(compressed_slice, &uncompressed_slice);
            EXPECT_TRUE(st.ok());

            EXPECT_EQ(orig, uncompressed);
        }
    }
}

TEST_F(BlockCompressionTest, multi) {
    test_multi_slices(segment_v2::CompressionTypePB::SNAPPY);
    test_multi_slices(segment_v2::CompressionTypePB::ZLIB);
    test_multi_slices(segment_v2::CompressionTypePB::LZ4);
    test_multi_slices(segment_v2::CompressionTypePB::LZ4F);
    test_multi_slices(segment_v2::CompressionTypePB::LZ4HC);
    test_multi_slices(segment_v2::CompressionTypePB::ZSTD);
}

// Parquet GZIP pages are decoded by libdeflate on every architecture, so check it
// against the zlib gzip codec and on the malformed inputs a data file can contain.
TEST_F(BlockCompressionTest, parquet_gzip) {
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(tparquet::CompressionCodec::GZIP, &codec).ok());
    BlockCompressionCodec* zlib_codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(TFileCompressType::GZ, &zlib_codec).ok());

    std::string empty_output;
    Slice empty_slice(empty_output);
    EXPECT_TRUE(codec->decompress(Slice(), &empty_slice).ok());
    EXPECT_EQ(0, empty_slice.size);

    // An empty page must not satisfy a header that declares uncompressed bytes.
    std::string dict_output(4, '\0');
    Slice dict_slice(dict_output);
    EXPECT_FALSE(codec->decompress(Slice(), &dict_slice).ok());

    for (size_t size : {1, 10, 1000, 65536, 1000000, 8 * 1024 * 1024}) {
        SCOPED_TRACE(size);
        // Repeat a short random string so that both literals and matches are exercised.
        std::string orig = generate_str(std::min<size_t>(size, 4096));
        while (orig.size() < size) {
            orig.append(orig, 0, std::min(orig.size(), size - orig.size()));
        }
        faststring compressed;
        ASSERT_TRUE(zlib_codec->compress(orig, &compressed).ok());

        std::string restored(size, '\0');
        Slice output(restored);
        ASSERT_TRUE(codec->decompress(Slice(compressed), &output).ok());
        EXPECT_EQ(orig, restored);

        Slice short_output(restored.data(), size - 1);
        EXPECT_FALSE(codec->decompress(Slice(compressed), &short_output).ok());

        // The page expands to fewer bytes than the header declares.
        std::string larger(size + 1, '\0');
        Slice long_output(larger);
        EXPECT_FALSE(codec->decompress(Slice(compressed), &long_output).ok());

        for (size_t len : {size_t {1}, size_t {10}, compressed.size() / 2, compressed.size() - 4,
                           compressed.size() - 1}) {
            output = Slice(restored);
            EXPECT_FALSE(codec->decompress(Slice(compressed.data(), len), &output).ok());
        }

        // Corrupt the gzip magic, a deflate byte and the CRC32 in the trailer.
        for (size_t pos : {size_t {0}, compressed.size() / 2, compressed.size() - 8}) {
            faststring corrupted;
            corrupted.assign_copy(compressed.data(), compressed.size());
            corrupted.data()[pos] ^= 0x55;
            output = Slice(restored);
            EXPECT_FALSE(codec->decompress(Slice(corrupted), &output).ok());
        }
    }

    std::string restored(16, '\0');
    Slice output(restored);
    EXPECT_FALSE(codec->decompress(Slice("not a gzip stream"), &output).ok());
}

static void check_concatenated_gzip_decompression(BlockCompressionCodec* codec,
                                                  const std::string& compressed,
                                                  const std::string& original) {
    std::string restored(original.size(), '\0');
    Slice output(restored);
    auto status = codec->decompress(Slice(compressed), &output);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(original, restored);
    EXPECT_EQ(original.size(), output.size);

    if (!original.empty()) {
        Slice short_output(restored.data(), original.size() - 1);
        EXPECT_FALSE(codec->decompress(Slice(compressed), &short_output).ok());
    }
    std::string larger(original.size() + 1, '\0');
    Slice long_output(larger);
    EXPECT_FALSE(codec->decompress(Slice(compressed), &long_output).ok());

    // Validate the final member even when previous members already filled the output.
    EXPECT_FALSE(codec->decompress(Slice(compressed.data(), compressed.size() - 1), &output).ok());
    std::string corrupted = compressed;
    corrupted[corrupted.size() - 8] ^= 1;
    EXPECT_FALSE(codec->decompress(Slice(corrupted), &output).ok());
    std::string trailing = compressed + "trailing junk";
    EXPECT_FALSE(codec->decompress(Slice(trailing), &output).ok());
}

TEST_F(BlockCompressionTest, parquet_gzip_concatenated_members) {
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(tparquet::CompressionCodec::GZIP, &codec).ok());
    BlockCompressionCodec* zlib_codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(TFileCompressType::GZ, &zlib_codec).ok());

    // A Parquet page's uncompressed size covers all members, including empty members.
    for (const std::vector<std::string>& members :
         {std::vector<std::string> {"first", std::string(4099, 'x')},
          std::vector<std::string> {"", "first", "", std::string(4099, 'x'), ""},
          std::vector<std::string> {"", ""}}) {
        std::string compressed;
        std::string original;
        for (const auto& member : members) {
            faststring encoded;
            ASSERT_TRUE(zlib_codec->compress(Slice(member), &encoded).ok());
            compressed.append(reinterpret_cast<const char*>(encoded.data()), encoded.size());
            original.append(member);
        }
        SCOPED_TRACE(original.size());
        check_concatenated_gzip_decompression(codec, compressed, original);
    }
}

static void check_snappy_decompression(BlockCompressionCodec* codec, const faststring& compressed,
                                       const std::string& original) {
    std::string restored(original.size(), '\0');
    Slice output(restored);
    ASSERT_TRUE(codec->decompress(Slice(compressed), &output).ok());
    EXPECT_EQ(original.size(), output.size);
    EXPECT_EQ(original, restored);
}

TEST_F(BlockCompressionTest, snappy_binary_and_block_boundaries) {
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(segment_v2::CompressionTypePB::SNAPPY, &codec).ok());
    std::mt19937 random(0);
    // Snappy compresses in 64 KiB blocks. Exercise both sides of that boundary,
    // including binary data whose embedded NULs must not truncate comparisons.
    for (size_t size : {0, 1, 63, 64, 65, 65535, 65536, 65537, 1048576}) {
        SCOPED_TRACE(size);
        for (bool compressible : {false, true}) {
            SCOPED_TRACE(compressible);
            std::string original(size, '\0');
            for (size_t i = 0; i < size; ++i) {
                original[i] = static_cast<char>(compressible ? i % 8 : random() & 0xff);
            }
            faststring compressed;
            ASSERT_TRUE(codec->compress(original, &compressed).ok());
            check_snappy_decompression(codec, compressed, original);

            // Empty slices at every position and uneven boundaries force the
            // Source adapter to advance across slices within a Snappy block.
            size_t split = size / 3;
            std::vector<Slice> slices = {Slice(), Slice(original.data(), split), Slice(),
                                         Slice(original.data() + split, size - split), Slice()};
            ASSERT_TRUE(codec->compress(slices, size, &compressed).ok());
            check_snappy_decompression(codec, compressed, original);
        }
    }
}

TEST_F(BlockCompressionTest, snappy_invalid_input) {
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(segment_v2::CompressionTypePB::SNAPPY, &codec).ok());
    const std::string original = "snappy block compression";
    faststring compressed;
    ASSERT_TRUE(codec->compress(original, &compressed).ok());
    std::string restored(original.size(), '\0');
    for (size_t size = 0; size < compressed.size(); ++size) {
        SCOPED_TRACE(size);
        Slice output(restored);
        EXPECT_FALSE(codec->decompress(Slice(compressed.data(), size), &output).ok());
    }
    restored.assign(original.size(), '\0');
    Slice output(restored.data(), restored.size() - 1);
    EXPECT_FALSE(codec->decompress(Slice(compressed), &output).ok());
    EXPECT_EQ(std::string(original.size(), '\0'), restored);

    // An unterminated length varint and a copy with no preceding literal.
    for (const auto& invalid : {std::string("\x80", 1), std::string("\x04\x01\x01", 3)}) {
        output = Slice(restored);
        EXPECT_FALSE(codec->decompress(invalid, &output).ok());
    }
}

TEST_F(BlockCompressionTest, snappy_1_1_10_compatibility) {
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(segment_v2::CompressionTypePB::SNAPPY, &codec).ok());
    // Frozen output of Snappy 1.1.10 RawCompress, including a literal containing
    // NUL and 0xff, and a copy tag. Do not regenerate with the linked library.
    const char compressed[] =
            "\x41\x30\x73\x6e\x61\x70\x70\x79\x00\x62\x6c\x6f\x63\x6b"
            "\xff\xce\x0d\x00";
    std::string expected;
    for (int i = 0; i < 5; ++i) {
        expected.append("snappy\0block\xff", 13);
    }
    std::string restored(expected.size() + 16, '\0');
    Slice output(restored);
    ASSERT_TRUE(codec->decompress(Slice(compressed, sizeof(compressed) - 1), &output).ok());
    EXPECT_EQ(expected.size(), output.size);
    EXPECT_EQ(expected, restored.substr(0, output.size));
}

} // namespace doris
