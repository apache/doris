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

#include <string>

#include "gtest/gtest_pred_impl.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
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

                EXPECT_STREQ(orig.c_str(), uncompressed.c_str());
            }
            // buffer not enough for decompress
            // snappy has no return value if given buffer is not enough
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

            EXPECT_STREQ(orig.c_str(), uncompressed.c_str());
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

TEST_F(BlockCompressionTest, GetCodecWithLevelDefaultReturnsSingleton) {
    for (auto type : {segment_v2::ZSTD, segment_v2::LZ4HC}) {
        BlockCompressionCodec* codec = nullptr;
        ASSERT_TRUE(get_block_compression_codec(type, 0, &codec).ok());
        ASSERT_NE(codec, nullptr);
        BlockCompressionCodec* singleton = nullptr;
        ASSERT_TRUE(get_block_compression_codec(type, &singleton).ok());
        ASSERT_EQ(codec, singleton);
    }
}

TEST_F(BlockCompressionTest, GetCodecWithZstdLevelReturnsSharedInstance) {
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(segment_v2::ZSTD, 9, &codec).ok());
    ASSERT_NE(codec, nullptr);
    BlockCompressionCodec* singleton = nullptr;
    ASSERT_TRUE(get_block_compression_codec(segment_v2::ZSTD, &singleton).ok());
    ASSERT_NE(codec, singleton); // leveled instance, not the type-only singleton
    // round-trip compress/decompress works at this level
    std::string in(4096, 'x');
    for (size_t i = 0; i < in.size(); ++i) in[i] = static_cast<char>(i % 251);
    faststring compressed;
    ASSERT_TRUE(codec->compress(Slice(in), &compressed).ok());
    std::string out(in.size(), '\0');
    Slice out_slice(out);
    ASSERT_TRUE(codec->decompress(Slice(compressed.data(), compressed.size()), &out_slice).ok());
    ASSERT_EQ(std::string(out_slice.data, out_slice.size), in);
}

// The observable effect of compression level is the compressed byte stream: two
// distinct levels of the same codec must produce DIFFERENT output on data that
// is neither trivially nor maximally compressible. If the writer/codec dropped
// the requested level, every level would collapse to the codec default and the
// outputs would be byte-identical. This is the real guard for the level fix in
// block_compression.cpp (ZSTD_c_compressionLevel / LZ4_resetStreamHC_fast); a
// lossless round-trip cannot detect a dropped level because decompression is
// level-independent. (Note: higher level does NOT guarantee smaller output --
// ZSTD level is search effort, not a monotonic size bound -- so we assert
// difference + valid round-trip, not an ordering.)
static std::string compress_at_level(segment_v2::CompressionTypePB type, int level,
                                     const std::string& in) {
    BlockCompressionCodec* codec = nullptr;
    EXPECT_TRUE(get_block_compression_codec(type, level, &codec).ok());
    EXPECT_NE(codec, nullptr);
    faststring compressed;
    EXPECT_TRUE(codec->compress(Slice(in), &compressed).ok());
    // every level must still decompress losslessly
    std::string out(in.size(), '\0');
    Slice out_slice(out);
    EXPECT_TRUE(codec->decompress(Slice(compressed.data(), compressed.size()), &out_slice).ok());
    EXPECT_EQ(std::string(out_slice.data, out_slice.size), in);
    return std::string(reinterpret_cast<const char*>(compressed.data()), compressed.size());
}

TEST_F(BlockCompressionTest, DowngradeLegacyCodecReadsLeveledData) {
    std::string in;
    in.reserve(64 * 1024);
    for (int i = 0; in.size() < 64 * 1024; ++i) {
        in += "apache doris column compression ";
        in += std::to_string(i % 1024);
    }

    struct Case {
        segment_v2::CompressionTypePB type;
        int level;
    };
    for (auto c : {Case {segment_v2::ZSTD, 9}, Case {segment_v2::LZ4HC, 9}}) {
        std::string compressed = compress_at_level(c.type, c.level, in);

        BlockCompressionCodec* legacy_codec = nullptr;
        ASSERT_TRUE(get_block_compression_codec(c.type, &legacy_codec).ok());
        ASSERT_NE(legacy_codec, nullptr);
        std::string out(in.size(), '\0');
        Slice out_slice(out);
        ASSERT_TRUE(legacy_codec->decompress(Slice(compressed), &out_slice).ok());
        EXPECT_EQ(std::string(out_slice.data, out_slice.size), in);
    }
}

TEST_F(BlockCompressionTest, DifferentLevelsProduceDifferentOutput) {
    // Moderately compressible data: enough redundancy that the level changes the
    // encoder's choices, but not so uniform that every level saturates to the
    // same output.
    std::string in;
    in.reserve(256 * 1024);
    const char* words[] = {"apache", "doris",      "compression", "column",  "segment",
                           "rowset", "vectorized", "pipeline",    "storage", "codec"};
    for (int i = 0; in.size() < 256 * 1024; ++i) {
        in += words[(i * 7) % 10];
        in += words[(i * 13) % 10];
        in += std::to_string(i % 512);
        in.push_back(' ');
    }

    // ZSTD: a low level and the max level must yield different byte streams.
    std::string zstd_low = compress_at_level(segment_v2::ZSTD, 1, in);
    std::string zstd_high = compress_at_level(segment_v2::ZSTD, 22, in);
    EXPECT_NE(zstd_low, zstd_high)
            << "ZSTD level 1 and level 22 produced identical output (level likely ignored)";

    // LZ4HC: likewise across its level range.
    std::string lz4hc_low = compress_at_level(segment_v2::LZ4HC, 1, in);
    std::string lz4hc_high = compress_at_level(segment_v2::LZ4HC, 12, in);
    EXPECT_NE(lz4hc_low, lz4hc_high)
            << "LZ4HC level 1 and level 12 produced identical output (level likely ignored)";
}

// A wide schema opens many column writers at the same codec+level. They must all
// share a single pooled codec instance (rather than one heavyweight context pool
// per column), and repeatedly acquiring them must not accumulate memory on the
// block-compression tracker once the shared context pool is warm.
TEST_F(BlockCompressionTest, WideSchemaSharesLeveledInstanceAndTrackerIsBalanced) {
    clear_leveled_compression_codec_pool_for_test();

    constexpr int kColumns = 128;
    auto tracker = ExecEnv::GetInstance()->block_compression_mem_tracker();

    struct Case {
        segment_v2::CompressionTypePB type;
        int level;
    };
    for (auto c : {Case {segment_v2::ZSTD, 9}, Case {segment_v2::LZ4HC, 9}}) {
        // Every column asking for the same (type, level) gets the same instance.
        BlockCompressionCodec* first = nullptr;
        ASSERT_TRUE(get_block_compression_codec(c.type, c.level, &first).ok());
        ASSERT_NE(first, nullptr);
        for (int i = 0; i < kColumns; ++i) {
            BlockCompressionCodec* codec = nullptr;
            ASSERT_TRUE(get_block_compression_codec(c.type, c.level, &codec).ok());
            ASSERT_EQ(codec, first) << "wide schema must share one leveled codec instance";
        }

        // A different level yields a distinct instance (keyed by codec+level).
        BlockCompressionCodec* other_level = nullptr;
        ASSERT_TRUE(get_block_compression_codec(c.type, c.level + 1, &other_level).ok());
        ASSERT_NE(other_level, first);

        // Drive many serial compressions across the shared instance; after the
        // pool is warm the tracker must not keep growing (contexts are reused,
        // and reusable buffers are released back, not retained per column).
        std::string in(64 * 1024, '\0');
        for (size_t i = 0; i < in.size(); ++i) in[i] = static_cast<char>((i * 7) % 251);

        auto compress_once = [&]() {
            faststring compressed;
            ASSERT_TRUE(first->compress(Slice(in), &compressed).ok());
        };
        compress_once(); // warm up: lazily allocates the single shared context
        int64_t warm = tracker->consumption();
        for (int i = 0; i < kColumns; ++i) {
            compress_once();
        }
        int64_t after = tracker->consumption();
        EXPECT_EQ(after, warm) << "shared pool must not grow per column on the compression tracker";
    }
}

} // namespace doris
