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

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/config.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/index/bloom_filter/bloom_filter.h"
#include "storage/index/global_point/global_point_index_format.h"
#include "storage/index/global_point/global_point_index_reader.h"
#include "storage/index/global_point/global_point_index_writer.h"
#include "util/hash_util.hpp"

namespace doris::segment_v2 {

namespace {

const std::string kTestDir = "./data_test/data/global_point_index_file_test";
constexpr int32_t kColUniqueId = 5;
constexpr int64_t kIndexId = 1001;

} // namespace

class GlobalPointIndexSizingTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_cap = config::global_point_index_max_write_path_bloom_bytes;
        _saved_estimated_rows = config::global_point_index_write_path_estimated_rows;
        _saved_blooms_per_tablet = config::global_point_index_expected_blooms_per_tablet;
    }
    void TearDown() override {
        config::global_point_index_max_write_path_bloom_bytes = _saved_cap;
        config::global_point_index_write_path_estimated_rows = _saved_estimated_rows;
        config::global_point_index_expected_blooms_per_tablet = _saved_blooms_per_tablet;
    }

    int64_t _saved_cap = 0;
    int64_t _saved_estimated_rows = 0;
    int32_t _saved_blooms_per_tablet = 0;
};

TEST_F(GlobalPointIndexSizingTest, WritePathDefaultIsCapped) {
    // 1M estimated rows size to about 2 MB, which the default 256 KB cap clips.
    auto sizing = compute_global_point_index_sizing(0.01, std::nullopt);
    EXPECT_EQ(sizing.bloom_bytes,
              static_cast<uint64_t>(config::global_point_index_max_write_path_bloom_bytes));
}

TEST_F(GlobalPointIndexSizingTest, HighCapIsNotReached) {
    config::global_point_index_max_write_path_bloom_bytes = 8 * 1024 * 1024;
    auto sizing = compute_global_point_index_sizing(0.01, std::nullopt);
    double expected_fpp = 0.01 / std::max(1, config::global_point_index_expected_blooms_per_tablet);
    uint64_t expected_bytes =
            BloomFilter::optimal_bit_num(
                    static_cast<uint64_t>(config::global_point_index_write_path_estimated_rows),
                    expected_fpp) /
            8;
    EXPECT_EQ(sizing.bloom_bytes, expected_bytes);
    EXPECT_DOUBLE_EQ(sizing.per_bloom_fpp, expected_fpp);
    EXPECT_LT(expected_bytes, 8ULL * 1024 * 1024);
}

TEST_F(GlobalPointIndexSizingTest, CapIsRoundedDownToPowerOfTwo) {
    config::global_point_index_max_write_path_bloom_bytes = 300000;
    EXPECT_EQ(compute_global_point_index_sizing(0.01, std::nullopt).bloom_bytes, 262144u);
    config::global_point_index_max_write_path_bloom_bytes = 8;
    EXPECT_EQ(compute_global_point_index_sizing(0.01, std::nullopt).bloom_bytes,
              static_cast<uint64_t>(BloomFilter::MINIMUM_BYTES));
    for (int64_t cap : {8, 1000, 262144, 300000, 5000000}) {
        config::global_point_index_max_write_path_bloom_bytes = cap;
        auto sizing = compute_global_point_index_sizing(0.01, std::nullopt);
        EXPECT_EQ(sizing.bloom_bytes & (sizing.bloom_bytes - 1), 0u) << "cap=" << cap;
    }
}

TEST_F(GlobalPointIndexSizingTest, ExactRowCountIsNotCapped) {
    auto sizing = compute_global_point_index_sizing(0.01, std::optional<int64_t>(100000000));
    EXPECT_GT(sizing.bloom_bytes,
              static_cast<uint64_t>(config::global_point_index_max_write_path_bloom_bytes) * 10);
}

// A bloom sized exactly for 16M rows is healthy; the capped load-path bloom for the same rowset is
// two orders of magnitude too small and must be reported.
TEST_F(GlobalPointIndexSizingTest, HealthSeparatesCorrectAndSaturatedBlooms) {
    constexpr int64_t kRows = 16000000;
    const double per_bloom_fpp = 0.01 / config::global_point_index_expected_blooms_per_tablet;
    auto healthy = compute_global_point_index_sizing(0.01, std::optional<int64_t>(kRows));
    EXPECT_EQ(check_global_point_index_health(static_cast<int64_t>(healthy.bloom_bytes), kRows,
                                              per_bloom_fpp, 50),
              GlobalPointIndexHealth::OK);
    auto capped = compute_global_point_index_sizing(0.01, std::nullopt);
    EXPECT_EQ(check_global_point_index_health(static_cast<int64_t>(capped.bloom_bytes), kRows,
                                              per_bloom_fpp, 50),
              GlobalPointIndexHealth::UNDERSIZED);
}

// The threshold follows the bloom's own fpp, so a deliberately loose index is not flagged.
TEST_F(GlobalPointIndexSizingTest, HealthAcceptsLooseFpp) {
    constexpr int64_t kRows = 1000000;
    for (double per_tablet_fpp : {0.01, 0.05, 0.1, 0.3, 0.5}) {
        const double per_bloom_fpp =
                per_tablet_fpp / config::global_point_index_expected_blooms_per_tablet;
        auto sizing =
                compute_global_point_index_sizing(per_tablet_fpp, std::optional<int64_t>(kRows));
        EXPECT_EQ(check_global_point_index_health(static_cast<int64_t>(sizing.bloom_bytes), kRows,
                                                  per_bloom_fpp, 50),
                  GlobalPointIndexHealth::OK)
                << "per-tablet fpp " << per_tablet_fpp;
    }
}

TEST_F(GlobalPointIndexSizingTest, HealthEdgeCases) {
    // Zero inserted values is EMPTY whatever the slack: nulls are not counted.
    EXPECT_EQ(check_global_point_index_health(262144, 0, 0.002, 50), GlobalPointIndexHealth::EMPTY);
    EXPECT_EQ(check_global_point_index_health(262144, 0, 0.002, 0), GlobalPointIndexHealth::EMPTY);
    // A non-positive slack disables UNDERSIZED.
    EXPECT_EQ(check_global_point_index_health(1024, 16000000, 0.002, 50),
              GlobalPointIndexHealth::UNDERSIZED);
    EXPECT_EQ(check_global_point_index_health(1024, 16000000, 0.002, 0),
              GlobalPointIndexHealth::OK);
    // No usable fpp: no verdict.
    EXPECT_EQ(check_global_point_index_health(1024, 16000000, 0.0, 50), GlobalPointIndexHealth::OK);
    EXPECT_EQ(check_global_point_index_health(1024, 16000000, 1.5, 50), GlobalPointIndexHealth::OK);
}

// The health check is only safe if expected_bits_per_key never claims more bits than a real bloom
// gets from BloomFilter::optimal_bit_num, over the whole fpp range the DDL allows.
TEST_F(GlobalPointIndexSizingTest, ExpectedBitsPerKeyStaysBelowOptimalBitNum) {
    for (double fpp : {1e-6, 1e-5, 1e-4, 0.001, 0.002, 0.01, 0.06, 0.1, 0.2, 0.3, 0.5}) {
        const int64_t n = 1000000;
        double from_helper = expected_bits_per_key(fpp);
        double from_bloom = static_cast<double>(BloomFilter::optimal_bit_num(n, fpp)) / n;
        EXPECT_GE(from_bloom, from_helper) << "fpp=" << fpp;
        EXPECT_LE(from_bloom, from_helper * 3.0) << "fpp=" << fpp;
    }
}

class GlobalPointIndexFileTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
    }

protected:
    // Writes a .gpidx file with `values` and returns its descriptor.
    static ColumnPointIndexPB write_file(const std::string& name,
                                         const std::vector<int32_t>& values, double fpp = 0.01) {
        GlobalPointIndexBuilder builder(kColUniqueId, kIndexId);
        EXPECT_TRUE(builder.init(1024, fpp).ok());
        builder.add_values(FieldType::OLAP_FIELD_TYPE_INT, values.data(), values.size());
        io::FileWriterPtr file_writer;
        EXPECT_TRUE(io::global_local_filesystem()
                            ->create_file(kTestDir + "/" + name, &file_writer)
                            .ok());
        ColumnPointIndexPB index_meta;
        EXPECT_TRUE(builder.finalize(file_writer.get(), &index_meta).ok());
        EXPECT_TRUE(file_writer->close().ok());
        index_meta.set_index_file_suffix(name);
        return index_meta;
    }

    static std::string read_all(const std::string& name) {
        io::FileReaderSPtr reader;
        EXPECT_TRUE(io::global_local_filesystem()->open_file(kTestDir + "/" + name, &reader).ok());
        std::string content(reader->size(), '\0');
        size_t bytes_read = 0;
        EXPECT_TRUE(reader->read_at(0, {content.data(), content.size()}, &bytes_read).ok());
        return content;
    }

    static void overwrite(const std::string& name, const std::string& content) {
        io::FileWriterPtr file_writer;
        ASSERT_TRUE(io::global_local_filesystem()
                            ->create_file(kTestDir + "/" + name, &file_writer)
                            .ok());
        ASSERT_TRUE(file_writer->append(Slice(content)).ok());
        ASSERT_TRUE(file_writer->close().ok());
    }

    static std::unique_ptr<BloomFilter> load(const std::string& name,
                                             const ColumnPointIndexPB& desc) {
        std::unique_ptr<BloomFilter> bloom;
        int64_t bytes_read = 0;
        // Never an error, whatever the file content.
        EXPECT_TRUE(try_load_global_point_index(io::global_local_filesystem(),
                                                kTestDir + "/" + name, desc, nullptr, &bloom,
                                                &bytes_read)
                            .ok());
        return bloom;
    }
};

TEST_F(GlobalPointIndexFileTest, HeaderIsSelfChecking) {
    auto desc = write_file("header.gpidx", {1, 2, 3, 4, 5});
    std::string content = read_all("header.gpidx");
    ASSERT_GE(content.size(), kGlobalPointIndexHeaderSize);
    const auto* header = reinterpret_cast<const GlobalPointIndexHeader*>(content.data());
    EXPECT_EQ(std::string(header->magic, 4), "GPIX");
    size_t body_size = content.size() - kGlobalPointIndexHeaderSize;
    EXPECT_EQ(header->num_bits, static_cast<uint64_t>(body_size - 1) * 8);
    EXPECT_EQ(header->total_rows, 5);
    EXPECT_EQ(header->body_crc32,
              HashUtil::zlib_crc_hash(content.data() + kGlobalPointIndexHeaderSize,
                                      static_cast<uint32_t>(body_size), 0));

    EXPECT_EQ(desc.offset(), static_cast<int64_t>(kGlobalPointIndexHeaderSize));
    EXPECT_EQ(desc.size(), static_cast<int64_t>(body_size));
    EXPECT_EQ(desc.column_unique_id(), kColUniqueId);
    EXPECT_EQ(desc.index_id(), kIndexId);
    EXPECT_EQ(desc.total_rows(), 5);
    EXPECT_DOUBLE_EQ(desc.fpp(), 0.01);
    EXPECT_EQ(desc.hash_strategy(), static_cast<int32_t>(HASH_MURMUR3_X64_64));
    EXPECT_EQ(desc.body_crc32(), header->body_crc32);
}

TEST_F(GlobalPointIndexFileTest, FinalizeAndDirectWriteAreByteIdentical) {
    GlobalPointIndexBuilder builder(kColUniqueId, kIndexId);
    ASSERT_TRUE(builder.init(1024, 0.01).ok());
    std::vector<int32_t> values = {10, 20, 30};
    builder.add_values(FieldType::OLAP_FIELD_TYPE_INT, values.data(), values.size());

    io::FileWriterPtr writer_a;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(kTestDir + "/a.gpidx", &writer_a).ok());
    ColumnPointIndexPB meta_a;
    ASSERT_TRUE(builder.finalize(writer_a.get(), &meta_a).ok());
    ASSERT_TRUE(writer_a->close().ok());

    io::FileWriterPtr writer_b;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(kTestDir + "/b.gpidx", &writer_b).ok());
    ColumnPointIndexPB meta_b;
    ASSERT_TRUE(write_global_point_index_file(writer_b.get(), builder.body(), builder.body_size(),
                                              kColUniqueId, kIndexId, builder.bloom_fpp(),
                                              builder.total_rows(), &meta_b)
                        .ok());
    ASSERT_TRUE(writer_b->close().ok());

    EXPECT_EQ(read_all("a.gpidx"), read_all("b.gpidx"));
    EXPECT_EQ(meta_a.SerializeAsString(), meta_b.SerializeAsString());
}

TEST_F(GlobalPointIndexFileTest, RoundTripFindsEveryValue) {
    std::vector<int32_t> values = {100, 200, 300, 400};
    auto desc = write_file("roundtrip.gpidx", values);
    auto bloom = load("roundtrip.gpidx", desc);
    ASSERT_NE(bloom, nullptr);
    for (int32_t v : values) {
        EXPECT_TRUE(bloom->test_bytes(reinterpret_cast<const char*>(&v), sizeof(v)));
    }
}

TEST_F(GlobalPointIndexFileTest, StringAndNullValues) {
    GlobalPointIndexBuilder builder(kColUniqueId, kIndexId);
    ASSERT_TRUE(builder.init(1024, 0.01).ok());
    std::vector<std::string> strings = {"event-1", "event-2", ""};
    std::vector<Slice> slices(strings.begin(), strings.end());
    builder.add_values(FieldType::OLAP_FIELD_TYPE_VARCHAR, slices.data(), slices.size());
    builder.add_nulls(2);
    EXPECT_EQ(builder.total_rows(), 3);

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(
            io::global_local_filesystem()->create_file(kTestDir + "/str.gpidx", &file_writer).ok());
    ColumnPointIndexPB desc;
    ASSERT_TRUE(builder.finalize(file_writer.get(), &desc).ok());
    ASSERT_TRUE(file_writer->close().ok());

    auto bloom = load("str.gpidx", desc);
    ASSERT_NE(bloom, nullptr);
    for (const auto& s : strings) {
        EXPECT_TRUE(bloom->test_bytes(s.data(), s.size())) << s;
    }
    EXPECT_TRUE(bloom->has_null());
}

// Every kind of damage makes the bloom unusable (null) without an error.
TEST_F(GlobalPointIndexFileTest, DamagedFileIsNotUsed) {
    auto desc = write_file("good.gpidx", {1, 2, 3});
    const std::string good = read_all("good.gpidx");

    std::string flipped_body = good;
    flipped_body[kGlobalPointIndexHeaderSize + 3] ^= 0x5a;
    overwrite("flipped.gpidx", flipped_body);
    EXPECT_EQ(load("flipped.gpidx", desc), nullptr);

    overwrite("truncated.gpidx", good.substr(0, good.size() - 1));
    EXPECT_EQ(load("truncated.gpidx", desc), nullptr);

    overwrite("short.gpidx", good.substr(0, kGlobalPointIndexHeaderSize - 1));
    EXPECT_EQ(load("short.gpidx", desc), nullptr);

    std::string bad_magic = good;
    bad_magic[0] = 'X';
    overwrite("magic.gpidx", bad_magic);
    EXPECT_EQ(load("magic.gpidx", desc), nullptr);

    std::string newer = good;
    reinterpret_cast<GlobalPointIndexHeader*>(newer.data())->format_version =
            GlobalPointIndexHeader::kFormatVersion + 1;
    overwrite("newer.gpidx", newer);
    EXPECT_EQ(load("newer.gpidx", desc), nullptr);

    EXPECT_EQ(load("missing.gpidx", desc), nullptr);

    // The file is intact but does not match the descriptor.
    ColumnPointIndexPB wrong_crc = desc;
    wrong_crc.set_body_crc32(desc.body_crc32() + 1);
    EXPECT_EQ(load("good.gpidx", wrong_crc), nullptr);
    ColumnPointIndexPB wrong_size = desc;
    wrong_size.set_size(desc.size() * 2);
    EXPECT_EQ(load("good.gpidx", wrong_size), nullptr);

    EXPECT_NE(load("good.gpidx", desc), nullptr);
}

} // namespace doris::segment_v2
