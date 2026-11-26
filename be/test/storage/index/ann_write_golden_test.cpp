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

// Frozen ANN index files. ANN is moving off the CLucene directory, and the files faiss writes must
// stay what they are, in V2 and in SNII: every sub-file of an HNSW, IVF and IVF_ON_DISK index is
// frozen in ann_write_golden.txt. faiss builds with OpenMP threads and an HNSW graph depends on
// how they interleave, so the builds here run on one thread, and each index is written twice to
// show the bytes repeat.

#include <CLucene.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "storage/index/ann/ann_index_writer.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/common/inverted_index_desc.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/golden_file.h"
#include "util/hash_util.hpp"

namespace doris::segment_v2 {

namespace {

constexpr const char* kGoldenFile = "./be/test/storage/index/ann_write_golden.txt";
constexpr const char* kTestDir = "./ut_dir/ann_write_golden_test";
constexpr int64_t kIndexId = 88;
constexpr uint32_t kDim = 8;
constexpr uint32_t kRows = 512;

struct AnnCase {
    const char* name;
    std::map<std::string, std::string> properties;
};

const std::vector<AnnCase>& ann_cases() {
    static const std::vector<AnnCase> cases = {
            {.name = "hnsw",
             .properties = {{"index_type", "hnsw"},
                            {"metric_type", "l2_distance"},
                            {"dim", std::to_string(kDim)},
                            {"max_degree", "16"}}},
            {.name = "ivf",
             .properties = {{"index_type", "ivf"},
                            {"metric_type", "inner_product"},
                            {"dim", std::to_string(kDim)},
                            {"nlist", "4"}}},
            {.name = "ivf_on_disk",
             .properties = {{"index_type", "ivf_on_disk"},
                            {"metric_type", "l2_distance"},
                            {"dim", std::to_string(kDim)},
                            {"nlist", "4"}}},
    };
    return cases;
}

struct Format {
    const char* name;
    InvertedIndexStorageFormatPB format;
};

constexpr Format kFormats[] = {
        {.name = "v2", .format = InvertedIndexStorageFormatPB::V2},
        {.name = "snii", .format = InvertedIndexStorageFormatPB::SNII},
};

TabletIndex ann_meta(const AnnCase& ann_case) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::ANN);
    pb.set_index_id(kIndexId);
    pb.set_index_name(std::string("ann_golden_") + ann_case.name);
    pb.add_col_unique_id(0);
    for (const auto& [key, value] : ann_case.properties) {
        pb.mutable_properties()->insert({key, value});
    }
    TabletIndex meta;
    meta.init_from_pb(pb);
    return meta;
}

// Fixed vectors with small integer coordinates, -32 to 31. Every distance and inner product between
// them is then exact in float (an L2 distance is at most 8 * 63^2 = 31752 < 2^24), whatever order
// the compiled faiss adds the terms in. With fractional coordinates the ASAN and RELEASE builds
// wrote different HNSW graphs, each build the same one every time: -O0 and -O3 round the sums
// differently, and the graph follows the comparisons.
std::vector<float> vectors() {
    std::vector<float> values(static_cast<size_t>(kRows) * kDim);
    for (uint32_t row = 0; row < kRows; ++row) {
        for (uint32_t d = 0; d < kDim; ++d) {
            const uint32_t mixed = row * 2654435761U + d * 40503U + (row >> 3) * 977U;
            values[static_cast<size_t>(row) * kDim + d] =
                    static_cast<float>(static_cast<int32_t>(mixed % 64U) - 32);
        }
    }
    return values;
}

// Writes the vectors into an ANN index; returns the index path prefix.
std::string write_ann_index(const std::string& name, InvertedIndexStorageFormatPB format,
                            const TabletIndex& meta) {
    const std::string prefix = std::string(kTestDir) + "/" + name;
    const auto fs = io::global_local_filesystem();
    io::FileWriterPtr file_writer;
    EXPECT_TRUE(
            fs->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer)
                    .ok());
    IndexFileWriter index_file_writer(fs, prefix, "ann_golden_rowset", /*seg_id=*/0, format,
                                      std::move(file_writer), /*can_use_ram_dir=*/false,
                                      /*tablet_id=*/8800);
    AnnIndexColumnWriter writer(&index_file_writer, &meta);
    EXPECT_TRUE(writer.init().ok());
    const std::vector<float> values = vectors();
    std::vector<uint64_t> offsets(kRows + 1);
    for (uint32_t row = 0; row <= kRows; ++row) {
        offsets[row] = static_cast<uint64_t>(row) * kDim;
    }
    EXPECT_TRUE(writer.add_array_values(sizeof(float),
                                        reinterpret_cast<const uint8_t*>(values.data()),
                                        /*null_map=*/nullptr, offsets.data(), kRows)
                        .ok());
    EXPECT_TRUE(writer.finish().ok());
    EXPECT_TRUE(index_file_writer.begin_close().ok());
    EXPECT_TRUE(index_file_writer.finish_close().ok());
    return prefix;
}

struct SubFile {
    uint64_t length = 0;
    uint64_t digest = 0;

    bool operator==(const SubFile&) const = default;
};

// Every sub-file of the ANN index, by name, read through the production reader.
std::map<std::string, SubFile> sub_files(const std::string& prefix,
                                         InvertedIndexStorageFormatPB format,
                                         const TabletIndex& meta) {
    std::map<std::string, SubFile> files;
    IndexFileReader reader(io::global_local_filesystem(), prefix, format);
    const Status status = reader.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    auto directory = reader.open(&meta);
    EXPECT_TRUE(directory.has_value()) << directory.error();
    if (!directory.has_value()) {
        return files;
    }
    std::vector<std::string> names;
    EXPECT_TRUE(directory.value()->list(&names));
    for (const std::string& name : names) {
        lucene::store::IndexInput* raw = nullptr;
        CLuceneError error;
        EXPECT_TRUE(directory.value()->openInput(name.c_str(), raw, error)) << error.what();
        std::unique_ptr<lucene::store::IndexInput> input(raw);
        std::vector<uint8_t> bytes(static_cast<size_t>(input->length()));
        input->readBytes(bytes.data(), static_cast<int32_t>(bytes.size()));
        input->close();
        files[name] = {.length = bytes.size(),
                       .digest = HashUtil::xxHash64WithSeed(
                               reinterpret_cast<const char*>(bytes.data()), bytes.size(), 0)};
    }
    return files;
}

} // namespace

class AnnWriteGolden : public testing::Test {
protected:
    void SetUp() override {
        _saved_omp_threads_limit = config::omp_threads_limit;
        config::omp_threads_limit = 1;
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
    }

    void TearDown() override {
        config::omp_threads_limit = _saved_omp_threads_limit;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

private:
    int32_t _saved_omp_threads_limit = -1;
};

TEST_F(AnnWriteGolden, MatchesTheFrozenDigests) {
    test::GoldenFile file(kGoldenFile);
    for (const Format& format : kFormats) {
        for (const AnnCase& ann_case : ann_cases()) {
            const std::string name = std::string(format.name) + "/" + ann_case.name;
            const TabletIndex meta = ann_meta(ann_case);
            std::map<std::string, SubFile> first;
            for (int round = 0; round < 2; ++round) {
                const std::string prefix =
                        write_ann_index(std::string(format.name) + "_" + ann_case.name + "_" +
                                                std::to_string(round),
                                        format.format, meta);
                const std::map<std::string, SubFile> files = sub_files(prefix, format.format, meta);
                ASSERT_FALSE(files.empty()) << name;
                if (round == 0) {
                    first = files;
                } else {
                    EXPECT_EQ(files, first) << name << " is not written the same way twice";
                }
            }
            for (const auto& [sub_file, digest] : first) {
                file.check(name, sub_file, digest.length, digest.digest, name);
            }
        }
    }
    file.finish([](const std::string&) { return true; }, true);
}

} // namespace doris::segment_v2
