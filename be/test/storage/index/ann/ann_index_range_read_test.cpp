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

// The I/O an ANN index costs on V1, V2 and SNII: how many range reads, and how many bytes, one
// load and three searches send to the index file. A range read is one read handed to the file
// reader -- by FSIndexInput::readInternal for V1 and V2, by DorisSniiFileReader for SNII -- counted
// in inverted_index_range_read_count of the IOContext's FileCacheStatistics. V1 and V2 read their
// files through CLucene's buffered inputs, and the numbers below were recorded on that code:
// whatever ANN reads through, they must keep issuing exactly these reads, or a cold query on
// remote storage changes cost. The reads are cold and bypass the file cache: every reader opens
// with NO_CACHE, and these local files never reach the file cache anyway.
//
// The sizes are chosen so that every file of these indexes is over the 128 KiB V1 keeps in its
// header, except the small ann.faiss of IVF_ON_DISK: V1 then loads that one from memory, and
// reads every other file through the compound file like V2 does.

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <memory>
#include <ostream>
#include <roaring/roaring.hh>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "exec/scan/vector_search_user_params.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "storage/cache/ann_index_ivf_list_cache.h"
#include "storage/index/ann/ann_index_reader.h"
#include "storage/index/ann/ann_index_writer.h"
#include "storage/index/ann/ann_search_params.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/common/inverted_index_desc.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {

constexpr const char* kTestDir = "./ut_dir/ann_index_range_read_test";
constexpr int64_t kIndexId = 4242;
constexpr uint32_t kDim = 32;
constexpr uint32_t kRows = 2048;
constexpr uint32_t kProbeRow = 17;

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
                            {"metric_type", "l2_distance"},
                            {"dim", std::to_string(kDim)},
                            {"nlist", "16"}}},
            {.name = "ivf_on_disk",
             .properties = {{"index_type", "ivf_on_disk"},
                            {"metric_type", "l2_distance"},
                            {"dim", std::to_string(kDim)},
                            {"nlist", "16"}}},
    };
    return cases;
}

struct Format {
    const char* name;
    InvertedIndexStorageFormatPB format;
};

constexpr Format kFormats[] = {
        {.name = "v1", .format = InvertedIndexStorageFormatPB::V1},
        {.name = "v2", .format = InvertedIndexStorageFormatPB::V2},
        {.name = "snii", .format = InvertedIndexStorageFormatPB::SNII},
};

struct Reads {
    int64_t count = 0;
    int64_t bytes = 0;

    bool operator==(const Reads&) const = default;
};

// One load, then three searches, each with its own IOContext the way three queries would have.
struct CaseReads {
    Reads load;
    // nprobe 4: IVF_ON_DISK reads 4 of the 16 lists.
    Reads narrow_topn;
    // Every list: IVF_ON_DISK reads the 12 it has not cached yet.
    Reads wide_range;
    // Every list again: all of them come out of the list cache.
    Reads wide_topn;

    bool operator==(const CaseReads&) const = default;
};

std::ostream& operator<<(std::ostream& os, const Reads& reads) {
    return os << "{" << reads.count << ", " << reads.bytes << "}";
}

std::ostream& operator<<(std::ostream& os, const CaseReads& reads) {
    return os << "{.load = " << reads.load << ", .narrow_topn = " << reads.narrow_topn
              << ", .wide_range = " << reads.wide_range << ", .wide_topn = " << reads.wide_topn
              << "}";
}

// Recorded on the CLucene-directory code (6e41ceb8f93) with inverted_index_read_buffer_size 4096.
// HNSW and IVF searches run in memory, so only their loads read: the large vectors in one read
// each, the small fields coalesced by the 4 KiB buffer. IVF_ON_DISK loads only metadata, which the
// first 4 KiB of the file already hold, and reads its lists at search time, two borrows per list
// through the list cache: about 16 KiB of codes in one direct read, then the ids from one refill.
// The narrow search reads 4 lists (8 reads), the range search the other 12 (24 reads).
//
// SNII reads its ANN blob through the container's own reader and a buffer of the same 4 KiB, and
// its loads include the 2 reads IndexFileReader::init makes of the container's tail pointer and
// directory. Recorded on the CLucene-directory code (00da6453bb6), which read the blob through a
// compound input over a second stream of the container, the same files cost: HNSW load {10,
// 557182}, IVF load {35, 280979}, IVF_ON_DISK load {3, 4191}, narrow top-N {8, 81495}, range
// search {24, 240248}. The native reads are as many, and where they read less, it is the stream's
// own refill running past the end of a sub-file that is gone: the 2602-byte ann.faiss of
// IVF_ON_DISK cost a 4096-byte read, now a read of its 2602 bytes.
const std::map<std::string, CaseReads>& expected_reads() {
    constexpr Reads kNone {.count = 0, .bytes = 0};
    static const std::map<std::string, CaseReads> expected = {
            {"v1/hnsw",
             {.load = {.count = 9, .bytes = 557141},
              .narrow_topn = kNone,
              .wide_range = kNone,
              .wide_topn = kNone}},
            {"v1/ivf",
             {.load = {.count = 34, .bytes = 280870},
              .narrow_topn = kNone,
              .wide_range = kNone,
              .wide_topn = kNone}},
            {"v1/ivf_on_disk",
             {.load = {.count = 1, .bytes = 4096},
              .narrow_topn = {.count = 8, .bytes = 81400},
              .wide_range = {.count = 24, .bytes = 240248},
              .wide_topn = kNone}},
            {"v2/hnsw",
             {.load = {.count = 9, .bytes = 557167},
              .narrow_topn = kNone,
              .wide_range = kNone,
              .wide_topn = kNone}},
            {"v2/ivf",
             {.load = {.count = 34, .bytes = 280896},
              .narrow_topn = kNone,
              .wide_range = kNone,
              .wide_topn = kNone}},
            {"v2/ivf_on_disk",
             {.load = {.count = 1, .bytes = 4096},
              .narrow_topn = {.count = 8, .bytes = 81400},
              .wide_range = {.count = 24, .bytes = 240248},
              .wide_topn = kNone}},
            {"snii/hnsw",
             {.load = {.count = 10, .bytes = 557182},
              .narrow_topn = kNone,
              .wide_range = kNone,
              .wide_topn = kNone}},
            {"snii/ivf",
             {.load = {.count = 35, .bytes = 280911},
              .narrow_topn = kNone,
              .wide_range = kNone,
              .wide_topn = kNone}},
            {"snii/ivf_on_disk",
             {.load = {.count = 3, .bytes = 2697},
              .narrow_topn = {.count = 8, .bytes = 81400},
              .wide_range = {.count = 24, .bytes = 240248},
              .wide_topn = kNone}},
    };
    return expected;
}

TabletIndex ann_meta(const AnnCase& ann_case) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::ANN);
    pb.set_index_id(kIndexId);
    pb.set_index_name(std::string("ann_range_read_") + ann_case.name);
    pb.add_col_unique_id(0);
    for (const auto& [key, value] : ann_case.properties) {
        pb.mutable_properties()->insert({key, value});
    }
    TabletIndex meta;
    meta.init_from_pb(pb);
    return meta;
}

// Distinct vectors with small integer coordinates, -32 to 31, from a fixed LCG. Every distance
// between them is then exact in float, so the k-means lists, and with them the reads, do not
// depend on how the compiled faiss orders its sums.
std::vector<float> vectors() {
    std::vector<float> values(static_cast<size_t>(kRows) * kDim);
    uint32_t state = 0x9E3779B9U;
    for (float& value : values) {
        state = state * 1664525U + 1013904223U;
        value = static_cast<float>(static_cast<int32_t>((state >> 24) % 64U) - 32);
    }
    return values;
}

std::string write_ann_index(const std::string& prefix, InvertedIndexStorageFormatPB format,
                            const TabletIndex& meta) {
    const auto fs = io::global_local_filesystem();
    io::FileWriterPtr file_writer;
    // V1 writes one file per index from begin_close(); V2 and SNII write through this writer.
    if (format != InvertedIndexStorageFormatPB::V1) {
        EXPECT_TRUE(fs->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                    &file_writer)
                            .ok());
    }
    IndexFileWriter index_file_writer(fs, prefix, "ann_range_read_rowset", /*seg_id=*/0, format,
                                      std::move(file_writer), /*can_use_ram_dir=*/false,
                                      /*tablet_id=*/4200);
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

Reads reads_of(const io::FileCacheStatistics& stats) {
    return {.count = stats.inverted_index_range_read_count,
            .bytes = stats.inverted_index_read_bytes};
}

std::vector<float> probe_vector() {
    const std::vector<float> values = vectors();
    return {values.begin() + static_cast<int64_t>(kProbeRow) * kDim,
            values.begin() + static_cast<int64_t>(kProbeRow + 1) * kDim};
}

// A top-N search that must find the probe row's own vector at distance zero.
void search_topn(AnnIndexReader* reader, int nprobe, Reads* reads) {
    io::FileCacheStatistics stats;
    io::IOContext io_ctx;
    io_ctx.file_cache_stats = &stats;
    const std::vector<float> query = probe_vector();
    roaring::Roaring candidates;
    candidates.addRange(0, kRows);
    VectorSearchUserParams user_params;
    user_params.ivf_nprobe = nprobe;
    AnnTopNParam param {.query_value = query.data(),
                        .query_value_size = kDim,
                        .limit = 4,
                        ._user_params = user_params,
                        .roaring = &candidates,
                        .rows_of_segment = kRows,
                        .enable_result_cache = false};
    AnnIndexStats ann_stats;
    const Status status = reader->query(&io_ctx, &param, &ann_stats);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(param.row_ids, nullptr);
    ASSERT_NE(param.distance, nullptr);
    ASSERT_FALSE(param.row_ids->empty());
    EXPECT_EQ((*param.row_ids)[0], kProbeRow);
    EXPECT_EQ(param.distance[0], 0.0F);
    *reads = reads_of(stats);
}

// A range search around the probe row: only its own vector lies within the radius.
void search_range(AnnIndexReader* reader, int nprobe, Reads* reads) {
    io::FileCacheStatistics stats;
    io::IOContext io_ctx;
    io_ctx.file_cache_stats = &stats;
    const std::vector<float> query = probe_vector();
    roaring::Roaring candidates;
    candidates.addRange(0, kRows);
    AnnRangeSearchParams params;
    params.is_le_or_lt = true;
    params.query_value = query.data();
    params.radius = 0.5F;
    params.roaring = &candidates;
    params.enable_result_cache = false;
    VectorSearchUserParams user_params;
    user_params.ivf_nprobe = nprobe;
    AnnRangeSearchResult result;
    AnnIndexStats ann_stats;
    const Status status = reader->range_search(params, user_params, &result, &ann_stats, &io_ctx);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(result.roaring, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), 1U);
    EXPECT_TRUE(result.roaring->contains(kProbeRow));
    *reads = reads_of(stats);
}

} // namespace

class AnnIndexRangeReadTest : public testing::Test {
protected:
    void SetUp() override {
        if (ExecEnv::GetInstance()->get_tmp_file_dirs() == nullptr) {
            const std::string tmp_dir = std::string(kTestDir) + "_tmp";
            (void)io::global_local_filesystem()->delete_directory(tmp_dir);
            ASSERT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
            std::vector<StorePath> paths;
            paths.emplace_back(tmp_dir, -1);
            auto tmp_file_dirs = std::make_unique<TmpFileDirs>(paths);
            ASSERT_TRUE(tmp_file_dirs->init().ok());
            ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
        }
        // One OpenMP thread: k-means and the HNSW graph then come out the same on every build, and
        // so do the lists IVF_ON_DISK reads.
        _saved_omp_threads_limit = config::omp_threads_limit;
        config::omp_threads_limit = 1;
        // Cold reads with the file cache bypassed: every reader then opens with NO_CACHE. These
        // are local files, which never go through the file cache in any case.
        _saved_enable_file_cache = config::enable_file_cache;
        config::enable_file_cache = false;
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
        // IVF_ON_DISK searches read through this cache in production (exec_env_init installs it).
        // Installed only when nobody has, and torn down only by its installer.
        if (AnnIndexIVFListCache::instance() == nullptr) {
            (void)AnnIndexIVFListCache::create_global_cache(64 * 1024 * 1024);
            _owns_ivf_list_cache = true;
        }
    }

    void TearDown() override {
        if (_owns_ivf_list_cache) {
            AnnIndexIVFListCache::destroy_global_cache();
        }
        config::omp_threads_limit = _saved_omp_threads_limit;
        config::enable_file_cache = _saved_enable_file_cache;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    // A directory no earlier run in this process used: the list cache is keyed by the index path,
    // and a cache owned by another fixture may still hold the lists of a previous repetition.
    static std::string fresh_prefix(const std::string& name) {
        static int run = 0;
        return std::string(kTestDir) + "/" + name + "_" + std::to_string(run++);
    }

private:
    int32_t _saved_omp_threads_limit = -1;
    bool _saved_enable_file_cache = true;
    bool _owns_ivf_list_cache = false;
};

TEST_F(AnnIndexRangeReadTest, LoadAndSearchIssueTheRecordedReads) {
    for (const Format& format : kFormats) {
        for (const AnnCase& ann_case : ann_cases()) {
            const std::string name = std::string(format.name) + "/" + ann_case.name;
            SCOPED_TRACE(name);
            const TabletIndex meta = ann_meta(ann_case);
            const std::string prefix =
                    write_ann_index(fresh_prefix(std::string(format.name) + "_" + ann_case.name),
                                    format.format, meta);

            CaseReads actual;
            {
                auto reader = std::make_shared<AnnIndexReader>(
                        &meta, std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                                 prefix, format.format));
                io::FileCacheStatistics load_stats;
                io::IOContext load_ctx;
                load_ctx.file_cache_stats = &load_stats;
                const Status status = reader->load_index(&load_ctx);
                ASSERT_TRUE(status.ok()) << status.to_string();
                actual.load = reads_of(load_stats);
                search_topn(reader.get(), /*nprobe=*/4, &actual.narrow_topn);
                search_range(reader.get(), /*nprobe=*/32, &actual.wide_range);
                search_topn(reader.get(), /*nprobe=*/32, &actual.wide_topn);
                ASSERT_FALSE(HasFatalFailure());
            }
            const auto it = expected_reads().find(name);
            if (it == expected_reads().end()) {
                ADD_FAILURE() << "no recorded reads for " << name << ", got " << actual;
                continue;
            }
            EXPECT_EQ(actual, it->second) << name << " reads differently from the recorded code";
        }
    }
}

} // namespace doris::segment_v2
