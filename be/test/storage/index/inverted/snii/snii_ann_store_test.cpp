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

// SNII's ANN storage on its own: SniiAnnSource reading an ANN blob's sub-files through the
// container's reader, and SniiAnnStaging holding faiss's output until the container is sealed.
// The container here is a few bytes in memory whose reader records every read it is handed, so
// the tests see exactly what goes to the file, with which context.

#include "storage/index/inverted/snii/snii_ann_store.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/snii/format/metadata_directory.h"
#include "storage/index/inverted/snii/snii_doris_adapter.h"
#include "storage/index/inverted/snii/staged_file_probe.h"
#include "storage/options.h"
#include "util/debug_points.h"
#include "util/slice.h"

namespace doris::segment_v2::snii_doris {
namespace {

constexpr const char* kTestDir = "./ut_dir/snii_ann_store_test";

void assert_ok(const Status& status) {
    ASSERT_TRUE(status.ok()) << status.to_string();
}

// Distinct per position, so a read at a wrong offset cannot come back with the right bytes.
std::vector<uint8_t> pattern(size_t size, uint8_t seed) {
    std::vector<uint8_t> bytes(size);
    for (size_t i = 0; i < size; ++i) {
        bytes[i] = static_cast<uint8_t>(seed + i * 7 + (i >> 8));
    }
    return bytes;
}

// What one read handed to the file carried.
struct FileRead {
    uint64_t offset = 0;
    size_t size = 0;
    bool is_index_data = false;
    bool is_inverted_index = false;
    io::FileCacheMissPolicy miss_policy = io::FileCacheMissPolicy::READ_THROUGH_AND_WRITE_BACK;
    io::FileCacheStatistics* stats = nullptr;
};

// A container in memory that records every read and can be made to fail them.
class RecordingFileReader final : public io::FileReader {
public:
    explicit RecordingFileReader(std::vector<uint8_t> bytes) : _bytes(std::move(bytes)) {}

    Status close() override { return Status::OK(); }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return _bytes.size(); }
    bool closed() const override { return false; }
    int64_t mtime() const override { return 0; }

    const std::vector<FileRead>& reads() const { return _reads; }
    // Fails every read after recording it; an OK status makes them succeed again.
    void fail_reads_with(Status status) { _failure = std::move(status); }
    // Makes every read take at least this long, so a timer around it cannot read zero.
    void set_read_delay(std::chrono::microseconds delay) { _delay = delay; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        _reads.push_back({.offset = offset,
                          .size = result.size,
                          .is_index_data = io_ctx->is_index_data,
                          .is_inverted_index = io_ctx->is_inverted_index,
                          .miss_policy = io_ctx->file_cache_miss_policy,
                          .stats = io_ctx->file_cache_stats});
        std::this_thread::sleep_for(_delay);
        RETURN_IF_ERROR(_failure);
        std::memcpy(result.data, _bytes.data() + offset, result.size);
        *bytes_read = result.size;
        return Status::OK();
    }

private:
    const std::vector<uint8_t> _bytes;
    const io::Path _path {"snii_ann_store_test.idx"};
    std::vector<FileRead> _reads;
    Status _failure;
    std::chrono::microseconds _delay {0};
};

// Enables one fault injection site for a scope and restores the process-wide config afterwards.
class ScopedDebugPoint {
public:
    explicit ScopedDebugPoint(const std::string& name) : _was_enabled(config::enable_debug_points) {
        config::enable_debug_points = true;
        DebugPoints::instance()->clear();
        DebugPoints::instance()->add(name);
    }
    ~ScopedDebugPoint() {
        DebugPoints::instance()->clear();
        config::enable_debug_points = _was_enabled;
    }

private:
    const bool _was_enabled;
};

class SniiAnnStoreTest : public testing::Test {
protected:
    // The container: a 16-byte head, then ann.faiss, then 4 KiB of another index's blob.
    static constexpr uint64_t kFileOffset = 16;
    static constexpr uint64_t kFileLength = 10000;
    static constexpr uint64_t kNextBlobLength = 4096;
    static constexpr size_t kBufferSize = 4096;

    void SetUp() override {
        // Staging files live under ExecEnv's tmp file dirs. Installed when no one has, as
        // SniiAnnContainerTest does: another suite's TearDown may have cleared them.
        if (ExecEnv::GetInstance()->get_tmp_file_dirs() == nullptr) {
            const std::string tmp_dir = std::string(kTestDir) + "/tmp";
            (void)io::global_local_filesystem()->delete_directory(tmp_dir);
            assert_ok(io::global_local_filesystem()->create_directory(tmp_dir));
            std::vector<StorePath> paths;
            paths.emplace_back(tmp_dir, -1);
            auto tmp_file_dirs = std::make_unique<TmpFileDirs>(paths);
            assert_ok(tmp_file_dirs->init());
            ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
        }
        _container = pattern(kFileOffset + kFileLength + kNextBlobLength, 0x2B);
        _file = std::make_shared<RecordingFileReader>(_container);
        snii::format::LogicalIndexMetadataRef entry;
        entry.index_id = 71;
        entry.kind = snii::format::LogicalIndexKind::kAnn;
        entry.files = {snii::format::NamedBlobFileRef {
                .name = "ann.faiss", .offset = kFileOffset, .length = kFileLength}};
        _source = std::make_unique<SniiAnnSource>(std::make_shared<DorisSniiFileReader>(_file),
                                                  std::move(entry), kBufferSize);
    }

    std::unique_ptr<AnnIndexInput> open_faiss() {
        auto input = _source->open_input("ann.faiss");
        EXPECT_TRUE(input.has_value()) << input.error().to_string();
        return input.has_value() ? std::move(input.value()) : nullptr;
    }

    // Reads [offset, offset + size) of ann.faiss and checks the bytes against the container.
    void read_and_check(AnnIndexInput* input, uint64_t offset, size_t size,
                        const io::IOContext* io_ctx = nullptr) {
        std::vector<uint8_t> out(size);
        assert_ok(input->read_at(offset, out.data(), size, io_ctx));
        const auto begin = _container.begin() + static_cast<int64_t>(kFileOffset + offset);
        EXPECT_EQ(out, std::vector<uint8_t>(begin, begin + static_cast<int64_t>(size)))
                << "bytes [" << offset << ", +" << size << ") of ann.faiss";
    }

    // The reads the file was handed, as (offset in the container, size) pairs.
    std::vector<std::pair<uint64_t, size_t>> file_reads() const {
        std::vector<std::pair<uint64_t, size_t>> reads;
        for (const FileRead& read : _file->reads()) {
            reads.emplace_back(read.offset, read.size);
        }
        return reads;
    }

    std::vector<uint8_t> _container;
    std::shared_ptr<RecordingFileReader> _file;
    std::unique_ptr<SniiAnnSource> _source;
};

using Reads = std::vector<std::pair<uint64_t, size_t>>;

// faiss::read_index reads an index in fields of a few bytes; one refill serves a run of them, and
// the refill that reaches the end of the file stops there.
TEST_F(SniiAnnStoreTest, SmallReadsShareOneRefillCutAtTheEndOfTheFile) {
    auto input = open_faiss();
    ASSERT_NE(input, nullptr);
    EXPECT_EQ(input->length(), kFileLength);

    // A fourcc, a dimension, a count.
    read_and_check(input.get(), 0, 4);
    read_and_check(input.get(), 4, 4);
    read_and_check(input.get(), 8, 8);
    EXPECT_EQ(file_reads(), (Reads {{kFileOffset, kBufferSize}}));

    // A refill at 8192 would run 2288 bytes past the file, into the next blob: it is cut at 1808.
    read_and_check(input.get(), 8192, 100);
    read_and_check(input.get(), 9990, 10);
    EXPECT_EQ(file_reads(), (Reads {{kFileOffset, kBufferSize}, {kFileOffset + 8192, 1808}}));
}

// The rest of a read that is at least as large as the buffer goes straight into the caller's
// memory, and leaves the buffer empty, as BufferedIndexInput leaves its own.
TEST_F(SniiAnnStoreTest, AReadAsLargeAsTheBufferGoesStraightToTheFile) {
    auto input = open_faiss();
    ASSERT_NE(input, nullptr);

    read_and_check(input.get(), 100, kBufferSize);
    read_and_check(input.get(), 0, 4);
    EXPECT_EQ(file_reads(), (Reads {{kFileOffset + 100, kBufferSize}, {kFileOffset, kBufferSize}}));

    // The buffer holds [0, 4096): it serves the first 96 bytes, and the other 4104 are read past
    // it. The read after that finds the buffer empty and refills it from where it starts.
    read_and_check(input.get(), 4000, 4200);
    read_and_check(input.get(), 8, 8);
    EXPECT_EQ(file_reads(), (Reads {{kFileOffset + 100, kBufferSize},
                                    {kFileOffset, kBufferSize},
                                    {kFileOffset + 4096, 4104},
                                    {kFileOffset + 8, kBufferSize}}));
}

// The container's reader bounds a read by the container, not by the sub-file. A read past the
// end of ann.faiss is refused before any byte is read, so a corrupt length cannot serve the next
// blob as the file's tail.
TEST_F(SniiAnnStoreTest, AReadPastTheEndOfTheFileFailsAndReadsNothing) {
    auto input = open_faiss();
    ASSERT_NE(input, nullptr);

    std::vector<uint8_t> out(64, 0xEE);
    // The last 56 bytes of the file and 8 of the next blob.
    const Status status = input->read_at(kFileLength - 56, out.data(), out.size(), nullptr);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status.to_string();
    EXPECT_NE(status.to_string().find("ann.faiss"), std::string::npos) << status.to_string();
    // A start past the end, and a size that would wrap offset + size around.
    EXPECT_FALSE(input->read_at(kFileLength + 1, out.data(), 0, nullptr).ok());
    EXPECT_FALSE(input->read_at(8, out.data(), std::numeric_limits<size_t>::max(), nullptr).ok());
    EXPECT_TRUE(_file->reads().empty()) << "a refused read reached the file";
    EXPECT_EQ(out, std::vector<uint8_t>(64, 0xEE)) << "a refused read wrote into the output";

    // The file's last bytes are served, by a refill that stops at the end of the file.
    read_and_check(input.get(), kFileLength - 8, 8);
    EXPECT_EQ(file_reads(), (Reads {{kFileOffset + kFileLength - 8, 8}}));
}

// A file the container's reader cannot find is an I/O failure for ANN, which has no fallback:
// index callers take NOT_FOUND for an index that is not there. The read goes out as index data,
// not metadata, whatever the caller's context says, as it did through FSIndexInput.
TEST_F(SniiAnnStoreTest, AFileTheReaderCannotFindIsAnIoError) {
    auto input = open_faiss();
    ASSERT_NE(input, nullptr);

    io::IOContext io_ctx;
    io_ctx.is_index_data = true;
    std::vector<uint8_t> out(4);
    _file->fail_reads_with(Status::NotFound("the object is gone"));
    Status status = input->read_at(0, out.data(), out.size(), &io_ctx);
    EXPECT_TRUE(status.is<ErrorCode::IO_ERROR>()) << status.to_string();
    EXPECT_NE(status.to_string().find("the object is gone"), std::string::npos)
            << status.to_string();
    EXPECT_NE(status.to_string().find("ann.faiss"), std::string::npos) << status.to_string();
    ASSERT_EQ(_file->reads().size(), 1U);
    EXPECT_FALSE(_file->reads()[0].is_index_data);
    EXPECT_TRUE(_file->reads()[0].is_inverted_index);

    // Any other failure keeps its own code.
    _file->fail_reads_with(Status::InternalError("the disk is gone"));
    status = input->read_at(0, out.data(), out.size(), &io_ctx);
    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>()) << status.to_string();

    // A failed refill buffers nothing: once the file reads again, so does the input.
    _file->fail_reads_with(Status::OK());
    read_and_check(input.get(), 0, 4);
    EXPECT_EQ(_file->reads().size(), 3U);
}

// Every read is charged to the context of its own call, the I/O time included: an IVF_ON_DISK
// input lives as long as the loaded index and reads for every query, each with its own context.
// A read the buffer serves costs nothing.
TEST_F(SniiAnnStoreTest, ReadsAreChargedToTheContextOfTheirCall) {
    auto input = open_faiss();
    ASSERT_NE(input, nullptr);
    constexpr auto kDelay = std::chrono::microseconds(200);
    _file->set_read_delay(kDelay);

    io::FileCacheStatistics load_stats;
    io::IOContext load_ctx;
    load_ctx.file_cache_stats = &load_stats;
    io::FileCacheStatistics query_stats;
    io::IOContext query_ctx;
    query_ctx.file_cache_stats = &query_stats;

    read_and_check(input.get(), 0, 4, &load_ctx);
    read_and_check(input.get(), 8, 4, &query_ctx);
    read_and_check(input.get(), 8192, 1000, &query_ctx);

    const int64_t delay_ns = std::chrono::nanoseconds(kDelay).count();
    EXPECT_EQ(load_stats.inverted_index_range_read_count, 1);
    EXPECT_EQ(load_stats.inverted_index_read_bytes, static_cast<int64_t>(kBufferSize));
    EXPECT_GE(load_stats.inverted_index_io_timer, delay_ns);
    EXPECT_EQ(query_stats.inverted_index_range_read_count, 1);
    EXPECT_EQ(query_stats.inverted_index_read_bytes, 1808);
    EXPECT_GE(query_stats.inverted_index_io_timer, delay_ns);
    ASSERT_EQ(_file->reads().size(), 2U);
    EXPECT_EQ(_file->reads()[0].stats, &load_stats);
    EXPECT_EQ(_file->reads()[1].stats, &query_stats);
}

// With inverted_index_snii_read_no_write_file_cache on, a query's miss is not written back to the
// file cache: ANN reads come from the SNII container, and the variable covers every SNII read.
// With it off, the default, they read through the cache as before.
TEST_F(SniiAnnStoreTest, TheSniiNoWriteFileCacheSwitchCoversAnnReads) {
    auto input = open_faiss();
    ASSERT_NE(input, nullptr);

    io::IOContext io_ctx;
    io_ctx.reader_type = ReaderType::READER_QUERY;
    io_ctx.inverted_index_snii_read_no_write_file_cache = true;
    read_and_check(input.get(), 0, 4, &io_ctx);
    io_ctx.inverted_index_snii_read_no_write_file_cache = false;
    read_and_check(input.get(), 4096, 4, &io_ctx);

    ASSERT_EQ(_file->reads().size(), 2U);
    EXPECT_EQ(_file->reads()[0].miss_policy, io::FileCacheMissPolicy::REMOTE_ONLY_ON_MISS);
    EXPECT_EQ(_file->reads()[1].miss_policy, io::FileCacheMissPolicy::READ_THROUGH_AND_WRITE_BACK);
}

// faiss opens only the files its index type writes, so a blob lacking one is a damaged container.
TEST_F(SniiAnnStoreTest, OpenInputRefusesAFileTheBlobLacks) {
    auto input = _source->open_input("ann.ivfdata");
    ASSERT_FALSE(input.has_value());
    EXPECT_TRUE(input.error().is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>())
            << input.error().to_string();
    EXPECT_NE(input.error().to_string().find("Failed to open index file: ann.ivfdata"),
              std::string::npos)
            << input.error().to_string();
    EXPECT_EQ(_source->debug_string(), "SniiAnnSource(index_id=71, suffix=)");
}

// close() may be called again and does nothing then, after a failed close as well: the staged
// file is never finalized twice, and the destructor of a closed output closes nothing.
TEST_F(SniiAnnStoreTest, AStagingOutputClosesOnce) {
    SniiAnnStaging staging;
    const std::vector<uint8_t> bytes = pattern(100, 0x11);
    {
        auto output = staging.create_output("ann.faiss");
        ASSERT_TRUE(output.has_value()) << output.error().to_string();
        assert_ok(output.value()->append(bytes.data(), bytes.size()));
        assert_ok(output.value()->close());
        assert_ok(output.value()->close());
    }
    EXPECT_EQ(staging.staged_bytes(), bytes.size());

    ScopedDebugPoint fault("StagedBlobFile::finalize_error");
    auto output = staging.create_output("ann.ivfdata");
    ASSERT_TRUE(output.has_value()) << output.error().to_string();
    assert_ok(output.value()->append(bytes.data(), bytes.size()));
    const Status failed = output.value()->close();
    EXPECT_FALSE(failed.ok());
    EXPECT_NE(failed.to_string().find(
                      "Failed to close ann.ivfdata: injected blob staging finalize failure"),
              std::string::npos)
            << failed.to_string();
    assert_ok(output.value()->close());
}

// The staged bytes are faiss's bytes, whatever the sizes of the appends that brought them: many
// small ones through the 64 KiB buffer, one larger than the buffer straight to the file, and the
// empty ones faiss makes of an empty vector, a null pointer with a size of 0. The files come out
// in name order, the reverse of the order faiss writes IVF_ON_DISK's two in.
TEST_F(SniiAnnStoreTest, StagedFilesComeOutWholeAndInNameOrder) {
    SniiAnnStaging staging;
    const std::vector<uint8_t> ivfdata = pattern(200 * 1024, 0x33);
    const std::vector<uint8_t> faiss = pattern(100, 0x55);
    {
        auto output = staging.create_output("ann.ivfdata");
        ASSERT_TRUE(output.has_value()) << output.error().to_string();
        constexpr size_t kSmallBytes = 70 * 1024;
        for (size_t offset = 0; offset < kSmallBytes; offset += 7) {
            assert_ok(output.value()->append(ivfdata.data() + offset, 7));
        }
        assert_ok(
                output.value()->append(ivfdata.data() + kSmallBytes, ivfdata.size() - kSmallBytes));
        assert_ok(output.value()->close());
    }
    {
        auto output = staging.create_output("ann.faiss");
        ASSERT_TRUE(output.has_value()) << output.error().to_string();
        assert_ok(output.value()->append(nullptr, 0));
        assert_ok(output.value()->append(faiss.data(), faiss.size()));
        assert_ok(output.value()->append(nullptr, 0));
        assert_ok(output.value()->close());
    }
    EXPECT_EQ(staging.staged_bytes(), ivfdata.size() + faiss.size());
    EXPECT_NE(staging.debug_string().find("SniiAnnStaging(files=2"), std::string::npos)
            << staging.debug_string();

    std::vector<snii::writer::BlobFileSource> sources = staging.take_blob_sources();
    EXPECT_EQ(staging.staged_bytes(), 0U);
    ASSERT_EQ(sources.size(), 2U);
    EXPECT_EQ(sources[0].name, "ann.faiss");
    EXPECT_EQ(sources[1].name, "ann.ivfdata");
    const auto expect_staged = [](const snii::writer::BlobFileSource& source,
                                  const std::vector<uint8_t>& expected) {
        SCOPED_TRACE(source.name);
        ASSERT_EQ(source.length, expected.size());
        std::vector<uint8_t> staged(source.length);
        assert_ok(source.read_fn(0, staged.size(), staged.data()));
        EXPECT_EQ(staged, expected);
    };
    expect_staged(sources[0], faiss);
    expect_staged(sources[1], ivfdata);
}

// Creating a name again replaces its file, as a filesystem directory truncates it, and dropping
// the staging's files unlinks them even while the staging itself lives on.
TEST_F(SniiAnnStoreTest, ARecreatedFileReplacesTheOldOneAndDiscardUnlinks) {
    const std::set<std::string> before = doris::snii_test::snii_staged_files("ann.faiss");
    SniiAnnStaging staging;
    const std::vector<uint8_t> old_bytes = pattern(300, 0x01);
    const std::vector<uint8_t> new_bytes = pattern(200, 0x02);
    for (const std::vector<uint8_t>* bytes : {&old_bytes, &new_bytes}) {
        auto output = staging.create_output("ann.faiss");
        ASSERT_TRUE(output.has_value()) << output.error().to_string();
        assert_ok(output.value()->append(bytes->data(), bytes->size()));
        assert_ok(output.value()->close());
    }
    EXPECT_EQ(staging.staged_bytes(), new_bytes.size());
    EXPECT_EQ(doris::snii_test::snii_staged_files("ann.faiss").size(), before.size() + 1)
            << "the replaced file is still on the temp filesystem";

    staging.discard_staged_files();
    EXPECT_EQ(staging.staged_bytes(), 0U);
    EXPECT_EQ(doris::snii_test::snii_staged_files("ann.faiss"), before);
}

} // namespace
} // namespace doris::segment_v2::snii_doris
