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

// SniiBlobDirectory: the read-only lucene::store::Directory shim that lets
// CLucene/faiss readers consume opaque blob files straight out of an SNII
// container. Contract pins (design 2026-07-28 §5.1):
//   * openInput on a 0-length entry returns a length()==0 input -- it must NOT
//     throw CL_ERR_EmptyIndexSegment (the searcher would mistake an empty BKD
//     index for a corrupt one);
//   * close() never throws (~bkd_reader is noexcept and calls it);
//   * write operations throw UnsupportedOperation;
//   * clones read independently (read_at is stateless -- no shared cursor).

#include "storage/index/snii/snii_blob_directory.h"

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/store/IndexInput.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/snii_doris_adapter.h"

using doris::Status;
using doris::segment_v2::snii_doris::DorisSniiFileReader;
using doris::segment_v2::snii_doris::SniiBlobDirectory;
using doris::snii::format::LogicalIndexKind;
using doris::snii::format::LogicalIndexMetadataRef;
using doris::snii::format::NamedBlobFileRef;

namespace {

constexpr size_t kBigLen = 70000; // > BufferedIndexInput::BUFFER_SIZE

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_blob_dir_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".bin";
}

std::vector<uint8_t> Pattern(size_t n, uint8_t seed) {
    std::vector<uint8_t> out(n);
    for (size_t i = 0; i < n; ++i) {
        out[i] = static_cast<uint8_t>(seed + i * 31 + (i >> 7));
    }
    return out;
}

struct Fixture {
    std::string path;
    std::vector<uint8_t> payload_a = Pattern(100, 7);
    std::vector<uint8_t> payload_b = Pattern(kBigLen, 91);
    LogicalIndexMetadataRef entry;
    std::shared_ptr<DorisSniiFileReader> reader;
    doris::segment_v2::snii_doris::SniiBlobDirectoryPtr dir;

    void Build() {
        path = TempPath();
        {
            doris::io::FileWriterPtr writer;
            ASSERT_TRUE(doris::io::global_local_filesystem()->create_file(path, &writer).ok());
            ASSERT_TRUE(writer->append(doris::Slice(payload_a.data(), payload_a.size())).ok());
            ASSERT_TRUE(writer->append(doris::Slice(payload_b.data(), payload_b.size())).ok());
            ASSERT_TRUE(writer->close().ok());
        }
        doris::io::FileReaderSPtr file_reader;
        ASSERT_TRUE(doris::io::global_local_filesystem()->open_file(path, &file_reader).ok());
        reader = std::make_shared<DorisSniiFileReader>(std::move(file_reader));

        entry.index_id = 9;
        entry.index_suffix = "bkd";
        entry.kind = LogicalIndexKind::kAnn;
        entry.files = {
                NamedBlobFileRef {.name = "a", .offset = 0, .length = payload_a.size()},
                NamedBlobFileRef {
                        .name = "b", .offset = payload_a.size(), .length = payload_b.size()},
                NamedBlobFileRef {
                        .name = "c", .offset = payload_a.size() + payload_b.size(), .length = 0},
        };
        ASSERT_TRUE(SniiBlobDirectory::open(reader, entry, reader->size(), &dir).ok());
        ASSERT_NE(nullptr, dir);
    }

    ~Fixture() {
        if (dir != nullptr) {
            dir->close();
        }
        std::remove(path.c_str());
    }
};

TEST(SniiBlobDirectory, ListsAndSizesFiles) {
    Fixture fx;
    fx.Build();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    std::vector<std::string> names;
    ASSERT_TRUE(fx.dir->list(&names));
    std::sort(names.begin(), names.end());
    EXPECT_EQ((std::vector<std::string> {"a", "b", "c"}), names);

    EXPECT_TRUE(fx.dir->fileExists("a"));
    EXPECT_FALSE(fx.dir->fileExists("nope"));
    EXPECT_EQ(static_cast<int64_t>(fx.payload_a.size()), fx.dir->fileLength("a"));
    EXPECT_EQ(0, fx.dir->fileLength("c"));
    EXPECT_EQ(0, fx.dir->fileModified("a"));
    EXPECT_FALSE(fx.dir->toString().empty());
}

TEST(SniiBlobDirectory, OpenInputReadsSeeksAndClonesIndependently) {
    Fixture fx;
    fx.Build();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());

    lucene::store::IndexInput* raw = nullptr;
    CLuceneError err;
    ASSERT_TRUE(fx.dir->openInput("a", raw, err));
    std::unique_ptr<lucene::store::IndexInput> input(raw);
    ASSERT_EQ(static_cast<int64_t>(fx.payload_a.size()), input->length());

    std::vector<uint8_t> got(fx.payload_a.size(), 0);
    input->readBytes(got.data(), static_cast<int32_t>(got.size()));
    EXPECT_EQ(0, std::memcmp(fx.payload_a.data(), got.data(), got.size()));

    input->seek(37);
    EXPECT_EQ(fx.payload_a[37], input->readByte());

    // Clone keeps its own position.
    std::unique_ptr<lucene::store::IndexInput> clone(input->clone());
    clone->seek(90);
    input->seek(10);
    EXPECT_EQ(fx.payload_a[90], clone->readByte());
    EXPECT_EQ(fx.payload_a[10], input->readByte());

    // A read larger than the internal buffer lands directly in the caller
    // buffer (BufferedIndexInput big-read bypass) and must still be exact.
    lucene::store::IndexInput* raw_b = nullptr;
    ASSERT_TRUE(fx.dir->openInput("b", raw_b, err));
    std::unique_ptr<lucene::store::IndexInput> input_b(raw_b);
    std::vector<uint8_t> got_b(kBigLen, 0);
    input_b->readBytes(got_b.data(), static_cast<int32_t>(got_b.size()));
    EXPECT_EQ(0, std::memcmp(fx.payload_b.data(), got_b.data(), got_b.size()));

    // Reading past the sub-file end throws CL_ERR_IO -- never bleeds into the
    // neighbouring blob bytes. NOTE: BufferedIndexInput clamps refill() to
    // length() and throws before ever calling readInternal, so this pins
    // CLucene's guard plus our length() being the SUB-FILE length (not the
    // container's); the shim's own bounds check is belt-and-braces behind it.
    input->seek(static_cast<int64_t>(fx.payload_a.size()) - 1);
    std::vector<uint8_t> two(2, 0);
    bool threw = false;
    try {
        input->readBytes(two.data(), 2);
    } catch (CLuceneError& e) {
        threw = true;
        EXPECT_EQ(CL_ERR_IO, e.number());
    }
    EXPECT_TRUE(threw);
    input->close();
    input_b->close();
    clone->close();
}

TEST(SniiBlobDirectory, ZeroLengthEntryOpensAsEmptyInput) {
    Fixture fx;
    fx.Build();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    lucene::store::IndexInput* raw = nullptr;
    CLuceneError err;
    // MUST NOT throw CL_ERR_EmptyIndexSegment: an empty BKD segment stores
    // 0-byte `bkd` / `bkd_index` files and bkd_reader::open() reads all three
    // before deciding the index is empty.
    ASSERT_TRUE(fx.dir->openInput("c", raw, err)) << err.what();
    std::unique_ptr<lucene::store::IndexInput> input(raw);
    EXPECT_EQ(0, input->length());
    input->close();
}

TEST(SniiBlobDirectory, MissingFileFailsWithoutThrowing) {
    Fixture fx;
    fx.Build();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    lucene::store::IndexInput* raw = nullptr;
    CLuceneError err;
    EXPECT_FALSE(fx.dir->openInput("nope", raw, err));
    EXPECT_EQ(nullptr, raw);
    EXPECT_EQ(CL_ERR_IO, err.number());
}

TEST(SniiBlobDirectory, WriteOperationsThrowUnsupportedAndCloseNeverThrows) {
    Fixture fx;
    fx.Build();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    EXPECT_THROW({ fx.dir->createOutput("x"); }, CLuceneError);
    EXPECT_THROW({ fx.dir->renameFile("a", "b"); }, CLuceneError);
    EXPECT_THROW({ fx.dir->touchFile("a"); }, CLuceneError);
    EXPECT_THROW({ fx.dir->deleteFile("a"); }, CLuceneError);

    EXPECT_NO_THROW({
        fx.dir->close();
        fx.dir->close(); // idempotent
    });
}

TEST(SniiBlobDirectory, OpenValidatesEntryKindAndBounds) {
    Fixture fx;
    fx.Build();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());

    LogicalIndexMetadataRef text = fx.entry;
    text.kind = LogicalIndexKind::kInverted;
    text.files.clear();
    doris::segment_v2::snii_doris::SniiBlobDirectoryPtr rejected;
    EXPECT_FALSE(SniiBlobDirectory::open(fx.reader, text, fx.reader->size(), &rejected).ok());

    LogicalIndexMetadataRef oob = fx.entry;
    oob.files[0].length = fx.reader->size() + 1;
    const Status status = SniiBlobDirectory::open(fx.reader, oob, fx.reader->size(), &rejected);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;

    // Bounds are checked against the DATA AREA end, not the file size: a file
    // reaching into the metadata directory / tail region must be rejected even
    // though it is inside the container.
    const uint64_t data_area_end = fx.entry.files[0].length; // ends before file "b"
    const Status into_metadata =
            SniiBlobDirectory::open(fx.reader, fx.entry, data_area_end, &rejected);
    EXPECT_TRUE(into_metadata.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>())
            << into_metadata;
}

TEST(SniiBlobDirectory, CloseReleasesReaderAndRefusesLaterOpens) {
    Fixture fx;
    fx.Build();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());

    // An input opened before close() stays valid (it holds its own reader ref);
    // this is what CLucene consumers rely on.
    lucene::store::IndexInput* raw = nullptr;
    CLuceneError err;
    ASSERT_TRUE(fx.dir->openInput("a", raw, err));
    std::unique_ptr<lucene::store::IndexInput> input(raw);

    const long held_before = fx.reader.use_count();
    fx.dir->close();
    EXPECT_EQ(held_before - 1, fx.reader.use_count()); // directory released its hold

    EXPECT_EQ(fx.payload_a[0], input->readByte());
    input->close();

    // Using a closed directory is a caller bug, reported rather than served.
    lucene::store::IndexInput* after = nullptr;
    EXPECT_FALSE(fx.dir->openInput("a", after, err));
    EXPECT_EQ(nullptr, after);
}

} // namespace
