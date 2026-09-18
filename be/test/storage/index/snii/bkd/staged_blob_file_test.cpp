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

#include "storage/index/snii/bkd/staged_blob_file.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "storage/index/snii/writer/temp_dir.h"

namespace doris::snii::bkd {
namespace {

std::vector<uint8_t> pattern(size_t bytes, uint8_t seed) {
    std::vector<uint8_t> out(bytes);
    for (size_t i = 0; i < bytes; ++i) {
        out[i] = static_cast<uint8_t>((i * 37 + seed * 11 + 3) & 0xFF);
    }
    return out;
}

bool exists(const std::string& path) {
    return ::access(path.c_str(), F_OK) == 0;
}

} // namespace

// The whole contract: what was appended comes back at any offset, in any order,
// in any slice size. read_fn is positional and the container calls it in chunks,
// so sequential-only staging would silently corrupt the sealed blob.
TEST(BkdStagedBlobFileTest, ReadsBackEveryByteAtAnyOffset) {
    std::unique_ptr<StagedBlobFile> file;
    ASSERT_TRUE(StagedBlobFile::create("roundtrip", &file).ok());

    // Uneven appends, so no read window lines up with a write boundary.
    std::vector<uint8_t> expected;
    for (const size_t chunk : {1U, 1000U, 7U, 64U * 1024U, 3U}) {
        const std::vector<uint8_t> part = pattern(chunk, static_cast<uint8_t>(chunk));
        ASSERT_TRUE(file->append(Slice(part)).ok());
        expected.insert(expected.end(), part.begin(), part.end());
        EXPECT_EQ(file->bytes_written(), expected.size());
    }
    ASSERT_TRUE(file->finalize().ok());

    // Whole file in one read.
    std::vector<uint8_t> whole(expected.size());
    ASSERT_TRUE(file->read_at(0, whole.size(), whole.data()).ok());
    EXPECT_EQ(whole, expected);

    // Backwards, and in windows that straddle the append boundaries.
    for (const size_t window : {1U, 3U, 4096U, 65536U}) {
        SCOPED_TRACE("window " + std::to_string(window));
        std::vector<uint8_t> actual(expected.size());
        size_t offset = expected.size();
        while (offset > 0) {
            const size_t len = std::min(window, offset);
            offset -= len;
            ASSERT_TRUE(file->read_at(offset, len, actual.data() + offset).ok());
        }
        EXPECT_EQ(actual, expected);
    }
}

// A read past the end must FAIL, not return short. The container checksums the
// buffer read_fn filled, so a silent short read would be sealed as if it were
// the real payload.
TEST(BkdStagedBlobFileTest, ReadingPastTheEndFails) {
    std::unique_ptr<StagedBlobFile> file;
    ASSERT_TRUE(StagedBlobFile::create("eof", &file).ok());
    const std::vector<uint8_t> body = pattern(100, 1);
    ASSERT_TRUE(file->append(Slice(body)).ok());
    ASSERT_TRUE(file->finalize().ok());

    std::vector<uint8_t> out(200);
    EXPECT_FALSE(file->read_at(0, 101, out.data()).ok());
    EXPECT_FALSE(file->read_at(100, 1, out.data()).ok());
    EXPECT_FALSE(file->read_at(99, 2, out.data()).ok());
    // The exact end is legal and reads nothing.
    EXPECT_TRUE(file->read_at(100, 0, out.data()).ok());
}

// An empty blob sub-file is legal (design 5.3: an empty index writes a
// zero-length bkd_data) and must stage and read as such rather than error.
TEST(BkdStagedBlobFileTest, EmptyFileIsLegal) {
    std::unique_ptr<StagedBlobFile> file;
    ASSERT_TRUE(StagedBlobFile::create("empty", &file).ok());
    ASSERT_TRUE(file->finalize().ok());
    EXPECT_EQ(file->bytes_written(), 0U);

    uint8_t unused = 0;
    EXPECT_TRUE(file->read_at(0, 0, &unused).ok());
    EXPECT_FALSE(file->read_at(0, 1, &unused).ok());
}

// The staging file is build-time scratch; nothing may survive the object.
//
// Asserted on THIS file's own path rather than by scanning the temp dir:
// resolve_temp_dir() is not stable across a whole test binary (other suites
// reconfigure it), so a directory scan would compare two different directories
// and report a phantom leak -- which is exactly what an earlier version of this
// test did, passing alone and failing in the full run.
TEST(BkdStagedBlobFileTest, DestructionRemovesTheTempFile) {
    std::string path;
    {
        std::unique_ptr<StagedBlobFile> file;
        ASSERT_TRUE(StagedBlobFile::create("cleanup", &file).ok());
        path = file->path();
        ASSERT_FALSE(path.empty());
        const std::vector<uint8_t> body = pattern(4096, 2);
        ASSERT_TRUE(file->append(Slice(body)).ok());
        ASSERT_TRUE(file->finalize().ok());
        EXPECT_TRUE(exists(path)) << "the staging file was never created: " << path;
    }
    EXPECT_FALSE(exists(path)) << "the staging file outlived its owner: " << path;
}

// The same for an ABANDONED build: an error between create() and finalize()
// still has to clean up, because nothing else knows the path.
TEST(BkdStagedBlobFileTest, AnUnfinalizedFileIsStillRemoved) {
    std::string path;
    {
        std::unique_ptr<StagedBlobFile> file;
        ASSERT_TRUE(StagedBlobFile::create("abandoned", &file).ok());
        path = file->path();
        const std::vector<uint8_t> body = pattern(64, 3);
        ASSERT_TRUE(file->append(Slice(body)).ok());
        // no finalize(): the build failed here
        EXPECT_TRUE(exists(path));
    }
    EXPECT_FALSE(exists(path));
}

} // namespace doris::snii::bkd
