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

// The CLucene adapters of the ANN storage interface: every CLuceneError comes out as
// INVERTED_INDEX_CLUCENE_ERROR, an output closes once, and reads go through CLucene's buffered
// input, so small adjacent reads cost one read of the file and the caller's IO context is bound for
// exactly one call.

#include "storage/index/inverted/clucene/lucene_ann_store.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/io_common.h"
#include "storage/index/ann/ann_index_storage.h"
#include "storage/index/inverted/clucene/scripted_lucene_directory.h"

namespace doris::segment_v2 {
namespace {

std::vector<uint8_t> numbered_bytes(size_t size) {
    std::vector<uint8_t> bytes(size);
    for (size_t i = 0; i < size; ++i) {
        bytes[i] = static_cast<uint8_t>(i * 7 + (i >> 8));
    }
    return bytes;
}

std::vector<uint8_t> slice(const std::vector<uint8_t>& bytes, size_t offset, size_t size) {
    return {bytes.begin() + static_cast<int64_t>(offset),
            bytes.begin() + static_cast<int64_t>(offset + size)};
}

void expect_clucene_error(const Status& status, std::string_view file, std::string_view cause) {
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>()) << status.to_string();
    EXPECT_NE(status.to_string().find(file), std::string::npos) << status.to_string();
    EXPECT_NE(status.to_string().find(cause), std::string::npos) << status.to_string();
}

// The reads a logging input sent to its file, as (offset, length).
using FileReads = std::vector<std::pair<int64_t, int32_t>>;

FileReads file_reads(const ScriptedReadLog& log) {
    FileReads reads;
    for (const ScriptedReadLog::Read& read : log.reads) {
        reads.emplace_back(read.offset, read.length);
    }
    return reads;
}

} // namespace

class LuceneAnnStoreTest : public testing::Test {
protected:
    // Writes `bytes` into a RAM file through the sink.
    void write_file(const std::string& name, const std::vector<uint8_t>& bytes) {
        auto output = _sink.create_output(name);
        ASSERT_TRUE(output.has_value()) << output.error();
        ASSERT_TRUE((*output)->append(bytes.data(), bytes.size()).ok());
        ASSERT_TRUE((*output)->close().ok());
    }

    std::shared_ptr<ScriptedLuceneDirectory> _dir = std::make_shared<ScriptedLuceneDirectory>();
    LuceneAnnSink _sink {_dir};
    LuceneAnnSource _source {_dir};
};

// What an output wrote, appended in pieces of any size, reads back at any offset.
TEST_F(LuceneAnnStoreTest, WhatIsAppendedReadsBackAtAnyOffset) {
    const std::vector<uint8_t> bytes = numbered_bytes(200000);
    {
        auto output = _sink.create_output("ann.faiss");
        ASSERT_TRUE(output.has_value()) << output.error();
        ASSERT_TRUE((*output)->append(bytes.data(), 3).ok());
        ASSERT_TRUE((*output)->append(bytes.data() + 3, 0).ok());
        ASSERT_TRUE((*output)->append(bytes.data() + 3, bytes.size() - 3).ok());
        ASSERT_TRUE((*output)->close().ok());
    }
    auto input = _source.open_input("ann.faiss");
    ASSERT_TRUE(input.has_value()) << input.error();
    EXPECT_EQ((*input)->length(), bytes.size());
    for (const size_t offset : {size_t {0}, size_t {1}, size_t {70000}, size_t {199990}}) {
        std::vector<uint8_t> read(10);
        ASSERT_TRUE((*input)->read_at(offset, read.data(), read.size(), nullptr).ok());
        EXPECT_EQ(read, slice(bytes, offset, read.size())) << offset;
    }
}

// The adapters own the CLucene boundary: no CLuceneError gets past them, and each comes out with
// the code ANN reported for CLucene failures before it had this interface, naming the file and
// keeping CLucene's own message.
TEST_F(LuceneAnnStoreTest, ACluceneErrorComesOutAsInvertedIndexCluceneError) {
    // Opening a file the directory does not have.
    auto absent = _source.open_input("ann.ivfdata");
    ASSERT_FALSE(absent.has_value());
    expect_clucene_error(absent.error(), "ann.ivfdata", "does not exist");

    // Creating a file the directory refuses.
    _dir->fail_create = true;
    auto refused = _sink.create_output("ann.faiss");
    ASSERT_FALSE(refused.has_value());
    expect_clucene_error(refused.error(), "ann.faiss", "injected create failure");
    _dir->fail_create = false;

    // Appending more than the output buffers, so the flush fails inside append.
    _dir->broken_outputs.insert("broken");
    const std::vector<uint8_t> bytes =
            numbered_bytes(lucene::store::BufferedIndexOutput::BUFFER_SIZE + 1);
    {
        auto output = _sink.create_output("broken");
        ASSERT_TRUE(output.has_value()) << output.error();
        expect_clucene_error((*output)->append(bytes.data(), bytes.size()), "broken",
                             "injected flush failure");
    }
    // Closing with bytes still buffered, so the flush fails inside close.
    {
        auto output = _sink.create_output("broken");
        ASSERT_TRUE(output.has_value()) << output.error();
        ASSERT_TRUE((*output)->append(bytes.data(), 10).ok());
        expect_clucene_error((*output)->close(), "broken", "injected flush failure");
    }

    // Reading past the end of a file.
    write_file("ann.faiss", numbered_bytes(100));
    auto input = _source.open_input("ann.faiss");
    ASSERT_TRUE(input.has_value()) << input.error();
    std::vector<uint8_t> buffer(16);
    expect_clucene_error((*input)->read_at(90, buffer.data(), buffer.size(), nullptr), "ann.faiss",
                         "EOF");

    // Reading a file whose read fails.
    _dir->logged_files["logged"] = numbered_bytes(100);
    _dir->log.fail = true;
    auto failing = _source.open_input("logged");
    ASSERT_TRUE(failing.has_value()) << failing.error();
    expect_clucene_error((*failing)->read_at(0, buffer.data(), buffer.size(), nullptr), "logged",
                         "injected read failure");
}

// close() tries once: a second call does nothing, after a failure too. An output dropped without
// close() -- a serialization that failed half way -- closes itself, so even a RAM file gets its
// length.
TEST_F(LuceneAnnStoreTest, AnOutputClosesExactlyOnce) {
    const std::vector<uint8_t> bytes = numbered_bytes(1000);
    {
        auto output = _sink.create_output("dropped");
        ASSERT_TRUE(output.has_value()) << output.error();
        ASSERT_TRUE((*output)->append(bytes.data(), bytes.size()).ok());
    }
    EXPECT_EQ(_dir->fileLength("dropped"), 1000);
    {
        auto output = _sink.create_output("closed");
        ASSERT_TRUE(output.has_value()) << output.error();
        ASSERT_TRUE((*output)->append(bytes.data(), bytes.size()).ok());
        ASSERT_TRUE((*output)->close().ok());
        ASSERT_TRUE((*output)->close().ok());
    }
    EXPECT_EQ(_dir->fileLength("closed"), 1000);

    _dir->broken_outputs.insert("broken");
    auto output = _sink.create_output("broken");
    ASSERT_TRUE(output.has_value()) << output.error();
    ASSERT_TRUE((*output)->append(bytes.data(), bytes.size()).ok());
    EXPECT_FALSE((*output)->close().ok());
    EXPECT_TRUE((*output)->close().ok());
}

// read_at keeps CLucene's read buffer, which faiss's many small reads rely on: 100 header fields of
// 8 bytes cost one refill of 4096 bytes, and going back into the buffer costs nothing.
TEST_F(LuceneAnnStoreTest, SmallAdjacentReadsShareOneBufferedRead) {
    const std::vector<uint8_t> bytes = numbered_bytes(100000);
    _dir->logged_files["logged"] = bytes;
    auto input = _source.open_input("logged");
    ASSERT_TRUE(input.has_value()) << input.error();
    std::vector<uint8_t> field(8);
    for (size_t offset = 0; offset < 800; offset += field.size()) {
        ASSERT_TRUE((*input)->read_at(offset, field.data(), field.size(), nullptr).ok());
        EXPECT_EQ(field, slice(bytes, offset, field.size()));
    }
    ASSERT_TRUE((*input)->read_at(16, field.data(), field.size(), nullptr).ok());
    EXPECT_EQ(file_reads(_dir->log), FileReads({{0, kScriptedReadBufferSize}}));
}

// A read larger than the buffer goes to the file directly, in one read.
TEST_F(LuceneAnnStoreTest, AReadLargerThanTheBufferGoesStraightToTheFile) {
    const std::vector<uint8_t> bytes = numbered_bytes(100000);
    _dir->logged_files["logged"] = bytes;
    auto input = _source.open_input("logged");
    ASSERT_TRUE(input.has_value()) << input.error();
    std::vector<uint8_t> large(20000);
    ASSERT_TRUE((*input)->read_at(50000, large.data(), large.size(), nullptr).ok());
    EXPECT_EQ(large, slice(bytes, 50000, large.size()));
    EXPECT_EQ(file_reads(_dir->log), FileReads({{50000, 20000}}));
}

// IVF_ON_DISK borrows a list's ids right after its codes, and they come out of the refill the codes
// caused: a list of 30 vectors of 32 floats is 3840 bytes of codes, then 240 bytes of ids.
TEST_F(LuceneAnnStoreTest, AListsIdsComeOutOfTheRefillOfItsCodes) {
    const std::vector<uint8_t> bytes = numbered_bytes(100000);
    _dir->logged_files["logged"] = bytes;
    auto input = _source.open_input("logged");
    ASSERT_TRUE(input.has_value()) << input.error();
    std::vector<uint8_t> codes(3840);
    std::vector<uint8_t> ids(240);
    ASSERT_TRUE((*input)->read_at(80000, codes.data(), codes.size(), nullptr).ok());
    ASSERT_TRUE((*input)->read_at(80000 + codes.size(), ids.data(), ids.size(), nullptr).ok());
    EXPECT_EQ(ids, slice(bytes, 80000 + codes.size(), ids.size()));
    EXPECT_EQ(file_reads(_dir->log), FileReads({{80000, kScriptedReadBufferSize}}));
}

// The caller's IO context reaches the file for the reads of its own call, and the input keeps no
// pointer to it afterwards: a search's context ends with its query, the input lives with the index.
TEST_F(LuceneAnnStoreTest, TheIoContextIsBoundForOneCall) {
    _dir->logged_files["logged"] = numbered_bytes(100000);
    auto input = _source.open_input("logged");
    ASSERT_TRUE(input.has_value()) << input.error();
    io::IOContext first;
    io::IOContext second;
    std::vector<uint8_t> buffer(16);
    ASSERT_TRUE((*input)->read_at(0, buffer.data(), buffer.size(), &first).ok());
    ASSERT_TRUE((*input)->read_at(10000, buffer.data(), buffer.size(), &second).ok());
    ASSERT_TRUE((*input)->read_at(20000, buffer.data(), buffer.size(), nullptr).ok());
    ASSERT_EQ(_dir->log.reads.size(), 3);
    EXPECT_EQ(_dir->log.reads[0].io_ctx, &first);
    EXPECT_EQ(_dir->log.reads[1].io_ctx, &second);
    EXPECT_EQ(_dir->log.reads[2].io_ctx, nullptr);
}

} // namespace doris::segment_v2
