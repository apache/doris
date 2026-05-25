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

#include "storage/index/inverted/spimi/file_lucene_output.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <string>

#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/index/inverted/spimi/lucene_output.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/segment_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

std::string MakeTempPath(const std::string& suffix) {
    char tmpl[] = "/tmp/spimi_file_output_test_XXXXXX";
    const int fd = mkstemp(tmpl);
    EXPECT_GE(fd, 0);
    if (fd >= 0) {
        ::close(fd);
        ::unlink(tmpl);
    }
    return std::string(tmpl) + suffix;
}

std::vector<uint8_t> ReadFileBytes(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    EXPECT_TRUE(in.good()) << "Failed to open " << path;
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void RemoveIfExists(const std::string& path) {
    (void)io::global_local_filesystem()->delete_file(path);
}

} // namespace

TEST(FileLuceneOutputTest, EncodingMatchesMemoryOutput) {
    const std::string path = MakeTempPath(".bin");
    RemoveIfExists(path);

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(path, &file_writer).ok());

    FileLuceneOutput file_out(file_writer.get(), /*buffer_size=*/8);
    MemoryLuceneOutput mem;

    // Exercise every primitive crossing the small (8-byte) flush boundary.
    auto write_both = [&](auto&& fn) {
        fn(static_cast<LuceneOutput*>(&file_out));
        fn(static_cast<LuceneOutput*>(&mem));
    };

    write_both([](LuceneOutput* o) { o->WriteInt(0x01020304); });
    write_both([](LuceneOutput* o) { o->WriteLong(-1); });
    write_both([](LuceneOutput* o) { o->WriteVInt(0x7FFFFFFF); });
    write_both([](LuceneOutput* o) { o->WriteVLong(0x0102030405060708LL); });
    const std::wstring wide = L"hello, 中文 🌐";
    write_both([&](LuceneOutput* o) {
        o->WriteSCharsFromWide(wide.data(), static_cast<int32_t>(wide.size()));
    });

    EXPECT_EQ(file_out.FilePointer(), mem.FilePointer());
    ASSERT_TRUE(file_out.Finish().ok());
    ASSERT_TRUE(file_writer->close().ok());

    const auto disk_bytes = ReadFileBytes(path);
    EXPECT_EQ(disk_bytes, mem.bytes());

    RemoveIfExists(path);
}

TEST(FileLuceneOutputTest, BufferBoundariesPreserveByteOrder) {
    const std::string path = MakeTempPath(".bin");
    RemoveIfExists(path);

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(path, &file_writer).ok());

    // Buffer is 4 bytes; write 10 bytes mixed via WriteByte and WriteBytes
    // so the flush triggers multiple times mid-stream.
    FileLuceneOutput out(file_writer.get(), /*buffer_size=*/4);
    out.WriteByte(0x11);
    out.WriteByte(0x22);
    const uint8_t chunk[] = {0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA};
    out.WriteBytes(chunk, sizeof(chunk));

    ASSERT_TRUE(out.Finish().ok());
    ASSERT_TRUE(file_writer->close().ok());

    const auto disk_bytes = ReadFileBytes(path);
    ASSERT_EQ(disk_bytes.size(), 10U);
    const uint8_t expected[] = {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA};
    for (size_t i = 0; i < 10; ++i) {
        EXPECT_EQ(disk_bytes[i], expected[i]) << "byte " << i;
    }
    RemoveIfExists(path);
}

TEST(FileLuceneOutputTest, RoundTripsSegmentWriterOutputThroughDisk) {
    // End-to-end: drive a SegmentWriter against FileLuceneOutputs backed by
    // four temp files, then re-read those files and compare against an
    // equivalent in-memory run.
    const std::string tis_path = MakeTempPath(".tis");
    const std::string tii_path = MakeTempPath(".tii");
    const std::string frq_path = MakeTempPath(".frq");
    const std::string prx_path = MakeTempPath(".prx");
    for (const auto& p : {tis_path, tii_path, frq_path, prx_path}) {
        RemoveIfExists(p);
    }

    // In-memory reference.
    MemoryLuceneOutput mem_tis, mem_tii, mem_frq, mem_prx;
    SegmentWriter mem_w(&mem_tis, &mem_tii, &mem_frq, &mem_prx);

    // Disk-backed writer.
    io::FileWriterPtr tis_fw, tii_fw, frq_fw, prx_fw;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(tis_path, &tis_fw).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_file(tii_path, &tii_fw).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_file(frq_path, &frq_fw).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_file(prx_path, &prx_fw).ok());
    FileLuceneOutput disk_tis(tis_fw.get());
    FileLuceneOutput disk_tii(tii_fw.get());
    FileLuceneOutput disk_frq(frq_fw.get());
    FileLuceneOutput disk_prx(prx_fw.get());
    SegmentWriter disk_w(&disk_tis, &disk_tii, &disk_frq, &disk_prx);

    SpimiPostingBuffer buffer;
    buffer.Append("apple", 0, 0);
    buffer.Append("apple", 2, 1);
    buffer.Append("apply", 1, 0);
    buffer.Append("banana", 3, 5);
    buffer.Append("banana", 3, 9);
    buffer.Sort();

    mem_w.Emit(buffer, 0);
    disk_w.Emit(buffer, 0);
    mem_w.Close();
    disk_w.Close();
    ASSERT_TRUE(disk_tis.Finish().ok());
    ASSERT_TRUE(disk_tii.Finish().ok());
    ASSERT_TRUE(disk_frq.Finish().ok());
    ASSERT_TRUE(disk_prx.Finish().ok());
    ASSERT_TRUE(tis_fw->close().ok());
    ASSERT_TRUE(tii_fw->close().ok());
    ASSERT_TRUE(frq_fw->close().ok());
    ASSERT_TRUE(prx_fw->close().ok());

    EXPECT_EQ(ReadFileBytes(tis_path), mem_tis.bytes());
    EXPECT_EQ(ReadFileBytes(tii_path), mem_tii.bytes());
    EXPECT_EQ(ReadFileBytes(frq_path), mem_frq.bytes());
    EXPECT_EQ(ReadFileBytes(prx_path), mem_prx.bytes());

    for (const auto& p : {tis_path, tii_path, frq_path, prx_path}) {
        RemoveIfExists(p);
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
