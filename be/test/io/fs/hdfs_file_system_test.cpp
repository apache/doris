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

#include <gtest/gtest.h>

#include "common/config.h"
#include "cpp/sync_point.h"
#include "io/fs/file_writer.h"
#include "io/fs/hdfs_file_writer.h"
#include "io/fs/local_file_system.h"

namespace doris {

static constexpr std::string_view test_dir = "ut_dir/hdfs_test";

TEST(HdfsFileSystemTest, Write) {
    Status st;
    const auto& local_fs = io::global_local_filesystem();
    st = local_fs->delete_directory(test_dir);
    ASSERT_TRUE(st.ok()) << st;
    st = local_fs->create_directory(test_dir);
    ASSERT_TRUE(st.ok()) << st;

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();

    io::FileWriterPtr local_file_writer;
    st = local_fs->create_file(fmt::format("{}/mock_hdfs_file", test_dir), &local_file_writer);
    ASSERT_TRUE(st.ok()) << st;

    SyncPoint::CallbackGuard guard1;
    sp->set_call_back(
            "HdfsFileWriter::close::hdfsHSync",
            [](auto&& args) {
                auto* ret = try_any_cast_ret<int>(args);
                ret->first = 0; // noop, return success
                ret->second = true;
            },
            &guard1);

    SyncPoint::CallbackGuard guard2;
    sp->set_call_back(
            "HdfsFileWriter::close::hdfsCloseFile",
            [&](auto&& args) {
                auto st = local_file_writer->close();
                ASSERT_TRUE(st.ok()) << st;
                auto* ret = try_any_cast_ret<int>(args);
                ret->first = 0; // return success
                ret->second = true;
            },
            &guard2);

    SyncPoint::CallbackGuard guard3;
    sp->set_call_back(
            "HdfsFileWriter::append_hdfs_file::hdfsWrite",
            [&](auto&& args) {
                auto content = try_any_cast<std::string_view>(args[0]);
                auto st = local_file_writer->append({content.data(), content.size()});
                ASSERT_TRUE(st.ok()) << st;
                auto* ret = try_any_cast_ret<int>(args);
                ret->first = content.size(); // return bytes written
                ret->second = true;
            },
            &guard3);

    SyncPoint::CallbackGuard guard4;
    sp->set_call_back(
            "HdfsFileWriter::finalize::hdfsFlush",
            [](auto&& args) {
                auto* ret = try_any_cast_ret<int>(args);
                ret->first = 0; // noop, return success
                ret->second = true;
            },
            &guard4);

    config::hdfs_write_batch_buffer_size_mb = 1;
    // TODO(plat1ko): Test write file cache

    io::FileWriterOptions opts {.write_file_cache = true};
    auto hdfs_file_writer = std::make_unique<io::HdfsFileWriter>("mock_hdfs_file", nullptr, nullptr,
                                                                 "fs_name", &opts);

    std::string content_1000(1000, 'a');
    constexpr size_t MB = 1024 * 1024;
    std::string content_1M(MB, 'b');
    std::string content_2M(2 * MB, 'c');

    for (int i = 0; i < 1200; ++i) {
        st = hdfs_file_writer->append(content_1000);
        ASSERT_TRUE(st.ok()) << st;
    }

    st = hdfs_file_writer->append(content_1M);
    ASSERT_TRUE(st.ok()) << st;

    st = hdfs_file_writer->append(content_2M);
    ASSERT_TRUE(st.ok()) << st;

    st = hdfs_file_writer->close();
    ASSERT_TRUE(st.ok()) << st;

    // Check mock file content
    io::FileReaderSPtr local_file_reader;
    st = local_fs->open_file(fmt::format("{}/mock_hdfs_file", test_dir), &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(hdfs_file_writer->bytes_appended(), local_file_reader->size());

    auto buf = std::make_unique<char[]>(1024 * 1024 * 2);
    size_t bytes_read;
    size_t offset = 0;
    for (int i = 0; i < 1200; ++i) {
        st = local_file_reader->read_at(offset, {buf.get(), content_1000.size()}, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(bytes_read, content_1000.size());
        ASSERT_EQ(std::string_view(buf.get(), bytes_read), content_1000);
        offset += bytes_read;
    }
    st = local_file_reader->read_at(offset, {buf.get(), content_1M.size()}, &bytes_read);
    ASSERT_EQ(bytes_read, content_1M.size());
    offset += bytes_read;
    ASSERT_EQ(std::string_view(buf.get(), bytes_read), content_1M);
    st = local_file_reader->read_at(offset, {buf.get(), content_2M.size()}, &bytes_read);
    ASSERT_EQ(bytes_read, content_2M.size());
    ASSERT_EQ(std::string_view(buf.get(), bytes_read), content_2M);
    offset += bytes_read;

    // TODO(plat1ko): Check cached content

    st = local_fs->delete_directory(test_dir);
}

} // namespace doris
