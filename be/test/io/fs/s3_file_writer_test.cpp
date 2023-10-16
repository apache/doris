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

#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <thread>

#include "common/config.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_bufferpool.h"
#include "io/fs/s3_file_system.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "testutil/http_utils.h"
#include "util/debug_points.h"
#include "util/slice.h"
#include "util/threadpool.h"
namespace doris {

static std::shared_ptr<io::S3FileSystem> s3_fs {nullptr};

class S3FileWriterTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        S3Conf s3_conf;
        config::enable_debug_points = true;
        DebugPoints::instance()->clear();
        s3_conf.ak = config::test_s3_ak;
        s3_conf.sk = config::test_s3_sk;
        s3_conf.endpoint = config::test_s3_endpoint;
        s3_conf.region = config::test_s3_region;
        s3_conf.bucket = config::test_s3_bucket;
        s3_conf.prefix = "s3_file_writer_test";
        static_cast<void>(
                io::S3FileSystem::create(std::move(s3_conf), "s3_file_writer_test", &s3_fs));
        std::cout << "s3 conf: " << s3_conf.to_string() << std::endl;
        ASSERT_EQ(Status::OK(), s3_fs->connect());

        std::unique_ptr<doris::ThreadPool> _s3_file_upload_thread_pool;
        static_cast<void>(ThreadPoolBuilder("S3FileUploadThreadPool")
                                  .set_min_threads(16)
                                  .set_max_threads(64)
                                  .build(&_s3_file_upload_thread_pool));
        ExecEnv::GetInstance()->_s3_file_upload_thread_pool =
                std::move(_s3_file_upload_thread_pool);
        ExecEnv::GetInstance()->_s3_buffer_pool = new io::S3FileBufferPool();
        io::S3FileBufferPool::GetInstance()->init(
                config::s3_write_buffer_whole_size, config::s3_write_buffer_size,
                ExecEnv::GetInstance()->_s3_file_upload_thread_pool.get());
    }

    static void TearDownTestSuite() {
        ExecEnv::GetInstance()->_s3_file_upload_thread_pool->shutdown();
        ExecEnv::GetInstance()->_s3_file_upload_thread_pool = nullptr;
        delete ExecEnv::GetInstance()->_s3_buffer_pool;
        ExecEnv::GetInstance()->_s3_buffer_pool = nullptr;
    }

private:
};

TEST_F(S3FileWriterTest, multi_part_io_error) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        POST_HTTP_TO_TEST_SERVER("/api/debug_point/add/s3_file_writer::_upload_one_part");
        Defer defer {[&]() {
            POST_HTTP_TO_TEST_SERVER("/api/debug_point/remove/s3_file_writer::_upload_one_part");
        }};
        auto client = s3_fs->get_client();
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state));

        char buf[buf_size];
        doris::Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(!s3_file_writer->finalize().ok());
        // The second part would fail uploading itself to s3
        // so the result of close should be not ok
        ASSERT_TRUE(!s3_file_writer->close().ok());
        bool exits = false;
        auto s = s3_fs->exists("multi_part_io_error", &exits);
        LOG(INFO) << "status is " << s;
        ASSERT_TRUE(!exits);
    }
}

TEST_F(S3FileWriterTest, put_object_io_error) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        POST_HTTP_TO_TEST_SERVER("/api/debug_point/add/s3_file_writer::_put_object");
        Defer defer {[&]() {
            POST_HTTP_TO_TEST_SERVER("/api/debug_point/remove/s3_file_writer::_put_object");
        }};
        auto client = s3_fs->get_client();
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("put_object_io_error", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        // Only upload 4MB to trigger put object operation
        auto file_size = 4 * 1024 * 1024;
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(!s3_file_writer->finalize().ok());
        // The object might be timeout but still succeed in loading
        ASSERT_TRUE(!s3_file_writer->close().ok());
    }
}

TEST_F(S3FileWriterTest, appendv_random_quit) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;
        POST_HTTP_TO_TEST_SERVER("/api/debug_point/add/s3_file_writer::appendv");
        Defer defer {[&]() {
            POST_HTTP_TO_TEST_SERVER("/api/debug_point/remove/s3_file_writer::appendv");
        }};

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("appendv_random_quit", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
        ASSERT_TRUE(!s3_file_writer->append(Slice(buf, bytes_read)).ok());
        bool exits = false;
        static_cast<void>(s3_fs->exists("appendv_random_quit", &exits));
        ASSERT_TRUE(!exits);
    }
}

TEST_F(S3FileWriterTest, multi_part_open_error) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 5 * 1024 * 1024;
        POST_HTTP_TO_TEST_SERVER(
                "/api/debug_point/add/s3_file_writer::_create_multi_upload_request");
        Defer defer {[&]() {
            POST_HTTP_TO_TEST_SERVER(
                    "/api/debug_point/remove/s3_file_writer::_create_multi_upload_request");
        }};

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(),
                  s3_fs->create_file("multi_part_open_error", &s3_file_writer, &state));

        auto buf = std::make_unique<char[]>(buf_size);
        Slice slice(buf.get(), buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
        // Directly write 5MB would cause one create multi part upload request
        // and it would be rejectd one error
        auto st = s3_file_writer->append(Slice(buf.get(), bytes_read));
        ASSERT_TRUE(!st.ok());
        bool exits = false;
        static_cast<void>(s3_fs->exists("multi_part_open_error", &exits));
        ASSERT_TRUE(!exits);
    }
}

TEST_F(S3FileWriterTest, normal) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("normal", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(s3_file_writer->finalize().ok());
        ASSERT_EQ(Status::OK(), s3_file_writer->close());
        int64_t s3_file_size = 0;
        ASSERT_EQ(Status::OK(), s3_fs->file_size("normal", &s3_file_size));
        ASSERT_EQ(s3_file_size, file_size);
    }
}

TEST_F(S3FileWriterTest, smallFile) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(fs->open_file("./be/test/olap/test_data/all_types_1000.txt", &local_file_reader)
                            .ok());

        constexpr int buf_size = 8192;

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("small", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(s3_file_writer->finalize().ok());
        ASSERT_EQ(Status::OK(), s3_file_writer->close());
        int64_t s3_file_size = 0;
        ASSERT_EQ(Status::OK(), s3_fs->file_size("small", &s3_file_size));
        ASSERT_EQ(s3_file_size, file_size);
    }
}

TEST_F(S3FileWriterTest, close_error) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(fs->open_file("./be/test/olap/test_data/all_types_1000.txt", &local_file_reader)
                            .ok());

        POST_HTTP_TO_TEST_SERVER("/api/debug_point/add/s3_file_writer::close");
        Defer defer {[&]() {
            POST_HTTP_TO_TEST_SERVER("/api/debug_point/remove/s3_file_writer::close");
        }};
        io::FileWriterPtr s3_file_writer;
        ASSERT_TRUE(s3_fs->create_file("close_error", &s3_file_writer, &state).ok());
        ASSERT_TRUE(!s3_file_writer->close().ok());
        bool exits = false;
        static_cast<void>(s3_fs->exists("close_error", &exits));
        ASSERT_TRUE(!exits);
    }
}

TEST_F(S3FileWriterTest, finalize_error) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(fs->open_file("./be/test/olap/test_data/all_types_1000.txt", &local_file_reader)
                            .ok());

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("finalize_error", &s3_file_writer, &state));

        POST_HTTP_TO_TEST_SERVER("/api/debug_point/add/s3_file_writer::finalize");
        Defer defer {[&]() {
            POST_HTTP_TO_TEST_SERVER("/api/debug_point/remove/s3_file_writer::finalize");
        }};

        constexpr int buf_size = 8192;

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(!s3_file_writer->finalize().ok());
        bool exits = false;
        static_cast<void>(s3_fs->exists("finalize_error", &exits));
        ASSERT_TRUE(!exits);
    }
}

TEST_F(S3FileWriterTest, multi_part_complete_error_2) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        POST_HTTP_TO_TEST_SERVER("/api/debug_point/add/s3_file_writer::_complete:2");
        Defer defer {[&]() {
            POST_HTTP_TO_TEST_SERVER("/api/debug_point/remove/s3_file_writer::_complete:2");
        }};
        auto client = s3_fs->get_client();
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(s3_file_writer->finalize().ok());
        // The second part would fail uploading itself to s3
        // so the result of close should be not ok
        auto st = s3_file_writer->close();
        ASSERT_TRUE(!st.ok());
        std::cout << st << std::endl;
    }
}

TEST_F(S3FileWriterTest, multi_part_complete_error_1) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        POST_HTTP_TO_TEST_SERVER("/api/debug_point/add/s3_file_writer::_complete:1");
        Defer defer {[&]() {
            POST_HTTP_TO_TEST_SERVER("/api/debug_point/remove/s3_file_writer::_complete:1");
        }};
        auto client = s3_fs->get_client();
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(s3_file_writer->finalize().ok());
        // The second part would fail uploading itself to s3
        // so the result of close should be not ok
        auto st = s3_file_writer->close();
        ASSERT_TRUE(!st.ok());
        std::cout << st << std::endl;
    }
}

TEST_F(S3FileWriterTest, multi_part_complete_error_3) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        POST_HTTP_TO_TEST_SERVER("/api/debug_point/add/s3_file_writer::_complete:3");
        Defer defer {[&]() {
            POST_HTTP_TO_TEST_SERVER("/api/debug_point/remove/s3_file_writer::_complete:3");
        }};
        auto client = s3_fs->get_client();
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(s3_file_writer->finalize().ok());
        // The second part would fail uploading itself to s3
        // so the result of close should be not ok
        auto st = s3_file_writer->close();
        ASSERT_TRUE(!st.ok());
        std::cout << st << std::endl;
    }
}

} // namespace doris