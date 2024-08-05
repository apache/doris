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

#include "io/fs/s3_file_writer.h"

#include <aws/core/utils/HashingUtils.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <gtest/gtest.h>

#include <any>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unordered_map>

#include "common/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_bufferpool.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_obj_storage_client.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "util/slice.h"
#include "util/threadpool.h"
#include "util/uuid_generator.h"
namespace doris {

static std::shared_ptr<io::S3FileSystem> s3_fs {nullptr};

// This MockS3Client is only responsible for handling normal situations,
// while error injection is left to other macros to resolve
class MockS3Client {
public:
    MockS3Client() = default;
    ~MockS3Client() = default;

    Aws::S3::Model::CreateMultipartUploadOutcome create_multi_part_upload(
            const Aws::S3::Model::CreateMultipartUploadRequest request) {
        auto uuid = UUIDGenerator::instance()->next_uuid();
        std::stringstream ss;
        ss << uuid;
        upload_id = ss.str();
        bucket = request.GetBucket();
        key = request.GetKey();
        auto result = Aws::S3::Model::CreateMultipartUploadResult();
        result.SetUploadId(upload_id);
        auto outcome = Aws::S3::Model::CreateMultipartUploadOutcome(std::move(result));
        return outcome;
    }

    Aws::S3::Model::AbortMultipartUploadOutcome abort_multi_part_upload(
            const Aws::S3::Model::AbortMultipartUploadRequest& request) {
        if (request.GetKey() != key || request.GetBucket() != bucket ||
            upload_id != request.GetUploadId()) {
            return Aws::S3::Model::AbortMultipartUploadOutcome(
                    Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::NO_SUCH_UPLOAD,
                                                             false));
        }
        uploaded_parts.clear();
        return Aws::S3::Model::AbortMultipartUploadOutcome(
                Aws::S3::Model::AbortMultipartUploadResult());
    }

    Aws::S3::Model::UploadPartOutcome upload_part(const Aws::S3::Model::UploadPartRequest& request,
                                                  std::string_view buf) {
        if (request.GetKey() != key || request.GetBucket() != bucket ||
            upload_id != request.GetUploadId()) {
            return Aws::S3::Model::UploadPartOutcome(Aws::Client::AWSError<Aws::S3::S3Errors>(
                    Aws::S3::S3Errors::NO_SUCH_UPLOAD, false));
        }
        if (request.ContentMD5HasBeenSet()) {
            const auto& origin_md5 = request.GetContentMD5();
            auto content = request.GetBody();
            Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*content));
            const auto& md5 = Aws::Utils::HashingUtils::Base64Encode(part_md5);
            if (origin_md5 != md5) {
                return Aws::S3::Model::UploadPartOutcome(Aws::Client::AWSError<Aws::S3::S3Errors>(
                        Aws::S3::S3Errors::INVALID_OBJECT_STATE, "wrong md5", "md5 not match",
                        false));
            }
        }
        {
            Slice slice {buf.data(), buf.size()};
            std::string str;
            str.resize(slice.get_size());
            std::memcpy(str.data(), slice.get_data(), slice.get_size());
            std::unique_lock lck {latch};
            uploaded_parts.insert({request.GetPartNumber(), std::move(str)});
            file_size += request.GetContentLength();
        }
        LOG_INFO("upload part size is {}", request.GetContentLength());
        return Aws::S3::Model::UploadPartOutcome(Aws::S3::Model::UploadPartResult());
    }

    Aws::S3::Model::CompleteMultipartUploadOutcome complete_multi_part_upload(
            const Aws::S3::Model::CompleteMultipartUploadRequest& request) {
        if (request.GetKey() != key || request.GetBucket() != bucket ||
            upload_id != request.GetUploadId()) {
            return Aws::S3::Model::CompleteMultipartUploadOutcome(
                    Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::NO_SUCH_UPLOAD,
                                                             false));
        }
        const auto& multi_part_upload = request.GetMultipartUpload();
        if (multi_part_upload.GetParts().size() != uploaded_parts.size()) {
            return Aws::S3::Model::CompleteMultipartUploadOutcome(
                    Aws::Client::AWSError<Aws::S3::S3Errors>(
                            Aws::S3::S3Errors::INVALID_OBJECT_STATE, "part num not match",
                            "part num not match", false));
        }
        for (size_t i = 0; i < multi_part_upload.GetParts().size(); i++) {
            if (i + 1 != multi_part_upload.GetParts().at(i).GetPartNumber()) {
                return Aws::S3::Model::CompleteMultipartUploadOutcome(
                        Aws::Client::AWSError<Aws::S3::S3Errors>(
                                Aws::S3::S3Errors::INVALID_OBJECT_STATE, "part num not coutinous",
                                "part num not coutinous", false));
            }
        }
        exists = true;
        return Aws::S3::Model::CompleteMultipartUploadOutcome(
                Aws::S3::Model::CompleteMultipartUploadResult());
    }

    Aws::S3::Model::PutObjectOutcome put_object(const Aws::S3::Model::PutObjectRequest& request,
                                                std::string_view& buf) {
        exists = true;
        file_size = request.GetContentLength();
        key = request.GetKey();
        bucket = request.GetBucket();
        Slice s {buf.data(), buf.size()};
        std::string str;
        str.resize(s.get_size());
        std::memcpy(str.data(), s.get_data(), s.get_size());
        uploaded_parts.insert({1, std::move(str)});
        return Aws::S3::Model::PutObjectOutcome(Aws::S3::Model::PutObjectResult());
    }

    Aws::S3::Model::HeadObjectOutcome head_object(
            const Aws::S3::Model::HeadObjectRequest& request) {
        if (request.GetKey() != key || request.GetBucket() != bucket || !exists) {
            auto error = Aws::Client::AWSError<Aws::S3::S3Errors>(
                    Aws::S3::S3Errors::RESOURCE_NOT_FOUND, false);
            error.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
            return Aws::S3::Model::HeadObjectOutcome(error);
        }
        auto result = Aws::S3::Model::HeadObjectResult();
        result.SetContentLength(file_size);
        return Aws::S3::Model::HeadObjectOutcome(result);
    }

    [[nodiscard]] const std::map<int64_t, std::string>& contents() const { return uploaded_parts; }

private:
    std::mutex latch;
    std::string upload_id;
    size_t file_size {0};
    std::map<int64_t, std::string> uploaded_parts;
    std::string key;
    std::string bucket;
    bool exists {false};
};

static std::shared_ptr<MockS3Client> mock_client = nullptr;

struct MockCallback {
    std::string point_name;
    std::function<void(std::vector<std::any>&&)> callback;
};

static auto test_mock_callbacks = std::array {
        MockCallback {"s3_file_writer::create_multi_part_upload",
                      [](auto&& outcome) {
                          const auto& req =
                                  try_any_cast<const Aws::S3::Model::CreateMultipartUploadRequest&>(
                                          outcome.at(0));
                          auto pair =
                                  try_any_cast_ret<Aws::S3::Model::CreateMultipartUploadOutcome>(
                                          outcome);
                          pair->second = true;
                          pair->first = mock_client->create_multi_part_upload(req);
                      }},
        MockCallback {"s3_file_writer::abort_multi_part",
                      [](auto&& outcome) {
                          const auto& req =
                                  try_any_cast<const Aws::S3::Model::AbortMultipartUploadRequest&>(
                                          outcome.at(0));
                          auto pair = try_any_cast_ret<Aws::S3::Model::AbortMultipartUploadOutcome>(
                                  outcome);
                          pair->second = true;
                          pair->first = mock_client->abort_multi_part_upload(req);
                      }},
        MockCallback {"s3_file_writer::upload_part",
                      [](auto&& outcome) {
                          const auto& req = try_any_cast<const Aws::S3::Model::UploadPartRequest&>(
                                  outcome.at(0));
                          const auto& buf = try_any_cast<std::string_view*>(outcome.at(1));
                          auto pair = try_any_cast_ret<Aws::S3::Model::UploadPartOutcome>(outcome);
                          pair->second = true;
                          pair->first = mock_client->upload_part(req, *buf);
                      }},
        MockCallback {
                "s3_file_writer::complete_multi_part",
                [](auto&& outcome) {
                    const auto& req =
                            try_any_cast<const Aws::S3::Model::CompleteMultipartUploadRequest&>(
                                    outcome.at(0));
                    auto pair = try_any_cast_ret<Aws::S3::Model::CompleteMultipartUploadOutcome>(
                            outcome);
                    pair->second = true;
                    pair->first = mock_client->complete_multi_part_upload(req);
                }},
        MockCallback {"s3_file_writer::put_object",
                      [](auto&& outcome) {
                          const auto& req = try_any_cast<const Aws::S3::Model::PutObjectRequest&>(
                                  outcome.at(0));
                          const auto& buf = try_any_cast<std::string_view*>(outcome.at(1));
                          auto pair = try_any_cast_ret<Aws::S3::Model::PutObjectOutcome>(outcome);
                          pair->second = true;
                          pair->first = mock_client->put_object(req, *buf);
                      }},
        MockCallback {"s3_file_system::head_object",
                      [](auto&& outcome) {
                          const auto& req = try_any_cast<const Aws::S3::Model::HeadObjectRequest&>(
                                  outcome.at(0));
                          auto pair = try_any_cast_ret<Aws::S3::Model::HeadObjectOutcome>(outcome);
                          pair->second = true;
                          pair->first = mock_client->head_object(req);
                      }},
        MockCallback {"s3_client_factory::create", [](auto&& outcome) {
                          auto pair = try_any_cast_ret<std::shared_ptr<io::S3ObjStorageClient>>(
                                  outcome);
                          pair->second = true;
                      }}};

class S3FileWriterTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        auto sp = SyncPoint::get_instance();
        sp->enable_processing();
        config::file_cache_enter_disk_resource_limit_mode_percent = 99;
        std::for_each(test_mock_callbacks.begin(), test_mock_callbacks.end(),
                      [sp](const MockCallback& mockcallback) {
                          sp->set_call_back(mockcallback.point_name, mockcallback.callback);
                      });
        std::string cur_path = std::filesystem::current_path();
        S3Conf s3_conf;
        s3_conf.client_conf.ak = "fake_ak";
        s3_conf.client_conf.sk = "fake_sk";
        s3_conf.client_conf.endpoint = "fake_s3_endpoint";
        s3_conf.client_conf.region = "fake_s3_region";
        s3_conf.bucket = "fake_s3_bucket";
        s3_conf.prefix = "s3_file_writer_test";
        LOG_INFO("s3 conf is {}", s3_conf.to_string());
        auto res = io::S3FileSystem::create(std::move(s3_conf), io::FileSystem::TMP_FS_ID);
        ASSERT_TRUE(res.has_value()) << res.error();
        s3_fs = res.value();

        std::unique_ptr<ThreadPool> _pool;
        std::ignore = ThreadPoolBuilder("s3_upload_file_thread_pool")
                              .set_min_threads(5)
                              .set_max_threads(10)
                              .build(&_pool);
        ExecEnv::GetInstance()->_s3_file_upload_thread_pool = std::move(_pool);
    }

    static void TearDownTestSuite() {
        auto sp = SyncPoint::get_instance();
        std::for_each(test_mock_callbacks.begin(), test_mock_callbacks.end(),
                      [sp](const MockCallback& mockcallback) {
                          sp->clear_call_back(mockcallback.point_name);
                      });
        sp->disable_processing();
    }
};

TEST_F(S3FileWriterTest, multi_part_io_error) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    auto sp = SyncPoint::get_instance();
    int largerThan5MB = 0;
    sp->set_call_back("S3FileWriter::_upload_one_part", [&largerThan5MB](auto&& outcome) {
        // Deliberately make one upload one part task fail to test if s3 file writer could
        // handle io error
        if (largerThan5MB > 0) {
            LOG(INFO) << "set upload one part to error";
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            auto ptr = try_any_cast<
                    Aws::Utils::Outcome<Aws::S3::Model::UploadPartResult, Aws::S3::S3Error>*>(
                    outcome.back());
            *ptr = Aws::Utils::Outcome<Aws::S3::Model::UploadPartResult, Aws::S3::S3Error>(
                    Aws::Client::AWSError<Aws::S3::S3Errors>());
        }
        largerThan5MB++;
    });
    Defer defer {[&]() { sp->clear_call_back("S3FileWriter::_upload_one_part"); }};
    auto client = s3_fs->client_holder();
    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    constexpr int buf_size = 8192;

    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    doris::Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    auto file_size = local_file_reader->size();
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        st = s3_file_writer->append(Slice(buf, bytes_read));
        ASSERT_TRUE(st.ok()) << st;
        offset += bytes_read;
    }
    ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
    st = s3_file_writer->close(true);
    ASSERT_TRUE(st.ok()) << st;
    // The second part would fail uploading itself to s3
    // so the result of close should be not ok
    st = s3_file_writer->close();
    ASSERT_FALSE(st.ok()) << st;
    bool exists = false;
    st = s3_fs->exists("multi_part_io_error", &exists);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(exists);
}

TEST_F(S3FileWriterTest, offset_test) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    std::map<int, std::shared_ptr<io::FileBuffer>> bufs;

    auto sp = SyncPoint::get_instance();
    // The buffer wouldn't be submitted to the threadpool after it reaches 5MB, it would immediately
    // return when it finishes the appending data logic
    sp->set_call_back("s3_file_writer::appenv_1", [&bufs](auto&& outcome) {
        std::shared_ptr<io::FileBuffer> buf =
                *try_any_cast<std::shared_ptr<io::FileBuffer>*>(outcome.at(0));
        int part_num = try_any_cast<int>(outcome.at(1));
        bufs.emplace(part_num, buf);
    });
    sp->set_call_back("UploadFileBuffer::append_data", [](auto&& outcome) {
        auto pair = try_any_cast_ret<Status>(outcome);
        io::UploadFileBuffer& buf = *try_any_cast<io::UploadFileBuffer*>(outcome.at(0));
        auto size = try_any_cast<size_t>(outcome.at(1));
        buf._size += size;
        pair->second = true;
    });
    sp->set_call_back("UploadFileBuffer::submit", [](auto&& outcome) {
        auto buf = try_any_cast<io::FileBuffer*>(outcome.at(0));
        auto* upload_buf = dynamic_cast<io::UploadFileBuffer*>(buf);
        upload_buf->set_status(Status::OK());
        auto pair = try_any_cast_ret<Status>(outcome);
        pair->second = true;
    });
    Defer defer {[&]() {
        sp->clear_call_back("s3_file_writer::appenv_1");
        sp->clear_call_back("UploadFileBuffer::append_data");
        sp->clear_call_back("UploadFileBuffer::submit");
    }};

    {
        constexpr int buf_size = 8192; // 8 * 1024
        char buf[buf_size];
        doris::Slice slice(buf, buf_size);
        bufs.clear();
        io::FileWriterPtr s3_file_writer;
        auto st = s3_fs->create_file("file1", &s3_file_writer, &state);
        ASSERT_TRUE(st.ok()) << st;
        size_t offset = 0;
        constexpr size_t slice_num = 10;
        std::array<Slice, slice_num> slices;
        slices.fill(slice);
        int cur_part_num = dynamic_cast<io::S3FileWriter*>(s3_file_writer.get())->_cur_part_num;
        for (auto s : slices) {
            st = s3_file_writer->append(s);
            ASSERT_TRUE(st.ok()) << st;
            cur_part_num = dynamic_cast<io::S3FileWriter*>(s3_file_writer.get())->_cur_part_num;
            const auto& buffer = bufs.at(cur_part_num);
            offset += s.get_size();
            ASSERT_EQ(buffer->get_file_offset(), 0);
            ASSERT_EQ(s3_file_writer->bytes_appended(), offset);
        }
    }

    {
        constexpr int buf_size = 8888;
        char buf[buf_size];
        doris::Slice slice(buf, buf_size);
        bufs.clear();
        io::FileWriterPtr s3_file_writer;
        auto st = s3_fs->create_file("file2", &s3_file_writer, &state);
        ASSERT_TRUE(st.ok()) << st;
        size_t offset = 0;
        constexpr size_t slice_num = 1024;
        std::array<Slice, slice_num> slices;
        slices.fill(slice);
        int cur_part_num = dynamic_cast<io::S3FileWriter*>(s3_file_writer.get())->_cur_part_num;
        for (auto s : slices) {
            st = s3_file_writer->append(s);
            ASSERT_TRUE(st.ok()) << st;
            auto ptr = dynamic_cast<io::S3FileWriter*>(s3_file_writer.get());
            cur_part_num = ptr->_cur_part_num;
            const auto& buffer = bufs.at(cur_part_num);
            offset += s.get_size();
            ASSERT_EQ(buffer->get_file_offset(), (cur_part_num - 1) * config::s3_write_buffer_size);
            ASSERT_EQ(s3_file_writer->bytes_appended(), offset);
        }
        st = s3_file_writer->close();
    }
}

TEST_F(S3FileWriterTest, put_object_io_error) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("S3FileWriter::_put_object", [](auto&& outcome) {
        // Deliberately make put object task fail to test if s3 file writer could
        // handle io error
        LOG(INFO) << "set put object to error";
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        io::S3FileWriter* writer = try_any_cast<io::S3FileWriter*>(outcome.at(0));
        auto* buf = try_any_cast<io::UploadFileBuffer*>(outcome.at(1));
        writer->_st = Status::IOError(
                "failed to put object (bucket={}, key={}, upload_id={}, exception=inject "
                "error): "
                "inject error",
                writer->_obj_storage_path_opts.bucket, writer->_obj_storage_path_opts.path.native(),
                writer->upload_id());
        buf->set_status(writer->_st);
        bool* pred = try_any_cast<bool*>(outcome.back());
        *pred = true;
    });
    Defer defer {[&]() { sp->clear_call_back("S3FileWriter::_put_object"); }};
    auto client = s3_fs->client_holder();
    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    constexpr int buf_size = 8192;

    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("put_object_io_error", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    // Only upload 4MB to trigger put object operation
    auto file_size = 4 * 1024 * 1024;
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        st = s3_file_writer->append(Slice(buf, bytes_read));
        ASSERT_TRUE(st.ok()) << st;
        offset += bytes_read;
    }
    ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
    st = s3_file_writer->close(true);
    ASSERT_TRUE(st.ok()) << st;
    // The object might be timeout but still succeed in loading
    st = s3_file_writer->close();
    ASSERT_FALSE(st.ok()) << st;
}

TEST_F(S3FileWriterTest, appendv_random_quit) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    io::FileReaderSPtr local_file_reader;

    ASSERT_EQ(Status::OK(),
              fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader));

    constexpr int buf_size = 8192;
    size_t quit_time = rand() % local_file_reader->size();
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("s3_file_writer::appenv", [&quit_time](auto&& st) {
        if (quit_time == 0) {
            auto pair = try_any_cast_ret<Status>(st);
            pair->second = true;
            pair->first = Status::InternalError("error");
            return;
        }
        quit_time--;
    });
    Defer defer {[&]() { sp->clear_call_back("s3_file_writer::appenv"); }};

    io::FileWriterPtr s3_file_writer;
    auto st = s3_fs->create_file("appendv_random_quit", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    auto file_size = local_file_reader->size();
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        auto st = s3_file_writer->append(Slice(buf, bytes_read));
        if (quit_time == 0) {
            ASSERT_FALSE(st.ok()) << st;
        } else {
            ASSERT_TRUE(st.ok()) << st;
        }
        offset += bytes_read;
    }
    bool exists = false;
    st = s3_fs->exists("appendv_random_quit", &exists);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(exists);
}

TEST_F(S3FileWriterTest, multi_part_open_error) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    constexpr int buf_size = 5 * 1024 * 1024;
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("s3_file_writer::_open", [](auto&& outcome) {
        auto open_outcome =
                try_any_cast<Aws::S3::Model::CreateMultipartUploadOutcome*>(outcome.back());
        *open_outcome =
                Aws::Utils::Outcome<Aws::S3::Model::CreateMultipartUploadResult, Aws::S3::S3Error>(
                        Aws::Client::AWSError<Aws::S3::S3Errors>());
    });
    Defer defer {[&]() { sp->clear_call_back("s3_file_writer::_open"); }};

    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("multi_part_open_error", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    auto buf = std::make_unique<char[]>(buf_size);
    Slice slice(buf.get(), buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    st = local_file_reader->read_at(offset, slice, &bytes_read);
    ASSERT_TRUE(st.ok()) << st;
    // Directly write 5MB would cause one create multi part upload request
    // and it would be rejectd one error
    st = s3_file_writer->append(Slice(buf.get(), bytes_read));
    ASSERT_FALSE(st.ok()) << st;
    bool exists = false;
    st = s3_fs->exists("multi_part_open_error", &exists);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(exists);
}

// TEST_F(S3FileWriterTest, write_into_cache_io_error) {
//     mock_client = std::make_shared<MockS3Client>();
//     std::filesystem::path caches_dir =
//             std::filesystem::current_path() / "s3_file_writer_cache_test";
//     std::string cache_base_path = caches_dir / "cache1" / "";
//     Defer fs_clear {[&]() {
//         if (std::filesystem::exists(cache_base_path)) {
//             std::error_code ec;
//             std::filesystem::remove_all(cache_base_path, ec);
//         }
//     }};
//     io::FileCacheSettings settings;
//     settings.query_queue_size = 10 * 1024 * 1024;
//     settings.query_queue_elements = 100;
//     settings.total_size = 10 * 1024 * 1024;
//     settings.max_file_block_size = 1 * 1024 * 1024;
//     settings.max_query_cache_size = 30;
//     io::FileCacheFactory::instance()._caches.clear();
//     io::FileCacheFactory::instance()._path_to_cache.clear();
//     io::FileCacheFactory::instance()._total_cache_size = 0;
//     auto cache = std::make_unique<io::BlockFileCache>(cache_base_path, settings);
//     ASSERT_TRUE(cache->initialize());
//     while (true) {
//         if (cache->get_lazy_open_success()) {
//             break;
//         };
//         std::this_thread::sleep_for(std::chrono::milliseconds(1));
//     }
//     io::FileCacheFactory::instance()._caches.emplace_back(std::move(cache));
//     doris::io::FileWriterOptions state;
//     auto fs = io::global_local_filesystem();

//     io::FileReaderSPtr local_file_reader;

//     auto st = fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader);
//     ASSERT_TRUE(st.ok()) << st;

//     constexpr int buf_size = 8192;
//     auto sp = SyncPoint::get_instance();
//     config::enable_file_cache = true;
//     // Make append to cache return one error to test if it would exit
//     sp->set_call_back("file_block::append", [](auto&& values) {
//         LOG(INFO) << "file segment append";
//         auto pairs = try_any_cast_ret<Status>(values);
//         pairs->second = true;
//         pairs->first = Status::IOError("failed to append to cache file segments");
//     });
//     sp->set_call_back("S3FileWriter::_complete:3", [](auto&& values) {
//         LOG(INFO) << "don't send s3 complete request";
//         auto pairs = try_any_cast_ret<Status>(values);
//         pairs->second = true;
//     });
//     sp->set_call_back("UploadFileBuffer::upload_to_local_file_cache", [](auto&& values) {
//         LOG(INFO) << "Check if upload failed due to injected error";
//         bool ret = *try_any_cast<bool*>(values.back());
//         ASSERT_FALSE(ret);
//     });
//     Defer defer {[&]() {
//         sp->clear_call_back("file_block::append");
//         sp->clear_call_back("S3FileWriter::_complete:3");
//         sp->clear_call_back("UploadFileBuffer::upload_to_local_file_cache");
//         config::enable_file_cache = false;
//     }};

//     io::FileWriterPtr s3_file_writer;
//     st = s3_fs->create_file("write_into_cache_io_error", &s3_file_writer, &state);
//     ASSERT_TRUE(st.ok()) << st;

//     char buf[buf_size];
//     Slice slice(buf, buf_size);
//     size_t offset = 0;
//     size_t bytes_read = 0;
//     auto file_size = local_file_reader->size();
//     LOG(INFO) << "file size is " << file_size;
//     while (offset < file_size) {
//         st = local_file_reader->read_at(offset, slice, &bytes_read);
//         ASSERT_TRUE(st.ok()) << st;
//         st = s3_file_writer->append(Slice(buf, bytes_read));
//         ASSERT_TRUE(st.ok()) << st;
//         offset += bytes_read;
//     }
//     st = s3_file_writer->finalize();
//     ASSERT_TRUE(st.ok()) << st;
//     st = s3_file_writer->close();
//     ASSERT_TRUE(st.ok()) << st;
// }

// TEST_F(S3FileWriterTest, DISABLED_read_from_cache_io_error) {
//     std::filesystem::path caches_dir =
//             std::filesystem::current_path() / "s3_file_writer_cache_test";
//     std::string cache_base_path = caches_dir / "cache2" / "";
//     Defer fs_clear {[&]() {
//         if (std::filesystem::exists(cache_base_path)) {
//             std::filesystem::remove_all(cache_base_path);
//         }
//     }};
//     io::FileCacheSettings settings;
//     settings.query_queue_size = 10 * 1024 * 1024;
//     settings.query_queue_elements = 100;
//     settings.total_size = 10 * 1024 * 1024;
//     settings.max_file_block_size = 1 * 1024 * 1024;
//     settings.max_query_cache_size = 30;
//     io::FileCacheFactory::instance()._caches.clear();
//     io::FileCacheFactory::instance()._path_to_cache.clear();
//     io::FileCacheFactory::instance()._total_cache_size = 0;
//     auto cache = std::make_unique<io::BlockFileCache>(cache_base_path, settings);
//     ASSERT_TRUE(cache->initialize());
//     while (true) {
//         if (cache->get_lazy_open_success()) {
//             break;
//         };
//         std::this_thread::sleep_for(std::chrono::milliseconds(1));
//     }
//     io::FileCacheFactory::instance()._caches.emplace_back(std::move(cache));
//     doris::io::FileWriterOptions state;
//     auto fs = io::global_local_filesystem();

//     io::FileReaderSPtr local_file_reader;

//     auto st = fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader);
//     ASSERT_TRUE(st.ok()) << st;

//     constexpr int buf_size = 8192;
//     std::atomic_int empty_slice_times = 2;
//     auto sp = SyncPoint::get_instance();
//     config::enable_file_cache = true;
//     // Make the s3 file buffer pool return empty slice for the first two part
//     // to let the first two part be written into file cache first
//     sp->set_call_back("s3_file_bufferpool::allocate", [&empty_slice_times](auto&& values) {
//         LOG(INFO) << "file buffer pool allocate";
//         empty_slice_times--;
//         if (empty_slice_times >= 0) {
//             auto pairs = try_any_cast_ret<Slice>(values);
//             pairs->second = true;
//             LOG(INFO) << "return empty slice";
//         }
//     });
//     // Make append to cache return one error to test if it would exit
//     sp->set_call_back("file_block::read_at", [](auto&& values) {
//         LOG(INFO) << "file segment read at";
//         auto pairs = try_any_cast_ret<Status>(values);
//         pairs->second = true;
//         pairs->first = Status::IOError("failed to read from local cache file segments");
//     });
//     // Let read from cache some time for the next buffer to get one empty slice
//     sp->set_call_back("upload_file_buffer::read_from_cache", [](auto&& /*values*/) {
//         std::this_thread::sleep_for(std::chrono::milliseconds(500));
//     });
//     Defer defer {[&]() {
//         sp->clear_call_back("s3_file_bufferpool::allocate");
//         sp->clear_call_back("file_block::read_at");
//         sp->clear_call_back("upload_file_buffer::read_from_cache");
//         config::enable_file_cache = false;
//     }};

//     io::FileWriterPtr s3_file_writer;
//     st = s3_fs->create_file("read_from_cache_local_io_error", &s3_file_writer, &state);
//     ASSERT_TRUE(st.ok()) << st;

//     char buf[buf_size];
//     Slice slice(buf, buf_size);
//     size_t offset = 0;
//     size_t bytes_read = 0;
//     auto file_size = local_file_reader->size();
//     LOG(INFO) << "file size is " << file_size;
//     while (offset < file_size) {
//         st = local_file_reader->read_at(offset, slice, &bytes_read);
//         ASSERT_TRUE(st.ok()) << st;
//         auto st = s3_file_writer->append(Slice(buf, bytes_read));
//         ASSERT_TRUE(st.ok()) << st;
//         offset += bytes_read;
//     }
//     st = s3_file_writer->finalize();
//     ASSERT_TRUE(st.ok()) << st;
//     st = s3_file_writer->close();
//     ASSERT_FALSE(!st.ok()) << st;
//     bool exists = false;
//     st = s3_fs->exists("read_from_cache_local_io_error", &exists);
//     ASSERT_TRUE(st.ok()) << st;
//     ASSERT_FALSE(exists);
// }

TEST_F(S3FileWriterTest, normal) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    io::FileReaderSPtr local_file_reader;

    ASSERT_TRUE(fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

    constexpr int buf_size = 8192;

    io::FileWriterPtr s3_file_writer;
    auto st = s3_fs->create_file("normal", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    auto file_size = local_file_reader->size();
    LOG_INFO("the file size is {}", file_size);
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        st = s3_file_writer->append(Slice(buf, bytes_read));
        ASSERT_TRUE(st.ok()) << st;
        offset += bytes_read;
    }
    ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
    st = s3_file_writer->close(true);
    ASSERT_TRUE(st.ok()) << st;
    st = s3_file_writer->close();
    ASSERT_TRUE(st.ok()) << st;
    int64_t s3_file_size = 0;
    st = s3_fs->file_size("normal", &s3_file_size);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(s3_file_size, file_size);
    const auto& contents = mock_client->contents();
    std::stringstream ss;
    for (size_t i = 1; i <= contents.size(); i++) {
        ss << contents.at(i);
    }
    std::string content = ss.str();
    std::unique_ptr<char[]> content_buf = std::make_unique<char[]>(file_size);
    Slice s(content_buf.get(), file_size);
    bytes_read = 0;
    st = local_file_reader->read_at(0, s, &bytes_read);
    ASSERT_EQ(0, std::memcmp(content.data(), s.get_data(), file_size));
}

TEST_F(S3FileWriterTest, smallFile) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/olap/test_data/all_types_1000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    constexpr int buf_size = 8192;

    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("small", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    auto file_size = local_file_reader->size();
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        st = s3_file_writer->append(Slice(buf, bytes_read));
        ASSERT_TRUE(st.ok()) << st;
        offset += bytes_read;
    }
    ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
    st = s3_file_writer->close(true);
    ASSERT_TRUE(st.ok()) << st;
    st = s3_file_writer->close();
    ASSERT_TRUE(st.ok()) << st;
    int64_t s3_file_size = 0;
    st = s3_fs->file_size("small", &s3_file_size);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(s3_file_size, file_size);
    const auto& contents = mock_client->contents();
    std::stringstream ss;
    for (size_t i = 1; i <= contents.size(); i++) {
        ss << contents.at(i);
    }
    std::string content = ss.str();
    std::unique_ptr<char[]> content_buf = std::make_unique<char[]>(file_size);
    Slice s(content_buf.get(), file_size);
    bytes_read = 0;
    st = local_file_reader->read_at(0, s, &bytes_read);
    ASSERT_EQ(0, std::memcmp(content.data(), s.get_data(), file_size));
}

TEST_F(S3FileWriterTest, close_error) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/olap/test_data/all_types_1000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("s3_file_writer::close", [](auto&& values) {
        auto pairs = try_any_cast_ret<Status>(values);
        pairs->second = true;
        pairs->first = Status::InternalError("failed to close s3 file writer");
        LOG(INFO) << "return error when closing s3 file writer";
    });
    sp->set_call_back("S3FileWriter::_put_object", [](auto&& values) {
        // Deliberately make put object task fail to test if s3 file writer could
        // handle io error
        LOG(INFO) << "set put object to error";
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        io::S3FileWriter* writer = try_any_cast<io::S3FileWriter*>(values.at(0));
        io::UploadFileBuffer* buf = try_any_cast<io::UploadFileBuffer*>(values.at(1));
        writer->_st = Status::IOError(
                "failed to put object (bucket={}, key={}, upload_id={}, exception=inject "
                "error): "
                "inject error",
                writer->_obj_storage_path_opts.bucket, writer->_obj_storage_path_opts.path.native(),
                writer->upload_id());
        buf->set_status(writer->_st);
        bool* pred = try_any_cast<bool*>(values.back());
        *pred = true;
    });
    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("close_error", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;
    Defer defer {[&]() {
        sp->clear_call_back("s3_file_writer::close");
        sp->clear_call_back("S3FileWriter::_put_object");
    }};

    st = s3_file_writer->close();
    ASSERT_FALSE(st.ok()) << st;
    bool exists = false;
    st = s3_fs->exists("close_error", &exists);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(exists);
}

TEST_F(S3FileWriterTest, multi_part_complete_error_2) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("S3FileWriter::_complete:2", [](auto&& outcome) {
        // Deliberately make one upload one part task fail to test if s3 file writer could
        // handle io error
        auto* parts = try_any_cast<std::vector<std::unique_ptr<Aws::S3::Model::CompletedPart>>*>(
                outcome.back());
        size_t size = parts->size();
        parts->back()->SetPartNumber(size + 2);
    });
    Defer defer {[&]() { sp->clear_call_back("S3FileWriter::_complete:2"); }};
    auto client = s3_fs->client_holder();
    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    constexpr int buf_size = 8192;

    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    auto file_size = local_file_reader->size();
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        st = s3_file_writer->append(Slice(buf, bytes_read));
        ASSERT_TRUE(st.ok()) << st;
        offset += bytes_read;
    }
    ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
    st = s3_file_writer->close(true);
    ASSERT_TRUE(st.ok()) << st;
    // The second part would fail uploading itself to s3
    // so the result of close should be not ok
    st = s3_file_writer->close();
    ASSERT_FALSE(st.ok()) << st;
}

TEST_F(S3FileWriterTest, multi_part_complete_error_1) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("S3FileWriter::_complete:1", [](auto&& outcome) {
        // Deliberately make one upload one part task fail to test if s3 file writer could
        // handle io error
        const auto& points = try_any_cast<const std::pair<
                std::atomic_bool*, std::vector<std::unique_ptr<Aws::S3::Model::CompletedPart>>*>&>(
                outcome.back());
        (*points.first) = false;
        points.second->pop_back();
    });
    Defer defer {[&]() { sp->clear_call_back("S3FileWriter::_complete:1"); }};
    auto client = s3_fs->client_holder();
    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    constexpr int buf_size = 8192;

    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    auto file_size = local_file_reader->size();
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        st = s3_file_writer->append(Slice(buf, bytes_read));
        ASSERT_TRUE(st.ok()) << st;
        offset += bytes_read;
    }
    ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
    st = s3_file_writer->close(true);
    ASSERT_TRUE(st.ok()) << st;
    // The second part would fail uploading itself to s3
    // so the result of close should be not ok
    st = s3_file_writer->close();
    ASSERT_FALSE(st.ok()) << st;
}

TEST_F(S3FileWriterTest, multi_part_complete_error_3) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("S3FileWriter::_complete:3", [](auto&& outcome) {
        auto pair = try_any_cast_ret<io::ObjectStorageResponse>(outcome);
        pair->second = true;
        pair->first = io::ObjectStorageResponse {
                .status = convert_to_obj_response(Status::IOError<false>("inject error"))};
    });
    Defer defer {[&]() { sp->clear_call_back("S3FileWriter::_complete:3"); }};
    auto client = s3_fs->client_holder();
    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    constexpr int buf_size = 8192;

    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    auto file_size = local_file_reader->size();
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        st = s3_file_writer->append(Slice(buf, bytes_read));
        ASSERT_TRUE(st.ok()) << st;
        offset += bytes_read;
    }
    ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
    st = s3_file_writer->close(true);
    ASSERT_TRUE(st.ok()) << st;
    // The second part would fail uploading itself to s3
    // so the result of close should be not ok
    st = s3_file_writer->close();
    ASSERT_FALSE(st.ok()) << st;
}

} // namespace doris
