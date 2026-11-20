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

#include "io/fs/s3_file_reader.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include "common/config.h"
#include "cpp/obj-client/s3_obj_storage_client.h"
#include "io/fs/read_io_trace_test_util.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"
#include "util/s3_util.h"
#include "util/threadpool.h"

namespace doris::io {

class MockRangeReadS3Client final : public Aws::S3::S3Client {
public:
    MOCK_METHOD(Aws::S3::Model::GetObjectOutcome, GetObject,
                (const Aws::S3::Model::GetObjectRequest&), (const, override));
};

namespace {

using namespace std::chrono_literals;

Aws::S3::Model::GetObjectOutcome successful_read(const Aws::S3::Model::GetObjectRequest& request) {
    auto* stream = request.GetResponseStreamFactory()();
    *stream << "0123456789abcdef";
    Aws::S3::Model::GetObjectResult result;
    result.ReplaceBody(stream);
    result.SetContentLength(16);
    return Aws::S3::Model::GetObjectOutcome(std::move(result));
}

class S3FileReaderTest : public testing::Test {
protected:
    static void SetUpTestSuite() { S3ClientFactory::instance(); }
};

TEST_F(S3FileReaderTest, ConcurrentReadsCollectAllStatistics) {
    constexpr size_t thread_count = 8;
    constexpr size_t reads_per_thread = 100;
    auto client = std::make_shared<MockRangeReadS3Client>();
    EXPECT_CALL(*client, GetObject(testing::_))
            .Times(thread_count * reads_per_thread)
            .WillRepeatedly(successful_read);
    auto holder = std::make_shared<ObjClientHolder>(S3ClientConf {});
    holder->_client = std::make_shared<S3ObjStorageClient>(client);
    RuntimeProfile profile("parallel-s3-reads");
    S3FileReader reader(holder, "bucket", "key", 16 * thread_count, &profile);
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(
            ThreadPoolBuilder("S3FileReaderTest").set_max_threads(thread_count).build(&pool).ok());
    CountDownLatch ready(thread_count);
    std::array<Status, thread_count> results;
    for (size_t index = 0; index < thread_count; ++index) {
        ASSERT_TRUE(pool->submit_func([&, index]() {
                            ready.count_down();
                            EXPECT_TRUE(ready.wait_for(5s));
                            for (size_t iteration = 0; iteration < reads_per_thread; ++iteration) {
                                std::string buffer(16, '\0');
                                size_t bytes_read = 0;
                                results[index] =
                                        reader.read_at(index * 16, Slice(buffer), &bytes_read);
                                if (!results[index].ok()) {
                                    return;
                                }
                                EXPECT_EQ(bytes_read, 16);
                                EXPECT_EQ(buffer, "0123456789abcdef");
                            }
                        }).ok());
    }
    pool->wait();
    for (const auto& result : results) {
        ASSERT_TRUE(result.ok()) << result;
    }
    reader._collect_profile_before_close();
    EXPECT_EQ(profile.get_counter("TotalGetRequest")->value(), thread_count * reads_per_thread);
    EXPECT_EQ(profile.get_counter("TotalBytesRead")->value(), 16 * thread_count * reads_per_thread);
    EXPECT_GT(profile.get_counter("TotalGetRequestTime")->value(), 0);
    EXPECT_EQ(profile.get_counter("TooManyRequestErr")->value(), 0);
}

TEST_F(S3FileReaderTest, TraceClipsEOFAndLabelsReadSources) {
    ReadIOTraceCapture trace;
    auto client = std::make_shared<MockRangeReadS3Client>();
    EXPECT_CALL(*client, GetObject(testing::_)).Times(4).WillRepeatedly(successful_read);
    auto holder = std::make_shared<ObjClientHolder>(S3ClientConf {});
    holder->_client = std::make_shared<S3ObjStorageClient>(client);
    S3FileReader reader(holder, "bucket", "key", 32, nullptr);
    IOContext context;
    context.reader_type = ReaderType::READER_QUERY;
    std::string buffer(32, '\0');
    size_t bytes_read = 0;
    for (auto source : {FileReadTraceSource::NORMAL, FileReadTraceSource::READ_AHEAD,
                        FileReadTraceSource::HOLE_FILL}) {
        context.read_trace_source = source;
        ASSERT_TRUE(reader.read_at(16, Slice(buffer), &bytes_read, &context).ok());
    }
    ASSERT_TRUE(reader.read_at(16, Slice(buffer), &bytes_read).ok());
    const auto events = trace.events("s3_get");
    ASSERT_EQ(events.size(), 4);
    EXPECT_STREQ(events[0]["source"].GetString(), "sync");
    EXPECT_STREQ(events[1]["source"].GetString(), "read_ahead");
    EXPECT_STREQ(events[2]["source"].GetString(), "hole_fill");
    EXPECT_STREQ(events[3]["source"].GetString(), "other");
    for (const auto& event : events) {
        EXPECT_EQ(event["offset"].GetUint64(), 16);
        EXPECT_EQ(event["size"].GetUint64(), 16);
        EXPECT_EQ(event["bytes"].GetUint64(), 16);
    }
}

TEST_F(S3FileReaderTest, CollectsRetryStatistics) {
    ReadIOTraceCapture trace;
    const auto old_retries = config::max_s3_client_retry;
    const auto old_base_wait = config::s3_read_base_wait_time_ms;
    const auto old_max_wait = config::s3_read_max_wait_time_ms;
    Defer restore {[&]() {
        config::max_s3_client_retry = old_retries;
        config::s3_read_base_wait_time_ms = old_base_wait;
        config::s3_read_max_wait_time_ms = old_max_wait;
    }};
    config::max_s3_client_retry = 1;
    config::s3_read_base_wait_time_ms = 1;
    config::s3_read_max_wait_time_ms = 2;
    auto client = std::make_shared<MockRangeReadS3Client>();
    EXPECT_CALL(*client, GetObject(testing::_))
            .WillOnce([](const Aws::S3::Model::GetObjectRequest&) {
                Aws::S3::S3Error error;
                error.SetResponseCode(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS);
                return Aws::S3::Model::GetObjectOutcome(std::move(error));
            })
            .WillOnce(successful_read);
    auto holder = std::make_shared<ObjClientHolder>(S3ClientConf {});
    holder->_client = std::make_shared<S3ObjStorageClient>(client);
    RuntimeProfile profile("retry-s3-read");
    S3FileReader reader(holder, "bucket", "key", 16, &profile);
    std::string buffer(16, '\0');
    size_t bytes_read = 0;
    IOContext io_context;
    io_context.read_trace_source = FileReadTraceSource::HOLE_FILL;
    io_context.read_trace_id = 321;
    ASSERT_TRUE(reader.read_at(0, Slice(buffer), &bytes_read, &io_context).ok());
    EXPECT_EQ(buffer, "0123456789abcdef");
    reader._collect_profile_before_close();
    EXPECT_EQ(profile.get_counter("TotalGetRequest")->value(), 2);
    EXPECT_EQ(profile.get_counter("TotalBytesRead")->value(), 16);
    EXPECT_EQ(profile.get_counter("TooManyRequestErr")->value(), 1);
    EXPECT_EQ(profile.get_counter("TooManyRequestSleepTime")->value(), 2);
    auto events = trace.events("s3_get");
    ASSERT_EQ(events.size(), 2);
    EXPECT_STREQ(events[0]["outcome"].GetString(), "failed_or_short");
    EXPECT_EQ(events[0]["attempt"].GetInt(), 0);
    EXPECT_STREQ(events[1]["outcome"].GetString(), "success");
    EXPECT_STREQ(events[1]["source"].GetString(), "hole_fill");
    EXPECT_STREQ(events[1]["file"].GetString(), "s3://bucket/key");
    EXPECT_EQ(events[1]["parent_id"].GetUint64(), 321);
    EXPECT_EQ(events[1]["attempt"].GetInt(), 1);
    EXPECT_EQ(events[1]["bytes"].GetUint64(), 16);
    EXPECT_GE(events[1]["time_ns"].GetInt64(), events[1]["start_ns"].GetInt64());
}

} // namespace
} // namespace doris::io
