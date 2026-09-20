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

#include "io/fs/read_io_trace.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <set>
#include <thread>

#include "io/fs/local_file_system.h"
#include "io/fs/read_io_trace_test_util.h"
#include "io/io_common.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace doris::io {
using namespace std::chrono_literals;

class ReadIOTraceWriterTest : public testing::Test {
public:
    void SetUp() override {
        _root = (std::filesystem::temp_directory_path() /
                 fmt::format("doris-read-io-trace-{}-{}", getpid(), MonotonicNanos()))
                        .native();
        _directory = _root + "/capture";
        ASSERT_TRUE(global_local_filesystem()->create_directory(_root).ok());
    }

    void TearDown() override {
        SyncPoint::get_instance()->disable_processing();
        EXPECT_TRUE(global_local_filesystem()->delete_directory(_root).ok());
    }

    std::vector<rapidjson::Document> records() {
        std::vector<rapidjson::Document> result;
        for (const auto& file : std::filesystem::directory_iterator(_directory)) {
            EXPECT_EQ(file.path().extension(), ".jsonl");
            std::ifstream stream(file.path());
            std::string line;
            while (std::getline(stream, line)) {
                rapidjson::Document document;
                document.Parse(line.data(), line.size());
                EXPECT_FALSE(document.HasParseError()) << line;
                if (document.HasParseError()) {
                    return {};
                }
                result.push_back(std::move(document));
            }
        }
        return result;
    }

    void observe_flush(ReadIOTraceWriter* writer, CountDownLatch* flushed) {
        SyncPoint::get_instance()->set_call_back(
                "ReadIOTraceWriter::after_write",
                [writer, flushed](auto&& args) {
                    if (try_any_cast<ReadIOTraceWriter*>(args[0]) == writer) {
                        flushed->count_down();
                    }
                },
                &_guard);
        SyncPoint::get_instance()->enable_processing();
    }

protected:
    std::string _root;
    std::string _directory;
    SyncPoint::CallbackGuard _guard;
};

TEST_F(ReadIOTraceWriterTest, IdleWriterCreatesNeitherThreadNorFiles) {
    ReadIOTraceWriter writer(_directory, 1024, 1s);
    EXPECT_EQ(writer._thread, nullptr);
    writer.stop();
    EXPECT_FALSE(std::filesystem::exists(_directory));
}

TEST_F(ReadIOTraceWriterTest, StopFlushesOwnedRecordsBelowThreshold) {
    ReadIOTraceWriter writer(_directory, 1024 * 1024, 1h);
    for (int index = 0; index < 100; ++index) {
        std::string line = fmt::format("{{\"index\":{}}}", index);
        writer.append(line);
    }
    EXPECT_FALSE(std::filesystem::exists(_directory));
    writer.stop();
    writer.stop();
    const auto result = records();
    ASSERT_EQ(result.size(), 101);
    for (int index = 0; index < 100; ++index) {
        EXPECT_EQ(result[index]["index"].GetInt(), index);
    }
    EXPECT_STREQ(result.back()["kind"].GetString(), "read_io_trace_status");
    EXPECT_EQ(result.back()["written_events"].GetUint64(), 100);
    EXPECT_EQ(result.back()["dropped_events"].GetUint64(), 0);
}

TEST_F(ReadIOTraceWriterTest, ByteThresholdWakesWriterBeforeInterval) {
    const std::string line = "{\"index\":1}";
    ReadIOTraceWriter writer(_directory, 2 * (line.size() + 1), 1h);
    CountDownLatch flushed(1);
    observe_flush(&writer, &flushed);
    Defer stop {[&] { writer.stop(); }};
    writer.append(line);
    EXPECT_FALSE(std::filesystem::exists(_directory));
    writer.append(line);
    ASSERT_TRUE(flushed.wait_for(5s));
    writer.stop();
    const auto result = records();
    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result.back()["written_events"].GetUint64(), 2);
}

TEST_F(ReadIOTraceWriterTest, IntervalFlushesSmallBatchEvenAfterTracingIsDisabled) {
    const bool enabled = config::enable_read_io_trace;
    Defer restore {[&] { config::enable_read_io_trace = enabled; }};
    ReadIOTraceWriter writer(_directory, 1024 * 1024, 20ms);
    CountDownLatch flushed(1);
    observe_flush(&writer, &flushed);
    Defer stop {[&] { writer.stop(); }};
    writer.append("{\"index\":1}");
    config::enable_read_io_trace = false;
    ASSERT_TRUE(flushed.wait_for(5s));
    writer.stop();
    const auto result = records();
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result.back()["written_events"].GetUint64(), 1);
}

TEST_F(ReadIOTraceWriterTest, ConcurrentProducersKeepEveryRecord) {
    ReadIOTraceWriter writer(_directory, 1024, 10ms);
    std::vector<std::thread> producers;
    for (int producer = 0; producer < 8; ++producer) {
        producers.emplace_back([&, producer] {
            for (int index = 0; index < 1000; ++index) {
                writer.append(fmt::format("{{\"index\":{}}}", producer * 1000 + index));
            }
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }
    writer.stop();
    const auto result = records();
    std::set<int> identities;
    for (const auto& record : result) {
        if (record.HasMember("index")) {
            EXPECT_TRUE(identities.insert(record["index"].GetInt()).second);
        }
    }
    EXPECT_EQ(identities.size(), 8000);
    ASSERT_FALSE(result.empty());
    EXPECT_EQ(result.back()["written_events"].GetUint64(), 8000);
    EXPECT_EQ(result.back()["dropped_events"].GetUint64(), 0);
}

TEST_F(ReadIOTraceWriterTest, ReportsRealFileErrorAndRecovers) {
    // A file occupying the output directory makes create_file fail with ENOTDIR.
    std::ofstream(_directory).close();
    ReadIOTraceWriter writer(_directory, 1, 1h);
    CountDownLatch flushed(1);
    observe_flush(&writer, &flushed);
    Defer stop {[&] { writer.stop(); }};
    writer.append("{\"index\":1}");
    ASSERT_TRUE(flushed.wait_for(5s));
    ASSERT_TRUE(global_local_filesystem()->delete_file(_directory).ok());
    writer.append("{\"index\":2}");
    writer.stop();
    const auto result = records();
    ASSERT_GE(result.size(), 2);
    EXPECT_EQ(result.front()["index"].GetInt(), 2);
    EXPECT_EQ(result.back()["written_events"].GetUint64(), 1);
    EXPECT_EQ(result.back()["dropped_events"].GetUint64(), 1);
}

TEST(ReadIOTraceTest, DisabledTracingEmitsNothingAndAllocatesNoIdentity) {
    ReadIOTraceCapture capture;
    config::enable_read_io_trace = false;
    EXPECT_EQ(ReadIOTrace::next_id(), 0);
    ReadIOTrace::record({.event = "test"});
    EXPECT_TRUE(capture.events("test").empty());
}

TEST(ReadIOTraceTest, EscapesIdentityAndCanBeReenabled) {
    ReadIOTraceCapture capture;
    TUniqueId query;
    query.hi = 1;
    query.lo = 2;
    IOContext context;
    context.query_id = &query;
    context.read_trace_source = FileReadTraceSource::HOLE_FILL;
    const auto id = ReadIOTrace::next_id();
    ASSERT_GT(id, 0);
    ReadIOTrace::record({.event = "test",
                         .context = &context,
                         .file = "s3://bucket/a\"b\n",
                         .id = id,
                         .parent_id = 123,
                         .offset = 17,
                         .size = 29,
                         .time_ns = 456,
                         .start_ns = 400,
                         .bytes = 29});
    auto events = capture.events("test");
    ASSERT_EQ(events.size(), 1);
    EXPECT_STREQ(events[0]["file"].GetString(), "s3://bucket/a\"b\n");
    EXPECT_STREQ(events[0]["source"].GetString(), "hole_fill");
    EXPECT_EQ(events[0]["id"].GetUint64(), id);
    EXPECT_EQ(events[0]["parent_id"].GetUint64(), 123);
    EXPECT_EQ(events[0]["time_ns"].GetInt64(), 456);
    config::enable_read_io_trace = false;
    ReadIOTrace::record({.event = "test"});
    config::enable_read_io_trace = true;
    ReadIOTrace::record({.event = "test"});
    EXPECT_EQ(capture.events("test").size(), 2);
}

} // namespace doris::io
