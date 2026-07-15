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

#include "load/stream_load/stream_load_recorder_manager.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "common/status.h"

namespace doris {

TEST(StreamLoadRecorderManagerTest, ParseAcceptedStreamLoadResponses) {
    EXPECT_TRUE(
            StreamLoadRecorderManager::_parse_stream_load_response(R"({"Status":"Success"})").ok());
    EXPECT_TRUE(StreamLoadRecorderManager::_parse_stream_load_response(
                        R"({"Status":"Publish Timeout"})")
                        .ok());
    EXPECT_TRUE(StreamLoadRecorderManager::_parse_stream_load_response(
                        R"({"Status":"Label Already Exists","ExistingJobStatus":"FINISHED"})")
                        .ok());

    EXPECT_FALSE(StreamLoadRecorderManager::_parse_stream_load_response(
                         R"({"Status":"Label Already Exists","ExistingJobStatus":"RUNNING"})")
                         .ok());
    EXPECT_FALSE(
            StreamLoadRecorderManager::_parse_stream_load_response(R"({"Status":"Fail"})").ok());
    EXPECT_FALSE(StreamLoadRecorderManager::_parse_stream_load_response("not json").ok());
}

TEST(StreamLoadRecorderManagerTest, RetryKeepsBoundedBatchAndStableLabel) {
    StreamLoadRecorderManager manager;
    manager._buffer.append("record");
    manager._record_num = 1;
    manager._last_fetch_key = "1000_source_label";
    manager._last_load_time = 0;

    std::vector<std::pair<std::string, std::string>> attempts;
    manager.set_stream_load_sender_for_test(
            [&attempts](const std::string& data, const std::string& label) {
                attempts.emplace_back(data, label);
                if (attempts.size() == 1) {
                    return Status::InternalError("injected retryable failure");
                }
                return Status::OK();
            });

    manager._load_if_necessary();
    ASSERT_EQ(attempts.size(), 1);
    EXPECT_TRUE(manager._has_pending_batch());
    EXPECT_EQ(manager._buffer.ToString(), "record");
    EXPECT_EQ(manager._record_num, 1);

    manager._load_if_necessary();
    ASSERT_EQ(attempts.size(), 2);
    EXPECT_EQ(attempts[0], attempts[1]);
    EXPECT_FALSE(manager._has_pending_batch());
    EXPECT_TRUE(manager._buffer.empty());
    EXPECT_EQ(manager._record_num, 0);
}

TEST(StreamLoadRecorderManagerTest, StopIsIdempotent) {
    StreamLoadRecorderManager manager;
    manager._worker_thread = std::thread([] {});

    ASSERT_TRUE(manager._worker_thread.joinable());
    manager.stop();
    EXPECT_FALSE(manager._worker_thread.joinable());

    manager.stop();
    EXPECT_FALSE(manager._worker_thread.joinable());
}

} // namespace doris
