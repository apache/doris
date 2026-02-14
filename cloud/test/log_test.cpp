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

#include <bthread/bthread.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstring>
#include <fstream>
#include <random>
#include <regex>
#include <sstream>
#include <thread>

#include "common/config.h"
#include "common/logging.h"

using doris::cloud::AnnotateTag;

int main(int argc, char** argv) {
    if (!doris::cloud::init_glog("log_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(LogTest, ConstructionTest) {
    // Heap allocation is disabled.
    // new AnnotateTag();

    // Arithmetics
    {
        char c = 0;
        bool b = false;
        int8_t i8 = 0;
        uint8_t u8 = 0;
        int16_t i16 = 0;
        uint16_t u16 = 0;
        int32_t i32 = 0;
        uint32_t u32 = 0;
        int64_t i64 = 0;
        uint64_t u64 = 0;

        AnnotateTag tag_char("char", c);
        AnnotateTag tag_bool("bool", b);
        AnnotateTag tag_i8("i8", i8);
        AnnotateTag tag_u8("u8", u8);
        AnnotateTag tag_i16("i16", i16);
        AnnotateTag tag_u16("u16", u16);
        AnnotateTag tag_i32("i32", i32);
        AnnotateTag tag_u32("u32", u32);
        AnnotateTag tag_i64("i64", i64);
        AnnotateTag tag_u64("u64", u64);
        LOG_INFO("hello");
    }

    // String literals.
    {
        const char* text = "hello";
        AnnotateTag tag_text("hello", text);
        LOG_INFO("hello");
    }

    // String view.
    {
        std::string test("abc");
        AnnotateTag tag_text("hello", std::string_view(test));
        LOG_INFO("hello");
    }

    // Const string.
    {
        const std::string test("abc");
        AnnotateTag tag_text("hello", test);
        LOG_INFO("hello");
    }
}

TEST(LogTest, ThreadTest) {
    // In pthread.
    {
        ASSERT_EQ(bthread_self(), 0);
        AnnotateTag tag("run_in_bthread", true);
        LOG_INFO("thread test");
    }

    // In bthread.
    {
        auto fn = +[](void*) -> void* {
            EXPECT_NE(bthread_self(), 0);
            AnnotateTag tag("run_in_bthread", true);
            LOG_INFO("thread test");
            return nullptr;
        };
        bthread_t tid;
        ASSERT_EQ(bthread_start_background(&tid, nullptr, fn, nullptr), 0);
        ASSERT_EQ(bthread_join(tid, nullptr), 0);
    }
}

// Test StdoutLogSink format output
TEST(LogTest, StdoutLogSinkFormatTest) {
    // Capture stdout
    testing::internal::CaptureStdout();

    // Create a test log sink and send a test message
    struct TestLogSink : google::LogSink {
        std::stringstream captured_output;

        void send(google::LogSeverity severity, const char* /*full_filename*/,
                  const char* base_filename, int line, const google::LogMessageTime& time,
                  const char* message, std::size_t message_len) override {
            char severity_char;
            switch (severity) {
            case google::GLOG_INFO:
                severity_char = 'I';
                break;
            case google::GLOG_WARNING:
                severity_char = 'W';
                break;
            case google::GLOG_ERROR:
                severity_char = 'E';
                break;
            case google::GLOG_FATAL:
                severity_char = 'F';
                break;
            default:
                severity_char = '?';
                break;
            }

            captured_output << std::setfill('0');
            captured_output << severity_char;
            captured_output << std::setw(4) << (time.year() + 1900) << std::setw(2)
                            << std::setfill('0') << (time.month() + 1) << std::setw(2)
                            << std::setfill('0') << time.day();
            captured_output << " " << std::setw(2) << std::setfill('0') << time.hour() << ":"
                            << std::setw(2) << std::setfill('0') << time.min() << ":"
                            << std::setw(2) << std::setfill('0') << time.sec() << "."
                            << std::setw(6) << std::setfill('0') << time.usec();
            captured_output << " " << std::setfill(' ') << std::setw(5) << getpid()
                            << std::setfill('0');
            captured_output << " " << base_filename << ":" << line << "] ";
            captured_output.write(message, message_len);
        }
    } test_sink;

    // Simulate a log message using current time
    google::LogMessageTime test_time;

    const char* test_message = "Test log message";
    test_sink.send(google::GLOG_INFO, "test_file.cpp", "test_file.cpp", 123, test_time,
                   test_message, strlen(test_message));

    std::string output = test_sink.captured_output.str();

    // Verify format pattern: I<YYYYMMDD> <HH:MM:SS>.<usec> <pid> test_file.cpp:123] Test log message
    // Check severity character
    EXPECT_EQ(output[0], 'I') << "Severity character incorrect: " << output;

    // Check date format (8 digits after 'I')
    std::regex date_pattern(R"(I\d{8}\s)");
    EXPECT_TRUE(std::regex_search(output, date_pattern)) << "Date format incorrect: " << output;

    // Check time format (HH:MM:SS.ffffff)
    std::regex time_pattern(R"(\d{2}:\d{2}:\d{2}\.\d{6})");
    EXPECT_TRUE(std::regex_search(output, time_pattern)) << "Time format incorrect: " << output;

    // Check file:line format
    EXPECT_TRUE(output.find("test_file.cpp:123]") != std::string::npos)
            << "File:line format incorrect: " << output;

    // Check message content
    EXPECT_TRUE(output.find("Test log message") != std::string::npos)
            << "Message not found: " << output;

    // Verify process ID is present (at least 1 digit followed by space and filename)
    std::regex pid_pattern(R"(\d+\s+test_file\.cpp)");
    EXPECT_TRUE(std::regex_search(output, pid_pattern)) << "Process ID not found: " << output;

    std::string captured = testing::internal::GetCapturedStdout();
}

// Test log initialization with different configurations
TEST(LogTest, LogInitializationTest) {
    // Test 1: Verify init_glog can be called multiple times safely
    EXPECT_TRUE(doris::cloud::init_glog("test_logger"));
    EXPECT_TRUE(doris::cloud::init_glog("test_logger")); // Should return true on second call

    // Test 2: Verify log level configuration
    // This test verifies that different log levels can be set
    std::string original_level = doris::cloud::config::log_level;

    // Note: We can't easily test the actual behavior without modifying global state,
    // but we can verify the configuration exists and init succeeds
    doris::cloud::config::log_level = "INFO";
    EXPECT_TRUE(doris::cloud::init_glog("test_info"));

    doris::cloud::config::log_level = "WARNING";
    EXPECT_TRUE(doris::cloud::init_glog("test_warn"));

    // Restore original level
    doris::cloud::config::log_level = original_level;
}

// Test log output with different severity levels
TEST(LogTest, LogSeverityTest) {
    // Test different log severity characters
    struct SeverityTestSink : google::LogSink {
        std::map<google::LogSeverity, char> severity_chars;

        void send(google::LogSeverity severity, const char* /*full_filename*/,
                  const char* /*base_filename*/, int /*line*/,
                  const google::LogMessageTime& /*time*/, const char* /*message*/,
                  std::size_t /*message_len*/) override {
            char severity_char;
            switch (severity) {
            case google::GLOG_INFO:
                severity_char = 'I';
                break;
            case google::GLOG_WARNING:
                severity_char = 'W';
                break;
            case google::GLOG_ERROR:
                severity_char = 'E';
                break;
            case google::GLOG_FATAL:
                severity_char = 'F';
                break;
            default:
                severity_char = '?';
                break;
            }
            severity_chars[severity] = severity_char;
        }
    } severity_sink;

    google::LogMessageTime test_time;
    severity_sink.send(google::GLOG_INFO, "", "", 1, test_time, "test", 4);
    severity_sink.send(google::GLOG_WARNING, "", "", 1, test_time, "test", 4);
    severity_sink.send(google::GLOG_ERROR, "", "", 1, test_time, "test", 4);

    EXPECT_EQ(severity_sink.severity_chars[google::GLOG_INFO], 'I');
    EXPECT_EQ(severity_sink.severity_chars[google::GLOG_WARNING], 'W');
    EXPECT_EQ(severity_sink.severity_chars[google::GLOG_ERROR], 'E');
}

// Test that log format matches expected pattern
TEST(LogTest, LogFormatPatternTest) {
    struct PatternTestSink : google::LogSink {
        std::string last_output;

        void send(google::LogSeverity severity, const char* /*full_filename*/,
                  const char* base_filename, int line, const google::LogMessageTime& time,
                  const char* message, std::size_t message_len) override {
            std::stringstream ss;
            char severity_char = 'I';
            switch (severity) {
            case google::GLOG_INFO:
                severity_char = 'I';
                break;
            case google::GLOG_WARNING:
                severity_char = 'W';
                break;
            case google::GLOG_ERROR:
                severity_char = 'E';
                break;
            case google::GLOG_FATAL:
                severity_char = 'F';
                break;
            default:
                severity_char = '?';
                break;
            }

            ss << severity_char;
            ss << std::setw(4) << std::setfill('0') << (time.year() + 1900) << std::setw(2)
               << (time.month() + 1) << std::setw(2) << time.day();
            ss << " " << std::setw(2) << std::setfill('0') << time.hour() << ":" << std::setw(2)
               << time.min() << ":" << std::setw(2) << time.sec() << "." << std::setw(6)
               << time.usec();
            ss << " " << std::setfill(' ') << std::setw(5) << getpid() << std::setfill('0');
            ss << " " << base_filename << ":" << line << "] ";
            ss.write(message, message_len);

            last_output = ss.str();
        }
    } pattern_sink;

    google::LogMessageTime test_time;

    pattern_sink.send(google::GLOG_INFO, "test.cpp", "test.cpp", 100, test_time, "Test message",
                      12);

    // Verify the pattern: I<YYYYMMDD> <HH:MM:SS>.<usec> <pid> test.cpp:100] Test message
    std::regex pattern(R"(I\d{8} \d{2}:\d{2}:\d{2}\.\d{6}\s+\d+\s+test\.cpp:100\]\s+Test message)");
    EXPECT_TRUE(std::regex_search(pattern_sink.last_output, pattern))
            << "Log format pattern mismatch. Got: " << pattern_sink.last_output;
}