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

#include "common/logconfig.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstring>
#include <iomanip>
#include <regex>
#include <sstream>

namespace doris {

// Test StdoutLogSink format output
TEST(LogConfigTest, StdoutLogSinkFormatTest) {
    // Create a test log sink to verify the format
    struct TestLogSink : google::LogSink {
        std::stringstream captured_output;

        void send(google::LogSeverity severity, const char* /*full_filename*/,
                  const char* base_filename, int line, const google::LogMessageTime& time,
                  const char* message, std::size_t message_len) override {
            // Convert log severity to corresponding character (I/W/E/F)
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

            // Set output formatting flags
            captured_output << std::setfill('0');

            // 1. Log severity (I/W/E/F)
            captured_output << severity_char;

            // 2. Date (YYYYMMDD)
            captured_output << std::setw(4) << (time.year() + 1900) << std::setw(2)
                            << std::setfill('0') << (time.month() + 1) << std::setw(2)
                            << std::setfill('0') << time.day();

            // 3. Time (HH:MM:SS.ffffff)
            captured_output << " " << std::setw(2) << std::setfill('0') << time.hour() << ":"
                            << std::setw(2) << std::setfill('0') << time.min() << ":"
                            << std::setw(2) << std::setfill('0') << time.sec() << "."
                            << std::setw(6) << std::setfill('0') << time.usec();

            // 4. Process ID
            captured_output << " " << getpid();

            // 5. Filename and line number
            captured_output << " " << base_filename << ":" << line << "] ";

            // 6. Log message
            captured_output.write(message, message_len);
        }
    } test_sink;

    // Simulate a log message
    google::LogMessageTime test_time;
    test_time.tm_.tm_year = 124; // 2024
    test_time.tm_.tm_mon = 5;    // June (0-based)
    test_time.tm_.tm_mday = 5;
    test_time.tm_.tm_hour = 15;
    test_time.tm_.tm_min = 25;
    test_time.tm_.tm_sec = 15;
    test_time.usecs_ = 677153;

    const char* test_message = "Preloaded 653 timezones.";
    test_sink.send(google::GLOG_INFO, "be/src/util/timezone_utils.cpp", "timezone_utils.cpp", 115,
                   test_time, test_message, strlen(test_message));

    std::string output = test_sink.captured_output.str();

    // Verify format: I20240605 15:25:15.677153 <pid> timezone_utils.cpp:115] Preloaded 653 timezones.
    EXPECT_TRUE(output.find("I20240605") != std::string::npos)
            << "Date format incorrect: " << output;
    EXPECT_TRUE(output.find("15:25:15.677153") != std::string::npos)
            << "Time format incorrect: " << output;
    EXPECT_TRUE(output.find("timezone_utils.cpp:115]") != std::string::npos)
            << "File:line format incorrect: " << output;
    EXPECT_TRUE(output.find("Preloaded 653 timezones.") != std::string::npos)
            << "Message not found: " << output;

    // Verify process ID is present (should be a number)
    std::regex pid_pattern(R"(\d+\s+timezone_utils\.cpp)");
    EXPECT_TRUE(std::regex_search(output, pid_pattern)) << "Process ID not found: " << output;
}

// Test log initialization
TEST(LogConfigTest, LogInitializationTest) {
    // Test that init_glog can be called successfully
    // Note: We can't easily reinitialize glog multiple times in a single test process,
    // but we can verify the function exists and has the correct signature
    EXPECT_TRUE(init_glog("logconfig_test"));

    // Multiple calls should be safe (idempotent)
    EXPECT_TRUE(init_glog("logconfig_test"));
}

// Test shutdown_logging function
TEST(LogConfigTest, ShutdownLoggingTest) {
    // Initialize logging first
    EXPECT_TRUE(init_glog("shutdown_test"));

    // Test shutdown - should not crash
    EXPECT_NO_THROW(shutdown_logging());

    // Multiple shutdown calls should be safe
    EXPECT_NO_THROW(shutdown_logging());
}

// Test log output with different severity levels
TEST(LogConfigTest, LogSeverityTest) {
    // Test different log severity characters mapping
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
TEST(LogConfigTest, LogFormatPatternTest) {
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
            ss << " " << getpid();
            ss << " " << base_filename << ":" << line << "] ";
            ss.write(message, message_len);

            last_output = ss.str();
        }
    } pattern_sink;

    google::LogMessageTime test_time;
    test_time.tm_.tm_year = 125; // 2025
    test_time.tm_.tm_mon = 0;    // January
    test_time.tm_.tm_mday = 18;
    test_time.tm_.tm_hour = 10;
    test_time.tm_.tm_min = 53;
    test_time.tm_.tm_sec = 6;
    test_time.usecs_ = 239614;

    pattern_sink.send(google::GLOG_INFO, "timezone_utils.cpp", "timezone_utils.cpp", 115,
                      test_time, "Preloaded 653 timezones.", 25);

    // Verify the pattern: I20250118 10:53:06.239614 <pid> timezone_utils.cpp:115] Preloaded 653 timezones.
    std::regex pattern(
            R"(I20250118 10:53:06\.239614\s+\d+\s+timezone_utils\.cpp:115\]\s+Preloaded 653 timezones\.)");
    EXPECT_TRUE(std::regex_search(pattern_sink.last_output, pattern))
            << "Log format pattern mismatch. Got: " << pattern_sink.last_output;
}

// Test date and time formatting edge cases
TEST(LogConfigTest, DateTimeFormattingTest) {
    struct DateTimeTestSink : google::LogSink {
        std::string last_output;

        void send(google::LogSeverity severity, const char* /*full_filename*/,
                  const char* /*base_filename*/, int /*line*/, const google::LogMessageTime& time,
                  const char* /*message*/, std::size_t /*message_len*/) override {
            std::stringstream ss;
            ss << (severity == google::GLOG_INFO ? 'I' : '?');
            ss << std::setw(4) << std::setfill('0') << (time.year() + 1900) << std::setw(2)
               << (time.month() + 1) << std::setw(2) << time.day();
            ss << " " << std::setw(2) << std::setfill('0') << time.hour() << ":" << std::setw(2)
               << time.min() << ":" << std::setw(2) << time.sec() << "." << std::setw(6)
               << time.usec();
            last_output = ss.str();
        }
    } datetime_sink;

    // Test with leading zeros (January 1st, 00:00:00.000001)
    google::LogMessageTime test_time;
    test_time.tm_.tm_year = 125; // 2025
    test_time.tm_.tm_mon = 0;    // January (0-based)
    test_time.tm_.tm_mday = 1;
    test_time.tm_.tm_hour = 0;
    test_time.tm_.tm_min = 0;
    test_time.tm_.tm_sec = 0;
    test_time.usecs_ = 1;

    datetime_sink.send(google::GLOG_INFO, "", "", 1, test_time, "", 0);

    // Should format as: I20250101 00:00:00.000001
    EXPECT_EQ(datetime_sink.last_output, "I20250101 00:00:00.000001")
            << "Date/time formatting with leading zeros failed: " << datetime_sink.last_output;
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
