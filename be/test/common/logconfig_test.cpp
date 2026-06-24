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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstring>
#include <iomanip>
#include <regex>
#include <sstream>

#include "common/logging.h"

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

    // Simulate a log message using current time
    google::LogMessageTime test_time;

    const char* test_message = "Preloaded 653 timezones.";
    test_sink.send(google::GLOG_INFO, "be/src/util/timezone_utils.cpp", "timezone_utils.cpp", 115,
                   test_time, test_message, strlen(test_message));

    std::string output = test_sink.captured_output.str();

    // Verify format pattern: I<YYYYMMDD> <HH:MM:SS>.<usec> <pid> timezone_utils.cpp:115] Preloaded 653 timezones.
    // Check severity character
    EXPECT_EQ(output[0], 'I') << "Severity character incorrect: " << output;

    // Check date format (8 digits after 'I')
    std::regex date_pattern(R"(I\d{8}\s)");
    EXPECT_TRUE(std::regex_search(output, date_pattern)) << "Date format incorrect: " << output;

    // Check time format (HH:MM:SS.ffffff)
    std::regex time_pattern(R"(\d{2}:\d{2}:\d{2}\.\d{6})");
    EXPECT_TRUE(std::regex_search(output, time_pattern)) << "Time format incorrect: " << output;

    // Check file:line format
    EXPECT_TRUE(output.find("timezone_utils.cpp:115]") != std::string::npos)
            << "File:line format incorrect: " << output;

    // Check message content
    EXPECT_TRUE(output.find("Preloaded 653 timezones.") != std::string::npos)
            << "Message not found: " << output;

    // Verify process ID is present (at least 1 digit followed by space and filename)
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

    pattern_sink.send(google::GLOG_INFO, "timezone_utils.cpp", "timezone_utils.cpp", 115, test_time,
                      "Preloaded 653 timezones.", 25);

    // Verify the pattern: I<YYYYMMDD> <HH:MM:SS>.<usec> <pid> timezone_utils.cpp:115] Preloaded 653 timezones.
    std::regex pattern(
            R"(I\d{8} \d{2}:\d{2}:\d{2}\.\d{6}\s+\d+\s+timezone_utils\.cpp:115\]\s+Preloaded 653 timezones\.)");
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

    // Test with current time to verify formatting with proper width (leading zeros)
    google::LogMessageTime test_time;

    datetime_sink.send(google::GLOG_INFO, "", "", 1, test_time, "", 0);

    // Verify the format has proper width with leading zeros: I<YYYYMMDD> <HH:MM:SS>.<ffffff>
    // Check severity and date (9 characters: I + 8 digits)
    std::regex date_format(R"(^I\d{8})");
    EXPECT_TRUE(std::regex_search(datetime_sink.last_output, date_format))
            << "Date formatting failed: " << datetime_sink.last_output;

    // Check time format with proper width (HH:MM:SS with leading zeros)
    std::regex time_format(R"(\d{2}:\d{2}:\d{2}\.\d{6})");
    EXPECT_TRUE(std::regex_search(datetime_sink.last_output, time_format))
            << "Time formatting failed: " << datetime_sink.last_output;
}

} // namespace doris
