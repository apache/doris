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

// be/test/vec/exec/format/file_reader/new_plain_text_line_reader_test.cpp

#include "format/file_reader/new_plain_text_line_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <utility>

#include "io/fs/file_reader.h"

namespace doris {

namespace {
class RecordingSplitFileReader : public io::FileReader {
public:
    explicit RecordingSplitFileReader(std::string content) : _content(std::move(content)) {}
    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return _content.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }

    std::vector<std::pair<size_t, size_t>> requests;

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext*) override {
        requests.emplace_back(offset, result.size);
        *bytes_read = std::min(result.size, _content.size() - std::min(offset, _content.size()));
        if (*bytes_read > 0) {
            std::memcpy(result.mutable_data(), _content.data() + offset, *bytes_read);
        }
        return Status::OK();
    }

private:
    io::Path _path {"split-prefix-test"};
    std::string _content;
    bool _closed = false;
};
} // namespace

TEST(PlainTextSplitPrefixTest, DelimiterOverlapControlsBackwardProbes) {
    const std::vector<std::pair<std::string, bool>> delimiters = {
            {"\n", false},
            {"abc", false},
            {"aaaaab", false},
            {"||", true},
            {"aba", true},
            {"abcab", true},
            {"ababcabab", true},
            {std::string(100 * 1024 - 1, 'a') + "b", false},
            {std::string(100 * 1024, 'a'), true}};
    for (const auto& [delimiter, overlaps] : delimiters) {
        SCOPED_TRACE(::testing::Message() << "delimiter length=" << delimiter.size()
                                          << ", prefix=" << delimiter.substr(0, 16));
        const std::string first = "xxx";
        const std::string content = first + delimiter + "second" + delimiter + "third";
        const size_t split = first.size() + delimiter.size();
        auto file = std::make_shared<RecordingSplitFileReader>(content);
        RuntimeProfile profile("split_prefix");
        NewPlainTextLineReader reader(
                &profile, file, nullptr,
                std::make_shared<PlainTextLineReaderCtx>(delimiter, delimiter.size(), false),
                content.size() - first.size(), first.size());
        bool eof = false;
        size_t skipped_lines = 0;
        ASSERT_TRUE(reader.skip_split_prefix(split, delimiter, &eof, nullptr, &skipped_lines).ok());
        ASSERT_FALSE(eof);
        EXPECT_EQ(skipped_lines, 1);
        ASSERT_FALSE(file->requests.empty());
        // Only a self-overlapping delimiter requires a probe before the initial read offset.
        EXPECT_EQ(file->requests.front().first < first.size(), overlaps);
        const uint8_t* line = nullptr;
        size_t size = 0;
        ASSERT_TRUE(reader.read_line(&line, &size, &eof, nullptr).ok());
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(line), size), "second");
    }
}

TEST(PlainTextSplitPrefixTest, NearbySynchronizationUsesSmallProbe) {
    const std::string content = std::string(8192, 'x') + "a|||b||c";
    const size_t split = 8192 + 4;
    auto file = std::make_shared<RecordingSplitFileReader>(content);
    RuntimeProfile profile("split_prefix");
    NewPlainTextLineReader reader(&profile, file, nullptr,
                                  std::make_shared<PlainTextLineReaderCtx>("||", 2, false),
                                  content.size() - split + 2, split - 2);
    bool eof = false;
    size_t skipped_lines = 0;
    ASSERT_TRUE(reader.skip_split_prefix(split, "||", &eof, nullptr, &skipped_lines).ok());
    ASSERT_FALSE(eof);
    ASSERT_GE(file->requests.size(), 2);
    EXPECT_EQ(file->requests.front().second, 1024);
    // The next read replays forward from the synchronization point; no larger probe was needed.
    EXPECT_GT(file->requests[1].first, file->requests[0].first);
    EXPECT_EQ(skipped_lines, 2);
    const uint8_t* line = nullptr;
    size_t size = 0;
    ASSERT_TRUE(reader.read_line(&line, &size, &eof, nullptr).ok());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(line), size), "c");
}

TEST(PlainTextSplitPrefixTest, LongRunGrowsProbesWithoutRereading) {
    const std::string prefix = "a" + std::string(256 * 1024 + 1, '|');
    const std::string content = prefix + "b||c";
    const size_t split = prefix.size();
    auto file = std::make_shared<RecordingSplitFileReader>(content);
    RuntimeProfile profile("split_prefix");
    NewPlainTextLineReader reader(&profile, file, nullptr,
                                  std::make_shared<PlainTextLineReaderCtx>("||", 2, false),
                                  content.size() - split + 2, split - 2);
    bool eof = false;
    ASSERT_TRUE(reader.skip_split_prefix(split, "||", &eof, nullptr).ok());
    ASSERT_FALSE(eof);
    size_t previous_offset = split - 2;
    size_t probe_size = 1024;
    size_t probes = 0;
    for (const auto& [offset, length] : file->requests) {
        if (offset >= previous_offset) {
            break; // Forward replay has begun.
        }
        EXPECT_EQ(offset + length, previous_offset);
        EXPECT_EQ(length, std::min(previous_offset, probe_size));
        previous_offset = offset;
        probe_size = std::min(probe_size * 2, size_t {64 * 1024});
        ++probes;
    }
    EXPECT_GT(probes, 6);
    EXPECT_EQ(previous_offset, 0);
    const uint8_t* line = nullptr;
    size_t size = 0;
    ASSERT_TRUE(reader.read_line(&line, &size, &eof, nullptr).ok());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(line), size), "c");
}

// Base test class for text line reader tests
class PlainTextLineReaderTest : public testing::Test {
protected:
    // Helper function to verify line splitting results
    void verify_split_result(const std::string& input, const std::string& line_delim, bool keep_cr,
                             const std::vector<std::string>& expected_lines) {
        PlainTextLineReaderCtx ctx(line_delim, line_delim.size(), keep_cr);
        const auto* data = reinterpret_cast<const uint8_t*>(input.c_str());
        size_t pos = 0;
        size_t size = input.size();
        std::vector<std::string> actual_lines;

        while (pos < size) {
            ctx.refresh();
            const auto* line_end = ctx.read_line(data + pos, size - pos);
            if (!line_end) {
                actual_lines.emplace_back(reinterpret_cast<const char*>(data + pos), size - pos);
                break;
            }
            size_t line_len = line_end - (data + pos);
            actual_lines.emplace_back(reinterpret_cast<const char*>(data + pos), line_len);
            pos += line_len + ctx.line_delimiter_length();
        }

        ASSERT_EQ(expected_lines, actual_lines);
    }
};

// Test cases for PlainTextLineReaderCtx
TEST_F(PlainTextLineReaderTest, PlainTextBasic) {
    verify_split_result("line1\nline2\nline3", "\n", false, {"line1", "line2", "line3"});

    verify_split_result("line1\r\nline2\r\nline3", "\r\n", false, {"line1", "line2", "line3"});

    verify_split_result("line1\r\nline2\r\nline3", "\n", true, {"line1\r", "line2\r", "line3"});

    verify_split_result("line1\n\nline3", "\n", false, {"line1", "", "line3"});

    verify_split_result("line1||line2||line3", "||", false, {"line1", "line2", "line3"});
}

// Test class for CSV line reader with enclosure support
class EncloseCsvLineReaderTest : public testing::Test {
protected:
    // Helper function to verify CSV splitting results including column positions
    void verify_csv_split(const std::string& input, const std::string& line_delim,
                          const std::string& col_sep, char enclose, char escape, bool keep_cr,
                          const std::vector<std::string>& expected_lines,
                          const std::vector<std::vector<size_t>>& expected_col_positions) {
        EncloseCsvLineReaderCtx ctx(line_delim, line_delim.size(), col_sep, col_sep.size(), 10,
                                    enclose, escape, keep_cr);

        const auto* data = reinterpret_cast<const uint8_t*>(input.c_str());
        size_t pos = 0;
        size_t size = input.size();
        std::vector<std::string> actual_lines;
        std::vector<std::vector<size_t>> actual_col_positions;

        while (pos < size) {
            ctx.refresh();
            const uint8_t* line_end = ctx.read_line(data + pos, size - pos);
            if (!line_end) {
                actual_lines.emplace_back(reinterpret_cast<const char*>(data + pos), size - pos);
                actual_col_positions.push_back(ctx.column_sep_positions());
                break;
            }
            size_t line_len = line_end - (data + pos);
            actual_lines.emplace_back(reinterpret_cast<const char*>(data + pos), line_len);
            actual_col_positions.push_back(ctx.column_sep_positions());
            pos += line_len + ctx.line_delimiter_length();
        }

        ASSERT_EQ(expected_lines, actual_lines);
        ASSERT_EQ(expected_col_positions, actual_col_positions);
    }
};

// Basic CSV format test cases
TEST_F(EncloseCsvLineReaderTest, CsvBasic) {
    verify_csv_split("a,b,c\nd,e,f", "\n", ",", '"', '\\', false, {"a,b,c", "d,e,f"},
                     {{1, 3}, {1, 3}});

    verify_csv_split("\"a,x\",b,c\n\"d,y\",e,f", "\n", ",", '"', '\\', false,
                     {"\"a,x\",b,c", "\"d,y\",e,f"}, {{5, 7}, {5, 7}});

    verify_csv_split("\"a\"\"x\",b,c\n\"d\\\"y\",e,f", "\n", ",", '"', '\\', false,
                     {R"("a""x",b,c)", R"("d\"y",e,f)"}, {{6, 8}, {6, 8}});

    verify_csv_split("a||b||c\nd||e||f", "\n", "||", '"', '\\', false, {"a||b||c", "d||e||f"},
                     {{1, 4}, {1, 4}});
}

// Edge cases and corner scenarios
TEST_F(EncloseCsvLineReaderTest, EdgeCases) {
    verify_csv_split("\n\na,b,c", "\n", ",", '"', '\\', false, {"", "", "a,b,c"}, {{}, {}, {1, 3}});

    verify_csv_split("\"abc,def\nghi,jkl", "\n", ",", '"', '\\', false, {"\"abc,def\nghi,jkl"},
                     {{}});

    verify_csv_split("a,b\r\nc,d\ne,f", "\r\n", ",", '"', '\\', false, {"a,b", "c,d\ne,f"},
                     {{1}, {1, 5}});

    verify_csv_split(R"(\,\"\n,b,c)", "\n", ",", '"', '\\', false, {R"(\,\"\n,b,c)"}, {{1, 6, 8}});
}

TEST_F(EncloseCsvLineReaderTest, QuoteEscaping) {
    // Test multiple quoted fields with double-quote escaping in one line
    verify_csv_split(R"("hello ""world\n""","foo ""bar""","test ""quote"" here")", "\n", ",", '"',
                     '\\', false, {R"("hello ""world\n""","foo ""bar""","test ""quote"" here")"},
                     {{19, 33}});

    // Test JSON-like string with escaped quotes
    verify_csv_split(
            R"({""code"": ""100"", ""message"": ""query success"", ""data"": {""status"": ""1""}})",
            "\n", ",", '"', '\\', false,
            {R"({""code"": ""100"", ""message"": ""query success"", ""data"": {""status"": ""1""}})"},
            {{18, 50}});

    // Test custom enclose character
    verify_csv_split(R"({|code|: |100|, |message|: |query success|, |data|: {|status|: |1|}})",
                     "\n", ",", '|', '\\', false,
                     {R"({|code|: |100|, |message|: |query success|, |data|: {|status|: |1|}})"},
                     {{14, 42}});
}

TEST_F(EncloseCsvLineReaderTest, MultiCharDelimiters) {
    // Test multi-character line delimiter
    verify_csv_split("a,b,c\r\n\nd,e,f", "\r\n\n", ",", '"', '\\', false, {"a,b,c", "d,e,f"},
                     {{1, 3}, {1, 3}});

    // Test multi-character column delimiter
    verify_csv_split("a|||b|||c\nd|||e|||f", "\n", "|||", '"', '\\', false,
                     {"a|||b|||c", "d|||e|||f"}, {{1, 5}, {1, 5}});

    // Test both multi-character line and column delimiters
    verify_csv_split("a|||b|||c\r\n\nd|||e|||f", "\r\n\n", "|||", '"', '\\', false,
                     {"a|||b|||c", "d|||e|||f"}, {{1, 5}, {1, 5}});

    verify_csv_split("\"a|||b\"|||c\r\n\n\"d|||e\"|||f", "\r\n\n", "|||", '"', '\\', false,
                     {"\"a|||b\"|||c", "\"d|||e\"|||f"}, {{7}, {7}});
}

} // namespace doris
