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

#include <filesystem>

#include "common/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "olap/wal_reader.h"
#include "olap/wal_writer.h"
#include "testutil/test_util.h"
#include "util/file_utils.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

class WalReaderWriterTest : public testing::Test {
public:
    // create a mock cgroup folder
    virtual void SetUp() {
        std::filesystem::remove_all(_s_test_data_path);
        EXPECT_FALSE(std::filesystem::exists(_s_test_data_path));
        // create a mock cgroup path
        EXPECT_TRUE(std::filesystem::create_directory(_s_test_data_path));
    }

    // delete the mock cgroup folder
    virtual void TearDown() { EXPECT_TRUE(std::filesystem::remove_all(_s_test_data_path)); }

    static std::string _s_test_data_path;
};

std::string WalReaderWriterTest::_s_test_data_path = "./log/wal_reader_writer_test";
const int COLUMN_SIZE = 4;
const int STRING_VALUE_LENGTH = 400;

void generate_row_number_value(PDataRow* row, int row_index) {
    for (int i = 0; i < COLUMN_SIZE; ++i) {
        auto col = row->add_col();
        col->set_value(std::to_string(row_index * COLUMN_SIZE + i));
    }
}

std::string generate_random_string(const int len) {
    std::string possible_characters =
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<> dist(0, possible_characters.size() - 1);

    std::string rand_str(len, '\0');
    for (auto& dis : rand_str) {
        dis = possible_characters[dist(generator)];
    }
    return rand_str;
}

void generate_row_string_value(PDataRow* row, int row_index) {
    for (int i = 0; i < COLUMN_SIZE; ++i) {
        auto col = row->add_col();
        if (i == 0) {
            col->set_value(std::to_string(row_index));
        } else {
            col->set_value(generate_random_string(STRING_VALUE_LENGTH));
        }
    }
}

TEST_F(WalReaderWriterTest, TestWriteAndRead1) {
    std::string file_name = _s_test_data_path + "/abcd123.txt";
    auto wal_writer = WalWriter(file_name);
    wal_writer.init();
    EXPECT_EQ(0, wal_writer.row_count());
    EXPECT_EQ(0, wal_writer.file_length());
    size_t file_len = 0;
    auto row_index = 0;
    // append one row
    {
        PDataRowArray rows;
        PDataRow* row = rows.Add();
        generate_row_number_value(row, row_index);
        file_len += row->ByteSizeLong() + WalWriter::ROW_LENGTH_SIZE;
        ++row_index;
        wal_writer.append_rows(rows);
        EXPECT_EQ(row_index, wal_writer.row_count());
        EXPECT_EQ(file_len, wal_writer.file_length());
    }
    // append 5 rows
    {
        PDataRowArray rows;
        for (int j = 0; j < 5; j++) {
            PDataRow* row = rows.Add();
            generate_row_number_value(row, row_index);
            file_len += row->ByteSizeLong() + WalWriter::ROW_LENGTH_SIZE;
            ++row_index;
        }
        wal_writer.append_rows(rows);
        EXPECT_EQ(row_index, wal_writer.row_count());
        EXPECT_EQ(file_len, wal_writer.file_length());
    }
    // append 4 rows
    {
        PDataRowArray rows;
        for (int j = 0; j < 4; j++) {
            PDataRow* row = rows.Add();
            generate_row_number_value(row, row_index);
            file_len += row->ByteSizeLong() + WalWriter::ROW_LENGTH_SIZE;
            ++row_index;
        }
        wal_writer.append_rows(rows);
        EXPECT_EQ(row_index, wal_writer.row_count());
        EXPECT_EQ(file_len, wal_writer.file_length());
    }
    wal_writer.finalize();
    std::vector<string> files;
    FileUtils::list_files(Env::Default(), _s_test_data_path, &files);
    EXPECT_EQ(1, files.size());
    auto wal_reader = WalReader(file_name);
    wal_reader.init();
    auto row_count = 0;
    while (true) {
        doris::PDataRow prow;
        Status st = wal_reader.read_row(prow);
        if (st.ok()) {
            PDataRow row;
            row.CopyFrom(prow);
            EXPECT_EQ(COLUMN_SIZE, row.col_size());
            for (auto i = 0; i < COLUMN_SIZE; ++i) {
                EXPECT_EQ(std::to_string(row_count * COLUMN_SIZE + i), row.col(i).value());
            }
            ++row_count;
        } else if (st.is_end_of_file()) {
            break;
        } else {
            std::cout << "failed read wal: " << st.to_string();
            EXPECT_TRUE(false);
        }
    }
    EXPECT_EQ(row_index, row_count);
}

TEST_F(WalReaderWriterTest, TestWriteAndRead2) {
    std::string file_name = _s_test_data_path + "/abcd456.txt";
    auto wal_writer = WalWriter(file_name);
    wal_writer.init();
    EXPECT_EQ(0, wal_writer.row_count());
    EXPECT_EQ(0, wal_writer.file_length());
    size_t file_len = 0;
    auto row_index = 0;
    // append 20*8000 rows
    for (int i = 0; i < 20; i++) {
        PDataRowArray rows;
        for (int j = 0; j < 8000; j++) {
            PDataRow* row = rows.Add();
            generate_row_string_value(row, row_index);
            file_len += row->ByteSizeLong() + WalWriter::ROW_LENGTH_SIZE;
            ++row_index;
        }
        wal_writer.append_rows(rows);
        EXPECT_EQ(row_index, wal_writer.row_count());
        EXPECT_EQ(file_len, wal_writer.file_length());
    }
    wal_writer.finalize();
    std::vector<string> files;
    FileUtils::list_files(Env::Default(), _s_test_data_path, &files);
    EXPECT_EQ(1, files.size());
    auto wal_reader = WalReader(file_name);
    wal_reader.init();
    auto row_count = 0;
    while (true) {
        doris::PDataRow prow;
        Status st = wal_reader.read_row(prow);
        if (st.ok()) {
            PDataRow row;
            row.CopyFrom(prow);
            EXPECT_EQ(COLUMN_SIZE, row.col_size());
            EXPECT_EQ(std::to_string(row_count), row.col(0).value());
            for (int i = 1; i < COLUMN_SIZE; ++i) {
                EXPECT_EQ(STRING_VALUE_LENGTH, row.col(i).value().length());
            }
            ++row_count;
        } else if (st.is_end_of_file()) {
            break;
        } else {
            std::cout << "failed read wal: " << st.to_string();
            EXPECT_TRUE(false);
        }
    }
    EXPECT_EQ(row_index, row_count);
}
} // namespace doris