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

#include "exec/csv_scanner.h"

#include <gtest/gtest.h>

#include "util/logging.h"

namespace doris {

class CsvScannerTest : public testing::Test {
public:
    CsvScannerTest() {}

protected:
    virtual void SetUp() { init(); }
    virtual void TearDown() { system("rm -rf ./test_run"); }

    void init();

    void init_desc_tbl();

private:
    std::vector<std::string> _file_paths;
};

void CsvScannerTest::init() {
    system("mkdir -p ./test_run");
    system("pwd");
    system("cp -r ./be/test/exec/test_data/csv_scanner ./test_run/.");

    _file_paths.push_back("./test_run/csv_scanner/csv_file1");
    _file_paths.push_back("./test_run/csv_scanner/csv_file2");
}

TEST_F(CsvScannerTest, normal_use) {
    CsvScanner scanner(_file_paths);
    Status status = scanner.open();
    EXPECT_TRUE(status.ok());

    std::string line_str;
    bool eos = false;
    status = scanner.get_next_row(&line_str, &eos);
    EXPECT_TRUE(status.ok());

    while (!eos) {
        status = scanner.get_next_row(&line_str, &eos);

        if (eos) {
            break;
        }
        EXPECT_TRUE(status.ok());

        LOG(WARNING) << line_str;
    }
}

TEST_F(CsvScannerTest, no_exist_files) {
    std::vector<std::string> no_exist_files;
    no_exist_files.push_back("no_exist_files1");
    no_exist_files.push_back("no_exist_files2");

    CsvScanner scanner(no_exist_files);
    Status status = scanner.open();
    // check until 'get_next_row()'
    EXPECT_TRUE(status.ok());

    std::string line_str;
    bool eos = false;
    status = scanner.get_next_row(&line_str, &eos);
    EXPECT_FALSE(status.ok());
}

} // end namespace doris
