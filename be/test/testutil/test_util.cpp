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

#include "testutil/test_util.h"

#ifndef __APPLE__
#else
#include <mach-o/dyld.h>
#endif

#include <common/config.h>
#include <ctype.h>
#include <glog/logging.h>
#include <libgen.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include <algorithm>
#include <string>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>

#include "absl/strings/substitute.h"
#include "gflags/gflags.h"
#include "olap/olap_common.h"

DEFINE_bool(gen_out, false, "generate expected check data for test");
DEFINE_bool(
        gen_regression_case, false,
        "generate regression test cases corrresponding to ut cases for ut cases that support it");

const std::string kApacheLicenseHeader =
        R"(// Licensed to the Apache Software Foundation (ASF) under one
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
)";

namespace doris {

static const char* const kSlowTestsEnvVar = "DORIS_ALLOW_SLOW_TESTS";

bool AllowSlowTests() {
    return GetBooleanEnvironmentVariable(kSlowTestsEnvVar);
}

bool GetBooleanEnvironmentVariable(const char* env_var_name) {
    const char* const e = getenv(env_var_name);
    if ((e == nullptr) || (strlen(e) == 0) || (strcasecmp(e, "false") == 0) ||
        (strcasecmp(e, "0") == 0) || (strcasecmp(e, "no") == 0)) {
        return false;
    }
    if ((strcasecmp(e, "true") == 0) || (strcasecmp(e, "1") == 0) || (strcasecmp(e, "yes") == 0)) {
        return true;
    }
    LOG(FATAL) << absl::Substitute("$0: invalid value for environment variable $1", e,
                                   env_var_name);
    return false; // unreachable
}

std::string GetCurrentRunningDir() {
    char exe[PATH_MAX];
    ssize_t r;

#ifdef __APPLE__
    uint32_t size = PATH_MAX;
    if (_NSGetExecutablePath(exe, &size) < 0) {
        return std::string();
    }
    r = strlen(exe) + 1;
#else
    if ((r = readlink("/proc/self/exe", exe, PATH_MAX)) < 0) {
        return std::string();
    }
#endif

    if (r == PATH_MAX) {
        r -= 1;
    }
    exe[r] = 0;
    char* dir = dirname(exe);
    return std::string(dir);
}

void InitConfig() {
    std::string conf_file = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!config::init(conf_file.c_str(), false)) {
        fprintf(stderr, "Init config file failed, path %s\n", conf_file.c_str());
        exit(-1);
    }
}

bool equal_ignore_case(std::string lhs, std::string rhs) {
    std::transform(lhs.begin(), lhs.end(), lhs.begin(), ::tolower);
    std::transform(rhs.begin(), rhs.end(), rhs.begin(), ::tolower);
    return lhs == rhs;
}

std::mt19937_64 rng_64(std::chrono::steady_clock::now().time_since_epoch().count());

int rand_rng_int(int l, int r) {
    std::uniform_int_distribution<int> u(l, r);
    return u(rng_64);
}
char rand_rng_char() {
    return (rand_rng_int(0, 1) ? 'a' : 'A') + rand_rng_int(0, 25);
}
std::string rand_rng_string(size_t length) {
    std::string s;
    while (length--) {
        s += rand_rng_char();
    }
    return s;
}
std::string rand_rng_by_type(FieldType fieldType) {
    if (fieldType == FieldType::OLAP_FIELD_TYPE_CHAR) {
        return rand_rng_string(rand_rng_int(1, 8));
    } else if (fieldType == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
        return rand_rng_string(rand_rng_int(1, 128));
    } else if (fieldType == FieldType::OLAP_FIELD_TYPE_STRING) {
        return rand_rng_string(rand_rng_int(1, 100000));
    } else {
        return std::to_string(rand_rng_int(1, 1000000));
    }
}

void load_columns_data_from_file(vectorized::MutableColumns& columns,
                                 vectorized::DataTypeSerDeSPtrs serders, char col_spliter,
                                 std::set<int> idxes, const std::string& column_data_file) {
    ASSERT_EQ(serders.size(), columns.size());
    // Load column data and expected data from CSV files
    std::vector<std::vector<std::string>> res;
    struct stat buff;
    if (stat(column_data_file.c_str(), &buff) == 0) {
        if (S_ISREG(buff.st_mode)) {
            // file
            load_data_from_csv(serders, columns, column_data_file, col_spliter, idxes);
        } else if (S_ISDIR(buff.st_mode)) {
            // dir
            std::filesystem::path fs_path(column_data_file);
            for (const auto& entry : std::filesystem::directory_iterator(fs_path)) {
                std::string file_path = entry.path().string();
                std::cout << "load data from file: " << file_path << std::endl;
                load_data_from_csv(serders, columns, file_path, col_spliter, idxes);
            }
        }
    }
}

// Helper function to load data from CSV, with index which splited by spliter and load to columns
void load_data_from_csv(const vectorized::DataTypeSerDeSPtrs serders,
                        vectorized::MutableColumns& columns, const std::string& file_path,
                        const char spliter, const std::set<int> idxes) {
    ASSERT_EQ(serders.size(), columns.size())
            << "serder size: " << serders.size() << " column size: " << columns.size();
    ASSERT_EQ(serders.size(), idxes.size())
            << "serder size: " << serders.size() << " idxes size: " << idxes.size();
    ASSERT_EQ(serders.size(), *idxes.end())
            << "serder size: " << serders.size() << " idxes size: " << *idxes.end();
    std::ifstream file(file_path);
    if (!file) {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "can not open the file: {} ",
                               file_path);
    }

    std::string line;
    vectorized::DataTypeSerDe::FormatOptions options;
    while (std::getline(file, line)) {
        std::stringstream lineStream(line);
        std::string value;
        int l_idx = 0;
        int c_idx = 0;
        while (std::getline(lineStream, value, spliter)) {
            if (!value.starts_with("//") && idxes.contains(l_idx)) {
                Slice string_slice(value.data(), value.size());
                if (auto st = serders[c_idx]->deserialize_one_cell_from_json(*columns[c_idx],
                                                                             string_slice, options);
                    !st.ok()) {
                    LOG(INFO) << "error in deserialize but continue: " << st.to_string();
                }
                ++c_idx;
            }
            ++l_idx;
        }
    }
    std::cout << "loading data done, file: " << file_path << ", row count: " << columns[0]->size()
              << std::endl;
}
// res_columns[i] is the i-th column of the data
void check_or_generate_res_file(const std::string& res_file_path,
                                const std::vector<std::vector<std::string>>& res_columns) {
    if (res_columns.empty()) {
        return;
    }
    auto row_count = res_columns[0].size();
    if (FLAGS_gen_out) {
        std::ofstream res_file(res_file_path);
        LOG(INFO) << "gen check data with file: " << res_file_path;
        if (!res_file.is_open()) {
            throw std::ios_base::failure("Failed to open file: " + res_file_path);
        }

        for (size_t i = 1; i < res_columns.size(); ++i) {
            EXPECT_EQ(res_columns[i].size(), row_count);
        }
        auto column_count = res_columns.size();
        for (size_t i = 0; i < row_count; ++i) {
            // output one row
            for (size_t j = 0; j < column_count; ++j) {
                const auto& column = res_columns[j];
                // output one column
                res_file << column[i];
                if (j < column_count - 1) {
                    res_file << ";"; // Add semicolon between columns
                }
            }
            if (i < row_count - 1) {
                res_file << "\n";
            }
        }
        res_file.close();
    } else {
        // we read generate file to check result
        LOG(INFO) << "check data with file: " << res_file_path;
        std::ifstream file(res_file_path);
        if (!file) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "can not open the file: {} ",
                                   res_file_path);
        }

        size_t file_line_count = 0;
        std::string line;
        while (getline(file, line)) {
            file_line_count++;
        }
        EXPECT_EQ(file_line_count, row_count);

        file.clear();
        file.seekg(0, std::ios::beg);

        size_t line_idx = 0;
        while (std::getline(file, line)) {
            std::stringstream line_stream(line);
            std::string value;
            size_t col_idx = 0;
            while (std::getline(line_stream, value, ';')) {
                EXPECT_EQ(value, res_columns[col_idx][line_idx]);
                col_idx++;
            }
            line_idx++;
        }
    }
}

} // namespace doris
