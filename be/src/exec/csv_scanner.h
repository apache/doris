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

#ifndef DORIS_BE_SRC_QUERY_EXEC_CSV_SCANNER_H
#define DORIS_BE_SRC_QUERY_EXEC_CSV_SCANNER_H

#include <fstream>
#include <string>
#include <vector>

#include "common/status.h"

namespace doris {

// Now, CsvScanNode and CsvScanner are only for unit test
class CsvScanner {
public:
    CsvScanner(const std::vector<std::string>& csv_file_paths);
    ~CsvScanner();

    Status open();
    Status get_next_row(std::string* line_str, bool* eos);

private:
    bool _is_open;
    std::vector<std::string> _file_paths;
    // the current opened file
    std::ifstream* _current_file;
    int32_t _current_file_idx;
};

} // end namespace doris
#endif // DORIS_BE_SRC_QUERY_EXEC_CSV_SCANNER_H
