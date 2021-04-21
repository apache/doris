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

#include <boost/algorithm/string.hpp>

namespace doris {
CsvScanner::CsvScanner(const std::vector<std::string>& csv_file_paths)
        : _is_open(false),
          _file_paths(csv_file_paths),
          _current_file(nullptr),
          _current_file_idx(0) {
    // do nothing
}

CsvScanner::~CsvScanner() {
    // close file
    if (_current_file != nullptr) {
        if (_current_file->is_open()) {
            _current_file->close();
        }
        delete _current_file;
        _current_file = nullptr;
    }
}

Status CsvScanner::open() {
    VLOG_CRITICAL << "CsvScanner::Connect";

    if (_is_open) {
        LOG(INFO) << "this scanner already opened";
        return Status::OK();
    }

    if (_file_paths.empty()) {
        return Status::InternalError("no file specified.");
    }

    _is_open = true;
    return Status::OK();
}

// TODO(lingbin): read more than one line at a time to reduce IO comsumption
Status CsvScanner::get_next_row(std::string* line_str, bool* eos) {
    if (_current_file == nullptr && _current_file_idx == _file_paths.size()) {
        *eos = true;
        return Status::OK();
    }

    if (_current_file == nullptr && _current_file_idx < _file_paths.size()) {
        std::string& file_path = _file_paths[_current_file_idx];
        LOG(INFO) << "open csv file: [" << _current_file_idx << "] " << file_path;

        _current_file = new std::ifstream(file_path, std::ifstream::in);
        if (!_current_file->is_open()) {
            return Status::InternalError("Fail to read csv file: " + file_path);
        }
        ++_current_file_idx;
    }

    getline(*_current_file, *line_str);
    if (_current_file->eof()) {
        _current_file->close();
        delete _current_file;
        _current_file = nullptr;

        if (_current_file_idx == _file_paths.size()) {
            *eos = true;
            return Status::OK();
        }
    }

    *eos = false;
    return Status::OK();
}
} // end namespace doris
