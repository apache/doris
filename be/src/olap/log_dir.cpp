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

#include "olap/log_dir.h"

#include <string>

#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "olap/utils.h" // for check_dir_existed

namespace doris {
using namespace ErrorCode;

static const char* const kTestFilePath = ".testfile";

LogDir::LogDir(const std::string& path)
        : _path(path),
          _fs(io::LocalFileSystem::create(path)),
          _available_bytes(0),
          _capacity_bytes(0),
          _is_used(true) {}

LogDir::~LogDir() {}

Status LogDir::update_capacity() {
    RETURN_IF_ERROR(io::global_local_filesystem()->get_space_info(_path, &_capacity_bytes,
                                                                  &_available_bytes));
    LOG(INFO) << "path: " << _path << " total capacity: " << _capacity_bytes
              << ", available capacity: " << _available_bytes;

    return Status::OK();
}

void LogDir::health_check() {
    // check disk
    Status res = _read_and_write_test_file();
    if (!res) {
        LOG(WARNING) << "log read/write test file occur IO Error. path=" << _path
                      << ", err: " << res;
        _is_used = !res.is<IO_ERROR>();
    }
}

Status LogDir::_read_and_write_test_file() {
    auto test_file = fmt::format("{}/{}", _path, kTestFilePath);
    return read_write_test_file(test_file);
}

} // namespace doris
