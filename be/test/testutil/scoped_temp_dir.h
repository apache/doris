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

#pragma once

#include <unistd.h>

#include <cerrno>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace doris::test {

class ScopedTempDirectory {
public:
    explicit ScopedTempDirectory(std::string_view prefix) {
        std::string path_template =
                (std::filesystem::temp_directory_path() / (std::string(prefix) + "_XXXXXX"))
                        .string();
        if (::mkdtemp(path_template.data()) == nullptr) {
            const int error = errno;
            throw std::filesystem::filesystem_error(
                    "cannot create temporary directory", path_template,
                    std::error_code(error, std::generic_category()));
        }
        _path = std::move(path_template);
    }

    ~ScopedTempDirectory() {
        std::error_code error;
        std::filesystem::remove_all(_path, error);
    }

    ScopedTempDirectory(const ScopedTempDirectory&) = delete;
    ScopedTempDirectory& operator=(const ScopedTempDirectory&) = delete;

    const std::filesystem::path& path() const { return _path; }

private:
    std::filesystem::path _path;
};

} // namespace doris::test
