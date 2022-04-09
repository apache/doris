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

#include "filesystem/local_file_system.h"

#include <fmt/format.h>

#include <filesystem>
#include <system_error>

#include "filesystem/read_stream.h"
#include "filesystem/write_stream.h"

namespace fs = std::filesystem;

namespace doris {

LocalFileSystem::LocalFileSystem(std::string path) : _path(std::move(path)) {}

LocalFileSystem::~LocalFileSystem() = default;

Status LocalFileSystem::exists(const std::string& path, bool* res) const {
    auto fs_path = fs::path(_path) / path;
    *res = fs::exists(fs_path);
    return Status::OK();
}

Status LocalFileSystem::is_file(const std::string& path, bool* res) const {
    auto fs_path = fs::path(_path) / path;
    *res = fs::is_regular_file(fs_path);
    return Status::OK();
}

Status LocalFileSystem::is_directory(const std::string& path, bool* res) const {
    auto fs_path = fs::path(_path) / path;
    *res = fs::is_directory(fs_path);
    return Status::OK();
}

Status LocalFileSystem::list(const std::string& path, std::vector<FileStat>* files) {
    files->clear();
    auto fs_path = fs::path(_path) / path;
    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(fs_path, ec)) {
        files->push_back({entry.path().filename(), entry.file_size()});
    }
    if (ec) {
        return Status::IOError(
                fmt::format("Cannot list {}: {}", fs_path.string(), std::strerror(ec.value())));
    }
    return Status::OK();
}

Status LocalFileSystem::delete_directory(const std::string& path) {
    auto fs_path = fs::path(_path) / path;
    if (!fs::exists(fs_path)) {
        return Status::OK();
    }
    if (!fs::is_directory(fs_path)) {
        return Status::IOError(fmt::format("{} is not a directory", fs_path.string()));
    }
    std::error_code ec;
    fs::remove_all(fs_path, ec);
    if (ec) {
        return Status::IOError(
                fmt::format("Cannot delete {}: {}", fs_path.string(), std::strerror(ec.value())));
    }
    return Status::OK();
}

Status LocalFileSystem::delete_file(const std::string& path) {
    auto fs_path = fs::path(_path) / path;
    if (!fs::exists(fs_path)) {
        return Status::OK();
    }
    if (!fs::is_regular_file(fs_path)) {
        return Status::IOError(fmt::format("{} is not a file", fs_path.string()));
    }
    std::error_code ec;
    fs::remove(fs_path, ec);
    if (ec) {
        return Status::IOError(
                fmt::format("Cannot delete {}: {}", fs_path.string(), std::strerror(ec.value())));
    }
    return Status::OK();
}

Status LocalFileSystem::create_directory(const std::string& path) {
    auto fs_path = fs::path(_path) / path;
    if (fs::exists(fs_path)) {
        return Status::IOError(fmt::format("{} exists", fs_path.string()));
    }
    std::error_code ec;
    fs::create_directories(fs_path, ec);
    if (ec) {
        return Status::IOError(
                fmt::format("Cannot create {}: {}", fs_path.string(), std::strerror(ec.value())));
    }
    return Status::OK();
}

Status LocalFileSystem::LocalFileSystem::read_file(const std::string& path, IOContext io_context,
                                                   std::unique_ptr<ReadStream>* stream) const {
    return Status::OK();
}

Status LocalFileSystem::write_file(const std::string& path, IOContext io_context,
                                   std::unique_ptr<WriteStream>* stream) {
    return Status::OK();
}

} // namespace doris
