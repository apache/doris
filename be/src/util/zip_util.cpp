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

#include "util/zip_util.h"

#include <memory>

#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "util/file_utils.h"
#include "util/time.h"

namespace doris {

#define DEFAULT_FILE_NAME_SIZE 256
const static ZPOS64_T DEFAULT_UNZIP_BUFFER = 1048576; // 1MB

using namespace strings;

Status ZipFile::close() {
    if (_zip_file != nullptr) {
        if (_open_current_file) {
            unzCloseCurrentFile(_zip_file);
        }
        unzClose(_zip_file);
    }

    for (auto& p : _clean_paths) {
        RETURN_IF_ERROR(FileUtils::remove_all(p));
    }

    return Status::OK();
}

Status ZipFile::extract(const std::string& target_path, const std::string& dir_name) {
    // check zip file
    _zip_file = unzOpen64(_zip_path.c_str());
    if (_zip_file == nullptr) {
        return Status::InvalidArgument("open zip file: " + _zip_path + " error");
    }

    unz_global_info64 global_info;
    int err = unzGetGlobalInfo64(_zip_file, &global_info);

    if (err != UNZ_OK) {
        return Status::IOError(
                strings::Substitute("read zip file info $0 error, code: $1", _zip_path, err));
    }

    // 0.check target path
    std::string target = target_path + "/" + dir_name;
    if (FileUtils::check_exist(target)) {
        return Status::AlreadyExist("path already exists: " + target);
    }

    // 1.create temp directory
    std::string temp =
            target_path + "/.tmp_" + std::to_string(GetCurrentTimeMicros()) + "_" + dir_name;
    _clean_paths.push_back(temp);

    RETURN_IF_ERROR(FileUtils::create_dir(temp));

    // 2.unzip to temp directory
    for (int i = 0; i < global_info.number_entry; ++i) {
        RETURN_IF_ERROR(extract_file(temp));
        unzGoToNextFile(_zip_file);
    }

    // 3.move to target directory
    RETURN_IF_ERROR(Env::Default()->rename_file(temp, target));
    _clean_paths.clear();

    return Status::OK();
}

Status ZipFile::extract_file(const std::string& target_path) {
    char file_name[DEFAULT_FILE_NAME_SIZE];

    unz_file_info64 file_info_inzip;

    int err = unzGetCurrentFileInfo64(_zip_file, &file_info_inzip, file_name,
                                      DEFAULT_FILE_NAME_SIZE, nullptr, 0, nullptr, 0);

    if (err != UNZ_OK) {
        return Status::IOError(strings::Substitute("read zip file info error, code: $0", err));
    }

    // is directory, mkdir
    std::string path = target_path + "/" + std::string(file_name);

    if (HasSuffixString(file_name, "/") || HasSuffixString(file_name, "\\")) {
        return FileUtils::create_dir(path);
    }

    // is file, unzip
    err = unzOpenCurrentFile(_zip_file);
    _open_current_file = true;

    if (UNZ_OK != err) {
        return Status::IOError(
                strings::Substitute("read zip file $0 info error, code: $1", file_name, err));
    }

    ZPOS64_T file_size = std::min(file_info_inzip.uncompressed_size, DEFAULT_UNZIP_BUFFER);
    std::unique_ptr<char[]> file_data(new char[file_size]);

    std::unique_ptr<WritableFile> wfile;
    RETURN_IF_ERROR(Env::Default()->new_writable_file(path, &wfile));

    size_t size = 0;
    do {
        size = unzReadCurrentFile(_zip_file, (voidp)file_data.get(), file_size);
        if (size < 0) {
            return Status::IOError(strings::Substitute("unzip file $0 failed", file_name));
        }

        RETURN_IF_ERROR(wfile->append(Slice(file_data.get(), size)));
    } while (size > 0);

    RETURN_IF_ERROR(wfile->flush(WritableFile::FLUSH_ASYNC));
    RETURN_IF_ERROR(wfile->sync());

    unzCloseCurrentFile(_zip_file);
    _open_current_file = false;

    return Status::OK();
}

} // namespace doris
