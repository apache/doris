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
#include "util/minizip/unzip.h"
#include "util/file_utils.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "env/env.h"
#include "util/time.h"

namespace doris {

#define DEFAULT_FILE_NAME_SIZE 256

#define BREAK_IF_STATUS_ERROR(stmt) { \
        st = (stmt);                  \
        if (UNLIKELY(!st.ok())) {     \
            break;                    \
        }                             \
}

using namespace strings;
        
Status extract_file(unzFile zfile, const std::string& target_path) {
    int err;
    char file_name[DEFAULT_FILE_NAME_SIZE];

    unz_file_info64 file_info_inzip;

    err = unzGetCurrentFileInfo64(zfile, &file_info_inzip, file_name, DEFAULT_FILE_NAME_SIZE, 
            NULL, 0, NULL, 0);

    if (UNZ_OK != err) {
        return Status::IOError(strings::Substitute("read zip file info error, code: $0", err));
    }

    // is directory, mkdir
    std::string path = target_path + "/" + std::string(file_name);

    if (HasSuffixString(file_name, "/") || HasSuffixString(file_name, "\\")) {
        FileUtils::create_dir(path);

        return Status::OK();
    }

    // is file, unzip
    Status st = Status::OK();
    char* file_data = NULL;
    do {
        err = unzOpenCurrentFile(zfile);
        if (UNZ_OK != err) {
            st = Status::IOError(strings::Substitute("read zip file $0 info error, code: $1", file_name, err));
            break;
        }

        uint64_t file_size = file_info_inzip.uncompressed_size;
        file_data = (char *)malloc(file_size);

        if (UNLIKELY(file_data == NULL)) {
            LOG(WARNING) << "malloc failed, size: " << file_size;
            st = Status::MemoryAllocFailed(strings::Substitute("malloc failed, file $0 size $1", file_name,
                                                               file_size));
            break;
        }

        if (unzReadCurrentFile(zfile, (voidp) file_data, file_size) < 0) {
            st = Status::IOError(strings::Substitute("unzip file $0 failed", file_name));
            break;
        }

        std::unique_ptr<WritableFile> wfile;
        
        BREAK_IF_STATUS_ERROR(Env::Default()->new_writable_file(path, &wfile));
        BREAK_IF_STATUS_ERROR(wfile->append(file_data));
        BREAK_IF_STATUS_ERROR(wfile->flush(WritableFile::FLUSH_ASYNC));
        BREAK_IF_STATUS_ERROR(wfile->sync());
        BREAK_IF_STATUS_ERROR(wfile->close());
    } while (0);
    
    unzCloseCurrentFile(zfile);

    if (file_data != NULL) {
        free(file_data);
    }

    return st;
}


Status zip_extract(const std::string& zip_path, const std::string& target_path, const std::string& dir_name) {
    // 0.check target path
    std::string target = target_path + "/" + dir_name;
    if (FileUtils::check_exist(target)) {
        return Status::AlreadyExist("path already exists: " + target);
    }

    // 1.read .zip info
    unzFile zip_file = unzOpen64(zip_path.c_str());
    if (zip_file == NULL) {
        return Status::InvalidArgument("open zip file: " + zip_path + " error");
    }

    unz_global_info64 global_info;
    int err = unzGetGlobalInfo64(zip_file, &global_info);

    if (UNZ_OK != err) {
        return Status::IOError(strings::Substitute("read zip file info $0 error, code: $1", zip_path, err));
    }

    // 2.create temp directory
    std::string temp = target_path + "/.tmp_" + std::to_string(GetCurrentTimeMicros())  + "_" + dir_name;
    RETURN_IF_ERROR(FileUtils::create_dir(temp));

    // 3.unzip to temp directory
    for (int i = 0; i < global_info.number_entry; ++i) {
        Status s = extract_file(zip_file, temp);
        if (!s.ok()) {
            FileUtils::remove_all(temp);
            return s;
        }
        unzGoToNextFile(zip_file);
    }

    // 4.move to target directory
    Status s = Env::Default()->rename_file(temp, target);
    if (!s.ok()) {
        FileUtils::remove_all(temp);
        return s;
    }
    return Status::OK();
}

}