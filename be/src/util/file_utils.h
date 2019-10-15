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

#ifndef DORIS_BE_UTIL_FILE_UTILS_H
#define DORIS_BE_UTIL_FILE_UTILS_H

#include <string>
#include <functional>

#include "common/status.h"

namespace doris {

class Env;

// Return true if file is '.' or '..'
inline bool is_dot_or_dotdot(const char* name) {
    return name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'));
}

class FileUtils {
public:

    // Create directory of dir_path, 
    // This function will create directory recursively,
    // if dir's parent directory doesn't exist
    //
    // RETURNS:
    //  Status::OK()      if create directory success or directory already exists
    static Status create_dir(const std::string& dir_path);

    // Delete file recursively.
    static Status remove_all(const std::string& dir_path);

    // List all files in the specified directory without '.' and '..'.
    // If you want retreive all files, you can use Env::iterate_dir.
    // All valid files will be stored in given *files.
    static Status list_files(
        Env* env,
        const std::string& dir,
        std::vector<std::string>* files);

    // Get the number of children belong to the specified directory, this
    // funciton also exclude '.' and '..'.
    // Return OK with *count is set to the count, if execute successful.
    static Status get_children_count(Env* env, const std::string& dir, int64_t* count);

    // If the file_path is not exist, or is not a dir, return false.
    static bool is_dir(const std::string& file_path);

    // Get file path from fd
    // Return 
    //  file path of this fd referenced
    //  "" if this fd is invalid
    static std::string path_of_fd(int fd);

    // split pathes in configue file to path
    // for example 
    // "/home/disk1/;/home/disk2"
    // will split to ['/home/disk1', '/home/disk2']
    static Status split_pathes(const char* path, std::vector<std::string>* path_vec);

    // copy the file from src path to dest path, it will overwrite the existing files
    static Status copy_file(const std::string& src_path, const std::string& dest_path);

    // calc md5sum of a local file
    static Status md5sum(const std::string& file, std::string* md5sum);

    static bool check_exist(const std::string& path);
};

}

#endif

