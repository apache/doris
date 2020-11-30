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

#ifndef DORIS_BE_SRC_RUNTIME_SMALL_FILE_MGR_H
#define DORIS_BE_SRC_RUNTIME_SMALL_FILE_MGR_H

#include <stdint.h>

#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "runtime/client_cache.h"

namespace doris {

class ExecEnv;

struct CacheEntry {
    std::string path; // absolute path
    std::string md5;
};

/*
 * SmallFileMgr is used to download small files saved in FE,
 * such as certification files, public/private keys
 */
class SmallFileMgr {
public:
    SmallFileMgr(ExecEnv* env, const std::string& local_path);

    ~SmallFileMgr();

    // call init() when BE start up. load all local files
    Status init();

    // get file by specified file_id, return 'file_path'
    // if file does not exist, it will be downloaded from FE
    Status get_file(int64_t file_id, const std::string& md5, std::string* file_path);

private:
    Status _load_local_files();

    // load one single local file
    Status _load_single_file(const std::string& path, const std::string& file_name);

    Status _check_file(const CacheEntry& entry, const std::string& md5);

    Status _download_file(int64_t file_id, const std::string& md5, std::string* file_path);

private:
    std::mutex _lock;
    ExecEnv* _exec_env;
    std::string _local_path;
    // file id -> small file
    std::unordered_map<int64_t, CacheEntry> _file_cache;
};

} // end namespace doris

#endif // DORIS_BE_SRC_RUNTIME_SMALL_FILE_MGR_H
