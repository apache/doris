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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/disk-io-mgr-internal.h
// and modified by Doris

#pragma once

#include <vector>
#include "vec/runtime/dict/dict.h"
#include "olap/options.h"

namespace doris {
namespace vectorized {

using DictDiff = phmap::flat_hash_map<int, phmap::flat_hash_set<std::string>>;

class DictMgr {
public:
    /**
     * @brief find the dict directory under store_paths, 
     * and load the dictionaries from it.
     * if not found, create dict directory under the first store_path, 
     * use it as dict directory.
     * 
     * @param store_paths 
     * @return 
     */
    void init(const std::vector<StorePath>& store_paths);

    GlobalDictSPtr get_dict_by_id(int dict_id);

    /**
     * @brief 
     * update BE dicts to new version. New version is pulled from FE
     * 
     */
    void update_dicts(/*TDicts dicts*/);

    /**
     * @brief BinaryDictPageBuilder invoke this func to merge dict diff
     * 
     * @param 
     * dict_id: 
     * diff : the new strings found by BinaryDictPageBuilder
     */
    void update_dict_diff(int dict_id, std::vector<std::string>& diff);

private:
    StorePath dict_path;
    //map dict_id to dict
    phmap::flat_hash_map<int, GlobalDictSPtr> _dict_map;

    //FE increases _global_version after some dictionaries updated.
    //if _global_version is different from that in query plan,
    //BE reports an dict_unmatch error to FE,
    //and then launches pull_dictionary task.
    size_t _global_version;

    DictDiff _dict_diff;
};

using DictMgrPtr = std::unique_ptr<DictMgr>;

} // namespace vectorized
} // namespace doris
