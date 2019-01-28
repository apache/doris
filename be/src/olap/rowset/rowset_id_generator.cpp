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
#include <string>
#include <fstream>

#include "olap/rowset/rowset_id_generator.h"

namespace doris {

RowsetIdGenerator* RowsetIdGenerator::_s_instance = nullptr;
std::mutex RowsetIdGenerator::_mlock;

RowsetIdGenerator* RowsetIdGenerator::instance() {
    if (_s_instance == nullptr) {
        std::lock_guard<std::mutex> lock(_mlock);
        if (_s_instance == nullptr) {
            _s_instance = new RowsetIdGenerator();
        }
    }
    return _s_instance;
}

OLAPStatus RowsetIdGenerator::get_next_id(DataDir* dir, RowsetId* gen_rowset_id) {
    WriteLock wrlock(&_ids_lock);
    // if could not find the dir in map, then load the start id from meta
    RowsetId batch_end_id = 10000;
    std::string key = END_ROWSET_ID;
    OLAPStatus s = OLAP_SUCCESS;
    if (_dir_ids.find(dir) == _dir_ids.end()) {
        // could not find dir in map, it means this dir is not loaded
        std::string value;
        OLAPStatus s = dir->get_meta()->get(DEFAULT_COLUMN_FAMILY_INDEX, key, value);
        if (s != OLAP_SUCCESS) {
            if (s == OLAP_ERR_META_KEY_NOT_FOUND) {
                s = dir->get_meta()->put(DEFAULT_COLUMN_FAMILY_INDEX, key, std::to_string(batch_end_id));
                if (s != OLAP_SUCCESS) {
                    return s;
                }
                _dir_ids[dir] = std::pair<RowsetId, RowsetId>(batch_end_id, batch_end_id);
            } else { 
                return s;
            }
        } else {
            batch_end_id = std::stol(value);
            _dir_ids[dir] = std::pair<RowsetId, RowsetId>(batch_end_id, batch_end_id);
        }
    }

    std::pair<RowsetId, RowsetId>& start_end_id = _dir_ids[dir];
    if (start_end_id.first + 1 >= start_end_id.second) {
        start_end_id.second = start_end_id.second + _batch_interval;
        s = dir->get_meta()->put(DEFAULT_COLUMN_FAMILY_INDEX, key, std::to_string(start_end_id.second));
        if (s != OLAP_SUCCESS) {
            return s;
        }
    } 

    start_end_id.first = start_end_id.first + 1;
    *gen_rowset_id = start_end_id.first;

    return OLAP_SUCCESS;
} // get_next_id

} // doris
