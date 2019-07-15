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
#include "olap/olap_meta.h"

namespace doris {

static RowsetId k_batch_interval = 10000;

OLAPStatus RowsetIdGenerator::init() {
    _next_id = k_batch_interval;
    _id_batch_end = (k_batch_interval << 1);
    // get last stored value from meta
    std::string value;
    OLAPStatus s = _meta->get(DEFAULT_COLUMN_FAMILY_INDEX, END_ROWSET_ID, &value);
    if (s == OLAP_SUCCESS) {
        _next_id = std::stol(value);
        _id_batch_end = _next_id + k_batch_interval;
    } else if (s != OLAP_ERR_META_KEY_NOT_FOUND) {
        return s;
    }
    // else: meta-key not found, we will initialize a initial state
    s = _meta->put(DEFAULT_COLUMN_FAMILY_INDEX, END_ROWSET_ID, std::to_string(_id_batch_end));
    if (s != OLAP_SUCCESS) {
        return s;
    }
    return OLAP_SUCCESS;
}

OLAPStatus RowsetIdGenerator::get_next_id(RowsetId* gen_rowset_id) {
    std::lock_guard<std::mutex> l(_lock);
    if (_next_id >= _id_batch_end) {
        _id_batch_end += k_batch_interval;
        auto s = _meta->put(DEFAULT_COLUMN_FAMILY_INDEX, END_ROWSET_ID, std::to_string(_id_batch_end));
        if (s != OLAP_SUCCESS) {
            return s;
        }
    }
    *gen_rowset_id = _next_id;
    ++_next_id;
    return OLAP_SUCCESS;
}

OLAPStatus RowsetIdGenerator::set_next_id(RowsetId new_rowset_id) {
    std::lock_guard<std::mutex> l(_lock);
    // must be < not <=
    if (new_rowset_id < _next_id) {
        return OLAP_SUCCESS;
    }
    if (new_rowset_id >= _id_batch_end) {
        _id_batch_end = new_rowset_id + k_batch_interval;
        auto s = _meta->put(DEFAULT_COLUMN_FAMILY_INDEX, END_ROWSET_ID, std::to_string(_id_batch_end));
        RETURN_NOT_OK(s);
    }
    _next_id = new_rowset_id + 1;
    return OLAP_SUCCESS;
}

} // doris
