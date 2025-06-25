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

#include "cloud/cloud_compaction_cv_mgr.h"

namespace doris {

TableConditionVairable* CloudCompactionCVMgr::get_table_cv(int64_t table_id) {
    TableConditionVairable* ret {nullptr};
    {
        std::lock_guard<std::mutex> lock(mtx);
        ret = &_table_cvs[table_id];
    }
    return ret;
}

void CloudCompactionCVMgr::wait_for(int64_t table_id, int64_t time_ms) {
    auto* table_entry = get_table_cv(table_id);
    std::unique_lock<std::mutex> lock(table_entry->mtx);
    uint32_t local_epoch = table_entry->epochs;
    table_entry->cv.wait_for(lock, std::chrono::milliseconds(time_ms),
                             [&] { return local_epoch != table_entry->epochs; });
}

void CloudCompactionCVMgr::notify_all(int64_t table_id) {
    auto* table_entry = get_table_cv(table_id);
    {
        std::lock_guard<std::mutex> lock(table_entry->mtx);
        ++table_entry->epochs;
        table_entry->cv.notify_all();
    }
}
} // namespace doris