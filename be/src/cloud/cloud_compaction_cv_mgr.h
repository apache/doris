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

#include <condition_variable>
#include <map>
#include <mutex>

namespace doris {

struct TableConditionVairable {
    std::mutex mtx;
    std::condition_variable cv;
    uint32_t epochs {0};
};

class CloudCompactionCVMgr {
public:
    CloudCompactionCVMgr() = default;
    ~CloudCompactionCVMgr() = default;
    CloudCompactionCVMgr(const CloudCompactionCVMgr&) = delete;
    CloudCompactionCVMgr& operator=(const CloudCompactionCVMgr&) = delete;

    void wait_for(int64_t table_id, int64_t time_ms);
    void notify_all(int64_t table_id);

private:
    TableConditionVairable* get_table_cv(int64_t table_id);

    std::mutex mtx;
    std::map<int64_t /* table_id */, TableConditionVairable> _table_cvs;
};
} // namespace doris