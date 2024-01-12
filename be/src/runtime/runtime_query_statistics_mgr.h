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

#include <shared_mutex>
#include <string>

#include "runtime/query_statistics.h"

namespace doris {

class QueryStatisticsCtx {
public:
    QueryStatisticsCtx(TNetworkAddress fe_addr) : fe_addr(fe_addr) {
        this->is_query_finished = false;
    }
    ~QueryStatisticsCtx() = default;

    std::vector<std::shared_ptr<QueryStatistics>> qs_list;
    bool is_query_finished;
    TNetworkAddress fe_addr;
    int64_t query_finish_time;
};

class RuntimeQueryStatiticsMgr {
public:
    RuntimeQueryStatiticsMgr() = default;
    ~RuntimeQueryStatiticsMgr() = default;

    void register_query_statistics(std::string query_id, std::shared_ptr<QueryStatistics> qs_ptr,
                                   TNetworkAddress fe_addr);

    void report_runtime_query_statistics();

    void set_query_finished(std::string query_id);

    std::shared_ptr<QueryStatistics> get_runtime_query_statistics(std::string query_id);

private:
    std::shared_mutex _qs_ctx_map_lock;
    std::map<std::string, std::unique_ptr<QueryStatisticsCtx>> _query_statistics_ctx_map;
};

} // namespace doris