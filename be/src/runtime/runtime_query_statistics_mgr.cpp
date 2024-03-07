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

#include "runtime/runtime_query_statistics_mgr.h"

#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/debug_util.h"
#include "util/time.h"

namespace doris {

void QueryStatisticsCtx::collect_query_statistics(TQueryStatistics* tq_s) {
    QueryStatistics tmp_qs;
    for (auto& qs_ptr : _qs_list) {
        tmp_qs.merge(*qs_ptr);
    }
    tmp_qs.to_thrift(tq_s);
    tq_s->__set_workload_group_id(_wg_id);
}

void RuntimeQueryStatiticsMgr::register_query_statistics(std::string query_id,
                                                         std::shared_ptr<QueryStatistics> qs_ptr,
                                                         TNetworkAddress fe_addr) {
    std::lock_guard<std::shared_mutex> write_lock(_qs_ctx_map_lock);
    if (_query_statistics_ctx_map.find(query_id) == _query_statistics_ctx_map.end()) {
        _query_statistics_ctx_map[query_id] = std::make_unique<QueryStatisticsCtx>(fe_addr);
    }
    _query_statistics_ctx_map.at(query_id)->_qs_list.push_back(qs_ptr);
}

void RuntimeQueryStatiticsMgr::report_runtime_query_statistics() {
    int64_t be_id = ExecEnv::GetInstance()->master_info()->backend_id;
    // 1 get query statistics map
    std::map<TNetworkAddress, std::map<std::string, TQueryStatistics>> fe_qs_map;
    std::map<std::string, std::pair<bool, bool>> qs_status; // <finished, timeout>
    {
        std::lock_guard<std::shared_mutex> write_lock(_qs_ctx_map_lock);
        int64_t current_time = MonotonicMillis();
        int64_t conf_qs_timeout = config::query_statistics_reserve_timeout_ms;
        for (auto& [query_id, qs_ctx_ptr] : _query_statistics_ctx_map) {
            if (fe_qs_map.find(qs_ctx_ptr->_fe_addr) == fe_qs_map.end()) {
                std::map<std::string, TQueryStatistics> tmp_map;
                fe_qs_map[qs_ctx_ptr->_fe_addr] = std::move(tmp_map);
            }

            TQueryStatistics ret_t_qs;
            qs_ctx_ptr->collect_query_statistics(&ret_t_qs);
            fe_qs_map.at(qs_ctx_ptr->_fe_addr)[query_id] = ret_t_qs;

            bool is_query_finished = qs_ctx_ptr->_is_query_finished;
            bool is_timeout_after_finish = false;
            if (is_query_finished) {
                is_timeout_after_finish =
                        (current_time - qs_ctx_ptr->_query_finish_time) > conf_qs_timeout;
            }
            qs_status[query_id] = std::make_pair(is_query_finished, is_timeout_after_finish);
        }
    }

    // 2 report query statistics to fe
    std::map<TNetworkAddress, bool> rpc_result;
    for (auto& [addr, qs_map] : fe_qs_map) {
        rpc_result[addr] = false;
        // 2.1 get client
        Status coord_status;
        FrontendServiceConnection coord(ExecEnv::GetInstance()->frontend_client_cache(), addr,
                                        &coord_status);
        std::string add_str = PrintThriftNetworkAddress(addr);
        if (!coord_status.ok()) {
            std::stringstream ss;
            LOG(WARNING) << "could not get client " << add_str
                         << " when report workload runtime stats, reason is "
                         << coord_status.to_string();
            continue;
        }

        // 2.2 send report
        TReportWorkloadRuntimeStatusParams report_runtime_params;
        report_runtime_params.__set_backend_id(be_id);
        report_runtime_params.__set_query_statistics_map(qs_map);

        TReportExecStatusParams params;
        params.__set_report_workload_runtime_status(report_runtime_params);

        TReportExecStatusResult res;
        Status rpc_status;
        try {
            coord->reportExecStatus(res, params);
            rpc_result[addr] = true;
        } catch (apache::thrift::TApplicationException& e) {
            LOG(WARNING) << "fe " << add_str
                         << " throw exception when report statistics, reason=" << e.what()
                         << " , you can see fe log for details.";
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG(WARNING) << "report workload runtime statistics to " << add_str
                         << " failed,  err: " << e.what();
            rpc_status = coord.reopen();
            if (!rpc_status.ok()) {
                LOG(WARNING)
                        << "reopen thrift client failed when report workload runtime statistics to"
                        << add_str;
            } else {
                try {
                    coord->reportExecStatus(res, params);
                    rpc_result[addr] = true;
                } catch (apache::thrift::transport::TTransportException& e2) {
                    LOG(WARNING) << "retry report workload runtime stats to " << add_str
                                 << " failed,  err: " << e2.what();
                }
            }
        }
    }

    //  3 when query is finished and (last rpc is send success), remove finished query statistics
    if (fe_qs_map.size() == 0) {
        return;
    }

    {
        std::lock_guard<std::shared_mutex> write_lock(_qs_ctx_map_lock);
        for (auto& [addr, qs_map] : fe_qs_map) {
            bool is_rpc_success = rpc_result[addr];
            for (auto& [query_id, qs] : qs_map) {
                auto& qs_status_pair = qs_status[query_id];
                bool is_query_finished = qs_status_pair.first;
                bool is_timeout_after_finish = qs_status_pair.second;
                if ((is_rpc_success && is_query_finished) || is_timeout_after_finish) {
                    _query_statistics_ctx_map.erase(query_id);
                }
            }
        }
    }
}

void RuntimeQueryStatiticsMgr::set_query_finished(std::string query_id) {
    // NOTE: here must be a write lock
    std::lock_guard<std::shared_mutex> write_lock(_qs_ctx_map_lock);
    // when a query get query_ctx succ, but failed before create node/operator,
    // it may not register query statistics, so it can not be mark finish
    if (_query_statistics_ctx_map.find(query_id) != _query_statistics_ctx_map.end()) {
        auto* qs_ptr = _query_statistics_ctx_map.at(query_id).get();
        qs_ptr->_is_query_finished = true;
        qs_ptr->_query_finish_time = MonotonicMillis();
    }
}

std::shared_ptr<QueryStatistics> RuntimeQueryStatiticsMgr::get_runtime_query_statistics(
        std::string query_id) {
    std::shared_lock<std::shared_mutex> read_lock(_qs_ctx_map_lock);
    if (_query_statistics_ctx_map.find(query_id) == _query_statistics_ctx_map.end()) {
        return nullptr;
    }
    std::shared_ptr<QueryStatistics> qs_ptr = std::make_shared<QueryStatistics>();
    for (auto const& qs : _query_statistics_ctx_map[query_id]->_qs_list) {
        qs_ptr->merge(*qs);
    }
    return qs_ptr;
}

void RuntimeQueryStatiticsMgr::get_metric_map(
        std::string query_id, std::map<WorkloadMetricType, std::string>& metric_map) {
    QueryStatistics ret_qs;
    int64_t query_time_ms = 0;
    {
        std::shared_lock<std::shared_mutex> read_lock(_qs_ctx_map_lock);
        if (_query_statistics_ctx_map.find(query_id) != _query_statistics_ctx_map.end()) {
            for (auto const& qs : _query_statistics_ctx_map[query_id]->_qs_list) {
                ret_qs.merge(*qs);
            }
            query_time_ms =
                    MonotonicMillis() - _query_statistics_ctx_map.at(query_id)->_query_start_time;
        }
    }
    metric_map.emplace(WorkloadMetricType::QUERY_TIME, std::to_string(query_time_ms));
    metric_map.emplace(WorkloadMetricType::SCAN_ROWS, std::to_string(ret_qs.get_scan_rows()));
    metric_map.emplace(WorkloadMetricType::SCAN_BYTES, std::to_string(ret_qs.get_scan_bytes()));
}

void RuntimeQueryStatiticsMgr::set_workload_group_id(std::string query_id, int64_t wg_id) {
    // wg id just need eventual consistency, read lock is ok
    std::shared_lock<std::shared_mutex> read_lock(_qs_ctx_map_lock);
    if (_query_statistics_ctx_map.find(query_id) != _query_statistics_ctx_map.end()) {
        _query_statistics_ctx_map.at(query_id)->_wg_id = wg_id;
    }
}

std::vector<TRow> RuntimeQueryStatiticsMgr::get_active_be_tasks_statistics(
        std::vector<std::string> filter_columns) {
    std::shared_lock<std::shared_mutex> read_lock(_qs_ctx_map_lock);
    std::vector<TRow> table_rows;
    int64_t be_id = ExecEnv::GetInstance()->master_info()->backend_id;

    for (auto& [query_id, qs_ctx_ptr] : _query_statistics_ctx_map) {
        TRow trow;

        TQueryStatistics tqs;
        qs_ctx_ptr->collect_query_statistics(&tqs);

        for (auto iter = filter_columns.begin(); iter != filter_columns.end(); iter++) {
            std::string col_name = *iter;

            TCell tcell;
            if (col_name == "beid") {
                tcell.longVal = be_id;
            } else if (col_name == "fehost") {
                tcell.stringVal = qs_ctx_ptr->_fe_addr.hostname;
            } else if (col_name == "queryid") {
                tcell.stringVal = query_id;
            } else if (col_name == "tasktimems") {
                if (qs_ctx_ptr->_is_query_finished) {
                    tcell.longVal = qs_ctx_ptr->_query_finish_time - qs_ctx_ptr->_query_start_time;
                } else {
                    tcell.longVal = MonotonicMillis() - qs_ctx_ptr->_query_start_time;
                }
            } else if (col_name == "taskcputimems") {
                tcell.longVal = tqs.cpu_ms;
            } else if (col_name == "scanrows") {
                tcell.longVal = tqs.scan_rows;
            } else if (col_name == "scanbytes") {
                tcell.longVal = tqs.scan_bytes;
            } else if (col_name == "bepeakmemorybytes") {
                tcell.longVal = tqs.max_peak_memory_bytes;
            } else if (col_name == "currentusedmemorybytes") {
                tcell.longVal = tqs.current_used_memory_bytes;
            } else if (col_name == "shufflesendbytes") {
                tcell.longVal = tqs.shuffle_send_bytes;
            } else if (col_name == "shufflesendRows") {
                tcell.longVal = tqs.shuffle_send_rows;
            }
            trow.column_value.push_back(tcell);
        }
        table_rows.push_back(trow);
    }
    return table_rows;
}

} // namespace doris