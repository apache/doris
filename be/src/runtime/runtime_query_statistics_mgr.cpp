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

#include "exec/schema_scanner/schema_scanner_helper.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/debug_util.h"
#include "util/time.h"
#include "vec/core/block.h"

namespace doris {

void QueryStatisticsCtx::collect_query_statistics(TQueryStatistics* tq_s) {
    QueryStatistics tmp_qs;
    for (auto& qs_ptr : _qs_list) {
        tmp_qs.merge(*qs_ptr);
    }
    tmp_qs.to_thrift(tq_s);
    tq_s->__set_workload_group_id(_wg_id);
}

void RuntimeQueryStatisticsMgr::register_query_statistics(std::string query_id,
                                                          std::shared_ptr<QueryStatistics> qs_ptr,
                                                          TNetworkAddress fe_addr,
                                                          TQueryType::type query_type) {
    std::lock_guard<std::shared_mutex> write_lock(_qs_ctx_map_lock);
    if (_query_statistics_ctx_map.find(query_id) == _query_statistics_ctx_map.end()) {
        _query_statistics_ctx_map[query_id] =
                std::make_unique<QueryStatisticsCtx>(fe_addr, query_type);
    }
    _query_statistics_ctx_map.at(query_id)->_qs_list.push_back(qs_ptr);
}

void RuntimeQueryStatisticsMgr::report_runtime_query_statistics() {
    int64_t be_id = ExecEnv::GetInstance()->master_info()->backend_id;
    // 1 get query statistics map
    std::map<TNetworkAddress, std::map<std::string, TQueryStatistics>> fe_qs_map;
    std::map<std::string, std::pair<bool, bool>> qs_status; // <finished, timeout>
    {
        std::lock_guard<std::shared_mutex> write_lock(_qs_ctx_map_lock);
        int64_t current_time = MonotonicMillis();
        int64_t conf_qs_timeout = config::query_statistics_reserve_timeout_ms;
        for (auto& [query_id, qs_ctx_ptr] : _query_statistics_ctx_map) {
            if (qs_ctx_ptr->_query_type == TQueryType::EXTERNAL) {
                continue;
            }
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
            LOG(WARNING) << "[report_query_statistics]could not get client " << add_str
                         << " when report workload runtime stats, reason:"
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
            LOG(WARNING) << "[report_query_statistics]fe " << add_str
                         << " throw exception when report statistics, reason:" << e.what()
                         << " , you can see fe log for details.";
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG(WARNING) << "[report_query_statistics]report workload runtime statistics to "
                         << add_str << " failed,  reason: " << e.what();
            rpc_status = coord.reopen();
            if (!rpc_status.ok()) {
                LOG(WARNING) << "[report_query_statistics]reopen thrift client failed when report "
                                "workload runtime statistics to"
                             << add_str;
            } else {
                try {
                    coord->reportExecStatus(res, params);
                    rpc_result[addr] = true;
                } catch (apache::thrift::transport::TTransportException& e2) {
                    LOG(WARNING)
                            << "[report_query_statistics]retry report workload runtime stats to "
                            << add_str << " failed,  reason: " << e2.what();
                } catch (std::exception& e) {
                    LOG_WARNING(
                            "[report_query_statistics]unknow exception when report workload "
                            "runtime statistics to {}, "
                            "reason:{}. ",
                            add_str, e.what());
                }
            }
        } catch (std::exception& e) {
            LOG_WARNING(
                    "[report_query_statistics]unknown exception when report workload runtime "
                    "statistics to {}, reason:{}. ",
                    add_str, e.what());
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

void RuntimeQueryStatisticsMgr::set_query_finished(std::string query_id) {
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

std::shared_ptr<QueryStatistics> RuntimeQueryStatisticsMgr::get_runtime_query_statistics(
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

void RuntimeQueryStatisticsMgr::get_metric_map(
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
    metric_map.emplace(WorkloadMetricType::QUERY_MEMORY_BYTES,
                       std::to_string(ret_qs.get_current_used_memory_bytes()));
}

void RuntimeQueryStatisticsMgr::set_workload_group_id(std::string query_id, int64_t wg_id) {
    // wg id just need eventual consistency, read lock is ok
    std::shared_lock<std::shared_mutex> read_lock(_qs_ctx_map_lock);
    if (_query_statistics_ctx_map.find(query_id) != _query_statistics_ctx_map.end()) {
        _query_statistics_ctx_map.at(query_id)->_wg_id = wg_id;
    }
}

void RuntimeQueryStatisticsMgr::get_active_be_tasks_block(vectorized::Block* block) {
    std::shared_lock<std::shared_mutex> read_lock(_qs_ctx_map_lock);
    int64_t be_id = ExecEnv::GetInstance()->master_info()->backend_id;

    // block's schema come from SchemaBackendActiveTasksScanner::_s_tbls_columns
    // before 2.1.7, there are 12 columns in "backend_active_tasks" table.
    // after 2.1.8, 2 new columns added.
    // check this to make it compatible with version before 2.1.7
    bool need_local_and_remote_bytes = (block->columns() > 12);
    for (auto& [query_id, qs_ctx_ptr] : _query_statistics_ctx_map) {
        int col_idx = 0;
        TQueryStatistics tqs;
        qs_ctx_ptr->collect_query_statistics(&tqs);
        SchemaScannerHelper::insert_int64_value(col_idx++, be_id, block);
        SchemaScannerHelper::insert_string_value(col_idx++, qs_ctx_ptr->_fe_addr.hostname, block);
        SchemaScannerHelper::insert_string_value(col_idx++, query_id, block);

        int64_t task_time = qs_ctx_ptr->_is_query_finished
                                    ? qs_ctx_ptr->_query_finish_time - qs_ctx_ptr->_query_start_time
                                    : MonotonicMillis() - qs_ctx_ptr->_query_start_time;
        SchemaScannerHelper::insert_int64_value(col_idx++, task_time, block);
        SchemaScannerHelper::insert_int64_value(col_idx++, tqs.cpu_ms, block);
        SchemaScannerHelper::insert_int64_value(col_idx++, tqs.scan_rows, block);
        SchemaScannerHelper::insert_int64_value(col_idx++, tqs.scan_bytes, block);

        if (need_local_and_remote_bytes) {
            SchemaScannerHelper::insert_int64_value(col_idx++, tqs.scan_bytes_from_local_storage,
                                                    block);
            SchemaScannerHelper::insert_int64_value(col_idx++, tqs.scan_bytes_from_remote_storage,
                                                    block);
        }

        SchemaScannerHelper::insert_int64_value(col_idx++, tqs.max_peak_memory_bytes, block);
        SchemaScannerHelper::insert_int64_value(col_idx++, tqs.current_used_memory_bytes, block);
        SchemaScannerHelper::insert_int64_value(col_idx++, tqs.shuffle_send_bytes, block);
        SchemaScannerHelper::insert_int64_value(col_idx++, tqs.shuffle_send_rows, block);

        std::stringstream ss;
        ss << qs_ctx_ptr->_query_type;
        SchemaScannerHelper::insert_string_value(col_idx++, ss.str(), block);
    }
}

} // namespace doris
