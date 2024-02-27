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

#include "runtime/task_execution_context.h"

#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/transport/TTransportException.h>

#include <memory>

#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "service/backend_options.h"

namespace doris {
std::string to_http_path(const std::string& file_name) {
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::webserver_port
        << "/api/_download_load?"
        << "token=" << ExecEnv::GetInstance()->token() << "&file=" << file_name;
    return url.str();
};

Status TaskExecutionContext::trigger_profile_report(
        std::shared_ptr<doris::ReportStatusRequest> request) {
    // Create a shared pointer to capture and extend the lifetime of TaskExecutionContext
    auto shared_this = shared_from_this();

    auto report_profile_task = [shared_this, request]() {
        shared_this->do_report_profile(request);

        if (!request->done) {
            shared_this->refresh_next_report_time();
        }
    };

    return ExecEnv::GetInstance()->fragment_mgr()->submit_report_profile_task(report_profile_task);
};

void TaskExecutionContext::do_report_profile(std::shared_ptr<doris::ReportStatusRequest> req) {
    using apache::thrift::TException;
    using apache::thrift::transport::TTransportException;
    DCHECK(req->status.ok() || req->done); // if !status.ok() => done
    Status exec_status = req->update_fn(req->status);
    Status coord_status;
    FrontendServiceConnection coord(ExecEnv::GetInstance()->frontend_client_cache(),
                                    req->coord_addr, &coord_status);
    if (!coord_status.ok()) {
        std::stringstream ss;
        UniqueId uid(req->query_id.hi, req->query_id.lo);
        static_cast<void>(req->update_fn(Status::InternalError(
                "query_id: {}, couldn't get a client for {}, reason is {}", uid.to_string(),
                PrintThriftNetworkAddress(req->coord_addr), coord_status.to_string())));
        return;
    }

    TReportExecStatusParams params;
    params.protocol_version = FrontendServiceVersion::V1;
    params.__set_query_id(req->query_id);
    params.__set_backend_num(req->backend_num);
    params.__set_fragment_instance_id(req->fragment_instance_id);
    params.__set_fragment_id(req->fragment_id);
    params.__set_status(exec_status.to_thrift());
    params.__set_done(req->done);
    params.__set_query_type(req->runtime_state->query_type());
    params.__set_finished_scan_ranges(req->runtime_state->num_finished_range());

    DCHECK(req->runtime_state != nullptr);

    if (req->runtime_state->query_type() == TQueryType::LOAD && !req->done && req->status.ok()) {
        // this is a load plan, and load is not finished, just make a brief report
        params.__set_loaded_rows(req->runtime_state->num_rows_load_total());
        params.__set_loaded_bytes(req->runtime_state->num_bytes_load_total());
    } else {
        if (req->runtime_state->query_type() == TQueryType::LOAD) {
            params.__set_loaded_rows(req->runtime_state->num_rows_load_total());
            params.__set_loaded_bytes(req->runtime_state->num_bytes_load_total());
        }
        if (req->is_pipeline_x) {
            params.__isset.detailed_report = true;
            DCHECK(!req->runtime_states.empty());
            const bool enable_profile = (*req->runtime_states.begin())->enable_profile();
            if (enable_profile) {
                params.__isset.profile = true;
                params.__isset.loadChannelProfile = false;
                for (auto* rs : req->runtime_states) {
                    DCHECK(req->load_channel_profile);
                    TDetailedReportParams detailed_param;
                    rs->load_channel_profile()->to_thrift(&detailed_param.loadChannelProfile);
                    // merge all runtime_states.loadChannelProfile to req->load_channel_profile
                    req->load_channel_profile->update(detailed_param.loadChannelProfile);
                }
                req->load_channel_profile->to_thrift(&params.loadChannelProfile);
            } else {
                params.__isset.profile = false;
            }

            if (enable_profile) {
                for (auto& pipeline_profile : req->runtime_state->pipeline_id_to_profile()) {
                    TDetailedReportParams detailed_param;
                    detailed_param.__isset.fragment_instance_id = false;
                    detailed_param.__isset.profile = true;
                    detailed_param.__isset.loadChannelProfile = false;
                    pipeline_profile->to_thrift(&detailed_param.profile);
                    params.detailed_report.push_back(detailed_param);
                }
            }
        } else {
            if (req->profile != nullptr) {
                req->profile->to_thrift(&params.profile);
                if (req->load_channel_profile) {
                    req->load_channel_profile->to_thrift(&params.loadChannelProfile);
                }
                params.__isset.profile = true;
                params.__isset.loadChannelProfile = true;
            } else {
                params.__isset.profile = false;
            }
        }

        if (!req->runtime_state->output_files().empty()) {
            params.__isset.delta_urls = true;
            for (auto& it : req->runtime_state->output_files()) {
                params.delta_urls.push_back(to_http_path(it));
            }
        } else if (!req->runtime_states.empty()) {
            for (auto* rs : req->runtime_states) {
                for (auto& it : rs->output_files()) {
                    params.delta_urls.push_back(to_http_path(it));
                }
            }
            if (!params.delta_urls.empty()) {
                params.__isset.delta_urls = true;
            }
        }

        // load rows
        static std::string s_dpp_normal_all = "dpp.norm.ALL";
        static std::string s_dpp_abnormal_all = "dpp.abnorm.ALL";
        static std::string s_unselected_rows = "unselected.rows";
        int64_t num_rows_load_success = 0;
        int64_t num_rows_load_filtered = 0;
        int64_t num_rows_load_unselected = 0;
        if (req->runtime_state->num_rows_load_total() > 0 ||
            req->runtime_state->num_rows_load_filtered() > 0) {
            params.__isset.load_counters = true;

            num_rows_load_success = req->runtime_state->num_rows_load_success();
            num_rows_load_filtered = req->runtime_state->num_rows_load_filtered();
            num_rows_load_unselected = req->runtime_state->num_rows_load_unselected();
        } else if (!req->runtime_states.empty()) {
            for (auto* rs : req->runtime_states) {
                if (rs->num_rows_load_total() > 0 || rs->num_rows_load_filtered() > 0) {
                    params.__isset.load_counters = true;
                    num_rows_load_success += rs->num_rows_load_success();
                    num_rows_load_filtered += rs->num_rows_load_filtered();
                    num_rows_load_unselected += rs->num_rows_load_unselected();
                }
            }
        }
        params.load_counters.emplace(s_dpp_normal_all, std::to_string(num_rows_load_success));
        params.load_counters.emplace(s_dpp_abnormal_all, std::to_string(num_rows_load_filtered));
        params.load_counters.emplace(s_unselected_rows, std::to_string(num_rows_load_unselected));

        if (!req->runtime_state->get_error_log_file_path().empty()) {
            params.__set_tracking_url(
                    to_load_error_http_path(req->runtime_state->get_error_log_file_path()));
        } else if (!req->runtime_states.empty()) {
            for (auto* rs : req->runtime_states) {
                if (!rs->get_error_log_file_path().empty()) {
                    params.__set_tracking_url(
                            to_load_error_http_path(rs->get_error_log_file_path()));
                }
            }
        }
        if (!req->runtime_state->export_output_files().empty()) {
            params.__isset.export_files = true;
            params.export_files = req->runtime_state->export_output_files();
        } else if (!req->runtime_states.empty()) {
            for (auto* rs : req->runtime_states) {
                if (!rs->export_output_files().empty()) {
                    params.__isset.export_files = true;
                    params.export_files.insert(params.export_files.end(),
                                               rs->export_output_files().begin(),
                                               rs->export_output_files().end());
                }
            }
        }
        if (!req->runtime_state->tablet_commit_infos().empty()) {
            params.__isset.commitInfos = true;
            params.commitInfos.reserve(req->runtime_state->tablet_commit_infos().size());
            for (auto& info : req->runtime_state->tablet_commit_infos()) {
                params.commitInfos.push_back(info);
            }
        } else if (!req->runtime_states.empty()) {
            for (auto* rs : req->runtime_states) {
                if (!rs->tablet_commit_infos().empty()) {
                    params.__isset.commitInfos = true;
                    params.commitInfos.insert(params.commitInfos.end(),
                                              rs->tablet_commit_infos().begin(),
                                              rs->tablet_commit_infos().end());
                }
            }
        }
        if (!req->runtime_state->error_tablet_infos().empty()) {
            params.__isset.errorTabletInfos = true;
            params.errorTabletInfos.reserve(req->runtime_state->error_tablet_infos().size());
            for (auto& info : req->runtime_state->error_tablet_infos()) {
                params.errorTabletInfos.push_back(info);
            }
        } else if (!req->runtime_states.empty()) {
            for (auto* rs : req->runtime_states) {
                if (!rs->error_tablet_infos().empty()) {
                    params.__isset.errorTabletInfos = true;
                    params.errorTabletInfos.insert(params.errorTabletInfos.end(),
                                                   rs->error_tablet_infos().begin(),
                                                   rs->error_tablet_infos().end());
                }
            }
        }

        // Send new errors to coordinator
        req->runtime_state->get_unreported_errors(&(params.error_log));
        params.__isset.error_log = (params.error_log.size() > 0);
    }

    if (ExecEnv::GetInstance()->master_info()->__isset.backend_id) {
        params.__set_backend_id(ExecEnv::GetInstance()->master_info()->backend_id);
    }

    TReportExecStatusResult res;
    Status rpc_status;

    VLOG_DEBUG << "reportExecStatus params is "
               << apache::thrift::ThriftDebugString(params).c_str();
    if (!exec_status.ok()) {
        LOG(WARNING) << "report error status: " << exec_status.msg()
                     << " to coordinator: " << req->coord_addr
                     << ", query id: " << print_id(req->query_id)
                     << ", instance id: " << print_id(req->fragment_instance_id);
    }
    try {
        try {
            coord->reportExecStatus(res, params);
        } catch (TTransportException& e) {
            LOG(WARNING) << "Retrying ReportExecStatus. query id: " << print_id(req->query_id)
                         << ", instance id: " << print_id(req->fragment_instance_id) << " to "
                         << req->coord_addr << ", err: " << e.what();
            rpc_status = coord.reopen();

            if (!rpc_status.ok()) {
                // we need to cancel the execution of this fragment
                static_cast<void>(req->update_fn(rpc_status));
                req->cancel_fn(PPlanFragmentCancelReason::INTERNAL_ERROR, "report rpc fail");
                return;
            }
            coord->reportExecStatus(res, params);
        }

        rpc_status = Status::create<false>(res.status);
    } catch (TException& e) {
        rpc_status = Status::InternalError("ReportExecStatus() to {} failed: {}",
                                           PrintThriftNetworkAddress(req->coord_addr), e.what());
    }

    if (!rpc_status.ok()) {
        LOG_INFO("Going to cancel instance {} since report exec status got rpc failed: {}",
                 print_id(req->fragment_instance_id), rpc_status.to_string());
        // we need to cancel the execution of this fragment
        static_cast<void>(req->update_fn(rpc_status));
        req->cancel_fn(PPlanFragmentCancelReason::INTERNAL_ERROR, std::string(rpc_status.msg()));
    }
};

}; // namespace doris