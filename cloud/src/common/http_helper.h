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

#include <brpc/controller.h>
#include <brpc/http_status_code.h>
#include <brpc/uri.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>
#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <google/protobuf/util/json_util.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <string_view>

#include "meta-service/meta_service_http.h"

namespace doris::cloud {

[[maybe_unused]] static std::string_view remove_version_prefix(std::string_view path) {
    if (path.size() > 3 && path.starts_with("v1/")) {
        path.remove_prefix(3);
    }
    return path;
}

const std::unordered_map<std::string_view, HttpHandlerInfo>& get_http_handlers();

// injection_point_http.cpp
[[maybe_unused]] HttpResponse process_injection_point(MetaServiceImpl* service,
                                                      brpc::Controller* ctrl);

// MetaService Http handlers
[[maybe_unused]] HttpResponse process_alter_cluster(MetaServiceImpl* service,
                                                    brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_get_obj_store_info(MetaServiceImpl* service,
                                                         brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_alter_obj_store_info(MetaServiceImpl* service,
                                                           brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_alter_storage_vault(MetaServiceImpl* service,
                                                          brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_update_ak_sk(MetaServiceImpl* service,
                                                   brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_create_instance(MetaServiceImpl* service,
                                                      brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_alter_instance(MetaServiceImpl* service,
                                                     brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_abort_txn(MetaServiceImpl* service, brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_abort_tablet_job(MetaServiceImpl* service,
                                                       brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_alter_ram_user(MetaServiceImpl* service,
                                                     brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_alter_iam(MetaServiceImpl* service, brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_adjust_rate_limit(MetaServiceImpl* service,
                                                        brpc::Controller* cntl);

[[maybe_unused]] HttpResponse process_query_rate_limit(MetaServiceImpl* service,
                                                       brpc::Controller* cntl);

[[maybe_unused]] HttpResponse process_decode_key(MetaServiceImpl*, brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_encode_key(MetaServiceImpl*, brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_get_value(MetaServiceImpl* service, brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_set_value(MetaServiceImpl* service, brpc::Controller* ctrl);

// show all key ranges and their count.
[[maybe_unused]] HttpResponse process_show_meta_ranges(MetaServiceImpl* service,
                                                       brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_get_instance_info(MetaServiceImpl* service,
                                                        brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_get_cluster(MetaServiceImpl* service, brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_get_tablet_stats(MetaServiceImpl* service,
                                                       brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_fix_tablet_stats(MetaServiceImpl* service,
                                                       brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_fix_tablet_db_id(MetaServiceImpl* service,
                                                       brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_get_stage(MetaServiceImpl* service, brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_get_cluster_status(MetaServiceImpl* service,
                                                         brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_txn_lazy_commit(MetaServiceImpl* service,
                                                      brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_list_snapshot(MetaServiceImpl* service,
                                                    brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_compact_snapshot(MetaServiceImpl* service,
                                                       brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_decouple_instance(MetaServiceImpl* service,
                                                        brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_set_snapshot_property(MetaServiceImpl* service,
                                                            brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_set_multi_version_status(MetaServiceImpl* service,
                                                               brpc::Controller* ctrl);

[[maybe_unused]] HttpResponse process_get_snapshot_property(MetaServiceImpl* service,
                                                            brpc::Controller* ctrl);

// Recycler HTTP handlers
[[maybe_unused]] HttpResponse process_recycle_instance(RecyclerServiceImpl* service,
                                                       brpc::Controller* cntl);

[[maybe_unused]] HttpResponse process_statistics_recycle(RecyclerServiceImpl* service,
                                                         brpc::Controller* cntl);

[[maybe_unused]] HttpResponse process_recycle_copy_jobs(RecyclerServiceImpl* service,
                                                        brpc::Controller* cntl);

[[maybe_unused]] HttpResponse process_recycle_job_info(RecyclerServiceImpl* service,
                                                       brpc::Controller* cntl);
[[maybe_unused]] HttpResponse process_check_instance(RecyclerServiceImpl* service,
                                                     brpc::Controller* cntl);

[[maybe_unused]] HttpResponse process_check_job_info(RecyclerServiceImpl* service,
                                                     brpc::Controller* cntl);

[[maybe_unused]] HttpResponse process_check_meta(RecyclerServiceImpl* service,
                                                 brpc::Controller* cntl);

[[maybe_unused]] HttpResponse process_adjust_rate_limiter(RecyclerServiceImpl*,
                                                          brpc::Controller* cntl);

// Both http handlers
[[maybe_unused]] HttpResponse process_show_config(MetaServiceImpl*, brpc::Controller* cntl);

[[maybe_unused]] HttpResponse process_update_config(MetaServiceImpl* service,
                                                    brpc::Controller* cntl);

} // namespace doris::cloud
