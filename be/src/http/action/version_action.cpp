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

#include "http/action/version_action.h"

#include <string>

#include "common/version_internal.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "util/easy_json.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

VersionAction::VersionAction(ExecEnv* exec_env, TPrivilegeHier::type hier,
                             TPrivilegeType::type type)
        : HttpHandlerWithAuth(exec_env, hier, type) {}

void VersionAction::handle(HttpRequest* req) {
    EasyJson be_version_info;
    be_version_info["msg"] = "success";
    be_version_info["code"] = 0;
    EasyJson data = be_version_info.Set("data", EasyJson::kObject);
    EasyJson version_info = data.Set("beVersionInfo", EasyJson::kObject);
    version_info["dorisBuildVersionPrefix"] = version::doris_build_version_prefix();
    version_info["dorisBuildVersionMajor"] = version::doris_build_version_major();
    version_info["dorisBuildVersionMinor"] = version::doris_build_version_minor();
    version_info["dorisBuildVersionPatch"] = version::doris_build_version_patch();
    version_info["dorisBuildVersionRcVersion"] = version::doris_build_version_rc_version();
    version_info["dorisBuildVersion"] = version::doris_build_version();
    version_info["dorisBuildHash"] = version::doris_build_hash();
    version_info["dorisBuildShortHash"] = version::doris_build_short_hash();
    version_info["dorisBuildTime"] = version::doris_build_time();
    version_info["dorisBuildInfo"] = version::doris_build_info();
    be_version_info["count"] = 0;

    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, be_version_info.ToString());
}

} // end namespace doris
