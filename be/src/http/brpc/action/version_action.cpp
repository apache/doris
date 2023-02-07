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

#include "version_action.h"

#include <brpc/http_method.h>

#include "gen_cpp/version.h"
#include "util/easy_json.h"

namespace doris {
VersionHandler::VersionHandler() : BaseHttpHandler("version") {}

void VersionHandler::handle_sync(brpc::Controller* cntl) {
    EasyJson be_version_info;
    be_version_info["msg"] = "success";
    be_version_info["code"] = 0;
    EasyJson data = be_version_info.Set("data", EasyJson::kObject);
    EasyJson version_info = data.Set("beVersionInfo", EasyJson::kObject);
    version_info["dorisBuildVersionPrefix"] = DORIS_BUILD_VERSION_PREFIX;
    version_info["dorisBuildVersionMajor"] = DORIS_BUILD_VERSION_MAJOR;
    version_info["dorisBuildVersionMinor"] = DORIS_BUILD_VERSION_MINOR;
    version_info["dorisBuildVersionPatch"] = DORIS_BUILD_VERSION_PATCH;
    version_info["dorisBuildVersionRcVersion"] = DORIS_BUILD_VERSION_RC_VERSION;
    version_info["dorisBuildVersion"] = DORIS_BUILD_VERSION;
    version_info["dorisBuildHash"] = DORIS_BUILD_HASH;
    version_info["dorisBuildShortHash"] = DORIS_BUILD_SHORT_HASH;
    version_info["dorisBuildTime"] = DORIS_BUILD_TIME;
    version_info["dorisBuildInfo"] = DORIS_BUILD_INFO;
    be_version_info["count"] = 0;

    on_succ_json(cntl, be_version_info.ToString());
}

bool VersionHandler::support_method(brpc::HttpMethod method) const {
    return method == brpc::HTTP_METHOD_GET;
}
} // namespace doris