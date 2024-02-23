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

#include "http/action/cloud_compaction_action.h"

#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"

namespace doris {
using namespace ErrorCode;

const static std::string HEADER_JSON = "application/json";

CloudCompactionAction::CloudCompactionAction(CompactionActionType ctype, ExecEnv* exec_env,
                                   CloudStorageEngine& engine, TPrivilegeHier::type hier,
                                   TPrivilegeType::type ptype)
        : HttpHandlerWithAuth(exec_env, hier, ptype), _engine(engine), _type(ctype) {}



void CloudCompactionAction::handle(HttpRequest* req) {
    Status st;
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, st.to_json());
}

} // end namespace doris
