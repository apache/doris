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

#include "http/action/dictionary_status_action.h"

#include <sstream>

#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "vec/functions/dictionary_factory.h"

namespace doris {

void DictionaryStatusAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "text/plain; version=0.0.4");

    std::vector<TDictionaryStatus> result;
    std::vector<int64_t> dict_ids {};
    ExecEnv::GetInstance()->dict_factory()->get_dictionary_status(result, dict_ids);
    std::stringstream ss;
    for (auto& status : result) {
        ss << "Dictionary ID: " << status.dictionary_id << ", Version ID: " << status.version_id
           << ", Memory Size: " << status.dictionary_memory_size << std::endl;
    }
    HttpChannel::send_reply(req, HttpStatus::OK, ss.str());
}
} // end namespace doris
