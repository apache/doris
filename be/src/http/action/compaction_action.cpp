// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/action/compaction_action.h"

#include <string>
#include <sstream>

#include <boost/algorithm/string.hpp>
#include "boost/lexical_cast.hpp"

#include "agent/cgroups_mgr.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/olap_engine.h"

namespace palo {

//example:
// http://host:port/api/compaction?tablet_id=10001&schema_hash=10001&compaction_type=base_compaction
// http://host:port/api/compaction?tablet_id=10001&schema_hash=10001&compaction_type=cumulative_compaction
const std::string TABLET_ID = "tablet_id";
const std::string SCHEMA_HASH = "schema_hash";
const std::string COMPACTION_TYPE = "compaction_type";

void CompactionAction::handle(HttpRequest *req) {
    LOG(INFO) << "accept one request " << req->debug_string();

    // add tid to cgroup in order to limit read bandwidth
    CgroupsMgr::apply_system_cgroup();
    // Get tablet id
    const std::string tablet_id_str = req->param(TABLET_ID);
    if (tablet_id_str.empty()) {
        std::string error_msg = std::string(
                "parameter " + TABLET_ID + " not specified in url.");

        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_msg);
        return;
    }

    // Get schema hash
    const std::string schema_hash_str = req->param(SCHEMA_HASH);
    if (schema_hash_str.empty()) {
        std::string error_msg = std::string(
                "parameter " + SCHEMA_HASH + " not specified in url.");
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_msg);
        return;
    }

    // valid str format
    int64_t tablet_id;
    int32_t schema_hash;
    try {
        tablet_id = boost::lexical_cast<int64_t>(tablet_id_str);
        schema_hash = boost::lexical_cast<int64_t>(schema_hash_str);
    } catch (boost::bad_lexical_cast& e) {
        std::string error_msg = std::string("param format is invalid: ") + std::string(e.what());
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_msg);
        return;
    }

    // Get compaction type
    const std::string& compaction_type = req->param(COMPACTION_TYPE);
    if (!boost::iequals(compaction_type, "base_compaction")
            && !boost::iequals(compaction_type, "cumulative_compaction")) {
        std::string error_msg = std::string(
                "parameter " + COMPACTION_TYPE + " not specified in url.");
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_msg);
        return;
    }

    TableInfo tablet_info(tablet_id, schema_hash);
    if (boost::iequals(compaction_type, "base_compaction")) {
        OLAPEngine::get_instance()->add_tablet_to_base_compaction_queue(tablet_info);
    } else {
        OLAPEngine::get_instance()->add_tablet_to_cumulative_compaction_queue(tablet_info);
    }
    HttpChannel::send_reply(req, "succeed add compaction to queue");
}

} // end namespace palo
