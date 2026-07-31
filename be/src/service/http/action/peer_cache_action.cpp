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

#include "service/http/action/peer_cache_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <string>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_warm_up_manager.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"

namespace doris {

static constexpr std::string_view HEADER_JSON = "application/json";

// Serialize a single TabletPeerCandidates into a rapidjson object
static void tablet_peer_to_json(int64_t tablet_id, const TabletPeerCandidates& tpc,
                                rapidjson::Writer<rapidjson::StringBuffer>& writer) {
    writer.StartObject();
    writer.Key("tablet_id");
    writer.Int64(tablet_id);

    writer.Key("candidates");
    writer.StartArray();
    for (const auto& c : tpc.candidates) {
        writer.StartObject();
        writer.Key("host");
        writer.String(c.host.c_str());
        writer.Key("brpc_port");
        writer.Int(c.brpc_port);
        writer.Key("compute_group_id");
        writer.String(c.compute_group_id.c_str());
        writer.Key("last_access_time_ms");
        writer.Int64(c.last_access_time_ms);
        writer.Key("consecutive_rpc_failures");
        writer.Int(c.consecutive_rpc_failures);
        writer.EndObject();
    }
    writer.EndArray();

    writer.Key("last_successful_compute_group_id");
    writer.String(tpc.last_successful_compute_group_id.c_str());
    writer.Key("fetching_from_fe");
    writer.Bool(tpc.fetching_from_fe);
    writer.Key("consecutive_all_miss");
    writer.Int(tpc.consecutive_all_miss);
    writer.Key("cooldown_until_ms");
    writer.Int64(tpc.cooldown_until_ms);

    writer.EndObject();
}

static int64_t parse_tablet_id(HttpRequest* req) {
    const std::string& tablet_id_str = req->param("tablet_id");
    if (tablet_id_str.empty()) {
        return -1;
    }
    try {
        return std::stoll(tablet_id_str);
    } catch (...) {
        return -1;
    }
}

Status PeerCacheAction::_handle_show(HttpRequest* req, std::string* result) {
    int64_t tablet_id = parse_tablet_id(req);
    if (tablet_id < 0) {
        return Status::InvalidArgument("missing or invalid parameter: tablet_id");
    }

    auto& mgr = _engine.cloud_warm_up_manager();
    auto opt = mgr.get_tablet_peer_info(tablet_id);
    if (!opt.has_value()) {
        return Status::NotFound("tablet_id {} has no peer candidates", tablet_id);
    }

    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    tablet_peer_to_json(tablet_id, opt.value(), writer);
    *result = buf.GetString();
    return Status::OK();
}

Status PeerCacheAction::_handle_show_all(HttpRequest* req, std::string* result) {
    int64_t limit = 1000; // default
    const std::string& limit_str = req->param("limit");
    if (!limit_str.empty()) {
        try {
            limit = std::stoll(limit_str);
        } catch (...) {
            return Status::InvalidArgument("invalid parameter: limit");
        }
    }

    auto& mgr = _engine.cloud_warm_up_manager();
    auto all = mgr.get_all_peer_info(limit);

    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    writer.StartObject();
    writer.Key("total");
    writer.Int64(static_cast<int64_t>(all.size()));
    writer.Key("tablets");
    writer.StartArray();
    for (const auto& [tid, tpc] : all) {
        tablet_peer_to_json(tid, tpc, writer);
    }
    writer.EndArray();
    writer.EndObject();
    *result = buf.GetString();
    return Status::OK();
}

Status PeerCacheAction::_handle_set(HttpRequest* req, std::string* result) {
    int64_t tablet_id = parse_tablet_id(req);
    if (tablet_id < 0) {
        return Status::InvalidArgument("missing or invalid parameter: tablet_id");
    }

    const std::string& body = req->get_request_body();
    if (body.empty()) {
        return Status::InvalidArgument("missing request body (JSON expected)");
    }

    rapidjson::Document doc;
    doc.Parse(body.c_str(), body.size());
    if (doc.HasParseError()) {
        return Status::InvalidArgument("invalid JSON body: parse error at offset {}",
                                       doc.GetErrorOffset());
    }

    TabletPeerCandidates tpc;

    // Parse candidates array
    if (doc.HasMember("candidates") && doc["candidates"].IsArray()) {
        for (const auto& item : doc["candidates"].GetArray()) {
            PeerCandidate c;
            if (item.HasMember("host") && item["host"].IsString()) {
                c.host = item["host"].GetString();
            }
            if (item.HasMember("brpc_port") && item["brpc_port"].IsInt()) {
                c.brpc_port = item["brpc_port"].GetInt();
            }
            if (item.HasMember("compute_group_id") && item["compute_group_id"].IsString()) {
                c.compute_group_id = item["compute_group_id"].GetString();
            }
            if (item.HasMember("last_access_time_ms") && item["last_access_time_ms"].IsInt64()) {
                c.last_access_time_ms = item["last_access_time_ms"].GetInt64();
            }
            if (item.HasMember("consecutive_rpc_failures") &&
                item["consecutive_rpc_failures"].IsInt()) {
                c.consecutive_rpc_failures = item["consecutive_rpc_failures"].GetInt();
            }
            // If last_access_time_ms not provided, set to current time
            if (c.last_access_time_ms == 0) {
                c.last_access_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                                std::chrono::system_clock::now().time_since_epoch())
                                                .count();
            }
            tpc.candidates.push_back(std::move(c));
        }
    }

    if (doc.HasMember("last_successful_compute_group_id") &&
        doc["last_successful_compute_group_id"].IsString()) {
        tpc.last_successful_compute_group_id = doc["last_successful_compute_group_id"].GetString();
    }
    if (doc.HasMember("consecutive_all_miss") && doc["consecutive_all_miss"].IsInt()) {
        tpc.consecutive_all_miss = doc["consecutive_all_miss"].GetInt();
    }
    if (doc.HasMember("cooldown_until_ms") && doc["cooldown_until_ms"].IsInt64()) {
        tpc.cooldown_until_ms = doc["cooldown_until_ms"].GetInt64();
    }

    auto& mgr = _engine.cloud_warm_up_manager();
    mgr.set_tablet_peer_candidates(tablet_id, std::move(tpc));

    *result = R"({"status":"OK","msg":"peer candidates set successfully"})";
    return Status::OK();
}

Status PeerCacheAction::_handle_remove(HttpRequest* req, std::string* result) {
    int64_t tablet_id = parse_tablet_id(req);
    if (tablet_id < 0) {
        return Status::InvalidArgument("missing or invalid parameter: tablet_id");
    }

    auto& mgr = _engine.cloud_warm_up_manager();
    mgr.remove_balanced_tablet(tablet_id);

    *result = R"({"status":"OK","msg":"peer candidates removed"})";
    return Status::OK();
}

Status PeerCacheAction::_handle_reset_cooldown(HttpRequest* req, std::string* result) {
    int64_t tablet_id = parse_tablet_id(req);
    if (tablet_id < 0) {
        return Status::InvalidArgument("missing or invalid parameter: tablet_id");
    }

    auto& mgr = _engine.cloud_warm_up_manager();
    auto opt = mgr.get_tablet_peer_info(tablet_id);
    if (!opt.has_value()) {
        return Status::NotFound("tablet_id {} has no peer candidates", tablet_id);
    }

    // Reset cooldown fields and write back
    auto tpc = std::move(opt.value());
    tpc.consecutive_all_miss = 0;
    tpc.cooldown_until_ms = 0;
    mgr.set_tablet_peer_candidates(tablet_id, std::move(tpc));

    *result = R"({"status":"OK","msg":"cooldown reset"})";
    return Status::OK();
}

void PeerCacheAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.data());

    const std::string& op = req->param("op");
    std::string result;
    Status st;

    if (op == "show") {
        st = _handle_show(req, &result);
    } else if (op == "show_all") {
        st = _handle_show_all(req, &result);
    } else if (op == "set") {
        st = _handle_set(req, &result);
    } else if (op == "remove") {
        st = _handle_remove(req, &result);
    } else if (op == "reset_cooldown") {
        st = _handle_reset_cooldown(req, &result);
    } else {
        st = Status::InvalidArgument(
                "unknown op '{}', supported: show, show_all, set, remove, reset_cooldown", op);
    }

    if (st.ok()) {
        HttpChannel::send_reply(req, HttpStatus::OK, result);
    } else {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, st.to_json());
    }
}

} // namespace doris
