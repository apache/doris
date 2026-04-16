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

#include "service/http/action/warmup_stats_action.h"

#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "cloud/cloud_warmup_metrics.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"
#include "util/easy_json.h"

namespace doris {

static std::list<std::string> split_dim_key(const std::string& dim_key) {
    auto pos = dim_key.find(',');
    if (pos == std::string::npos) {
        return {dim_key};
    }
    return {dim_key.substr(0, pos), dim_key.substr(pos + 1)};
}

// Fill windowed num/size metrics into a JSON object
static void fill_windowed(EasyJson& parent, const std::string& key, MBvarWindowedAdder& num_adder,
                          MBvarWindowedAdder& size_adder, const std::string& dim_key) {
    EasyJson obj = parent.Set(key, EasyJson::kObject);
    EasyJson num = obj.Set("num", EasyJson::kObject);
    num["5m"] = num_adder.get_window_value(dim_key, 0);
    num["30m"] = num_adder.get_window_value(dim_key, 1);
    num["2h"] = num_adder.get_window_value(dim_key, 2);
    EasyJson size = obj.Set("size", EasyJson::kObject);
    size["5m"] = size_adder.get_window_value(dim_key, 0);
    size["30m"] = size_adder.get_window_value(dim_key, 1);
    size["2h"] = size_adder.get_window_value(dim_key, 2);
}

void WarmUpStatsAction::handle(HttpRequest* req) {
    // Collect all dimension keys from all metrics
    std::set<std::string> all_keys;
    for (auto& k : g_warmup_ed_requested_segment_num.list_dimensions()) all_keys.insert(k);
    for (auto& k : g_warmup_ed_finish_segment_num.list_dimensions()) all_keys.insert(k);
    for (auto& k : g_warmup_ed_fail_segment_num.list_dimensions()) all_keys.insert(k);

    // Group by job_id
    std::map<std::string, std::vector<std::string>> job_to_keys;
    for (auto& key : all_keys) {
        auto pos = key.find(',');
        std::string jid = (pos != std::string::npos) ? key.substr(0, pos) : "0";
        job_to_keys[jid].push_back(key);
    }

    EasyJson result;
    result["code"] = 0;
    EasyJson jobs = result.Set("data", EasyJson::kArray);

    for (auto& [jid, keys] : job_to_keys) {
        EasyJson job_entry = jobs.PushBack(EasyJson::kObject);
        try {
            job_entry["job_id"] = static_cast<int64_t>(std::stoll(jid));
        } catch (...) {
            job_entry["job_id"] = 0;
        }
        EasyJson tables = job_entry.Set("tables", EasyJson::kArray);

        for (auto& dim_key : keys) {
            auto pos = dim_key.find(',');
            std::string tid_str = (pos != std::string::npos) ? dim_key.substr(pos + 1) : dim_key;

            EasyJson entry = tables.PushBack(EasyJson::kObject);
            try {
                entry["table_id"] = static_cast<int64_t>(std::stoll(tid_str));
            } catch (...) {
                entry["table_id"] = 0;
            }

            // requested
            EasyJson req_obj = entry.Set("requested", EasyJson::kObject);
            fill_windowed(req_obj, "seg", g_warmup_ed_requested_segment_num,
                          g_warmup_ed_requested_segment_size, dim_key);
            fill_windowed(req_obj, "idx", g_warmup_ed_requested_index_num,
                          g_warmup_ed_requested_index_size, dim_key);

            // finish
            EasyJson fin_obj = entry.Set("finish", EasyJson::kObject);
            fill_windowed(fin_obj, "seg", g_warmup_ed_finish_segment_num,
                          g_warmup_ed_finish_segment_size, dim_key);
            fill_windowed(fin_obj, "idx", g_warmup_ed_finish_index_num,
                          g_warmup_ed_finish_index_size, dim_key);

            // fail
            EasyJson fail_obj = entry.Set("fail", EasyJson::kObject);
            fill_windowed(fail_obj, "seg", g_warmup_ed_fail_segment_num,
                          g_warmup_ed_fail_segment_size, dim_key);
            fill_windowed(fail_obj, "idx", g_warmup_ed_fail_index_num, g_warmup_ed_fail_index_size,
                          dim_key);

            // Timestamps
            auto dims = split_dim_key(dim_key);
            auto* trigger_ts = g_warmup_ed_last_trigger_ts.get_stats(dims);
            entry["last_trigger_ts"] = trigger_ts ? trigger_ts->get_value() : 0;
            auto* finish_ts = g_warmup_ed_last_finish_ts.get_stats(dims);
            entry["last_finish_ts"] = finish_ts ? finish_ts->get_value() : 0;
        }
    }

    req->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json");
    HttpChannel::send_reply(req, HttpStatus::OK, result.ToString());
}

} // namespace doris
