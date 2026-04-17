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
#include <set>
#include <string>

#include "cloud/cloud_warmup_metrics.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"
#include "util/easy_json.h"

namespace doris {

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
    // Collect all job_id dimension keys from all metrics
    std::set<std::string> all_keys;
    for (auto& k : g_warmup_ed_requested_segment_num.list_dimensions()) all_keys.insert(k);
    for (auto& k : g_warmup_ed_finish_segment_num.list_dimensions()) all_keys.insert(k);
    for (auto& k : g_warmup_ed_fail_segment_num.list_dimensions()) all_keys.insert(k);

    EasyJson result;
    result["code"] = 0;
    EasyJson jobs = result.Set("data", EasyJson::kArray);

    for (auto& job_id_str : all_keys) {
        EasyJson entry = jobs.PushBack(EasyJson::kObject);
        try {
            entry["job_id"] = static_cast<int64_t>(std::stoll(job_id_str));
        } catch (...) {
            entry["job_id"] = 0;
        }

        // requested
        EasyJson req_obj = entry.Set("requested", EasyJson::kObject);
        fill_windowed(req_obj, "seg", g_warmup_ed_requested_segment_num,
                      g_warmup_ed_requested_segment_size, job_id_str);
        fill_windowed(req_obj, "idx", g_warmup_ed_requested_index_num,
                      g_warmup_ed_requested_index_size, job_id_str);

        // finish
        EasyJson fin_obj = entry.Set("finish", EasyJson::kObject);
        fill_windowed(fin_obj, "seg", g_warmup_ed_finish_segment_num,
                      g_warmup_ed_finish_segment_size, job_id_str);
        fill_windowed(fin_obj, "idx", g_warmup_ed_finish_index_num, g_warmup_ed_finish_index_size,
                      job_id_str);

        // fail
        EasyJson fail_obj = entry.Set("fail", EasyJson::kObject);
        fill_windowed(fail_obj, "seg", g_warmup_ed_fail_segment_num, g_warmup_ed_fail_segment_size,
                      job_id_str);
        fill_windowed(fail_obj, "idx", g_warmup_ed_fail_index_num, g_warmup_ed_fail_index_size,
                      job_id_str);

        // Timestamps
        auto* trigger_ts = g_warmup_ed_last_trigger_ts.get_stats(std::list<std::string> {job_id_str});
        entry["last_trigger_ts"] = trigger_ts ? trigger_ts->get_value() : 0;
        auto* finish_ts = g_warmup_ed_last_finish_ts.get_stats(std::list<std::string> {job_id_str});
        entry["last_finish_ts"] = finish_ts ? finish_ts->get_value() : 0;
    }

    req->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json");
    HttpChannel::send_reply(req, HttpStatus::OK, result.ToString());
}

} // namespace doris
