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

#include "service/http/action/compaction_profile_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <ctime>
#include <exception>
#include <string>

#include "common/logging.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"
#include "storage/compaction_task_tracker.h"

namespace doris {

namespace {

// Format millisecond timestamp to "YYYY-MM-DD HH:MM:SS" string.
// Returns empty string for 0 timestamps.
std::string format_timestamp_ms(int64_t timestamp_ms) {
    if (timestamp_ms <= 0) {
        return "";
    }
    time_t ts = static_cast<time_t>(timestamp_ms / 1000);
    struct tm local_tm;
    if (localtime_r(&ts, &local_tm) == nullptr) {
        return "";
    }
    char buf[64];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &local_tm);
    return std::string(buf);
}

} // namespace

CompactionProfileAction::CompactionProfileAction(ExecEnv* exec_env, TPrivilegeHier::type hier,
                                                 TPrivilegeType::type ptype)
        : HttpHandlerWithAuth(exec_env, hier, ptype) {}

void CompactionProfileAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HttpHeaders::JSON_TYPE.data());

    // Parse optional parameters
    int64_t tablet_id = 0;
    int64_t top_n = 0;
    std::string compact_type;
    int success_filter = -1; // -1 = no filter, 0 = failed only, 1 = success only

    // tablet_id
    const auto& tablet_id_str = req->param("tablet_id");
    if (!tablet_id_str.empty()) {
        try {
            tablet_id = std::stoll(tablet_id_str);
            if (tablet_id < 0) {
                HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST,
                                        R"({"status":"Error","msg":"tablet_id must be >= 0"})");
                return;
            }
        } catch (const std::exception& e) {
            auto msg = R"({"status":"Error","msg":"invalid tablet_id: )" + std::string(e.what()) +
                       "\"}";
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, msg);
            return;
        }
    }

    // top_n
    const auto& top_n_str = req->param("top_n");
    if (!top_n_str.empty()) {
        try {
            top_n = std::stoll(top_n_str);
            if (top_n < 0) {
                HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST,
                                        R"({"status":"Error","msg":"top_n must be >= 0"})");
                return;
            }
        } catch (const std::exception& e) {
            auto msg =
                    R"({"status":"Error","msg":"invalid top_n: )" + std::string(e.what()) + "\"}";
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, msg);
            return;
        }
    }

    // compact_type
    compact_type = req->param("compact_type");
    if (!compact_type.empty() && compact_type != "base" && compact_type != "cumulative" &&
        compact_type != "full") {
        HttpChannel::send_reply(
                req, HttpStatus::BAD_REQUEST,
                R"({"status":"Error","msg":"compact_type must be one of: base, cumulative, full"})");
        return;
    }

    // success
    const auto& success_str = req->param("success");
    if (!success_str.empty()) {
        if (success_str == "true") {
            success_filter = 1;
        } else if (success_str == "false") {
            success_filter = 0;
        } else {
            HttpChannel::send_reply(
                    req, HttpStatus::BAD_REQUEST,
                    R"({"status":"Error","msg":"success must be 'true' or 'false'"})");
            return;
        }
    }

    // Get completed tasks from tracker
    auto tasks = CompactionTaskTracker::instance()->get_completed_tasks(
            tablet_id, top_n, compact_type, success_filter);

    // Build JSON response
    rapidjson::Document root;
    root.SetObject();
    auto& allocator = root.GetAllocator();

    root.AddMember("status", "Success", allocator);

    rapidjson::Value profiles(rapidjson::kArrayType);

    for (const auto& task : tasks) {
        rapidjson::Value profile(rapidjson::kObjectType);

        profile.AddMember("compaction_id", task.compaction_id, allocator);

        {
            rapidjson::Value v;
            v.SetString(to_string(task.compaction_type), allocator);
            profile.AddMember("compaction_type", v, allocator);
        }

        profile.AddMember("tablet_id", task.tablet_id, allocator);
        profile.AddMember("table_id", task.table_id, allocator);
        profile.AddMember("partition_id", task.partition_id, allocator);

        {
            rapidjson::Value v;
            v.SetString(to_string(task.trigger_method), allocator);
            profile.AddMember("trigger_method", v, allocator);
        }

        profile.AddMember("compaction_score", task.compaction_score, allocator);

        // Datetime fields
        {
            auto s = format_timestamp_ms(task.scheduled_time_ms);
            rapidjson::Value v;
            v.SetString(s.c_str(), static_cast<rapidjson::SizeType>(s.size()), allocator);
            profile.AddMember("scheduled_time", v, allocator);
        }
        {
            auto s = format_timestamp_ms(task.start_time_ms);
            rapidjson::Value v;
            v.SetString(s.c_str(), static_cast<rapidjson::SizeType>(s.size()), allocator);
            profile.AddMember("start_time", v, allocator);
        }
        {
            auto s = format_timestamp_ms(task.end_time_ms);
            rapidjson::Value v;
            v.SetString(s.c_str(), static_cast<rapidjson::SizeType>(s.size()), allocator);
            profile.AddMember("end_time", v, allocator);
        }

        // Derived: cost_time_ms = end_time_ms - start_time_ms
        int64_t cost_time_ms = 0;
        if (task.start_time_ms > 0 && task.end_time_ms > 0) {
            cost_time_ms = task.end_time_ms - task.start_time_ms;
        }
        profile.AddMember("cost_time_ms", cost_time_ms, allocator);

        // Derived: success = (status == FINISHED)
        bool success = (task.status == CompactionTaskStatus::FINISHED);
        profile.AddMember("success", success, allocator);

        // Input statistics
        profile.AddMember("input_rowsets_count", task.input_rowsets_count, allocator);
        profile.AddMember("input_row_num", task.input_row_num, allocator);
        profile.AddMember("input_data_size", task.input_data_size, allocator);
        profile.AddMember("input_index_size", task.input_index_size, allocator);
        profile.AddMember("input_total_size", task.input_total_size, allocator);
        profile.AddMember("input_segments_num", task.input_segments_num, allocator);

        {
            rapidjson::Value v;
            v.SetString(task.input_version_range.c_str(),
                        static_cast<rapidjson::SizeType>(task.input_version_range.size()),
                        allocator);
            profile.AddMember("input_version_range", v, allocator);
        }

        // Output statistics
        profile.AddMember("merged_rows", task.merged_rows, allocator);
        profile.AddMember("filtered_rows", task.filtered_rows, allocator);
        profile.AddMember("output_rows", task.output_rows, allocator);
        profile.AddMember("output_row_num", task.output_row_num, allocator);
        profile.AddMember("output_data_size", task.output_data_size, allocator);
        profile.AddMember("output_index_size", task.output_index_size, allocator);
        profile.AddMember("output_total_size", task.output_total_size, allocator);
        profile.AddMember("output_segments_num", task.output_segments_num, allocator);

        {
            rapidjson::Value v;
            v.SetString(task.output_version.c_str(),
                        static_cast<rapidjson::SizeType>(task.output_version.size()), allocator);
            profile.AddMember("output_version", v, allocator);
        }

        // Merge performance
        profile.AddMember("merge_latency_ms", task.merge_latency_ms, allocator);

        // IO statistics
        profile.AddMember("bytes_read_from_local", task.bytes_read_from_local, allocator);
        profile.AddMember("bytes_read_from_remote", task.bytes_read_from_remote, allocator);

        // Resources
        profile.AddMember("peak_memory_bytes", task.peak_memory_bytes, allocator);
        profile.AddMember("is_vertical", task.is_vertical, allocator);
        profile.AddMember("permits", task.permits, allocator);

        // Vertical compaction progress
        profile.AddMember("vertical_total_groups", task.vertical_total_groups, allocator);
        profile.AddMember("vertical_completed_groups", task.vertical_completed_groups, allocator);

        // Status message (only for failed tasks)
        if (!task.status_msg.empty()) {
            rapidjson::Value v;
            v.SetString(task.status_msg.c_str(),
                        static_cast<rapidjson::SizeType>(task.status_msg.size()), allocator);
            profile.AddMember("status_msg", v, allocator);
        }

        profiles.PushBack(profile, allocator);
    }

    root.AddMember("compaction_profiles", profiles, allocator);

    rapidjson::StringBuffer str_buf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(str_buf);
    root.Accept(writer);

    HttpChannel::send_reply(req, HttpStatus::OK, str_buf.GetString());
}
} // namespace doris
