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

#include "service/http/action/log_query_action.h"

#include <rapidjson/document.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"
#include "util/easy_json.h"

namespace doris {

namespace {

constexpr int DEFAULT_MAX_ENTRIES = 20;
constexpr int MAX_MAX_ENTRIES = 200;
constexpr int DEFAULT_MAX_BYTES_PER_NODE = 256 * 1024;
constexpr int MAX_MAX_BYTES_PER_NODE = 1024 * 1024;
constexpr int MAX_EXAMPLES_PER_GROUP = 2;
constexpr int MAX_EVENT_TEXT_LENGTH = 4096;

const std::string HEADER_JSON = "application/json";
const std::regex FE_PATTERN(R"(^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(,\d{3})?.*)");
const std::regex BE_PATTERN(R"(^[IWEF]\d{8} \d{2}:\d{2}:\d{2}\.\d{6}.*)");
const std::regex GC_PATTERN(R"(^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{4})\].*)");
const std::regex UUID_PATTERN(
        R"(([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}))");
const std::regex HOST_PORT_PATTERN(R"(((\d{1,3}\.){3}\d{1,3}:\d+))");
const std::regex IP_PATTERN(R"(((\d{1,3}\.){3}\d{1,3}))");
const std::regex HEX_PATTERN(R"((0x[0-9a-fA-F]+))");
const std::regex LARGE_NUMBER_PATTERN(R"((\b\d{4,}\b))");

enum class LogType { BE_INFO, BE_WARNING, BE_GC, BE_JNI };

struct QueryRequest {
    std::vector<std::string> log_types;
    int64_t start_time_ms = -1;
    int64_t end_time_ms = -1;
    std::string keyword;
    std::string reduction_mode = "grouped";
    int max_entries = DEFAULT_MAX_ENTRIES;
    int max_bytes_per_node = DEFAULT_MAX_BYTES_PER_NODE;
};

struct Event {
    std::optional<int64_t> time_ms;
    std::string first_line;
    std::string message;

    int64_t sort_time() const {
        return time_ms.has_value() ? *time_ms : std::numeric_limits<int64_t>::min();
    }
};

struct Group {
    std::string pattern;
    int count = 0;
    std::optional<int64_t> first_time_ms;
    std::optional<int64_t> last_time_ms;
    std::vector<std::string> examples;
};

struct QueryResult {
    std::string log_type;
    std::string reduction_mode;
    std::vector<std::string> scanned_files;
    int matched_event_count = 0;
    int returned_item_count = 0;
    bool truncated = false;
    std::string error;
    std::vector<Group> groups;
    std::vector<Event> events;
};

bool parse_request(const std::string& body, QueryRequest* request, std::string* err) {
    rapidjson::Document document;
    document.Parse(body.c_str());
    if (document.HasParseError() || !document.IsObject()) {
        *err = "Invalid JSON request body";
        return false;
    }
    if (!document.HasMember("logTypes") || !document["logTypes"].IsArray()) {
        *err = "logTypes is required";
        return false;
    }
    for (const auto& value : document["logTypes"].GetArray()) {
        if (!value.IsString()) {
            *err = "logTypes must be strings";
            return false;
        }
        std::string log_type = value.GetString();
        std::transform(log_type.begin(), log_type.end(), log_type.begin(),
                       [](unsigned char ch) { return std::tolower(ch); });
        request->log_types.push_back(log_type);
    }
    if (document.HasMember("startTimeMs") && document["startTimeMs"].IsInt64()) {
        request->start_time_ms = document["startTimeMs"].GetInt64();
    }
    if (document.HasMember("endTimeMs") && document["endTimeMs"].IsInt64()) {
        request->end_time_ms = document["endTimeMs"].GetInt64();
    }
    if (request->start_time_ms >= 0 && request->end_time_ms >= 0 &&
        request->start_time_ms > request->end_time_ms) {
        *err = "startTimeMs must be <= endTimeMs";
        return false;
    }
    if (document.HasMember("keyword") && document["keyword"].IsString()) {
        request->keyword = document["keyword"].GetString();
    }
    if (document.HasMember("reductionMode") && document["reductionMode"].IsString()) {
        request->reduction_mode = document["reductionMode"].GetString();
        std::transform(request->reduction_mode.begin(), request->reduction_mode.end(),
                       request->reduction_mode.begin(),
                       [](unsigned char ch) { return std::tolower(ch); });
    }
    if (request->reduction_mode != "grouped" && request->reduction_mode != "raw") {
        *err = "Unsupported reductionMode";
        return false;
    }
    if (document.HasMember("maxEntries") && document["maxEntries"].IsInt()) {
        request->max_entries = std::clamp(document["maxEntries"].GetInt(), 1, MAX_MAX_ENTRIES);
    }
    if (document.HasMember("maxBytesPerNode") && document["maxBytesPerNode"].IsInt()) {
        request->max_bytes_per_node =
                std::clamp(document["maxBytesPerNode"].GetInt(), 8 * 1024, MAX_MAX_BYTES_PER_NODE);
    }
    return true;
}

std::optional<LogType> parse_log_type(const std::string& log_type) {
    if (log_type == "be.info") {
        return LogType::BE_INFO;
    }
    if (log_type == "be.warning") {
        return LogType::BE_WARNING;
    }
    if (log_type == "be.gc") {
        return LogType::BE_GC;
    }
    if (log_type == "be.jni") {
        return LogType::BE_JNI;
    }
    return std::nullopt;
}

std::string get_log_dir() {
    if (!config::sys_log_dir.empty()) {
        return config::sys_log_dir;
    }
    if (const char* log_dir = std::getenv("LOG_DIR"); log_dir != nullptr) {
        return std::string(log_dir);
    }
    if (const char* doris_home = std::getenv("DORIS_HOME"); doris_home != nullptr) {
        return std::string(doris_home) + "/log";
    }
    return "log";
}

std::vector<std::string> file_prefixes(LogType log_type) {
    switch (log_type) {
    case LogType::BE_INFO:
        return {"be.INFO.log"};
    case LogType::BE_WARNING:
        return {"be.WARNING.log"};
    case LogType::BE_GC:
        return {"be.gc.log"};
    case LogType::BE_JNI:
        return {"jni.log"};
    }
    return {};
}

bool matches_file(LogType log_type, const std::string& file_name) {
    for (const auto& prefix : file_prefixes(log_type)) {
        if (file_name.rfind(prefix, 0) == 0) {
            return true;
        }
    }
    return false;
}

std::vector<std::filesystem::path> list_candidate_files(const std::string& log_dir,
                                                        LogType log_type) {
    std::vector<std::filesystem::path> files;
    std::error_code ec;
    if (!std::filesystem::is_directory(log_dir, ec)) {
        return files;
    }
    for (const auto& entry : std::filesystem::directory_iterator(log_dir, ec)) {
        if (ec) {
            break;
        }
        if (!entry.is_regular_file()) {
            continue;
        }
        std::string file_name = entry.path().filename().string();
        if (matches_file(log_type, file_name)) {
            files.push_back(entry.path());
        }
    }
    std::sort(files.begin(), files.end(), [](const auto& lhs, const auto& rhs) {
        std::error_code left_ec;
        std::error_code right_ec;
        auto left_time = std::filesystem::last_write_time(lhs, left_ec);
        auto right_time = std::filesystem::last_write_time(rhs, right_ec);
        if (left_ec || right_ec) {
            return lhs.filename().string() > rhs.filename().string();
        }
        return left_time > right_time;
    });
    return files;
}

std::string read_tail(const std::filesystem::path& path, int bytes) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        return "";
    }
    input.seekg(0, std::ios::end);
    std::streamoff size = input.tellg();
    std::streamoff start = std::max<std::streamoff>(0, size - bytes);
    input.seekg(start, std::ios::beg);
    std::string content(static_cast<size_t>(size - start), '\0');
    input.read(content.data(), size - start);
    return content;
}

std::string read_recent_text(const std::vector<std::filesystem::path>& files,
                             std::vector<std::string>* scanned_files, int max_bytes) {
    std::vector<std::string> chunks;
    int remaining = max_bytes;
    for (const auto& path : files) {
        if (remaining <= 0) {
            break;
        }
        std::error_code ec;
        auto file_size = std::filesystem::file_size(path, ec);
        if (ec) {
            continue;
        }
        scanned_files->push_back(path.filename().string());
        if (file_size <= static_cast<uint64_t>(remaining)) {
            std::ifstream input(path, std::ios::binary);
            std::stringstream buffer;
            buffer << input.rdbuf();
            chunks.push_back(buffer.str());
            remaining -= static_cast<int>(file_size);
        } else {
            chunks.push_back(read_tail(path, remaining));
            remaining = 0;
        }
    }
    std::reverse(chunks.begin(), chunks.end());
    std::stringstream merged;
    for (size_t i = 0; i < chunks.size(); ++i) {
        if (i != 0) {
            merged << '\n';
        }
        merged << chunks[i];
    }
    return merged.str();
}

bool is_event_start(const std::string& line, LogType log_type) {
    switch (log_type) {
    case LogType::BE_INFO:
    case LogType::BE_WARNING:
        return std::regex_match(line, BE_PATTERN);
    case LogType::BE_GC:
        return std::regex_match(line, GC_PATTERN);
    case LogType::BE_JNI:
        return std::regex_match(line, FE_PATTERN);
    }
    return false;
}

std::optional<int64_t> to_epoch_ms(std::tm* tm_value, int millis, int offset_minutes = 0,
                                   bool utc = false) {
    time_t seconds = utc ? timegm(tm_value) : mktime(tm_value);
    if (seconds < 0) {
        return std::nullopt;
    }
    int64_t epoch_ms = static_cast<int64_t>(seconds) * 1000 + millis;
    epoch_ms -= static_cast<int64_t>(offset_minutes) * 60 * 1000;
    return epoch_ms;
}

std::optional<int64_t> parse_time_ms(const std::string& line, LogType log_type) {
    std::tm tm_value {};
    switch (log_type) {
    case LogType::BE_INFO:
    case LogType::BE_WARNING: {
        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int micros = 0;
        if (std::sscanf(line.c_str() + 1, "%4d%2d%2d %2d:%2d:%2d.%6d", &year, &month, &day, &hour,
                        &minute, &second, &micros) != 7) {
            return std::nullopt;
        }
        tm_value.tm_year = year - 1900;
        tm_value.tm_mon = month - 1;
        tm_value.tm_mday = day;
        tm_value.tm_hour = hour;
        tm_value.tm_min = minute;
        tm_value.tm_sec = second;
        return to_epoch_ms(&tm_value, micros / 1000);
    }
    case LogType::BE_JNI: {
        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int millis = 0;
        if (std::sscanf(line.c_str(), "%4d-%2d-%2d %2d:%2d:%2d,%3d", &year, &month, &day, &hour,
                        &minute, &second, &millis) >= 6) {
            tm_value.tm_year = year - 1900;
            tm_value.tm_mon = month - 1;
            tm_value.tm_mday = day;
            tm_value.tm_hour = hour;
            tm_value.tm_min = minute;
            tm_value.tm_sec = second;
            return to_epoch_ms(&tm_value, millis);
        }
        return std::nullopt;
    }
    case LogType::BE_GC: {
        std::smatch match;
        if (!std::regex_match(line, match, GC_PATTERN)) {
            return std::nullopt;
        }
        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int millis = 0;
        int offset = 0;
        if (std::sscanf(match[1].str().c_str(), "%4d-%2d-%2dT%2d:%2d:%2d.%3d%d", &year, &month,
                        &day, &hour, &minute, &second, &millis, &offset) != 8) {
            return std::nullopt;
        }
        tm_value.tm_year = year - 1900;
        tm_value.tm_mon = month - 1;
        tm_value.tm_mday = day;
        tm_value.tm_hour = hour;
        tm_value.tm_min = minute;
        tm_value.tm_sec = second;
        int offset_minutes = (offset / 100) * 60 + (offset % 100);
        return to_epoch_ms(&tm_value, millis, offset_minutes, true);
    }
    }
    return std::nullopt;
}

std::vector<Event> parse_events(const std::string& content, LogType log_type) {
    std::vector<Event> events;
    std::stringstream stream(content);
    std::string line;
    std::optional<Event> current;
    while (std::getline(stream, line)) {
        if (is_event_start(line, log_type)) {
            if (current.has_value()) {
                events.push_back(*current);
            }
            Event event;
            event.first_line = line;
            event.message = line;
            event.time_ms = parse_time_ms(line, log_type);
            current = event;
        } else if (current.has_value()) {
            current->message.append("\n").append(line);
        }
    }
    if (current.has_value()) {
        events.push_back(*current);
    }
    return events;
}

std::string to_lower_copy(const std::string& value) {
    std::string lowered = value;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                   [](unsigned char ch) { return std::tolower(ch); });
    return lowered;
}

std::vector<Event> filter_events(const std::vector<Event>& events, const QueryRequest& request) {
    std::vector<Event> filtered;
    std::string lowered_keyword = to_lower_copy(request.keyword);
    for (const auto& event : events) {
        if (request.start_time_ms >= 0 && event.time_ms.has_value() &&
            *event.time_ms < request.start_time_ms) {
            continue;
        }
        if (request.end_time_ms >= 0 && event.time_ms.has_value() &&
            *event.time_ms >= request.end_time_ms) {
            continue;
        }
        if (!lowered_keyword.empty() &&
            to_lower_copy(event.message).find(lowered_keyword) == std::string::npos) {
            continue;
        }
        filtered.push_back(event);
    }
    return filtered;
}

std::string trim_text(const std::string& text) {
    if (static_cast<int>(text.size()) <= MAX_EVENT_TEXT_LENGTH) {
        return text;
    }
    return text.substr(0, MAX_EVENT_TEXT_LENGTH) + "...";
}

std::string normalize_pattern(std::string line) {
    line = std::regex_replace(line, UUID_PATTERN, "<UUID>");
    line = std::regex_replace(line, HOST_PORT_PATTERN, "<HOST:PORT>");
    line = std::regex_replace(line, IP_PATTERN, "<IP>");
    line = std::regex_replace(line, HEX_PATTERN, "<HEX>");
    line = std::regex_replace(line, LARGE_NUMBER_PATTERN, "<NUM>");
    return line;
}

std::vector<Group> build_groups(const std::vector<Event>& events, int max_entries,
                                bool* truncated) {
    std::unordered_map<std::string, Group> group_map;
    for (const auto& event : events) {
        std::string pattern = normalize_pattern(event.first_line);
        auto it = group_map.find(pattern);
        if (it == group_map.end()) {
            Group group;
            group.pattern = pattern;
            it = group_map.emplace(pattern, std::move(group)).first;
        }
        Group& group = it->second;
        group.count++;
        if (event.time_ms.has_value()) {
            if (!group.first_time_ms.has_value() || *event.time_ms < *group.first_time_ms) {
                group.first_time_ms = event.time_ms;
            }
            if (!group.last_time_ms.has_value() || *event.time_ms > *group.last_time_ms) {
                group.last_time_ms = event.time_ms;
            }
        }
        if (group.examples.size() < MAX_EXAMPLES_PER_GROUP) {
            group.examples.push_back(trim_text(event.message));
        }
    }
    std::vector<Group> groups;
    groups.reserve(group_map.size());
    for (auto& entry : group_map) {
        groups.push_back(std::move(entry.second));
    }
    std::sort(groups.begin(), groups.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.count != rhs.count) {
            return lhs.count > rhs.count;
        }
        return lhs.last_time_ms.value_or(std::numeric_limits<int64_t>::min()) >
               rhs.last_time_ms.value_or(std::numeric_limits<int64_t>::min());
    });
    *truncated = static_cast<int>(groups.size()) > max_entries;
    if (*truncated) {
        groups.resize(max_entries);
    }
    return groups;
}

QueryResult query_single_type(const QueryRequest& request, const std::string& log_dir,
                              const std::string& type_name, LogType log_type, int bytes_per_type) {
    QueryResult result;
    result.log_type = type_name;
    result.reduction_mode = request.reduction_mode;

    auto candidate_files = list_candidate_files(log_dir, log_type);
    if (candidate_files.empty()) {
        result.error = "No log files matched the log type";
        return result;
    }

    std::string content = read_recent_text(candidate_files, &result.scanned_files, bytes_per_type);
    auto parsed_events = parse_events(content, log_type);
    auto filtered_events = filter_events(parsed_events, request);
    result.matched_event_count = static_cast<int>(filtered_events.size());

    if (request.reduction_mode == "raw") {
        std::sort(
                filtered_events.begin(), filtered_events.end(),
                [](const auto& lhs, const auto& rhs) { return lhs.sort_time() > rhs.sort_time(); });
        result.truncated = static_cast<int>(filtered_events.size()) > request.max_entries;
        if (result.truncated) {
            filtered_events.resize(request.max_entries);
        }
        result.returned_item_count = static_cast<int>(filtered_events.size());
        for (auto& event : filtered_events) {
            event.message = trim_text(event.message);
            result.events.push_back(std::move(event));
        }
        return result;
    }

    result.groups = build_groups(filtered_events, request.max_entries, &result.truncated);
    result.returned_item_count = static_cast<int>(result.groups.size());
    return result;
}

EasyJson build_response(const std::vector<QueryResult>& results) {
    EasyJson response;
    response["msg"] = "success";
    response["code"] = 0;
    EasyJson data = response.Set("data", EasyJson::kObject);
    EasyJson results_json = data.Set("results", EasyJson::kArray);
    for (const auto& result : results) {
        EasyJson result_json = results_json.PushBack(EasyJson::kObject);
        result_json["logType"] = result.log_type;
        result_json["reductionMode"] = result.reduction_mode;
        result_json["matchedEventCount"] = result.matched_event_count;
        result_json["returnedItemCount"] = result.returned_item_count;
        result_json["truncated"] = result.truncated;
        if (!result.error.empty()) {
            result_json["error"] = result.error;
        }
        EasyJson scanned_files_json = result_json.Set("scannedFiles", EasyJson::kArray);
        for (const auto& scanned_file : result.scanned_files) {
            scanned_files_json.PushBack(scanned_file);
        }
        EasyJson groups_json = result_json.Set("groups", EasyJson::kArray);
        for (const auto& group : result.groups) {
            EasyJson group_json = groups_json.PushBack(EasyJson::kObject);
            group_json["pattern"] = group.pattern;
            group_json["count"] = group.count;
            if (group.first_time_ms.has_value()) {
                group_json["firstTimeMs"] = *group.first_time_ms;
            }
            if (group.last_time_ms.has_value()) {
                group_json["lastTimeMs"] = *group.last_time_ms;
            }
            EasyJson examples_json = group_json.Set("examples", EasyJson::kArray);
            for (const auto& example : group.examples) {
                examples_json.PushBack(example);
            }
        }
        EasyJson events_json = result_json.Set("events", EasyJson::kArray);
        for (const auto& event : result.events) {
            EasyJson event_json = events_json.PushBack(EasyJson::kObject);
            if (event.time_ms.has_value()) {
                event_json["timeMs"] = *event.time_ms;
            }
            event_json["firstLine"] = event.first_line;
            event_json["message"] = event.message;
        }
    }
    response["count"] = static_cast<int>(results.size());
    return response;
}

EasyJson build_error_response(const std::string& message) {
    EasyJson response;
    response["msg"] = "Bad Request";
    response["code"] = 400;
    response["data"] = message;
    response["count"] = 0;
    return response;
}

} // namespace

LogQueryAction::LogQueryAction(ExecEnv* exec_env)
        : HttpHandlerWithAuth(exec_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN) {}

void LogQueryAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());

    QueryRequest request;
    std::string err;
    if (!parse_request(req->get_request_body(), &request, &err)) {
        HttpChannel::send_reply(req, HttpStatus::OK, build_error_response(err).ToString());
        return;
    }

    std::vector<std::pair<std::string, LogType>> log_types;
    for (const auto& log_type_name : request.log_types) {
        auto log_type = parse_log_type(log_type_name);
        if (!log_type.has_value()) {
            HttpChannel::send_reply(
                    req, HttpStatus::OK,
                    build_error_response("Unsupported log type: " + log_type_name).ToString());
            return;
        }
        log_types.emplace_back(log_type_name, *log_type);
    }

    int bytes_per_type =
            std::max(8 * 1024, request.max_bytes_per_node / static_cast<int>(log_types.size()));
    std::vector<QueryResult> results;
    std::string log_dir = get_log_dir();
    for (const auto& [type_name, log_type] : log_types) {
        results.push_back(query_single_type(request, log_dir, type_name, log_type, bytes_per_type));
    }

    HttpChannel::send_reply(req, HttpStatus::OK, build_response(results).ToString());
}

} // namespace doris
