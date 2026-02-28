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

#include "runtime/stream_load/stream_load_recorder_manager.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <inttypes.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>

#include <chrono>
#include <sstream>
#include <thread>

#include "common/config.h"
#include "common/status.h"
#include "http/http_client.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/stream_load/stream_load_recorder.h"
#include "runtime/thread_context.h"
#include "util/time.h"

namespace doris {

static constexpr int64_t DEFAULT_STREAM_LOAD_TIMEOUT_SEC = 600; // 10 minutes
static constexpr const char* DEFAULT_INTERNAL_DB_NAME = "__internal_schema";
static constexpr const char* STREAM_LOAD_RECORD_TABLE = "audit_log";
static constexpr const char* COLUMN_SEPARATOR = "\t";
static constexpr const char* LINE_DELIMITER = "\n";
static constexpr const char* LAST_FETCH_KEY_STORAGE_KEY =
        "stream_load_recorder_manager_last_fetch_key";

StreamLoadRecorderManager::StreamLoadRecorderManager()
        : _stop(false), _last_load_time(UnixMillis()), _record_num(0), _last_fetch_key("-1") {}

StreamLoadRecorderManager::~StreamLoadRecorderManager() = default;

void StreamLoadRecorderManager::start() {
    _recorder = ExecEnv::GetInstance()->storage_engine().get_stream_load_recorder();
    _mem_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "StreamLoadRecorderManager");
    _load_last_fetch_key();
    _worker_thread = std::thread(&StreamLoadRecorderManager::_worker_thread_func, this);
}

void StreamLoadRecorderManager::_load_last_fetch_key() {
    if (!_recorder) {
        LOG(WARNING) << "StreamLoadRecorder is not initialized";
        return;
    }

    std::string value;
    Status st = _recorder->get(LAST_FETCH_KEY_STORAGE_KEY, &value, true);
    if (st.ok() && !value.empty()) {
        _last_fetch_key = value;
        LOG(INFO) << "Loaded stream load recorder manager last fetch key from RocksDB: "
                  << _last_fetch_key;
    } else {
        LOG(INFO) << "No stream load recorder manager last fetch key found in RocksDB, starting "
                     "from beginning";
        _last_fetch_key = "-1";
    }
}

void StreamLoadRecorderManager::stop() {
    _stop = true;
    if (_worker_thread.joinable()) {
        _worker_thread.join();
    }
}

void StreamLoadRecorderManager::_worker_thread_func() {
    SCOPED_ATTACH_TASK(_mem_tracker);
    while (!_stop) {
        _fetch_and_buffer_records();
        _load_if_necessary();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

void StreamLoadRecorderManager::_fetch_and_buffer_records() {
    if (!_recorder) {
        LOG(WARNING) << "StreamLoadRecorder is not initialized";
        return;
    }

    std::map<std::string, std::string> records;
    Status st =
            _recorder->get_batch(_last_fetch_key, config::stream_load_record_batch_size, &records);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to fetch stream load records from RocksDB: " << st;
        return;
    }
    if (records.empty()) {
        return;
    }

    for (const auto& [key, value] : records) {
        if (key == LAST_FETCH_KEY_STORAGE_KEY) {
            _last_fetch_key = key;
            continue;
        }
        std::string record_line = _parse_and_format_record(value);
        if (!record_line.empty()) {
            _buffer.append(record_line);
            _buffer.append(LINE_DELIMITER, strlen(LINE_DELIMITER));
            _record_num++;
        }
        _last_fetch_key = key;
    }

    VLOG(1) << "Fetched " << records.size() << " stream load records, total " << _record_num
            << " records buffered";
}

void StreamLoadRecorderManager::_save_last_fetch_key() {
    if (!_recorder) {
        LOG(WARNING) << "StreamLoadRecorder is not initialized";
        return;
    }

    if (_last_fetch_key == "-1" || _last_fetch_key == LAST_FETCH_KEY_STORAGE_KEY) {
        return;
    }

    Status st = _recorder->put(LAST_FETCH_KEY_STORAGE_KEY, _last_fetch_key, true);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to save stream load recorder manager last fetch key to RocksDB: "
                     << st;
    }
}

std::string StreamLoadRecorderManager::_parse_and_format_record(const std::string& json_record) {
    rapidjson::Document doc;
    if (doc.Parse(json_record.data(), json_record.length()).HasParseError()) {
        LOG(WARNING) << "Failed to parse JSON record: " << json_record;
        return "";
    }

    auto get_string = [&doc](const char* key) -> std::string {
        if (doc.HasMember(key) && doc[key].IsString()) {
            return doc[key].GetString();
        }
        return "";
    };

    auto get_int64 = [&doc](const char* key) -> int64_t {
        if (doc.HasMember(key) && doc[key].IsInt64()) {
            return doc[key].GetInt64();
        }
        return 0;
    };

    auto timestamp_to_datetime = [](int64_t ts_ms) -> std::string {
        if (ts_ms <= 0) {
            return "";
        }
        time_t ts_sec = ts_ms / 1000;
        int64_t ms_part = ts_ms % 1000;
        struct tm tm_buf;
        localtime_r(&ts_sec, &tm_buf);
        char buf[64];
        snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%03" PRId64,
                 tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday, tm_buf.tm_hour,
                 tm_buf.tm_min, tm_buf.tm_sec, ms_part);
        return {buf};
    };

    std::string label = get_string("Label");
    std::string db = get_string("Db");
    std::string table = get_string("Table");
    std::string user = get_string("User");
    std::string client_ip = get_string("ClientIp");
    std::string status = get_string("Status");
    std::string message = get_string("Message");
    std::string error_url = get_string("ErrorURL");
    int64_t total_rows = get_int64("NumberTotalRows");
    int64_t loaded_rows = get_int64("NumberLoadedRows");
    int64_t filtered_rows = get_int64("NumberFilteredRows");
    int64_t unselected_rows = get_int64("NumberUnselectedRows");
    int64_t load_bytes = get_int64("LoadBytes");
    int64_t start_time = get_int64("StartTime");
    int64_t finish_time = get_int64("FinishTime");
    std::string comment = get_string("Comment");
    int64_t query_time = (finish_time > start_time) ? (finish_time - start_time) : 0;
    std::string stmt = fmt::format(
            "STREAM LOAD: table={}, label={}, status={}, "
            "total_rows={}, loaded_rows={}, filtered_rows={}, "
            "unselected_rows={}, load_bytes={}, url={}",
            table, label, status, total_rows, loaded_rows, filtered_rows, unselected_rows,
            load_bytes, error_url);
    if (!comment.empty()) {
        stmt += fmt::format(", comment={}", comment);
    }

    // Use column mapping in stream load to map these to AUDIT_SCHEMA columns.
    std::stringstream ss;
    ss << label
       << COLUMN_SEPARATOR // query_id, we use label replace it for it is hard to get query id and label query is convenient for users
       << timestamp_to_datetime(finish_time)
       << COLUMN_SEPARATOR                // time (convert to datetime string)
       << client_ip << COLUMN_SEPARATOR   // client_ip
       << user << COLUMN_SEPARATOR        // user
       << db << COLUMN_SEPARATOR          // db
       << status << COLUMN_SEPARATOR      // state
       << message << COLUMN_SEPARATOR     // error_message
       << query_time << COLUMN_SEPARATOR  // query_time
       << load_bytes << COLUMN_SEPARATOR  // scan_bytes
       << total_rows << COLUMN_SEPARATOR  // scan_rows
       << loaded_rows << COLUMN_SEPARATOR // return_rows
       << stmt;                           // stmt

    return ss.str();
}

void StreamLoadRecorderManager::_load_if_necessary() {
    int64_t current_time = UnixMillis();
    bool should_load = _buffer.size() >= config::stream_load_record_batch_bytes ||
                       (current_time - _last_load_time) >=
                               config::stream_load_record_batch_interval_secs * 1000;
    if (!should_load || _buffer.size() == 0) {
        return;
    }

    Status st = _send_stream_load(_buffer.ToString());
    if (!st.ok()) {
        LOG(WARNING) << "Failed to load stream load records to audit log table: " << st
                     << ", discard current batch";
    }

    _save_last_fetch_key();

    _reset_batch(current_time);
}

Status StreamLoadRecorderManager::_send_stream_load(const std::string& data) {
    std::string url = _generate_url();
    if (url.empty()) {
        return Status::InternalError("FE address is not available yet");
    }
    std::string label = _generate_label();

    HttpClient client;
    Status st = client.init(url);
    if (!st.ok()) {
        return Status::InternalError("Failed to init http client: {}", st.to_string());
    }
    client.set_authorization("Basic YWRtaW46");
    client.set_header("Expect", "100-continue");
    client.set_content_type("text/plain; charset=UTF-8");
    client.set_header("label", label);
    client.set_header("timeout", std::to_string(DEFAULT_STREAM_LOAD_TIMEOUT_SEC));
    client.set_header("max_filter_ratio", "1.0");
    client.set_header("column_separator", COLUMN_SEPARATOR);
    client.set_header("line_delimiter", LINE_DELIMITER);
    client.set_header("columns",
                      "query_id,time,client_ip,user,db,state,error_message,"
                      "query_time,scan_bytes,scan_rows,return_rows,stmt");
    client.set_header("skip_record_to_audit_log_table",
                      "true"); // Prevent infinite loop: don't record audit loads
    client.set_timeout_ms(DEFAULT_STREAM_LOAD_TIMEOUT_SEC * 1000);

    std::string response;
    st = client.execute_put_request(data, &response);
    if (!st.ok()) {
        return Status::InternalError("Failed to execute stream load request: {}", st.to_string());
    }
    long http_status = client.get_http_status();
    if (http_status != 200) {
        return Status::InternalError("Stream load failed with HTTP status {}: {}", http_status,
                                     response);
    }
    rapidjson::Document doc;
    if (!doc.Parse(response.data(), response.length()).HasParseError()) {
        if (doc.HasMember("Status") && doc["Status"].IsString()) {
            std::string status = doc["Status"].GetString();
            if (status != "Success") {
                return Status::InternalError("Stream load status is not Success: {}", response);
            }
        }
    }

    return Status::OK();
}

std::string StreamLoadRecorderManager::_generate_label() {
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    auto now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::tm tm_buf;
    localtime_r(&now_time_t, &tm_buf);
    return fmt::format("audit_log_{:04d}{:02d}{:02d}_{:02d}{:02d}{:02d}_{:03d}",
                       tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday, tm_buf.tm_hour,
                       tm_buf.tm_min, tm_buf.tm_sec, static_cast<int>(now_ms.count()));
}

std::string StreamLoadRecorderManager::_generate_url() {
    return fmt::format("http://127.0.0.1:{}/api/{}/{}/_stream_load", config::webserver_port,
                       DEFAULT_INTERNAL_DB_NAME, STREAM_LOAD_RECORD_TABLE);
}

void StreamLoadRecorderManager::_reset_batch(int64_t current_time) {
    _buffer.clear();
    _last_load_time = current_time;
    _record_num = 0;
}

} // namespace doris
