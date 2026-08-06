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

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "util/faststring.h"

namespace doris {

class ExecEnv;
class MemTrackerLimiter;
class StreamLoadRecorder;
class Status;

/**
 * StreamLoadRecorderManager is responsible for periodically fetching stream load records
 * from RocksDB (via StreamLoadRecorder) and sending them to the FE's audit_log table
 * via HTTP stream load. Records are formatted to match AUDIT_SCHEMA.
 *
 * This is similar to the FE's AuditLoader logic, where audit events are buffered and sent
 * to the internal audit table when certain thresholds are reached (size or time-based).
 *
 * The manager runs a background worker thread that:
 * 1. Periodically fetches stream load records from RocksDB
 * 2. Parses JSON and formats as TSV matching AUDIT_SCHEMA
 * 3. Buffers them in memory
 * 4. Sends them via HTTP stream load when:
 *    - Buffer size exceeds threshold (default: 50MB)
 *    - Time since last load exceeds threshold (default: 60s)
 *    - Force flush is requested (e.g., during shutdown)
 */
class StreamLoadRecorderManager {
public:
    StreamLoadRecorderManager();

    ~StreamLoadRecorderManager();

    void start();

    void stop();

private:
    void _load_last_fetch_key();

    void _worker_thread_func();

    void _fetch_and_buffer_records();

    void _save_last_fetch_key();

    std::string _parse_and_format_record(const std::string& json_record);

    void _load_if_necessary();

    Status _send_stream_load(const std::string& data);

    std::string _generate_label();

    std::string _generate_url();

    void _reset_batch(int64_t current_time);

private:
    std::shared_ptr<StreamLoadRecorder> _recorder;
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;

    std::thread _worker_thread;
    std::atomic<bool> _stop;

    faststring _buffer;

    int64_t _last_load_time;
    int64_t _record_num;

    std::string _last_fetch_key;
};

} // namespace doris
