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

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

namespace doris {
class Thread;
}

namespace doris::io {

struct IOContext;

/// Optional diagnostic event, emitted as one JSONL record. Offsets and sizes are
/// bytes; timestamps are monotonic nanoseconds. IDs are unique within one BE process, not pointers.
/// A GET's parent_id names its read-ahead range or hole-fill task. A fragment's parent_id names
/// the supplying range, while id names the receiving hole-fill task.
struct ReadIOTraceEvent {
    std::string_view event {};
    const IOContext* context {nullptr};
    std::string_view file {};
    uint64_t id {0};
    uint64_t parent_id {0};
    size_t offset {0};
    size_t size {0};
    int64_t time_ns {0}; // zero means record() captures the event time
    int64_t start_ns {0};
    size_t bytes {0};
    int status {0};
    int attempt {0};
    std::string_view outcome {};
    int64_t remote_bytes {0};
    size_t disk_bytes {0};
    size_t inflight_bytes {0};
    size_t available_bytes {0}; // union of disk/inflight coverage, not their sum
};

/// Producers append serialized records to memory; a background thread swaps out the batch and
/// writes it outside the lock, on reaching the byte threshold or flush interval. The thread and
/// files are created lazily. stop() drains accepted records and joins the writer.
class ReadIOTraceWriter {
public:
    ReadIOTraceWriter(std::string directory, size_t flush_bytes,
                      std::chrono::milliseconds flush_interval);
    ~ReadIOTraceWriter();

    void append(std::string_view line);
    // Called by the owner after producers stop. Repeated calls by the owner are allowed.
    void stop();

private:
    void _run();
    void _drop(uint64_t events); // requires _mutex

    const std::string _directory;
    const size_t _flush_bytes;
    const std::chrono::milliseconds _flush_interval;
    std::mutex _mutex;
    std::condition_variable _cv;
    std::shared_ptr<Thread> _thread;
    bool _stopping {false};
    std::string _pending;
    uint64_t _pending_events {0};
    uint64_t _dropped_events {0};
};

/// Diagnostic logging only: no interval history, read coordination or cache decisions in BE.
/// Check enabled() before constructing expensive arguments or probing cache coverage. record()
/// snapshots borrowed fields into JSON before returning; the writer batches those owned bytes.
class ReadIOTrace {
public:
    static bool enabled();
    static uint64_t next_id(); // returns zero while disabled
    static void record(const ReadIOTraceEvent& event);
    static void shutdown(); // flush queued records on both normal and fast BE shutdown
};

} // namespace doris::io
