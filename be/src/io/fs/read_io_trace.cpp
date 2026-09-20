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

#include "io/fs/read_io_trace.h"

#include <bvar/reducer.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <unistd.h>

#include <atomic>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "io/io_common.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris::io {
namespace {

std::atomic<uint64_t> read_trace_next_id {0};
std::atomic<uint64_t> read_trace_event_count {0};
std::atomic<uint64_t> read_trace_reported_limit {0};
bvar::Adder<uint64_t> read_trace_logged_events {"doris_read_io_trace_events"};
bvar::Adder<uint64_t> read_trace_dropped_events {"doris_read_io_trace_dropped_events"};

const char* read_trace_source(const IOContext* context) {
    if (context == nullptr) {
        return "other";
    }
    switch (context->read_trace_source) {
    case FileReadTraceSource::READ_AHEAD:
        return "read_ahead";
    case FileReadTraceSource::HOLE_FILL:
        return "hole_fill";
    case FileReadTraceSource::NORMAL:
        return context->reader_type == ReaderType::READER_QUERY ? "sync" : "other";
    }
    __builtin_unreachable();
}

} // namespace

bool ReadIOTrace::enabled() {
    return config::enable_read_io_trace;
}

uint64_t ReadIOTrace::next_id() {
    return enabled() ? read_trace_next_id.fetch_add(1, std::memory_order_relaxed) + 1 : 0;
}

void ReadIOTrace::record(const ReadIOTraceEvent& event) {
    if (!enabled()) {
        return;
    }
    const int64_t time_ns = event.time_ns != 0 ? event.time_ns : MonotonicNanos();
    const uint64_t sequence = read_trace_event_count.fetch_add(1, std::memory_order_relaxed) + 1;
    const auto limit = static_cast<uint64_t>(config::read_io_trace_max_events);
    if (sequence > limit) {
        read_trace_dropped_events << 1;
        if (read_trace_reported_limit.exchange(limit, std::memory_order_relaxed) != limit) {
            LOG(WARNING) << "READ_IO_TRACE_LIMIT max_events=" << limit
                         << "; capture is incomplete; restart BE or increase the limit for further "
                            "capture";
        }
        return;
    }

    // Distinguish BE restarts even when the operating system reuses a PID.
    static const std::string process =
            std::to_string(getpid()) + "-" + std::to_string(UnixMicros());
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    const auto string_field = [&](const char* key, std::string_view value) {
        writer.Key(key);
        writer.String(value.empty() ? "" : value.data(),
                      static_cast<rapidjson::SizeType>(value.size()));
    };
    const auto number_field = [&](const char* key, uint64_t value) {
        writer.Key(key);
        writer.Uint64(value);
    };
    writer.StartObject();
    number_field("v", 1);
    string_field("process", process);
    number_field("seq", sequence);
    string_field("event", event.event);
    string_field("source", read_trace_source(event.context));
    string_field("query", event.context != nullptr && event.context->query_id != nullptr
                                  ? print_id(*event.context->query_id)
                                  : "unknown");
    string_field("file", event.file);
    number_field("id", event.id);
    number_field("parent_id", event.parent_id);
    number_field("offset", event.offset);
    number_field("size", event.size);
    writer.Key("time_ns");
    writer.Int64(time_ns);
    writer.Key("start_ns");
    writer.Int64(event.start_ns);
    number_field("bytes", event.bytes);
    writer.Key("status");
    writer.Int(event.status);
    writer.Key("attempt");
    writer.Int(event.attempt);
    string_field("outcome", event.outcome);
    writer.Key("remote_bytes");
    writer.Int64(event.remote_bytes);
    number_field("disk_bytes", event.disk_bytes);
    number_field("inflight_bytes", event.inflight_bytes);
    number_field("available_bytes", event.available_bytes);
    writer.EndObject();
    std::string line(buffer.GetString(), buffer.GetSize());
    TEST_SYNC_POINT_CALLBACK("ReadIOTrace::record", &line);
    LOG(INFO) << "READ_IO_TRACE " << line;
    read_trace_logged_events << 1;
}

} // namespace doris::io
