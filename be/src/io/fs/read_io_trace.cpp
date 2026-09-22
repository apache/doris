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
#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "runtime/thread_context.h"
#include "util/thread.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris::io {
namespace {

std::atomic<uint64_t> read_trace_next_id {0};
std::atomic<uint64_t> read_trace_event_count {0};
bvar::Adder<uint64_t> read_trace_logged_events {"doris_read_io_trace_events"};
bvar::Adder<uint64_t> read_trace_dropped_events {"doris_read_io_trace_dropped_events"};
bvar::Adder<int64_t> read_trace_pending_bytes {"doris_read_io_trace_pending_bytes"};
bvar::Adder<int64_t> read_trace_record_time_ns {"doris_read_io_trace_record_time_ns"};

const std::string& read_trace_process() {
    // Distinguish BE restarts even when the operating system reuses a PID.
    static const std::string process =
            std::to_string(getpid()) + "-" + std::to_string(UnixMicros());
    return process;
}

ReadIOTraceWriter& read_trace_writer() {
    static ReadIOTraceWriter writer(
            config::read_io_trace_dir.empty() ? (Path(FLAGS_log_dir) / "read_io_trace").native()
                                              : config::read_io_trace_dir,
            cast_set<size_t>(config::read_io_trace_flush_bytes),
            std::chrono::milliseconds(config::read_io_trace_flush_interval_ms));
    return writer;
}

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

ReadIOTraceWriter::ReadIOTraceWriter(std::string directory, size_t flush_bytes,
                                     std::chrono::milliseconds flush_interval)
        : _directory(std::move(directory)),
          _flush_bytes(flush_bytes),
          _flush_interval(flush_interval) {
    DORIS_CHECK(_flush_bytes > 0);
    DORIS_CHECK(_flush_interval.count() > 0);
}

ReadIOTraceWriter::~ReadIOTraceWriter() {
    stop();
}

void ReadIOTraceWriter::_drop(uint64_t events) {
    _dropped_events += events;
    read_trace_dropped_events << events;
}

void ReadIOTraceWriter::append(std::string_view line) {
    std::lock_guard lock(_mutex);
    if (_stopping) {
        _drop(1);
        return;
    }
    if (_thread == nullptr) {
        const auto status = Thread::create(
                "io", "read_io_trace", [this] { _run(); }, &_thread);
        if (!status.ok()) {
            LOG(WARNING) << "failed to start read IO trace writer: " << status;
            _stopping = true;
            _drop(1);
            return;
        }
    }
    _pending.append(line);
    _pending.push_back('\n');
    ++_pending_events;
    read_trace_pending_bytes << cast_set<int64_t>(line.size() + 1);
    if (_pending.size() >= _flush_bytes) {
        _cv.notify_one();
    }
}

void ReadIOTraceWriter::stop() {
    {
        std::lock_guard lock(_mutex);
        if (_stopping) {
            return;
        }
        _stopping = true;
        _cv.notify_one();
    }
    if (_thread != nullptr) {
        _thread->join();
    }
}

void ReadIOTraceWriter::_run() {
    SCOPED_INIT_THREAD_CONTEXT();
    FileWriterPtr file;
    uint64_t file_index = 0;
    uint64_t written_events = 0;
    uint64_t reported_drops = 0;
    std::string batch;
    const auto close_file = [&] {
        if (file != nullptr) {
            WARN_IF_ERROR(file->close(), "failed to close read IO trace file");
            file.reset();
        }
    };
    Defer close_on_exit(close_file);
    const auto write_batch = [&](std::string_view checkpoint) -> Status {
        if (file == nullptr) {
            auto fs = global_local_filesystem();
            RETURN_IF_ERROR(fs->create_directory(_directory));
            const auto path = Path(_directory) / fmt::format("read_io_trace.{}.{}.jsonl",
                                                             read_trace_process(), file_index++);
            const FileWriterOptions options {.sync_file_data = false};
            RETURN_IF_ERROR(fs->create_file(path, &file, &options));
        }
        const Slice slices[] = {Slice(batch), Slice(checkpoint.data(), checkpoint.size())};
        return file->appendv(slices, 2);
    };
    while (true) {
        uint64_t events;
        uint64_t dropped;
        bool stopping;
        {
            std::unique_lock lock(_mutex);
            _cv.wait_for(lock, _flush_interval,
                         [&] { return _stopping || _pending.size() >= _flush_bytes; });
            stopping = _stopping;
            dropped = _dropped_events;
            if (_pending.empty() && dropped == reported_drops) {
                if (stopping) {
                    break;
                }
                continue;
            }
            batch.swap(_pending);
            events = std::exchange(_pending_events, 0);
        }
        TEST_SYNC_POINT_CALLBACK("ReadIOTraceWriter::before_write", this);
        // This checkpoint follows its batch in the same write. It lets offline analysis detect
        // missing files or IO failures, including a failed tail with no later event-sequence gap.
        const auto checkpoint = fmt::format(
                "{{\"v\":1,\"kind\":\"read_io_trace_status\",\"process\":\"{}\","
                "\"written_events\":{},\"dropped_events\":{}}}\n",
                read_trace_process(), written_events + events, dropped);
        const auto status = write_batch(checkpoint);
        if (status.ok()) {
            written_events += events;
            reported_drops = dropped;
            read_trace_logged_events << events;
        } else {
            LOG_EVERY_N(WARNING, 30) << "failed to write read IO trace: " << status;
            close_file();
            std::lock_guard lock(_mutex);
            _drop(events);
        }
        read_trace_pending_bytes << -cast_set<int64_t>(batch.size());
        batch.clear();
        TEST_SYNC_POINT_CALLBACK("ReadIOTraceWriter::after_write", this);
        if (stopping) {
            break;
        }
    }
}

bool ReadIOTrace::enabled() {
    return config::enable_read_io_trace;
}

uint64_t ReadIOTrace::next_id() {
    return enabled() ? read_trace_next_id.fetch_add(1, std::memory_order_relaxed) + 1 : 0;
}

void ReadIOTrace::shutdown() {
    read_trace_writer().stop();
}

void ReadIOTrace::record(const ReadIOTraceEvent& event) {
    if (!enabled()) {
        return;
    }
    const auto record_start = MonotonicNanos();
    Defer record_time {[&] { read_trace_record_time_ns << MonotonicNanos() - record_start; }};
    const int64_t time_ns = event.time_ns != 0 ? event.time_ns : MonotonicNanos();
    const uint64_t sequence = read_trace_event_count.fetch_add(1, std::memory_order_relaxed) + 1;
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
    string_field("process", read_trace_process());
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
    if (event.writeback_timing != nullptr) {
        writer.Key("details");
        writer.StartObject();
        const auto& timing = *event.writeback_timing;
        number_field("complete_submit_ns", timing.complete_submit_ns);
        number_field("partial_submit_ns", timing.partial_submit_ns);
        number_field("lifecycle_trace_ns", timing.lifecycle_trace_ns);
        writer.EndObject();
    }
    if (event.hole_submit_timing != nullptr) {
        writer.Key("details");
        writer.StartObject();
        const auto& timing = *event.hole_submit_timing;
        number_field("cache_probe_ns", timing.cache_probe_ns);
        number_field("queue_lock_wait_ns", timing.queue_lock_wait_ns);
        number_field("queue_lock_hold_ns", timing.queue_lock_hold_ns);
        number_field("allocation_ns", timing.allocation_ns);
        number_field("fragment_lock_wait_ns", timing.fragment_lock_wait_ns);
        number_field("fragment_lock_hold_ns", timing.fragment_lock_hold_ns);
        number_field("copy_ns", timing.copy_ns);
        number_field("copied_bytes", timing.copied_bytes);
        number_field("lifecycle_trace_ns", timing.lifecycle_trace_ns);
        number_field("queue_size", timing.queue_size);
        writer.EndObject();
    }
    if (event.queue_scan != nullptr) {
        writer.Key("details");
        writer.StartObject();
        const auto& scan = *event.queue_scan;
        number_field("queue_size", scan.queue_size);
        number_field("scanned", scan.scanned);
        number_field("delayed", scan.delayed);
        number_field("capacity_waits", scan.capacity_waits);
        number_field("discarded", scan.discarded);
        number_field("discard_check_ns", scan.discard_check_ns);
        number_field("capacity_check_ns", scan.capacity_check_ns);
        writer.EndObject();
    }
    writer.EndObject();
    std::string line(buffer.GetString(), buffer.GetSize());
    TEST_SYNC_POINT_CALLBACK("ReadIOTrace::record", &line);
    read_trace_writer().append(line);
}

} // namespace doris::io
