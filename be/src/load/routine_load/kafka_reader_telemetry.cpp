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

#include "load/routine_load/kafka_reader_telemetry.h"

#include "common/logging.h"
#include "util/debug_points.h"
#include "util/time.h"

namespace doris {

KafkaReaderTelemetry::KafkaReaderTelemetry(int64_t job_id, const UniqueId& reader, bool share,
                                           bool kafka)
        : _job_id(job_id), _reader(reader), _share(share) {
    DBUG_EXECUTE_IF("routine_load.reader_telemetry", { _enabled = kafka; });
}

KafkaReaderTelemetry::~KafkaReaderTelemetry() {
    flush();
}

void KafkaReaderTelemetry::received(int32_t partition, int64_t now_ms) {
    if (!_enabled) {
        return;
    }
    auto& window = _windows[static_cast<uint32_t>(partition) % _windows.size()];
    // Flush collisions instead of retaining unbounded per-partition state.
    if (window.records > 0 && (window.partition != partition || now_ms - window.first_ms >= 250)) {
        emit(window);
    }
    if (window.records == 0) {
        window.partition = partition;
        window.first_ms = now_ms;
    }
    ++window.records;
    window.last_ms = now_ms;
}

void KafkaReaderTelemetry::emit(Window& window) const {
    if (window.records == 0) {
        return;
    }
    LOG(INFO) << "kafka_reader_sample job=" << _job_id << " reader=" << _reader
              << " mode=" << (_share ? "C" : "A") << " partition=" << window.partition
              << " records=" << window.records << " first_ms=" << window.first_ms
              << " last_ms=" << window.last_ms;
    window.records = 0;
}

void KafkaReaderTelemetry::flush() {
    for (auto& window : _windows) {
        emit(window);
    }
}

void KafkaReaderTelemetry::timing(const char* stage, int64_t elapsed_ns, int64_t records) const {
    if (_enabled) {
        LOG(INFO) << "kafka_reader_stage job=" << _job_id << " reader=" << _reader
                  << " mode=" << (_share ? "C" : "A") << " stage=" << stage << " ns=" << elapsed_ns
                  << " time_ms=" << UnixMillis() << " records=" << records;
    }
}

} // namespace doris
