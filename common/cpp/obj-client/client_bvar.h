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

#include <bvar/latency_recorder.h>

#include <chrono>

namespace doris {

namespace client_bvar {

class ScopedLatency {
public:
    explicit ScopedLatency(bvar::LatencyRecorder& recorder)
            : recorder_(recorder), start_(std::chrono::steady_clock::now()) {}
    ~ScopedLatency() {
        recorder_ << std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::steady_clock::now() - start_)
                             .count();
    }

    ScopedLatency(const ScopedLatency&) = delete;
    ScopedLatency& operator=(const ScopedLatency&) = delete;

private:
    bvar::LatencyRecorder& recorder_;
    std::chrono::steady_clock::time_point start_;
};

extern bvar::LatencyRecorder s3_get_latency;
extern bvar::LatencyRecorder s3_put_latency;
extern bvar::LatencyRecorder s3_delete_object_latency;
extern bvar::LatencyRecorder s3_delete_objects_latency;
extern bvar::LatencyRecorder s3_head_latency;
extern bvar::LatencyRecorder s3_multi_part_upload_latency;
extern bvar::LatencyRecorder s3_list_latency;
extern bvar::LatencyRecorder s3_list_object_versions_latency;
extern bvar::LatencyRecorder s3_get_bucket_version_latency;
extern bvar::LatencyRecorder s3_copy_object_latency;
} // namespace client_bvar

} // namespace doris
