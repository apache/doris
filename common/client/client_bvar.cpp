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

#include "client_bvar.h"

#include <bvar/latency_recorder.h>

namespace doris {

namespace client_bvar {
bvar::LatencyRecorder s3_get_latency("s3_get");
bvar::LatencyRecorder s3_put_latency("s3_put");
bvar::LatencyRecorder s3_delete_object_latency("s3_delete_object");
bvar::LatencyRecorder s3_delete_objects_latency("s3_delete_objects");
bvar::LatencyRecorder s3_head_latency("s3_head");
bvar::LatencyRecorder s3_multi_part_upload_latency("s3_multi_part_upload");
bvar::LatencyRecorder s3_list_latency("s3_list");
bvar::LatencyRecorder s3_list_object_versions_latency("s3_list_object_versions");
bvar::LatencyRecorder s3_get_bucket_version_latency("s3_get_bucket_version");
bvar::LatencyRecorder s3_copy_object_latency("s3_copy_object");

}; // namespace client_bvar
} // namespace doris