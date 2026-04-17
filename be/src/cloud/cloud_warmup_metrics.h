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

#include <bvar/bvar.h>
#include <bvar/multi_dimension.h>

#include "util/bvar_windowed_adder.h"

namespace doris {

// Source BE metrics — keyed by job_id (defined in cloud_warm_up_manager.cpp)
extern MBvarWindowedAdder g_warmup_ed_requested_segment_num;
extern MBvarWindowedAdder g_warmup_ed_requested_segment_size;
extern MBvarWindowedAdder g_warmup_ed_requested_index_num;
extern MBvarWindowedAdder g_warmup_ed_requested_index_size;
extern bvar::MultiDimension<bvar::Status<int64_t>> g_warmup_ed_last_trigger_ts;

// Target BE metrics — keyed by job_id (defined in cloud_internal_service.cpp)
extern MBvarWindowedAdder g_warmup_ed_finish_segment_num;
extern MBvarWindowedAdder g_warmup_ed_finish_segment_size;
extern MBvarWindowedAdder g_warmup_ed_finish_index_num;
extern MBvarWindowedAdder g_warmup_ed_finish_index_size;
extern MBvarWindowedAdder g_warmup_ed_fail_segment_num;
extern MBvarWindowedAdder g_warmup_ed_fail_segment_size;
extern MBvarWindowedAdder g_warmup_ed_fail_index_num;
extern MBvarWindowedAdder g_warmup_ed_fail_index_size;
extern bvar::MultiDimension<bvar::Status<int64_t>> g_warmup_ed_last_finish_ts;

} // namespace doris
