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

#include "runtime/stream_load/new_load_stream_mgr.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(new_stream_load_pipe_count, MetricUnit::NOUNIT);

NewLoadStreamMgr::NewLoadStreamMgr() {
    // Each StreamLoadPipe has a limited buffer size (default 1M), it's not needed to count the
    // actual size of all StreamLoadPipe.
    REGISTER_HOOK_METRIC(new_stream_load_pipe_count, [this]() { return _stream_map.size(); });
}

NewLoadStreamMgr::~NewLoadStreamMgr() {
    DEREGISTER_HOOK_METRIC(new_stream_load_pipe_count);
}
} // namespace doris
