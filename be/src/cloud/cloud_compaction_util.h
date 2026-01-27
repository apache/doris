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

namespace doris {

class CloudTablet;

// [compaction_rw_separation] Check if this cluster should do compaction for the given tablet.
// Returns true if:
//   1. No active cluster record exists (any cluster can compact)
//   2. This cluster is the last active cluster
//   3. Last active cluster is unavailable (SUSPENDED/MANUAL_SHUTDOWN/deleted) and timeout reached
// Returns false if:
//   1. Last active cluster is still active (NORMAL status)
//   2. Last active cluster is unavailable but timeout not reached yet
bool should_do_compaction_for_cluster(CloudTablet* tablet);

} // namespace doris
