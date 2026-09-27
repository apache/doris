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

#include <gen_cpp/internal_service.pb.h>

namespace doris {

class CloudStorageEngine;

// Answers a plan-time GLOBAL_POINT prune request: for every requested tablet, whether it may
// contain one of the probe values at the requested snapshot version.
//
// Only in-memory tablet metadata and the file cache are used: a tablet that is not cached on this
// BE, or whose cached rowsets have not reached the snapshot version, is kept (answered as a
// candidate) instead of being synced from the meta service. A tablet is left out of the
// candidates only when every visible non-empty rowset has a usable bloom and none of them matches.
void handle_global_point_index_prune(CloudStorageEngine& engine,
                                     const PGlobalPointIndexPruneRequest& request,
                                     PGlobalPointIndexPruneResponse* response);

} // namespace doris
