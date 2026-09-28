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

#include <gen_cpp/Types_types.h>

#include <filesystem>
#include <string>

namespace doris {

// Per pipeline worker timeline log. Every worker thread appends to its own file
// `<worker_timeline_log_dir()>/<scheduler>_<index>.log`, one line per event:
//
//   <unix timestamp in microseconds> <query id> START|END
//
// START is written when the worker begins working on a pipeline task of the query and
// END when it releases that task.

// `${LOG_DIR}/worker_timeline/<process startup time, YYYYMMDD-HHMMSS>`, fixed on first call.
const std::filesystem::path& worker_timeline_log_dir();

// Creates worker_timeline_log_dir() if the log is enabled. Called once on BE startup.
void init_worker_timeline_log_dir();

// Must only be called from the pipeline worker thread identified by `scheduler` and `index`.
void worker_timeline_record(const std::string& scheduler, int index, const TUniqueId& query_id,
                            bool start);

} // namespace doris
