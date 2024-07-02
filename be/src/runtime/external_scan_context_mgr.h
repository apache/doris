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
#include <stdint.h>
#include <time.h>

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "common/status.h"
#include "gutil/ref_counted.h"
#include "util/countdown_latch.h"

namespace doris {
class ExecEnv;
class Thread;

struct ScanContext {
public:
    TUniqueId fragment_instance_id;
    TUniqueId query_id;
    int64_t offset;
    // use this access_time to clean zombie context
    time_t last_access_time;
    std::mutex _local_lock;
    std::string context_id;
    short keep_alive_min;
    ScanContext(std::string id) : context_id(std::move(id)) {}
};

class ExternalScanContextMgr {
public:
    ExternalScanContextMgr(ExecEnv* exec_env);
    ~ExternalScanContextMgr() = default;

    void stop();

    Status create_scan_context(std::shared_ptr<ScanContext>* p_context);

    Status get_scan_context(const std::string& context_id, std::shared_ptr<ScanContext>* p_context);

    Status clear_scan_context(const std::string& context_id);

private:
    ExecEnv* _exec_env = nullptr;
    std::map<std::string, std::shared_ptr<ScanContext>> _active_contexts;
    void gc_expired_context();

    CountDownLatch _stop_background_threads_latch;
    scoped_refptr<Thread> _keep_alive_reaper;

    std::mutex _lock;
};

} // namespace doris
