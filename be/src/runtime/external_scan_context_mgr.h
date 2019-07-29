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

#include <boost/thread/lock_guard.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include <memory>
#include <string>
#include <time.h>
#include <map>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"

namespace doris {

struct Context{
public:
    TUniqueId fragment_instance_id;
    int64_t offset;
    // use this access_time to clean zombie context
    time_t last_access_time;
    boost::mutex _local_lock;
    std::string context_id;
    short keep_alive_min;
    Context(std::string context_id) : context_id(context_id) {}
    Context(TUniqueId fragment_instance_id, int64_t offset) : fragment_instance_id(fragment_instance_id), offset(offset) {}
};

class ExternalScanContextMgr {

public:

    ExternalScanContextMgr(ExecEnv* exec_env);

    ~ExternalScanContextMgr() {
        _is_stop = true;
        _keep_alive_reaper->join();
    }

    Status create_scan_context(std::shared_ptr<Context>* p_context);

    Status get_scan_context(const std::string& context_id, std::shared_ptr<Context>* p_context);

    Status clear_scan_context(const std::string& context_id);


private:
    ExecEnv* _exec_env;
    std::map<std::string, std::shared_ptr<Context>> _active_contexts;
    
    void gc_expired_context();
    bool _is_stop;

    boost::scoped_ptr<boost::thread> _keep_alive_reaper;

    boost::mutex _lock;

    u_int32_t _scan_context_gc_interval_min;

};

}