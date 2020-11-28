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

#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>

#include "gen_cpp/Types_types.h"
#include "gutil/ref_counted.h"
#include "util/countdown_latch.h"
#include "util/hash_util.hpp"
#include "util/thread.h"

namespace doris {

class ExecEnv;

class BrokerMgr {
public:
    BrokerMgr(ExecEnv* exec_env);
    ~BrokerMgr();
    void init();
    const std::string& get_client_id(const TNetworkAddress& address);

private:
    void ping(const TNetworkAddress& addr);
    void ping_worker();

    ExecEnv* _exec_env;
    std::string _client_id;
    std::mutex _mutex;
    std::unordered_set<TNetworkAddress> _broker_set;

    CountDownLatch _stop_background_threads_latch;
    scoped_refptr<Thread> _ping_thread;
};

} // namespace doris
