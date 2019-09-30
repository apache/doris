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

#include <unordered_map>
#include <memory>
#include <mutex>
#include <ostream>
#include <sstream>
#include <thread>
#include <ctime>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/tablets_channel.h"
#include "util/hash_util.hpp"
#include "util/uid_util.h"

#include "service/brpc.h"

namespace doris {

class ExecEnv;

class Cache;

//  Mgr -> load -> tablet
// All dispached load data for this backend is routed from this class
class TabletWriterMgr {
public:
    TabletWriterMgr(ExecEnv* exec_env);
    ~TabletWriterMgr();

    // open a new backend
    Status open(const PTabletWriterOpenRequest& request);

    // this batch must belong to a index in one transaction
    // when batch.
    Status add_batch(const PTabletWriterAddBatchRequest& request,
                     google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec,
                     int64_t* wait_lock_time_ns);

    // cancel all tablet stream for 'load_id' load
    // id: stream load's id
    Status cancel(const PTabletWriterCancelRequest& request);

    Status start_bg_worker();

private:
    ExecEnv* _exec_env;
    // lock protect the channel map
    std::mutex _lock;

    // A map from load_id|index_id to load channel
    butil::FlatMap<
        TabletsChannelKey,
        std::shared_ptr<TabletsChannel>,
        TabletsChannelKeyHasher> _tablets_channels;

    Cache* _lastest_success_channel = nullptr;

    // thread to clean timeout tablets_channel
    std::thread _tablets_channel_clean_thread;

    Status _start_tablets_channel_clean();
};

}
