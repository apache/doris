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
#include <mutex>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "util/uid_util.h"

namespace doris {

class Cache;
class MemTracker;
class TabletsChannel;

// A LoadChannel manages tablets channels for all indexes
// corresponding to a certain load job
class LoadChannel {
public:
    LoadChannel(const UniqueId& load_id);
    ~LoadChannel();

    // open a new load channel if not exist
    Status open(const PTabletWriterOpenRequest& request);

    // this batch must belong to a index in one transaction
    Status add_batch(const PTabletWriterAddBatchRequest& request);

    // return true if this load channel has been opened and all tablets channels are closed then.
    bool is_finished();

    time_t last_updated_time() { return _last_updated_time; }

private:
    UniqueId _load_id;
    // this mem tracker tracks the total mem comsuption of this load task
    std::unique_ptr<MemTracker> _mem_tracker; 

    // lock protect the tablets channel map
    std::mutex _lock;
    // index id -> tablets channel
    std::unordered_map<int64_t, std::shared_ptr<TabletsChannel>> _tablets_channels;

    Cache* _lastest_success_channel = nullptr;
    // set to true if at least one tablets channel has been opened
    bool _opened = false;

    time_t _last_updated_time;
};

}
