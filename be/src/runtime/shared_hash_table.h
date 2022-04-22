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

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <thread>

#include "common/object_pool.h"
#include "common/status.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {
class SharedHashTableVal;
class Arena;
namespace vectorized {
class SharedHashTableContext;
class SharedStructure;
}
class SharedHashTableMgr {
public:
    SharedHashTableMgr() = default;
    ~SharedHashTableMgr() = default;
    void set_shared_hash_table_params(TSharedHashTableParams shared_hash_table_params) {
        contain_shared_hash_table = shared_hash_table_params.contain_shared_hash_table;
        is_shared_hash_table_leader = shared_hash_table_params.is_shared_hash_table_leader;
        is_runtime_filter_leader = shared_hash_table_params.is_runtime_filter_leader;
        instacnces_count_in_same_process = shared_hash_table_params.instacnces_count_in_same_process;
    }
    bool get_contain_shared_hash_table() {
        return contain_shared_hash_table;
    }
    bool get_is_shared_hash_table_leader() {
        return is_shared_hash_table_leader;
    }
    bool get_is_runtime_filter_leader() {
        return is_runtime_filter_leader;
    }
    int get_instacnces_count_in_same_process() {
        return instacnces_count_in_same_process;
    }
private:
    bool contain_shared_hash_table = false;
    bool is_shared_hash_table_leader = false;
    bool is_runtime_filter_leader = false;
    int instacnces_count_in_same_process = 0;
};

class SharedHashTableVal {
private:
    std::condition_variable _condition_setup;
    std::condition_variable _condition_barrier;
    std::mutex _mutex;
    std::shared_ptr<vectorized::SharedStructure> _shared_structure;
    int _sharers_count = 0;
public:
    SharedHashTableVal() = default;
    ~SharedHashTableVal() = default;
    void set_sharers_count(int count) {_sharers_count = count;}
    int  get_sharers_count() {return _sharers_count;}
    bool shared_hash_table_operate(bool is_leader, std::shared_ptr<vectorized::SharedStructure>& shared_structure);
    bool shared_hash_table_barrier();
    void get_callback(vectorized::SharedHashTableContext* sharedHashTableContext);
};
// controller -> <query-id, entity>
// SharedHashTableControlEntity is the context used by runtimefilter for merging
// During a query, only the last sink node owns this class, with the end of the query,
// the class is destroyed with the last fragment_exec.
class SharedHashTableControlEntity {
public:
    SharedHashTableControlEntity() : _query_id(0, 0) {}
    ~SharedHashTableControlEntity() = default;

    Status init(UniqueId query_id,
                int instacnces_count_in_same_process,
                const std::vector<int32_t>& shared_hash_table_ids);

    UniqueId query_id() { return _query_id; }
    Status find_hash_table_val(int hash_table_id, SharedHashTableVal* &val);
private:
    UniqueId _query_id;
    // protect _shared_hash_table_map
    std::mutex _shared_hash_table_mutex;
    // hash-table-id -> val
    std::map<int, std::shared_ptr<SharedHashTableVal>> _shared_hash_table_map;
};

// SharedHashTableController has a map query-id -> entity
class SharedHashTableController {
public:
    SharedHashTableController() = default;
    ~SharedHashTableController() = default;

    // thread safe
    // add a query-id -> entity
    // If a query-id -> entity already exists
    // add_entity will return a exists entity
    Status add_entity(const TExecPlanFragmentParams& params,
                      std::shared_ptr<SharedHashTableControlEntity>* handle);
    // thread safe
    // increate a reference count
    // if a query-id is not exist
    // Status.not_ok will be returned and a empty ptr will returned by *handle
    Status acquire(UniqueId query_id, std::shared_ptr<SharedHashTableControlEntity>* handle);

    // thread safe
    // remove a entity by query-id
    // remove_entity will be called automatically by entity when entity is destroyed
    Status remove_entity(UniqueId queryId);

private:
    // protect _hash_table_controller_map to resolve write conflicts.
    // Read _hash_table_controller_map do not need lock.
    std::mutex _controller_mutex;
    // We store the weak pointer here.
    // When the external object is destroyed, we need to clear this record
    using SharedHashTableControllerMap =
            std::map<std::string, std::weak_ptr<SharedHashTableControlEntity>>;
    // str(query-id) -> entity
    SharedHashTableControllerMap _hash_table_controller_map;
};

using shared_hash_table_entity_closer = std::function<void(SharedHashTableControlEntity*)>;

void shared_hash_table_entity_close(SharedHashTableController* controller,
                                    SharedHashTableControlEntity* entity);

} // namespace doris
