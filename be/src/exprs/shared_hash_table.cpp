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

#include "runtime/shared_hash_table.h"

#include <mutex>
#include <string>
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/plan_fragment_executor.h"
#include "util/time.h"
//#include "vec/common/arena.h"

namespace doris {
Status SharedHashTableVal::get_callback(vectorized::shared_hash_table_operator* hash_table_accessor,
                                        vectorized::shared_hash_table_barrier* hash_table_barrier) {
    *hash_table_accessor = std::bind(&SharedHashTableVal::shared_hash_table_operate,
                                     this, std::placeholders::_1);

    *hash_table_barrier = std::bind(&SharedHashTableVal::shared_hash_table_barrier,this);
    return Status::OK();
}

bool SharedHashTableVal::shared_hash_table_operate(vectorized::SharedHashTableContext& shared_hash_table_ctx) {
    std::unique_lock<std::mutex> unique_lock(_mutex);
    if (shared_hash_table_ctx._is_leader) {
        // set the hash table variants
        _hash_table_variants = shared_hash_table_ctx._hash_table_variants;
        _build_blocks = shared_hash_table_ctx._build_blocks;
        _inserted_rows = shared_hash_table_ctx._inserted_rows;
        _arena = shared_hash_table_ctx._arena;
        _probe_key_sz = shared_hash_table_ctx._probe_key_sz;
        _build_key_sz = shared_hash_table_ctx._build_key_sz;
        // wakeup follower.
        _condition_setup.notify_all();
    } else {
        // get the hash table variants
        if (!_hash_table_variants) {
            // weakup to handle cancel event.
            if (_condition_setup.wait_for(unique_lock, std::chrono::milliseconds(::doris::config::thrift_rpc_timeout_ms)) == std::cv_status::timeout) {
                return false;
            }
        }
        shared_hash_table_ctx._hash_table_variants = _hash_table_variants;
        shared_hash_table_ctx._build_blocks = _build_blocks;
        shared_hash_table_ctx._inserted_rows = _inserted_rows;
        shared_hash_table_ctx._arena = _arena;
        shared_hash_table_ctx._probe_key_sz = _probe_key_sz;
        shared_hash_table_ctx._build_key_sz = _build_key_sz;
    }
    return true;
}

bool SharedHashTableVal::shared_hash_table_barrier() {
    std::unique_lock<std::mutex> unique_lock(_mutex);
    _sharers_count--;
    if (_sharers_count == 0) {
         _condition_barrier.notify_all();
    } else {
        while (_sharers_count > 0) {
            if (_condition_barrier.wait_for(unique_lock, std::chrono::milliseconds(::doris::config::thrift_rpc_timeout_ms)) == std::cv_status::timeout) {
                return false;
            }
        }
    }
    return true;
}

Status SharedHashTableControlEntity::init(UniqueId query_id,
                                          int instacnces_count_in_same_process,
                                          const std::vector<int32_t>& shared_hash_table_ids) {
    std::lock_guard<std::mutex> guard(_shared_hash_table_mutex);
    for (auto shared_hash_table_id : shared_hash_table_ids) {
        std::shared_ptr<SharedHashTableVal> val = std::make_shared<SharedHashTableVal>();

        val->set_sharers_count(instacnces_count_in_same_process);
        _shared_hash_table_map.emplace(shared_hash_table_id, val);
    }
    return Status::OK();
}

Status SharedHashTableControlEntity::find_hash_table_val(int hash_table_id, SharedHashTableVal* &val) {
    std::lock_guard<std::mutex> guard(_shared_hash_table_mutex); 
    auto iter = _shared_hash_table_map.find(hash_table_id);
    if (iter == _shared_hash_table_map.end()) {
        LOG(WARNING) << "not found entity, shared-hash-table-id:" << hash_table_id;
        return Status::InvalidArgument("not found entity");
    }

    val = iter->second.get();
    return Status::OK();
}

Status SharedHashTableController::add_entity(
       const TExecPlanFragmentParams& params,
       std::shared_ptr<SharedHashTableControlEntity>* handle) {
    // Not set instacnces_count_in_same_process,indicate not use shared hash table.
    if (!params.params.__isset.shared_hash_table_params) {
        return Status::OK();
    }
    shared_hash_table_entity_closer entity_closer =
            std::bind(shared_hash_table_entity_close, this, std::placeholders::_1);

    std::lock_guard<std::mutex> guard(_controller_mutex);
    UniqueId query_id(params.params.query_id);
    std::string query_id_str = query_id.to_string();
    auto iter = _hash_table_controller_map.find(query_id_str);

    if (iter == _hash_table_controller_map.end()) {
        *handle = std::shared_ptr<SharedHashTableControlEntity>(
                new SharedHashTableControlEntity(), entity_closer);
        _hash_table_controller_map[query_id_str] = *handle;
        RETURN_IF_ERROR(handle->get()->init(query_id, params.params.shared_hash_table_params.instacnces_count_in_same_process, 
                                            params.params.shared_hash_table_params.shared_hash_table_ids));
    } else {
        *handle = _hash_table_controller_map[query_id_str].lock();
    }
    return Status::OK();
}

Status SharedHashTableController::acquire(
        UniqueId query_id, std::shared_ptr<SharedHashTableControlEntity>* handle) {
    std::lock_guard<std::mutex> guard(_controller_mutex);
    std::string query_id_str = query_id.to_string();
    auto iter = _hash_table_controller_map.find(query_id_str);
    if (iter == _hash_table_controller_map.end()) {
        LOG(WARNING) << "not found entity, query-id:" << query_id_str;
        return Status::InvalidArgument("not found entity");
    }
    *handle = _hash_table_controller_map[query_id_str].lock();
    if (*handle == nullptr) {
        return Status::InvalidArgument("entity is closed");
    }
    return Status::OK();
}

Status SharedHashTableController::remove_entity(UniqueId queryId) {
    std::lock_guard<std::mutex> guard(_controller_mutex);
    _hash_table_controller_map.erase(queryId.to_string());
    return Status::OK();
}

// auto called while call ~std::shared_ptr<SharedHashTableController>
void shared_hash_table_entity_close(SharedHashTableController* controller,
                                    SharedHashTableControlEntity* entity) {
    controller->remove_entity(entity->query_id());
    delete entity;
}



} // namespace doris
