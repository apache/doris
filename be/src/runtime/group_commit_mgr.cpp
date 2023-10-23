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

#include "runtime/group_commit_mgr.h"

#include <gen_cpp/Types_types.h>

#include <memory>
#include <numeric>

#include "client_cache.h"
<<<<<<< HEAD
#include "common/config.h"
#include "common/object_pool.h"
#include "exec/data_sink.h"
#include "io/fs/stream_load_pipe.h"
=======
>>>>>>> 6031b601b9 ([improvement](insert) refactor group commit insert into)
#include "olap/wal_manager.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
<<<<<<< HEAD
#include "vec/core/future_block.h"
#include "vec/exec/scan/new_file_scan_node.h"
#include "vec/sink/group_commit_block_sink.h"
=======
>>>>>>> 6031b601b9 ([improvement](insert) refactor group commit insert into)

namespace doris {

Status LoadBlockQueue::add_block(std::shared_ptr<vectorized::FutureBlock> block) {
    DCHECK(block->get_schema_version() == schema_version);
    std::unique_lock l(*_mutex);
    RETURN_IF_ERROR(_status);
    while (*_all_block_queues_bytes > config::group_commit_max_queue_size) {
        _put_cond.wait_for(
                l, std::chrono::milliseconds(LoadBlockQueue::MAX_BLOCK_QUEUE_ADD_WAIT_TIME));
    }
    if (block->rows() > 0) {
        _block_queue.push_back(block);
        *_all_block_queues_bytes += block->bytes();
        *_single_block_queue_bytes += block->bytes();
    }
    _get_cond.notify_all();
    return Status::OK();
}

Status LoadBlockQueue::get_block(vectorized::Block* block, bool* find_block, bool* eos) {
    *find_block = false;
    *eos = false;
    std::unique_lock l(*_mutex);
    if (!need_commit) {
        auto left_milliseconds = config::group_commit_interval_ms -
                                 std::chrono::duration_cast<std::chrono::milliseconds>(
                                         std::chrono::steady_clock::now() - _start_time)
                                         .count();
        if (left_milliseconds <= 0) {
            need_commit = true;
        }
    }
    while (_status.ok() && _block_queue.empty() &&
           (!need_commit || (need_commit && !_load_ids.empty()))) {
        CHECK(*_single_block_queue_bytes == 0);
        auto left_milliseconds = config::group_commit_interval_ms;
        if (!need_commit) {
            left_milliseconds = config::group_commit_interval_ms -
                                std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - _start_time)
                                        .count();
            if (left_milliseconds <= 0) {
                need_commit = true;
                break;
            }
        }
#if !defined(USE_BTHREAD_SCANNER)
        _get_cond.wait_for(l, std::chrono::milliseconds(left_milliseconds));
#else
        _get_cond.wait_for(l, left_milliseconds * 1000);
#endif
    }
    if (!_block_queue.empty()) {
        auto& future_block = _block_queue.front();
        auto* fblock = static_cast<vectorized::FutureBlock*>(block);
        fblock->swap_future_block(future_block);
        *find_block = true;
        _block_queue.pop_front();
        *_all_block_queues_bytes -= fblock->bytes();
        *_single_block_queue_bytes -= block->bytes();
    }
    if (_block_queue.empty() && need_commit && _load_ids.empty()) {
        CHECK(*_single_block_queue_bytes == 0);
        *eos = true;
    } else {
        *eos = false;
    }
    _put_cond.notify_all();
    return Status::OK();
}

void LoadBlockQueue::remove_load_id(const UniqueId& load_id) {
    std::unique_lock l(*_mutex);
    if (_load_ids.find(load_id) != _load_ids.end()) {
        _load_ids.erase(load_id);
        _get_cond.notify_all();
    }
}

Status LoadBlockQueue::add_load_id(const UniqueId& load_id) {
    std::unique_lock l(*_mutex);
    if (need_commit) {
        return Status::InternalError("block queue is set need commit, id=" +
                                     load_instance_id.to_string());
    }
    _load_ids.emplace(load_id);
    return Status::OK();
}

void LoadBlockQueue::cancel(const Status& st) {
    DCHECK(!st.ok());
    std::unique_lock l(*_mutex);
    _status = st;
    while (!_block_queue.empty()) {
        {
            auto& future_block = _block_queue.front();
            std::unique_lock<doris::Mutex> l0(*(future_block->lock));
            future_block->set_result(st, future_block->rows(), 0);
            *_all_block_queues_bytes -= future_block->bytes();
            *_single_block_queue_bytes -= future_block->bytes();
            future_block->cv->notify_all();
        }
        _block_queue.pop_front();
    }
}

Status GroupCommitTable::get_first_block_load_queue(
        int64_t table_id, std::shared_ptr<vectorized::FutureBlock> block,
        std::shared_ptr<LoadBlockQueue>& load_block_queue) {
    DCHECK(table_id == _table_id);
    auto base_schema_version = block->get_schema_version();
    {
        std::unique_lock l(_lock);
        for (int i = 0; i < 3; i++) {
            bool is_schema_version_match = true;
            for (auto it = _load_block_queues.begin(); it != _load_block_queues.end(); ++it) {
                if (!it->second->need_commit) {
                    if (base_schema_version == it->second->schema_version) {
                        if (it->second->add_load_id(block->get_load_id()).ok()) {
                            load_block_queue = it->second;
                            return Status::OK();
                        }
                    } else if (base_schema_version < it->second->schema_version) {
                        is_schema_version_match = false;
                    }
                }
            }
            if (!is_schema_version_match) {
                return Status::DataQualityError("schema version not match");
            }
            if (!_need_plan_fragment) {
                _need_plan_fragment = true;
                RETURN_IF_ERROR(_thread_pool->submit_func([&] {
                    [[maybe_unused]] auto st = _create_group_commit_load(load_block_queue);
                }));
            }
#if !defined(USE_BTHREAD_SCANNER)
            _cv.wait_for(l, std::chrono::seconds(4));
#else
            _cv.wait_for(l, 4 * 1000000);
#endif
            if (load_block_queue != nullptr) {
                if (load_block_queue->schema_version == base_schema_version) {
                    if (load_block_queue->add_load_id(block->get_load_id()).ok()) {
                        return Status::OK();
                    }
                } else if (base_schema_version < load_block_queue->schema_version) {
                    return Status::DataQualityError("schema version not match");
                }
                load_block_queue.reset();
            }
        }
    }
    return Status::InternalError("can not get a block queue");
}

Status GroupCommitTable::_create_group_commit_load(
        std::shared_ptr<LoadBlockQueue>& load_block_queue) {
    Status st = Status::OK();
    std::unique_ptr<int, std::function<void(int*)>> finish_plan_func((int*)0x01, [&](int*) {
        if (!st.ok()) {
            std::unique_lock l(_lock);
            _need_plan_fragment = false;
            _cv.notify_all();
        }
    });
    TStreamLoadPutRequest request;
    UniqueId load_id = UniqueId::gen_uid();
    TUniqueId tload_id;
    tload_id.__set_hi(load_id.hi);
    tload_id.__set_lo(load_id.lo);
    std::regex reg("-");
    std::string label = "group_commit_" + std::regex_replace(load_id.to_string(), reg, "_");
    std::stringstream ss;
    ss << "insert into doris_internal_table_id(" << _table_id << ") WITH LABEL " << label
       << " select * from group_commit(\"table_id\"=\"" << _table_id << "\")";
    request.__set_load_sql(ss.str());
    request.__set_loadId(tload_id);
    request.__set_label(label);
    request.__set_token("group_commit"); // this is a fake, fe not check it now
    request.__set_max_filter_ratio(1.0);
    request.__set_strictMode(false);
    if (_exec_env->master_info()->__isset.backend_id) {
        request.__set_backend_id(_exec_env->master_info()->backend_id);
    } else {
        LOG(WARNING) << "_exec_env->master_info not set backend_id";
    }
    TStreamLoadPutResult result;
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&result, &request](FrontendServiceConnection& client) {
                client->streamLoadPut(result, request);
            },
            10000L);
    RETURN_IF_ERROR(st);
    st = Status::create(result.status);
    if (!st.ok()) {
        LOG(WARNING) << "create group commit load error, st=" << st.to_string();
    }
    RETURN_IF_ERROR(st);
    auto schema_version = result.base_schema_version;
    auto is_pipeline = result.__isset.pipeline_params;
    auto& params = result.params;
    auto& pipeline_params = result.pipeline_params;
    int64_t txn_id;
    TUniqueId instance_id;
    if (!is_pipeline) {
        DCHECK(params.fragment.output_sink.olap_table_sink.db_id == _db_id);
        txn_id = params.txn_conf.txn_id;
        instance_id = params.params.fragment_instance_id;
    } else {
        DCHECK(pipeline_params.fragment.output_sink.olap_table_sink.db_id == _db_id);
        txn_id = pipeline_params.txn_conf.txn_id;
        DCHECK(pipeline_params.local_params.size() == 1);
        instance_id = pipeline_params.local_params[0].fragment_instance_id;
    }
    VLOG_DEBUG << "create plan fragment, db_id=" << _db_id << ", table=" << _table_id
               << ", schema version=" << schema_version << ", label=" << label
               << ", txn_id=" << txn_id << ", instance_id=" << print_id(instance_id)
               << ", is_pipeline=" << is_pipeline;
    {
        load_block_queue = std::make_shared<LoadBlockQueue>(
                instance_id, label, txn_id, schema_version, _all_block_queues_bytes);
        std::unique_lock l(_lock);
        _load_block_queues.emplace(instance_id, load_block_queue);
        _need_plan_fragment = false;
        _cv.notify_all();
    }
    st = _exec_plan_fragment(_db_id, _table_id, label, txn_id, is_pipeline, params,
                             pipeline_params);
    if (!st.ok()) {
        static_cast<void>(_finish_group_commit_load(_db_id, _table_id, label, txn_id, instance_id,
                                                    st, true, nullptr));
    }
    return st;
}

Status GroupCommitTable::_finish_group_commit_load(int64_t db_id, int64_t table_id,
                                                   const std::string& label, int64_t txn_id,
                                                   const TUniqueId& instance_id, Status& status,
                                                   bool prepare_failed, RuntimeState* state) {
    {
        std::lock_guard<doris::Mutex> l(_lock);
        if (prepare_failed || !status.ok()) {
            auto it = _load_block_queues.find(instance_id);
            if (it != _load_block_queues.end()) {
                it->second->cancel(status);
            }
        }
        _load_block_queues.erase(instance_id);
    }
    Status st;
    Status result_status;
    if (status.ok()) {
        // commit txn
        TLoadTxnCommitRequest request;
        request.__set_auth_code(0); // this is a fake, fe not check it now
        request.__set_db_id(db_id);
        request.__set_table_id(table_id);
        request.__set_txnId(txn_id);
        if (state) {
            request.__set_commitInfos(state->tablet_commit_infos());
        }
        TLoadTxnCommitResult result;
        TNetworkAddress master_addr = _exec_env->master_info()->network_address;
        st = ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) {
                    client->loadTxnCommit(result, request);
                },
                10000L);
        result_status = Status::create(result.status);
    } else {
        // abort txn
        TLoadTxnRollbackRequest request;
        request.__set_auth_code(0); // this is a fake, fe not check it now
        request.__set_db_id(db_id);
        request.__set_txnId(txn_id);
        request.__set_reason(status.to_string());
        TLoadTxnRollbackResult result;
        TNetworkAddress master_addr = _exec_env->master_info()->network_address;
        st = ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) {
                    client->loadTxnRollback(result, request);
                },
                10000L);
        result_status = Status::create(result.status);
    }
    if (!st.ok()) {
        LOG(WARNING) << "request finish error, db_id=" << db_id << ", table_id=" << table_id
                     << ", label=" << label << ", txn_id=" << txn_id
                     << ", instance_id=" << print_id(instance_id)
                     << ", executor status=" << status.to_string()
                     << ", request commit status=" << st.to_string();
        if (!prepare_failed) {
            RETURN_IF_ERROR(_exec_env->wal_mgr()->add_wal_path(_db_id, table_id, txn_id, label));
            std::string wal_path;
            RETURN_IF_ERROR(_exec_env->wal_mgr()->get_wal_path(txn_id, wal_path));
            RETURN_IF_ERROR(_exec_env->wal_mgr()->add_recover_wal(
                    std::to_string(db_id), std::to_string(table_id),
                    std::vector<std::string> {wal_path}));
        }
        return st;
    }
    // TODO handle execute and commit error
    if (!prepare_failed && !result_status.ok()) {
        RETURN_IF_ERROR(_exec_env->wal_mgr()->add_wal_path(_db_id, table_id, txn_id, label));
        std::string wal_path;
        RETURN_IF_ERROR(_exec_env->wal_mgr()->get_wal_path(txn_id, wal_path));
        RETURN_IF_ERROR(_exec_env->wal_mgr()->add_recover_wal(std::to_string(db_id),
                                                              std::to_string(table_id),
                                                              std::vector<std::string> {wal_path}));
    } else {
        RETURN_IF_ERROR(_exec_env->wal_mgr()->delete_wal(txn_id));
    }
    std::stringstream ss;
    ss << "finish group commit, db_id=" << db_id << ", table_id=" << table_id << ", label=" << label
       << ", txn_id=" << txn_id << ", instance_id=" << print_id(instance_id);
    if (prepare_failed) {
        ss << ", prepare status=" << status.to_string();
    } else {
        ss << ", execute status=" << status.to_string();
    }
    ss << ", commit status=" << result_status.to_string();
    if (state && !(state->get_error_log_file_path().empty())) {
        ss << ", error_url=" << state->get_error_log_file_path();
    }
    ss << ", rows=" << state->num_rows_load_success();
    LOG(INFO) << ss.str();
    return st;
}

Status GroupCommitTable::_exec_plan_fragment(int64_t db_id, int64_t table_id,
                                             const std::string& label, int64_t txn_id,
                                             bool is_pipeline,
                                             const TExecPlanFragmentParams& params,
                                             const TPipelineFragmentParams& pipeline_params) {
    auto finish_cb = [db_id, table_id, label, txn_id, this](RuntimeState* state, Status* status) {
        static_cast<void>(_finish_group_commit_load(db_id, table_id, label, txn_id,
                                                    state->fragment_instance_id(), *status, false,
                                                    state));
    };
    if (is_pipeline) {
        return _exec_env->fragment_mgr()->exec_plan_fragment(pipeline_params, finish_cb);
    } else {
        return _exec_env->fragment_mgr()->exec_plan_fragment(params, finish_cb);
    }
}

Status GroupCommitTable::get_load_block_queue(const TUniqueId& instance_id,
                                              std::shared_ptr<LoadBlockQueue>& load_block_queue) {
    std::unique_lock l(_lock);
    auto it = _load_block_queues.find(instance_id);
    if (it == _load_block_queues.end()) {
        return Status::InternalError("group commit load instance " + print_id(instance_id) +
                                     " not found");
    }
    load_block_queue = it->second;
    return Status::OK();
}

GroupCommitMgr::GroupCommitMgr(ExecEnv* exec_env) : _exec_env(exec_env) {
    static_cast<void>(ThreadPoolBuilder("GroupCommitThreadPool")
                              .set_min_threads(1)
                              .set_max_threads(config::group_commit_insert_threads)
                              .build(&_thread_pool));
    _all_block_queues_bytes = std::make_shared<std::atomic_size_t>(0);
}

GroupCommitMgr::~GroupCommitMgr() {
    LOG(INFO) << "GroupCommitMgr is destoried";
}

void GroupCommitMgr::stop() {
    _thread_pool->shutdown();
    LOG(INFO) << "GroupCommitMgr is stopped";
}

Status GroupCommitMgr::get_first_block_load_queue(
        int64_t db_id, int64_t table_id, std::shared_ptr<vectorized::FutureBlock> block,
        std::shared_ptr<LoadBlockQueue>& load_block_queue) {
    std::shared_ptr<GroupCommitTable> group_commit_table;
    {
        std::lock_guard wlock(_lock);
        if (_table_map.find(table_id) == _table_map.end()) {
            _table_map.emplace(table_id, std::make_shared<GroupCommitTable>(
                                                 _exec_env, _thread_pool.get(), db_id, table_id,
                                                 _all_block_queues_bytes));
        }
        group_commit_table = _table_map[table_id];
    }
    return group_commit_table->get_first_block_load_queue(table_id, block, load_block_queue);
}

Status GroupCommitMgr::get_load_block_queue(int64_t table_id, const TUniqueId& instance_id,
                                            std::shared_ptr<LoadBlockQueue>& load_block_queue) {
    std::shared_ptr<GroupCommitTable> group_commit_table;
    {
        std::lock_guard<doris::Mutex> l(_lock);
        auto it = _table_map.find(table_id);
        if (it == _table_map.end()) {
            return Status::NotFound("table_id: " + std::to_string(table_id) +
                                    ", instance_id: " + print_id(instance_id) + " dose not exist");
        }
        group_commit_table = it->second;
    }
    return group_commit_table->get_load_block_queue(instance_id, load_block_queue);
}
} // namespace doris
