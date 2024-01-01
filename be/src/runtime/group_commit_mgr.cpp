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
#include <glog/logging.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "client_cache.h"
#include "common/config.h"
#include "common/status.h"
#include "olap/wal_manager.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
#include "vec/core/block.h"

namespace doris {

Status LoadBlockQueue::add_block(std::shared_ptr<vectorized::Block> block, bool write_wal) {
    std::unique_lock l(mutex);
    RETURN_IF_ERROR(status);
    while (_all_block_queues_bytes->load(std::memory_order_relaxed) >
           config::group_commit_max_queue_size) {
        _put_cond.wait_for(
                l, std::chrono::milliseconds(LoadBlockQueue::MAX_BLOCK_QUEUE_ADD_WAIT_TIME));
    }
    if (block->rows() > 0) {
        _block_queue.push_back(block);
        if (write_wal) {
            auto st = _v_wal_writer->write_wal(block.get());
            if (!st.ok()) {
                _cancel_without_lock(st);
                return st;
            }
        }
        _all_block_queues_bytes->fetch_add(block->bytes(), std::memory_order_relaxed);
    }
    _get_cond.notify_all();
    return Status::OK();
}

Status LoadBlockQueue::get_block(RuntimeState* runtime_state, vectorized::Block* block,
                                 bool* find_block, bool* eos) {
    *find_block = false;
    *eos = false;
    std::unique_lock l(mutex);
    if (!need_commit) {
        auto left_milliseconds =
                _group_commit_interval_ms - std::chrono::duration_cast<std::chrono::milliseconds>(
                                                    std::chrono::steady_clock::now() - _start_time)
                                                    .count();
        if (left_milliseconds <= 0) {
            need_commit = true;
        }
    }
    while (!runtime_state->is_cancelled() && status.ok() && _block_queue.empty() &&
           (!need_commit || (need_commit && !_load_ids.empty()))) {
        auto left_milliseconds = _group_commit_interval_ms;
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - _start_time)
                                .count();
        if (!need_commit) {
            left_milliseconds = _group_commit_interval_ms - duration;
            if (left_milliseconds <= 0) {
                need_commit = true;
                break;
            }
        } else {
            if (duration >= 10 * _group_commit_interval_ms) {
                std::stringstream ss;
                ss << "[";
                for (auto& id : _load_ids) {
                    ss << id.to_string() << ", ";
                }
                ss << "]";
                LOG(INFO) << "find one group_commit need to commit, txn_id=" << txn_id
                          << ", label=" << label << ", instance_id=" << load_instance_id
                          << ", duration=" << duration << ", load_ids=" << ss.str()
                          << ", runtime_state=" << runtime_state;
            }
        }
        _get_cond.wait_for(l, std::chrono::milliseconds(left_milliseconds));
    }
    if (runtime_state->is_cancelled()) {
        auto st = Status::Cancelled(runtime_state->cancel_reason());
        _cancel_without_lock(st);
        return st;
    }
    if (!_block_queue.empty()) {
        auto fblock = _block_queue.front();
        block->swap(*fblock.get());
        *find_block = true;
        _block_queue.pop_front();
        _all_block_queues_bytes->fetch_sub(block->bytes(), std::memory_order_relaxed);
    }
    if (_block_queue.empty() && need_commit && _load_ids.empty()) {
        *eos = true;
    } else {
        *eos = false;
    }
    _put_cond.notify_all();
    return Status::OK();
}

void LoadBlockQueue::remove_load_id(const UniqueId& load_id) {
    std::unique_lock l(mutex);
    if (_load_ids.find(load_id) != _load_ids.end()) {
        _load_ids.erase(load_id);
        _get_cond.notify_all();
    }
}

Status LoadBlockQueue::add_load_id(const UniqueId& load_id) {
    std::unique_lock l(mutex);
    if (need_commit) {
        return Status::InternalError("block queue is set need commit, id=" +
                                     load_instance_id.to_string());
    }
    _load_ids.emplace(load_id);
    return Status::OK();
}

void LoadBlockQueue::cancel(const Status& st) {
    DCHECK(!st.ok());
    std::unique_lock l(mutex);
    _cancel_without_lock(st);
}

void LoadBlockQueue::_cancel_without_lock(const Status& st) {
    LOG(INFO) << "cancel group_commit, instance_id=" << load_instance_id << ", label=" << label
              << ", status=" << st.to_string();
    status = st;
    while (!_block_queue.empty()) {
        {
            auto& future_block = _block_queue.front();
            _all_block_queues_bytes->fetch_sub(future_block->bytes(), std::memory_order_relaxed);
        }
        _block_queue.pop_front();
    }
}

Status GroupCommitTable::get_first_block_load_queue(
        int64_t table_id, int64_t base_schema_version, const UniqueId& load_id,
        std::shared_ptr<LoadBlockQueue>& load_block_queue, int be_exe_version) {
    DCHECK(table_id == _table_id);
    {
        std::unique_lock l(_lock);
        for (int i = 0; i < 3; i++) {
            bool is_schema_version_match = true;
            for (auto it = _load_block_queues.begin(); it != _load_block_queues.end(); ++it) {
                if (!it->second->need_commit) {
                    if (base_schema_version == it->second->schema_version) {
                        if (it->second->add_load_id(load_id).ok()) {
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
                    [[maybe_unused]] auto st =
                            _create_group_commit_load(load_block_queue, be_exe_version);
                }));
            }
            _cv.wait_for(l, std::chrono::seconds(4));
            if (load_block_queue != nullptr) {
                if (load_block_queue->schema_version == base_schema_version) {
                    if (load_block_queue->add_load_id(load_id).ok()) {
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
        std::shared_ptr<LoadBlockQueue>& load_block_queue, int be_exe_version) {
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
    // this is an internal interface, use admin to pass the auth check
    request.__set_user("admin");
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
                instance_id, label, txn_id, schema_version, _all_block_queues_bytes,
                result.wait_internal_group_commit_finish, result.group_commit_interval_ms);
        std::unique_lock l(_lock);
        _load_block_queues.emplace(instance_id, load_block_queue);
        _need_plan_fragment = false;
        _exec_env->wal_mgr()->add_wal_status_queue(_table_id, txn_id,
                                                   WalManager::WalStatus::PREPARE);
        //create wal
        if (!is_pipeline) {
            RETURN_IF_ERROR(load_block_queue->create_wal(
                    _db_id, _table_id, txn_id, label, _exec_env->wal_mgr(),
                    params.desc_tbl.slotDescriptors, be_exe_version));
        } else {
            RETURN_IF_ERROR(load_block_queue->create_wal(
                    _db_id, _table_id, txn_id, label, _exec_env->wal_mgr(),
                    pipeline_params.desc_tbl.slotDescriptors, be_exe_version));
        }
        _cv.notify_all();
    }
    st = _exec_plan_fragment(_db_id, _table_id, label, txn_id, is_pipeline, params,
                             pipeline_params);
    if (!st.ok()) {
        static_cast<void>(_finish_group_commit_load(_db_id, _table_id, label, txn_id, instance_id,
                                                    st, nullptr));
    }
    return st;
}

Status GroupCommitTable::_finish_group_commit_load(int64_t db_id, int64_t table_id,
                                                   const std::string& label, int64_t txn_id,
                                                   const TUniqueId& instance_id, Status& status,
                                                   RuntimeState* state) {
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
    std::shared_ptr<LoadBlockQueue> load_block_queue;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _load_block_queues.find(instance_id);
        if (it != _load_block_queues.end()) {
            load_block_queue = it->second;
            if (!status.ok()) {
                load_block_queue->cancel(status);
            }
            //close wal
            RETURN_IF_ERROR(load_block_queue->close_wal());
            // notify sync mode loads
            {
                std::unique_lock l2(load_block_queue->mutex);
                load_block_queue->process_finish = true;
            }
            load_block_queue->internal_group_commit_finish_cv.notify_all();
        }
        _load_block_queues.erase(instance_id);
    }
    // status: exec_plan_fragment result
    // st: commit txn rpc status
    // result_status: commit txn result
    if (status.ok() && st.ok() &&
        (result_status.ok() || result_status.is<ErrorCode::PUBLISH_TIMEOUT>())) {
        RETURN_IF_ERROR(_exec_env->wal_mgr()->delete_wal(
                txn_id, load_block_queue->block_queue_pre_allocated.load()));
        RETURN_IF_ERROR(_exec_env->wal_mgr()->erase_wal_status_queue(table_id, txn_id));
    } else {
        std::string wal_path;
        RETURN_IF_ERROR(_exec_env->wal_mgr()->get_wal_path(txn_id, wal_path));
        RETURN_IF_ERROR(_exec_env->wal_mgr()->add_recover_wal(db_id, table_id, txn_id, wal_path));
        _exec_env->wal_mgr()->add_wal_status_queue(table_id, txn_id, WalManager::WalStatus::REPLAY);
    }
    std::stringstream ss;
    ss << "finish group commit, db_id=" << db_id << ", table_id=" << table_id << ", label=" << label
       << ", txn_id=" << txn_id << ", instance_id=" << print_id(instance_id)
       << ", exec_plan_fragment status=" << status.to_string()
       << ", commit/abort txn rpc status=" << st.to_string()
       << ", commit/abort txn status=" << result_status.to_string();
    if (state) {
        if (!state->get_error_log_file_path().empty()) {
            ss << ", error_url=" << state->get_error_log_file_path();
        }
        ss << ", rows=" << state->num_rows_load_success();
    }
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
                                                    state->fragment_instance_id(), *status, state));
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

Status GroupCommitMgr::get_first_block_load_queue(int64_t db_id, int64_t table_id,
                                                  int64_t base_schema_version,
                                                  const UniqueId& load_id,
                                                  std::shared_ptr<LoadBlockQueue>& load_block_queue,
                                                  int be_exe_version) {
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
    RETURN_IF_ERROR(group_commit_table->get_first_block_load_queue(
            table_id, base_schema_version, load_id, load_block_queue, be_exe_version));
    return Status::OK();
}

Status GroupCommitMgr::get_load_block_queue(int64_t table_id, const TUniqueId& instance_id,
                                            std::shared_ptr<LoadBlockQueue>& load_block_queue) {
    std::shared_ptr<GroupCommitTable> group_commit_table;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _table_map.find(table_id);
        if (it == _table_map.end()) {
            return Status::NotFound("table_id: " + std::to_string(table_id) +
                                    ", instance_id: " + print_id(instance_id) + " dose not exist");
        }
        group_commit_table = it->second;
    }
    return group_commit_table->get_load_block_queue(instance_id, load_block_queue);
}

Status LoadBlockQueue::create_wal(int64_t db_id, int64_t tb_id, int64_t wal_id,
                                  const std::string& import_label, WalManager* wal_manager,
                                  std::vector<TSlotDescriptor>& slot_desc, int be_exe_version) {
    RETURN_IF_ERROR(ExecEnv::GetInstance()->wal_mgr()->add_wal_path(db_id, tb_id, wal_id,
                                                                    import_label, wal_base_path));
    _v_wal_writer = std::make_shared<vectorized::VWalWriter>(
            tb_id, wal_id, import_label, wal_manager, slot_desc, be_exe_version);
    return _v_wal_writer->init();
}

Status LoadBlockQueue::close_wal() {
    if (_v_wal_writer != nullptr) {
        RETURN_IF_ERROR(_v_wal_writer->close());
    }
    return Status::OK();
}

bool LoadBlockQueue::has_enough_wal_disk_space(
        const std::vector<std::shared_ptr<vectorized::Block>>& blocks, const TUniqueId& load_id,
        bool is_blocks_contain_all_load_data) {
    size_t blocks_size = 0;
    for (auto block : blocks) {
        blocks_size += block->bytes();
    }
    size_t content_length = 0;
    Status st = ExecEnv::GetInstance()->group_commit_mgr()->get_load_info(load_id, &content_length);
    if (st.ok()) {
        RETURN_IF_ERROR(ExecEnv::GetInstance()->group_commit_mgr()->remove_load_info(load_id));
    } else {
        return Status::InternalError("can not find load id.");
    }
    size_t pre_allocated = is_blocks_contain_all_load_data
                                   ? blocks_size
                                   : (blocks_size > content_length ? blocks_size : content_length);
    auto* wal_mgr = ExecEnv::GetInstance()->wal_mgr();
    size_t available_bytes = 0;
    {
        st = wal_mgr->get_wal_dir_available_size(wal_base_path, &available_bytes);
        if (!st.ok()) {
            LOG(WARNING) << "get wal disk available size filed!";
        }
    }
    if (pre_allocated < available_bytes) {
        st = wal_mgr->update_wal_dir_pre_allocated(wal_base_path, pre_allocated, true);
        if (!st.ok()) {
            LOG(WARNING) << "update wal dir pre_allocated failed, reason: " << st.to_string();
        }
        block_queue_pre_allocated.fetch_add(pre_allocated);
        return true;
    } else {
        return false;
    }
}

Status GroupCommitMgr::update_load_info(TUniqueId load_id, size_t content_length) {
    std::unique_lock l(_load_info_lock);
    if (_load_id_to_content_length_map.find(load_id) == _load_id_to_content_length_map.end()) {
        _load_id_to_content_length_map.insert(std::make_pair(load_id, content_length));
    }
    return Status::OK();
}

Status GroupCommitMgr::get_load_info(TUniqueId load_id, size_t* content_length) {
    std::shared_lock l(_load_info_lock);
    if (_load_id_to_content_length_map.find(load_id) != _load_id_to_content_length_map.end()) {
        *content_length = _load_id_to_content_length_map[load_id];
        return Status::OK();
    }
    return Status::InternalError("can not find load id!");
}

Status GroupCommitMgr::remove_load_info(TUniqueId load_id) {
    std::unique_lock l(_load_info_lock);
    if (_load_id_to_content_length_map.find(load_id) == _load_id_to_content_length_map.end()) {
        return Status::InternalError("can not remove load id!");
    }
    _load_id_to_content_length_map.erase(load_id);
    return Status::OK();
}
} // namespace doris
