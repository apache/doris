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

#include <chrono>

#include "client_cache.h"
#include "common/compiler_util.h"
#include "common/config.h"
#include "common/status.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "util/debug_points.h"
#include "util/thrift_rpc_helper.h"

namespace doris {

Status LoadBlockQueue::add_block(RuntimeState* runtime_state,
                                 std::shared_ptr<vectorized::Block> block, bool write_wal) {
    std::unique_lock l(mutex);
    RETURN_IF_ERROR(status);
    auto start = std::chrono::steady_clock::now();
    DBUG_EXECUTE_IF("LoadBlockQueue.add_block.back_pressure_time_out", {
        start = std::chrono::steady_clock::now() - std::chrono::milliseconds(120000);
    });
    while (!runtime_state->is_cancelled() && status.ok() &&
           _all_block_queues_bytes->load(std::memory_order_relaxed) >=
                   config::group_commit_queue_mem_limit) {
        _put_cond.wait_for(l,
                           std::chrono::milliseconds(LoadBlockQueue::MEM_BACK_PRESSURE_WAIT_TIME));
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start);
        if (duration.count() > LoadBlockQueue::MEM_BACK_PRESSURE_WAIT_TIMEOUT) {
            return Status::TimedOut<false>(
                    "Wal memory back pressure wait too much time! Load block queue txn id: {}, "
                    "label: {}, instance id: {}, consumed memory: {}",
                    txn_id, label, load_instance_id.to_string(),
                    _all_block_queues_bytes->load(std::memory_order_relaxed));
        }
    }
    if (UNLIKELY(runtime_state->is_cancelled())) {
        return runtime_state->cancel_reason();
    }
    RETURN_IF_ERROR(status);
    if (block->rows() > 0) {
        if (!config::group_commit_wait_replay_wal_finish) {
            _block_queue.push_back(block);
            _data_bytes += block->bytes();
            _all_block_queues_bytes->fetch_add(block->bytes(), std::memory_order_relaxed);
        }
        if (write_wal || config::group_commit_wait_replay_wal_finish) {
            auto st = _v_wal_writer->write_wal(block.get());
            if (!st.ok()) {
                _cancel_without_lock(st);
                return st;
            }
        }
    }
    if (!_need_commit) {
        if (_data_bytes >= _group_commit_data_bytes) {
            VLOG_DEBUG << "group commit meets commit condition for data size, label=" << label
                       << ", instance_id=" << load_instance_id << ", data_bytes=" << _data_bytes;
            _need_commit = true;
            data_size_condition = true;
        }
        if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                                                  _start_time)
                    .count() >= _group_commit_interval_ms) {
            VLOG_DEBUG << "group commit meets commit condition for time interval, label=" << label
                       << ", instance_id=" << load_instance_id << ", data_bytes=" << _data_bytes;
            _need_commit = true;
        }
    }
    _get_cond.notify_all();
    return Status::OK();
}

Status LoadBlockQueue::get_block(RuntimeState* runtime_state, vectorized::Block* block,
                                 bool* find_block, bool* eos) {
    *find_block = false;
    *eos = false;
    std::unique_lock l(mutex);
    if (!_need_commit) {
        if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                                                  _start_time)
                    .count() >= _group_commit_interval_ms) {
            _need_commit = true;
        }
    }
    while (!runtime_state->is_cancelled() && status.ok() && _block_queue.empty() &&
           (!_need_commit || (_need_commit && !_load_ids.empty()))) {
        auto left_milliseconds = _group_commit_interval_ms;
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - _start_time)
                                .count();
        if (!_need_commit) {
            left_milliseconds = _group_commit_interval_ms - duration;
            if (left_milliseconds <= 0) {
                _need_commit = true;
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
        auto st = runtime_state->cancel_reason();
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
    if (_block_queue.empty() && _need_commit && _load_ids.empty()) {
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
    if (_need_commit) {
        return Status::InternalError<false>("block queue is set need commit, id=" +
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
        std::shared_ptr<LoadBlockQueue>& load_block_queue, int be_exe_version,
        std::shared_ptr<MemTrackerLimiter> mem_tracker) {
    DCHECK(table_id == _table_id);
    {
        std::unique_lock l(_lock);
        for (int i = 0; i < 3; i++) {
            bool is_schema_version_match = true;
            for (const auto& [_, inner_block_queue] : _load_block_queues) {
                if (!inner_block_queue->need_commit()) {
                    if (base_schema_version == inner_block_queue->schema_version) {
                        if (inner_block_queue->add_load_id(load_id).ok()) {
                            load_block_queue = inner_block_queue;
                            return Status::OK();
                        }
                    } else if (base_schema_version < inner_block_queue->schema_version) {
                        is_schema_version_match = false;
                    }
                }
            }
            if (!is_schema_version_match) {
                return Status::DataQualityError<false>(
                        "schema version not match, maybe a schema change is in process. Please "
                        "retry this load manually.");
            }
            if (!_is_creating_plan_fragment) {
                _is_creating_plan_fragment = true;
                RETURN_IF_ERROR(_thread_pool->submit_func([&] {
                    auto st = _create_group_commit_load(be_exe_version, mem_tracker);
                    if (!st.ok()) {
                        LOG(WARNING) << "create group commit load error, st=" << st.to_string();
                        std::unique_lock l(_lock);
                        _is_creating_plan_fragment = false;
                        _cv.notify_all();
                    }
                }));
            }
            _cv.wait_for(l, std::chrono::seconds(4));
        }
    }
    return Status::InternalError<false>("can not get a block queue for table_id: " +
                                        std::to_string(_table_id));
}

Status GroupCommitTable::_create_group_commit_load(int be_exe_version,
                                                   std::shared_ptr<MemTrackerLimiter> mem_tracker) {
    Status st = Status::OK();
    TStreamLoadPutRequest request;
    UniqueId load_id = UniqueId::gen_uid();
    TUniqueId tload_id;
    bool is_pipeline = true;
    TStreamLoadPutResult result;
    std::string label;
    int64_t txn_id;
    TUniqueId instance_id;
    {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker);
        tload_id.__set_hi(load_id.hi);
        tload_id.__set_lo(load_id.lo);
        std::regex reg("-");
        label = "group_commit_" + std::regex_replace(load_id.to_string(), reg, "_");
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
        TNetworkAddress master_addr = _exec_env->master_info()->network_address;
        st = ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&result, &request](FrontendServiceConnection& client) {
                    client->streamLoadPut(result, request);
                },
                10000L);
        if (!st.ok()) {
            LOG(WARNING) << "create group commit load rpc error, st=" << st.to_string();
            return st;
        }
        st = Status::create<false>(result.status);
        if (!st.ok()) {
            LOG(WARNING) << "create group commit load error, st=" << st.to_string();
            return st;
        }
        auto schema_version = result.base_schema_version;
        is_pipeline = result.__isset.pipeline_params;
        auto& params = result.params;
        auto& pipeline_params = result.pipeline_params;
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
            auto load_block_queue = std::make_shared<LoadBlockQueue>(
                    instance_id, label, txn_id, schema_version, _all_block_queues_bytes,
                    result.wait_internal_group_commit_finish, result.group_commit_interval_ms,
                    result.group_commit_data_bytes);
            std::unique_lock l(_lock);
            //create wal
            if (!is_pipeline) {
                RETURN_IF_ERROR(load_block_queue->create_wal(
                        _db_id, _table_id, txn_id, label, _exec_env->wal_mgr(),
                        params.fragment.output_sink.olap_table_sink.schema.slot_descs,
                        be_exe_version));
            } else {
                RETURN_IF_ERROR(load_block_queue->create_wal(
                        _db_id, _table_id, txn_id, label, _exec_env->wal_mgr(),
                        pipeline_params.fragment.output_sink.olap_table_sink.schema.slot_descs,
                        be_exe_version));
            }
            _load_block_queues.emplace(instance_id, load_block_queue);
            _is_creating_plan_fragment = false;
            _cv.notify_all();
        }
    }
    st = _exec_plan_fragment(_db_id, _table_id, label, txn_id, is_pipeline, result.params,
                             result.pipeline_params);
    if (!st.ok()) {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker);
        auto finish_st = _finish_group_commit_load(_db_id, _table_id, label, txn_id, instance_id,
                                                   st, nullptr);
        if (!finish_st.ok()) {
            LOG(WARNING) << "finish group commit error, label=" << label
                         << ", st=" << finish_st.to_string();
        }
    }
    return st;
}

Status GroupCommitTable::_finish_group_commit_load(int64_t db_id, int64_t table_id,
                                                   const std::string& label, int64_t txn_id,
                                                   const TUniqueId& instance_id, Status& status,
                                                   RuntimeState* state) {
    Status st;
    Status result_status;
    DBUG_EXECUTE_IF("LoadBlockQueue._finish_group_commit_load.err_status",
                    { status = Status::InternalError(""); });
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
        result_status = Status::create<false>(result.status);
        DBUG_EXECUTE_IF("LoadBlockQueue._finish_group_commit_load.err_status", {
            std ::string msg = "abort txn";
            LOG(INFO) << "debug promise set: " << msg;
            ExecEnv::GetInstance()->group_commit_mgr()->debug_promise.set_value(
                    Status ::InternalError(msg));
            return status;
        });
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
    DBUG_EXECUTE_IF("LoadBlockQueue._finish_group_commit_load.err_st",
                    { st = Status::InternalError(""); });
    if (status.ok() && st.ok() &&
        (result_status.ok() || result_status.is<ErrorCode::PUBLISH_TIMEOUT>())) {
        if (!config::group_commit_wait_replay_wal_finish) {
            auto delete_st = _exec_env->wal_mgr()->delete_wal(table_id, txn_id);
            if (!delete_st.ok()) {
                LOG(WARNING) << "fail to delete wal " << txn_id << ", st=" << delete_st.to_string();
            }
        }
    } else {
        std::string wal_path;
        RETURN_IF_ERROR(_exec_env->wal_mgr()->get_wal_path(txn_id, wal_path));
        RETURN_IF_ERROR(_exec_env->wal_mgr()->add_recover_wal(db_id, table_id, txn_id, wal_path));
    }
    std::stringstream ss;
    ss << "finish group commit, db_id=" << db_id << ", table_id=" << table_id << ", label=" << label
       << ", txn_id=" << txn_id << ", instance_id=" << print_id(instance_id)
       << ", exec_plan_fragment status=" << status.to_string()
       << ", commit/abort txn rpc status=" << st.to_string()
       << ", commit/abort txn status=" << result_status.to_string()
       << ", this group commit includes " << load_block_queue->group_commit_load_count << " loads"
       << ", flush because meet "
       << (load_block_queue->data_size_condition ? "data size " : "time ") << "condition"
       << ", wal space info:" << ExecEnv::GetInstance()->wal_mgr()->get_wal_dirs_info_string();
    if (state) {
        if (!state->get_error_log_file_path().empty()) {
            ss << ", error_url=" << state->get_error_log_file_path();
        }
        ss << ", rows=" << state->num_rows_load_success();
    }
    LOG(INFO) << ss.str();
    DBUG_EXECUTE_IF("LoadBlockQueue._finish_group_commit_load.get_wal_back_pressure_msg", {
        std ::string msg = _exec_env->wal_mgr()->get_wal_dirs_info_string();
        LOG(INFO) << "debug promise set: " << msg;
        ExecEnv::GetInstance()->group_commit_mgr()->debug_promise.set_value(
                Status ::InternalError(msg));
    };);
    return st;
}

Status GroupCommitTable::_exec_plan_fragment(int64_t db_id, int64_t table_id,
                                             const std::string& label, int64_t txn_id,
                                             bool is_pipeline,
                                             const TExecPlanFragmentParams& params,
                                             const TPipelineFragmentParams& pipeline_params) {
    auto finish_cb = [db_id, table_id, label, txn_id, this](RuntimeState* state, Status* status) {
        DCHECK(state);
        auto finish_st = _finish_group_commit_load(db_id, table_id, label, txn_id,
                                                   state->fragment_instance_id(), *status, state);
        if (!finish_st.ok()) {
            LOG(WARNING) << "finish group commit error, label=" << label
                         << ", st=" << finish_st.to_string();
        }
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
                                                  int be_exe_version,
                                                  std::shared_ptr<MemTrackerLimiter> mem_tracker) {
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
            table_id, base_schema_version, load_id, load_block_queue, be_exe_version, mem_tracker));
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
    std::string real_label = config::group_commit_wait_replay_wal_finish
                                     ? import_label + "_test_wait"
                                     : import_label;
    RETURN_IF_ERROR(ExecEnv::GetInstance()->wal_mgr()->create_wal_path(
            db_id, tb_id, wal_id, real_label, _wal_base_path, WAL_VERSION));
    _v_wal_writer = std::make_shared<vectorized::VWalWriter>(
            db_id, tb_id, wal_id, real_label, wal_manager, slot_desc, be_exe_version);
    return _v_wal_writer->init();
}

Status LoadBlockQueue::close_wal() {
    if (_v_wal_writer != nullptr) {
        RETURN_IF_ERROR(_v_wal_writer->close());
    }
    return Status::OK();
}

bool LoadBlockQueue::has_enough_wal_disk_space(size_t estimated_wal_bytes) {
    DBUG_EXECUTE_IF("LoadBlockQueue.has_enough_wal_disk_space.low_space", { return false; });
    auto* wal_mgr = ExecEnv::GetInstance()->wal_mgr();
    size_t available_bytes = 0;
    {
        Status st = wal_mgr->get_wal_dir_available_size(_wal_base_path, &available_bytes);
        if (!st.ok()) {
            LOG(WARNING) << "get wal dir available size failed, st=" << st.to_string();
        }
    }
    if (estimated_wal_bytes < available_bytes) {
        Status st =
                wal_mgr->update_wal_dir_estimated_wal_bytes(_wal_base_path, estimated_wal_bytes, 0);
        if (!st.ok()) {
            LOG(WARNING) << "update wal dir estimated_wal_bytes failed, reason: " << st.to_string();
        }
        return true;
    } else {
        return false;
    }
}
} // namespace doris
