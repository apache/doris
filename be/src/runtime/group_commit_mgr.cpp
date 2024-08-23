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
#include "cloud/config.h"
#include "common/compiler_util.h"
#include "common/config.h"
#include "common/status.h"
#include "pipeline/dependency.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "util/debug_points.h"
#include "util/thrift_rpc_helper.h"

namespace doris {

Status LoadBlockQueue::add_block(RuntimeState* runtime_state,
                                 std::shared_ptr<vectorized::Block> block, bool write_wal,
                                 UniqueId& load_id) {
    DBUG_EXECUTE_IF("LoadBlockQueue.add_block.failed",
                    { return Status::InternalError("LoadBlockQueue.add_block.failed"); });
    std::unique_lock l(mutex);
    RETURN_IF_ERROR(status);
    if (UNLIKELY(runtime_state->is_cancelled())) {
        return runtime_state->cancel_reason();
    }
    RETURN_IF_ERROR(status);
    if (block->rows() > 0) {
        if (!config::group_commit_wait_replay_wal_finish) {
            _block_queue.emplace_back(block);
            _data_bytes += block->bytes();
            int before_block_queues_bytes = _all_block_queues_bytes->load();
            _all_block_queues_bytes->fetch_add(block->bytes(), std::memory_order_relaxed);
            std::stringstream ss;
            ss << "[";
            for (const auto& id : _load_ids_to_write_dep) {
                ss << id.first.to_string() << ", ";
            }
            ss << "]";
            VLOG_DEBUG << "[Group Commit Debug] (LoadBlockQueue::add_block). "
                       << "block queue size is " << _block_queue.size() << ", block rows is "
                       << block->rows() << ", block bytes is " << block->bytes()
                       << ", before add block, all block queues bytes is "
                       << before_block_queues_bytes
                       << ", after add block, all block queues bytes is "
                       << _all_block_queues_bytes->load() << ", txn_id=" << txn_id
                       << ", label=" << label << ", instance_id=" << load_instance_id
                       << ", load_ids=" << ss.str() << ", runtime_state=" << runtime_state
                       << ", the block is " << block->dump_data() << ", the block column size is "
                       << block->columns_bytes();
        }
        if (write_wal || config::group_commit_wait_replay_wal_finish) {
            auto st = _v_wal_writer->write_wal(block.get());
            if (!st.ok()) {
                _cancel_without_lock(st);
                return st;
            }
        }
        if (!runtime_state->is_cancelled() && status.ok() &&
            _all_block_queues_bytes->load(std::memory_order_relaxed) >=
                    config::group_commit_queue_mem_limit) {
            DCHECK(_load_ids_to_write_dep.find(load_id) != _load_ids_to_write_dep.end());
            _load_ids_to_write_dep[load_id]->block();
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
    for (auto read_dep : _read_deps) {
        read_dep->set_ready();
    }
    return Status::OK();
}

Status LoadBlockQueue::get_block(RuntimeState* runtime_state, vectorized::Block* block,
                                 bool* find_block, bool* eos,
                                 std::shared_ptr<pipeline::Dependency> get_block_dep) {
    *find_block = false;
    *eos = false;
    std::unique_lock l(mutex);
    if (runtime_state->is_cancelled() || !status.ok()) {
        auto st = runtime_state->cancel_reason();
        _cancel_without_lock(st);
        return status;
    }
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - _start_time)
                            .count();
    if (!_need_commit && duration >= _group_commit_interval_ms) {
        _need_commit = true;
    }
    auto get_load_ids = [&]() {
        std::stringstream ss;
        ss << "[";
        for (auto& id : _load_ids_to_write_dep) {
            ss << id.first.to_string() << ", ";
        }
        ss << "]";
        return ss.str();
    };
    if (_block_queue.empty()) {
        if (_need_commit && duration >= 10 * _group_commit_interval_ms) {
            auto last_print_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                                               std::chrono::steady_clock::now() - _last_print_time)
                                               .count();
            if (last_print_duration >= 10000) {
                _last_print_time = std::chrono::steady_clock::now();
                LOG(INFO) << "find one group_commit need to commit, txn_id=" << txn_id
                          << ", label=" << label << ", instance_id=" << load_instance_id
                          << ", duration=" << duration << ", load_ids=" << get_load_ids();
            }
        }
        if (!_load_ids_to_write_dep.empty()) {
            get_block_dep->block();
        }
    } else {
        const BlockData block_data = _block_queue.front();
        block->swap(*block_data.block);
        *find_block = true;
        _block_queue.pop_front();
        int before_block_queues_bytes = _all_block_queues_bytes->load();
        _all_block_queues_bytes->fetch_sub(block_data.block_bytes, std::memory_order_relaxed);
        VLOG_DEBUG << "[Group Commit Debug] (LoadBlockQueue::get_block). "
                   << "block queue size is " << _block_queue.size() << ", block rows is "
                   << block->rows() << ", block bytes is " << block->bytes()
                   << ", before remove block, all block queues bytes is "
                   << before_block_queues_bytes
                   << ", after remove block, all block queues bytes is "
                   << _all_block_queues_bytes->load() << ", txn_id=" << txn_id
                   << ", label=" << label << ", instance_id=" << load_instance_id
                   << ", load_ids=" << get_load_ids() << ", the block is " << block->dump_data()
                   << ", the block column size is " << block->columns_bytes();
    }
    if (_block_queue.empty() && _need_commit && _load_ids_to_write_dep.empty()) {
        *eos = true;
    } else {
        *eos = false;
    }
    if (_all_block_queues_bytes->load(std::memory_order_relaxed) <
        config::group_commit_queue_mem_limit) {
        for (auto& id : _load_ids_to_write_dep) {
            id.second->set_ready();
        }
    }
    return Status::OK();
}

Status LoadBlockQueue::remove_load_id(const UniqueId& load_id) {
    std::unique_lock l(mutex);
    if (_load_ids_to_write_dep.find(load_id) != _load_ids_to_write_dep.end()) {
        _load_ids_to_write_dep[load_id]->set_always_ready();
        _load_ids_to_write_dep.erase(load_id);
        for (auto read_dep : _read_deps) {
            read_dep->set_ready();
        }
        return Status::OK();
    }
    return Status::NotFound<false>("load_id=" + load_id.to_string() +
                                   " not in block queue, label=" + label);
}

bool LoadBlockQueue::contain_load_id(const UniqueId& load_id) {
    std::unique_lock l(mutex);
    return _load_ids_to_write_dep.find(load_id) != _load_ids_to_write_dep.end();
}

Status LoadBlockQueue::add_load_id(const UniqueId& load_id,
                                   const std::shared_ptr<pipeline::Dependency> put_block_dep) {
    std::unique_lock l(mutex);
    if (_need_commit) {
        return Status::InternalError<false>("block queue is set need commit, id=" +
                                            load_instance_id.to_string());
    }
    _load_ids_to_write_dep[load_id] = put_block_dep;
    group_commit_load_count.fetch_add(1);
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
    status =
            Status::Cancelled("cancel group_commit, label=" + label + ", status=" + st.to_string());
    while (!_block_queue.empty()) {
        const BlockData& block_data = _block_queue.front().block;
        int before_block_queues_bytes = _all_block_queues_bytes->load();
        _all_block_queues_bytes->fetch_sub(block_data.block_bytes, std::memory_order_relaxed);
        std::stringstream ss;
        ss << "[";
        for (const auto& id : _load_ids_to_write_dep) {
            ss << id.first.to_string() << ", ";
        }
        ss << "]";
        VLOG_DEBUG << "[Group Commit Debug] (LoadBlockQueue::_cancel_without_block). "
                   << "block queue size is " << _block_queue.size() << ", block rows is "
                   << block_data.block->rows() << ", block bytes is " << block_data.block->bytes()
                   << ", before remove block, all block queues bytes is "
                   << before_block_queues_bytes
                   << ", after remove block, all block queues bytes is "
                   << _all_block_queues_bytes->load() << ", txn_id=" << txn_id
                   << ", label=" << label << ", instance_id=" << load_instance_id
                   << ", load_ids=" << ss.str() << ", the block is "
                   << block_data.block->dump_data() << ", the block column size is "
                   << block_data.block->columns_bytes();
        _block_queue.pop_front();
    }
    for (auto& id : _load_ids_to_write_dep) {
        id.second->set_always_ready();
    }
    for (auto read_dep : _read_deps) {
        read_dep->set_ready();
    }
}

Status GroupCommitTable::get_first_block_load_queue(
        int64_t table_id, int64_t base_schema_version, const UniqueId& load_id,
        std::shared_ptr<LoadBlockQueue>& load_block_queue, int be_exe_version,
        std::shared_ptr<MemTrackerLimiter> mem_tracker,
        std::shared_ptr<pipeline::Dependency> create_plan_dep,
        std::shared_ptr<pipeline::Dependency> put_block_dep) {
    DCHECK(table_id == _table_id);
    std::unique_lock l(_lock);
    auto try_to_get_matched_queue = [&]() -> Status {
        for (const auto& [_, inner_block_queue] : _load_block_queues) {
            if (inner_block_queue->contain_load_id(load_id)) {
                load_block_queue = inner_block_queue;
                return Status::OK();
            }
        }
        for (const auto& [_, inner_block_queue] : _load_block_queues) {
            if (!inner_block_queue->need_commit()) {
                if (base_schema_version == inner_block_queue->schema_version) {
                    if (inner_block_queue->add_load_id(load_id, put_block_dep).ok()) {
                        load_block_queue = inner_block_queue;
                        return Status::OK();
                    }
                } else {
                    return Status::DataQualityError<false>(
                            "schema version not match, maybe a schema change is in process. "
                            "Please retry this load manually.");
                }
            }
        }
        return Status::InternalError<false>("can not get a block queue for table_id: " +
                                            std::to_string(_table_id));
    };

    if (try_to_get_matched_queue().ok()) {
        return Status::OK();
    }
    create_plan_dep->block();
    _create_plan_deps.emplace(load_id,
                              std::make_tuple(create_plan_dep, put_block_dep, base_schema_version));
    if (!_is_creating_plan_fragment) {
        _is_creating_plan_fragment = true;
        RETURN_IF_ERROR(
                _thread_pool->submit_func([&, be_exe_version, mem_tracker, dep = create_plan_dep] {
                    Defer defer {[&, dep = dep]() {
                        std::unique_lock l(_lock);
                        for (auto it : _create_plan_deps) {
                            std::get<0>(it.second)->set_ready();
                        }
                        _create_plan_deps.clear();
                        _is_creating_plan_fragment = false;
                    }};
                    auto st = _create_group_commit_load(be_exe_version, mem_tracker);
                    if (!st.ok()) {
                        LOG(WARNING) << "create group commit load error, st=" << st.to_string();
                    }
                }));
    }
    return try_to_get_matched_queue();
}

void GroupCommitTable::remove_load_id(const UniqueId& load_id) {
    std::unique_lock l(_lock);
    if (_create_plan_deps.find(load_id) != _create_plan_deps.end()) {
        _create_plan_deps.erase(load_id);
        return;
    }
    for (const auto& [_, inner_block_queue] : _load_block_queues) {
        if (inner_block_queue->remove_load_id(load_id).ok()) {
            return;
        }
    }
}

Status GroupCommitTable::_create_group_commit_load(int be_exe_version,
                                                   std::shared_ptr<MemTrackerLimiter> mem_tracker) {
    Status st = Status::OK();
    TStreamLoadPutResult result;
    std::string label;
    int64_t txn_id;
    TUniqueId instance_id;
    {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker);
        UniqueId load_id = UniqueId::gen_uid();
        TUniqueId tload_id;
        tload_id.__set_hi(load_id.hi);
        tload_id.__set_lo(load_id.lo);
        std::regex reg("-");
        label = "group_commit_" + std::regex_replace(load_id.to_string(), reg, "_");
        std::stringstream ss;
        ss << "insert into doris_internal_table_id(" << _table_id << ") WITH LABEL " << label
           << " select * from group_commit(\"table_id\"=\"" << _table_id << "\")";
        TStreamLoadPutRequest request;
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
        if (st.ok() && !result.__isset.pipeline_params) {
            st = Status::InternalError("Non-pipeline is disabled!");
        }
        if (!st.ok()) {
            LOG(WARNING) << "create group commit load error, st=" << st.to_string();
            return st;
        }
        auto schema_version = result.base_schema_version;
        auto& pipeline_params = result.pipeline_params;
        DCHECK(pipeline_params.fragment.output_sink.olap_table_sink.db_id == _db_id);
        txn_id = pipeline_params.txn_conf.txn_id;
        DCHECK(pipeline_params.local_params.size() == 1);
        instance_id = pipeline_params.local_params[0].fragment_instance_id;
        VLOG_DEBUG << "create plan fragment, db_id=" << _db_id << ", table=" << _table_id
                   << ", schema version=" << schema_version << ", label=" << label
                   << ", txn_id=" << txn_id << ", instance_id=" << print_id(instance_id);
        {
            auto load_block_queue = std::make_shared<LoadBlockQueue>(
                    instance_id, label, txn_id, schema_version, _all_block_queues_bytes,
                    result.wait_internal_group_commit_finish, result.group_commit_interval_ms,
                    result.group_commit_data_bytes);
            RETURN_IF_ERROR(load_block_queue->create_wal(
                    _db_id, _table_id, txn_id, label, _exec_env->wal_mgr(),
                    pipeline_params.fragment.output_sink.olap_table_sink.schema.slot_descs,
                    be_exe_version));

            std::unique_lock l(_lock);
            _load_block_queues.emplace(instance_id, load_block_queue);
            std::vector<UniqueId> success_load_ids;
            for (const auto& [id, load_info] : _create_plan_deps) {
                auto create_dep = std::get<0>(load_info);
                auto put_dep = std::get<1>(load_info);
                if (load_block_queue->schema_version == std::get<2>(load_info)) {
                    if (load_block_queue->add_load_id(id, put_dep).ok()) {
                        create_dep->set_ready();
                        success_load_ids.emplace_back(id);
                    }
                }
            }
            for (const auto& id : success_load_ids) {
                _create_plan_deps.erase(id);
            }
        }
    }
    st = _exec_plan_fragment(_db_id, _table_id, label, txn_id, result.pipeline_params);
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
    DBUG_EXECUTE_IF("LoadBlockQueue._finish_group_commit_load.load_error",
                    { status = Status::InternalError("load_error"); });
    if (status.ok()) {
        DBUG_EXECUTE_IF("LoadBlockQueue._finish_group_commit_load.commit_error",
                        { status = Status::InternalError(""); });
        // commit txn
        TLoadTxnCommitRequest request;
        request.__set_auth_code(0); // this is a fake, fe not check it now
        request.__set_db_id(db_id);
        request.__set_table_id(table_id);
        request.__set_txnId(txn_id);
        request.__set_thrift_rpc_timeout_ms(config::txn_commit_rpc_timeout_ms);
        request.__set_groupCommit(true);
        request.__set_receiveBytes(state->num_bytes_load_total());
        if (_exec_env->master_info()->__isset.backend_id) {
            request.__set_backendId(_exec_env->master_info()->backend_id);
        } else {
            LOG(WARNING) << "_exec_env->master_info not set backend_id";
        }
        if (state) {
            request.__set_commitInfos(state->tablet_commit_infos());
        }
        TLoadTxnCommitResult result;
        TNetworkAddress master_addr = _exec_env->master_info()->network_address;
        int retry_times = 0;
        while (retry_times < config::mow_stream_load_commit_retry_times) {
            st = ThriftRpcHelper::rpc<FrontendServiceClient>(
                    master_addr.hostname, master_addr.port,
                    [&request, &result](FrontendServiceConnection& client) {
                        client->loadTxnCommit(result, request);
                    },
                    config::txn_commit_rpc_timeout_ms);
            result_status = Status::create(result.status);
            // DELETE_BITMAP_LOCK_ERROR will be retried
            if (result_status.ok() || !result_status.is<ErrorCode::DELETE_BITMAP_LOCK_ERROR>()) {
                break;
            }
            LOG_WARNING("Failed to commit txn on group commit")
                    .tag("label", label)
                    .tag("txn_id", txn_id)
                    .tag("retry_times", retry_times)
                    .error(result_status);
            retry_times++;
        }
        DBUG_EXECUTE_IF("LoadBlockQueue._finish_group_commit_load.commit_success_and_rpc_error",
                        { result_status = Status::InternalError("commit_success_and_rpc_error"); });
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
                });
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
                for (auto dep : load_block_queue->dependencies) {
                    dep->set_always_ready();
                }
            }
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
        if (dp->param<int64_t>("table_id", -1) == table_id) {
            std ::string msg = _exec_env->wal_mgr()->get_wal_dirs_info_string();
            LOG(INFO) << "table_id" << std::to_string(table_id) << " set debug promise: " << msg;
            ExecEnv::GetInstance()->group_commit_mgr()->debug_promise.set_value(
                    Status ::InternalError(msg));
        }
    };);
    return st;
}

Status GroupCommitTable::_exec_plan_fragment(int64_t db_id, int64_t table_id,
                                             const std::string& label, int64_t txn_id,
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
    return _exec_env->fragment_mgr()->exec_plan_fragment(pipeline_params,
                                                         QuerySource::GROUP_COMMIT_LOAD, finish_cb);
}

Status GroupCommitTable::get_load_block_queue(const TUniqueId& instance_id,
                                              std::shared_ptr<LoadBlockQueue>& load_block_queue,
                                              std::shared_ptr<pipeline::Dependency> get_block_dep) {
    std::unique_lock l(_lock);
    auto it = _load_block_queues.find(instance_id);
    if (it == _load_block_queues.end()) {
        return Status::InternalError("group commit load instance " + print_id(instance_id) +
                                     " not found");
    }
    load_block_queue = it->second;
    load_block_queue->append_read_dependency(get_block_dep);
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
        int64_t db_id, int64_t table_id, int64_t base_schema_version, const UniqueId& load_id,
        std::shared_ptr<LoadBlockQueue>& load_block_queue, int be_exe_version,
        std::shared_ptr<MemTrackerLimiter> mem_tracker,
        std::shared_ptr<pipeline::Dependency> create_plan_dep,
        std::shared_ptr<pipeline::Dependency> put_block_dep) {
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
            table_id, base_schema_version, load_id, load_block_queue, be_exe_version, mem_tracker,
            create_plan_dep, put_block_dep));
    return Status::OK();
}

Status GroupCommitMgr::get_load_block_queue(int64_t table_id, const TUniqueId& instance_id,
                                            std::shared_ptr<LoadBlockQueue>& load_block_queue,
                                            std::shared_ptr<pipeline::Dependency> get_block_dep) {
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
    return group_commit_table->get_load_block_queue(instance_id, load_block_queue, get_block_dep);
}

void GroupCommitMgr::remove_load_id(int64_t table_id, const UniqueId& load_id) {
    std::lock_guard wlock(_lock);
    if (_table_map.find(table_id) != _table_map.end()) {
        _table_map.find(table_id)->second->remove_load_id(load_id);
    }
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

void LoadBlockQueue::append_dependency(std::shared_ptr<pipeline::Dependency> finish_dep) {
    std::lock_guard<std::mutex> lock(mutex);
    // If not finished, dependencies should be blocked.
    if (!process_finish) {
        finish_dep->block();
        dependencies.push_back(finish_dep);
    }
}

void LoadBlockQueue::append_read_dependency(std::shared_ptr<pipeline::Dependency> read_dep) {
    std::lock_guard<std::mutex> lock(mutex);
    _read_deps.push_back(read_dep);
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
