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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/HeartbeatService.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>

#include "client_cache.h"
#include "common/object_pool.h"
#include "exec/data_sink.h"
#include "io/fs/stream_load_pipe.h"
#include "olap/wal_manager.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/thrift_rpc_helper.h"
#include "vec/exec/scan/new_file_scan_node.h"

namespace doris {

class TPlan;
class FragmentExecState;

Status LoadBlockQueue::add_block(std::shared_ptr<vectorized::FutureBlock> block) {
    DCHECK(block->get_schema_version() == schema_version);
    std::unique_lock l(*_mutex);
    RETURN_IF_ERROR(_status);
    if (block->rows() > 0) {
        _block_queue.push_back(block);
    }
    if (block->is_eos()) {
        _load_ids.erase(block->get_load_id());
    } else if (block->is_first()) {
        _load_ids.emplace(block->get_load_id());
    }
    _cv->notify_one();
    return Status::OK();
}

Status LoadBlockQueue::get_block(vectorized::Block* block, bool* find_block, bool* eos) {
    *find_block = false;
    *eos = false;
    std::unique_lock l(*_mutex);
    if (!need_commit) {
        auto left_seconds = 10 - std::chrono::duration_cast<std::chrono::seconds>(
                                         std::chrono::steady_clock::now() - _start_time)
                                         .count();
        if (left_seconds <= 0) {
            need_commit = true;
        }
    }
    while (_status.ok() && _block_queue.empty() &&
           (!need_commit || (need_commit && !_load_ids.empty()))) {
        // TODO make 10s as a config
        auto left_seconds = 10;
        if (!need_commit) {
            left_seconds = 10 - std::chrono::duration_cast<std::chrono::seconds>(
                                        std::chrono::steady_clock::now() - _start_time)
                                        .count();
            if (left_seconds <= 0) {
                need_commit = true;
                break;
            }
        }
#if !defined(USE_BTHREAD_SCANNER)
        _cv->wait_for(l, std::chrono::seconds(left_seconds));
#else
        _cv->wait_for(l, left_seconds * 1000000);
#endif
    }
    if (!_block_queue.empty()) {
        auto& future_block = _block_queue.front();
        auto* fblock = static_cast<vectorized::FutureBlock*>(block);
        fblock->swap_future_block(future_block);
        *find_block = true;
        _block_queue.pop_front();
    }
    if (_block_queue.empty()) {
        if (need_commit && _load_ids.empty()) {
            *eos = true;
        } else {
            *eos = false;
        }
    }
    return Status::OK();
}

void LoadBlockQueue::remove_load_id(const UniqueId& load_id) {
    std::unique_lock l(*_mutex);
    if (_load_ids.find(load_id) != _load_ids.end()) {
        _load_ids.erase(load_id);
        _cv->notify_one();
    }
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
            future_block->cv->notify_all();
        }
        _block_queue.pop_front();
    }
}

Status GroupCommitTable::get_first_block_load_queue(
        int64_t table_id, std::shared_ptr<vectorized::FutureBlock> block,
        std::shared_ptr<LoadBlockQueue>& load_block_queue) {
    DCHECK(table_id == _table_id);
    DCHECK(block->is_first() == true);
    {
        std::unique_lock l(_lock);
        for (auto it = _load_block_queues.begin(); it != _load_block_queues.end(); ++it) {
            // TODO if block schema version is less than fragment schema version, return error
            if (!it->second->need_commit &&
                it->second->schema_version == block->get_schema_version()) {
                if (block->get_schema_version() == it->second->schema_version) {
                    load_block_queue = it->second;
                    break;
                } else if (block->get_schema_version() < it->second->schema_version) {
                    return Status::DataQualityError("schema version not match");
                }
            }
        }
    }
    if (load_block_queue == nullptr) {
        Status st = Status::OK();
        for (int i = 0; i < 3; ++i) {
            std::unique_lock l(_request_fragment_mutex);
            // check if there is a re-usefully fragment
            {
                std::unique_lock l1(_lock);
                for (auto it = _load_block_queues.begin(); it != _load_block_queues.end(); ++it) {
                    // TODO if block schema version is less than fragment schema version, return error
                    if (!it->second->need_commit) {
                        if (block->get_schema_version() == it->second->schema_version) {
                            load_block_queue = it->second;
                            break;
                        } else if (block->get_schema_version() < it->second->schema_version) {
                            return Status::DataQualityError("schema version not match");
                        }
                    }
                }
            }
            if (load_block_queue == nullptr) {
                st = _create_group_commit_load(table_id, load_block_queue);
                if (LIKELY(st.ok())) {
                    break;
                }
            }
        }
        RETURN_IF_ERROR(st);
        if (load_block_queue->schema_version != block->get_schema_version()) {
            // TODO check this is the first block
            return Status::DataQualityError("schema version not match");
        }
    }
    return Status::OK();
}

Status GroupCommitTable::_create_group_commit_load(
        int64_t table_id, std::shared_ptr<LoadBlockQueue>& load_block_queue) {
    TStreamLoadPutRequest request;
    UniqueId load_id = UniqueId::gen_uid();
    TUniqueId tload_id;
    tload_id.__set_hi(load_id.hi);
    tload_id.__set_lo(load_id.lo);
    std::regex reg("-");
    std::string label = "group_commit_" + std::regex_replace(load_id.to_string(), reg, "_");
    std::stringstream ss;
    ss << "insert into " << table_id << " WITH LABEL " << label
       << " select * from group_commit(\"table_id\"=\"" << table_id << "\")";
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
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&result, &request](FrontendServiceConnection& client) {
                client->streamLoadPut(result, request);
            },
            10000L));
    Status st = Status::create(result.status);
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
    VLOG_DEBUG << "create plan fragment, db_id=" << _db_id << ", table=" << table_id
               << ", schema version=" << schema_version << ", label=" << label
               << ", txn_id=" << txn_id << ", instance_id=" << print_id(instance_id)
               << ", is_pipeline=" << is_pipeline;
    {
        load_block_queue =
                std::make_shared<LoadBlockQueue>(instance_id, label, txn_id, schema_version);
        std::unique_lock l(_lock);
        _load_block_queues.emplace(instance_id, load_block_queue);
    }
    params.__set_import_label(label);
    st = _exec_plan_fragment(_db_id, table_id, label, txn_id, is_pipeline, params, pipeline_params);
    if (!st.ok()) {
        _finish_group_commit_load(_db_id, table_id, label, txn_id, instance_id, st, true, nullptr);
    }
    return st;
}

Status GroupCommitTable::_finish_group_commit_load(int64_t db_id, int64_t table_id,
                                                   const std::string& label, int64_t txn_id,
                                                   const TUniqueId& instance_id, Status& status,
                                                   bool prepare_failed, RuntimeState* state) {
    {
        std::lock_guard<doris::Mutex> l(_lock);
        if (prepare_failed) {
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
    LOG(INFO) << ss.str();
    return st;
}

Status GroupCommitTable::_exec_plan_fragment(int64_t db_id, int64_t table_id,
                                             const std::string& label, int64_t txn_id,
                                             bool is_pipeline,
                                             const TExecPlanFragmentParams& params,
                                             const TPipelineFragmentParams& pipeline_params) {
    auto finish_cb = [db_id, table_id, label, txn_id, this](RuntimeState* state, Status* status) {
        _finish_group_commit_load(db_id, table_id, label, txn_id, state->fragment_instance_id(),
                                  *status, false, state);
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
    ThreadPoolBuilder("InsertIntoGroupCommitThreadPool")
            .set_min_threads(config::group_commit_insert_threads)
            .set_max_threads(config::group_commit_insert_threads)
            .build(&_insert_into_thread_pool);
}

GroupCommitMgr::~GroupCommitMgr() {
    LOG(INFO) << "GroupCommitMgr is destoried";
}

void GroupCommitMgr::stop() {
    _insert_into_thread_pool->shutdown();
    LOG(INFO) << "GroupCommitMgr is stopped";
}

Status GroupCommitMgr::group_commit_insert(int64_t table_id, const TPlan& plan,
                                           const TDescriptorTable& tdesc_tbl,
                                           const TScanRangeParams& scan_range_params,
                                           const PGroupCommitInsertRequest* request,
                                           PGroupCommitInsertResponse* response) {
    auto& nodes = plan.nodes;
    DCHECK(nodes.size() > 0);
    auto& plan_node = nodes.at(0);

    TUniqueId load_id;
    load_id.__set_hi(request->load_id().hi());
    load_id.__set_lo(request->load_id().lo());

    std::vector<std::shared_ptr<doris::vectorized::FutureBlock>> future_blocks;
    {
        // 1. Prepare a pipe, then append rows to pipe,
        // then scan node scans from the pipe, like stream load.
        std::shared_ptr<LoadBlockQueue> load_block_queue;
        auto pipe = std::make_shared<io::StreamLoadPipe>(
                io::kMaxPipeBufferedBytes /* max_buffered_bytes */, 64 * 1024 /* min_chunk_size */,
                -1 /* total_length */, true /* use_proto */);
        std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
        ctx->pipe = pipe;
        RETURN_IF_ERROR(_exec_env->new_load_stream_mgr()->put(load_id, ctx));
        std::unique_ptr<int, std::function<void(int*)>> remove_pipe_func((int*)0x01, [&](int*) {
            if (load_block_queue != nullptr) {
                load_block_queue->remove_load_id(load_id);
            }
            _exec_env->new_load_stream_mgr()->remove(load_id);
        });
        _insert_into_thread_pool->submit_func(
                std::bind<void>(&GroupCommitMgr::_append_row, this, pipe, request));

        // 2. FileScanNode consumes data from the pipe.
        std::unique_ptr<RuntimeState> runtime_state = RuntimeState::create_unique();
        TQueryOptions query_options;
        query_options.query_type = TQueryType::LOAD;
        TQueryGlobals query_globals;
        runtime_state->init(load_id, query_options, query_globals, _exec_env);
        runtime_state->set_query_mem_tracker(std::make_shared<MemTrackerLimiter>(
                MemTrackerLimiter::Type::LOAD, fmt::format("Load#Id={}", print_id(load_id)), -1));
        DescriptorTbl* desc_tbl = nullptr;
        RETURN_IF_ERROR(DescriptorTbl::create(runtime_state->obj_pool(), tdesc_tbl, &desc_tbl));
        runtime_state->set_desc_tbl(desc_tbl);
        auto file_scan_node =
                vectorized::NewFileScanNode(runtime_state->obj_pool(), plan_node, *desc_tbl);
        std::unique_ptr<int, std::function<void(int*)>> close_scan_node_func(
                (int*)0x01, [&](int*) { file_scan_node.close(runtime_state.get()); });
        // TFileFormatType::FORMAT_PROTO, TFileType::FILE_STREAM, set _range.load_id
        RETURN_IF_ERROR(file_scan_node.init(plan_node, runtime_state.get()));
        RETURN_IF_ERROR(file_scan_node.prepare(runtime_state.get()));
        std::vector<TScanRangeParams> params_vector;
        params_vector.emplace_back(scan_range_params);
        file_scan_node.set_scan_ranges(params_vector);
        RETURN_IF_ERROR(file_scan_node.open(runtime_state.get()));

        // 3. Put the block into block queue.
        std::unique_ptr<doris::vectorized::Block> _block =
                doris::vectorized::Block::create_unique();
        bool eof = false;
        bool first = true;
        while (!eof) {
            // TODO what to do if read one block error
            RETURN_IF_ERROR(file_scan_node.get_next(runtime_state.get(), _block.get(), &eof));
            std::shared_ptr<doris::vectorized::FutureBlock> future_block =
                    std::make_shared<doris::vectorized::FutureBlock>();
            future_block->swap(*(_block.get()));
            future_block->set_info(request->base_schema_version(), load_id, first, eof);
            if (load_block_queue == nullptr) {
                RETURN_IF_ERROR(_get_first_block_load_queue(request->db_id(), table_id,
                                                            future_block, load_block_queue));
                response->set_label(load_block_queue->label);
                response->set_txn_id(load_block_queue->txn_id);
            }
            // TODO what to do if add one block error
            RETURN_IF_ERROR(load_block_queue->add_block(future_block));
            if (future_block->rows() > 0) {
                future_blocks.emplace_back(future_block);
            }
            first = false;
        }
        if (!runtime_state->get_error_log_file_path().empty()) {
            LOG(INFO) << "id=" << print_id(load_id)
                      << ", url=" << runtime_state->get_error_log_file_path()
                      << ", load rows=" << runtime_state->num_rows_load_total()
                      << ", filter rows=" << runtime_state->num_rows_load_filtered()
                      << ", unselect rows=" << runtime_state->num_rows_load_unselected()
                      << ", success rows=" << runtime_state->num_rows_load_success();
        }
    }
    int64_t total_rows = 0;
    int64_t loaded_rows = 0;
    // 4. wait to wal
    for (const auto& future_block : future_blocks) {
        std::unique_lock<doris::Mutex> l(*(future_block->lock));
        if (!future_block->is_handled()) {
            future_block->cv->wait(l);
        }
        // future_block->get_status()
        total_rows += future_block->get_total_rows();
        loaded_rows += future_block->get_loaded_rows();
    }
    response->set_loaded_rows(loaded_rows);
    response->set_filtered_rows(total_rows - loaded_rows);
    return Status::OK();
}

Status GroupCommitMgr::_append_row(std::shared_ptr<io::StreamLoadPipe> pipe,
                                   const PGroupCommitInsertRequest* request) {
    for (int i = 0; i < request->data().size(); ++i) {
        std::unique_ptr<PDataRow> row(new PDataRow());
        row->CopyFrom(request->data(i));
        // TODO append may error when pipe is cancelled
        RETURN_IF_ERROR(pipe->append(std::move(row)));
    }
    pipe->finish();
    return Status::OK();
}

Status GroupCommitMgr::_get_first_block_load_queue(
        int64_t db_id, int64_t table_id, std::shared_ptr<vectorized::FutureBlock> block,
        std::shared_ptr<LoadBlockQueue>& load_block_queue) {
    std::shared_ptr<GroupCommitTable> group_commit_table;
    {
        std::lock_guard wlock(_lock);
        if (_table_map.find(table_id) == _table_map.end()) {
            _table_map.emplace(table_id,
                               std::make_shared<GroupCommitTable>(_exec_env, db_id, table_id));
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