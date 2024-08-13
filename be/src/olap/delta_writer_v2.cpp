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

#include "olap/delta_writer_v2.h"

#include <brpc/controller.h>
#include <butil/errno.h>
#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <filesystem>
#include <ostream>
#include <string>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "gutil/integral_types.h"
#include "gutil/strings/numbers.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer_v2.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "service/backend_options.h"
#include "util/brpc_client_cache.h"
#include "util/debug_points.h"
#include "util/mem_info.h"
#include "util/ref_count_closure.h"
#include "util/stopwatch.hpp"
#include "util/time.h"
#include "vec/core/block.h"
#include "vec/sink/load_stream_stub.h"

namespace doris {
using namespace ErrorCode;

DeltaWriterV2::DeltaWriterV2(WriteRequest* req,
                             const std::vector<std::shared_ptr<LoadStreamStub>>& streams,
                             RuntimeState* state)
        : _state(state),
          _req(*req),
          _tablet_schema(new TabletSchema),
          _memtable_writer(new MemTableWriter(*req)),
          _streams(streams) {}

void DeltaWriterV2::_update_profile(RuntimeProfile* profile) {
    auto child = profile->create_child(fmt::format("DeltaWriterV2 {}", _req.tablet_id), true, true);
    auto write_memtable_timer = ADD_TIMER(child, "WriteMemTableTime");
    auto wait_flush_limit_timer = ADD_TIMER(child, "WaitFlushLimitTime");
    auto close_wait_timer = ADD_TIMER(child, "CloseWaitTime");
    COUNTER_SET(write_memtable_timer, _write_memtable_time);
    COUNTER_SET(wait_flush_limit_timer, _wait_flush_limit_time);
    COUNTER_SET(close_wait_timer, _close_wait_time);
}

DeltaWriterV2::~DeltaWriterV2() {
    if (!_is_init) {
        return;
    }

    // cancel and wait all memtables in flush queue to be finished
    static_cast<void>(_memtable_writer->cancel());
}

Status DeltaWriterV2::init() {
    if (_is_init) {
        return Status::OK();
    }
    // build tablet schema in request level
    DBUG_EXECUTE_IF("DeltaWriterV2.init.stream_size", { _streams.clear(); });
    if (_streams.size() == 0 || _streams[0]->tablet_schema(_req.index_id) == nullptr) {
        return Status::InternalError("failed to find tablet schema for {}", _req.index_id);
    }
    _build_current_tablet_schema(_req.index_id, _req.table_schema_param.get(),
                                 *_streams[0]->tablet_schema(_req.index_id));
    RowsetWriterContext context;
    context.txn_id = _req.txn_id;
    context.load_id = _req.load_id;
    context.index_id = _req.index_id;
    context.partition_id = _req.partition_id;
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAPPING;
    context.tablet_schema = _tablet_schema;
    context.newest_write_timestamp = UnixSeconds();
    context.tablet = nullptr;
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.tablet_id = _req.tablet_id;
    context.partition_id = _req.partition_id;
    context.tablet_schema_hash = _req.schema_hash;
    context.enable_unique_key_merge_on_write = _streams[0]->enable_unique_mow(_req.index_id);
    context.rowset_type = RowsetTypePB::BETA_ROWSET;
    context.rowset_id = ExecEnv::GetInstance()->storage_engine().next_rowset_id();
    context.data_dir = nullptr;
    context.partial_update_info = _partial_update_info;
    context.memtable_on_sink_support_index_v2 = true;

    _rowset_writer = std::make_shared<BetaRowsetWriterV2>(_streams);
    RETURN_IF_ERROR(_rowset_writer->init(context));
    ThreadPool* wg_thread_pool_ptr = nullptr;
    if (_state->get_query_ctx()) {
        wg_thread_pool_ptr = _state->get_query_ctx()->get_memtable_flush_pool();
    }
    RETURN_IF_ERROR(_memtable_writer->init(_rowset_writer, _tablet_schema, _partial_update_info,
                                           wg_thread_pool_ptr,
                                           _streams[0]->enable_unique_mow(_req.index_id)));
    ExecEnv::GetInstance()->memtable_memory_limiter()->register_writer(_memtable_writer);
    _is_init = true;
    _streams.clear();
    return Status::OK();
}

Status DeltaWriterV2::write(const vectorized::Block* block, const std::vector<uint32_t>& row_idxs) {
    if (UNLIKELY(row_idxs.empty())) {
        return Status::OK();
    }
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (!_is_init && !_is_cancelled) {
        RETURN_IF_ERROR(init());
    }
    {
        SCOPED_RAW_TIMER(&_wait_flush_limit_time);
        auto memtable_flush_running_count_limit = config::memtable_flush_running_count_limit;
        DBUG_EXECUTE_IF("DeltaWriterV2.write.back_pressure",
                        { memtable_flush_running_count_limit = 0; });
        while (_memtable_writer->flush_running_count() >= memtable_flush_running_count_limit) {
            if (_state->is_cancelled()) {
                return _state->cancel_reason();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    SCOPED_RAW_TIMER(&_write_memtable_time);
    return _memtable_writer->write(block, row_idxs);
}

Status DeltaWriterV2::close() {
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (!_is_init && !_is_cancelled) {
        // if this delta writer is not initialized, but close() is called.
        // which means this tablet has no data loaded, but at least one tablet
        // in same partition has data loaded.
        // so we have to also init this DeltaWriterV2, so that it can create an empty rowset
        // for this tablet when being closed.
        RETURN_IF_ERROR(init());
    }
    return _memtable_writer->close();
}

Status DeltaWriterV2::close_wait(int32_t& num_segments, RuntimeProfile* profile) {
    SCOPED_RAW_TIMER(&_close_wait_time);
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init)
            << "delta writer is supposed be to initialized before close_wait() being called";

    if (profile != nullptr) {
        _update_profile(profile);
    }
    RETURN_IF_ERROR(_memtable_writer->close_wait(profile));
    num_segments = _rowset_writer->next_segment_id();

    _delta_written_success = true;
    return Status::OK();
}

Status DeltaWriterV2::cancel() {
    return cancel_with_status(Status::Cancelled("already cancelled"));
}

Status DeltaWriterV2::cancel_with_status(const Status& st) {
    std::lock_guard<std::mutex> l(_lock);
    if (_is_cancelled) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_memtable_writer->cancel_with_status(st));
    _is_cancelled = true;
    return Status::OK();
}

void DeltaWriterV2::_build_current_tablet_schema(int64_t index_id,
                                                 const OlapTableSchemaParam* table_schema_param,
                                                 const TabletSchema& ori_tablet_schema) {
    _tablet_schema->copy_from(ori_tablet_schema);
    // find the right index id
    int i = 0;
    auto indexes = table_schema_param->indexes();
    for (; i < indexes.size(); i++) {
        if (indexes[i]->index_id == index_id) {
            break;
        }
    }

    if (!indexes.empty() && !indexes[i]->columns.empty() &&
        indexes[i]->columns[0]->unique_id() >= 0) {
        _tablet_schema->build_current_tablet_schema(index_id, table_schema_param->version(),
                                                    indexes[i], ori_tablet_schema);
    }

    _tablet_schema->set_table_id(table_schema_param->table_id());
    _tablet_schema->set_db_id(table_schema_param->db_id());
    if (table_schema_param->is_partial_update()) {
        _tablet_schema->set_auto_increment_column(table_schema_param->auto_increment_coulumn());
    }
    // set partial update columns info
    _partial_update_info = std::make_shared<PartialUpdateInfo>();
    _partial_update_info->init(*_tablet_schema, table_schema_param->is_partial_update(),
                               table_schema_param->partial_update_input_columns(),
                               table_schema_param->is_strict_mode(),
                               table_schema_param->timestamp_ms(), table_schema_param->timezone(),
                               table_schema_param->auto_increment_coulumn());
}

} // namespace doris
