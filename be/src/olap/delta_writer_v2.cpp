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

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
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
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "util/brpc_client_cache.h"
#include "util/mem_info.h"
#include "util/ref_count_closure.h"
#include "util/stopwatch.hpp"
#include "util/time.h"
#include "vec/core/block.h"
#include "vec/sink/load_stream_stub.h"

namespace doris {
using namespace ErrorCode;

Status DeltaWriterV2::open(WriteRequest* req,
                           const std::vector<std::shared_ptr<LoadStreamStub>>& streams,
                           DeltaWriterV2** writer, RuntimeProfile* profile) {
    *writer = new DeltaWriterV2(req, streams, StorageEngine::instance(), profile);
    return Status::OK();
}

DeltaWriterV2::DeltaWriterV2(WriteRequest* req,
                             const std::vector<std::shared_ptr<LoadStreamStub>>& streams,
                             StorageEngine* storage_engine, RuntimeProfile* profile)
        : _req(*req),
          _tablet_schema(new TabletSchema),
          _profile(profile->create_child(fmt::format("DeltaWriterV2 {}", _req.tablet_id), true,
                                         true)),
          _memtable_writer(new MemTableWriter(*req)),
          _streams(streams) {
    _init_profile(profile);
}

void DeltaWriterV2::_init_profile(RuntimeProfile* profile) {
    _write_memtable_timer = ADD_TIMER(_profile, "WriteMemTableTime");
    _close_wait_timer = ADD_TIMER(_profile, "CloseWaitTime");
}

DeltaWriterV2::~DeltaWriterV2() {
    if (!_is_init) {
        return;
    }

    // cancel and wait all memtables in flush queue to be finished
    _memtable_writer->cancel();
}

Status DeltaWriterV2::init() {
    if (_is_init) {
        return Status::OK();
    }
    // build tablet schema in request level
    _build_current_tablet_schema(_req.index_id, _req.table_schema_param,
                                 *_streams[0]->tablet_schema(_req.index_id));
    RowsetWriterContext context;
    context.txn_id = _req.txn_id;
    context.load_id = _req.load_id;
    context.index_id = _req.index_id;
    context.partition_id = _req.partition_id;
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAPPING;
    context.tablet_schema = _tablet_schema;
    context.original_tablet_schema = _tablet_schema;
    context.newest_write_timestamp = UnixSeconds();
    context.tablet = nullptr;
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.tablet_id = _req.tablet_id;
    context.partition_id = _req.partition_id;
    context.tablet_schema_hash = _req.schema_hash;
    context.enable_unique_key_merge_on_write = _streams[0]->enable_unique_mow(_req.index_id);
    context.rowset_type = RowsetTypePB::BETA_ROWSET;
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.data_dir = nullptr;

    _rowset_writer = std::make_shared<BetaRowsetWriterV2>(_streams);
    _rowset_writer->init(context);
    _memtable_writer->init(_rowset_writer, _tablet_schema,
                           _streams[0]->enable_unique_mow(_req.index_id));
    ExecEnv::GetInstance()->memtable_memory_limiter()->register_writer(_memtable_writer);
    _is_init = true;
    _streams.clear();
    return Status::OK();
}

Status DeltaWriterV2::append(const vectorized::Block* block) {
    return write(block, {}, true);
}

Status DeltaWriterV2::write(const vectorized::Block* block, const std::vector<int>& row_idxs,
                            bool is_append) {
    if (UNLIKELY(row_idxs.empty() && !is_append)) {
        return Status::OK();
    }
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (!_is_init && !_is_cancelled) {
        RETURN_IF_ERROR(init());
    }
    SCOPED_TIMER(_write_memtable_timer);
    return _memtable_writer->write(block, row_idxs, is_append);
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

Status DeltaWriterV2::close_wait() {
    SCOPED_TIMER(_close_wait_timer);
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init)
            << "delta writer is supposed be to initialized before close_wait() being called";

    RETURN_IF_ERROR(_memtable_writer->close_wait(_profile));

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

int64_t DeltaWriterV2::mem_consumption(MemType mem) {
    return _memtable_writer->mem_consumption(mem);
}

int64_t DeltaWriterV2::active_memtable_mem_consumption() {
    return _memtable_writer->active_memtable_mem_consumption();
}

int64_t DeltaWriterV2::partition_id() const {
    return _req.partition_id;
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

    if (indexes.size() > 0 && indexes[i]->columns.size() != 0 &&
        indexes[i]->columns[0]->unique_id() >= 0) {
        _tablet_schema->build_current_tablet_schema(index_id, table_schema_param->version(),
                                                    indexes[i], ori_tablet_schema);
    }

    _tablet_schema->set_table_id(table_schema_param->table_id());
    // set partial update columns info
    _tablet_schema->set_partial_update_info(table_schema_param->is_partial_update(),
                                            table_schema_param->partial_update_input_columns());
}

} // namespace doris
