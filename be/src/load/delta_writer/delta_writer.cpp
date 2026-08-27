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

#include "load/delta_writer/delta_writer.h"

#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <ostream>
#include <string>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/block/block.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "load/memtable/memtable_flush_executor.h"
#include "load/memtable/memtable_memory_limiter.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/olap_define.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset_builder.h"
#include "storage/schema_change/schema_change.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet_info.h"
#include "util/mem_info.h"
#include "util/stopwatch.hpp"
#include "util/time.h"

namespace doris {
using namespace ErrorCode;

BaseDeltaWriter::BaseDeltaWriter(const WriteRequest& req, RuntimeProfile* profile,
                                 const UniqueId& load_id)
        : _req(req), _memtable_writer(new MemTableWriter(req)) {
    if (profile != nullptr) {
        _init_profile(profile);
    }
}

DeltaWriter::DeltaWriter(StorageEngine& engine, const WriteRequest& req, RuntimeProfile* profile,
                         const UniqueId& load_id)
        : BaseDeltaWriter(req, profile, load_id), _engine(engine) {
    DCHECK(req.write_req_type == WriteRequestType::DATA);
    _rowset_builder = std::make_unique<RowsetBuilder>(_engine, req, profile);
}

DeltaWriter::DeltaWriter(StorageEngine& engine, const WriteRequest& group_build_req,
                         const WriteRequest& sub_data_req, const WriteRequest& sub_row_binlog_req,
                         RuntimeProfile* profile, const UniqueId& load_id)
        : BaseDeltaWriter(group_build_req, profile, load_id), _engine(engine) {
    DCHECK(group_build_req.write_req_type == WriteRequestType::GROUP &&
           sub_data_req.write_req_type == WriteRequestType::DATA &&
           sub_row_binlog_req.write_req_type == WriteRequestType::ROW_BINLOG);
    _rowset_builder = std::make_unique<GroupRowsetBuilder>(_engine, group_build_req, sub_data_req,
                                                           sub_row_binlog_req, profile);
}

void BaseDeltaWriter::_init_profile(RuntimeProfile* profile) {
    DCHECK(profile != nullptr);
    _profile = profile->create_child(fmt::format("DeltaWriter {}", _req.tablet_id), true, true);
    _close_wait_timer = ADD_TIMER(_profile, "CloseWaitTime");
    _wait_flush_limit_timer = ADD_TIMER(_profile, "WaitFlushLimitTime");
}

void DeltaWriter::_init_profile(RuntimeProfile* profile) {
    DCHECK(profile != nullptr);
    BaseDeltaWriter::_init_profile(profile);
    _commit_txn_timer = ADD_TIMER(_profile, "CommitTxnTime");
}

BaseDeltaWriter::~BaseDeltaWriter() {
    if (!_is_init) {
        return;
    }

    // cancel and wait all memtables in flush queue to be finished
    static_cast<void>(_memtable_writer->cancel());

    if (_rowset_builder->tablet() != nullptr) {
        const FlushStatistic& stat = _memtable_writer->get_flush_token_stats();
        _rowset_builder->tablet()->flush_bytes->increment(stat.flush_size_bytes);
        _rowset_builder->tablet()->flush_finish_count->increment(stat.flush_finish_count);
    }
}

void BaseDeltaWriter::collect_tablet_load_rowset_num_info(
        BaseTablet* tablet,
        google::protobuf::RepeatedPtrField<PTabletLoadRowsetInfo>* tablet_infos) {
    if (tablet == nullptr) {
        return;
    }
    auto max_version_config = tablet->max_version_config();
    if (auto version_cnt = tablet->tablet_meta()->version_count();
        UNLIKELY(version_cnt >
                 (max_version_config * config::load_back_pressure_version_threshold / 100))) {
        auto* load_info = tablet_infos->Add();
        load_info->set_current_rowset_nums(static_cast<int32_t>(version_cnt));
        load_info->set_max_config_rowset_nums(max_version_config);
    }
}

void BaseDeltaWriter::set_tablet_load_rowset_num_info(
        google::protobuf::RepeatedPtrField<PTabletLoadRowsetInfo>* tablet_infos) {
    auto* tablet = _rowset_builder->tablet().get();
    collect_tablet_load_rowset_num_info(tablet, tablet_infos);
}

int64_t BaseDeltaWriter::table_id() const {
    DORIS_CHECK(_req.table_schema_param != nullptr);
    return _req.table_schema_param->table_id();
}

DeltaWriter::~DeltaWriter() = default;

Status BaseDeltaWriter::init() {
    if (_is_init) {
        return Status::OK();
    }
    std::shared_ptr<WorkloadGroup> wg_sptr = nullptr;
    if (doris::thread_context()->is_attach_task()) {
        wg_sptr = doris::thread_context()->resource_ctx()->workload_group();
    }
    RETURN_IF_ERROR(_rowset_builder->init());
    RETURN_IF_ERROR(_memtable_writer->init(
            _rowset_builder->rowset_writer(), _rowset_builder->tablet_schema(),
            _rowset_builder->get_partial_update_info(), wg_sptr,
            _rowset_builder->tablet_sptr()->enable_unique_key_merge_on_write()));
    ExecEnv::GetInstance()->memtable_memory_limiter()->register_writer(_memtable_writer);
    _is_init = true;
    return Status::OK();
}

Status DeltaWriter::write(const Block* block, const TabletAddRowsPayload& rows,
                          bool* memtable_flushed) {
    if (memtable_flushed != nullptr) {
        *memtable_flushed = false;
    }
    if (UNLIKELY(rows.row_idxs.empty())) {
        return Status::OK();
    }
    if (_req.enable_table_memtable_backpressure && !_req.is_high_priority) {
        ExecEnv::GetInstance()->memtable_memory_limiter()->handle_table_memtable_backpressure(
                [this]() {
                    std::lock_guard<std::mutex> l(_lock);
                    return _is_cancelled;
                },
                table_id());
    }
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (!_is_init && !_is_cancelled) {
        RETURN_IF_ERROR(init());
    }
    {
        SCOPED_TIMER(_wait_flush_limit_timer);
        const auto effective_flush_running_count_limit =
                config::memtable_flush_running_count_limit *
                (_req.write_req_type == WriteRequestType::GROUP ? 2 : 1);
        while (_memtable_writer->flush_running_count() >= effective_flush_running_count_limit) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    return _memtable_writer->write(block, rows, memtable_flushed);
}

Status BaseDeltaWriter::wait_flush() {
    return _memtable_writer->wait_flush();
}

Status BaseDeltaWriter::flush_memtable_async() {
    return _memtable_writer->flush_async();
}

Status DeltaWriter::flush_memtable_async() {
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    return BaseDeltaWriter::flush_memtable_async();
}

Status DeltaWriter::close() {
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (!_is_init && !_is_cancelled) {
        // if this delta writer is not initialized, but close() is called.
        // which means this tablet has no data loaded, but at least one tablet
        // in same partition has data loaded.
        // so we have to also init this DeltaWriter, so that it can create an empty rowset
        // for this tablet when being closed.
        RETURN_IF_ERROR(init());
    }
    return _memtable_writer->close();
}

Status BaseDeltaWriter::build_rowset() {
    SCOPED_TIMER(_close_wait_timer);
    RETURN_IF_ERROR(_memtable_writer->close_wait(_profile));
    return _rowset_builder->build_rowset();
}

Status DeltaWriter::build_rowset() {
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init)
            << "delta writer is supposed be to initialized before build_rowset() being called";
    return BaseDeltaWriter::build_rowset();
}

Status BaseDeltaWriter::submit_calc_delete_bitmap_task() {
    return _rowset_builder->submit_calc_delete_bitmap_task();
}

Status BaseDeltaWriter::wait_calc_delete_bitmap() {
    return _rowset_builder->wait_calc_delete_bitmap();
}

Status DeltaWriter::commit_txn() {
    std::lock_guard<std::mutex> l(_lock);
    SCOPED_TIMER(_commit_txn_timer);
    return _rowset_builder->commit_txn();
}

Status BaseDeltaWriter::cancel() {
    return cancel_with_status(Status::Cancelled("already cancelled"));
}

Status BaseDeltaWriter::cancel_with_status(const Status& st) {
    if (_is_cancelled) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_memtable_writer->cancel_with_status(st));
    _is_cancelled = true;
    return Status::OK();
}

Status DeltaWriter::cancel_with_status(const Status& st) {
    std::lock_guard<std::mutex> l(_lock);
    return BaseDeltaWriter::cancel_with_status(st);
}

int64_t BaseDeltaWriter::mem_consumption(MemType mem) {
    return _memtable_writer->mem_consumption(mem);
}

int64_t BaseDeltaWriter::num_rows_filtered() const {
    auto rowset_writer = _rowset_builder->rowset_writer();
    return rowset_writer == nullptr ? 0 : rowset_writer->num_rows_filtered();
}

} // namespace doris
