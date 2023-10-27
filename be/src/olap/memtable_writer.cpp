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

#include "olap/memtable_writer.h"

#include <fmt/format.h>

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
#include "gutil/strings/numbers.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "olap/memtable.h"
#include "olap/memtable_flush_executor.h"
#include "olap/memtable_memory_limiter.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "service/backend_options.h"
#include "util/mem_info.h"
#include "util/stopwatch.hpp"
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

MemTableWriter::MemTableWriter(const WriteRequest& req) : _req(req) {}

MemTableWriter::~MemTableWriter() {
    if (!_is_init) {
        return;
    }
    if (_flush_token != nullptr) {
        // cancel and wait all memtables in flush queue to be finished
        _flush_token->cancel();
    }
    _mem_table.reset();
}

Status MemTableWriter::init(std::shared_ptr<RowsetWriter> rowset_writer,
                            TabletSchemaSPtr tablet_schema,
                            std::shared_ptr<PartialUpdateInfo> partial_update_info,
                            bool unique_key_mow) {
    _rowset_writer = rowset_writer;
    _tablet_schema = tablet_schema;
    _unique_key_mow = unique_key_mow;
    _partial_update_info = partial_update_info;

    _reset_mem_table();

    // create flush handler
    // by assigning segment_id to memtable before submiting to flush executor,
    // we can make sure same keys sort in the same order in all replicas.
    bool should_serial = false;
    RETURN_IF_ERROR(StorageEngine::instance()->memtable_flush_executor()->create_flush_token(
            _flush_token, _rowset_writer.get(), should_serial, _req.is_high_priority));

    _is_init = true;
    return Status::OK();
}

Status MemTableWriter::append(const vectorized::Block* block) {
    return write(block, {}, true);
}

Status MemTableWriter::write(const vectorized::Block* block, const std::vector<int>& row_idxs,
                             bool is_append) {
    if (UNLIKELY(row_idxs.empty() && !is_append)) {
        return Status::OK();
    }
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (_is_cancelled) {
        return _cancel_status;
    }
    if (!_is_init) {
        return Status::Error<NOT_INITIALIZED>("delta segment writer has not been initialized");
    }
    if (_is_closed) {
        return Status::Error<ALREADY_CLOSED>("write block after closed tablet_id={}, load_id={}-{}",
                                             _req.tablet_id, _req.load_id.hi(), _req.load_id.lo());
    }

    if (is_append) {
        _total_received_rows += block->rows();
    } else {
        _total_received_rows += row_idxs.size();
    }
    _mem_table->insert(block, row_idxs, is_append);

    if (UNLIKELY(_mem_table->need_agg() && config::enable_shrink_memory)) {
        _mem_table->shrink_memtable_by_agg();
    }
    if (UNLIKELY(_mem_table->need_flush())) {
        auto s = _flush_memtable_async();
        _reset_mem_table();
        if (UNLIKELY(!s.ok())) {
            return s;
        }
    }

    return Status::OK();
}

Status MemTableWriter::_flush_memtable_async() {
    DCHECK(_flush_token != nullptr);
    return _flush_token->submit(std::move(_mem_table));
}

Status MemTableWriter::flush_async() {
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init || _is_closed) {
        // This writer is uninitialized or closed before flushing, do nothing.
        // We return OK instead of NOT_INITIALIZED or ALREADY_CLOSED.
        // Because this method maybe called when trying to reduce mem consumption,
        // and at that time, the writer may not be initialized yet and that is a normal case.
        return Status::OK();
    }

    if (_is_cancelled) {
        return _cancel_status;
    }

    VLOG_NOTICE << "flush memtable to reduce mem consumption. memtable size: "
                << _mem_table->memory_usage() << ", tablet: " << _req.tablet_id
                << ", load id: " << print_id(_req.load_id);
    auto s = _flush_memtable_async();
    _reset_mem_table();
    return s;
}

Status MemTableWriter::wait_flush() {
    {
        std::lock_guard<std::mutex> l(_lock);
        if (!_is_init || _is_closed) {
            // return OK instead of NOT_INITIALIZED or ALREADY_CLOSED for same reason
            // as described in flush_async()
            return Status::OK();
        }
        if (_is_cancelled) {
            return _cancel_status;
        }
    }
    SCOPED_RAW_TIMER(&_wait_flush_time_ns);
    RETURN_IF_ERROR(_flush_token->wait());
    return Status::OK();
}

void MemTableWriter::_reset_mem_table() {
#ifndef BE_TEST
    auto mem_table_insert_tracker = std::make_shared<MemTracker>(
            fmt::format("MemTableManualInsert:TabletId={}:MemTableNum={}#loadID={}",
                        std::to_string(tablet_id()), _mem_table_num,
                        UniqueId(_req.load_id).to_string()),
            ExecEnv::GetInstance()->memtable_memory_limiter()->mem_tracker());
    auto mem_table_flush_tracker = std::make_shared<MemTracker>(
            fmt::format("MemTableHookFlush:TabletId={}:MemTableNum={}#loadID={}",
                        std::to_string(tablet_id()), _mem_table_num++,
                        UniqueId(_req.load_id).to_string()),
            ExecEnv::GetInstance()->memtable_memory_limiter()->mem_tracker());
#else
    auto mem_table_insert_tracker = std::make_shared<MemTracker>(fmt::format(
            "MemTableManualInsert:TabletId={}:MemTableNum={}#loadID={}",
            std::to_string(tablet_id()), _mem_table_num, UniqueId(_req.load_id).to_string()));
    auto mem_table_flush_tracker = std::make_shared<MemTracker>(fmt::format(
            "MemTableHookFlush:TabletId={}:MemTableNum={}#loadID={}", std::to_string(tablet_id()),
            _mem_table_num++, UniqueId(_req.load_id).to_string()));
#endif
    {
        std::lock_guard<SpinLock> l(_mem_table_tracker_lock);
        _mem_table_insert_trackers.push_back(mem_table_insert_tracker);
        _mem_table_flush_trackers.push_back(mem_table_flush_tracker);
    }
    _mem_table.reset(new MemTable(_req.tablet_id, _tablet_schema.get(), _req.slots, _req.tuple_desc,
                                  _unique_key_mow, _partial_update_info.get(),
                                  mem_table_insert_tracker, mem_table_flush_tracker));

    _segment_num++;
}

Status MemTableWriter::close() {
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (_is_cancelled) {
        return _cancel_status;
    }
    if (!_is_init) {
        return Status::Error<NOT_INITIALIZED>("delta segment writer has not been initialized");
    }
    if (_is_closed) {
        LOG(WARNING) << "close after closed tablet_id=" << _req.tablet_id
                     << " load_id=" << _req.load_id;
        return Status::OK();
    }

    auto s = _flush_memtable_async();
    _mem_table.reset();
    _is_closed = true;
    if (UNLIKELY(!s.ok())) {
        return s;
    } else {
        return Status::OK();
    }
}

Status MemTableWriter::_do_close_wait() {
    SCOPED_RAW_TIMER(&_close_wait_time_ns);
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init)
            << "delta writer is supposed be to initialized before close_wait() being called";

    if (_is_cancelled) {
        return _cancel_status;
    }

    Status st;
    // return error if previous flush failed
    {
        SCOPED_RAW_TIMER(&_wait_flush_time_ns);
        st = _flush_token->wait();
    }
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << "previous flush failed tablet " << _req.tablet_id;
        return st;
    }

    _mem_table.reset();

    if (_rowset_writer->num_rows() + _flush_token->memtable_stat().merged_rows !=
        _total_received_rows) {
        LOG(WARNING) << "the rows number written doesn't match, rowset num rows written to file: "
                     << _rowset_writer->num_rows()
                     << ", merged_rows: " << _flush_token->memtable_stat().merged_rows
                     << ", total received rows: " << _total_received_rows;
        return Status::InternalError("rows number written by delta writer dosen't match");
    }

    // const FlushStatistic& stat = _flush_token->get_stats();
    // print slow log if wait more than 1s
    /*if (_wait_flush_timer->elapsed_time() > 1000UL * 1000 * 1000) {
        LOG(INFO) << "close delta writer for tablet: " << req.tablet_id
                  << ", load id: " << print_id(_req.load_id) << ", wait close for "
                  << _wait_flush_timer->elapsed_time() << "(ns), stats: " << stat;
    }*/

    return Status::OK();
}

void MemTableWriter::_update_profile(RuntimeProfile* profile) {
    // NOTE: MemTableWriter may be accessed when profile is out of scope, in MemTableMemoryLimiter.
    // To avoid accessing dangling pointers, we cannot make profile as a member of MemTableWriter.
    auto child =
            profile->create_child(fmt::format("MemTableWriter {}", _req.tablet_id), true, true);
    auto lock_timer = ADD_TIMER(child, "LockTime");
    auto sort_timer = ADD_TIMER(child, "MemTableSortTime");
    auto agg_timer = ADD_TIMER(child, "MemTableAggTime");
    auto memtable_duration_timer = ADD_TIMER(child, "MemTableDurationTime");
    auto segment_writer_timer = ADD_TIMER(child, "SegmentWriterTime");
    auto wait_flush_timer = ADD_TIMER(child, "MemTableWaitFlushTime");
    auto put_into_output_timer = ADD_TIMER(child, "MemTablePutIntoOutputTime");
    auto delete_bitmap_timer = ADD_TIMER(child, "DeleteBitmapTime");
    auto close_wait_timer = ADD_TIMER(child, "CloseWaitTime");
    auto sort_times = ADD_COUNTER(child, "MemTableSortTimes", TUnit::UNIT);
    auto agg_times = ADD_COUNTER(child, "MemTableAggTimes", TUnit::UNIT);
    auto segment_num = ADD_COUNTER(child, "SegmentNum", TUnit::UNIT);
    auto raw_rows_num = ADD_COUNTER(child, "RawRowNum", TUnit::UNIT);
    auto merged_rows_num = ADD_COUNTER(child, "MergedRowNum", TUnit::UNIT);

    COUNTER_UPDATE(lock_timer, _lock_watch.elapsed_time());
    COUNTER_SET(delete_bitmap_timer, _rowset_writer->delete_bitmap_ns());
    COUNTER_SET(segment_writer_timer, _rowset_writer->segment_writer_ns());
    COUNTER_SET(wait_flush_timer, _wait_flush_time_ns);
    COUNTER_SET(close_wait_timer, _close_wait_time_ns);
    COUNTER_SET(segment_num, _segment_num);
    const auto& memtable_stat = _flush_token->memtable_stat();
    COUNTER_SET(sort_timer, memtable_stat.sort_ns);
    COUNTER_SET(agg_timer, memtable_stat.agg_ns);
    COUNTER_SET(memtable_duration_timer, memtable_stat.duration_ns);
    COUNTER_SET(put_into_output_timer, memtable_stat.put_into_output_ns);
    COUNTER_SET(sort_times, memtable_stat.sort_times);
    COUNTER_SET(agg_times, memtable_stat.agg_times);
    COUNTER_SET(raw_rows_num, memtable_stat.raw_rows);
    COUNTER_SET(merged_rows_num, memtable_stat.merged_rows);
}

Status MemTableWriter::cancel() {
    return cancel_with_status(Status::Cancelled("already cancelled"));
}

Status MemTableWriter::cancel_with_status(const Status& st) {
    std::lock_guard<std::mutex> l(_lock);
    if (_is_cancelled) {
        return Status::OK();
    }
    _mem_table.reset();
    if (_flush_token != nullptr) {
        // cancel and wait all memtables in flush queue to be finished
        _flush_token->cancel();
    }
    _is_cancelled = true;
    _cancel_status = st;
    return Status::OK();
}

const FlushStatistic& MemTableWriter::get_flush_token_stats() {
    return _flush_token->get_stats();
}

int64_t MemTableWriter::mem_consumption(MemType mem) {
    if (!_is_init) {
        // This method may be called before this writer is initialized.
        // So _flush_token may be null.
        return 0;
    }
    int64_t mem_usage = 0;
    {
        std::lock_guard<SpinLock> l(_mem_table_tracker_lock);
        if ((mem & MemType::WRITE) == MemType::WRITE) { // 3 & 2 = 2
            for (const auto& mem_table_tracker : _mem_table_insert_trackers) {
                mem_usage += mem_table_tracker->consumption();
            }
        }
        if ((mem & MemType::FLUSH) == MemType::FLUSH) { // 3 & 1 = 1
            for (const auto& mem_table_tracker : _mem_table_flush_trackers) {
                mem_usage += mem_table_tracker->consumption();
            }
        }
    }
    return mem_usage;
}

int64_t MemTableWriter::active_memtable_mem_consumption() {
    return _mem_table != nullptr ? _mem_table->memory_usage() : 0;
}

} // namespace doris
