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

#pragma once

#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "olap/delta_writer_context.h"
#include "olap/memtable.h"
#include "olap/olap_common.h"
#include "olap/partial_update_info.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "util/spinlock.h"
#include "util/uid_util.h"

namespace doris {

class FlushToken;
class MemTable;
class MemTracker;
class StorageEngine;
class TupleDescriptor;
class SlotDescriptor;
class OlapTableSchemaParam;
class RowsetWriter;
struct FlushStatistic;

namespace vectorized {
class Block;
} // namespace vectorized

enum MemType { WRITE = 1, FLUSH = 2, ALL = 3 };

// Writer for a particular (load, index, tablet).
// This class is NOT thread-safe, external synchronization is required.
class MemTableWriter {
public:
    MemTableWriter(const WriteRequest& req);

    ~MemTableWriter();

    Status init(std::shared_ptr<RowsetWriter> rowset_writer, TabletSchemaSPtr tablet_schema,
                std::shared_ptr<PartialUpdateInfo> partial_update_info,
                ThreadPool* wg_flush_pool_ptr, bool unique_key_mow = false);

    Status write(const vectorized::Block* block, const std::vector<uint32_t>& row_idxs);

    // flush the last memtable to flush queue, must call it before close_wait()
    Status close();
    // wait for all memtables to be flushed, update profiles if provided.
    // mem_consumption() should be 0 after this function returns.
    Status close_wait(RuntimeProfile* profile = nullptr) {
        RETURN_IF_ERROR(_do_close_wait());
        if (profile != nullptr) {
            _update_profile(profile);
        }
        return Status::OK();
    }

    // abandon current memtable and wait for all pending-flushing memtables to be destructed.
    // mem_consumption() should be 0 after this function returns.
    Status cancel();
    Status cancel_with_status(const Status& st);

    int64_t mem_consumption(MemType mem);
    int64_t active_memtable_mem_consumption();

    // Submit current memtable to flush queue, and return without waiting.
    // This is currently for reducing mem consumption of this memtable writer.
    Status flush_async();

    // Wait all memtable in flush queue to be flushed
    Status wait_flush();

    int64_t tablet_id() const { return _req.tablet_id; }

    int64_t total_received_rows() const { return _total_received_rows; }

    const FlushStatistic& get_flush_token_stats();

    uint64_t flush_running_count() const;

private:
    // push a full memtable to flush executor
    Status _flush_memtable_async();

    void _reset_mem_table();

    Status _do_close_wait();
    void _update_profile(RuntimeProfile* profile);

    std::atomic<bool> _is_init = false;
    bool _is_cancelled = false;
    bool _is_closed = false;
    Status _cancel_status;
    WriteRequest _req;
    std::shared_ptr<RowsetWriter> _rowset_writer;
    std::unique_ptr<MemTable> _mem_table;
    TabletSchemaSPtr _tablet_schema;
    bool _unique_key_mow = false;

    // This variable is accessed from writer thread and token flush thread
    // use a shared ptr to avoid use after free problem.
    std::shared_ptr<FlushToken> _flush_token;
    std::vector<std::shared_ptr<MemTracker>> _mem_table_insert_trackers;
    std::vector<std::shared_ptr<MemTracker>> _mem_table_flush_trackers;
    SpinLock _mem_table_tracker_lock;
    SpinLock _mem_table_ptr_lock;
    std::atomic<uint32_t> _mem_table_num = 1;
    QueryThreadContext _query_thread_context;

    std::mutex _lock;

    // total rows num written by MemTableWriter
    std::atomic<int64_t> _total_received_rows = 0;
    int64_t _wait_flush_time_ns = 0;
    int64_t _close_wait_time_ns = 0;
    int64_t _segment_num = 0;

    MonotonicStopWatch _lock_watch;

    std::shared_ptr<PartialUpdateInfo> _partial_update_info;
};

} // namespace doris
