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
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "olap/delta_writer_context.h"
#include "olap/memtable_writer.h"
#include "olap/olap_common.h"
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

namespace vectorized {
class Block;
} // namespace vectorized

class BaseRowsetBuilder;

// Writer for a particular (load, index, tablet).
// This class is NOT thread-safe, external synchronization is required.
class BaseDeltaWriter {
public:
    BaseDeltaWriter(WriteRequest* req, RuntimeProfile* profile, const UniqueId& load_id);

    virtual ~BaseDeltaWriter();

    Status init();

    Status write(const vectorized::Block* block, const std::vector<uint32_t>& row_idxs,
                 bool is_append = false);

    Status append(const vectorized::Block* block);

    // flush the last memtable to flush queue, must call it before build_rowset()
    Status close();
    // wait for all memtables to be flushed.
    // mem_consumption() should be 0 after this function returns.
    Status build_rowset();
    Status submit_calc_delete_bitmap_task();
    Status wait_calc_delete_bitmap();

    // abandon current memtable and wait for all pending-flushing memtables to be destructed.
    // mem_consumption() should be 0 after this function returns.
    Status cancel();
    Status cancel_with_status(const Status& st);

    int64_t mem_consumption(MemType mem);

    // Wait all memtable in flush queue to be flushed
    Status wait_flush();

    int64_t partition_id() const { return _req.partition_id; }

    int64_t tablet_id() const { return _req.tablet_id; }

    int64_t txn_id() const { return _req.txn_id; }

    int64_t total_received_rows() const { return _memtable_writer->total_received_rows(); }

    int64_t num_rows_filtered() const;

protected:
    virtual void _init_profile(RuntimeProfile* profile);

    bool _is_init = false;
    bool _is_cancelled = false;
    WriteRequest _req;
    std::unique_ptr<BaseRowsetBuilder> _rowset_builder;
    std::shared_ptr<MemTableWriter> _memtable_writer;

    std::mutex _lock;

    // total rows num written by DeltaWriter
    std::atomic<int64_t> _total_received_rows = 0;

    RuntimeProfile* _profile = nullptr;
    RuntimeProfile::Counter* _close_wait_timer = nullptr;
    RuntimeProfile::Counter* _wait_flush_limit_timer = nullptr;

    MonotonicStopWatch _lock_watch;
};

// `StorageEngine` mixin for `BaseDeltaWriter`
class DeltaWriter final : public BaseDeltaWriter {
public:
    DeltaWriter(StorageEngine& engine, WriteRequest* req, RuntimeProfile* profile,
                const UniqueId& load_id);

    ~DeltaWriter() override;

    Status commit_txn(const PSlaveTabletNodes& slave_tablet_nodes);

    bool check_slave_replicas_done(google::protobuf::Map<int64_t, PSuccessSlaveTabletNodeIds>*
                                           success_slave_tablet_node_ids);

    void add_finished_slave_replicas(google::protobuf::Map<int64_t, PSuccessSlaveTabletNodeIds>*
                                             success_slave_tablet_node_ids);

    void finish_slave_tablet_pull_rowset(int64_t node_id, bool is_succeed);

private:
    void _init_profile(RuntimeProfile* profile) override;

    void _request_slave_tablet_pull_rowset(PNodeInfo node_info);

    StorageEngine& _engine;
    std::unordered_set<int64_t> _unfinished_slave_node;
    PSuccessSlaveTabletNodeIds _success_slave_node_ids;
    std::shared_mutex _slave_node_lock;

    RuntimeProfile::Counter* _commit_txn_timer = nullptr;
};

} // namespace doris
