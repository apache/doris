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

#include "gen_cpp/internal_service.pb.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/tablet.h"
#include "util/spinlock.h"

namespace doris {

class FlushToken;
class MemTable;
class MemTracker;
class RowBatch;
class Schema;
class StorageEngine;
class Tuple;
class TupleDescriptor;
class TupleRow;
class SlotDescriptor;

enum WriteType { LOAD = 1, LOAD_DELETE = 2, DELETE = 3 };

struct WriteRequest {
    int64_t tablet_id;
    int32_t schema_hash;
    WriteType write_type;
    int64_t txn_id;
    int64_t partition_id;
    PUniqueId load_id;
    TupleDescriptor* tuple_desc;
    // slots are in order of tablet's schema
    const std::vector<SlotDescriptor*>* slots;
    bool is_high_priority = false;
    POlapTableSchemaParam ptable_schema_param;
    int64_t index_id;
};

// Writer for a particular (load, index, tablet).
// This class is NOT thread-safe, external synchronization is required.
class DeltaWriter {
public:
    static Status open(WriteRequest* req, DeltaWriter** writer,
                       const UniqueId& load_id = TUniqueId(), bool is_vec = false);

    ~DeltaWriter();

    Status init();

    Status write(Tuple* tuple);
    Status write(const RowBatch* row_batch, const std::vector<int>& row_idxs);
    Status write(const vectorized::Block* block, const std::vector<int>& row_idxs);

    // flush the last memtable to flush queue, must call it before close_wait()
    Status close();
    // wait for all memtables to be flushed.
    // mem_consumption() should be 0 after this function returns.
    Status close_wait(const PSlaveTabletNodes& slave_tablet_nodes, const bool write_single_replica);

    bool check_slave_replicas_done(google::protobuf::Map<int64_t, PSuccessSlaveTabletNodeIds>*
                                           success_slave_tablet_node_ids);

    void add_finished_slave_replicas(google::protobuf::Map<int64_t, PSuccessSlaveTabletNodeIds>*
                                             success_slave_tablet_node_ids);

    // abandon current memtable and wait for all pending-flushing memtables to be destructed.
    // mem_consumption() should be 0 after this function returns.
    Status cancel();
    Status cancel_with_status(const Status& st);

    // submit current memtable to flush queue, and wait all memtables in flush queue
    // to be flushed.
    // This is currently for reducing mem consumption of this delta writer.
    // If need_wait is true, it will wait for all memtable in flush queue to be flushed.
    // Otherwise, it will just put memtables to the flush queue and return.
    Status flush_memtable_and_wait(bool need_wait);

    int64_t partition_id() const;

    int64_t mem_consumption();

    // Wait all memtable in flush queue to be flushed
    Status wait_flush();

    int64_t tablet_id() { return _tablet->tablet_id(); }

    int32_t schema_hash() { return _tablet->schema_hash(); }

    void save_mem_consumption_snapshot();

    int64_t get_memtable_consumption_inflush() const;

    int64_t get_memtable_consumption_snapshot() const;

    void finish_slave_tablet_pull_rowset(int64_t node_id, bool is_succeed);

private:
    DeltaWriter(WriteRequest* req, StorageEngine* storage_engine, const UniqueId& load_id,
                bool is_vec);

    // push a full memtable to flush executor
    Status _flush_memtable_async();

    void _garbage_collection();

    void _reset_mem_table();

    void _build_current_tablet_schema(int64_t index_id,
                                      const POlapTableSchemaParam& table_schema_param,
                                      const TabletSchema& ori_tablet_schema);

    void _request_slave_tablet_pull_rowset(PNodeInfo node_info);

    bool _is_init = false;
    bool _is_cancelled = false;
    Status _cancel_status;
    WriteRequest _req;
    TabletSharedPtr _tablet;
    RowsetSharedPtr _cur_rowset;
    std::unique_ptr<RowsetWriter> _rowset_writer;
    // TODO: Recheck the lifetime of _mem_table, Look should use unique_ptr
    std::unique_ptr<MemTable> _mem_table;
    std::unique_ptr<Schema> _schema;
    //const TabletSchema* _tablet_schema;
    // tablet schema owned by delta writer, all write will use this tablet schema
    // it's build from tablet_schema（stored when create tablet） and OlapTableSchema
    // every request will have it's own tablet schema so simple schema change can work
    TabletSchemaSPtr _tablet_schema;
    bool _delta_written_success;

    StorageEngine* _storage_engine;
    UniqueId _load_id;
    std::unique_ptr<FlushToken> _flush_token;
    std::vector<std::shared_ptr<MemTracker>> _mem_table_tracker;
    SpinLock _mem_table_tracker_lock;
    std::atomic<uint32_t> _mem_table_num = 1;

    std::mutex _lock;

    // use in vectorized load
    bool _is_vec;

    // memory consumption snapshot for current delta_writer, only
    // used for std::sort
    int64_t _mem_consumption_snapshot = 0;
    // memory consumption snapshot for current memtable, only
    // used for std::sort
    int64_t _memtable_consumption_snapshot = 0;

    std::unordered_set<int64_t> _unfinished_slave_node;
    PSuccessSlaveTabletNodeIds _success_slave_node_ids;
    std::shared_mutex _slave_node_lock;

    DeleteBitmapPtr _delete_bitmap = nullptr;
    // current rowset_ids, used to do diff in publish_version
    RowsetIdUnorderedSet _rowset_ids;
    // current max version, used to calculate delete bitmap
    int64_t _cur_max_version;
};

} // namespace doris
