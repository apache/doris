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

#ifndef DORIS_BE_SRC_DELTA_WRITER_H
#define DORIS_BE_SRC_DELTA_WRITER_H

#include "gen_cpp/internal_service.pb.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/tablet.h"

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
};

// Writer for a particular (load, index, tablet).
// This class is NOT thread-safe, external synchronization is required.
class DeltaWriter {
public:
    static OLAPStatus open(WriteRequest* req, const std::shared_ptr<MemTracker>& parent,
                           DeltaWriter** writer);

    ~DeltaWriter();

    OLAPStatus init();

    OLAPStatus write(Tuple* tuple);
    OLAPStatus write(const RowBatch* row_batch, const std::vector<int>& row_idxs);
    // flush the last memtable to flush queue, must call it before close_wait()
    OLAPStatus close();
    // wait for all memtables to be flushed.
    // mem_consumption() should be 0 after this function returns.
    OLAPStatus close_wait();

    // abandon current memtable and wait for all pending-flushing memtables to be destructed.
    // mem_consumption() should be 0 after this function returns.
    OLAPStatus cancel();

    // submit current memtable to flush queue, and wait all memtables in flush queue
    // to be flushed.
    // This is currently for reducing mem consumption of this delta writer.
    // If need_wait is true, it will wait for all memtable in flush queue to be flushed.
    // Otherwise, it will just put memtables to the flush queue and return.
    OLAPStatus flush_memtable_and_wait(bool need_wait);

    int64_t partition_id() const;

    int64_t mem_consumption() const;

    // Wait all memtable in flush queue to be flushed
    OLAPStatus wait_flush();

    int64_t tablet_id() { return _tablet->tablet_id(); }

    int32_t schema_hash() { return _tablet->schema_hash(); }
    
    int64_t memtable_consumption() const;
    
    int64_t save_mem_consumption_snapshot();

    int64_t get_mem_consumption_snapshot() const;

private:
    DeltaWriter(WriteRequest* req, const std::shared_ptr<MemTracker>& parent,
                StorageEngine* storage_engine);

    // push a full memtable to flush executor
    OLAPStatus _flush_memtable_async();

    void _garbage_collection();

    void _reset_mem_table();

private:
    bool _is_init = false;
    bool _is_cancelled = false;
    WriteRequest _req;
    TabletSharedPtr _tablet;
    RowsetSharedPtr _cur_rowset;
    std::unique_ptr<RowsetWriter> _rowset_writer;
    std::shared_ptr<MemTable> _mem_table;
    std::unique_ptr<Schema> _schema;
    const TabletSchema* _tablet_schema;
    bool _delta_written_success;

    StorageEngine* _storage_engine;
    std::unique_ptr<FlushToken> _flush_token;
    std::shared_ptr<MemTracker> _parent_mem_tracker;
    std::shared_ptr<MemTracker> _mem_tracker;

    // The counter of number of segment flushed already.
    int64_t _segment_counter = 0;

    std::mutex _lock;

    //only used for std::sort more detail see issue(#9237)
    int64_t _mem_consumption_snapshot = 0;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_DELTA_WRITER_H
