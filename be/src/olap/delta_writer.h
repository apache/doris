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

#include "olap/tablet.h"
#include "gen_cpp/internal_service.pb.h"
#include "olap/rowset/rowset_writer.h"

namespace doris {

class FlushHandler;
class MemTable;
class MemTracker;
class Schema;
class StorageEngine;
class Tuple;
class TupleDescriptor;
class SlotDescriptor;

enum WriteType {
    LOAD = 1,
    LOAD_DELETE = 2,
    DELETE = 3
};

struct WriteRequest {
    int64_t tablet_id;
    int32_t schema_hash;
    WriteType write_type;
    int64_t txn_id;
    int64_t partition_id;
    PUniqueId load_id;
    bool need_gen_rollup;
    TupleDescriptor* tuple_desc;
    // slots are in order of tablet's schema
    const std::vector<SlotDescriptor*>* slots;
};

// Writer for a particular (load, index, tablet).
// This class is NOT thread-safe, external synchronization is required.
class DeltaWriter {
public:
    static OLAPStatus open(WriteRequest* req, MemTracker* mem_tracker, DeltaWriter** writer);

    DeltaWriter(WriteRequest* req, MemTracker* parent, StorageEngine* storage_engine);

    OLAPStatus init();

    ~DeltaWriter();

    OLAPStatus write(Tuple* tuple);
    // flush the last memtable to flush queue, must call it before close_wait()
    OLAPStatus close();
    // wait for all memtables to be flushed.
    // mem_consumption() should be 0 after this function returns.
    OLAPStatus close_wait(google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec);

    // abandon current memtable and wait for all pending-flushing memtables to be destructed.
    // mem_consumption() should be 0 after this function returns.
    OLAPStatus cancel();

    // submit current memtable to flush queue, and wait all memtables in flush queue
    // to be flushed.
    // This is currently for reducing mem consumption of this delta writer.
    OLAPStatus flush_memtable_and_wait();

    int64_t partition_id() const;

    int64_t mem_consumption() const;

private:
    // push a full memtable to flush executor
    OLAPStatus _flush_memtable_async();

    void _garbage_collection();

    void _reset_mem_table();

private:
    bool _is_init = false;
    WriteRequest _req;
    TabletSharedPtr _tablet;
    RowsetSharedPtr _cur_rowset;
    RowsetSharedPtr _new_rowset;
    TabletSharedPtr _new_tablet;
    std::unique_ptr<RowsetWriter> _rowset_writer;
    std::shared_ptr<MemTable> _mem_table;
    Schema* _schema;
    const TabletSchema* _tablet_schema;
    bool _delta_written_success;

    StorageEngine* _storage_engine;
    std::shared_ptr<FlushHandler> _flush_handler;
    std::unique_ptr<MemTracker> _mem_tracker;
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_DELTA_WRITER_H
