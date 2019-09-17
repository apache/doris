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

#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/schema_change.h"
#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "gen_cpp/internal_service.pb.h"
#include "olap/rowset/rowset_writer.h"
#include "util/blocking_queue.hpp"

namespace doris {

class SegmentGroup;
class MemTable;
class MemTableFlushExecutor;
class Schema;

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

class DeltaWriter {
public:
    static OLAPStatus open(
        WriteRequest* req, 
        MemTableFlushExecutor* _flush_executor,
        DeltaWriter** writer);

    OLAPStatus init();

    DeltaWriter(WriteRequest* req, MemTableFlushExecutor* _flush_executor);

    ~DeltaWriter();

    OLAPStatus write(Tuple* tuple);
    // flush the last memtable to flush queue, must call it before close
    OLAPStatus flush();

    

    OLAPStatus close(google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec);

    OLAPStatus cancel();

    int64_t partition_id() const { return _req.partition_id; }

    OLAPStatus get_flush_status() { return _flush_status.load(); }
    RowsetWriter* rowset_writer() { return _rowset_writer.get(); }

    void update_flush_time(int64_t flush_ns) {
        _flush_time_ns += flush_ns;
        _flush_count++;
    }

private:
    // push a full memtable to flush executor
    OLAPStatus _flush_memtable_async();

    void _garbage_collection();

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

    // the flush status of previous memtable.
    // the default is OLAP_SUCCESS, and once it changes to some ERROR code,
    // it will never change back to OLAP_SUCCESS.
    // this status will be checked each time the next memtable is going to be flushed,
    // so that if the previous flush is already failed, no need to flush next memtable.
    std::atomic<OLAPStatus> _flush_status;
    // the future of the very last memtable flush execution.
    // because the flush of this delta writer's memtables are executed serially,
    // if the last memtable is flushed, all previous memtables should already be flushed.
    // so we only need to wait and block on the last memtable's flush future.
    std::future<OLAPStatus> _flush_future;
    // total flush time and flush count of memtables
    int64_t _flush_time_ns;
    int64_t _flush_count;

    MemTableFlushExecutor* _flush_executor;
    // the idx of flush queues vector in MemTableFlushExecutor.
    // this idx is got from MemTableFlushExecutor,
    // and memtables of this delta writer will be pushed to this certain flush queue only.
    int32_t _flush_queue_idx;
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_DELTA_WRITER_H
