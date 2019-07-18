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

#include "olap/memtable.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/schema_change.h"
#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "gen_cpp/internal_service.pb.h"
#include "olap/rowset/rowset_writer.h"

namespace doris {

class SegmentGroup;

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
};

class DeltaWriter {
public:
    static OLAPStatus open(WriteRequest* req, DeltaWriter** writer);
    OLAPStatus init();
    DeltaWriter(WriteRequest* req);
    ~DeltaWriter();
    OLAPStatus write(Tuple* tuple);
    OLAPStatus close(google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec);

    OLAPStatus cancel();

    int64_t partition_id() const { return _req.partition_id; }

private:
    void _garbage_collection();

private:
    bool _is_init = false;
    WriteRequest _req;
    TabletSharedPtr _tablet;
    RowsetSharedPtr _cur_rowset;
    RowsetSharedPtr _new_rowset;
    TabletSharedPtr _new_tablet;
    RowsetWriterSharedPtr _rowset_writer;
    MemTable* _mem_table;
    Schema* _schema;
    const TabletSchema* _tablet_schema;
    std::vector<uint32_t> _col_ids;
    bool _delta_written_success;
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_DELTA_WRITER_H
