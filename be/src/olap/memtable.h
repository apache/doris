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

#ifndef DORIS_BE_SRC_OLAP_MEMTABLE_H
#define DORIS_BE_SRC_OLAP_MEMTABLE_H

#include <future>
#include <memory>

#include "olap/schema.h"
#include "olap/skiplist.h"
#include "runtime/tuple.h"
#include "olap/rowset/rowset_writer.h"

namespace doris {

class RowCursor;

class MemTable {
public:
    MemTable(int64_t tablet_id, Schema* schema, const TabletSchema* tablet_schema,
             const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
             KeysType keys_type);
    ~MemTable();
    int64_t tablet_id() { return _tablet_id; }
    size_t memory_usage();
    void insert(Tuple* tuple);
    OLAPStatus flush(RowsetWriter* rowset_writer);
    OLAPStatus close(RowsetWriter* rowset_writer);

    std::future<OLAPStatus> get_flush_future() { return _flush_promise.get_future(); }
    void set_flush_status(OLAPStatus st) { _flush_promise.set_value(st); }

private:
    int64_t _tablet_id; 
    Schema* _schema;
    const TabletSchema* _tablet_schema;
    TupleDescriptor* _tuple_desc;
    // the slot in _slot_descs are in order of tablet's schema
    const std::vector<SlotDescriptor*>* _slot_descs;
    KeysType _keys_type;

    struct RowCursorComparator {
        const Schema* _schema;
        RowCursorComparator(const Schema* schema);
        int operator()(const char* left, const char* right) const;
    };

    RowCursorComparator _row_comparator;
    Arena _arena;

    typedef SkipList<char*, RowCursorComparator> Table;
    char* _tuple_buf;
    size_t _schema_size;
    Table* _skip_list;

    // the promise it to save result status of flush
    std::promise<OLAPStatus> _flush_promise;
}; // class MemTable

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_MEMTABLE_H
