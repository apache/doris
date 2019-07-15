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

#include <memory>

#include "olap/schema.h"
#include "olap/skiplist.h"
#include "runtime/tuple.h"
#include "olap/rowset/rowset_writer.h"

namespace doris {

class RowCursor;

class MemTable {
public:
    MemTable(Schema* schema, const TabletSchema* tablet_schema,
             std::vector<uint32_t>* col_ids, TupleDescriptor* tuple_desc,
             KeysType keys_type);
    ~MemTable();
    size_t memory_usage();
    void insert(Tuple* tuple);
    OLAPStatus flush(RowsetWriterSharedPtr rowset_writer);
    OLAPStatus close(RowsetWriterSharedPtr rowset_writer);
private:
    Schema* _schema;
    const TabletSchema* _tablet_schema;
    TupleDescriptor* _tuple_desc;
    std::vector<uint32_t>* _col_ids;
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
}; // class MemTable

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_MEMTABLE_H
