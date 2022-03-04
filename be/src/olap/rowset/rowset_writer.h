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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_WRITER_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_WRITER_H

#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/types.pb.h"
#include "gutil/macros.h"
#include "olap/column_mapping.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer_context.h"

namespace doris {

struct ContiguousRow;
class MemTable;
class RowCursor;

class RowsetWriter {
public:
    RowsetWriter() = default;
    virtual ~RowsetWriter() = default;

    virtual OLAPStatus init(const RowsetWriterContext& rowset_writer_context) = 0;

    // Memory note: input `row` is guaranteed to be copied into writer's internal buffer, including all slice data
    // referenced by `row`. That means callers are free to de-allocate memory for `row` after this method returns.
    virtual OLAPStatus add_row(const RowCursor& row) = 0;
    virtual OLAPStatus add_row(const ContiguousRow& row) = 0;

    // Precondition: the input `rowset` should have the same type of the rowset we're building
    virtual OLAPStatus add_rowset(RowsetSharedPtr rowset) = 0;

    // Precondition: the input `rowset` should have the same type of the rowset we're building
    virtual OLAPStatus add_rowset_for_linked_schema_change(RowsetSharedPtr rowset,
                                                           const SchemaMapping& schema_mapping) = 0;

    // explicit flush all buffered rows into segment file.
    // note that `add_row` could also trigger flush when certain conditions are met
    virtual OLAPStatus flush() = 0;

    virtual OLAPStatus flush_single_memtable(MemTable* memtable, int64_t* flush_size) {
        return OLAP_ERR_FUNC_NOT_IMPLEMENTED;
    }

    // finish building and return pointer to the built rowset (guaranteed to be inited).
    // return nullptr when failed
    virtual RowsetSharedPtr build() = 0;

    virtual Version version() = 0;

    virtual int64_t num_rows() = 0;

    virtual RowsetId rowset_id() = 0;

    virtual RowsetTypePB type() const = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(RowsetWriter);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_WRITER_H
