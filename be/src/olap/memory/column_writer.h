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

#include "olap/memory/common.h"

namespace doris {
namespace memory {

class Column;

// Exclusive writer for column, also with some reader's functionality
//
// Example writer usage:
//   scoped_refptr<Column> column;
//   std::unique_ptr<ColumnWriter> writer;
//   // create writer
//   RETURN_IF_ERROR(column->create_writer(&writer).ok());
//   writer->insert(new_rowid, &value);
//   writer->update(rowid, &value);
//   writer->finalize(new_version);
//   // get new column refptr
//   // if a COW has been done, column will point to new column object
//   // else column will remain the same.
//   writer->get_new_column(&column);
class ColumnWriter {
public:
    virtual ~ColumnWriter() {}

    // Get cell by rid, caller needs to make sure rid is in valid range
    //
    // In some cases, user may need a read-write style operation, so writer also need
    // get operation. For example:
    // 1. check the update value is the same as original value, so the update can be ignored
    // 2. support counter(v=v+1) style update
    //
    // Note: this is the same method as ColumnReader::get
    virtual const void* get(const uint32_t rid) const = 0;

    // Borrow a vtable slot to do typed hashcode calculation, mainly used to find
    // row by rowkey using hash index.
    //
    // When perform an update/insert, user needs to firstly query hashindex to find rowid by
    // rowkey. This requires ColumnWriter providing hashcode and equals methods.
    //
    // It's designed to support array operator, so there are two parameters for user
    // to pass array start and array index.
    //
    // Note: this is the same method as ColumnReader::hashcode
    virtual uint64_t hashcode(const void* rhs, size_t rhs_idx) const = 0;

    // Check cell equality, mainly used to find row by rowkey using hash index.
    //
    // Note: this is the same method as ColumnReader::equals
    virtual bool equals(const uint32_t rid, const void* rhs, size_t rhs_idx) const = 0;

    virtual string debug_string() const = 0;

    // Insert/append a cell at specified row
    virtual Status insert(uint32_t rid, const void* value) = 0;

    // Update a cell at specified row
    virtual Status update(uint32_t rid, const void* value) = 0;

    // Finish and finalize this write with a version
    virtual Status finalize(uint64_t version) = 0;

    // Get column object refptr, it will point to a new object if a COW has been performed,
    // or it will remain the same with the old Column object.
    virtual Status get_new_column(scoped_refptr<Column>* ret) = 0;
};

} // namespace memory
} // namespace doris
