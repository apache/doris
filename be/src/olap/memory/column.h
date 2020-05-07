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

#include "olap/memory/column_block.h"
#include "olap/memory/column_delta.h"
#include "olap/memory/common.h"
#include "olap/memory/schema.h"

namespace doris {
namespace memory {

class ColumnReader;
class ColumnWriter;

// Column store all the data of a column, including base and deltas.
// It supports single-writer multi-reader concurrency.
// It's properties are all immutable except _base and _versions.
// _base and _versions use std::vector, which is basically thread-safe
// in-practice for single-writer/multi-reader access, if there isn't
// any over-capacity realloc or delta compaction/GC caused data change.
// When these situations occur, we do a copy-on-write.
//
// TODO: add column read&writer
class Column : public RefCountedThreadSafe<Column> {
public:
    static const uint32_t BLOCK_SIZE = 1 << 16;
    static const uint32_t BLOCK_MASK = 0xffff;

    // create a Column which provided column schema, underlying storage_type and initial version
    Column(const ColumnSchema& cs, ColumnType storage_type, uint64_t version);

    // copy-on-write a new Column with new capacity
    Column(const Column& rhs, size_t new_base_capacity, size_t new_version_capacity);

    // get column schema
    const ColumnSchema& schema() { return _cs; }

    // get memory usage in bytes
    size_t memory() const;

    string debug_string() const;

    // read this Column at a specific version, get a reader for this Column
    // support multiple concurrent readers
    Status read(uint64_t version, std::unique_ptr<ColumnReader>* reader);

    // write this Column, get a writer for this Column
    // caller needs to make sure there is only one or no writer exists at any time
    Status write(std::unique_ptr<ColumnWriter>* writer);

private:
    ColumnSchema _cs;
    // For some types the storage_type may be different from actual type from schema.
    // For example, string stored in dictionary, so column_block store a integer id,
    // and the storage type may change as the dictionary grows, e.g. from uint8 to uint16
    ColumnType _storage_type;
    // base's position at _versions vector
    ssize_t _base_idx;
    // base data, a vector of ColumnBlocks
    vector<scoped_refptr<ColumnBlock>> _base;
    struct VersionInfo {
        VersionInfo() = default;
        explicit VersionInfo(uint64_t version) : version(version) {}
        uint64_t version = 0;
        // null if it's base
        scoped_refptr<ColumnDelta> delta;
    };
    // version vector
    vector<VersionInfo> _versions;

    Status capture_version(uint64_t version, vector<ColumnDelta*>* deltas,
                           uint64_t* real_version) const;

    void capture_latest(vector<ColumnDelta*>* deltas) const;

    DISALLOW_COPY_AND_ASSIGN(Column);
};

} // namespace memory
} // namespace doris
