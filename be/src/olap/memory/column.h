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
template <class, bool, class>
class TypedColumnReader;
template <class, bool, class, class>
class TypedColumnWriter;

// Column store all the data of a column, including base and deltas.
//
// It's properties are all immutable except _base and _versions.
// _base and _versions use std::vector, which is basically thread-safe
// in-practice for single-writer/multi-reader access, if there isn't
// any over-capacity realloc or delta compaction/GC caused data change.
// When these situations occur, we do a copy-on-write.
//
// Note: support single-writer multi-reader concurrency.
class Column : public RefCountedThreadSafe<Column> {
public:
    static const uint32_t BLOCK_SIZE = 1 << 16;
    static const uint32_t BLOCK_MASK = 0xffff;
    // Base vector capacity min grow step size
    static const uint32_t BASE_CAPACITY_MIN_STEP_SIZE = 8;
    // Base vector capacity max grow step size
    static const uint32_t BASE_CAPACITY_MAX_STEP_SIZE = 8;
    // Version vector capacity grow step size
    static const uint32_t VERSION_CAPACITY_STEP_SIZE = 8;

    // Get block id by rowid
    static uint32_t block_id(uint32_t rid) { return rid >> 16; }

    // Get index in block by rowid
    static uint32_t index_in_block(uint32_t rid) { return rid & BLOCK_MASK; }

    // Create a Column which provided column schema, underlying storage_type and initial version
    Column(const ColumnSchema& cs, ColumnType storage_type, uint64_t version);

    // Copy-on-write a new Column with new capacity
    Column(const Column& rhs, size_t new_base_capacity, size_t new_version_capacity);

    // Get column schema
    const ColumnSchema& schema() { return _cs; }

    // Get memory usage in bytes
    size_t memory() const;

    string debug_string() const;

    // Read this Column at a specific version, get a reader for this Column.
    // Column supports multiple concurrent readers
    Status create_reader(uint64_t version, std::unique_ptr<ColumnReader>* reader);

    // Write this Column, get a writer for this Column
    // Caller needs to make sure there is no more than one writer at any time
    Status create_writer(std::unique_ptr<ColumnWriter>* writer);

private:
    template <class, bool, class>
    friend class TypedColumnReader;
    template <class, bool, class, class>
    friend class TypedColumnWriter;

    ColumnSchema _cs;
    // For some types the storage_type may be different from actual type from schema.
    //
    // For example, string stored in dictionary, so column_block store a integer id,
    // and the storage type may change as the dictionary grows, e.g. from uint8 to uint16
    ColumnType _storage_type;
    // Base's position at _versions vector
    size_t _base_idx;
    // Base data, a vector of ColumnBlocks
    vector<scoped_refptr<ColumnBlock>> _base;
    struct VersionInfo {
        VersionInfo() = default;
        explicit VersionInfo(uint64_t version) : version(version) {}
        uint64_t version = 0;
        // null if it's base
        scoped_refptr<ColumnDelta> delta;
    };
    // Version vector
    vector<VersionInfo> _versions;

    // Get related deltas of a specified version, and it's corresponding real_version
    //
    // For example:
    // If we have [1,3,5,7,13,16,20,30] in versions array, and base is at version 13
    // capture version 24 will get deltas=[13, 16, 20], and real_version 20
    // capture version 4 will get deltas=[7,5], and real_version 5
    Status capture_version(uint64_t version, vector<ColumnDelta*>* deltas,
                           uint64_t* real_version) const;

    // Get latest version's related delta
    void capture_latest(vector<ColumnDelta*>* deltas, uint64_t* version) const;

    DISALLOW_COPY_AND_ASSIGN(Column);
};

} // namespace memory
} // namespace doris
