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
#include "olap/memory/schema.h"

namespace doris {
namespace memory {

class HashIndex;
class ColumnReader;
class PartialRowBatch;
class Column;
class ColumnWriter;

// A MemTablet can contain multiple MemSubTablets (currently only one).
// MemSubTablet hold a HashIndex and a collection of columns.
// It supports single-writer multi-reader concurrently.
//
// Example read usage:
// std::unique_ptr<ColumnReader> reader;
// sub_tablet->read_column(version, cid, &reader);
// // read(scan/get) cells using reader
//
// Example write usage:
// WrintTxn* wtxn;
// MemSubTablet* sub_tablet;
// sub_tablet->begin_write(current_schema);
// for (size_t i = 0; i < wtxn->batch_size(); i++) {
//     auto batch = wtxn->get_batch(i);
//     PartialRowReader reader(*batch);
//     for (size_t j = 0; j < reader.size(); j++) {
//         RETURN_IF_ERROR(reader.read(j));
//         RETURN_IF_ERROR(_sub_tablet->apply_partial_row(reader));
//     }
// }
// sub_tablet->commit_write(version);
class MemSubTablet {
public:
    // Create a new empty MemSubTablet, with specified schema and initial version
    static Status create(uint64_t version, const Schema& schema,
                         std::unique_ptr<MemSubTablet>* ret);

    // Destructor
    ~MemSubTablet();

    // Return number of rows of the latest version, including rows marked as delete
    size_t latest_size() const { return _versions.back().size; }

    // Return number of rows of the specified version, including rows marked as delete
    Status get_size(uint64_t version, size_t* size) const;

    // Read a column with specified by column id(cid) and version, return a column reader
    Status read_column(uint64_t version, uint32_t cid, std::unique_ptr<ColumnReader>* reader);

    // Get a hash index read reference to read
    Status get_index_to_read(scoped_refptr<HashIndex>* index);

    // Start a exclusive write batch
    // Note: caller should make sure schema is valid during write
    Status begin_write(scoped_refptr<Schema>* schema);

    // Apply a partial row to this MemSubTablet
    Status apply_partial_row_batch(PartialRowBatch* batch);

    // Finalize the whole write batch, with specified version
    Status commit_write(uint64_t version);

private:
    MemSubTablet();
    scoped_refptr<HashIndex> rebuild_hash_index(size_t new_capacity);

    mutable std::mutex _lock;
    scoped_refptr<HashIndex> _index;
    struct VersionInfo {
        VersionInfo(uint64_t version, uint64_t size) : version(version), size(size) {}
        uint64_t version = 0;
        uint64_t size = 0;
    };
    std::vector<VersionInfo> _versions;
    // Map from cid to column
    std::vector<scoped_refptr<Column>> _columns;

    // Temporary write state variables
    scoped_refptr<Schema> _schema;
    size_t _row_size = 0;
    // If a copy-on-write is performed on HashIndex, this variable holds
    // the new reference, otherwise it holds the same reference as _index
    scoped_refptr<HashIndex> _write_index;
    // Map from cid to current writers
    std::vector<std::unique_ptr<ColumnWriter>> _writers;
    // Temporary variable to reuse hash entry vector
    std::vector<uint32_t> _temp_hash_entries;
    // Write stats
    double _write_start = 0;
    size_t _num_insert = 0;
    size_t _num_update = 0;
    size_t _num_update_cell = 0;

    DISALLOW_COPY_AND_ASSIGN(MemSubTablet);
};

} // namespace memory
} // namespace doris
