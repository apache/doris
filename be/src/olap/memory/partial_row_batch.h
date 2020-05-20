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

// A chunk of memory that stores a batch of serialized partial rows
//
// Serialization format for a batch:
// 4 byte len | serialized partial row
// 4 byte len | serialized partial row
// ...
// 4 byte len | serialized partial row
//
// Serialization format for a partial row
// bit vector(se + null) byte size (2 byte) |
// bit vector mark set cells |
// bit vector mark nullable cells' null value |
// 8bit padding
// serialized not null cells
//
// Note: currently only fixed length column types are supported. All length and scalar types store
// in native byte order(little endian in x86-64).
//
// Note: The serialization format is simple, it only provides basic functionalities
// so we can quickly complete the whole create/read/write pipeline. The format may change
// as the project evolves.
class PartialRowBatch {
public:
    static const size_t DEFAULT_BYTE_CAPACITY = 1 << 20;
    static const size_t DEFAULT_ROW_CAPACIT = 1 << 16;

    PartialRowBatch(scoped_refptr<Schema>* schema, size_t byte_capacity = DEFAULT_BYTE_CAPACITY,
                    size_t row_capacity = DEFAULT_ROW_CAPACIT);
    ~PartialRowBatch();

    const Schema& schema() const { return *_schema.get(); }

    size_t row_size() const { return _row_offsets.size(); }
    size_t row_capacity() const { return _row_capacity; }
    size_t byte_size() const { return _bsize; }
    size_t byte_capacity() const { return _byte_capacity; }

    const uint8_t* get_row(size_t idx) const;

private:
    friend class PartialRowWriter;
    friend class PartialRowReader;
    scoped_refptr<Schema> _schema;
    vector<uint32_t> _row_offsets;
    uint8_t* _data;
    size_t _bsize;
    size_t _byte_capacity;
    size_t _row_capacity;
};

// Writer for PartialRowBatch
class PartialRowWriter {
public:
    explicit PartialRowWriter(const Schema& schema);
    ~PartialRowWriter();

    void start_row();
    Status write_row_to_batch(PartialRowBatch* batch);

    // set cell value by column name
    // param data's memory must remain valid before calling build
    Status set(const string& col, const void* data);

    // set cell value by column id
    // param data's memory must remain valid before calling build
    Status set(uint32_t cid, const void* data);

    // set this row is delete operation
    Status set_delete();

private:
    size_t byte_size() const;
    Status write(uint8_t** ppos);

    const Schema& _schema;
    struct CellInfo {
        CellInfo() = default;
        uint32_t isset = 0;
        uint32_t isnullable = 0;
        const uint8_t* data = nullptr;
    };
    vector<CellInfo> _temp_cells;
    size_t _bit_set_size;
    size_t _bit_null_size;
};

// Reader for PartialRowBatch
class PartialRowReader {
public:
    explicit PartialRowReader(const PartialRowBatch& batch);
    // Return number of row operations
    size_t size() const { return _batch.row_size(); }
    // Read row with index idx
    Status read(size_t idx);
    // Get row operation cell count
    size_t cell_size() const { return _cells.size(); }
    // Get row operation cell by index idx, return ColumnSchema and data pointer
    Status get_cell(size_t idx, const ColumnSchema** cs, const void** data) const;

private:
    const PartialRowBatch& _batch;
    const Schema& _schema;
    bool _delete = false;
    size_t _bit_set_size;
    struct CellInfo {
        CellInfo(uint32_t cid, const void* data)
                : cid(cid), data(reinterpret_cast<const uint8_t*>(data)) {}
        uint32_t cid = 0;
        const uint8_t* data = nullptr;
    };
    vector<CellInfo> _cells;
};
//
} // namespace memory
} // namespace doris
