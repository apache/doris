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
// User can iterate through all the partial rows, get each partial row's cells.
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
// Example usage:
// PartialRowBatch rb(&schema);
// rb.load(buffer);
// while (true) {
//     bool has;
//     rb.next(&has);
//     if (!has) break;
//     for (size_t j=0; j < reader.cell_size(); j++) {
//         const ColumnSchema* cs = nullptr;
//         const void* data = nullptr;
//         // get column cell type and data
//         rb.get_cell(j, &cs, &data);
//     }
// }
//
// Note: currently only fixed length column types are supported. All length and scalar types store
// in native byte order(little endian in x86-64).
//
// Note: The serialization format is simple, it only provides basic functionalities
// so we can quickly complete the whole create/read/write pipeline. The format may change
// as the project evolves.
class PartialRowBatch {
public:
    explicit PartialRowBatch(scoped_refptr<Schema>* schema);
    ~PartialRowBatch();

    const Schema& schema() const { return *_schema.get(); }

    // Load from a serialized buffer
    Status load(std::vector<uint8_t>&& buffer);

    // Return row count in this batch
    size_t row_size() const { return _row_size; }

    // Iterate to next row, mark has_row to false if there is no more rows
    Status next_row(bool* has_row);

    // Get row operation cell count
    size_t cur_row_cell_size() const { return _cells.size(); }
    // Get row operation cell by index idx, return ColumnSchema and data pointer
    Status cur_row_get_cell(size_t idx, const ColumnSchema** cs, const void** data) const;

private:
    scoped_refptr<Schema> _schema;

    bool _delete = false;
    size_t _bit_set_size = 0;
    struct CellInfo {
        CellInfo(uint32_t cid, const void* data)
                : cid(cid), data(reinterpret_cast<const uint8_t*>(data)) {}
        uint32_t cid = 0;
        const uint8_t* data = nullptr;
    };
    vector<CellInfo> _cells;

    size_t _next_row = 0;
    size_t _row_size = 0;
    const uint8_t* _pos = nullptr;
    std::vector<uint8_t> _buffer;
};

// Writer for PartialRowBatch
//
// Example usage:
// scoped_refptr<Schema> sc;
// Schema::create("id int,uv int,pv int,city tinyint null", &sc);
// PartialRowWriter writer(&sc);
// writer.start_batch();
// for (auto& row : rows) {
//     writer.start_row();
//     writer.set("column_name", value);
//     ...
//     writer.set(column_id, value);
//     writer.end_row();
// }
// vector<uint8_t> buffer;
// writer.finish_batch(&buffer);
class PartialRowWriter {
public:
    static const size_t DEFAULT_BYTE_CAPACITY = 1 << 20;
    static const size_t DEFAULT_ROW_CAPACIT = 1 << 16;

    explicit PartialRowWriter(const scoped_refptr<Schema>& schema);
    ~PartialRowWriter();

    Status start_batch(size_t row_capacity = DEFAULT_ROW_CAPACIT,
                       size_t byte_capacity = DEFAULT_BYTE_CAPACITY);

    // Start writing a new row
    Status start_row();

    // Set cell value by column name
    // param data's memory must remain valid before calling build
    Status set(const string& col, const void* data);

    // Set cell value by column id
    // param data's memory must remain valid before calling build
    Status set(uint32_t cid, const void* data);

    // set this row is delete operation
    Status set_delete();

    // Finish writing a row
    Status end_row();

    // Finish writing the whole ParitialRowBatch, return a serialized buffer
    Status finish_batch(vector<uint8_t>* buffer);

private:
    Status set(const ColumnSchema* cs, uint32_t cid, const void* data);
    size_t byte_size() const;
    Status write(uint8_t** ppos);

    scoped_refptr<Schema> _schema;
    struct CellInfo {
        CellInfo() = default;
        uint32_t isset;
        uint32_t isnullable;
        const uint8_t* data;
    };
    vector<CellInfo> _temp_cells;
    size_t _bit_set_size = 0;
    size_t _bit_nullable_size = 0;
    size_t _row_size = 0;
    size_t _row_capacity = 0;

    std::vector<uint8_t> _buffer;
};

} // namespace memory
} // namespace doris
