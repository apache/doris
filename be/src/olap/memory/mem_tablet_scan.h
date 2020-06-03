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
#include "olap/memory/row_block.h"
#include "olap/memory/schema.h"

namespace doris {
namespace memory {

class ScanSpec {
public:
    ScanSpec(vector<std::string>&& columns, uint64_t version = UINT64_MAX,
             uint64_t limit = UINT64_MAX)
            : _version(version), _limit(limit), _columns(std::move(columns)) {}

    uint64_t version() const { return _version; }

    uint64_t limit() const { return _limit; }

    const vector<std::string> columns() const { return _columns; }

private:
    friend class MemTablet;

    uint64_t _version = UINT64_MAX;
    uint64_t _limit = UINT64_MAX;
    vector<std::string> _columns;
};

class HashIndex;
class MemTablet;
class ColumnReader;

// Scanner for MemTablet
class MemTabletScan {
public:
    ~MemTabletScan();

    // Get next row_block, it will remain valid until next call to next_block.
    Status next_block(const RowBlock** block);

private:
    friend class MemTablet;

    MemTabletScan(std::shared_ptr<MemTablet>&& tablet, std::unique_ptr<ScanSpec>* spec,
                  size_t num_rows, std::vector<std::unique_ptr<ColumnReader>>* readers);

    std::shared_ptr<MemTablet> _tablet;
    const Schema* _schema = nullptr;
    std::unique_ptr<ScanSpec> _spec;

    size_t _num_rows = 0;
    size_t _num_blocks = 0;
    // full scan support
    std::vector<std::unique_ptr<ColumnReader>> _readers;

    // returned block
    std::unique_ptr<RowBlock> _row_block;
    size_t _next_block = 0;

    DISALLOW_COPY_AND_ASSIGN(MemTabletScan);
};

} // namespace memory
} // namespace doris
