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

#include "olap/fs/fs_util.h"
#include "olap/memtable.h"
#include "olap/schema.h"

namespace doris {
namespace memory {

extern const char* k_wtd_magic;
extern const uint32_t k_wtd_magic_length;

// WriteTxnData MemTable
// Write WTD files to disk. Files' emporary path is 'tablet->tablet_path()/{txn_id}_{segment_id}.wtd', need to evolve.
class WTDMemTable : public MemTable {
public:
    WTDMemTable(const doris::Schema* schema, fs::WritableBlock* wblock);
    int64_t tablet_id() const override { return 0; }
    size_t memory_usage() const override { return 0; }
    void insert(const void* data) override;
    OLAPStatus flush() override;
    OLAPStatus close() override { return OLAPStatus::OLAP_SUCCESS; }

private:
    const doris::Schema* _schema;
    fs::WritableBlock* _wblock;

    int64_t _num_rows{0};
    std::vector<uint8_t> _buffer;
};
} // namespace memory
} // namespace doris