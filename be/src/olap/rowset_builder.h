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

#include "olap/ata_writer.h"

namespace doris {

class Rowset;

class RowsetBuilder : public ColumnDataWriter {
public:
    RowsetBuil(OLAPTablePtr table, Rowset* rowset, ColumnDataWriter* writer, bool is_push_write)
        : ColumnDataWriter(is_push_write, table),
        _rowset(rowset),
        _writer(write) {
    }

    virtual ~RowSetBuilder() {
    }

    OLAPStatus init() override {
        return _writer->init();
    }

    OLAPStatus attached_by(RowCursor* row_cursor) override {
        return _writer->attached_by(row_cursor);
    }
    OLAPStatus write(const char* row) override {
        return _writer->write(row);
    }
    OLAPStatus finalize() override {
        return _writer->finalize();
    }
    uint64_t written_bytes() override {
        return _writer->written_bytes();
    }
    MemPool* mem_pool() override {
        return _writer->mem_pool();
    }

    Rowset* rowset() { return _rowset; }
    ColumnDataWriter* writer() { return _writer; }

private:
    Rowset* _rowset;
    ColumnDataWriter* _writer;
};

}
