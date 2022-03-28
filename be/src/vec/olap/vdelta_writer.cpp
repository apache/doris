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

#include "vdelta_writer.h"
#include "olap/storage_engine.h"
#include "olap/memtable.h"

namespace doris {

namespace vectorized {

VDeltaWriter::VDeltaWriter(WriteRequest* req, StorageEngine* storage_engine)
        : DeltaWriter(req, storage_engine) {}

VDeltaWriter::~VDeltaWriter() {

}

OLAPStatus VDeltaWriter::open(WriteRequest* req, VDeltaWriter** writer) {
    *writer = new VDeltaWriter(req, StorageEngine::instance());
    return OLAP_SUCCESS;
}

OLAPStatus VDeltaWriter::write_block(const vectorized::Block* block, const std::vector<int>& row_idxs) {
    if (UNLIKELY(row_idxs.empty())) {
        return OLAP_SUCCESS;
    }
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init && !_is_cancelled) {
        RETURN_NOT_OK(init());
    }

    if (_is_cancelled) {
        return OLAP_ERR_ALREADY_CANCELLED;
    }

    int start = 0, end = 0;

    const size_t num_rows = row_idxs.size();
    for (; start < num_rows;) {
        auto count = end + 1 - start;
        if (end == num_rows - 1 || (row_idxs[end + 1] - row_idxs[start]) != count) {
            _mem_table->insert(block, row_idxs[start], count);
            start += count;
            end = start;
        } else {
            end++;
        }
    }

    if (_mem_table->memory_usage() >= config::write_buffer_size) {
        RETURN_NOT_OK(_flush_memtable_async());
        _reset_mem_table();
    }

    return OLAP_SUCCESS;
}

void VDeltaWriter::_reset_mem_table() {
    _mem_table.reset(new MemTable(_tablet->tablet_id(), _schema.get(), _tablet_schema, _req.slots,
                                  _req.tuple_desc, _tablet->keys_type(), _rowset_writer.get(),
                                  _mem_tracker, true));
}

} // namespace vectorized

} // namespace doris
