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

#include "olap/memory/column_delta.h"

namespace doris {
namespace memory {

size_t ColumnDelta::memory() const {
    return _index->memory() + _nulls.bsize() + _data.bsize();
}

Status ColumnDelta::alloc(size_t nblock, size_t size, size_t esize, bool has_null) {
    if (_data || _nulls) {
        LOG(FATAL) << "reinit column delta";
    }
    _index.reset(new DeltaIndex());
    _index->block_ends().resize(nblock, 0);
    Status ret = _index->data().alloc(size * sizeof(uint16_t));
    if (!ret.ok()) {
        return ret;
    }
    ret = _data.alloc(size * esize);
    if (!ret.ok()) {
        return ret;
    }
    if (has_null) {
        ret = _nulls.alloc(size);
        if (!ret.ok()) {
            _data.clear();
            return Status::MemoryAllocFailed("init column delta nulls");
        }
        _nulls.set_zero();
    }
    _data.set_zero();
    _size = size;
    return Status::OK();
}

} // namespace memory
} // namespace doris
