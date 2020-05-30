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

#include "olap/memory/column_block.h"

namespace doris {
namespace memory {

size_t ColumnBlock::memory() const {
    return _data.bsize() + _nulls.bsize();
}

Status ColumnBlock::alloc(size_t size, size_t esize) {
    if (_data || _nulls) {
        LOG(FATAL) << "reinit column page";
    }
    RETURN_IF_ERROR(_data.alloc(size * esize));
    _data.set_zero();
    _size = size;
    return Status::OK();
}

Status ColumnBlock::set_null(uint32_t idx) {
    if (!_nulls) {
        RETURN_IF_ERROR(_nulls.alloc(_size));
        _nulls.set_zero();
    }
    _nulls.as<bool>()[idx] = true;
    return Status::OK();
}

Status ColumnBlock::set_not_null(uint32_t idx) {
    if (_nulls) {
        _nulls.as<bool>()[idx] = false;
    }
    return Status::OK();
}

Status ColumnBlock::copy_to(ColumnBlock* dest, size_t size, size_t esize) {
    if (size > dest->size()) {
        return Status::InvalidArgument("ColumnBlock copy to a smaller ColumnBlock");
    }
    if (dest->nulls()) {
        if (nulls()) {
            memcpy(dest->nulls().data(), nulls().data(), size);
        } else {
            memset(dest->nulls().data(), 0, size);
        }
    } else if (nulls()) {
        RETURN_IF_ERROR(dest->nulls().alloc(dest->size()));
        memcpy(dest->nulls().data(), nulls().data(), size);
    }
    memcpy(dest->data().data(), data().data(), size * esize);
    return Status::OK();
}

} // namespace memory
} // namespace doris
