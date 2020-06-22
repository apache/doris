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

#include "olap/memory/column.h"

namespace doris {
namespace memory {

// Holder for a ColumnBlock
// Although ColumnBlock support reference counting, we avoid using it because
// a reader already hold a proxy to all the ColumnBlocks, it's unnecessary
// and inefficient to inc/dec refcount, so we use this holder instead.
//
// If the underlying column data doesn't need merge-on-read, we can use the
// underlying base's ColumnBlock directly, and _own_cb equals false.
//
// If there is any deltas need to be merged, a new ColumnBlock will be
// created, and _own_cb equals true.
//
// Note: this class is only intended for single-thread single reader usage.
class ColumnBlockHolder {
public:
    ColumnBlockHolder() {}
    ColumnBlockHolder(ColumnBlock* cb, bool own) : _cb(cb), _own_cb(own) {}

    void init(ColumnBlock* cb, bool own) {
        release();
        _cb = cb;
        _own_cb = own;
    }

    void release() {
        if (_own_cb) {
            // use delete _cb directly will cause DCHECK fail in DEBUG mode
            // so we use scoped_refptr to free _cb instead
            scoped_refptr<ColumnBlock> ref(_cb);
            _own_cb = false;
        }
        _cb = nullptr;
    }

    ColumnBlock* get() const { return _cb; }

    bool own() const { return _own_cb; }

    ~ColumnBlockHolder() { release(); }

private:
    ColumnBlock* _cb = nullptr;
    bool _own_cb = false;
};

// Read only column reader, captures a specific version of a Column
class ColumnReader {
public:
    virtual ~ColumnReader() {}

    // Get cell by rid, caller needs to make sure rid is in valid range
    virtual const void* get(const uint32_t rid) const = 0;

    // Get whole block by block id, caller needs to make sure block is in valid range
    virtual Status get_block(size_t nrows, size_t block, ColumnBlockHolder* cbh) const = 0;

    // Borrow a vtable slot to do typed hashcode calculation, mainly used to find
    // row by rowkey using hash index.
    //
    // It's designed to support array operator, so there are two parameters for user
    // to pass array start and array index.
    virtual uint64_t hashcode(const void* rhs, size_t rhs_idx) const = 0;

    // Check cell equality, mainly used to find row by rowkey using hash index.
    virtual bool equals(const uint32_t rid, const void* rhs, size_t rhs_idx) const = 0;

    virtual string debug_string() const = 0;
};

} // namespace memory
} // namespace doris
