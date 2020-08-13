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

#include <stdint.h>

#include <atomic>
#include <string>
#include <vector>

#include "gutil/ref_counted.h"

namespace doris {
namespace memory {

struct HashChunk;

// A hash index, which maps uint64_t hashcode(key) to uint32_t rowid(value).
//
// From the user's perspective, it doesn't store row-key values, so when user
// lookup a row-key, user need to first calculate row-key's hashcode, then
// query the hash index to get a list of candidate rowids, then check all
// the candidates rowids for equality.
//
// Note: this hash index do not support delete.
//
// Note: This hash index is thread-safe, but it only support
// single-writer/multi-reader style concurrency. When hash index reaches it's
// capacity, a writer need to expand&rebuild, this is not thread-safe, so the
// writer need to do a copy-on-write, and make it's owner to link to the new
// reference, while readers still reading the old reference.
//
// To add rowkeys into hash index, typical usage may look like:
// vector<uint32_t> entries;
// uint64_t hashcode = hashcode(rowkey)
// uint64_t new_entry_pos = hashIndex.find(hashcode, &entries)
// bool found = false;
// for (auto rowid : entries) {
//     if (rowkey == get_row_key(rowid)) {
//          // found existing rowkey
//          found = true;
//          break;
//     }
// }
// if (!found) {
//     hashIndex.set(new_entry_pos, hashcode, new_row_id);
// }
//
// Note: for more info, please refer to:
// https://engineering.fb.com/developer-tools/f14/
class HashIndex : public RefCountedThreadSafe<HashIndex> {
public:
    static const uint64_t npos = (uint64_t)-1;

    // Create hash index with capacity
    explicit HashIndex(size_t capacity);
    ~HashIndex();

    // Return number of elements
    size_t size() const { return _size; }

    // Return max number of elements this hash index can hold
    // If size >= max_size, this hash index needs to expand and rebuild
    size_t max_size() const { return _max_size; }

    // Find by key hash, put all candidate values into entries,
    // and return a entry position, so later user can use this position to
    // add a value with the same key hash directly into this hash index.
    uint64_t find(uint64_t key_hash, std::vector<uint32_t>* entries) const;

    // Set a value with hash key_hash, at a entry position returned by find
    void set(uint64_t entry_pos, uint64_t key_hash, uint32_t value);

    // Add a value with hash key_hash
    bool add(uint64_t key_hash, uint32_t value);

    // return true if this hash index needs rebuild
    bool need_rebuild() const { return _size >= _max_size; }

    // dump debug information
    const std::string dump() const;

private:
    std::atomic<size_t> _size;
    size_t _max_size;
    size_t _num_chunks;
    size_t _chunk_mask;
    HashChunk* _chunks;
};

} // namespace memory
} // namespace doris
