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

#include "olap/memory/column_reader.h"
#include "util/hash_util.hpp"

namespace doris {
namespace memory {

// Those methods need to be shared by reader/writer, so extract them as template functions

// Get Column's storage cell address by rowid.
//
// Template type meanings:
// RW: Reader or Writer object type
// T: column type used for interface
// Nullable: whether column type is nullable
// ST: column type used for internal storage
template <class RW, class T, bool Nullable, class ST>
inline const void* TypedColumnGet(const RW& rw, const uint32_t rid) {
    for (ssize_t i = rw._deltas.size() - 1; i >= 0; i--) {
        ColumnDelta* pdelta = rw._deltas[i];
        uint32_t pos = pdelta->find_idx(rid);
        if (pos != DeltaIndex::npos) {
            if (Nullable) {
                bool isnull = pdelta->nulls() && pdelta->nulls().as<bool>()[pos];
                if (isnull) {
                    return nullptr;
                } else {
                    return &(pdelta->data().template as<ST>()[pos]);
                }
            } else {
                return &(pdelta->data().template as<ST>()[pos]);
            }
        }
    }
    uint32_t bid = Column::block_id(rid);
    DCHECK(bid < rw._base->size());
    uint32_t idx = Column::index_in_block(rid);
    DCHECK(idx * sizeof(ST) < (*rw._base)[bid]->data().bsize());
    if (Nullable) {
        bool isnull = (*rw._base)[bid]->is_null(idx);
        if (isnull) {
            return nullptr;
        } else {
            return &((*rw._base)[bid]->data().template as<ST>()[idx]);
        }
    } else {
        return &((*rw._base)[bid]->data().template as<ST>()[idx]);
    }
}

// Get a cell's hashcode of a typed array
//
// Template type meanings:
// T: column type used for interface
// ST: column type used for internal storage
template <class T, class ST>
inline uint64_t TypedColumnHashcode(const void* rhs, size_t rhs_idx) {
    if (std::is_same<T, ST>::value) {
        const T* prhs = ((const T*)rhs) + rhs_idx;
        return HashUtil::fnv_hash64(prhs, sizeof(T), 0);
    } else {
        // TODO: support other type's hash
        return 0;
    }
}

// Compare equality of a typed array's cell with this column's cell
//
// Template type meanings:
// RW: Reader/Writer object type
// T: column type used for interface
// Nullable: whether column type is nullable
// ST: column type used for internal storage
template <class RW, class T, bool Nullable, class ST>
bool TypedColumnEquals(const RW& rw, const uint32_t rid, const void* rhs, size_t rhs_idx) {
    const T& rhs_value = ((const T*)rhs)[rhs_idx];
    for (ssize_t i = rw._deltas.size() - 1; i >= 0; i--) {
        ColumnDelta* pdelta = rw._deltas[i];
        uint32_t pos = pdelta->find_idx(rid);
        if (pos != DeltaIndex::npos) {
            if (Nullable) {
                CHECK(false) << "only used for key column";
                return false;
            } else {
                return (pdelta->data().template as<T>()[pos]) == rhs_value;
            }
        }
    }
    uint32_t bid = Column::block_id(rid);
    DCHECK(bid < rw._base->size());
    uint32_t idx = Column::index_in_block(rid);
    DCHECK(idx * sizeof(ST) < (*rw._base)[bid]->data().bsize());
    if (Nullable) {
        CHECK(false) << "only used for key column";
        return false;
    } else {
        DCHECK(rhs);
        return ((*rw._base)[bid]->data().template as<T>()[idx]) == rhs_value;
    }
}

// ColumnReader typed implementations
// currently only works for int8/int16/int32/int64/int128/float/double
//
// Template type meanings:
// T: column type used for interface
// Nullable: whether column type is nullable
// ST: column type used for internal storage
//
// For fixed size scalar types(int/float), T should be the same as ST
// For var size typse(string), when using dict encoding, ST may be integer
//
// TODO: add string and other varlen type support
template <class T, bool Nullable = false, class ST = T>
class TypedColumnReader : public ColumnReader {
public:
    TypedColumnReader(Column* column, uint64_t version, uint64_t real_version,
                      vector<ColumnDelta*>&& deltas)
            : _column(column),
              _base(&_column->_base),
              _version(version),
              _real_version(real_version),
              _deltas(std::move(deltas)) {}

    const void* get(const uint32_t rid) const {
        return TypedColumnGet<TypedColumnReader<T, Nullable, ST>, T, Nullable, ST>(*this, rid);
    }

    // Get a whole block's content specified by blockid(bid), and return to cbh.
    //
    // Caller needs to specify number of rows by parameter nrows, nrows should be equal to 64K
    // if the block is not the last block, nrows should be less or equal to 64K if the block
    // is the last block, and since Column object doesn't store information about number of
    // rows(it is stored in MemSubTablet), so it needs to be provided by caller.
    Status get_block(size_t nrows, size_t bid, ColumnBlockHolder* cbh) const {
        // Check if this block has any updates in delta
        bool base_only = true;
        for (size_t i = 0; i < _deltas.size(); ++i) {
            if (_deltas[i]->contains_block(bid)) {
                base_only = false;
                break;
            }
        }
        auto& block = (*_base)[bid];
        if (base_only) {
            // If no updates, just reference the base block, no need to copy data
            cbh->init(block.get(), false);
            return Status::OK();
        }
        // Have updates, need a new block buffer, copy base data and apply updates
        if (!cbh->own() || cbh->get()->size() < nrows) {
            // need to create new column block
            cbh->release();
            cbh->init(new ColumnBlock(), true);
            cbh->get()->alloc(nrows, sizeof(T));
        }
        // Copy base to block buffer
        ColumnBlock& cb = *cbh->get();
        RETURN_IF_ERROR(block->copy_to(&cb, nrows, sizeof(ST)));
        // Apply updates
        for (auto delta : _deltas) {
            uint32_t start, end;
            delta->index()->block_range(bid, &start, &end);
            if (end == start) {
                continue;
            }
            const uint16_t* poses = (const uint16_t*)delta->index()->data().data();
            const ST* data = delta->data().as<ST>();
            if (Nullable) {
                if (delta->nulls()) {
                    const bool* nulls = delta->nulls().as<bool>();
                    for (uint32_t i = start; i < end; i++) {
                        uint16_t pos = poses[i];
                        bool isnull = nulls[i];
                        if (isnull) {
                            cb.nulls().as<bool>()[pos] = true;
                        } else {
                            cb.nulls().as<bool>()[pos] = false;
                            cb.data().as<ST>()[pos] = data[i];
                        }
                    }
                } else {
                    for (uint32_t i = start; i < end; i++) {
                        uint16_t pos = poses[i];
                        cb.nulls().as<bool>()[pos] = true;
                        cb.data().as<ST>()[pos] = data[i];
                    }
                }
            } else {
                for (uint32_t i = start; i < end; i++) {
                    cb.data().as<ST>()[poses[i]] = data[i];
                }
            }
        }
        return Status::OK();
    }

    uint64_t hashcode(const void* rhs, size_t rhs_idx) const {
        return TypedColumnHashcode<T, ST>(rhs, rhs_idx);
    }

    bool equals(const uint32_t rid, const void* rhs, size_t rhs_idx) const {
        return TypedColumnEquals<TypedColumnReader<T, Nullable, ST>, T, Nullable, ST>(*this, rid,
                                                                                      rhs, rhs_idx);
    }

    string debug_string() const {
        return StringPrintf("%s version=%zu(real=%zu) ndelta=%zu", _column->debug_string().c_str(),
                            _version, _real_version, _deltas.size());
    }

private:
    template <class Reader, class T2, bool Nullable2, class ST2>
    friend const void* TypedColumnGet(const Reader& rw, const uint32_t rid);

    template <class T2, class ST2>
    friend bool TypedColumnHashcode(const void*, size_t);

    template <class Reader, class T2, bool Nullable2, class ST2>
    friend bool TypedColumnEquals(const Reader&, const uint32_t, const void*, size_t);

    scoped_refptr<Column> _column;
    vector<scoped_refptr<ColumnBlock>>* _base;
    uint64_t _version;
    uint64_t _real_version;
    vector<ColumnDelta*> _deltas;
};

} // namespace memory
} // namespace doris
