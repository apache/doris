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
#include "olap/memory/column_writer.h"
#include "olap/memory/typed_column_reader.h"

namespace doris {
namespace memory {

// Used to hold temporary nullable update cells in ColumnWriter
template <class T>
class NullableUpdateType {
public:
    bool& isnull() { return _isnull; }
    T& value() { return _value; }

private:
    bool _isnull;
    T _value;
};

// Used to hold temporary update cells in ColumnWriter
template <class T>
class UpdateType {
public:
    bool& isnull() { return _fake_isnull; /*unused*/ }
    T& value() { return _value; }

private:
    T _value;
    bool _fake_isnull;
};

// ColumnWriter typed implementations
//
// Template type meanings:
// T: column type used for interface
// Nullable: whether column type is nullable
// ST: column type used for internal storage
// UT: type used in map to temporarily store update values
//
// Note: currently only works for int8/int16/int32/int64/int128/float/double
// TODO: add string and other varlen type support
template <class T, bool Nullable = false, class ST = T, class UT = T>
class TypedColumnWriter : public ColumnWriter {
public:
    TypedColumnWriter(Column* column, uint64_t version, uint64_t real_version,
                      vector<ColumnDelta*>&& deltas)
            : _column(column),
              _base(&_column->_base),
              _version(version),
              _real_version(real_version),
              _deltas(std::move(deltas)) {}

    const void* get(const uint32_t rid) const {
        return TypedColumnGet<TypedColumnWriter<T, Nullable, ST, UT>, T, Nullable, ST>(*this, rid);
    }

    uint64_t hashcode(const void* rhs, size_t rhs_idx) const {
        return TypedColumnHashcode<T, ST>(rhs, rhs_idx);
    }

    bool equals(const uint32_t rid, const void* rhs, size_t rhs_idx) const {
        return TypedColumnEquals<TypedColumnWriter<T, Nullable, ST, UT>, T, Nullable, ST>(
                *this, rid, rhs, rhs_idx);
    }

    string debug_string() const {
        return StringPrintf("%s version=%zu(real=%zu) ndelta=%zu insert:%zu update:%zu",
                            _column->debug_string().c_str(), _version, _real_version,
                            _deltas.size(), _num_insert, _num_update);
    }

    Status insert(uint32_t rid, const void* value) {
        uint32_t bid = Column::block_id(rid);
        if (bid >= _base->size()) {
            RETURN_IF_ERROR(add_block());
            // add one block should be enough
            CHECK(bid < _base->size());
        }
        auto& block = (*_base)[bid];
        uint32_t idx = Column::index_in_block(rid);
        DCHECK(idx * sizeof(T) < block->data().bsize());
        if (Nullable) {
            if (value) {
                if (std::is_same<T, ST>::value) {
                    block->set_not_null(idx);
                    block->data().as<ST>()[idx] = *static_cast<const ST*>(value);
                } else {
                    // TODO: string support
                    LOG(FATAL) << "varlen column type not supported";
                }
            } else {
                block->set_null(idx);
            }
        } else {
            DCHECK(value);
            if (std::is_same<T, ST>::value) {
                block->data().as<ST>()[idx] = *static_cast<const ST*>(value);
            } else {
                // TODO: string support
                LOG(FATAL) << "varlen column type not supported";
            }
        }
        _num_insert++;
        return Status::OK();
    }

    Status update(uint32_t rid, const void* value) {
        DCHECK_LT(rid, _base->size() * Column::BLOCK_SIZE);
        if (Nullable) {
            auto& uv = _updates[rid];
            if (value) {
                uv.isnull() = false;
                if (std::is_same<T, ST>::value) {
                    uv.value() = *reinterpret_cast<const T*>(value);
                } else {
                    // TODO: string support
                }
            } else {
                _update_has_null = true;
                uv.isnull() = true;
                if (std::is_same<T, ST>::value) {
                    uv.value() = (T)0;
                } else {
                    // TODO: string support
                }
            }
        } else {
            auto& uv = _updates[rid];
            DCHECK(value);
            if (std::is_same<T, ST>::value) {
                uv.value() = *static_cast<const ST*>(value);
            } else {
                // TODO: string support
            }
        }
        _num_update++;
        return Status::OK();
    }

    Status finalize(uint64_t version) {
        if (_updates.size() == 0) {
            // insert(append) only
            return Status::OK();
        }
        // prepare delta
        size_t nblock = _base->size();
        scoped_refptr<ColumnDelta> delta(new ColumnDelta());
        RETURN_IF_ERROR(delta->alloc(nblock, _updates.size(), sizeof(ST), _update_has_null));
        DeltaIndex* index = delta->index();
        vector<uint32_t>& block_ends = index->block_ends();
        Buffer& idxdata = index->data();
        Buffer& data = delta->data();
        Buffer& nulls = delta->nulls();
        uint32_t cidx = 0;
        uint32_t curbid = 0;
        for (auto& e : _updates) {
            uint32_t rid = e.first;
            uint32_t bid = Column::block_id(rid);
            while (curbid < bid) {
                block_ends[curbid] = cidx;
                curbid++;
            }
            idxdata.as<uint16_t>()[cidx] = Column::index_in_block(rid);
            if (Nullable) {
                bool isnull = e.second.isnull();
                if (isnull) {
                    nulls.as<bool>()[cidx] = true;
                } else {
                    data.as<ST>()[cidx] = e.second.value();
                }
            } else {
                data.as<ST>()[cidx] = e.second.value();
            }
            cidx++;
        }
        while (curbid < nblock) {
            block_ends[curbid] = cidx;
            curbid++;
        }
        _updates.clear();
        RETURN_IF_ERROR(add_delta(std::move(delta), version));
        return Status::OK();
    }

    Status get_new_column(scoped_refptr<Column>* ret) {
        if (ret->get() != _column.get()) {
            DLOG(INFO) << StringPrintf("%s switch new column", _column->debug_string().c_str());
            (*ret).swap(_column);
            _column.reset();
        }
        return Status::OK();
    }

private:
    // Expand base vector, do a copy-on-write
    Status expand_base() {
        size_t added = std::min((size_t)Column::BASE_CAPACITY_MAX_STEP_SIZE, _base->capacity());
        size_t new_base_capacity =
                padding(_base->capacity() + added, Column::BASE_CAPACITY_MIN_STEP_SIZE);
        // check if version needs expanding too
        size_t new_version_capacity = 0;
        if (_column->_versions.size() == _column->_versions.capacity()) {
            new_version_capacity =
                    padding(_column->_versions.capacity() + Column::VERSION_CAPACITY_STEP_SIZE,
                            Column::VERSION_CAPACITY_STEP_SIZE);
        }
        // check pool doesn't need expanding
        DCHECK_EQ(_base->size(), _base->capacity());
        DCHECK(_base->capacity() < new_base_capacity);
        DLOG(INFO) << StringPrintf("%s memory=%.1lfM expand base base=%zu version=%zu",
                                   _column->schema().debug_string().c_str(),
                                   _column->memory() / 1000000.0, new_base_capacity,
                                   new_version_capacity);
        scoped_refptr<Column> cow(
                new Column(*_column.get(), new_base_capacity, new_version_capacity));
        cow.swap(_column);
        _base = &(_column->_base);
        return Status::OK();
    }

    // Try to append a new block to this column, if base vector is at it's capacity,
    // call expand base.
    Status add_block() {
        if (_base->size() == _base->capacity()) {
            RETURN_IF_ERROR(expand_base());
        }
        CHECK_LT(_base->size(), _base->capacity());
        scoped_refptr<ColumnBlock> block(new ColumnBlock());
        RETURN_IF_ERROR(block->alloc(Column::BLOCK_SIZE, sizeof(ST)));
        _base->emplace_back(std::move(block));
        if (_column->schema().cid() == 1) {
            // only log when first column add block
            DLOG(INFO) << StringPrintf("Column(cid=%u) add ColumnBlock %zu/%zu",
                                       _column->schema().cid(), _base->size(), _base->capacity());
        }
        return Status::OK();
    }

    // Expand versions vector, do a copy-on-write
    Status expand_versions() {
        size_t new_capacity =
                padding(_column->_versions.capacity() + Column::VERSION_CAPACITY_STEP_SIZE,
                        Column::VERSION_CAPACITY_STEP_SIZE);
        DLOG(INFO) << StringPrintf("%s memory=%.1lfM expand delta base=%zu version=%zu",
                                   _column->schema().debug_string().c_str(),
                                   _column->memory() / 1000000.0, _base->capacity(), new_capacity);
        scoped_refptr<Column> cow(new Column(*_column.get(), 0, new_capacity));
        cow.swap(_column);
        return Status::OK();
    }

    // Try to add a new delta to versions vector, if versions vector is at it's capcacity,
    // call expand versions
    Status add_delta(scoped_refptr<ColumnDelta>&& delta, uint64_t version) {
        if (_column->_versions.size() == _column->_versions.capacity()) {
            expand_versions();
        }
        DLOG(INFO) << StringPrintf("%s add version %zu update: %zu",
                                   _column->debug_string().c_str(), version, delta->size());
        CHECK_LT(_column->_versions.size(), _column->_versions.capacity());
        _column->_versions.emplace_back();
        Column::VersionInfo& vinfo = _column->_versions.back();
        vinfo.version = version;
        vinfo.delta = delta;
        return Status::OK();
    }

    template <class Reader, class T2, bool Nullable2, class ST2>
    friend const void* TypedColumnGet(const Reader& reader, const uint32_t rid);

    template <class T2, class ST2>
    friend bool TypedColumnHashcode(const void*, size_t);

    template <class Reader, class T2, bool Nullable2, class ST2>
    friend bool TypedColumnEquals(const Reader&, const uint32_t, const void*, size_t);

    // The following members need to be identical to TypedColumnReader
    // because they share same reader methods: get/hashcode/equals
    scoped_refptr<Column> _column;
    vector<scoped_refptr<ColumnBlock>>* _base;
    uint64_t _version;
    uint64_t _real_version;
    vector<ColumnDelta*> _deltas;

    size_t _num_insert = 0;
    size_t _num_update = 0;
    bool _update_has_null = false;
    typedef typename std::conditional<Nullable, NullableUpdateType<UT>, UpdateType<UT>>::type
            UpdateMapType;
    // Temporary stoarage to hold all updated cells' values
    // rowid -> updated values
    std::map<uint32_t, UpdateMapType> _updates;
};

} // namespace memory
} // namespace doris
