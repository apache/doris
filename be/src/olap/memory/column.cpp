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

#include "olap/memory/column.h"

namespace doris {
namespace memory {

Column::Column(const ColumnSchema& cs, ColumnType storage_type, uint64_t version)
        : _cs(cs), _storage_type(storage_type), _base_idx(0) {
    _base.reserve(64);
    _versions.reserve(64);
    _versions.emplace_back(version);
    DLOG(INFO) << "create %s" << debug_string();
}

Column::Column(const Column& rhs, size_t new_base_capacity, size_t new_version_capacity)
        : _cs(rhs._cs), _storage_type(rhs._storage_type), _base_idx(rhs._base_idx) {
    _base.reserve(std::max(new_base_capacity, rhs._base.capacity()));
    _base.resize(rhs._base.size());
    for (size_t i = 0; i < _base.size(); i++) {
        _base[i] = rhs._base[i];
    }
    _versions.reserve(std::max(new_version_capacity, rhs._versions.capacity()));
    _versions.resize(rhs._versions.size());
    for (size_t i = 0; i < _versions.size(); i++) {
        _versions[i] = rhs._versions[i];
    }
}

size_t Column::memory() const {
    size_t bs = _base.size();
    size_t ds = _versions.size();
    size_t base_memory = 0;
    for (size_t i = 0; i < bs; i++) {
        base_memory += _base[i]->memory();
    }
    size_t delta_memory = 0;
    for (size_t i = 0; i < ds; i++) {
        if (_versions[i].delta) {
            delta_memory += _versions[i].delta->memory();
        }
    }
    return base_memory + delta_memory;
}

string Column::debug_string() const {
    return StringPrintf("Column(%s version=%zu)", _cs.debug_string().c_str(),
                        _versions.back().version);
}

Status Column::capture_version(uint64_t version, vector<ColumnDelta*>* deltas,
                               uint64_t* real_version) const {
    uint64_t base_version = _versions[_base_idx].version;
    *real_version = base_version;
    if (version < base_version) {
        uint64_t oldest = _versions[0].version;
        if (version < oldest) {
            return Status::NotFound(
                    StringPrintf("version %zu(oldest=%zu) deleted", version, oldest));
        }
        DCHECK_GT(_base_idx, 0);
        for (ssize_t i = _base_idx - 1; i >= 0; i--) {
            uint64_t v = _versions[i].version;
            if (v >= version) {
                DCHECK(_versions[i].delta);
                *real_version = v;
                deltas->emplace_back(_versions[i].delta.get());
                if (v == version) {
                    break;
                }
            } else {
                break;
            }
        }
    } else if (version > base_version) {
        size_t vsize = _versions.size();
        for (size_t i = _base_idx + 1; i < vsize; i++) {
            uint64_t v = _versions[i].version;
            if (v <= version) {
                DCHECK(_versions[i].delta);
                *real_version = v;
                deltas->emplace_back(_versions[i].delta.get());
                if (v == version) {
                    break;
                }
            } else {
                break;
            }
        }
    }
    return Status::OK();
}

void Column::capture_latest(vector<ColumnDelta*>* deltas) const {
    deltas->reserve(_versions.size() - _base_idx - 1);
    for (size_t i = _base_idx + 1; i < _versions.size(); i++) {
        deltas->emplace_back(_versions[i].delta.get());
    }
}

Status Column::read(uint64_t version, std::unique_ptr<ColumnReader>* reader) {
    return Status::NotSupported("not supported");
}

Status Column::write(std::unique_ptr<ColumnWriter>* writer) {
    return Status::NotSupported("not supported");
}

} // namespace memory
} // namespace doris
