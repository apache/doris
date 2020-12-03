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

#include "olap/memory/typed_column_reader.h"
#include "olap/memory/typed_column_writer.h"

namespace doris {
namespace memory {

Column::Column(const ColumnSchema& cs, ColumnType storage_type, uint64_t version)
        : _cs(cs), _storage_type(storage_type), _base_idx(0) {
    _base.reserve(BASE_CAPACITY_MIN_STEP_SIZE);
    _versions.reserve(VERSION_CAPACITY_STEP_SIZE);
    _versions.emplace_back(version);
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

std::string Column::debug_string() const {
    return StringPrintf("Column(%s version=%zu)", _cs.debug_string().c_str(),
                        _versions.back().version);
}

Status Column::capture_version(uint64_t version, std::vector<ColumnDelta*>* deltas,
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
        for (ssize_t i = static_cast<ssize_t>(_base_idx) - 1; i >= 0; i--) {
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

void Column::capture_latest(vector<ColumnDelta*>* deltas, uint64_t* version) const {
    deltas->reserve(_versions.size() - _base_idx - 1);
    for (size_t i = _base_idx + 1; i < _versions.size(); i++) {
        deltas->emplace_back(_versions[i].delta.get());
    }
    *version = _versions.back().version;
}

Status Column::create_reader(uint64_t version, std::unique_ptr<ColumnReader>* reader) {
    ColumnType type = schema().type();
    bool nullable = schema().is_nullable();
    std::vector<ColumnDelta*> deltas;
    uint64_t real_version;
    RETURN_IF_ERROR(capture_version(version, &deltas, &real_version));

#define CREATE_READER(T)                                                                          \
    if (nullable) {                                                                               \
        (*reader).reset(                                                                          \
                new TypedColumnReader<T, true>(this, version, real_version, std::move(deltas)));  \
    } else {                                                                                      \
        (*reader).reset(                                                                          \
                new TypedColumnReader<T, false>(this, version, real_version, std::move(deltas))); \
    }

    switch (type) {
    case OLAP_FIELD_TYPE_BOOL:
    case OLAP_FIELD_TYPE_TINYINT:
        CREATE_READER(int8_t)
        break;
    case OLAP_FIELD_TYPE_SMALLINT:
        CREATE_READER(int16_t)
        break;
    case OLAP_FIELD_TYPE_INT:
        CREATE_READER(int32_t)
        break;
    case OLAP_FIELD_TYPE_BIGINT:
        CREATE_READER(int64_t)
        break;
    case OLAP_FIELD_TYPE_LARGEINT:
        CREATE_READER(int128_t)
        break;
    case OLAP_FIELD_TYPE_FLOAT:
        CREATE_READER(float)
        break;
    case OLAP_FIELD_TYPE_DOUBLE:
        CREATE_READER(double)
        break;
    default:
        return Status::NotSupported("create column reader: type not supported");
    }
#undef CREATE_READER
    return Status::OK();
}

Status Column::create_writer(std::unique_ptr<ColumnWriter>* writer) {
    ColumnType type = schema().type();
    bool nullable = schema().is_nullable();
    std::vector<ColumnDelta*> deltas;
    uint64_t real_version;
    capture_latest(&deltas, &real_version);

#define CREATE_WRITER(T)                                                                  \
    if (nullable) {                                                                       \
        (*writer).reset(new TypedColumnWriter<T, true>(this, real_version, real_version,  \
                                                       std::move(deltas)));               \
    } else {                                                                              \
        (*writer).reset(new TypedColumnWriter<T, false>(this, real_version, real_version, \
                                                        std::move(deltas)));              \
    }

    switch (type) {
    case OLAP_FIELD_TYPE_BOOL:
    case OLAP_FIELD_TYPE_TINYINT:
        CREATE_WRITER(int8_t)
        break;
    case OLAP_FIELD_TYPE_SMALLINT:
        CREATE_WRITER(int16_t)
        break;
    case OLAP_FIELD_TYPE_INT:
        CREATE_WRITER(int32_t)
        break;
    case OLAP_FIELD_TYPE_BIGINT:
        CREATE_WRITER(int64_t)
        break;
    case OLAP_FIELD_TYPE_LARGEINT:
        CREATE_WRITER(int128_t)
        break;
    case OLAP_FIELD_TYPE_FLOAT:
        CREATE_WRITER(float)
        break;
    case OLAP_FIELD_TYPE_DOUBLE:
        CREATE_WRITER(double)
        break;
    default:
        return Status::NotSupported("create column writer: type not supported");
    }
#undef CREATE_WRITER
    return Status::OK();
}

} // namespace memory
} // namespace doris
