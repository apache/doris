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

#include "olap/memory/mem_sub_tablet.h"

#include "olap/memory/column.h"
#include "olap/memory/column_reader.h"
#include "olap/memory/column_writer.h"
#include "olap/memory/hash_index.h"
#include "olap/memory/partial_row_batch.h"
#include "olap/memory/schema.h"

namespace doris {
namespace memory {

Status MemSubTablet::create(uint64_t version, const Schema& schema,
                            std::unique_ptr<MemSubTablet>* ret) {
    std::unique_ptr<MemSubTablet> tmp(new MemSubTablet());
    tmp->_versions.reserve(64);
    tmp->_versions.emplace_back(version, 0);
    tmp->_columns.resize(schema.cid_size());
    for (size_t i = 0; i < schema.num_columns(); i++) {
        // TODO: support storage_type != c.type
        auto& c = *schema.get(i);
        if (!supported(c.type())) {
            return Status::NotSupported("column type not supported");
        }
        tmp->_columns[c.cid()].reset(new Column(c, c.type(), version));
    }
    tmp.swap(*ret);
    return Status::OK();
}

MemSubTablet::MemSubTablet() : _index(new HashIndex(1 << 16)) {}

MemSubTablet::~MemSubTablet() {}

Status MemSubTablet::get_size(uint64_t version, size_t* size) const {
    std::lock_guard<std::mutex> lg(_lock);
    if (version == static_cast<uint64_t>(-1)) {
        // get latest
        *size = _versions.back().size;
        return Status::OK();
    }
    if (_versions[0].version > version) {
        return Status::NotFound("get_size failed, version too old");
    }
    for (size_t i = 1; i < _versions.size(); i++) {
        if (_versions[i].version > version) {
            *size = _versions[i - 1].size;
            return Status::OK();
        }
    }
    *size = _versions.back().size;
    return Status::OK();
}

Status MemSubTablet::read_column(uint64_t version, uint32_t cid,
                                 std::unique_ptr<ColumnReader>* reader) {
    scoped_refptr<Column> cl;
    {
        std::lock_guard<std::mutex> lg(_lock);
        if (cid < _columns.size()) {
            cl = _columns[cid];
        }
    }
    if (cl.get() == nullptr) {
        return Status::NotFound("column not found");
    }
    return cl->create_reader(version, reader);
}

Status MemSubTablet::get_index_to_read(scoped_refptr<HashIndex>* index) {
    *index = _index;
    return Status::OK();
}

Status MemSubTablet::begin_write(scoped_refptr<Schema>* schema) {
    if (_schema != nullptr) {
        return Status::InternalError("Another write is in-progress or error occurred");
    }
    _schema = *schema;
    _row_size = latest_size();
    _write_index = _index;
    _writers.clear();
    _writers.resize(_columns.size());
    // precache key columns
    for (size_t i = 0; i < _schema->num_key_columns(); i++) {
        uint32_t cid = _schema->get(i)->cid();
        if (_writers[cid] == nullptr) {
            RETURN_IF_ERROR(_columns[cid]->create_writer(&_writers[cid]));
        }
    }
    _temp_hash_entries.reserve(8);

    // setup stats
    _write_start = GetMonoTimeSecondsAsDouble();
    _num_insert = 0;
    _num_update = 0;
    _num_update_cell = 0;
    return Status::OK();
}

Status MemSubTablet::apply_partial_row_batch(PartialRowBatch* batch) {
    while (true) {
        bool has_row = false;
        RETURN_IF_ERROR(batch->next_row(&has_row));
        if (!has_row) {
            break;
        }
        DCHECK_GE(batch->cur_row_cell_size(), 1);
        const ColumnSchema* dsc;
        const void* key;
        // get key column and find in hash index
        // TODO: support multi-column row key
        batch->cur_row_get_cell(0, &dsc, &key);
        ColumnWriter* keyw = _writers[1].get();
        // find candidate rowids, and check equality
        uint64_t hashcode = keyw->hashcode(key, 0);
        _temp_hash_entries.clear();
        uint32_t newslot = _write_index->find(hashcode, &_temp_hash_entries);
        uint32_t rid = -1;
        for (size_t i = 0; i < _temp_hash_entries.size(); i++) {
            uint32_t test_rid = _temp_hash_entries[i];
            if (keyw->equals(test_rid, key, 0)) {
                rid = test_rid;
                break;
            }
        }
        // if rowkey not found, do insertion/append
        if (rid == -1) {
            rid = _row_size;
            // add all columns
            //DLOG(INFO) << StringPrintf"insert rid=%u", rid);
            for (size_t i = 0; i < batch->cur_row_cell_size(); i++) {
                const void* data;
                RETURN_IF_ERROR(batch->cur_row_get_cell(i, &dsc, &data));
                uint32_t cid = dsc->cid();
                if (_writers[cid] == nullptr) {
                    RETURN_IF_ERROR(_columns[cid]->create_writer(&_writers[cid]));
                }
                RETURN_IF_ERROR(_writers[cid]->insert(rid, data));
            }
            _write_index->set(newslot, hashcode, rid);
            _row_size++;
            if (_write_index->need_rebuild()) {
                scoped_refptr<HashIndex> new_index;
                // TODO: trace memory usage
                size_t new_capacity = _row_size * 2;
                while (true) {
                    new_index = rebuild_hash_index(new_capacity);
                    if (new_index.get() != nullptr) {
                        break;
                    } else {
                        new_capacity += 1 << 16;
                    }
                }
                _write_index = new_index;
            }
            _num_insert++;
        } else {
            // rowkey found, do update
            // add non-key columns
            for (size_t i = 1; i < batch->cur_row_cell_size(); i++) {
                const void* data;
                RETURN_IF_ERROR(batch->cur_row_get_cell(i, &dsc, &data));
                uint32_t cid = dsc->cid();
                if (cid > _schema->num_key_columns()) {
                    if (_writers[cid] == nullptr) {
                        RETURN_IF_ERROR(_columns[cid]->create_writer(&_writers[cid]));
                    }
                    RETURN_IF_ERROR(_writers[cid]->update(rid, data));
                }
            }
            _num_update++;
            _num_update_cell += batch->cur_row_cell_size() - 1;
        }
    }
    return Status::OK();
}

Status MemSubTablet::commit_write(uint64_t version) {
    for (size_t cid = 0; cid < _writers.size(); cid++) {
        if (_writers[cid] != nullptr) {
            // Should not fail in normal cases, fatal error if commit failed
            RETURN_IF_ERROR(_writers[cid]->finalize(version));
        }
    }
    {
        std::lock_guard<std::mutex> lg(_lock);
        if (_index.get() != _write_index.get()) {
            _index = _write_index;
        }
        for (size_t cid = 0; cid < _writers.size(); cid++) {
            if (_writers[cid] != nullptr) {
                // Should not fail in normal cases, fatal error if commit failed
                RETURN_IF_ERROR(_writers[cid]->get_new_column(&_columns[cid]));
            }
        }
        _versions.emplace_back(version, _row_size);
    }
    _write_index.reset();
    _writers.clear();
    _schema = nullptr;
    LOG(INFO) << StringPrintf("commit writetxn(insert=%zu update=%zu update_cell=%zu) %.3lfs",
                              _num_insert, _num_update, _num_update_cell,
                              GetMonoTimeSecondsAsDouble() - _write_start);
    return Status::OK();
}

scoped_refptr<HashIndex> MemSubTablet::rebuild_hash_index(size_t new_capacity) {
    double t0 = GetMonoTimeSecondsAsDouble();
    ColumnWriter* keyw = _writers[1].get();
    scoped_refptr<HashIndex> hi(new HashIndex(new_capacity));
    for (size_t i = 0; i < _row_size; i++) {
        const void* data = keyw->get(i);
        DCHECK(data);
        uint64_t hashcode = keyw->hashcode(data, 0);
        if (!hi->add(hashcode, i)) {
            double t1 = GetMonoTimeSecondsAsDouble();
            LOG(INFO) << StringPrintf("Rebuild hash index %zu failed time: %.3lfs, expand",
                                      new_capacity, t1 - t0);
            return scoped_refptr<HashIndex>();
        }
    }
    double t1 = GetMonoTimeSecondsAsDouble();
    LOG(INFO) << StringPrintf("Rebuild hash index %zu time: %.3lfs", new_capacity, t1 - t0);
    return hi;
}

} // namespace memory
} // namespace doris
