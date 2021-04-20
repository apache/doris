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

#include "olap/memory/partial_row_batch.h"

#include "util/bitmap.h"

namespace doris {
namespace memory {

// Methods for PartialRowBatch

PartialRowBatch::PartialRowBatch(scoped_refptr<Schema>* schema)
        : _schema(*schema), _bit_set_size(_schema->cid_size()) {
    _cells.reserve(_schema->num_columns());
}

PartialRowBatch::~PartialRowBatch() {}

Status PartialRowBatch::load(std::vector<uint8_t>&& buffer) {
    _buffer = std::move(buffer);
    _pos = _buffer.data();
    _row_size = *reinterpret_cast<const uint64_t*>(_pos);
    _pos += sizeof(uint64_t);
    _next_row = 0;
    return Status::OK();
}

Status PartialRowBatch::next_row(bool* has_row) {
    DCHECK_LE(_next_row, _row_size);
    *has_row = false;
    if (_next_row == _row_size) {
        return Status::OK();
    }
    DCHECK_LE(_pos, _buffer.data() + _buffer.size());
    _cells.clear();
    uint32_t row_bsize = *reinterpret_cast<const uint32_t*>(_pos);
    _pos += sizeof(uint32_t);
    const uint8_t* cur = _pos;
    size_t bit_all_size = *reinterpret_cast<const uint16_t*>(cur);
    cur += 2;
    DCHECK_LE(bit_all_size, 65535);
    const uint8_t* bitvec = cur;
    cur += bit_all_size;
    size_t cur_nullable_idx = _bit_set_size;
    if (BitmapTest(bitvec, 0)) {
        _delete = true;
    }
    size_t cid = 1;
    while (BitmapFindFirstSet(bitvec, cid, _bit_set_size, &cid)) {
        const ColumnSchema* cs = _schema->get_by_cid(cid);
        DCHECK(cs);
        if (cs->is_nullable()) {
            if (BitmapTest(bitvec, cur_nullable_idx)) {
                // is null
                _cells.emplace_back(cid, nullptr);
            } else {
                // not null
                _cells.emplace_back(cid, cur);
            }
            cur_nullable_idx++;
        } else {
            _cells.emplace_back(cid, cur);
        }
        const uint8_t* pdata = _cells.back().data;
        if (pdata != nullptr) {
            size_t bsize = _schema->get_column_byte_size(cid);
            if (bsize == 0) {
                return Status::NotSupported("varlen column type not supported");
                // size_t sz = *(uint16_t*)cur;
                // cur += (sz + 2);
            } else {
                cur += bsize;
            }
        }
        cid++;
    }
    if (_pos + row_bsize != cur) {
        return Status::InternalError("PartialRowBatch data corruption");
    }
    _pos = cur;
    *has_row = true;
    _next_row++;
    return Status::OK();
}

Status PartialRowBatch::cur_row_get_cell(size_t idx, const ColumnSchema** cs,
                                         const void** data) const {
    if (idx >= _cells.size()) {
        return Status::InvalidArgument("get_cell: idx exceed cells size");
    }
    auto& cell = _cells[idx];
    *cs = _schema->get_by_cid(cell.cid);
    *data = cell.data;
    return Status::OK();
}

// Methods for PartialRowWriter

PartialRowWriter::PartialRowWriter(const scoped_refptr<Schema>& schema)
        : _schema(schema), _bit_set_size(_schema->cid_size()), _bit_nullable_size(0) {
    _temp_cells.resize(_schema->cid_size());
    memset(&(_temp_cells[0]), 0, sizeof(CellInfo) * _temp_cells.size());
}

Status PartialRowWriter::start_batch(size_t row_capacity, size_t byte_capacity) {
    _row_size = 0;
    _row_capacity = row_capacity;
    // reserve space for _row_size
    _buffer.resize(sizeof(uint64_t));
    _buffer.reserve(byte_capacity);
    return Status::OK();
}

PartialRowWriter::~PartialRowWriter() {}

Status PartialRowWriter::start_row() {
    if (_row_size >= _row_capacity) {
        return Status::InvalidArgument("over capacity");
    }
    _bit_nullable_size = 0;
    memset(&(_temp_cells[0]), 0, sizeof(CellInfo) * _temp_cells.size());
    return Status::OK();
}

Status PartialRowWriter::end_row() {
    if (_row_size >= _row_capacity) {
        return Status::InvalidArgument("over capacity");
    }
    size_t row_byte_size = byte_size();
    size_t new_size = _buffer.size() + row_byte_size + 4;
    size_t old_size = _buffer.size();
    if (new_size > _buffer.capacity()) {
        return Status::InvalidArgument("over capacity");
    }
    _buffer.resize(new_size);
    uint8_t* pos = _buffer.data() + old_size;
    *reinterpret_cast<uint32_t*>(pos) = row_byte_size;
    pos += sizeof(uint32_t);
    Status st = write(&pos);
    DCHECK_EQ(pos, _buffer.data() + new_size);
    if (!st.ok()) {
        _buffer.resize(old_size);
        return st;
    }
    _row_size++;
    return Status::OK();
}

Status PartialRowWriter::set(const ColumnSchema* cs, uint32_t cid, const void* data) {
    if (cs->is_nullable() || (data != nullptr)) {
        if (cs->is_nullable() && !_temp_cells[cid].isnullable) {
            _bit_nullable_size++;
        }
        _temp_cells[cid].isnullable = cs->is_nullable();
        _temp_cells[cid].isset = 1;
        _temp_cells[cid].data = reinterpret_cast<const uint8_t*>(data);
    } else {
        return Status::InvalidArgument("not nullable column set to null");
    }
    return Status::OK();
}

Status PartialRowWriter::set(const string& col, const void* data) {
    auto cs = _schema->get_by_name(col);
    if (cs == nullptr) {
        return Status::NotFound("col name not found");
    }
    return set(cs, cs->cid(), data);
}

Status PartialRowWriter::set(uint32_t cid, const void* data) {
    auto cs = _schema->get_by_cid(cid);
    if (cs == nullptr) {
        return Status::NotFound("cid not found");
    }
    return set(cs, cs->cid(), data);
}

Status PartialRowWriter::set_delete() {
    // TODO: support delete
    // _temp_cells[0].isset = 1;
    return Status::NotSupported("delete not supported");
}

size_t PartialRowWriter::byte_size() const {
    // TODO: support delete
    size_t bit_all_size = num_block(_bit_set_size + _bit_nullable_size, 8);
    size_t data_size = 2 + bit_all_size;
    for (size_t i = 1; i < _temp_cells.size(); i++) {
        if (_temp_cells[i].data != nullptr) {
            size_t bsize = _schema->get_column_byte_size(i);
            if (bsize == 0) {
                LOG(FATAL) << "varlen column type not supported";
                //data_size += 2 + reinterpret_cast<Slice*>(_temp_cells[i].data)->size();
            } else {
                data_size += bsize;
            }
        }
    }
    return data_size;
}

Status PartialRowWriter::write(uint8_t** ppos) {
    size_t bit_all_size = num_block(_bit_set_size + _bit_nullable_size, 8);
    if (bit_all_size >= 65536) {
        return Status::NotSupported("too many columns");
    }
    // using reference is more convenient
    uint8_t*& pos = *ppos;
    *reinterpret_cast<uint16_t*>(pos) = (uint16_t)bit_all_size;
    pos += 2;
    uint8_t* bitvec = pos;
    pos += bit_all_size;
    memset(bitvec, 0, bit_all_size);
    if (_temp_cells[0].isset) {
        // deleted
        BitmapSet(bitvec, 0);
    }
    size_t cur_nullable_idx = _bit_set_size;
    for (size_t i = 1; i < _temp_cells.size(); i++) {
        if (_temp_cells[i].isset) {
            BitmapSet(bitvec, i);
            if (_temp_cells[i].isnullable) {
                if (_temp_cells[i].data == nullptr) {
                    BitmapSet(bitvec, cur_nullable_idx);
                }
                cur_nullable_idx++;
            }
            const uint8_t* pdata = _temp_cells[i].data;
            if (pdata != nullptr) {
                size_t bsize = _schema->get_column_byte_size(i);
                if (bsize == 0) {
                    return Status::NotSupported("varlen column type not supported");
                    // Some incomplete code to write string(Slice), may be useful in future
                    // size_t sz = ((Slice*)pdata)->size();
                    // *(uint16_t*)pos = (uint16_t)sz;
                    // pos += 2;
                    // memcpy(pos, ((Slice*)pdata)->data(), sz);
                    // pos += sz;
                } else {
                    memcpy(pos, _temp_cells[i].data, bsize);
                    pos += bsize;
                }
            }
        } else if (i <= _schema->num_key_columns()) {
            return Status::InvalidArgument("build without key columns");
        }
    }
    return Status::OK();
}

Status PartialRowWriter::finish_batch(vector<uint8_t>* buffer) {
    *reinterpret_cast<uint64_t*>(_buffer.data()) = _row_size;
    _buffer.swap(*buffer);
    _row_size = 0;
    return Status::OK();
}

} // namespace memory
} // namespace doris
