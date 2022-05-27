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

#include "runtime/dpp_writer.h"

#include <stdio.h>

#include <vector>

#include "exprs/expr_context.h"
#include "olap/utils.h"
#include "runtime/primitive_type.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"
#include "util/types.h"

namespace doris {

DppWriter::DppWriter(int32_t schema_hash, const std::vector<ExprContext*>& output_expr_ctxs,
                     FileHandler* fp)
        : _schema_hash(schema_hash),
          _output_expr_ctxs(output_expr_ctxs),
          _fp(fp),
          _buf(nullptr),
          _end(nullptr),
          _pos(nullptr),
          _write_len(0),
          _content_adler32(1) {
    _num_null_slots = 0;
    for (int i = 0; i < _output_expr_ctxs.size(); ++i) {
        if (true == _output_expr_ctxs[i]->is_nullable()) {
            _num_null_slots += 1;
        }
    }
    _num_null_bytes = (_num_null_slots + 7) / 8;
}

DppWriter::~DppWriter() {
    if (_buf) {
        delete[] _buf;
    }
}

Status DppWriter::open() {
    // Write header
    _header.mutable_message()->set_schema_hash(_schema_hash);
    _header.prepare(_fp);
    _content_adler32 = 1;
    // seek to size()
    _fp->seek(_header.size(), SEEK_SET);

    // new buf
    const int k_buf_len = 16 * 1024;
    _buf = new char[k_buf_len];
    _pos = _buf;
    _end = _buf + k_buf_len;
    return Status::OK();
}

void DppWriter::reset_buf() {
    _pos = _buf;
}

void DppWriter::append_to_buf(const void* ptr, int len) {
    if (_pos + len > _end) {
        // enlarge
        int cur_len = _pos - _buf;
        int old_buf_len = _end - _buf;
        int new_len = std::max(2 * old_buf_len, old_buf_len + len);
        char* new_buf = new char[new_len];
        memcpy(new_buf, _buf, cur_len);
        delete[] _buf;
        _buf = new_buf;
        _pos = _buf + cur_len;
        _end = _buf + new_len;
    }

    memcpy(_pos, ptr, len);
    _pos += len;
}

void DppWriter::increase_buf(int len) {
    //increase buf to store nullptr bytes
    //len is the bytes of nullptr
    if (_pos + len > _end) {
        int cur_len = _pos - _buf;
        int old_buf_len = _end - _buf;
        int new_len = std::max(2 * old_buf_len, old_buf_len + len);
        char* new_buf = new char[new_len];
        memcpy(new_buf, _buf, cur_len);
        delete[] _buf;
        _buf = new_buf;
        _pos = _buf + cur_len;
        _end = _buf + new_len;
    }

    memset(_pos, 0, len);
    _pos += len;
}

Status DppWriter::append_one_row(TupleRow* row) {
    int num_columns = _output_expr_ctxs.size();
    int off = 0;
    int pos = _pos - _buf;
    increase_buf(_num_null_bytes);
    for (int i = 0; i < num_columns; ++i) {
        char* position = _buf + pos;
        void* item = _output_expr_ctxs[i]->get_value(row);
        // What happened failed???
        if (true == _output_expr_ctxs[i]->is_nullable()) {
            int index = off % 8;
            if (item == nullptr) {
                //store nullptr bytes
                position[off / 8] |= 1 << (7 - index);
                off += 1;
                continue;
            } else {
                position[off / 8] &= ~(1 << (7 - index));
                off += 1;
            }
        }
        switch (_output_expr_ctxs[i]->root()->type().type) {
        case TYPE_TINYINT:
            append_to_buf(item, 1);
            break;
        case TYPE_SMALLINT:
            append_to_buf(item, 2);
            break;
        case TYPE_INT:
            append_to_buf(item, 4);
            break;
        case TYPE_BIGINT:
            append_to_buf(item, 8);
            break;
        case TYPE_LARGEINT:
            append_to_buf(item, 16);
            break;
        case TYPE_FLOAT:
            append_to_buf(item, 4);
            break;
        case TYPE_DOUBLE:
            append_to_buf(item, 8);
            break;
        case TYPE_DATE: {
            const DateTimeValue* time_val = (const DateTimeValue*)(item);
            uint64_t val = time_val->to_olap_date();
            uint8_t char_val = val & 0xff;
            append_to_buf(&char_val, 1);
            val >>= 8;
            char_val = val & 0xff;
            append_to_buf(&char_val, 1);
            val >>= 8;
            char_val = val & 0xff;
            append_to_buf(&char_val, 1);
            break;
        }
        case TYPE_DATETIME: {
            const DateTimeValue* time_val = (const DateTimeValue*)(item);
            uint64_t val = time_val->to_olap_datetime();
            append_to_buf(&val, 8);
            break;
        }
        case TYPE_VARCHAR: {
        case TYPE_HLL:
        case TYPE_STRING:
            const StringValue* str_val = (const StringValue*)(item);
            if (UNLIKELY(str_val->ptr == nullptr && str_val->len != 0)) {
                return Status::InternalError("String value ptr is null");
            }

            // write len first
            uint16_t len = str_val->len;
            if (len != str_val->len) {
                std::stringstream ss;
                ss << "length of string is overflow.len=" << str_val->len;
                return Status::InternalError(ss.str());
            }
            append_to_buf(&len, 2);
            // passing a nullptr pointer to memcpy may be core/
            if (len == 0) {
                break;
            }
            append_to_buf(str_val->ptr, len);
            break;
        }
        case TYPE_CHAR: {
            const StringValue* str_val = (const StringValue*)(item);
            if (UNLIKELY(str_val->ptr == nullptr || str_val->len == 0)) {
                return Status::InternalError("String value ptr is null");
            }
            append_to_buf(str_val->ptr, str_val->len);
            break;
        }
        case TYPE_DECIMALV2: {
            const DecimalV2Value decimal_val(reinterpret_cast<const PackedInt128*>(item)->value);
            int64_t int_val = decimal_val.int_value();
            int32_t frac_val = decimal_val.frac_value();
            append_to_buf(&int_val, sizeof(int_val));
            append_to_buf(&frac_val, sizeof(frac_val));
            break;
        }
        default: {
            std::stringstream ss;
            ss << "Unknown column type " << _output_expr_ctxs[i]->root()->type();
            return Status::InternalError(ss.str());
        }
        }
    }

    return Status::OK();
}

Status DppWriter::add_batch(RowBatch* batch) {
    int num_rows = batch->num_rows();
    if (num_rows <= 0) {
        return Status::OK();
    }

    Status status;
    for (int i = 0; i < num_rows; ++i) {
        reset_buf();
        TupleRow* row = batch->get_row(i);
        status = append_one_row(row);
        if (!status.ok()) {
            LOG(WARNING) << "convert row to dpp output failed. reason: " << status.get_error_msg();

            return status;
        }
        int len = _pos - _buf;
        Status olap_status = _fp->write(_buf, len);
        if (!olap_status.ok()) {
            return Status::InternalError("write to file failed.");
        }
        _content_adler32 = olap_adler32(_content_adler32, _buf, len);
        _write_len += len;
    }

    return status;
}

Status DppWriter::write_header() {
    _header.set_file_length(_header.size() + _write_len);
    _header.set_checksum(_content_adler32);
    _header.serialize(_fp);
    return Status::OK();
}

Status DppWriter::close() {
    // Write header
    return write_header();
}

} // namespace doris
