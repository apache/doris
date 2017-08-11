// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include "olap/row_block.h"

#include <sys/mman.h>

#include <algorithm>
#include <cstring>

#include "exprs/expr.h"
#include "olap/field.h"
#include "olap/row_cursor.h"
#include "olap/utils.h"

using std::exception;
using std::lower_bound;
using std::nothrow;
using std::pair;
using std::upper_bound;
using std::vector;

namespace palo {
RowBlock::RowBlock(const vector<FieldInfo>& tablet_schema) :
        _is_inited(false),
        _is_use_vectorized(false),
        _buf_len(0),
        _fix_row_len(0),
        _extend_len(0),
        _used_buf_size(0),
        _init_buf_len(0),
        _grid_items_size(0),
        _init_row_num(0),
        _tablet_schema(tablet_schema),
        _grid_items(NULL),
        _buf(NULL),
        _string_buf_head_ptr(NULL),
        _string_buf_array(NULL),
        _vectorized_row_batch(NULL) {}

RowBlock::~RowBlock() {
    SAFE_DELETE_ARRAY(_grid_items);
    SAFE_DELETE_ARRAY(_buf);
    SAFE_DELETE_ARRAY(_string_buf_array);
    SAFE_DELETE(_vectorized_row_batch);
}

OLAPStatus RowBlock::init(const RowBlockInfo& block_info) {
    if (_is_inited) {
        OLAP_LOG_WARNING("fail to init RowBlock; RowBlock has been inited.");
        // 这里不能goto，否则就会放掉buffer
        return OLAP_ERR_INIT_FAILED;
    }

    _grid_items = new (nothrow) GridItem[_tablet_schema.size()];
    if (_grid_items == NULL) {
        OLAP_LOG_WARNING("fail to malloc '_grid_items'.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    _grid_items_size = _tablet_schema.size();
    _info = block_info;
    _data_file_type = block_info.data_file_type;
    _null_supported = block_info.null_supported;

    // 分配内存
    if (_allocate_buffer() != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to allocate buffer for row_block");

        SAFE_DELETE_ARRAY(_grid_items);
        return OLAP_ERR_MALLOC_ERROR;
    }

    _set_field_offsets();

    _is_inited = true;

    return OLAP_SUCCESS;
}

OLAPStatus RowBlock::compress(char* dest_buffer,
                          size_t dest_len,
                          size_t* written_len,
                          OLAPCompressionType compression_type) const {
    CHECK_ROWBLOCK_INIT();
    if (dest_buffer == NULL || written_len == NULL) {
        OLAP_LOG_WARNING("input NULL pointer. [dest_buffer=%p written_len=%p]",
                         dest_buffer,
                         written_len);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // 这里有个变更，就是压缩的大小不在按照固定的大小压缩，而是根据_used_buf_size压缩
    return olap_compress(_buf,
                         _used_buf_size,
                         dest_buffer,
                         dest_len,
                         written_len,
                         compression_type);
}

OLAPStatus RowBlock::decompress(const char* src_buffer,
                            size_t src_len,
                            OLAPCompressionType compression_type) {
    CHECK_ROWBLOCK_INIT();
    if (src_buffer == NULL) {
        OLAP_LOG_WARNING("input src_buffer is NULL pointer.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    size_t written_len = 0;
    OLAPStatus res = OLAP_SUCCESS;
    res = olap_decompress(src_buffer,
                          src_len,
                          _buf,
                          _buf_len,
                          &written_len,
                          compression_type);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to do olap_decompress. [res=%d]", res);
        return res;
    }

    uint32_t checksum = olap_crc32(CRC32_INIT, _buf, written_len);
    if (_info.checksum != checksum) {
        OLAP_LOG_WARNING("crc32 value does not match. [crc32_value=%u _info.checksum=%u]",
                         checksum,
                         _info.checksum);
        return OLAP_ERR_CHECKSUM_ERROR;
    }

    return res;
}

OLAPStatus RowBlock::eval_conjuncts(std::vector<ExprContext*> conjunct_ctxs) {
    return eval_conjuncts(conjunct_ctxs, _tablet_schema);
}

OLAPStatus RowBlock::eval_conjuncts(std::vector<ExprContext*> conjunct_ctxs,
                                    const std::vector<FieldInfo>& query_schema) {
    OLAPStatus status;

    status = _load_to_vectorized_row_batch(query_schema);
    if (OLAP_SUCCESS != status) {
        OLAP_LOG_WARNING("fail to convert to vectorized_row_batch.");
        return status;
    }

    for (int i = 0; i < conjunct_ctxs.size(); ++i) {
        if (!conjunct_ctxs[i]->root()->evaluate(_vectorized_row_batch)) {
            return OLAP_ERR_EVAL_CONJUNCTS_ERROR;
        }
    }
    _is_use_vectorized = true;

    return OLAP_SUCCESS;
}

OLAPStatus RowBlock::set_row(uint32_t row_index, const RowCursor& cursor) {
    CHECK_ROWBLOCK_INIT();
    if (row_index >= _info.row_num) {
        OLAP_LOG_WARNING("input 'row_index' exceeds _info.row_num."
                         "[row_index=%u; _info.row_num=%u]",
                         row_index,
                         _info.row_num);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (cursor.field_count() != _grid_items_size) {
        OLAP_LOG_WARNING("input row cursor is not valid for this row block. "
                         "[input_cursor_field_count=%lu; row_block_field_count=%lu]",
                         cursor.field_count(),
                         _grid_items_size);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    OLAPStatus res = OLAP_SUCCESS;
    size_t field_count = _grid_items_size;
    for (size_t i = 0; i < field_count; ++i) {
        char* buf = NULL;
        if (_tablet_schema[i].type == OLAP_FIELD_TYPE_VARCHAR 
                || _tablet_schema[i].type == OLAP_FIELD_TYPE_HLL) {
            buf = _string_buf_array[i].buf_ptr +
                      //row_num() * _null_byte_num + 
                      row_index * _string_buf_array[i].string_row_length;
        } else {
            buf = _buf + _grid_items[i].offset + row_index * _grid_items[i].width;
        }

        res = cursor.write_by_index(i, buf);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to write field value. [row_index=%u; field_index=%lu res=%d]",
                             row_index,
                             i,
                             res);
            return res;
        }
    }

    return res;
}

OLAPStatus RowBlock::finalize(uint32_t row_num) {
    CHECK_ROWBLOCK_INIT();
    if (row_num > _init_row_num) {
        OLAP_LOG_WARNING("Intput row num is larger than internal row num."
                         "[row_num=%u; _info.row_num=%u]",
                         row_num,
                         _info.row_num);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    size_t field_count = _grid_items_size;
    uint32_t offset = 0;
    // 构造紧致后每个field的起点位置
    vector<uint32_t> new_offset_array;
    for (size_t i = 0; i < field_count; ++i) {
        new_offset_array.push_back(offset);
        offset += _grid_items[i].width * row_num;
    }

    //_grid_items[0].offset = row_num * _null_byte_num;
    for (size_t i = 0; i < field_count; ++i) {
        char* old_offset = _buf + _grid_items[i].offset;
        char* new_offset = _buf + new_offset_array[i];
        uint32_t field_len = _grid_items[i].width;

        memmove(new_offset, old_offset, field_len * row_num);
        _grid_items[i].offset = new_offset_array[i];
    }

    // 额外加入这部分代码，重新调整块内的偏移
    if (_rearrange_string_buffer(row_num, &_used_buf_size)) {
        OLAP_LOG_WARNING("rearrange varchar buffer failed");
        return OLAP_ERR_BUFFER_OVERFLOW;
    }

    // 重置_info
    _info.row_num = row_num;
    // 计算checksum
    _info.checksum = olap_crc32(CRC32_INIT, _buf, _used_buf_size);
    _info.unpacked_len = _used_buf_size;

    return OLAP_SUCCESS;
}

OLAPStatus RowBlock::find_row(const RowCursor& key,
                              bool find_last,
                              uint32_t* row_index) const {
    CHECK_ROWBLOCK_INIT();
    if (row_index == NULL) {
        OLAP_LOG_WARNING("input 'row_index' is NULL.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    OLAPStatus res = OLAP_SUCCESS;
    RowCursor helper_cursor;
    if ((res = helper_cursor.init(_tablet_schema)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Init helper cursor fail. [res=%d]", res);
        return OLAP_ERR_INIT_FAILED;
    }

    BinarySearchIterator it_start(0u);
    BinarySearchIterator it_end(
            _is_use_vectorized ? _vectorized_row_batch->size() : _info.row_num);
    BinarySearchIterator it_result(0u);

    RowBlockComparator block_comparator(this, &helper_cursor);

    try {
        if (!find_last) {
            it_result = lower_bound(it_start, it_end, key, block_comparator);
            *row_index = *it_result;
        } else {
            it_result = upper_bound(it_start, it_end, key, block_comparator);
            *row_index = *it_result;
        }
    } catch (exception& e) {
        OLAP_LOG_FATAL("exception happens. [e.what='%s']", e.what());
        return OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION;
    }

    return OLAP_SUCCESS;
}

OLAPStatus RowBlock::backup() {
    if (NULL == _vectorized_row_batch) {
        OLAP_LOG_WARNING("fail to backup.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
    _vectorized_row_batch->backup();

    return OLAP_SUCCESS;
}

OLAPStatus RowBlock::restore() {
    if (NULL == _vectorized_row_batch) {
        OLAP_LOG_WARNING("fail to backup.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
    _vectorized_row_batch->restore();

    return OLAP_SUCCESS;
}

OLAPStatus RowBlock::clear() {
    CHECK_ROWBLOCK_INIT();

    _info.row_num = _init_row_num;
    _info.checksum = 0;
    _buf_len = _init_buf_len;

    memset(_buf, 0, _buf_len);
    _set_field_offsets();

    return OLAP_SUCCESS;
}

OLAPStatus RowBlock::_load_to_vectorized_row_batch(const std::vector<FieldInfo>& query_schema) {
    if (NULL == _vectorized_row_batch) {
        // TODO(lingbin): is the MemTracker should come from runtime-state?
        _vectorized_row_batch
            = new (nothrow) VectorizedRowBatch(_tablet_schema, _info.row_num, new MemTracker(-1));
        if (NULL == _vectorized_row_batch) {
            OLAP_LOG_WARNING("fail to allocte VectorizedRowBatch.");
            return OLAP_ERR_MALLOC_ERROR;
        }
    }

    MemPool* mem_pool = _vectorized_row_batch->mem_pool();
    int size = _vectorized_row_batch->capacity();
    for (int field_index = 0, query_field_index = 0;
            field_index < _tablet_schema.size() && query_field_index < query_schema.size();
            ++field_index) {
        if (_tablet_schema[field_index].unique_id != query_schema[query_field_index].unique_id
                || _vectorized_row_batch->column(field_index)->col_data() != NULL) {
            continue;
        }
        ++query_field_index;

        switch (_tablet_schema[field_index].type) {
        case OLAP_FIELD_TYPE_CHAR: {
            StringValue* value = reinterpret_cast<StringValue*>(
                                         mem_pool->allocate(
                                                get_slot_size(TYPE_CHAR) * _info.row_num));
            char* raw = _buf + _grid_items[field_index].offset;
            for (int i = 0; i < size; ++i) {
                value[i].ptr = raw + _grid_items[field_index].width * i;
                value[i].len = strnlen(value[i].ptr, _tablet_schema[field_index].length);
            }
            _vectorized_row_batch->column(field_index)->set_col_data(value);
            break;
        }
        case OLAP_FIELD_TYPE_VARCHAR:
        case OLAP_FIELD_TYPE_HLL: {
            typedef uint32_t OffsetValueType;
            typedef uint16_t LengthValueType;
            StringValue* value = reinterpret_cast<StringValue*>(
                                         mem_pool->allocate(
                                                get_slot_size(TYPE_VARCHAR) * _info.row_num));
            OffsetValueType* offsets = reinterpret_cast<VarCharField::OffsetValueType*>(
                                           _buf + _grid_items[field_index].offset);
            for (int i = 0; i < size; ++i) {
                value[i].len
                    = *reinterpret_cast<VarCharField::LengthValueType*>(_buf + offsets[i]);
                value[i].ptr = _buf + offsets[i] + sizeof(VarCharField::LengthValueType);
            }

            _vectorized_row_batch->column(field_index)->set_col_data(value);
            break;
        }
        default: {
            _vectorized_row_batch->column(field_index)->set_col_data(
                    _buf + _grid_items[field_index].offset);
            break;
        }
        }
    }
    _vectorized_row_batch->set_size(_info.row_num);
    return OLAP_SUCCESS;
}

OLAPStatus RowBlock::_allocate_buffer() {
    OLAPStatus res = OLAP_SUCCESS;

    // 计算fix_row_len
    _fix_row_len = 0;
    for (vector<FieldInfo>::const_iterator iter = _tablet_schema.begin();
            iter != _tablet_schema.end(); ++iter) {
        if (iter->type == OLAP_FIELD_TYPE_VARCHAR || iter->type == OLAP_FIELD_TYPE_HLL) {
            // 变长部分额外计算下实际最大的字符串长度（此处length已经包括记录Length的2个字节）
            _fix_row_len += sizeof(VarCharField::OffsetValueType);
            if (OLAP_DATA_FILE == _data_file_type) {
                if (false == _null_supported) {
                    _extend_len += iter->length;
                } else {
                    _extend_len += iter->length + sizeof(char);
                }
            } else if (COLUMN_ORIENTED_FILE == _data_file_type) {
                _extend_len += iter->length + sizeof(char);
            }
        } else {
            // 一般的field无需计算额外消耗的空间
            if (OLAP_DATA_FILE == _data_file_type) {
                if (false == _null_supported) {
                    _fix_row_len += iter->length;
                } else {
                    _fix_row_len += iter->length + sizeof(char);
                }
            } else if (COLUMN_ORIENTED_FILE == _data_file_type) {
                _fix_row_len += iter->length + sizeof(char);
            }
        }
    }

    //_fix_row_len += _null_byte_num;

    bool auto_allocate = (_info.unpacked_len == 0);
    if (auto_allocate) {
        OLAP_LOG_DEBUG("auto_allocate detected");
        // 重新计算一个block占的内存大小, 定长部分加上一个额外的用于重排string的行长
        _buf_len = (_fix_row_len + _extend_len) * _info.row_num + _extend_len;
    } else {
        // 如果传入的参数有unpack的大小，就意味着是读，直接分配内存
        _buf_len = _info.unpacked_len;
    }

    // 分配内存
    if (!_check_memory_limit(_buf_len)) {
        OLAP_LOG_WARNING("too much memory required.[size=%lu]", _buf_len);
        res = OLAP_ERR_MALLOC_ERROR;
        goto ALLOCATE_EXIT;
    }

    _buf = new (nothrow) char[_buf_len];
    if (_buf == NULL) {
        OLAP_LOG_WARNING("fail to alloc memory for _buf. [alloc_size=%lu]", _buf_len);
        res = OLAP_ERR_MALLOC_ERROR;
        goto ALLOCATE_EXIT;
    }
    memset(_buf, 0, _buf_len);

    if (auto_allocate) {
        // 这里多出来的一行空间是作为交换空间使用的。
        // string从定长部分向后推一行的位置开始存，这样在后边改成致密排列的时候，
        // 可以直接从后向前拷贝。
        _string_buf_head_ptr = _buf + _fix_row_len * (_info.row_num) + _extend_len;

        _string_buf_array = new (nothrow) StringBuffer[_tablet_schema.size()];
        if (_string_buf_array == NULL) {
            OLAP_LOG_WARNING("fail to allocate string buffer array");
            res = OLAP_ERR_MALLOC_ERROR;
            goto ALLOCATE_EXIT;
        }
        // 指向字符部分起始位置
        char* buf_helper_ptr = _string_buf_head_ptr;
        for (size_t i = 0; i < _tablet_schema.size(); i++) {
            if (_tablet_schema[i].type == OLAP_FIELD_TYPE_VARCHAR || _tablet_schema[i].type == OLAP_FIELD_TYPE_HLL) {
                // 保存行长和起始的指针即可，其他可通过计算得出
                _string_buf_array[i].string_row_length = _tablet_schema[i].length;
                if (OLAP_DATA_FILE == _data_file_type) {
                    if (false == _null_supported) {
                        _string_buf_array[i].string_row_length = _tablet_schema[i].length;
                    } else {
                        _string_buf_array[i].string_row_length = _tablet_schema[i].length;
                        _string_buf_array[i].string_row_length += sizeof(char);
                    }
                } else if (COLUMN_ORIENTED_FILE == _data_file_type) {
                    _string_buf_array[i].string_row_length = _tablet_schema[i].length;
                    _string_buf_array[i].string_row_length += sizeof(char);
                }
                _string_buf_array[i].buf_ptr = buf_helper_ptr;
                // 移动到下一个变长field的位置
                buf_helper_ptr += _string_buf_array[i].string_row_length * _info.row_num;
            }
        }
    }
    _init_buf_len = _buf_len;
    _init_row_num = _info.row_num;

ALLOCATE_EXIT:
    if (res != OLAP_SUCCESS) {
        SAFE_DELETE_ARRAY(_string_buf_array);
        SAFE_DELETE_ARRAY(_buf);
    }

    return res;
}

OLAPStatus RowBlock::_rearrange_string_buffer(uint32_t row_num, size_t* output_size) {
    // 考虑到buffer不一定被写满，所以需要重新计算偏移
    char* cur_write_ptr = _buf + _fix_row_len * row_num;
    // 如此定义的好处是，以后改变了类型，这边可以直接跟着变。
    VarCharField::LengthValueType* string_length_ptr = NULL;
    VarCharField::OffsetValueType* cur_offset_ptr = NULL;
    // 一个block的大小应该不能超过4G，uint32足矣
    uint32_t offset = 0;
    for (size_t col = 0; col < _tablet_schema.size(); ++col) {
        if (_tablet_schema[col].type == OLAP_FIELD_TYPE_VARCHAR || _tablet_schema[col].type == OLAP_FIELD_TYPE_HLL) {
            // 指向一个新的变长字符串列
            char *cur_read_ptr = _string_buf_array[col].buf_ptr;
            // offset是一列的最大长度
            offset = _string_buf_array[col].string_row_length;
            // 这个指针指向该变长字符串列写偏移的位置，后边需要重新计算这些偏移
            cur_offset_ptr = reinterpret_cast<uint32_t*>(_buf + _grid_items[col].offset);
            // 逐行调整偏移
            for (size_t i = 0; i < row_num; i++) {
                // 获取长度
                if (OLAP_DATA_FILE == _data_file_type) {
                    if (false == _null_supported) {
                        string_length_ptr = reinterpret_cast<VarCharField::LengthValueType*>(
                                cur_read_ptr);
                    } else {
                        string_length_ptr = reinterpret_cast<VarCharField::LengthValueType*>(
                                cur_read_ptr + sizeof(char));
                    }
                } else if (COLUMN_ORIENTED_FILE == _data_file_type) {
                    string_length_ptr = reinterpret_cast<VarCharField::LengthValueType*>(
                            cur_read_ptr + sizeof(char));
                }
                // 获取实际的拷贝长度，这个长度是string长度+ 字符串头表示长度的数字
                size_t copy_size = 0;
                if (OLAP_DATA_FILE == _data_file_type) {
                    if (false == _null_supported) {
                        copy_size = (*string_length_ptr) +
                            sizeof(VarCharField::LengthValueType);
                    } else {
                        copy_size = (*string_length_ptr) +
                            sizeof(VarCharField::LengthValueType) + sizeof(char);
                    }
                } else if (COLUMN_ORIENTED_FILE == _data_file_type) {
                    copy_size = (*string_length_ptr) +
                        sizeof(VarCharField::LengthValueType) + sizeof(char); 
                }
                // 其实这里应该是不会溢出的，最多写满
                if (static_cast<size_t>(cur_write_ptr - _buf + copy_size) > _buf_len) {
                    OLAP_LOG_WARNING("not enough buffer, need %lu but has %lu",
                                     cur_write_ptr - _buf + copy_size,
                                     _buf_len);
                    return OLAP_ERR_BUFFER_OVERFLOW;
                }
                memcpy(cur_write_ptr, cur_read_ptr, copy_size);
                // 在offset里保存从_buf起的偏移量
                *cur_offset_ptr = cur_write_ptr - _buf;
                // 移动到下个位置
                cur_read_ptr += offset;
                cur_write_ptr += copy_size;
                // 注意这里cur_offset_ptr的类型，直接向后移动即可，不能按字节数加
                ++cur_offset_ptr;
            }
        }
    }
    *output_size = cur_write_ptr - _buf;

    return OLAP_SUCCESS;
}

void RowBlock::_set_field_offsets() {
    // 初始化field_offset
    uint32_t offset = 0;
    for (size_t i = 0; i < _tablet_schema.size(); ++i) {
        _grid_items[i].offset = offset;

        if (_tablet_schema[i].type == OLAP_FIELD_TYPE_VARCHAR || _tablet_schema[i].type == OLAP_FIELD_TYPE_HLL) {
            _grid_items[i].width = sizeof(VarCharField::OffsetValueType);
        } else {
            if (OLAP_DATA_FILE == _data_file_type) {
                if (false == _null_supported) {
                    _grid_items[i].width = _tablet_schema[i].length;
                } else {
                    _grid_items[i].width = _tablet_schema[i].length;
                    _grid_items[i].width += sizeof(char);
                }
            } else if (COLUMN_ORIENTED_FILE == _data_file_type) {
                _grid_items[i].width = _tablet_schema[i].length;
                _grid_items[i].width += sizeof(char);
            }
        }

        offset += _grid_items[i].width * _info.row_num;
    }
}

inline bool RowBlock::_check_memory_limit(size_t _buf_len) const {
    uint64_t max_unpacked_row_block_size = config::max_unpacked_row_block_size;
    max_unpacked_row_block_size =
            max_unpacked_row_block_size == 0 ? OLAP_DEFAULT_MAX_UNPACKED_ROW_BLOCK_SIZE
            : max_unpacked_row_block_size;

    return _buf_len <= max_unpacked_row_block_size;
}

}  // namespace palo
