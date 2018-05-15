// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
        _capacity(0),
        _tablet_schema(tablet_schema) {
    _tracker.reset(new MemTracker(-1));
    _mem_pool.reset(new MemPool(_tracker.get()));
}

RowBlock::~RowBlock() {
    delete[] _mem_buf;
    delete[] _storage_buf;
}

OLAPStatus RowBlock::init(const RowBlockInfo& block_info) {
    _field_count = _tablet_schema.size();
    _info = block_info;
    _data_file_type = block_info.data_file_type;
    _null_supported = block_info.null_supported;
    _capacity = _info.row_num;
    _compute_layout();
    _mem_buf = new char[_mem_buf_bytes];
    _storage_buf = new char[_storage_buf_bytes];
    return OLAP_SUCCESS;
}

OLAPStatus RowBlock::serialize_to_row_format(
        char* dest_buffer, size_t dest_len, size_t* written_len,
        OLAPCompressionType compression_type) {
    _convert_memory_to_storage(_info.row_num);
    _info.checksum = olap_crc32(CRC32_INIT, _storage_buf, _storage_buf_used_bytes);
    _info.unpacked_len = _storage_buf_used_bytes;
    return olap_compress(_storage_buf,
                         _storage_buf_used_bytes,
                         dest_buffer,
                         dest_len,
                         written_len,
                         compression_type);
}

OLAPStatus RowBlock::decompress(const char* src_buffer,
                            size_t src_len,
                            OLAPCompressionType compression_type) {
    size_t written_len = 0;
    OLAPStatus res = olap_decompress(src_buffer,
                                     src_len,
                                     _storage_buf,
                                     _storage_buf_bytes,
                                     &written_len,
                                     compression_type);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to do olap_decompress. [res=%d]", res);
        return res;
    }
    if (_need_checksum) {
        uint32_t checksum = olap_crc32(CRC32_INIT, _storage_buf, written_len);
        if (_info.checksum != checksum) {
            OLAP_LOG_WARNING("crc32 value does not match. [crc32_value=%u _info.checksum=%u]",
                             checksum,
                             _info.checksum);
            return OLAP_ERR_CHECKSUM_ERROR;
        }
    }
    _convert_storage_to_memory();
    return OLAP_SUCCESS;
}

void RowBlock::_convert_storage_to_memory() {
    /*
     * This function is used to convert storage
     * layout to memory layout for row-oriented storage.
     * In this procedure, string type(Varchar/Char/Hyperloglog)
     * should be converted with caution.
     * This function will not be called in columnar-oriented storage.
     */

    char* storage_ptr = _storage_buf;

    // some data file in history not suppored null
    size_t null_byte = has_nullbyte() ? 1 : 0;
    for (int col = 0; col < _field_count; ++col) {
        char* memory_ptr = _mem_buf + _field_offset_in_memory[col];
        if (_tablet_schema[col].type == OLAP_FIELD_TYPE_VARCHAR ||
                _tablet_schema[col].type == OLAP_FIELD_TYPE_HLL) {
            for (int row = 0; row < _info.row_num; ++row) {
                /*
                 * Varchar is in offset -> nullbyte|length|content format in storage
                 * Varchar is in nullbyte|length|ptr in memory
                 * We need copy three part: nullbyte|length|content
                 * 1. get values' pointer using offset
                 * 2. copy null byte
                 * 3. copy length and content into addrs pointed by ptr
                 */

                // 1: work out the string pointer by offset
                uint32_t offset = *reinterpret_cast<StringOffsetType*>(storage_ptr);
                storage_ptr += sizeof(StringOffsetType);
                char* value_ptr = _storage_buf + offset;

                // 2: copy null byte
                *reinterpret_cast<bool*>(memory_ptr) = false;
                memory_copy(memory_ptr, value_ptr, null_byte);

                // 3. copy length and content
                size_t storage_field_bytes =
                    *(StringLengthType*)(value_ptr + null_byte);
                value_ptr += sizeof(StringLengthType);
                StringSlice* slice = reinterpret_cast<StringSlice*>(memory_ptr + 1);
                slice->data = value_ptr + null_byte;
                slice->size = storage_field_bytes;

                memory_ptr += _mem_row_bytes;
            }
        } else if (_tablet_schema[col].type == OLAP_FIELD_TYPE_CHAR) {
            size_t storage_field_bytes = _tablet_schema[col].length;
            for (int row = 0; row < _info.row_num; ++row) {
                /*
                 * Char is in nullbyte|content with fixed length in storage
                 * Char is in nullbyte|length|ptr in memory
                 * We need copy three part: nullbyte|length|content
                 * 1. copy null byte
                 * 2. copy length and content into addrs pointed by ptr
                 */

                // 1. copy null byte
                *reinterpret_cast<bool*>(memory_ptr) = false;
                memory_copy(memory_ptr, storage_ptr, null_byte);

                // 2. copy length and content
                StringSlice* slice = reinterpret_cast<StringSlice*>(memory_ptr + 1);
                slice->data = storage_ptr + null_byte;
                slice->size = storage_field_bytes;

                storage_ptr += storage_field_bytes + null_byte;
                memory_ptr += _mem_row_bytes;
            }
        } else {
            size_t storage_field_bytes = _tablet_schema[col].length;
            for (int row = 0; row < _info.row_num; ++row) {
                // Content of not string type can be copied using addr
                *reinterpret_cast<bool*>(memory_ptr) = false;
                memory_copy(memory_ptr + 1 - null_byte, storage_ptr, storage_field_bytes + null_byte);

                storage_ptr += storage_field_bytes + null_byte;
                memory_ptr += _mem_row_bytes;
            }
        }
    }
}

void RowBlock::_convert_memory_to_storage(uint32_t num_rows) {
    // this function is reverse procedure of convert_storage_to_memory

    char* storage_ptr = _storage_buf;
    // Point to start of storage viriable part
    char* storage_variable_ptr = _storage_buf + num_rows * _storage_row_fixed_bytes;
    size_t null_byte = has_nullbyte() ? 1 : 0;
    for (int col = 0; col < _field_count; ++col) {
        char* memory_ptr = _mem_buf + _field_offset_in_memory[col];
        if (_tablet_schema[col].type == OLAP_FIELD_TYPE_VARCHAR ||
                _tablet_schema[col].type == OLAP_FIELD_TYPE_HLL) {
            for (int row = 0; row < num_rows; ++row) {
                /*
                 * Varchar is in offset -> nullbyte|length|content format in storage
                 * Varchar is in nullbyte|length|ptr in memory
                 * We need set three part: offset -> nullbyte|length|content
                 * 1. set offset
                 * 2. copy null byte
                 * 3. copy length and content into sucessive addrs
                 */

                // 1: set offset
                size_t offset = storage_variable_ptr - _storage_buf;
                *reinterpret_cast<StringOffsetType*>(storage_ptr) = offset;
                storage_ptr += sizeof(StringOffsetType);

                // 2: copy null byte
                memory_copy(storage_variable_ptr, memory_ptr, null_byte);
                storage_variable_ptr += null_byte;

                // 3. copy length and content
                StringSlice* slice = reinterpret_cast<StringSlice*>(memory_ptr + 1);
                *reinterpret_cast<StringLengthType*>(storage_variable_ptr) = slice->size;
                storage_variable_ptr += sizeof(StringLengthType);
                memory_copy(storage_variable_ptr, slice->data, slice->size);
                storage_variable_ptr += slice->size;

                memory_ptr += _mem_row_bytes;
            }
        } else if (_tablet_schema[col].type == OLAP_FIELD_TYPE_CHAR) {
            size_t storage_field_bytes = _tablet_schema[col].length;
            for (int row = 0; row < num_rows; ++row) {
                /*
                 * Char is in nullbyte|content with fixed length in storage
                 * Char is in nullbyte|length|ptr in memory
                 * We need set two part: nullbyte|content
                 * 1. copy null byte
                 * 2. copy content
                 */

                // 1. copy null byte
                memory_copy(storage_ptr, memory_ptr, null_byte);

                // 2. copy content
                StringSlice* slice = reinterpret_cast<StringSlice*>(memory_ptr + 1);
                memory_copy(storage_ptr + null_byte, slice->data, slice->size);
                memory_ptr += _mem_row_bytes;
                storage_ptr += storage_field_bytes + null_byte;
            }
        } else {
            // Memory layout is equal with storage layout, there is nullbyte
            // for all field. So we need to copy this to storage
            size_t storage_field_bytes = _tablet_schema[col].length;
            char* memory_ptr = _mem_buf + _field_offset_in_memory[col];
            for (int row = 0; row < num_rows; ++row) {
                memory_copy(storage_ptr, memory_ptr + 1 - null_byte, storage_field_bytes + null_byte);
                storage_ptr += storage_field_bytes + null_byte;
                memory_ptr += _mem_row_bytes;
            }
        }
    }
    _storage_buf_used_bytes = storage_variable_ptr - _storage_buf;
}

OLAPStatus RowBlock::finalize(uint32_t row_num) {
    if (row_num > _capacity) {
        OLAP_LOG_WARNING("Intput row num is larger than internal row num."
                         "[row_num=%u; _info.row_num=%u]",
                         row_num,
                         _info.row_num);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
    _info.row_num = row_num;
    return OLAP_SUCCESS;
}

OLAPStatus RowBlock::find_row(const RowCursor& key,
                              bool find_last,
                              uint32_t* row_index) const {
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
    BinarySearchIterator it_end(_info.row_num);
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

void RowBlock::clear() {
    _info.row_num = _capacity;
    _info.checksum = 0;
    _mem_pool->clear();
}

void RowBlock::_compute_layout() {
    size_t memory_size = 0;
    size_t storage_fixed_bytes = 0;
    size_t storage_variable_bytes = 0;
    for (auto& field : _tablet_schema) {
        _field_offset_in_memory.push_back(memory_size);

        // All field has a nullbyte in memory
        if (field.type == OLAP_FIELD_TYPE_VARCHAR || field.type == OLAP_FIELD_TYPE_HLL) {
            // 变长部分额外计算下实际最大的字符串长度（此处length已经包括记录Length的2个字节）
            storage_fixed_bytes += sizeof(StringOffsetType);
            storage_variable_bytes += field.length;
            if (has_nullbyte()) {
                storage_variable_bytes += sizeof(char);
            }
            memory_size += sizeof(StringSlice) + sizeof(char);
        } else {
            storage_fixed_bytes += field.length;
            if (has_nullbyte()) {
                storage_fixed_bytes += sizeof(char);
            }
            if (field.type == OLAP_FIELD_TYPE_CHAR) {
                memory_size += sizeof(StringSlice) + sizeof(char);
            } else {
                memory_size += field.length + sizeof(char);
            }
        }
    }
    _mem_row_bytes = memory_size;
    _mem_buf_bytes = _mem_row_bytes * _info.row_num;

    _storage_row_fixed_bytes = storage_fixed_bytes;
    _storage_buf_bytes = (storage_fixed_bytes + storage_variable_bytes) * _info.row_num;
    if (_info.unpacked_len != 0) {
        // If we already known unpacked length, just use this length
        _storage_buf_bytes = _info.unpacked_len;
    }
}

inline bool RowBlock::_check_memory_limit(size_t buf_len) const {
    uint64_t max_unpacked_row_block_size = config::max_unpacked_row_block_size;
    max_unpacked_row_block_size =
            max_unpacked_row_block_size == 0 ? OLAP_DEFAULT_MAX_UNPACKED_ROW_BLOCK_SIZE
            : max_unpacked_row_block_size;

    return buf_len <= max_unpacked_row_block_size;
}

}  // namespace palo
