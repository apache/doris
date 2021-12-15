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

#include "olap/olap_index.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <fstream>

#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/rowset/column_data.h"
#include "olap/utils.h"
#include "olap/wrapper_field.h"

using std::ifstream;
using std::string;
using std::vector;

namespace doris {

MemIndex::MemIndex()
        : _key_length(0),
          _num_entries(0),
          _index_size(0),
          _data_size(0),
          _num_rows(0),
          _tracker(new MemTracker(-1)),
          _mem_pool(new MemPool(_tracker.get())) {}

MemIndex::~MemIndex() {
    _num_entries = 0;
    for (vector<SegmentMetaInfo>::iterator it = _meta.begin(); it != _meta.end(); ++it) {
        free(it->buffer.data);
        it->buffer.data = nullptr;
        it->buffer.length = 0;
    }
}

OLAPStatus MemIndex::load_segment(const char* file, size_t* current_num_rows_per_row_block,
                                  bool use_cache) {
    OLAPStatus res = OLAP_SUCCESS;

    SegmentMetaInfo meta;
    uint32_t adler_checksum = 0;
    uint32_t num_entries = 0;

    if (file == nullptr) {
        res = OLAP_ERR_INPUT_PARAMETER_ERROR;
        LOG(WARNING) << "load index error. file=" << file << ", res=" << res;
        return res;
    }

    FileHandler file_handler;
    if (use_cache) {
        if ((res = file_handler.open_with_cache(file, O_RDONLY)) != OLAP_SUCCESS) {
            LOG(WARNING) << "open index error. file=" << file << ", res=" << res;
            return res;
        }
    } else {
        if ((res = file_handler.open(file, O_RDONLY)) != OLAP_SUCCESS) {
            LOG(WARNING) << "open index error. file=" << file << ", res=" << res;
            return res;
        }
    }

    if ((res = meta.file_header.unserialize(&file_handler)) != OLAP_SUCCESS) {
        LOG(WARNING) << "load index error. file=" << file << ", res=" << res;
        file_handler.close();
        return res;
    }

    // 允许索引内容为空
    // 索引长度必须为索引项长度的整数倍
    size_t storage_length = meta.file_header.file_length() - meta.file_header.size();
    bool null_supported = false;
    //null_supported是为了兼容之前没有NULL字节的数据。
    //目前索引里面都加入了NULL的标志位，entry length都算了NULL标志位构成的bytes
    //对于没有标志位的索引，读取数据之后需要对每个字段补齐这部分。
    if (!meta.file_header.message().has_null_supported()) {
        null_supported = false;
    } else {
        null_supported = meta.file_header.message().null_supported();
    }
    size_t num_short_key_columns = short_key_num();
    bool is_align = false;
    if (!null_supported) {
        is_align = (0 == storage_length % (entry_length() - num_short_key_columns));
    } else {
        is_align = (0 == storage_length % entry_length());
    }
    if (!is_align) {
        res = OLAP_ERR_INDEX_LOAD_ERROR;
        LOG(WARNING) << "load index error. file=" << file << ", res=" << res;
        file_handler.close();
        return res;
    }

    // calculate the total size of all segments
    if (!null_supported) {
        _index_size += meta.file_header.file_length() + num_entries * num_short_key_columns;
        num_entries = storage_length / (entry_length() - num_short_key_columns);
    } else {
        _index_size += meta.file_header.file_length();
        num_entries = storage_length / entry_length();
    }
    _data_size += meta.file_header.extra().data_length;
    _num_rows += meta.file_header.extra().num_rows;

    meta.range.first = _num_entries;
    meta.range.last = meta.range.first + num_entries;
    _num_entries = meta.range.last;
    _meta.push_back(meta);

    (current_num_rows_per_row_block == nullptr ||
     (*current_num_rows_per_row_block = meta.file_header.message().num_rows_per_block()));

    if (OLAP_UNLIKELY(num_entries == 0)) {
        file_handler.close();
        return OLAP_SUCCESS;
    }

    // convert index memory layout for string type
    // previous layout is size|data,
    // target type is ptr|size, ptr pointer to data
    char* storage_data = reinterpret_cast<char*>(calloc(storage_length, 1));
    if (storage_data == nullptr) {
        res = OLAP_ERR_MALLOC_ERROR;
        OLAP_LOG_WARNING("load segment for loading index error. [file=%s; res=%d]", file, res);
        file_handler.close();
        return res;
    }

    // 读取索引内容
    // 为了启动加速，此处可使用mmap方式。
    if (file_handler.pread(storage_data, storage_length, meta.file_header.size()) != OLAP_SUCCESS) {
        res = OLAP_ERR_IO_ERROR;
        OLAP_LOG_WARNING("load segment for loading index error. [file=%s; res=%d]", file, res);
        file_handler.close();
        free(storage_data);
        return res;
    }

    // checksum validation
    adler_checksum = olap_adler32(ADLER32_INIT, storage_data, storage_length);
    if (adler_checksum != meta.file_header.checksum()) {
        res = OLAP_ERR_INDEX_CHECKSUM_ERROR;
        OLAP_LOG_WARNING("checksum validation error.");
        OLAP_LOG_WARNING("load segment for loading index error. [file=%s; res=%d]", file, res);
        file_handler.close();
        free(storage_data);
        return res;
    }

    /*
     * convert storage layout to memory layout for olap/index
     * In this procedure, string type(Varchar/Char) should be
     * converted with caution. Hyperloglog type will not be
     * key, it can not to be handled.
     */

    size_t storage_row_bytes = entry_length();
    storage_row_bytes -= (null_supported ? 0 : num_short_key_columns);
    char* storage_ptr = storage_data;
    size_t storage_field_offset = 0;

    size_t mem_row_bytes = new_entry_length();
    char* mem_buf = reinterpret_cast<char*>(calloc(num_entries * mem_row_bytes, 1));
    memset(mem_buf, 0, num_entries * mem_row_bytes);
    char* mem_ptr = mem_buf;
    size_t mem_field_offset = 0;

    size_t null_byte = null_supported ? 1 : 0;
    for (size_t i = 0; i < num_short_key_columns; ++i) {
        const TabletColumn& column = (*_short_key_columns)[i];
        storage_ptr = storage_data + storage_field_offset;
        storage_field_offset += column.index_length() + null_byte;
        mem_ptr = mem_buf + mem_field_offset;
        if (column.type() == OLAP_FIELD_TYPE_VARCHAR) {
            mem_field_offset += sizeof(Slice) + 1;
            for (size_t j = 0; j < num_entries; ++j) {
                /*
                 * Varchar is null_byte|length|content in OlapIndex storage
                 * Varchar is in nullbyte|length|ptr in memory
                 * We need copy three part: nullbyte|length|content
                 * 1. copy null byte
                 * 2. copy length and content into addrs pointed by ptr
                 */

                // 1. copy null_byte
                memory_copy(mem_ptr, storage_ptr, null_byte);

                // 2. copy length and content
                bool is_null = *reinterpret_cast<bool*>(mem_ptr);
                if (!is_null) {
                    size_t storage_field_bytes =
                            *reinterpret_cast<VarcharLengthType*>(storage_ptr + null_byte);
                    Slice* slice = reinterpret_cast<Slice*>(mem_ptr + 1);
                    char* data = reinterpret_cast<char*>(_mem_pool->allocate(storage_field_bytes));
                    memory_copy(data, storage_ptr + sizeof(VarcharLengthType) + null_byte,
                                storage_field_bytes);
                    slice->data = data;
                    slice->size = storage_field_bytes;
                }

                mem_ptr += mem_row_bytes;
                storage_ptr += storage_row_bytes;
            }
        } else if (column.type() == OLAP_FIELD_TYPE_STRING) {
            mem_field_offset += sizeof(Slice) + 1;
            for (size_t j = 0; j < num_entries; ++j) {
                /*
                 * string is null_byte|length|content in OlapIndex storage
                 * string is in nullbyte|length|ptr in memory
                 * We need copy three part: nullbyte|length|content
                 * 1. copy null byte
                 * 2. copy length and content into addrs pointed by ptr
                 */

                // 1. copy null_byte
                memory_copy(mem_ptr, storage_ptr, null_byte);

                // 2. copy length and content
                bool is_null = *reinterpret_cast<bool*>(mem_ptr);
                if (!is_null) {
                    size_t storage_field_bytes =
                            *reinterpret_cast<StringLengthType*>(storage_ptr + null_byte);
                    Slice* slice = reinterpret_cast<Slice*>(mem_ptr + 1);
                    char* data = reinterpret_cast<char*>(_mem_pool->allocate(storage_field_bytes));
                    memory_copy(data, storage_ptr + sizeof(StringLengthType) + null_byte,
                                storage_field_bytes);
                    slice->data = data;
                    slice->size = storage_field_bytes;
                }

                mem_ptr += mem_row_bytes;
                storage_ptr += storage_row_bytes;
            }
        } else if (column.type() == OLAP_FIELD_TYPE_CHAR) {
            mem_field_offset += sizeof(Slice) + 1;
            size_t storage_field_bytes = column.index_length();
            for (size_t j = 0; j < num_entries; ++j) {
                /*
                 * Char is in nullbyte|content with fixed length in OlapIndex
                 * Char is in nullbyte|length|ptr in memory
                 * We need copy three part: nullbyte|length|content
                 * 1. copy null byte
                 * 2. copy length and content into addrs pointed by ptr
                 */

                // 1. copy null_byte
                memory_copy(mem_ptr, storage_ptr, null_byte);

                // 2. copy length and content
                bool is_null = *reinterpret_cast<bool*>(mem_ptr);
                if (!is_null) {
                    Slice* slice = reinterpret_cast<Slice*>(mem_ptr + 1);
                    char* data = reinterpret_cast<char*>(_mem_pool->allocate(storage_field_bytes));
                    memory_copy(data, storage_ptr + null_byte, storage_field_bytes);
                    slice->data = data;
                    slice->size = storage_field_bytes;
                }

                mem_ptr += mem_row_bytes;
                storage_ptr += storage_row_bytes;
            }
        } else {
            size_t storage_field_bytes = column.index_length();
            mem_field_offset += storage_field_bytes + 1;
            for (size_t j = 0; j < num_entries; ++j) {
                // 1. copy null_byte
                memory_copy(mem_ptr, storage_ptr, null_byte);

                // 2. copy content
                bool is_null = *reinterpret_cast<bool*>(mem_ptr);
                if (!is_null) {
                    memory_copy(mem_ptr + 1, storage_ptr + null_byte, storage_field_bytes);
                }

                mem_ptr += mem_row_bytes;
                storage_ptr += storage_row_bytes;
            }
        }
    }

    mem_ptr = mem_buf + mem_field_offset;
    storage_ptr = storage_data + storage_field_offset;
    size_t data_file_offset = sizeof(data_file_offset_t);
    for (size_t j = 0; j < num_entries; ++j) {
        memory_copy(mem_ptr, storage_ptr, data_file_offset);
        mem_ptr += mem_row_bytes;
        storage_ptr += storage_row_bytes;
    }

    _meta.back().buffer.data = mem_buf;
    _meta.back().buffer.length = num_entries * mem_row_bytes;
    free(storage_data);

    file_handler.close();
    return OLAP_SUCCESS;
}

OLAPStatus MemIndex::init(size_t short_key_len, size_t new_short_key_len, size_t short_key_num,
                          std::vector<TabletColumn>* short_key_columns) {
    if (short_key_columns == nullptr) {
        LOG(WARNING) << "fail to init MemIndex, nullptr short key columns.";
        return OLAP_ERR_INDEX_LOAD_ERROR;
    }

    _key_length = short_key_len;
    _new_key_length = new_short_key_len;
    _key_num = short_key_num;
    _short_key_columns = short_key_columns;

    return OLAP_SUCCESS;
}

// Find and return the IndexOffset of the element prior to the first element which
// is key's lower_bound, or upper_bound if key exists, or return the last element in MemIndex
// This process is consists of two phases of binary search.
// Here try to find the first segment which range covers k,
// and find the index item inside the segment previously found.
//
// There 're a little more detail of the binary search.
// The semantics here is to guarantee there's no
// omissions for given k, consider the following case:
// [4, offset] ---------------------> [(4, xxxx), (4, xxxy), (5, xxxx), (5, xxxy)]
// [5, offset] ---------------------> [(5, yyyy), (5, yyyx), (6, ...)]
// because of our sparse index, the first item which short key equals 5(5, xxxx) is indexed
// by shortkey 4 in the first index item, if we want to find the first key not less than 6, we
// should return the first index instead the second.
const OLAPIndexOffset MemIndex::find(const RowCursor& k, RowCursor* helper_cursor,
                                     bool find_last) const {
    if (begin() == end()) {
        return begin();
    }

    OLAPIndexOffset offset;
    BinarySearchIterator it;
    BinarySearchIterator seg_beg(0);
    BinarySearchIterator seg_fin(segment_count());

    try {
        SegmentComparator seg_comparator(this, helper_cursor);

        // first step, binary search for the correct segment
        if (!find_last) {
            it = std::lower_bound(seg_beg, seg_fin, k, seg_comparator);
        } else {
            it = std::upper_bound(seg_beg, seg_fin, k, seg_comparator);
        }

        iterator_offset_t off = 0;
        if (*it > 0) {
            off = *it - 1;
        }

        // set segment id
        offset.segment = off;
        IndexComparator index_comparator(this, helper_cursor);
        // second step, binary search index item in given segment
        BinarySearchIterator index_beg(0);
        BinarySearchIterator index_fin(_meta[off].count());

        if (index_comparator.set_segment_id(off) != OLAP_SUCCESS) {
            throw "index of of range";
        }

        if (!find_last) {
            it = std::lower_bound(index_beg, index_fin, k, index_comparator);
        } else {
            it = std::upper_bound(index_beg, index_fin, k, index_comparator);
        }

        offset.offset = *it;
        VLOG_NOTICE << "show real offset iterator value. off=" << *it;
        VLOG_NOTICE << "show result offset. seg_off=" << offset.segment
                    << ", off=" << offset.offset;
    } catch (...) {
        OLAP_LOG_WARNING("fail to compare value in memindex. [cursor='%s' find_last=%d]",
                         k.to_string().c_str(), find_last);
        return end();
    }

    return offset;
}

const OLAPIndexOffset MemIndex::next(const OLAPIndexOffset& pos) const {
    OLAPIndexOffset off;

    if (pos.segment >= segment_count()) {
        return end();
    } else if (pos.segment == segment_count() - 1) {
        if (pos.offset >= _meta[pos.segment].count() - 1) {
            return end();
        } else {
            off.segment = pos.segment;
            off.offset = pos.offset + 1;
            return off;
        }
    } else {
        if (pos.offset >= _meta[pos.segment].count() - 1) {
            off.segment = pos.segment + 1;
            off.offset = 0;
        } else {
            off.segment = pos.segment;
            off.offset = pos.offset + 1;
        }

        return off;
    }
}

const OLAPIndexOffset MemIndex::prev(const OLAPIndexOffset& pos) const {
    OLAPIndexOffset off;

    if (pos.offset == 0) {
        if (pos.segment == 0) {
            off = pos;
        } else {
            off.segment = pos.segment - 1;
            off.offset = _meta[off.segment].count() - 1;
        }
    } else {
        off.segment = pos.segment;
        off.offset = pos.offset - 1;
    }

    return off;
}

const OLAPIndexOffset MemIndex::get_offset(const RowBlockPosition& pos) const {
    uint32_t file_header_size = _meta[pos.segment].file_header.size();
    if (pos.segment >= segment_count() ||
        pos.index_offset > file_header_size + _meta[pos.segment].buffer.length ||
        (pos.index_offset - file_header_size) % new_entry_length() != 0) {
        return end();
    }

    OLAPIndexOffset off;
    off.segment = pos.segment;
    off.offset = (pos.index_offset - _meta[pos.segment].file_header.size()) / new_entry_length();

    return off;
}

OLAPStatus MemIndex::get_entry(const OLAPIndexOffset& pos, EntrySlice* slice) const {
    if (pos.segment >= segment_count() || pos.offset >= _meta[pos.segment].count()) {
        return OLAP_ERR_INDEX_EOF;
    }

    slice->length = new_entry_length();
    slice->data = _meta[pos.segment].buffer.data + pos.offset * new_entry_length();

    return OLAP_SUCCESS;
}

OLAPStatus MemIndex::get_row_block_position(const OLAPIndexOffset& pos,
                                            RowBlockPosition* rbp) const {
    if (zero_num_rows()) {
        return OLAP_ERR_INDEX_EOF;
    }

    if (pos.segment >= segment_count() || pos.offset >= _meta[pos.segment].count()) {
        OLAP_LOG_WARNING(
                "fail to get RowBlockPosition from OLAPIndexOffset. "
                "[IndexOffset={segment=%u offset=%u} segment_count=%lu items_count=%lu]",
                pos.segment, pos.offset, segment_count(),
                pos.segment < segment_count() ? _meta[pos.segment].count() : 0);
        return OLAP_ERR_INDEX_EOF;
    }

    rbp->segment = pos.segment;
    rbp->data_offset =
            *reinterpret_cast<uint32_t*>(_meta[pos.segment].buffer.data +
                                         pos.offset * new_entry_length() + new_short_key_length());
    rbp->index_offset = _meta[pos.segment].file_header.size() + pos.offset * new_entry_length();

    if (pos.offset == _meta[pos.segment].count() - 1) {
        rbp->block_size = _meta[pos.segment].file_header.extra().data_length - rbp->data_offset;
    } else {
        uint32_t next_offset = *reinterpret_cast<uint32_t*>(_meta[pos.segment].buffer.data +
                                                            (pos.offset + 1) * new_entry_length() +
                                                            new_short_key_length());
        rbp->block_size = next_offset - rbp->data_offset;
    }

    return OLAP_SUCCESS;
}

const OLAPIndexOffset MemIndex::get_relative_offset(iterator_offset_t absolute_offset) const {
    int begin = 0;
    int end = segment_count() - 1;
    OLAPIndexOffset offset(0, 0);

    while (begin <= end) {
        size_t mid = (begin + end) / 2;
        if (absolute_offset >= _meta[mid].range.last) {
            begin = mid + 1;
        } else if (absolute_offset < _meta[mid].range.first) {
            end = mid - 1;
        } else {
            offset.segment = mid;
            break;
        }
    }

    // 这里不考虑没有找到的情况
    offset.offset = absolute_offset - _meta[offset.segment].range.first;
    return offset;
}
} // namespace doris
