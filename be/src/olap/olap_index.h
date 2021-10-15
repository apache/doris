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

#ifndef DORIS_BE_SRC_OLAP_OLAP_INDEX_H
#define DORIS_BE_SRC_OLAP_OLAP_INDEX_H

#include <condition_variable>
#include <iterator>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gen_cpp/column_data_file.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/field.h"
#include "olap/file_helper.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/row.h"
#include "olap/row_cursor.h"
#include "olap/utils.h"

namespace doris {
class IndexComparator;
class SegmentGroup;
class RowBlock;
class RowCursor;
class SegmentComparator;
class WrapperField;

typedef uint32_t data_file_offset_t;

struct OLAPIndexFixedHeader {
    uint32_t data_length;
    uint64_t num_rows;
};

struct EntrySlice {
    char* data;
    size_t length;
    EntrySlice() : data(nullptr), length(0) {}
};

// Range of offset in one segment
struct IDRange {
    uint32_t first;
    uint32_t last;
};

// index offset
// 2-Dimension Offset, the first is segment id, the second is offset inside segment
struct OLAPIndexOffset {
    OLAPIndexOffset() : segment(0), offset(0) {}
    OLAPIndexOffset(const iterator_offset_t& seg, const iterator_offset_t& off)
            : segment(seg), offset(off) {}
    OLAPIndexOffset(const OLAPIndexOffset& off) : segment(off.segment), offset(off.offset) {}

    bool operator==(const OLAPIndexOffset& other) const {
        return segment == other.segment && offset == other.offset;
    }

    iterator_offset_t segment;
    iterator_offset_t offset;
};

// 唯一标识一个RowBlock在Data文件和Index文件中的位置
struct RowBlockPosition {
    RowBlockPosition() : segment(0), block_size(0), data_offset(0), index_offset(0) {}

    bool operator==(const RowBlockPosition& other) const {
        return (segment == other.segment && data_offset == other.data_offset &&
                block_size == other.block_size && index_offset == other.index_offset);
    }

    bool operator!=(const RowBlockPosition& other) const { return !(*this == other); }

    bool operator>(const RowBlockPosition& other) const {
        if (segment < other.segment) {
            return false;
        } else if (segment > other.segment) {
            return true;
        } else {
            if (data_offset > other.data_offset) {
                return true;
            } else {
                return false;
            }
        }
    }

    bool operator>=(const RowBlockPosition& other) const {
        if (segment < other.segment) {
            return false;
        } else if (segment > other.segment) {
            return true;
        } else {
            if (data_offset >= other.data_offset) {
                return true;
            } else {
                return false;
            }
        }
    }

    // 供日志输出
    std::string to_string() const {
        char message[1024] = {'\0'};
        snprintf(message, sizeof(message),
                 "{segment=%u block_size=%u data_offset=%u index_offset=%u}", segment, block_size,
                 data_offset, index_offset);
        return std::string(message);
    }

    uint32_t segment;
    uint32_t block_size;
    uint32_t data_offset;  // offset in data file
    uint32_t index_offset; // offset in index file
};

// In memory presentation of index meta information
struct SegmentMetaInfo {
    SegmentMetaInfo() {
        range.first = range.last = 0;
        buffer.length = 0;
        buffer.data = nullptr;
    }

    const size_t count() const { return range.last - range.first; }

    IDRange range;
    EntrySlice buffer;
    FileHeader<OLAPIndexHeaderMessage, OLAPIndexFixedHeader> file_header;
};

// In memory index structure, all index hold here
class MemIndex {
public:
    friend class SegmentGroup;
    friend class IndexComparator;
    friend class SegmentComparator;

    MemIndex();
    ~MemIndex();

    // 初始化MemIndex, 传入short_key的总长度和对应的Field数组
    OLAPStatus init(size_t short_key_len, size_t new_short_key_len, size_t short_key_num,
                    std::vector<TabletColumn>* short_key_columns);

    // 加载一个segment到内存
    OLAPStatus load_segment(const char* file, size_t* current_num_rows_per_row_block,
                            bool use_cache = true);

    // Return the IndexOffset of the first element, physically, it's (0, 0)
    const OLAPIndexOffset begin() const {
        OLAPIndexOffset off;
        off.segment = off.offset = 0;
        return off;
    }

    // Indicates a logical IndexOffset beyond MemIndex
    const OLAPIndexOffset end() const {
        OLAPIndexOffset off;
        off.segment = segment_count();
        off.offset = _num_entries;
        return off;
    }

    // Return the IndexOffset of position prior to the first element which is either
    // not less than(find_last is false) or greater than (find_last is true) 'key';
    // or, return the IndexOffset of last element inside MemIndex
    // in case of nothing matched with 'key'
    const OLAPIndexOffset find(const RowCursor& key, RowCursor* helper_cursor,
                               bool find_last) const;

    // Same with begin()
    const OLAPIndexOffset find_first() const { return begin(); }

    // Return IndexOffset of last element if exists, or return (0, 0) if MemIndex is empty()
    const OLAPIndexOffset find_last() const {
        if (_num_entries == 0 || segment_count() == 0) {
            return end();
        } else {
            OLAPIndexOffset off;
            off.segment = segment_count() - 1;
            off.offset = _meta[segment_count() - 1].count() - 1;
            return off;
        }
    }

    // Return IndexOffset of next element
    const OLAPIndexOffset next(const OLAPIndexOffset& pos) const;
    // Return IndexOffset of prev element
    const OLAPIndexOffset prev(const OLAPIndexOffset& pos) const;
    // Calculate IndexOffset from RowBlockPosition
    const OLAPIndexOffset get_offset(const RowBlockPosition& pos) const;

    // Return the 1-dimension, plain offset from IndexOffset
    // For example, there're 1000 index items, divided into 10 segments, 100 items in each segment,
    // the 2-dimension offset of the first element of first segment is (0, 0),
    // it's plain offset is 0,
    // the 2-dimension offset of the first element of second segment is (1, 0),
    // it's plain offset is 100
    const iterator_offset_t get_absolute_offset(const OLAPIndexOffset& offset) const {
        if (offset.segment >= segment_count() || offset.offset >= _meta[offset.segment].count()) {
            return _num_entries;
        } else {
            return _meta[offset.segment].range.first + offset.offset;
        }
    }

    // Return the 2-dimension, logical Offset from plain offset
    const OLAPIndexOffset get_relative_offset(iterator_offset_t absolute_offset) const;

    // Return content of index item, which IndexOffset is pos
    OLAPStatus get_entry(const OLAPIndexOffset& pos, EntrySlice* slice) const;

    // Return RowBlockPosition from IndexOffset
    OLAPStatus get_row_block_position(const OLAPIndexOffset& pos, RowBlockPosition* rbp) const;

    const size_t short_key_num() const { return _key_num; }

    // Return length of short keys in bytes, for example, there're two short key columns:
    // uint32_t/uint64_t the length is sizeof(uint32_t) + sizeof(uint64_t)
    const size_t short_key_length() const { return _key_length; }

    const size_t new_short_key_length() const { return _new_key_length; }

    // Return length of full index item,
    // which actually equals to short_key_length() plus sizeof(data_file_offset_t)
    const size_t entry_length() const { return short_key_length() + sizeof(data_file_offset_t); }

    const size_t new_entry_length() const { return _new_key_length + sizeof(data_file_offset_t); }

    // Return short key FieldInfo array
    const std::vector<TabletColumn>& short_key_columns() const { return *_short_key_columns; }

    // Return the number of indices in MemIndex
    size_t count() const { return _num_entries; }

    // Return the number of segments in MemIndex
    size_t segment_count() const { return _meta.size(); }

    bool zero_num_rows() const { return _num_entries == 0; }

    const size_t index_size() const { return _index_size; };

    const size_t data_size() const { return _data_size; };

    const size_t num_rows() const { return _num_rows; }

    bool delete_flag() const {
        if (_meta[0].file_header.message().has_delete_flag()) {
            return _meta[0].file_header.message().delete_flag();
        } else {
            return false;
        }
    }

    bool get_null_supported(uint32_t seg_id) {
        if (false == _meta[seg_id].file_header.message().has_null_supported()) {
            return false;
        } else {
            return _meta[seg_id].file_header.message().null_supported();
        }
    }

private:
    std::vector<SegmentMetaInfo> _meta;
    size_t _key_length;
    size_t _new_key_length;
    size_t _key_num;
    size_t _num_entries;
    size_t _index_size;
    size_t _data_size;
    size_t _num_rows;
    std::vector<TabletColumn>* _short_key_columns;

    std::shared_ptr<MemTracker> _tracker;
    std::unique_ptr<MemPool> _mem_pool;
    DISALLOW_COPY_AND_ASSIGN(MemIndex);
};

// 在同一个Segment内进行二分查找的比较类
class IndexComparator {
public:
    IndexComparator(const MemIndex* index, RowCursor* cursor)
            : _index(index), _cur_seg(0), _helper_cursor(cursor) {}

    // Destructor do nothing
    ~IndexComparator() {}

    bool operator()(const iterator_offset_t& index, const RowCursor& key) {
        return _compare(index, key, COMPARATOR_LESS);
    }

    bool operator()(const RowCursor& key, const iterator_offset_t& index) {
        return _compare(index, key, COMPARATOR_LARGER);
    }

    OLAPStatus set_segment_id(const iterator_offset_t& segment_id) {
        if (segment_id >= _index->segment_count()) {
            return OLAP_ERR_INDEX_EOF;
        }

        _cur_seg = segment_id;
        return OLAP_SUCCESS;
    }

private:
    bool _compare(const iterator_offset_t& index, const RowCursor& key, ComparatorEnum comparator) {
        EntrySlice slice;
        OLAPIndexOffset offset(_cur_seg, index);
        _index->get_entry(offset, &slice);

        _helper_cursor->attach(slice.data);

        if (comparator == COMPARATOR_LESS) {
            return index_compare_row(*_helper_cursor, key) < 0;
        } else {
            return index_compare_row(*_helper_cursor, key) > 0;
        }
    }

    const MemIndex* _index;
    iterator_offset_t _cur_seg;
    RowCursor* _helper_cursor;
};

// 用于寻找索引所在的Segment的比较类
class SegmentComparator {
public:
    SegmentComparator(const MemIndex* index, RowCursor* cursor)
            : _index(index), _helper_cursor(cursor) {}

    // Destructor do nothing
    ~SegmentComparator() {}

    bool operator()(const iterator_offset_t index, const RowCursor& key) {
        return _compare(index, key, COMPARATOR_LESS);
    }

    bool operator()(const RowCursor& key, iterator_offset_t index) {
        return _compare(index, key, COMPARATOR_LARGER);
    }

private:
    bool _compare(const iterator_offset_t& index, const RowCursor& key, ComparatorEnum comparator) {
        EntrySlice slice;
        slice.data = _index->_meta[index].buffer.data;
        //slice.length = _index->short_key_length();
        slice.length = _index->new_short_key_length();

        _helper_cursor->attach(slice.data);

        if (comparator == COMPARATOR_LESS) {
            return index_compare_row(*_helper_cursor, key) < 0;
        } else {
            return index_compare_row(*_helper_cursor, key) > 0;
        }
    }

    const MemIndex* _index;
    RowCursor* _helper_cursor;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_INDEX_H
