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

#ifndef BDG_PALO_BE_SRC_OLAP_OLAP_INDEX_H
#define BDG_PALO_BE_SRC_OLAP_OLAP_INDEX_H

#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <iterator>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/column_data_file.pb.h"
#include "olap/atomic.h"
#include "olap/field.h"
#include "olap/file_helper.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_table.h"
#include "olap/row_cursor.h"
#include "olap/utils.h"

namespace palo {
class IndexComparator;
class OLAPIndex;
class OLAPTable;
class RowBlock;
class RowCursor;
class SegmentComparator;

typedef uint32_t data_file_offset_t;
typedef std::vector<FieldInfo> RowFields;

struct OLAPIndexFixedHeader {
    OLAPIndexFixedHeader() : data_length(0), num_rows(0) {}

    uint32_t data_length;
    uint64_t num_rows;
};

struct Slice {
    char* data;
    size_t length;
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
    OLAPIndexOffset(const iterator_offset_t& seg, const iterator_offset_t& off) :
            segment(seg),
            offset(off) {}
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
        return (segment == other.segment
                    && block_size == other.block_size
                    && data_offset == other.data_offset
                    && index_offset == other.index_offset);
    }

    bool operator!=(const RowBlockPosition& other) const {
        return !(*this == other);
    }

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
        snprintf(message,
                 sizeof(message),
                 "{segment=%u block_size=%u data_offset=%u index_offset=%u}",
                 segment,
                 block_size,
                 data_offset,
                 index_offset);
        return std::string(message);
    }

    uint32_t segment;
    uint32_t block_size;
    uint32_t data_offset;   // offset in data file
    uint32_t index_offset;  // offset in index file
};

// In memory presentation of index meta information
struct SegmentMetaInfo {
    SegmentMetaInfo() {
        range.first = range.last = 0;
        buffer.length = 0;
        buffer.data = NULL;
    }

    const size_t count() const {
        return range.last - range.first;
    }

    IDRange     range;
    Slice       buffer;
    FileHeader<OLAPIndexHeaderMessage, OLAPIndexFixedHeader>  file_header;
};

// In memory index structure, all index hold here
class MemIndex {
public:
    friend class OLAPIndex;
    friend class IndexComparator;
    friend class SegmentComparator;

    explicit MemIndex() :
            _key_length(0),
            _num_entries(0),
            _index_size(0),
            _data_size(0),
            _num_rows(0) {}
    ~MemIndex();

    // 初始化MemIndex, 传入short_key的总长度和对应的Field数组
    OLAPStatus init(size_t short_key_len, size_t short_key_num, RowFields* fields);

    // 加载一个segment到内存
    OLAPStatus load_segment(const char* file, size_t *current_num_rows_per_row_block);

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
    const OLAPIndexOffset find(const RowCursor& key,
                               RowCursor* helper_cursor,
                               bool find_last) const;

    // Same with begin()
    const OLAPIndexOffset find_first() const {
        return begin();
    }

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
        //size_t num_rows_per_block =
        //  _meta[offset.segment].file_header.message().num_rows_per_block();
        if (offset.segment >= segment_count() || offset.offset >= _meta[offset.segment].count()) {
            return _num_entries;
        } else {
            return _meta[offset.segment].range.first + offset.offset;
        }
    }

    // Return the 2-dimension, logical Offset from plain offset
    const OLAPIndexOffset get_relative_offset(iterator_offset_t absolute_offset) const;

    // Return content of index item, which IndexOffset is pos
    OLAPStatus get_entry(const OLAPIndexOffset& pos, Slice* slice) const;

    // Return RowBlockPosition from IndexOffset
    OLAPStatus get_row_block_position(const OLAPIndexOffset& pos, RowBlockPosition* rbp) const;

    const size_t short_key_num() const {
        return _key_num;
    }

    // Return length of short keys in bytes, for example, there're two short key columns:
    // uint32_t/uint64_t the length is sizeof(uint32_t) + sizeof(uint64_t)
    const size_t short_key_length() const {
        return _key_length;
    }

    // Return length of full index item,
    // which actually equals to short_key_length() plus sizeof(data_file_offset_t)
    const size_t entry_length() const {
        return short_key_length() + sizeof(data_file_offset_t);
    }

    // Return short key FieldInfo array
    const RowFields& short_key_fields() const {
        return *_fields;
    }

    // Return the number of indices in MemIndex
    size_t count() const {
        return _num_entries;
    }

    // Return the number of segments in MemIndex
    size_t segment_count() const {
        return _meta.size();
    }
    
    bool empty() const {
        return _num_entries == 0;
    }
    
    const size_t index_size() const {
        return _index_size;
    };
    
    const size_t data_size() const {
        return _data_size;
    };

    const size_t num_rows() const {
        return _num_rows;
    }

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
    size_t _key_num;
    size_t _num_entries;
    size_t _index_size;
    size_t _data_size;
    size_t _num_rows;
    RowFields*  _fields;

    DISALLOW_COPY_AND_ASSIGN(MemIndex);
};

// 在同一个Segment内进行二分查找的比较类
class IndexComparator {
public:
    IndexComparator(const MemIndex* index, RowCursor* cursor) :
            _index(index),
            _cur_seg(0),
            _helper_cursor(cursor) {}

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
    bool _compare(const iterator_offset_t& index,
                  const RowCursor& key,
                  ComparatorEnum comparator) {
        Slice slice;
        OLAPIndexOffset offset(_cur_seg, index);
        _index->get_entry(offset, &slice);

        if (_helper_cursor->attach(slice.data, _index->short_key_length()) != OLAP_SUCCESS) {
            throw ComparatorException();
        }

        if (comparator == COMPARATOR_LESS) {
            return _helper_cursor->index_cmp(key) < 0;
        } else {
            return _helper_cursor->index_cmp(key) > 0;
        }
    }

    const MemIndex* _index;
    iterator_offset_t _cur_seg;
    RowCursor* _helper_cursor;
};

// 用于寻找索引所在的Segment的比较类
class SegmentComparator {
public:
    SegmentComparator(const MemIndex* index, RowCursor* cursor) :
            _index(index),
            _helper_cursor(cursor) {}

    // Destructor do nothing
    ~SegmentComparator() {}
    
    bool operator()(const iterator_offset_t index, const RowCursor& key) {
        return _compare(index, key, COMPARATOR_LESS);
    }

    bool operator()(const RowCursor& key, iterator_offset_t index) {
        return _compare(index, key, COMPARATOR_LARGER);
    }

private:
    bool _compare(const iterator_offset_t& index,
                  const RowCursor& key,
                  ComparatorEnum comparator) {
        Slice slice;
        slice.data = _index->_meta[index].buffer.data;
        slice.length = _index->short_key_length();

        if (_helper_cursor->attach(slice.data, _index->short_key_length()) != OLAP_SUCCESS) {
            throw ComparatorException();
        }

        if (comparator == COMPARATOR_LESS) {
            return _helper_cursor->index_cmp(key) < 0;
        } else {
            return _helper_cursor->index_cmp(key) > 0;
        }
    }

    const MemIndex* _index;
    RowCursor* _helper_cursor;
};

// Class for managing OLAP table indices
// For fast key lookup, we maintain a sparse index for every data file. The
// index is sparse because we only have one pointer per row block. Each
// index entry contains the short key for the first row of the
// corresponding row block
class OLAPIndex {
    friend class MemIndex;
public:
    OLAPIndex(OLAPTable* table,
              Version version,
              VersionHash version_hash,
              bool delete_flag,
              uint32_t num_segments,
              time_t max_timestamp);

    virtual ~OLAPIndex();

    // Load the index into memory.
    OLAPStatus load();
    bool index_loaded();
    OLAPStatus load_pb(const char* file, uint32_t seg_id);

    bool has_column_statistics() {
        return _inited_column_statistics;
    }

    OLAPStatus set_column_statistics(std::vector<std::pair<Field *, Field *> > &column_statistics);
    
    std::vector<std::pair<Field *, Field *>> &get_column_statistics() {
        return _column_statistics;
    }

    OLAPStatus set_column_statistics_from_string(
            std::vector<std::pair<std::string, std::string>> &column_statistics_string,
            std::vector<bool> &has_null_flags);

    // 检查index文件和data文件的有效性
    OLAPStatus validate();

    // Finds position of the first (or last if find_last is set) row
    // block that may contain the smallest key equal to or greater than
    // 'key'. Returns true on success. If find_last is set, note that
    // the position is the last block that can possibly contain the
    // given key.
    OLAPStatus find_row_block(const RowCursor& key,
                          RowCursor* helper_cursor,
                          bool find_last,
                          RowBlockPosition* position) const;

    // Finds position of first row block contain the smallest key equal
    // to or greater than 'key'. Returns true on success.
    OLAPStatus find_short_key(const RowCursor& key,
                          RowCursor* helper_cursor,
                          bool find_last,
                          RowBlockPosition* position) const;

    // Returns position of the first row block in the index.
    OLAPStatus find_first_row_block(RowBlockPosition* position) const;

    // Returns position of the last row block in the index.
    OLAPStatus find_last_row_block(RowBlockPosition* position) const;

    // Given the position of a row block, finds position of the next block.
    // Sets eof to tru if there are no more blocks to go through, and
    // returns false. Returns true on success.
    OLAPStatus find_next_row_block(RowBlockPosition* position, bool* eof) const;

    // Given two positions in an index, low and high, set output to be
    // the midpoint between those two positions.  Returns the distance
    // between low and high as computed by ComputeDistance.
    OLAPStatus find_mid_point(const RowBlockPosition& low,
                          const RowBlockPosition& high,
                          RowBlockPosition* output,
                          uint32_t* dis) const;

    OLAPStatus find_prev_point(const RowBlockPosition& current, RowBlockPosition* prev) const;

    OLAPStatus get_row_block_entry(const RowBlockPosition& pos, Slice* entry) const;

    // Given a starting row block position, advances the position by
    // num_row_blocks, then stores back the new position through the
    // pointer.  Returns true on success, false on attempt to seek past
    // the last block.
    OLAPStatus advance_row_block(int64_t num_row_blocks, RowBlockPosition* position) const;

    // Computes the distance between two positions, in row blocks.
    uint32_t compute_distance(const RowBlockPosition& position1,
                              const RowBlockPosition& position2) const;

    // The following four functions are used for creating new index
    // files. AddSegment() and FinalizeSegment() start and end a new
    // segment respectively, while IndexRowBlock() and IndexShortKey()
    // add a new index entry to the current segment.
    OLAPStatus add_segment();
    OLAPStatus add_short_key(const RowCursor& short_key, const uint32_t data_offset);
    OLAPStatus add_row_block(const RowBlock& row_block, const uint32_t data_offset);
    OLAPStatus finalize_segment(uint32_t data_segment_size, int64_t num_rows);
    void sync();

    // reference count
    void acquire();
    void release();
    bool is_in_use();
    int64_t ref_count();

    // delete all files (*.idx; *.dat)
    void delete_all_files();

    // getters and setters.
    // get associated OLAPTable pointer
    OLAPTable* table() const {
        return _table;
    }

    void set_table(OLAPTable* table) {
        _table = table;
    }
    
    Version version() const {
        return _version;
    }
    
    VersionHash version_hash() const;

    bool delete_flag() const {
        return _index.delete_flag();
    }

    uint32_t num_segments() const {
        return _num_segments;
    }
    
    void set_num_segments(uint32_t num_segments) {
        _num_segments = num_segments;
    }
    
    time_t max_timestamp() const {
        return _max_timestamp;
    }
    
    size_t index_size() const {
        return _index.index_size();
    }
    
    size_t data_size() const {
        return _index.data_size();
    }

    int64_t num_rows() const {
        return _index.num_rows();
    }
    
    const size_t short_key_length() const {
        return _short_key_length;
    }
    
    const RowFields& short_key_fields() const {
        return _short_key_info_list;
    }
    
    bool empty() const {
        return _index.empty();
    }
    
    // return count of entries in MemIndex
    uint64_t num_index_entries() const;

    size_t current_num_rows_per_row_block() const {
        return _current_num_rows_per_row_block;
    }

    OLAPStatus get_row_block_position(const OLAPIndexOffset& pos, RowBlockPosition* rbp) const {
        return _index.get_row_block_position(pos, rbp);
    }
    
    inline const FileHeader<column_file::ColumnDataHeaderMessage>& get_seg_pb(uint32_t seg_id) const {
        return _seg_pb_map.at(seg_id);
    }

    inline bool get_null_supported(uint32_t seg_id) {
        return _index.get_null_supported(seg_id);
    }

private:
    void _check_io_error(OLAPStatus res);

    std::string _construct_index_file_path(const Version& version,
                                           VersionHash version_hash,
                                           uint32_t segment) const {
        return OLAPTable::construct_file_path(_header_file_name,
                                              version,
                                              version_hash,
                                              segment,
                                              "idx");
    }

    std::string _construct_data_file_path(const Version& version,
                                          VersionHash version_hash,
                                          uint32_t segment) const {
        return OLAPTable::construct_file_path(_header_file_name,
                                              version,
                                              version_hash,
                                              segment,
                                              "dat");
    }

    OLAPTable* _table;                 // table definition for this index
    Version _version;                  // version of associated data file
    bool _delete_flag;
    time_t _max_timestamp;             // max pusher delta timestamp
    uint32_t _num_segments;            // number of segments in this index
    VersionHash _version_hash;      // version hash for this index
    bool _index_loaded;                // whether the index has been read
    atomic_t _ref_count;               // reference count
    MemIndex _index;

    std::string _header_file_name;     // the name of the related header file
    // short key对应的field_info数组
    RowFields _short_key_info_list;
    // short key对应的总长度
    size_t _short_key_length;

    // 以下是写入流程时需要的一些中间状态
    // 当前写入文件的FileHandler
    FileHandler _current_file_handler;
    // 当前写入的FileHeader
    FileHeader<OLAPIndexHeaderMessage, OLAPIndexFixedHeader> _file_header;
    // 当前写入的short_key的buf
    char* _short_key_buf;
    // 当前写入的segment的checksum
    uint32_t _checksum;
    // 当前写入时用作索引项的RowCursor
    RowCursor _current_index_row;

    // Lock held while loading the index.
    mutable boost::mutex _index_load_lock;

    size_t _current_num_rows_per_row_block;

    bool _inited_column_statistics;

    std::vector<std::pair<Field *, Field *> > _column_statistics;
    std::vector<bool> _has_null_flags;
    std::unordered_map<uint32_t, FileHeader<column_file::ColumnDataHeaderMessage> > _seg_pb_map;

    DISALLOW_COPY_AND_ASSIGN(OLAPIndex);
};

class OLAPUnusedIndex {
    DECLARE_SINGLETON(OLAPUnusedIndex);
public:
    OLAPStatus init() {
        clear();
        return OLAP_SUCCESS;
    }

    void clear() {
        _unused_index_list.clear();
    }

    void start_delete_unused_index();

    void add_unused_index(OLAPIndex* olap_index);

private:
    typedef std::list<OLAPIndex*> unused_index_list_t;
    unused_index_list_t _unused_index_list;
    MutexLock _mutex;

    DISALLOW_COPY_AND_ASSIGN(OLAPUnusedIndex);
};

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_OLAP_INDEX_H
