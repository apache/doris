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

#pragma once

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
#include "olap/olap_index.h"
#include "olap/utils.h"

namespace doris {

// Class for managing OLAP table indices
// For fast key lookup, we maintain a sparse index for every data file. The
// index is sparse because we only have one pointer per row block. Each
// index entry contains the short key for the first row of the
// corresponding row block
class Rowset {
    friend class MemIndex;
public:
    Rowset(OLAPTable* table, Version version, VersionHash version_hash,
              bool delete_flag, int rowset_id, int32_t num_segments);

    Rowset(OLAPTable* table, bool delete_flag, int32_t rowset_id,
              int32_t num_segments, bool is_pending,
              TPartitionId partition_id, TTransactionId transaction_id);

    virtual ~Rowset();

    // Load the index into memory.
    OLAPStatus load();
    bool index_loaded();
    OLAPStatus load_pb(const char* file, uint32_t seg_id);

    bool has_column_statistics() {
        return _column_statistics.size() != 0;
    }

    OLAPStatus add_column_statistics_for_linked_schema_change(
        const std::vector<std::pair<WrapperField*, WrapperField*>>& column_statistic_fields);

    OLAPStatus add_column_statistics(
        const std::vector<std::pair<WrapperField*, WrapperField*>>& column_statistic_fields);

    OLAPStatus add_column_statistics(
        std::vector<std::pair<std::string, std::string>> &column_statistic_strings,
        std::vector<bool> &null_vec);

    const std::vector<std::pair<WrapperField*, WrapperField*>>& get_column_statistics() {
        return _column_statistics;
    }

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

    OLAPStatus get_row_block_entry(const RowBlockPosition& pos, EntrySlice* entry) const;

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
    inline OLAPTable* table() const { return _table; }
    inline void set_table(OLAPTable* table) { _table = table; }

    inline Version version() const { return _version; }
    inline VersionHash version_hash() const { return _version_hash; }

    inline bool is_pending() const { return _is_pending; }
    inline void set_pending_finished() { _is_pending = false; }

    inline TPartitionId partition_id() const { return _partition_id; }
    inline TTransactionId transaction_id() const { return _transaction_id; }

    inline bool delete_flag() const { return _delete_flag; }

    inline int32_t rowset_id() const { return _rowset_id; }
    inline void set_rowset_id(int32_t rowset_id) { _rowset_id = rowset_id; }

    inline PUniqueId load_id() const { return _load_id; }
    inline void set_load_id(const PUniqueId& load_id) { _load_id = load_id; }

    inline int32_t num_segments() const { return _num_segments; }
    inline void set_num_segments(int32_t num_segments) { _num_segments = num_segments; }

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

    const size_t new_short_key_length() const {
        return _new_short_key_length;
    }

    const RowFields& short_key_fields() const {
        return _short_key_info_list;
    }

    bool empty() const {
        return _empty;
    }

    bool zero_num_rows() const {
        // previous version may has non-sense file in disk.
        // to be compatible, it should be handled.
        return _index.zero_num_rows();
    }

    void set_empty(bool empty) {
        _empty = empty;
    }

    // return count of entries in MemIndex
    uint64_t num_index_entries() const;

    size_t current_num_rows_per_row_block() const {
        return _current_num_rows_per_row_block;
    }

    OLAPStatus get_row_block_position(const OLAPIndexOffset& pos, RowBlockPosition* rbp) const {
        return _index.get_row_block_position(pos, rbp);
    }

    inline const FileHeader<ColumnDataHeaderMessage>* get_seg_pb(uint32_t seg_id) const {
        return &(_seg_pb_map.at(seg_id));
    }

    inline bool get_null_supported(uint32_t seg_id) {
        return _index.get_null_supported(seg_id);
    }

    std::string construct_index_file_path(int32_t rowset_id, int32_t segment) const;
    std::string construct_data_file_path(int32_t rowset_id, int32_t segment) const;
    void publish_version(Version version, VersionHash version_hash);

private:
    void _check_io_error(OLAPStatus res);

    OLAPTable* _table;                 // table definition for this index
    Version _version;                  // version of associated data file
    VersionHash _version_hash;         // version hash for this index
    bool _delete_flag;
    int32_t _rowset_id;                // rowset id of olapindex
    PUniqueId _load_id;                // load id for rowset
    int32_t _num_segments;             // number of segments in this index
    bool _index_loaded;                // whether the index has been read
    atomic_t _ref_count;               // reference count
    MemIndex _index;
    bool _is_pending;
    TPartitionId _partition_id;
    TTransactionId _transaction_id;

    // short key对应的field_info数组
    RowFields _short_key_info_list;
    // short key对应的总长度
    size_t _short_key_length;
    size_t _new_short_key_length;
    // 当前写入的short_key的buf
    char* _short_key_buf;
    // 当前写入的segment的checksum
    uint32_t _checksum;
    // 当前写入时用作索引项的RowCursor
    RowCursor _current_index_row;

    // 以下是写入流程时需要的一些中间状态
    // 当前写入文件的FileHandler
    FileHandler _current_file_handler;
    bool _file_created;
    bool _new_segment_created;
    // 当前写入的FileHeader
    FileHeader<OLAPIndexHeaderMessage, OLAPIndexFixedHeader> _file_header;

    bool _empty;

    // Lock held while loading the index.
    mutable boost::mutex _index_load_lock;

    size_t _current_num_rows_per_row_block;

    std::vector<std::pair<WrapperField*, WrapperField*>> _column_statistics;
    std::unordered_map<uint32_t, FileHeader<ColumnDataHeaderMessage> > _seg_pb_map;

    DISALLOW_COPY_AND_ASSIGN(Rowset);
};

}
