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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_GROUP_H
#define DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_GROUP_H

#include <condition_variable>
#include <iterator>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gen_cpp/column_data_file.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/column_mapping.h"
#include "olap/field.h"
#include "olap/file_helper.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_index.h"
#include "olap/row_cursor.h"
#include "olap/utils.h"

namespace doris {

// Class for segments management
// For fast key lookup, we maintain a sparse index for every data file. The
// index is sparse because we only have one pointer per row block. Each
// index entry contains the short key for the first row of the
// corresponding row block
class SegmentGroup {
    friend class MemIndex;

public:
    SegmentGroup(int64_t tablet_id, const RowsetId& rowset_id, const TabletSchema* tablet_schema,
                 const std::string& rowset_path_prefix, Version version,
                 bool delete_flag, int segment_group_id, int32_t num_segments);

    SegmentGroup(int64_t tablet_id, const RowsetId& rowset_id, const TabletSchema* tablet_schema,
                 const std::string& rowset_path_prefix, bool delete_flag, int32_t segment_group_id,
                 int32_t num_segments, bool is_pending, TPartitionId partition_id,
                 TTransactionId transaction_id);

    virtual ~SegmentGroup();

    // Load the index into memory.
    OLAPStatus load(bool use_cache = true);
    bool index_loaded();
    OLAPStatus load_pb(const char* file, uint32_t seg_id);

    bool has_zone_maps() { return _zone_maps.size() != 0; }

    OLAPStatus add_zone_maps_for_linked_schema_change(
            const std::vector<std::pair<WrapperField*, WrapperField*>>& zone_map_fields,
            const SchemaMapping& schema_mapping);

    OLAPStatus add_zone_maps(
            const std::vector<std::pair<WrapperField*, WrapperField*>>& zone_map_fields);

    OLAPStatus add_zone_maps(std::vector<std::pair<std::string, std::string>>& zone_map_strings,
                             std::vector<bool>& null_vec);

    const std::vector<std::pair<WrapperField*, WrapperField*>>& get_zone_maps() {
        return _zone_maps;
    }

    // 检查index文件和data文件的有效性
    OLAPStatus validate();

    // this function should be called after load
    bool check();

    // Finds position of first row block contain the smallest key equal
    // to or greater than 'key'. Returns true on success.
    OLAPStatus find_short_key(const RowCursor& key, RowCursor* helper_cursor, bool find_last,
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
    OLAPStatus find_mid_point(const RowBlockPosition& low, const RowBlockPosition& high,
                              RowBlockPosition* output, uint32_t* dis) const;

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

    // reference count
    void acquire();
    void release();
    bool is_in_use();
    int64_t ref_count();

    // delete all files (*.idx; *.dat)
    bool delete_all_files();

    inline Version version() const { return _version; }
    inline void set_version(Version version) { _version = version; }

    inline bool is_pending() const { return _is_pending; }
    inline void set_pending_finished() { _is_pending = false; }

    inline TPartitionId partition_id() const { return _partition_id; }
    inline TTransactionId transaction_id() const { return _txn_id; }

    inline bool delete_flag() const { return _delete_flag; }

    inline int32_t segment_group_id() const { return _segment_group_id; }
    inline void set_segment_group_id(int32_t segment_group_id) {
        _segment_group_id = segment_group_id;
    }

    inline PUniqueId load_id() const { return _load_id; }
    inline void set_load_id(const PUniqueId& load_id) { _load_id = load_id; }

    inline int32_t num_segments() const { return _num_segments; }
    inline void set_num_segments(int32_t num_segments) { _num_segments = num_segments; }

    size_t index_size() const { return _index.index_size(); }

    size_t data_size() const { return _index.data_size(); }

    int64_t num_rows() const { return _index.num_rows(); }

    const size_t short_key_length() const { return _short_key_length; }

    const size_t new_short_key_length() const { return _new_short_key_length; }

    const std::vector<TabletColumn>& short_key_columns() const { return _short_key_columns; }

    bool empty() const { return _empty; }

    bool zero_num_rows() const {
        // previous version may has non-sense file in disk.
        // to be compatible, it should be handled.
        return _index.zero_num_rows();
    }

    void set_empty(bool empty) { _empty = empty; }

    // return count of entries in MemIndex
    uint64_t num_index_entries() const;

    OLAPStatus get_row_block_position(const OLAPIndexOffset& pos, RowBlockPosition* rbp) const {
        return _index.get_row_block_position(pos, rbp);
    }

    inline const FileHeader<ColumnDataHeaderMessage>* get_seg_pb(uint32_t seg_id) const {
        return &(_seg_pb_map.at(seg_id));
    }

    inline bool get_null_supported(uint32_t seg_id) { return _index.get_null_supported(seg_id); }

    std::string construct_index_file_path(const std::string& snapshot_path,
                                          int32_t segment_id) const;
    std::string construct_index_file_path(int32_t segment_id) const;
    std::string construct_data_file_path(const std::string& snapshot_path,
                                         int32_t segment_id) const;
    std::string construct_data_file_path(int32_t segment_id) const;

    // these two functions are for compatible, and will be deleted later
    // so it is better not to use it.
    std::string construct_old_index_file_path(const std::string& path_prefix,
                                              int32_t segment_id) const;
    std::string construct_old_data_file_path(const std::string& path_prefix,
                                             int32_t segment_id) const;

    size_t current_num_rows_per_row_block() const;

    const TabletSchema& get_tablet_schema();

    int get_num_key_columns();

    // for agg, uniq and replace model  get_num_zone_map_columns() = get_num_key_columns(),
    // for duplicate mode get_num_zone_map_columns() == num_columns
    int get_num_zone_map_columns();

    int get_num_short_key_columns();

    size_t get_num_rows_per_row_block();

    std::string rowset_path_prefix();

    int64_t get_tablet_id();

    const RowsetId& rowset_id() { return _rowset_id; }

    OLAPStatus convert_from_old_files(const std::string& snapshot_path,
                                      std::vector<std::string>* success_links);

    OLAPStatus convert_to_old_files(const std::string& snapshot_path,
                                    std::vector<std::string>* success_links);

    OLAPStatus remove_old_files(std::vector<std::string>* links_to_remove);

    OLAPStatus copy_files_to(const std::string& dir);

    OLAPStatus link_segments_to_path(const std::string& dest_path, const RowsetId& rowset_id);

private:
    std::string _construct_file_name(int32_t segment_id, const std::string& suffix) const;
    std::string _construct_file_name(const RowsetId& rowset_id, int32_t segment_id,
                                     const std::string& suffix) const;

    std::string _construct_old_pending_file_path(const std::string& path_prefix, int32_t segment_id,
                                                 const std::string& suffix) const;

    std::string _construct_old_file_path(const std::string& path_prefix, int32_t segment_id,
                                         const std::string& suffix) const;

    std::string _construct_err_sg_file_path(const std::string& path_prefix, int32_t segment_id,
                                            const std::string& suffix) const;

    std::string _construct_err_sg_index_file_path(const std::string& path_prefix,
                                                  int32_t segment_id) const;

    std::string _construct_err_sg_data_file_path(const std::string& path_prefix,
                                                 int32_t segment_id) const;

private:
    int64_t _tablet_id;
    RowsetId _rowset_id;
    const TabletSchema* _schema;
    std::string _rowset_path_prefix; // path of rowset
    Version _version;                // version of associated data file
    bool _delete_flag;
    int32_t _segment_group_id;       // segment group id of segment group
    PUniqueId _load_id;              // load id for segment group
    int32_t _num_segments;           // number of segments in this segment group
    bool _index_loaded;              // whether the segment group has been read
    std::atomic<int64_t> _ref_count; // reference count
    MemIndex _index;
    bool _is_pending;
    TPartitionId _partition_id;
    TTransactionId _txn_id;

    // short key对应的column information
    std::vector<TabletColumn> _short_key_columns;
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
    bool _new_segment_created;
    // 当前写入的FileHeader
    FileHeader<OLAPIndexHeaderMessage, OLAPIndexFixedHeader> _file_header;

    bool _empty;

    // Lock held while loading the index.
    mutable std::mutex _index_load_lock;
    size_t _current_num_rows_per_row_block;

    std::vector<std::pair<WrapperField*, WrapperField*>> _zone_maps;
    std::unordered_map<uint32_t, FileHeader<ColumnDataHeaderMessage>> _seg_pb_map;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_GROUP_H
