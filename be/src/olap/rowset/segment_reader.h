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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_READER_H
#define DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_READER_H

#include <gen_cpp/column_data_file.pb.h>
#include <gen_cpp/olap_common.pb.h>

#include <iostream>
#include <map>
#include <string>

#include "olap/bloom_filter_reader.h"
#include "olap/column_predicate.h"
#include "olap/compress.h"
#include "olap/delete_handler.h"
#include "olap/file_helper.h"
#include "olap/file_stream.h"
#include "olap/in_stream.h"
#include "olap/lru_cache.h"
#include "olap/olap_cond.h"
#include "olap/olap_define.h"
#include "olap/row_cursor.h"
#include "olap/rowset/column_reader.h"
#include "olap/rowset/segment_group.h"
#include "olap/stream_index_reader.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"

namespace doris {

// SegmentReader is used to read a Segment file
class SegmentReader {
public:
    SegmentReader(const std::string file, SegmentGroup* segment_group, uint32_t segment_id,
                  const std::vector<uint32_t>& used_columns,
                  const std::set<uint32_t>& load_bf_columns, const Conditions* conditions,
                  const DeleteHandler* delete_handler, const DelCondSatisfied delete_status,
                  Cache* lru_cache, RuntimeState* runtime_state, OlapReaderStatistics* stats,
                  const std::shared_ptr<MemTracker>& parent_tracker = nullptr);

    ~SegmentReader();

    // Initialize segmentreader:
    // 1. Deserialize the pb header to obtain the necessary information;
    // 2. Check the file version
    // 3. Get the decompressor
    // @return [description]
    OLAPStatus init(bool is_using_cache);

    // Must called before seek to block.
    // TODO(zc)
    OLAPStatus prepare(const std::vector<uint32_t>& columns);

    // Specify the first block and the last block to read, and initialize the column reader
    // seek_to_block supports being called multiple times
    // Inputs:
    //   first_block: the first block that needs to be read
    //   last_block: The last block that needs to be read, if last_block is greater than the largest block,
    // read all blocks
    // 1. Filter the statistics in segment_group according to conditions and determine the list of blocks that need to be read
    // 2. Read blocks, construct InStream
    // 3. Create and initialize Readers
    // Outputs:
    // next_block_id:
    //      block with next_block_id would read if get_block called again.
    //      this field is used to set batch's limit when client found logical end is reach
    OLAPStatus seek_to_block(uint32_t first_block, uint32_t last_block, bool without_filter,
                             uint32_t* next_block_id, bool* eof);

    // get vector batch from this segment.
    // next_block_id:
    //      block with next_block_id would read if get_block called again.
    //      this field is used to set batch's limit when client found logical end is reach
    // ATTN: If you change batch to contain more columns, you must call seek_to_block again.
    OLAPStatus get_block(VectorizedRowBatch* batch, uint32_t* next_block_id, bool* eof);

    bool eof() const { return _eof; }

    // Returns the number of blocks in the current segment
    uint32_t block_count() const { return _block_count; }

    // Returns the number of rows in each block in the current segment
    uint32_t num_rows_in_block() { return _num_rows_in_block; }

    bool is_using_mmap() { return _is_using_mmap; }

    // Only allowed to be selected before initialization, and cannot be changed afterwards
    // There is no need for dynamic switching at the moment
    void set_is_using_mmap(bool is_using_mmap) { _is_using_mmap = is_using_mmap; }

private:
    typedef std::vector<ColumnId>::iterator ColumnIdIterator;

    // Used to indicate a range of data to be read
    struct DiskRange {
        int64_t offset;
        int64_t end;
        DiskRange() : offset(0), end(0) {}
    };

    struct VectorizedPositionInfo {
        uint32_t column_id;
        uint32_t column_position;
        uint32_t offset_position;
    };

    static CacheKey _construct_index_stream_key(char* buf, size_t len, const std::string& file_name,
                                                ColumnId unique_column_id,
                                                StreamInfoMessage::Kind kind);

    static void _delete_cached_index_stream(const CacheKey& key, void* value);

    // Determine whether the current column needs to be read
    // When _include_columns is empty, return true directly
    inline bool _is_column_included(ColumnId column_unique_id) {
        return _include_columns.count(column_unique_id) != 0;
    }

    inline bool _is_bf_column_included(ColumnId column_unique_id) {
        return _include_bf_columns.count(column_unique_id) != 0;
    }

    // Load files and necessary file information
    OLAPStatus _load_segment_file();

    // Set the encoding map and use it when creating columns
    void _set_column_map();

    //Get the current file compression format from the header and generate a decompressor, which can be called by _decompressor
    // @return Return OLAP_SUCCESS on behalf of the version check passed
    OLAPStatus _set_decompressor();

    // Set segment related information, decompressor, column, encoding, etc.
    OLAPStatus _set_segment_info();

    // Check the listed file version
    // @return Return OLAP_SUCCESS on behalf of the version check passed
    OLAPStatus _check_file_version();

    // Select the column to be read
    OLAPStatus _pick_columns();

    // Select the range to be read according to the conditions, and use the conditions to mark the appropriate block between the first block and the last block
    // NOTE. Note that the range is [first_block, last_block], closed interval
    // @param  first_block : Starting block number
    // @param  last_block  : End block number
    // @return
    OLAPStatus _pick_row_groups(uint32_t first_block, uint32_t last_block);
    OLAPStatus _pick_delete_row_groups(uint32_t first_block, uint32_t last_block);

    // Load the index, read the index of the required column into memory
    OLAPStatus _load_index(bool is_using_cache);

    // Read all the columns, the complete stream, (here just create the stream, because there is no mmap in the orc file, 
    // it means the actual data is read, but there is no actual read here, just circle the required range)
    OLAPStatus _read_all_data_streams(size_t* buffer_size);

    // Filter and read, (like _read_all_data_streams, there is no actual read data)
    // Create reader
    OLAPStatus _create_reader(size_t* buffer_size);

    // we implement seek to block in two phase. first, we just only move _next_block_id
    // to the position that we want goto; second, we seek the column streams to the
    // position we going to read.
    void _seek_to_block(int64_t block_id, bool without_filter);

    // seek to block id without check. only seek in cids's read stream.
    // because some columns may not be read
    OLAPStatus _seek_to_block_directly(int64_t block_id, const std::vector<uint32_t>& cids);

    // Jump to a row entry
    OLAPStatus _seek_to_row_entry(int64_t block_id);

    OLAPStatus _reset_readers();

    // Get the current table-level schema.
    inline const TabletSchema& tablet_schema() { return _segment_group->get_tablet_schema(); }

    inline const ColumnDataHeaderMessage& _header_message() { return _file_header->message(); }

    OLAPStatus _init_include_blocks(uint32_t first_block, uint32_t last_block);

    inline const int32_t _get_included_row_index_stream_num() {
        int32_t included_row_index_stream_num = 0;
        for (int32_t i = 0; i < _header_message().stream_info_size(); ++i) {
            const StreamInfoMessage& message = _header_message().stream_info(i);
            ColumnId unique_column_id = message.column_unique_id();
            if (0 == _unique_id_to_segment_id_map.count(unique_column_id)) {
                continue;
            }

            if ((_is_column_included(unique_column_id) &&
                 message.kind() == StreamInfoMessage::ROW_INDEX) ||
                (_is_bf_column_included(unique_column_id) &&
                 message.kind() == StreamInfoMessage::BLOOM_FILTER)) {
                ++included_row_index_stream_num;
            }
        }
        return included_row_index_stream_num;
    }

    OLAPStatus _load_to_vectorized_row_batch(VectorizedRowBatch* batch, size_t size);

    FieldAggregationMethod _get_aggregation_by_index(uint32_t index) {
        const TabletSchema& tablet_schema = _segment_group->get_tablet_schema();
        if (index < tablet_schema.num_columns()) {
            return tablet_schema.column(index).aggregation();
        }

        return OLAP_FIELD_AGGREGATION_UNKNOWN;
    }

    FieldType _get_field_type_by_index(uint32_t index) {
        const TabletSchema& tablet_schema = _segment_group->get_tablet_schema();
        if (index < tablet_schema.num_columns()) {
            return tablet_schema.column(index).type();
        }

        return OLAP_FIELD_TYPE_NONE;
    }

private:
    static const int32_t BYTE_STREAM_POSITIONS = 1;
    static const int32_t RUN_LENGTH_BYTE_POSITIONS = BYTE_STREAM_POSITIONS + 1;
    static const int32_t BITFIELD_POSITIONS = RUN_LENGTH_BYTE_POSITIONS + 1;
    static const int32_t RUN_LENGTH_INT_POSITIONS = BYTE_STREAM_POSITIONS + 1;
    // The meaning of this value is that 8 = the largest int type length, and an int reader can read up to 12 characters at a time (MAX SCOPE).
    // Assuming no compression at all, then there will be so many bytes, 2 should be the control character?
    // Then read at most so many, it must be enough to solve the next field.
    static const int32_t WORST_UNCOMPRESSED_SLOP = 2 + 8 * 512;
    static const uint32_t CURRENT_COLUMN_DATA_VERSION = 1;

    std::string _file_name; // File name
    SegmentGroup* _segment_group;
    uint32_t _segment_id;
    // columns that can be used by client. when client seek to range's start or end,
    // client may read more columns than normal read.
    // For example:
    //  table1's schema is 'k1, k2, v1'. which k1, k2 is key column, v1 is value column.
    //  for query 'select sum(v1) from table1', client split all data to sub-range in logical,
    //  so, one sub-range need to seek to right position with k1 and k2; then only read v1.
    //  In this situation, _used_columns contains (k1, k2, v1)
    std::vector<uint32_t> _used_columns;
    UniqueIdSet _load_bf_columns;
    const Conditions* _conditions;    // Column filter
    doris::FileHandler _file_handler; // File handler

    const DeleteHandler* _delete_handler = nullptr;
    DelCondSatisfied _delete_status;

    bool _eof; // EOF Sign

    // If this field is true, client must to call seek_to_block before
    // calling get_block.
    bool _need_to_seek_block = true;

    int64_t _end_block;            // The end block read this time
    int64_t _current_block_id = 0; // Block currently read

    // this is set by _seek_to_block, when get_block is called, first
    // seek to this block_id, then read block.
    int64_t _next_block_id = 0;
    int64_t _block_count; // In each column, the number of index entries should be equal.

    uint64_t _num_rows_in_block;
    bool _null_supported;
    uint64_t _header_length; // Header(FixHeader+PB) size, need to offset when reading data

    std::vector<ColumnReader*> _column_readers;      // Actual data reader
    std::vector<StreamIndexReader*> _column_indices; // Save the index of the column

    UniqueIdSet _include_columns; // Used to determine whether the column is included
    UniqueIdSet _include_bf_columns;
    UniqueIdToColumnIdMap _tablet_id_to_unique_id_map;  // The mapping from tablet id to unique id
    UniqueIdToColumnIdMap _unique_id_to_tablet_id_map;  // Mapping from unique id to tablet id
    UniqueIdToColumnIdMap _unique_id_to_segment_id_map; // Mapping from unique id to segment id

    std::map<ColumnId, StreamIndexReader*> _indices;
    std::map<StreamName, ReadOnlyFileStream*> _streams; // Need to read the stream
    UniqueIdEncodingMap _encodings_map;                 // Save encoding
    std::map<ColumnId, BloomFilterIndexReader*> _bloom_filters;
    Decompressor _decompressor; // According to the compression format, set the decompressor
    StorageByteBuffer* _mmap_buffer;

    /*
     * _include_blocks is used for saving the state of block when encountering delete conditions,
     * in this place the delete condition include the delete condition, the query filter condition
     * and the bloom filter condition.
     * DEL_SATISFIED is for block meet the delete condition, should be filtered.
     *    but it is not stored, it reflect in _include_blocks.
     * DEL_NOT_SATISFIED is for block not meet the delete condition, should be held to read.
     * DEL_PARTIAL_SATISFIED is for block can't be filtered by the delete condition in block level.
    */
    uint8_t* _include_blocks;
    uint32_t _remain_block;
    bool _need_block_filter; // Used in combination with include blocks, if none of them are in, no longer read
    bool _is_using_mmap;     // When this flag is true, use mmap to read the file
    bool _is_data_loaded;
    size_t _buffer_size;

    std::vector<Cache::Handle*> _cache_handle;
    const FileHeader<ColumnDataHeaderMessage>* _file_header;

    std::shared_ptr<MemTracker> _tracker;
    std::unique_ptr<MemPool> _mem_pool;

    StorageByteBuffer* _shared_buffer;
    Cache* _lru_cache;
    RuntimeState* _runtime_state; // Used to count runtime information such as memory consumption
    OlapReaderStatistics* _stats;

    // Set when seek_to_block is called, valid until next seek_to_block is called.
    bool _without_filter = false;

    DISALLOW_COPY_AND_ASSIGN(SegmentReader);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_READER_H
