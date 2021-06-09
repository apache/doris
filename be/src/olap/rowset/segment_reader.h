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

// SegmentReader 用于读取一个Segment文件
class SegmentReader {
public:
    SegmentReader(const std::string file, SegmentGroup* segment_group, uint32_t segment_id,
                  const std::vector<uint32_t>& used_columns,
                  const std::set<uint32_t>& load_bf_columns, const Conditions* conditions,
                  const DeleteHandler* delete_handler, const DelCondSatisfied delete_status,
                  Cache* lru_cache, RuntimeState* runtime_state, OlapReaderStatistics* stats,
                  const std::shared_ptr<MemTracker>& parent_tracker = nullptr);

    ~SegmentReader();

    // 初始化segmentreader：
    // 1. 反序列化pb头，获取必要的信息；
    // 2. 检查文件版本
    // 3. 获取解压缩器
    // @return [description]
    OLAPStatus init(bool is_using_cache);

    // Must called before seek to block.
    // TODO(zc)
    OLAPStatus prepare(const std::vector<uint32_t>& columns);

    // 指定读取的第一个block和最后一个block，并初始化column reader
    // seek_to_block支持被多次调用
    // Inputs:
    //   first_block: 需要读取的第一个block
    //   last_block:  需要读取的最后一个block,如果last_block大于最大的block,
    //                则读取所有的block
    // 1. 按conditions过滤segment_group中的统计信息,  确定需要读取的block列表
    // 2. 读取blocks, 构造InStream
    // 3. 创建并初始化Readers
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

    // 返回当前segment中block的数目
    uint32_t block_count() const { return _block_count; }

    // 返回当前segment中，每块的行数
    uint32_t num_rows_in_block() { return _num_rows_in_block; }

    bool is_using_mmap() { return _is_using_mmap; }

    // 只允许在初始化之前选择，之后则无法更改
    // 暂时没有动态切换的需求
    void set_is_using_mmap(bool is_using_mmap) { _is_using_mmap = is_using_mmap; }

private:
    typedef std::vector<ColumnId>::iterator ColumnIdIterator;

    // 用于表示一段要读取的数据范围
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

    // 判断当前列是否需要读取
    // 当_include_columns为空时，直接返回true
    inline bool _is_column_included(ColumnId column_unique_id) {
        return _include_columns.count(column_unique_id) != 0;
    }

    inline bool _is_bf_column_included(ColumnId column_unique_id) {
        return _include_bf_columns.count(column_unique_id) != 0;
    }

    // 加载文件和必要的文件信息
    OLAPStatus _load_segment_file();

    // 设置encoding map，创建列时使用
    void _set_column_map();

    // 从header中获取当前文件压缩格式，并生成解压器，可通过_decompressor调用
    // @return 返回OLAP_SUCCESS代表版本检查通过
    OLAPStatus _set_decompressor();

    // 设置segment的相关信息，解压器，列，编码等
    OLAPStatus _set_segment_info();

    // 检查列存文件版本
    // @return 返回OLAP_SUCCESS代表版本检查通过
    OLAPStatus _check_file_version();

    // 选出要读取的列
    OLAPStatus _pick_columns();

    // 根据条件选出要读取的范围，会用条件在first block和last block之间标记合适的区块
    // NOTE. 注意范围是[first_block, last_block], 闭区间
    // @param  first_block 起始块号
    // @param  last_block  结束块号
    // @return
    OLAPStatus _pick_row_groups(uint32_t first_block, uint32_t last_block);
    OLAPStatus _pick_delete_row_groups(uint32_t first_block, uint32_t last_block);

    // 加载索引，将需要的列的索引读入内存
    OLAPStatus _load_index(bool is_using_cache);

    // 读出所有列，完整的流，（这里只是创建stream，在orc file里因为没有mmap因
    // 此意味着实际的数据读取， 而在这里并没有实际的读，只是圈出来需要的范围）
    OLAPStatus _read_all_data_streams(size_t* buffer_size);

    // 过滤并读取，（和_read_all_data_streams一样，也没有实际的读取数据）
    // 创建reader
    OLAPStatus _create_reader(size_t* buffer_size);

    // we implement seek to block in two phase. first, we just only move _next_block_id
    // to the position that we want goto; second, we seek the column streams to the
    // position we going to read.
    void _seek_to_block(int64_t block_id, bool without_filter);

    // seek to block id without check. only seek in cids's read stream.
    // because some columns may not be read
    OLAPStatus _seek_to_block_directly(int64_t block_id, const std::vector<uint32_t>& cids);

    // 跳转到某个row entry
    OLAPStatus _seek_to_row_entry(int64_t block_id);

    OLAPStatus _reset_readers();

    // 获取当前的table级schema。
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
    // 这个值的含义是，8 = 最大的int类型长度，int reader 一次读取最多会搞出来12个字符（MAX SCOPE）.
    // 假设完全没压缩，那么就会有这么多个字节，2应该是控制字符？
    // 那么最多读这么多，就一定足够把下一个字段解出来。
    static const int32_t WORST_UNCOMPRESSED_SLOP = 2 + 8 * 512;
    static const uint32_t CURRENT_COLUMN_DATA_VERSION = 1;

    std::string _file_name; // 文件名
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
    const Conditions* _conditions;    // 列过滤条件
    doris::FileHandler _file_handler; // 文件handler

    const DeleteHandler* _delete_handler = nullptr;
    DelCondSatisfied _delete_status;

    bool _eof; // eof标志

    // If this field is true, client must to call seek_to_block before
    // calling get_block.
    bool _need_to_seek_block = true;

    int64_t _end_block;            // 本次读取的结束块
    int64_t _current_block_id = 0; // 当前读取到的块

    // this is set by _seek_to_block, when get_block is called, first
    // seek to this block_id, then read block.
    int64_t _next_block_id = 0;
    int64_t _block_count; // 每一列中，index entry的数目应该相等。

    uint64_t _num_rows_in_block;
    bool _null_supported;
    uint64_t _header_length; // Header(FixHeader+PB)大小，读数据时需要偏移

    std::vector<ColumnReader*> _column_readers;      // 实际的数据读取器
    std::vector<StreamIndexReader*> _column_indices; // 保存column的index

    UniqueIdSet _include_columns; // 用于判断该列是不是被包含
    UniqueIdSet _include_bf_columns;
    UniqueIdToColumnIdMap _tablet_id_to_unique_id_map;  // tablet id到unique id的映射
    UniqueIdToColumnIdMap _unique_id_to_tablet_id_map;  // unique id到tablet id的映射
    UniqueIdToColumnIdMap _unique_id_to_segment_id_map; // unique id到segment id的映射

    std::map<ColumnId, StreamIndexReader*> _indices;
    std::map<StreamName, ReadOnlyFileStream*> _streams; //需要读取的流
    UniqueIdEncodingMap _encodings_map;                 // 保存encoding
    std::map<ColumnId, BloomFilterIndexReader*> _bloom_filters;
    Decompressor _decompressor; //根据压缩格式，设置的解压器
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
    bool _need_block_filter; //与include blocks组合使用，如果全不中，就不再读
    bool _is_using_mmap;     // 这个标记为true时，使用mmap来读取文件
    bool _is_data_loaded;
    size_t _buffer_size;

    std::vector<Cache::Handle*> _cache_handle;
    const FileHeader<ColumnDataHeaderMessage>* _file_header;

    std::shared_ptr<MemTracker> _tracker;
    std::unique_ptr<MemPool> _mem_pool;

    StorageByteBuffer* _shared_buffer;
    Cache* _lru_cache;
    RuntimeState* _runtime_state; // 用于统计内存消耗等运行时信息
    OlapReaderStatistics* _stats;

    // Set when seek_to_block is called, valid until next seek_to_block is called.
    bool _without_filter = false;

    DISALLOW_COPY_AND_ASSIGN(SegmentReader);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_READER_H
