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

#ifndef BDG_PALO_BE_SRC_OLAP_OLAP_DATA_H
#define BDG_PALO_BE_SRC_OLAP_OLAP_DATA_H

#include <map>
#include <string>
#include <vector>

#include "olap/i_data.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_index.h"
#include "olap/olap_table.h"
#include "olap/row_block.h"

namespace palo {

// Row Storage Table is deprecated.
// This file will be removed in succedent release.

class OLAPTable;
class RowBlock;
class RowCursor;

// Class for managing data files.
//
// Typically, an 'OLAPData' represent a single version data file. Internally, an OLAPData is split
// into segments of typically 4GB files. Within each segment, the data is stored as RowBlocks.
//
// The interface for 'OLAPData' is cursor based. There are methods for positioning the cursor 
// inside the data file, and then get_next_row() will fetch the next row.
//
// The original OLAPData File is a kind of row-column mixed file type. And it is named as
// RAW_COLUMN_MIXED.
//
// To reduce the number of code to refactor, the class name "OLAPData" is reserved for
// the original RAW_COLUMN_MIXED.
class OLAPData: public IData {
public:
    explicit OLAPData(Rowset* index);
    virtual ~OLAPData();

    // 初始化, 和unpickle统一到同一流程上
    virtual OLAPStatus init();

    OLAPStatus get_first_row_block(RowBlock** row_block,
                               const char** packed_row_block,
                               uint32_t* packed_row_block_size);

    // Advances to the next row block and returns it through row_block.
    // Also returns the corresponding packed_row_block and
    // packed_row_block_size, which is the size of packed_row_block.
    // Returns OLAP_SUCCESS on success or end of file has been reached;
    // other if in case of error.  The class retains ownership of row_block and
    // packed_row_block.
    OLAPStatus get_next_row_block(RowBlock** row_block,
                              const char** packed_row_block,
                              uint32_t* packed_row_block_size);

    virtual OLAPStatus get_next_row_block(RowBlock** row_block);

    virtual OLAPStatus get_first_row_block(RowBlock** row_block);

    RowBlock* seek_and_get_row_block(const RowBlockPosition& position);

    // Points the internal cursor to either the first row or the last row.
    // Returns NULL in case of an error.
    const RowCursor* get_first_row();

    // Advances the internal cursor to the next row and returns that row.
    // Sets _eof if there is no more row left.
    const RowCursor* get_next_row();

    // Points internal cursor to the first row equal to or larger than 'key'.
    // to key. Returns a pointer to the row or NULL if 1) there is an
    // error, or 2) the key exceeds any row in the table.
    const RowCursor* find_row(const RowCursor& key, bool find_last_key, bool is_end_key);

    // find_last_end_key false:<; true:<=
    OLAPStatus set_end_key(const RowCursor* end_key, bool find_last_end_key);

    OLAPStatus prepare_block_read(
        const RowCursor* start_key, bool find_start_key,
        const RowCursor* end_key, bool find_end_key,
        RowBlock** block) override;

    OLAPStatus get_next_block(RowBlock** block) override;

    // The following four functions are used for creating new date
    // files. add_segment() and finalize_segment() start and end a new
    // segment respectively, while add_row_block() and add_packed_rowblock()
    // add a new data block to the current segment.(only writer)
    OLAPStatus add_segment();

    // TODO(fdy): 未实现方法,等待有使用需求时再实现
    OLAPStatus add_packed_rowblock(
            const char* packed_row_block, const uint32_t packed_row_block_size);

    // @brief add row block into OLAPData, row block will be compressed before writing.
    // @param [in] row_block
    // @param [out] start_data_offset  start data offset of the row block in OLAPData Segment. It
    //                                 is used as data offset of corresponding index item.
    // @param [out] end_data_offset    end data offset of the row block in OLAPData Segment. it
    //                                 equals to the current segment file length.
    // @return  OLAPStatus  OLAP_SUCCESS if succeed, or else OLAP_ERR_XXX
    // @note
    OLAPStatus add_row_block(RowBlock* row_block,
                             uint32_t* start_data_offset,
                             uint32_t* end_data_offset);

    // 结束segment,回写头部
    OLAPStatus finalize_segment(uint32_t* data_offset);

    void sync();

    // 腌制和反腌制方法
    // 腌制要减index的ref, munmap和close fd, 用于存储在session中
    virtual OLAPStatus pickle();
    
    // 反腌制要加index的ref, open和mmap fd, 从session中拿出之后为继续使用做准备
    virtual OLAPStatus unpickle();

    Version version() const {
        return olap_index()->version();
    }
    
    VersionHash version_hash() const {
        return olap_index()->version_hash();
    }
    
    uint32_t num_segments() const {
        return olap_index()->num_segments();
    }
    
private:
    // RowBlock代理,内部类，可以代理指定position的RowBlock，为RowBlock提供类似Iterator服务。
    class RowBlockBroker {
    public:
        RowBlockBroker(OLAPTable* olap_table, Rowset* olap_index, RuntimeState* runtime_state);
        ~RowBlockBroker();

        OLAPStatus init();

        // 根据block position，在文件中定位row_block
        OLAPStatus change_to(const RowBlockPosition& row_block_pos);
        // 释放当前持有的row_block
        OLAPStatus release();

        const RowCursor* first();
        const RowCursor* last();
        const RowCursor* current();

        const RowCursor* get_row(uint32_t row_index);
        const RowCursor* next(bool* end_of_row_block);
        const RowCursor* find_row(const RowCursor& key, bool find_last_key, bool* end_of_row_block);

        void set_end_row(const RowBlockPosition& end_block_position, uint32_t end_row_index) {
            _end_block_position = end_block_position;
            _end_row_index = end_row_index;
        }

        void set_end_row_flag(bool flg) {
            _is_set_end_row = flg;
        }

        const RowBlockPosition& end_block_position() {
            return _end_block_position;
        }

        const uint32_t end_row_index() {
            return _end_row_index;
        }

        bool get_set_end_row_flag() {
            return _is_set_end_row;
        }

        const RowBlockPosition& position() {
            return _row_block_pos;
        }
        
        const uint32_t row_index() {
            return _row_index;
        }

        RowBlock* row_block() {
            return _row_block;
        }

        RowBlock* get_row_block_to_read() {
            _row_block->set_pos(_row_index);
            _row_block->set_limit(_num_rows);
            return _row_block;
        }

        const char* packed_row_block() {
            return _read_buffer + _row_block_header_size;
        }

        const uint32_t packed_row_block_size() {
            return _packed_row_block_size;
        }

        const bool is_end_block() {
            return _is_end_block;
        }

        uint32_t num_rows() const { return _num_rows; }

        Tuple* get_next_tuple();
        
    private:
        OLAPStatus _get_row_block(const RowBlockPosition& row_block_pos);

        FileHandler _file_handler; // 内部持有资源的管理者
        RowBlockPosition _row_block_pos;
        char* _read_buffer; // 包含RowBlockHeader和数据
        uint32_t _packed_row_block_size; // 数据的长度
        uint32_t _row_block_header_size; // Header的长度
        uint32_t _unpacked_len; // 解压后的大小 由于header只有指针，没法写进去
        
        RowBlock* _row_block;
        uint32_t _num_rows;
        uint32_t _row_index; // 记录_row_cursor在当前row_block中的位置
        RowCursor _row_cursor;

        RowBlockPosition _end_block_position;
        uint32_t _end_row_index;
        bool _is_set_end_row;

        OLAPTable* _olap_table;
        Rowset* _olap_index;

        uint64_t _data_read_buf_size;
        bool _is_end_block;
        RuntimeState* _runtime_state;
    };  // end of class RowBlockBroker

    // 存在于磁盘上每个row_block之前的头信息
    struct __attribute__((packed)) RowBlockHeader {
        uint32_t checksum;      // 压缩之前数据的校验和
        uint32_t packed_len;    // 压缩后的长度，目前row block内部用不到
        uint32_t num_rows;      // block内有效数据行数
    };

    struct __attribute__((packed)) RowBlockHeaderV2 {
        uint32_t magic_num;
        uint32_t version;
        uint32_t checksum;      // 压缩之前数据的校验和
        uint32_t packed_len;    // 压缩后的长度，目前row block内部用不到
        uint32_t num_rows;      // block内有效数据行数
        uint32_t unpacked_len;  // 解压缩之后的长度
    };

    static const uint32_t OLAP_DEFAULT_DATA_READ_BUF_SIZE =
        OLAP_DEFAULT_MAX_PACKED_ROW_BLOCK_SIZE + sizeof(RowBlockHeaderV2);

    // 一个Session期间需要保存的信息，避免同一个Session的多个request发起多次定位seek。
    struct SessionStatus {
        RowBlockPosition position;
        uint32_t row_index;
        RowBlockPosition end_block_position;
        uint32_t end_row_index;
        bool is_set_end_row;
    };

    // 供OLAPData写流程所使用的参数
    struct WriteDescriptor {
        WriteDescriptor() : checksum(0), segment(0), packed_buffer(NULL) {};

        FileHandler file_handle;
        FileHeader<OLAPDataHeaderMessage> file_header;
        uint32_t checksum;
        uint32_t segment;
        char* packed_buffer;    // buffer size = OLAP_MAX_PACKED_ROW_BLOCK_SIZE
    };

    void _check_io_error(OLAPStatus res);

    OLAPTable* _olap_table;

    // 当前olapdata是否处于腌制状态
    bool _is_pickled;
    SessionStatus* _session_status;
    // RowBlock管理者，负责管理RowBlock及所需资源，如文件描述符等
    RowBlockBroker* _row_block_broker;
    // for writing
    WriteDescriptor* _write_descriptor;

    // Add the semicolon just for eagle.py!
    DISALLOW_COPY_AND_ASSIGN(OLAPData);
};

class OLAPDataComparator {
public:
    OLAPDataComparator(RowBlockPosition position,
                       OLAPData* olap_data,
                       const Rowset* index,
                       RowCursor* helper_cursor) :
            _start_block_position(position),
            _olap_data(olap_data),
            _index(index),
            _helper_cursor(helper_cursor) {}

    // This class is used as functor. So, destructor do nothing here
    ~OLAPDataComparator() {}

    // less comparator function
    bool operator()(const iterator_offset_t& index, const RowCursor& key) const {
        return _compare(index, key, COMPARATOR_LESS);
    }

    // larger comparator function
    bool operator()(const RowCursor& key, const iterator_offset_t& index) const {
        return _compare(index, key, COMPARATOR_LARGER);
    }

private:
    bool _compare(const iterator_offset_t& index,
                  const RowCursor& key,
                  ComparatorEnum comparator_enum) const {
        OLAPStatus res = OLAP_SUCCESS;
        RowBlockPosition position = _start_block_position;
        if ((res = _index->advance_row_block(index, &position)) != OLAP_SUCCESS) {
            OLAP_LOG_FATAL("fail to advance row block. [res=%d]", res);
            throw ComparatorException();
        }

        RowBlock* block = _olap_data->seek_and_get_row_block(position);
        if (block == NULL) {
            OLAP_LOG_FATAL("fail to seek and get row block.");
            throw ComparatorException();
        }

        // 取block里的最后一条数据与key进行比较，返回小于的结果
        // TODO(hujie01): 比较block暂时不使用过滤条件
        uint32_t row_num = block->row_block_info().row_num;
        block->get_row(row_num - 1, _helper_cursor);

        if (comparator_enum == COMPARATOR_LESS) {
            return _helper_cursor->cmp(key) < 0;
        } else {
            return _helper_cursor->cmp(key) > 0;
        }
    }

    const RowBlockPosition _start_block_position;
    OLAPData* _olap_data;
    const Rowset* _index;
    RowCursor* _helper_cursor;
};

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_OLAP_DATA_H
