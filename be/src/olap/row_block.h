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

#ifndef BDG_PALO_BE_SRC_OLAP_ROW_BLOCK_H
#define BDG_PALO_BE_SRC_OLAP_ROW_BLOCK_H

#include <exception>
#include <iterator>
#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/row_cursor.h"
#include "olap/utils.h"
#include "runtime/vectorized_row_batch.h"

#define CHECK_ROWBLOCK_INIT() \
    if (!_is_inited) {\
        OLAP_LOG_WARNING("fail to use uninited RowBlock.");\
        return OLAP_ERR_NOT_INITED;\
    }

namespace palo {

class ExprContext;

struct RowBlockInfo {
    RowBlockInfo() : checksum(0), row_num(0), unpacked_len(0) {}
    RowBlockInfo(uint32_t value, uint32_t num, uint32_t unpacked_length) :
            checksum(value),
            row_num(num),
            unpacked_len(unpacked_length) {}

    uint32_t checksum;
    uint32_t row_num;       // block最大数据行数
    uint32_t unpacked_len;
    DataFileType data_file_type;
    bool null_supported;
};

// 用于rowblock做行列转换的标尺，用来辅助做行列转换。
// offset是列存模式下某一列的存储起始位置，width是某一列的存储长度
struct GridItem {
    uint32_t offset;
    uint32_t width;
};

struct StringBuffer {
    StringBuffer(): buf_ptr(NULL) {}

    uint32_t string_row_length;
    char* buf_ptr;
};

// 一般由256或512行组成一个RowBlock。
// RowBlock类有如下职责：
// 1. 外界从磁盘上读取未解压数据，用decompress函数传给RowBlock，解压后的数据保存在
// RowBlock的内部buf中；
// 2. 给定row_index，读取内部各field的值
// 3. 给定查询的key，在RowBlock内做二分查找，返回起点的行偏移；
// 4. 向量化的条件过滤下推到RowBlock级别进行，因此增加完成过滤的数据读取借口
class RowBlock {
    // Please keep these classes as 'friend'.  They have to use lots of private fields for
    // faster operation.
    friend class RowBlockChanger;
public:
    RowBlock(const std::vector<FieldInfo>& tablet_schema);

    // 注意回收内部buffer
    ~RowBlock();

    // row_num是RowBlock的最大行数，fields为了初始化各个field的起始位置。
    // 在field都为定长的情况下根据这两个值可以确定RowBlock内部buffer的大小，
    // 目前只考虑定长，因此在函数可以分配内存资源。
    OLAPStatus init(const RowBlockInfo& block_info);
    inline void reset_block() {
        memset(_buf, 0, _buf_len);
    }

    // 将内部buffer的内容压缩并输出
    OLAPStatus compress(char* dest_buffer,
                    size_t dest_len,
                    size_t* written_len,
                    OLAPCompressionType compression_type) const;

    // 将外部buffer中的压缩数据解压到本地的buffer, 如果里面已经有数据了，则覆盖。
    OLAPStatus decompress(const char* src_buffer,
                          size_t src_len,
                          OLAPCompressionType compression_type);

    // 向量化地执行过滤条件
    // columns信息用于VectorizedRowBatch的延迟加载
    OLAPStatus eval_conjuncts(std::vector<ExprContext*> conjuncts);

    OLAPStatus eval_conjuncts(std::vector<ExprContext*> conjuncts,
                              const std::vector<FieldInfo>& query_schema);

    inline OLAPStatus get_row_to_write(uint32_t row_index, 
                                      RowCursor* cursor) const;

    // 根据行偏移量，设置RowCursor的field偏移
    // _is_use_vectorized为true，则从经过向量化条件过滤的VectorizedRowBatch中读取
    // 此时row_index表示VectorizedRowBatch中的行偏移
    // 反之读取原始数据，row_index表示原始数据的行偏移
    inline OLAPStatus get_row_to_read(uint32_t row_index, 
                                      RowCursor* cursor) const;

    inline OLAPStatus get_row_to_read(uint32_t row_index, 
                                      RowCursor* cursor,
                                      bool force_read_raw_data) const;

    // 按照给定的行序号写入一行数据到内部buf中
    OLAPStatus set_row(uint32_t row_index, const RowCursor& cursor);

    // 结束本批次rowblock的写入行为，如果传入的row_num与内部初始化传入的row_num不同，
    // 则自己做紧致化，并修改内部的row_num
    // finalize之后不能再用set_row写入，调用clear可恢复初始状态
    // finalize会计算checksum的值，填入_info.checksum
    OLAPStatus finalize(uint32_t row_num);

    // 根据key的值在RowBlock内部做二分查找，返回第一条对应的row_index，
    // find_last为false，找到的lowerbound，反之对应的upperbound。
    // _is_use_vectorized为true，则在经过向量化条件过滤的VectorizedRowBatch中查找，反之使用原始数据
    OLAPStatus find_row(const RowCursor& key, 
                        bool find_last, 
                        uint32_t* row_index) const;

    OLAPStatus backup();

    OLAPStatus restore();

    const uint32_t row_num() const {
        return _is_use_vectorized ? _vectorized_row_batch->size() : _info.row_num;
    }

    const RowBlockInfo& row_block_info() const {
        return _info;
    }

    const std::vector<FieldInfo>& tablet_schema() const {
        return _tablet_schema;
    }

    size_t buf_len() const {
        return _buf_len;
    }

    char* buf() const {
        return _buf;
    }

    // 这个变量是用来记录这个m_buf内实际使用的字节数
    size_t used_buf_len() const {
        return _used_buf_size;
    }

    size_t allocated_row_num() const {
        return _init_row_num;
    }

    // 重用rowblock之前需调用clear，恢复到init之后的原始状态
    OLAPStatus clear();

    // 分配内存部分单独拿出来
    OLAPStatus _allocate_buffer();

private:
    // 仿函数里，根据iterator的operator*返回的序号获取数据结构的值，
    // 与待比较的值完成比较，less就是小于函数
    class RowBlockComparator {
    public:
        RowBlockComparator(const RowBlock* container, 
                           RowCursor* helper_cursor) :
                _container(container),
                _helper_cursor(helper_cursor) {}

        // 因为是仿函数，所以析构函数不需要delete指针成员
        ~RowBlockComparator() {}
        
        // less comparator
        bool operator()(const iterator_offset_t& index, const RowCursor& key) const {
            return _compare(index, key, COMPARATOR_LESS);
        }
        // larger comparator
        bool operator()(const RowCursor& key, const iterator_offset_t& index) const {
            return _compare(index, key, COMPARATOR_LARGER);
        }

    private:
        bool _compare(const iterator_offset_t& index,
                      const RowCursor& key,
                      ComparatorEnum comparator_enum) const {
            OLAPStatus res = OLAP_SUCCESS;

            res = _container->get_row_to_read(index, _helper_cursor);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to get row to read. [res=%d]", res);
                throw ComparatorException();
            }

            if (comparator_enum == COMPARATOR_LESS) {
                return _helper_cursor->cmp(key) < 0;
            } else {
                return _helper_cursor->cmp(key) > 0;
            }
        }

        const RowBlock* _container;
        RowCursor* _helper_cursor;
    };

    OLAPStatus _load_to_vectorized_row_batch(const std::vector<FieldInfo>& query_schema);

    // rearrange string buffer
    OLAPStatus _rearrange_string_buffer(uint32_t row_num, size_t* output_size);

    // 设置内部行列转换标尺
    void _set_field_offsets();

    bool _check_memory_limit(size_t _buf_len) const;

    bool _is_inited;           // 是否正常完成初始化
    bool _is_use_vectorized;   // 是否执行向量化条件过滤
    size_t _buf_len;           // buffer长度
    size_t _fix_row_len;       // buffer中，定长部分的长度
    size_t _extend_len;
    size_t _used_buf_size;     // 这个变量是用来记录这个m_buf内实际使用的字节数
    size_t _init_buf_len;      // 做完init之后的buf_len
    size_t _grid_items_size;
    uint32_t _init_row_num;    // 做完init之后的row_num
    RowBlockInfo _info;        // 头信息
    const std::vector<FieldInfo>& _tablet_schema;     // 内部保存的schema句柄

    GridItem* _grid_items;     // 替换_field_offset实现
    char* _buf;                // 保存解压后数据的buffer
    char* _string_buf_head_ptr;
    StringBuffer* _string_buf_array;
    //size_t _null_field_num;
    size_t _null_byte_num;
    bool _null_supported;
    DataFileType _data_file_type;

    VectorizedRowBatch* _vectorized_row_batch;

    // 由于内部持有内存资源，所以这里禁止拷贝和赋值
    DISALLOW_COPY_AND_ASSIGN(RowBlock);
};

// 写在头文件中，便于编译器inline
inline OLAPStatus RowBlock::get_row_to_write(uint32_t row_index, RowCursor* cursor) const {
    CHECK_ROWBLOCK_INIT();

    if (row_index >= _info.row_num) {
        OLAP_LOG_WARNING("input row index exceeds row_num in row block info."
                         "[row_index=%u; _info.row_num=%u]",
                         row_index,
                         _info.row_num);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (cursor == NULL) {
        OLAP_LOG_WARNING("input row cursor is NULL pointer.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (cursor->field_count() != _grid_items_size) {
        OLAP_LOG_WARNING("input row cursor is invalid for this row block. "
                         "[input_cursor_field_count=%lu; row_block_field_count=%lu]",
                         cursor->field_count(),
                         _grid_items_size);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    OLAPStatus res = OLAP_SUCCESS;

    for (uint32_t i = 0; i < _grid_items_size; ++i) {
        // 如果是varchar类型，要先做一下偏移，让string被写入到buf后边的非定长部分
        if (_tablet_schema[i].type == OLAP_FIELD_TYPE_VARCHAR 
                || _tablet_schema[i].type == OLAP_FIELD_TYPE_HLL) {
            if (OLAP_DATA_FILE == _data_file_type) {
                if (false == _null_supported) {
                    res = cursor->attach_by_index(i, _string_buf_array[i].buf_ptr +
                                  row_index * _string_buf_array[i].string_row_length, false);
                } else {
                    res = cursor->attach_by_index(i, _string_buf_array[i].buf_ptr +
                                  row_index * _string_buf_array[i].string_row_length, true);
                }
            } else if (COLUMN_ORIENTED_FILE == _data_file_type) {
                res = cursor->attach_by_index(i, _string_buf_array[i].buf_ptr +
                              row_index * _string_buf_array[i].string_row_length, true);
            }
        } else {
            if (OLAP_DATA_FILE == _data_file_type) {
                if (false == _null_supported) {
                    res = cursor->attach_by_index(i, _buf + _grid_items[i].offset +
                                  row_index * _grid_items[i].width, false);
                } else {
                    res = cursor->attach_by_index(i, _buf + _grid_items[i].offset +
                                  row_index * _grid_items[i].width, true);
                }
            } else if (COLUMN_ORIENTED_FILE == _data_file_type) {
                    res = cursor->attach_by_index(i, _buf + _grid_items[i].offset +
                                  row_index * _grid_items[i].width, true);
            }
        }

        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to set cursor offsets. "
                             "[res=%d row_index=%u field_index=%u]",
                             res, row_index, i);
            return res;
        }
    }

    return res;
}

inline OLAPStatus RowBlock::get_row_to_read(uint32_t row_index, 
                                            RowCursor* cursor,
                                            bool force_read_raw_data) const {
    CHECK_ROWBLOCK_INIT();

    if (cursor == NULL) {
        OLAP_LOG_WARNING("input row cursor is NULL pointer.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (cursor->field_count() != _grid_items_size) {
        OLAP_LOG_WARNING("input row cursor is invalid for this row block. "
                         "[input_cursor_field_count=%lu row_block_field_count=%lu]",
                         cursor->field_count(),
                         _grid_items_size);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (!force_read_raw_data && _is_use_vectorized) {
        if (NULL == _vectorized_row_batch) {
            OLAP_LOG_WARNING("fail to get_row_to_read since _vectorized_row_batch is NULL");
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
        if (row_index >= _vectorized_row_batch->size()) {
            OLAP_LOG_WARNING("input row_index exceeds _vectorized_row_batch.size."
                             "[row_index=%u; _vectorized_row_batch.size=%u]",
                             row_index,
                             _vectorized_row_batch->size());
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
        if (_vectorized_row_batch->selected_in_use()) {
            row_index = _vectorized_row_batch->selected()[row_index];
        }
    } else {
        if (row_index >= _info.row_num) {
            OLAP_LOG_WARNING("input row_index exceeds _info.row_num."
                             "[row_index=%u; _info.row_num=%u]",
                             row_index,
                             _info.row_num);
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
    }

    OLAPStatus res = OLAP_SUCCESS;
    VarCharField::OffsetValueType* offset;

    //cursor->attach_null_array(_buf + row_index * _null_byte_num);
    for (uint32_t i = 0; i < _grid_items_size; ++i) {
        if (_tablet_schema[i].type == OLAP_FIELD_TYPE_VARCHAR 
                || _tablet_schema[i].type == OLAP_FIELD_TYPE_HLL) {
            offset = reinterpret_cast<VarCharField::OffsetValueType*>(
                             _buf + _grid_items[i].offset + row_index * _grid_items[i].width);
            if (OLAP_DATA_FILE == _data_file_type) {
                if (false == _null_supported) {
                    res = cursor->attach_by_index(i, _buf + (*offset), false);
                } else {
                    res = cursor->attach_by_index(i, _buf + (*offset), true);
                }
            } else if (COLUMN_ORIENTED_FILE == _data_file_type) {
                    res = cursor->attach_by_index(i, _buf + (*offset), true);
            }
        } else {
            if (OLAP_DATA_FILE == _data_file_type) {
                if (false == _null_supported) {
                    res = cursor->attach_by_index(i, _buf +
                                  _grid_items[i].offset + row_index * _grid_items[i].width, false);
                } else {
                    res = cursor->attach_by_index(i, _buf +
                                  _grid_items[i].offset + row_index * _grid_items[i].width, true);
                }
            } else if (COLUMN_ORIENTED_FILE == _data_file_type) {
                    res = cursor->attach_by_index(i, _buf +
                                  _grid_items[i].offset + row_index * _grid_items[i].width, true);
            }
        }

        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to set cursor offsets."
                             "[res=%d row_index=%u field_index=%u]",
                             res, row_index, i);
            return res;
        }
    }
    return res;
}

// 此函数保持和以前一样
inline OLAPStatus RowBlock::get_row_to_read(uint32_t row_index, 
                                            RowCursor* cursor) const {
    return get_row_to_read(row_index, cursor, false);
}

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_ROW_BLOCK_H
