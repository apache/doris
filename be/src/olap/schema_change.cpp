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

#include "olap/schema_change.h"

#include <pthread.h>
#include <signal.h>

#include <algorithm>
#include <vector>

#include "olap/field.h"
#include "olap/i_data.h"
#include "olap/merger.h"
#include "olap/olap_data.h"
#include "olap/olap_engine.h"
#include "olap/olap_table.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/writer.h"
#include "common/resource_tls.h"
#include "agent/cgroups_mgr.h"


using std::deque;
using std::list;
using std::nothrow;
using std::pair;
using std::string;
using std::stringstream;
using std::vector;

namespace palo {

bool version_entity_sorter(const VersionEntity& a, const VersionEntity& b) {
    if (a.version.first != b.version.first) {
        return a.version.first < b.version.first;
    } else {
        return a.version.second < b.version.second;
    }
}

RowBlockChanger::RowBlockChanger(const std::vector<FieldInfo> &tablet_schema,
                                 const SmartOLAPTable &ref_olap_table) {
    _schema_mapping.resize(tablet_schema.size());
}

RowBlockChanger::RowBlockChanger(const vector<FieldInfo>& tablet_schema,
                                 const SmartOLAPTable& ref_olap_table,
                                 const DeleteHandler& delete_handler) {
    _schema_mapping.resize(tablet_schema.size());
    _delete_handler = delete_handler;
}

RowBlockChanger::~RowBlockChanger() {
    SchemaMapping::iterator it = _schema_mapping.begin();
    for (; it != _schema_mapping.end(); ++it) {
        SAFE_DELETE(it->default_value);
    }
    _schema_mapping.clear();

    _delete_handler.finalize();
}

ColumnMapping* RowBlockChanger::get_mutable_column_mapping(size_t column_index) {
    if (column_index >= _schema_mapping.size()) {
        return NULL;
    }

    return &(_schema_mapping[column_index]);
}

#define TYPE_REINTERPRET_CAST(from_type, to_type) \
{ \
    char* ref_pos = ref_block._buf + ref_block._grid_items[ref_column].offset; \
    char* new_pos = mutable_block->_buf + mutable_block->_grid_items[i].offset; \
    for (size_t row = 0, row_num = ref_block.row_block_info().row_num; \
            row < row_num; ++row) { \
        if (is_data_left_vec[row] != 0) { \
            from_type* ref_offset = NULL;\
            if (OLAP_DATA_FILE == ref_block._data_file_type) {\
                if (false == ref_block._null_supported) {\
                    ref_offset = reinterpret_cast<from_type *>( \
                        ref_pos + row * ref_block._grid_items[ref_column].width); \
                } else {\
                    *new_pos = *(ref_pos + row * ref_block._grid_items[ref_column].width); \
                    ref_offset = reinterpret_cast<from_type *>( \
                        ref_pos + row * ref_block._grid_items[ref_column].width + 1); \
                }\
            } else if (COLUMN_ORIENTED_FILE == ref_block._data_file_type) {\
                *new_pos = *(ref_pos + row * ref_block._grid_items[ref_column].width); \
                ref_offset = reinterpret_cast<from_type *>( \
                    ref_pos + row * ref_block._grid_items[ref_column].width + 1); \
            }\
            to_type *new_offset = reinterpret_cast<to_type *>(new_pos + 1);\
            *(new_offset) = static_cast<to_type>(*ref_offset); \
            new_pos += mutable_block->_grid_items[i].width;\
        } \
    } \
    break; \
}

#define LARGEINT_REINTERPRET_CAST(from_type, to_type) \
{ \
    char* ref_pos = ref_block._buf + ref_block._grid_items[ref_column].offset; \
    char* new_pos = mutable_block->_buf + mutable_block->_grid_items[i].offset; \
    for (size_t row = 0, row_num = ref_block.row_block_info().row_num; \
         row < row_num; ++row) { \
        if (is_data_left_vec[row] != 0) { \
            from_type *ref_offset = NULL;\
            if (OLAP_DATA_FILE == ref_block._data_file_type) {\
                if (false == ref_block._null_supported) {\
                    ref_offset = reinterpret_cast<from_type *>( \
                        ref_pos + row * ref_block._grid_items[ref_column].width); \
                } else { \
                    *new_pos = *(ref_pos + row * ref_block._grid_items[ref_column].width); \
                    ref_offset = reinterpret_cast<from_type *>( \
                        ref_pos + row * ref_block._grid_items[ref_column].width + 1); \
                } \
            } else if (COLUMN_ORIENTED_FILE == ref_block._data_file_type) {\
                *new_pos = *(ref_pos + row * ref_block._grid_items[ref_column].width); \
                ref_offset = reinterpret_cast<from_type *>( \
                    ref_pos + row * ref_block._grid_items[ref_column].width + 1); \
            } \
            to_type *new_offset = reinterpret_cast<to_type *>(new_pos + 1);\
            to_type temp = static_cast<to_type>(*ref_offset); \
            *(new_offset) = temp; \
            new_pos += mutable_block->_grid_items[i].width;\
        } \
    } \
    break; \
}

#define CONVERT_FROM_TYPE(from_type) \
{ \
    switch (mutable_block->_tablet_schema[i].type) {\
    case OLAP_FIELD_TYPE_TINYINT: \
        TYPE_REINTERPRET_CAST(from_type, int8_t); \
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT: \
        TYPE_REINTERPRET_CAST(from_type, uint8_t); \
    case OLAP_FIELD_TYPE_SMALLINT: \
        TYPE_REINTERPRET_CAST(from_type, int16_t); \
    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT: \
        TYPE_REINTERPRET_CAST(from_type, uint16_t); \
    case OLAP_FIELD_TYPE_INT: \
        TYPE_REINTERPRET_CAST(from_type, int32_t); \
    case OLAP_FIELD_TYPE_UNSIGNED_INT: \
        TYPE_REINTERPRET_CAST(from_type, uint32_t); \
    case OLAP_FIELD_TYPE_BIGINT: \
        TYPE_REINTERPRET_CAST(from_type, int64_t); \
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT: \
        TYPE_REINTERPRET_CAST(from_type, uint64_t); \
    case OLAP_FIELD_TYPE_LARGEINT: \
        LARGEINT_REINTERPRET_CAST(from_type, int128_t); \
    case OLAP_FIELD_TYPE_DOUBLE: \
        TYPE_REINTERPRET_CAST(from_type, double); \
    default: \
        OLAP_LOG_WARNING("the column type which was altered to was" \
                         " unsupported. [origin_type=%d alter_type=%d]", \
                         ref_block._tablet_schema[ref_column].type, \
                         mutable_block->_tablet_schema[i].type); \
        return false; \
    } \
    break; \
}

#define ASSIGN_DEFAULT_VALUE(length) \
    case length: { \
        for (size_t row = 0; row < ref_block.row_block_info().row_num; ++row) { \
            memcpy(buf, _schema_mapping[i].default_value->buf(), length); \
            buf += length; \
        } \
        break; \
    }

bool RowBlockChanger::change_row_block(
        const DataFileType df_type,
        const RowBlock& ref_block,
        int32_t data_version,
        RowBlock* mutable_block,
        uint64_t* filted_rows) const {
    if (mutable_block == NULL || !mutable_block->_is_inited) {
        OLAP_LOG_FATAL("mutable block is uninitialized.");
        return false;
    } else if (!ref_block._is_inited) {
        OLAP_LOG_WARNING("the row block referenced is uninited.");
        return false;
    } else if (mutable_block->_tablet_schema.size() != _schema_mapping.size()) {
        OLAP_LOG_WARNING("mutable block does not match with schema mapping rules. "
                         "[block_schema_size=%ld, mapping_schema_size=%ld]",
                         mutable_block->_tablet_schema.size(),
                         _schema_mapping.size());
        return false;
    }

    if (mutable_block->allocated_row_num() < ref_block.row_block_info().row_num) {
        OLAP_LOG_WARNING("mutable block is not large enough for storing the changed block. "
                         "[mutable_block_size=%ld, ref_block_size=%u]",
                         mutable_block->allocated_row_num(),
                         ref_block.row_block_info().row_num);
        return false;
    }

    if (!ref_block._is_inited) {
        OLAP_LOG_WARNING("the row block referenced is uninited.");
        return false;
    }

    mutable_block->clear();

    RowCursor write_helper;
    if (write_helper.init(mutable_block->_tablet_schema) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init rowcursor.");
        return false;
    }

    RowCursor read_helper;
    if (read_helper.init(ref_block._tablet_schema) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init rowcursor.");
        return false;
    }

    // a.1 先判断数据是否需要过滤，最终只有标记为1的才是留下需要的
    //   对于没有filter的来说，相当于全部设置为1后留下
    const uint32_t row_num = ref_block.row_block_info().row_num;
    // (0表示过滤掉不要，1表示要,过程中2表示此row要切后续不需要再比较其他列)
    vector<int8_t> is_data_left_vec(row_num, 1);

    // 一行一行地进行比较
    for (size_t row_index = 0; row_index < row_num; ++row_index) {
        if (ref_block.get_row_to_read(row_index, &read_helper) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to get row to read");
            return false;
        }

        // filter data according to delete conditions specified in DeleteData command
        if (df_type == OLAP_DATA_FILE) {
            if (is_data_left_vec[row_index] == 1) {
                if (_delete_handler.is_filter_data(data_version, read_helper)) {
                    is_data_left_vec[row_index] = 0;
                }
            }
        }
    }

    // a.2 计算留下的row num
    uint32_t new_row_num = 0;
    for (uint32_t i = 0; i < row_num; ++i) {
        if (is_data_left_vec[i] != 0) {
            ++new_row_num;
        }
    }
    *filted_rows = row_num - new_row_num;

    const bool need_filter_data = (new_row_num != row_num);
    const bool filter_all = (new_row_num == 0);

    // b. 根据前面的过滤信息，只对还标记为1的处理
    for (size_t i = 0, len = mutable_block->tablet_schema().size(); !filter_all && i < len; ++i) {
        int32_t ref_column = _schema_mapping[i].ref_column;

        if (_schema_mapping[i].ref_column >= 0) {
            // new column will be assigned as referenced column
            // check if the type of new column is equal to the older's.
            if (mutable_block->tablet_schema()[i].type 
                    == ref_block.tablet_schema()[ref_column].type) {
                // 效率低下，也可以直接计算变长域拷贝，但仍然会破坏封装
                for (size_t row_index = 0, new_row_index = 0; 
                        row_index < ref_block.row_block_info().row_num; ++row_index) {
                    // 不需要的row，每次处理到这个row时就跳过
                    if (need_filter_data && is_data_left_vec[row_index] == 0) {
                        continue;
                    }

                    // 指定新的要写入的row index（不同于读的row_index）
                    if (mutable_block->get_row_to_write(new_row_index++, &write_helper)
                            != OLAP_SUCCESS) {
                        OLAP_LOG_WARNING("fail to get row to write");
                        return false;
                    }

                    if (ref_block.get_row_to_read(row_index, &read_helper) != OLAP_SUCCESS) {
                        OLAP_LOG_WARNING("fail to get row to read");
                        return false;
                    }

                    if (true == read_helper.is_null(ref_column)) {
                        write_helper.set_null(i);
                    } else {
                        // 要写入的
                        const Field* field_to_read = read_helper.get_field_by_index(ref_column);
                        if (NULL == field_to_read) {
                            OLAP_LOG_WARNING("faile to get ref field.[index=%d]", ref_column);
                            return false;
                        }

                        write_helper.set_not_null(i);
                        if (write_helper.read_by_index(i, field_to_read->buf()) != OLAP_SUCCESS) {
                            OLAP_LOG_WARNING("faile to read field");
                            return false;
                        }
                    }
                }

                // 从ref_column 写入 i列。
            } else if (mutable_block->tablet_schema()[i].type == OLAP_FIELD_TYPE_VARCHAR
                       && ref_block.tablet_schema()[ref_column].type == OLAP_FIELD_TYPE_CHAR) {
                // 效率低下，也可以直接计算变长域拷贝，但仍然会破坏封装
                for (size_t row_index = 0, new_row_index = 0;
                        row_index < ref_block.row_block_info().row_num; ++row_index) {
                    // 不需要的row，每次处理到这个row时就跳过
                    if (need_filter_data && is_data_left_vec[row_index] == 0) {
                        continue;
                    }

                    // 指定新的要写入的row index（不同于读的row_index）
                    if (mutable_block->get_row_to_write(new_row_index++, &write_helper)
                            != OLAP_SUCCESS) {
                        OLAP_LOG_WARNING("fail to get row to write");
                        return false;
                    }

                    if (ref_block.get_row_to_read(row_index, &read_helper) != OLAP_SUCCESS) {
                        OLAP_LOG_WARNING("fail to get row to read");
                        return false;
                    }

                    if (true == read_helper.is_null(ref_column)) {
                        write_helper.set_null(i);
                    } else {
                        // 要写入的
                        const Field* field_to_read = read_helper.get_field_by_index(ref_column);
                        if (NULL == field_to_read) {
                            OLAP_LOG_WARNING("faile to get ref field.[index=%d]", ref_column);
                            return false;
                        }

                        write_helper.set_not_null(i);
                        int p = ref_block.tablet_schema()[ref_column].length - 1;
                        char* buf = field_to_read->buf();
                        while (p >= 0 && buf[p] == '\0') {
                            p--;
                        }
                        if (write_helper.read_by_index(
                                i, field_to_read->buf(), p + 1) != OLAP_SUCCESS) {
                            OLAP_LOG_WARNING("faile to read field");
                            return false;
                        }
                    }
                }

                // 从ref_column 写入 i列。
            } else {
                // copy and alter the field
                // 此处可以暂时不动，新类型暂时不涉及类型转换
                switch (ref_block._tablet_schema[ref_column].type) {
                case OLAP_FIELD_TYPE_TINYINT:
                    CONVERT_FROM_TYPE(int8_t);
                case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
                    CONVERT_FROM_TYPE(uint8_t);
                case OLAP_FIELD_TYPE_SMALLINT:
                    CONVERT_FROM_TYPE(int16_t);
                case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
                    CONVERT_FROM_TYPE(uint16_t);
                case OLAP_FIELD_TYPE_INT:
                    CONVERT_FROM_TYPE(int32_t);
                case OLAP_FIELD_TYPE_UNSIGNED_INT:
                    CONVERT_FROM_TYPE(uint32_t);
                case OLAP_FIELD_TYPE_BIGINT:
                    CONVERT_FROM_TYPE(int64_t);
                case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
                    CONVERT_FROM_TYPE(uint64_t);
                default:
                    OLAP_LOG_WARNING("the column type which was altered from was"
                                     " unsupported. [from_type=%d]",
                                     ref_block._tablet_schema[ref_column].type);
                    return false;
                }

                if (mutable_block->tablet_schema()[i].type <
                        ref_block.tablet_schema()[ref_column].type) {
                    OLAP_LOG_DEBUG("type degraded while altering column. "
                                   "[column='%s' origin_type=%d alter_type=%d]",
                                   mutable_block->tablet_schema()[i].name.c_str(),
                                   ref_block._tablet_schema[ref_column].type,
                                   mutable_block->_tablet_schema[i].type);
                }
            }
        } else {
            // 新增列，写入默认值
            for (size_t row_index = 0, new_row_index = 0;
                    row_index < ref_block.row_block_info().row_num; ++row_index) {
                // 不需要的row，每次处理到这个row时就跳过
                if (need_filter_data && is_data_left_vec[row_index] == 0) {
                    continue;
                }

                if (mutable_block->get_row_to_write(new_row_index++, &write_helper)
                        != OLAP_SUCCESS) {
                    OLAP_LOG_WARNING("fail to get row to write");
                    return false;
                }

                if (_schema_mapping[i].default_value->is_null()) {
                    write_helper.set_null(i);
                } else {
                    write_helper.set_not_null(i);
                    write_helper.read_by_index(i, _schema_mapping[i].default_value->buf());
                }
            }
        }
    }

    // NOTE 当前mutable_block的内存row_num还是和ref一样多
    //  （其实在init时就可以重新init成少的，filter留下的new_row_num）
    // 在split_table时，可能会出现因为过滤导致没有数据
    mutable_block->finalize(new_row_num);
    // OLAP_LOG_DEBUG("finilize one block with row_num=%u. ", new_row_num);
    return true;
}

#undef CONVERT_FROM_TYPE
#undef TYPE_REINTERPRET_CAST
#undef ASSIGN_DEFAULT_VALUE

RowBlockSorter::RowBlockSorter(RowBlockAllocator* row_block_allocator) :
        _row_block_allocator(row_block_allocator),
        _swap_row_block(NULL) {}

RowBlockSorter::~RowBlockSorter() {
    if (_swap_row_block) {
        _row_block_allocator->release(_swap_row_block);
        _swap_row_block = NULL;
    }
}

bool RowBlockSorter::sort(RowBlock** row_block) {
    uint32_t row_num = (*row_block)->row_block_info().row_num;
    DataFileType data_file_type = (*row_block)->row_block_info().data_file_type;
    bool null_supported = (*row_block)->row_block_info().null_supported;

    if (_swap_row_block == NULL || _swap_row_block->allocated_row_num() < row_num) {
        if (_swap_row_block != NULL) {
            _row_block_allocator->release(_swap_row_block);
            _swap_row_block = NULL;
        }

        if (_row_block_allocator->allocate(&_swap_row_block, row_num, 
                                    data_file_type, null_supported) != OLAP_SUCCESS
                || _swap_row_block == NULL) {
            OLAP_LOG_WARNING("fail to allocate memory.");
            return false;
        }
    }

    RowBlock* temp = NULL;
    vector<RowCursor*> row_cursor_list((*row_block)->row_block_info().row_num, NULL);

    // create an list of row cursor as long as the number of rows in data block.
    for (size_t i = 0; i < (*row_block)->row_block_info().row_num; ++i) {
        if ((row_cursor_list[i] = new(nothrow) RowCursor()) == NULL) {
            OLAP_LOG_WARNING("failed to malloc RowCursor. [size=%ld]", sizeof(RowCursor));
            goto SORT_ERR_EXIT;
        }

        if (row_cursor_list[i]->init((*row_block)->tablet_schema()) != OLAP_SUCCESS) {
            goto SORT_ERR_EXIT;
        }

        if ((*row_block)->get_row_to_read(i, row_cursor_list[i]) != OLAP_SUCCESS) {
            goto SORT_ERR_EXIT;
        }
    }

    // Must use 'std::' because this class has a function whose name is sort too
    std::stable_sort(row_cursor_list.begin(), row_cursor_list.end(), _row_cursor_comparator);

    // copy the results sorted to temp row block.
    _swap_row_block->clear();

    for (size_t i = 0; i < row_cursor_list.size(); ++i) {
        if (_swap_row_block->set_row(i, (*row_cursor_list[i])) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("failed to set row for row block. [row=%ld]", i);
            goto SORT_ERR_EXIT;
        }
    }

    _swap_row_block->finalize(row_cursor_list.size());

    for (size_t i = 0; i < (*row_block)->row_block_info().row_num; ++i) {
        SAFE_DELETE(row_cursor_list[i]);
    }

    // swap the row block for reducing memory allocating.
    temp = *row_block;
    *row_block = _swap_row_block;
    _swap_row_block = temp;

    return true;

SORT_ERR_EXIT:
    for (size_t i = 0; i < (*row_block)->row_block_info().row_num; ++i) {
        SAFE_DELETE(row_cursor_list[i]);
    }

    return false;
}

RowBlockAllocator::RowBlockAllocator(const vector<FieldInfo>& tablet_schema,
                                     size_t memory_limitation) :
        _tablet_schema(tablet_schema),
        _memory_allocated(0),
        _memory_limitation(memory_limitation) {
    _row_len = 0;
    _row_len += tablet_schema.size();
    for (vector<FieldInfo>::const_iterator it = tablet_schema.begin();
            it != tablet_schema.end(); ++it) {
        _row_len += (*it).length;
    }

    OLAP_LOG_DEBUG("RowBlockAllocator(). [row_len=%ld]", _row_len);
}

RowBlockAllocator::~RowBlockAllocator() {
    if (_memory_allocated != 0) {
        OLAP_LOG_WARNING("memory lost in RowBlockAllocator. [memory_size=%ld]", _memory_allocated);
    }
}

OLAPStatus RowBlockAllocator::allocate(RowBlock** row_block,
                                       size_t num_rows,
                                       DataFileType data_file_type,
                                       bool null_supported) {
    size_t row_block_size = _row_len * num_rows;

    if (_memory_limitation > 0
            && _memory_allocated + row_block_size > _memory_limitation) {
        OLAP_LOG_DEBUG("RowBlockAllocator::alocate() memory exceeded. [m_memory_allocated=%ld]",
                       _memory_allocated);
        *row_block = NULL;
        return OLAP_SUCCESS;
    }

    // TODO(lijiao) : 为什么舍弃原有的m_row_block_buffer
    *row_block = new(nothrow) RowBlock(_tablet_schema);

    if (*row_block == NULL) {
        OLAP_LOG_WARNING("failed to malloc RowBlock. [size=%ld]", sizeof(RowBlock));
        return OLAP_ERR_MALLOC_ERROR;
    }

    RowBlockInfo row_block_info(0U, num_rows, 0);
    row_block_info.data_file_type = data_file_type;
    row_block_info.null_supported = null_supported;
    OLAPStatus res = OLAP_SUCCESS;

    if ((res = (*row_block)->init(row_block_info)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to init row block.");
        SAFE_DELETE(*row_block);
        return res;
    }

    _memory_allocated += row_block_size;
    OLAP_LOG_DEBUG("RowBlockAllocator::allocate() "
                   "[this=%p num_rows=%ld m_memory_allocated=%ld p=%p]",
                   this,
                   num_rows,
                   _memory_allocated,
                   *row_block);
    return res;
}

void RowBlockAllocator::release(RowBlock* row_block) {
    if (row_block == NULL) {
        OLAP_LOG_WARNING("null row block released.");
        return;
    }

    _memory_allocated -= row_block->allocated_row_num() * _row_len;

    OLAP_LOG_DEBUG("RowBlockAllocator::release() "
                   "[this=%p num_rows=%ld m_memory_allocated=%ld p=%p]",
                   this,
                   row_block->allocated_row_num(),
                   _memory_allocated,
                   row_block);
    delete row_block;
}

RowBlockMerger::RowBlockMerger(SmartOLAPTable olap_table) : _olap_table(olap_table) {}

RowBlockMerger::~RowBlockMerger() {}

bool RowBlockMerger::merge(
        const vector<RowBlock*>& row_block_arr,
        IWriter* writer,
        uint64_t* merged_rows) {
    uint64_t tmp_merged_rows = 0;
    RowCursor row_cursor;
    if (row_cursor.init(_olap_table->tablet_schema()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init row cursor.");
        goto MERGE_ERR;
    }

    _make_heap(row_block_arr);

    while (_heap.size() > 0) {
        if (writer->attached_by(&row_cursor) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("writer error.");
            goto MERGE_ERR;
        }

        row_cursor.copy(*(_heap.top().row_cursor));

        if (!_pop_heap()) {
            goto MERGE_ERR;
        }

        if (KeysType::DUP_KEYS == _olap_table->keys_type()) {
            writer->next(row_cursor);
            continue;
        }

        while (!_heap.empty() && row_cursor.full_key_cmp(*(_heap.top().row_cursor)) == 0) {
            row_cursor.aggregate(*(_heap.top().row_cursor));
            ++tmp_merged_rows;
            if (!_pop_heap()) {
                goto MERGE_ERR;
            }
        }
        row_cursor.finalize_one_merge();
        writer->next(row_cursor);
    }
    if (writer->finalize() != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to finalizing writer.");
        goto MERGE_ERR;
    }

    *merged_rows = tmp_merged_rows;
    return true;

MERGE_ERR:
    while (_heap.size() > 0) {
        MergeElement element = _heap.top();
        _heap.pop();
        SAFE_DELETE(element.row_cursor);
    }

    return false;
}

bool RowBlockMerger::_make_heap(const vector<RowBlock*>& row_block_arr) {
    for (vector<RowBlock*>::const_iterator it = row_block_arr.begin();
            it != row_block_arr.end(); ++it) {
        MergeElement element;
        element.row_block = *it;
        element.row_block_index = 0;
        element.row_cursor = new(nothrow) RowCursor();

        if (element.row_cursor == NULL) {
            OLAP_LOG_FATAL("failed to malloc RowCursor. [size=%ld]", sizeof(RowCursor));
            return false;
        }

        if (element.row_cursor->init(element.row_block->tablet_schema()) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("failed to init row cursor.");
            SAFE_DELETE(element.row_cursor);
            return false;
        }

        if (OLAP_SUCCESS != element.row_block->get_row_to_read(element.row_block_index,
                                                               element.row_cursor)) {
            OLAP_LOG_WARNING("failed to get row from row block. [row_block_index=%d]",
                             element.row_block_index);
            SAFE_DELETE(element.row_cursor);
            return false;
        }

        _heap.push(element);
    }

    return true;
}

bool RowBlockMerger::_pop_heap() {
    MergeElement element = _heap.top();
    _heap.pop();

    if (++element.row_block_index >= element.row_block->row_block_info().row_num) {
        SAFE_DELETE(element.row_cursor);
        return true;
    }

    if (OLAP_SUCCESS != element.row_block->get_row_to_read(element.row_block_index,
                                                           element.row_cursor)) {
        OLAP_LOG_WARNING("failed to get row from row block. [row_block_index=%d]",
                         element.row_block_index);
        return false;
    }

    _heap.push(element);
    return true;
}

LinkedSchemaChange::LinkedSchemaChange(
        SmartOLAPTable base_olap_table, SmartOLAPTable new_olap_table) :
        _base_olap_table(base_olap_table),
        _new_olap_table(new_olap_table) {}

SchemaChangeDirectly::SchemaChangeDirectly(
        SmartOLAPTable olap_table,
        const RowBlockChanger& row_block_changer) :
        _olap_table(olap_table),
        _row_block_changer(row_block_changer),
        _row_block_allocator(NULL),
        _src_cursor(NULL),
        _dst_cursor(NULL) {}

SchemaChangeDirectly::~SchemaChangeDirectly() {
    OLAP_LOG_DEBUG("~SchemaChangeDirectly()");
    SAFE_DELETE(_row_block_allocator);
    SAFE_DELETE(_src_cursor);
    SAFE_DELETE(_dst_cursor);
}

bool SchemaChangeDirectly::_write_row_block(IWriter* writer, RowBlock* row_block) {
    for (uint32_t i = 0; i < row_block->row_block_info().row_num; i++) {
        if (OLAP_SUCCESS != writer->attached_by(_dst_cursor)) {
            OLAP_LOG_WARNING("fail to attach writer");
            return false;
        }

        if (OLAP_SUCCESS != row_block->get_row_to_read(i, _src_cursor)) {
            OLAP_LOG_WARNING("fail to get row from row block.");
            return false;
        }

        _dst_cursor->copy(*_src_cursor);
        writer->next(*_dst_cursor);
    }

    return true;
}

bool LinkedSchemaChange::process(IData* olap_data, OLAPIndex* new_olap_index) {
    for (size_t i = 0; i < olap_data->olap_index()->num_segments(); ++i) {
        string index_path = _new_olap_table->construct_index_file_path(
                new_olap_index->version(), new_olap_index->version_hash(), i);
        string base_table_index_path = _base_olap_table->construct_index_file_path(
                new_olap_index->version(), new_olap_index->version_hash(), i);
        if (link(base_table_index_path.c_str(), index_path.c_str()) == 0) {
            OLAP_LOG_DEBUG("success to create hard link. [from_path=%s to_path=%s]",
                           base_table_index_path.c_str(), index_path.c_str());
        } else {
            OLAP_LOG_WARNING("fail to create hard link. [from_path=%s to_path=%s]",
                             base_table_index_path.c_str(), index_path.c_str());
            return false;
        }

        string data_path = _new_olap_table->construct_data_file_path(
                new_olap_index->version(), new_olap_index->version_hash(), i);
        string base_table_data_path = _base_olap_table->construct_data_file_path(
                new_olap_index->version(), new_olap_index->version_hash(), i);
        if (link(base_table_data_path.c_str(), data_path.c_str()) == 0) {
            OLAP_LOG_DEBUG("success to create hard link. [from_path=%s to_path=%s]",
                           base_table_data_path.c_str(), data_path.c_str());
        } else {
            OLAP_LOG_WARNING("fail to create hard link. [from_path=%s to_path=%s]",
                             base_table_data_path.c_str(), data_path.c_str());
            return false;
        }
    }

    new_olap_index->set_num_segments(olap_data->olap_index()->num_segments());

    if (OLAP_SUCCESS != new_olap_index->load()) {
        OLAP_LOG_WARNING("fail to reload index. [table='%s' version='%d-%d']",
                         _new_olap_table->full_name().c_str(),
                         new_olap_index->version().first,
                         new_olap_index->version().second);
        return false;
    }

    return true;
}

bool SchemaChangeDirectly::process(IData* olap_data, OLAPIndex* new_olap_index) {
    bool result = true;
    DataFileType data_file_type = new_olap_index->table()->data_file_type();
    bool null_supported = true;

    if (NULL == _row_block_allocator) {
        if (NULL == (_row_block_allocator =
                         new(nothrow) RowBlockAllocator(_olap_table->tablet_schema(), 0))) {
            OLAP_LOG_FATAL("failed to malloc RowBlockAllocator. [size=%ld]",
                           sizeof(RowBlockAllocator));
            return false;
        }
    }

    if (NULL == _src_cursor) {
        _src_cursor = new(nothrow) RowCursor();
        if (NULL == _src_cursor) {
            OLAP_LOG_WARNING("fail to allocate row cursor.");
            return false;
        }

        if (OLAP_SUCCESS != _src_cursor->init(_olap_table->tablet_schema())) {
            OLAP_LOG_WARNING("fail to init row cursor.");
            return false;
        }
    }

    if (NULL == _dst_cursor) {
        _dst_cursor = new(nothrow) RowCursor();
        if (NULL == _dst_cursor) {
            OLAP_LOG_WARNING("fail to allocate row cursor.");
            return false;
        }

        if (OLAP_SUCCESS != _dst_cursor->init(_olap_table->tablet_schema())) {
            OLAP_LOG_WARNING("fail to init row cursor.");
            return false;
        }
    }

    RowBlock* ref_row_block = NULL;
    OLAPStatus res = olap_data->get_first_row_block(&ref_row_block);
    if (OLAP_SUCCESS != res) {
        // Create empty version when olap_data is empty
        if (olap_data->eof()) {
            OLAP_LOG_DEBUG("src delta is empty, create an empty version instead.");
            res = create_init_version(
                    new_olap_index->table()->tablet_id(),
                    new_olap_index->table()->schema_hash(),
                    new_olap_index->version(),
                    new_olap_index->version_hash(),
                    new_olap_index);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to create init version. [res=%d]", res);
                result = false;
            }
        } else {
            OLAP_LOG_WARNING("failed to get first row block.");
            result = false;
        }

        return result;
    }

    OLAP_LOG_DEBUG("init writer. [table='%s' block_row_size=%lu]",
                   _olap_table->full_name().c_str(),
                   _olap_table->num_rows_per_row_block());

    RowBlock* new_row_block = NULL;
    IWriter* writer = IWriter::create(_olap_table, new_olap_index, false);
    if (NULL == writer) {
        OLAP_LOG_WARNING("failed to create writer.");
        result = false;
        goto DIRECTLY_PROCESS_ERR;
    }

    if (OLAP_SUCCESS != writer->init()) {
        OLAP_LOG_WARNING("failed to init writer.");
        result = false;
        goto DIRECTLY_PROCESS_ERR;
    }

    // Reset filted_rows and merged_rows statistic
    reset_merged_rows();
    reset_filted_rows();

    while (NULL != ref_row_block) {
        // 注意这里强制分配和旧块等大的块(小了可能会存不下)
        if (NULL == new_row_block
                || new_row_block->allocated_row_num() < ref_row_block->row_block_info().row_num) {
            if (NULL != new_row_block) {
                _row_block_allocator->release(new_row_block);
                new_row_block = NULL;
            }

            if (OLAP_SUCCESS != _row_block_allocator->allocate(
                                        &new_row_block,
                                        ref_row_block->row_block_info().row_num,
                                        data_file_type, null_supported)) {
                OLAP_LOG_WARNING("failed to allocate RowBlock.");
                result = false;
                goto DIRECTLY_PROCESS_ERR;
            }
        } else {
            new_row_block->clear();
        }

        // 将ref改为new。这一步按道理来说确实需要等大的块，但理论上和writer无关。
        uint64_t filted_rows = 0;
        if (!_row_block_changer.change_row_block(olap_data->data_file_type(),
                                                 *ref_row_block,
                                                 olap_data->version().second,
                                                 new_row_block,
                                                 &filted_rows)) {
            OLAP_LOG_WARNING("failed to change data in row block.");
            result = false;
            goto DIRECTLY_PROCESS_ERR;
        }
        add_filted_rows(filted_rows);

        if (!_write_row_block(writer, new_row_block)) {
            OLAP_LOG_WARNING("failed to write row block.");
            result = false;
            goto DIRECTLY_PROCESS_ERR;
        }

        olap_data->get_next_row_block(&ref_row_block);

    }

    if (OLAP_SUCCESS != writer->finalize()) {
        result = false;
        goto DIRECTLY_PROCESS_ERR;
    }

    if (OLAP_SUCCESS != new_olap_index->load()) {
        OLAP_LOG_WARNING("fail to reload index. [table='%s' version='%d-%d']",
                         _olap_table->full_name().c_str(),
                         new_olap_index->version().first,
                         new_olap_index->version().second);
        result = false;
        goto DIRECTLY_PROCESS_ERR;
    }

    if (olap_data->data_file_type() == COLUMN_ORIENTED_FILE) {
        reset_filted_rows();
        add_filted_rows(olap_data->get_filted_rows());
    }

    // Check row num changes
    if (config::row_nums_check) {
        if (olap_data->olap_index()->num_rows()
            != new_olap_index->num_rows() + merged_rows() + filted_rows()) {
            OLAP_LOG_FATAL("fail to check row num! "
                           "[source_rows=%lu merged_rows=%lu filted_rows=%lu new_index_rows=%lu]",
                           olap_data->olap_index()->num_rows(),
                           merged_rows(), filted_rows(), new_olap_index->num_rows());
            result = false;
        }
    } else {
        OLAP_LOG_INFO("all row nums. "
                      "[source_rows=%lu merged_rows=%lu filted_rows=%lu new_index_rows=%lu]",
                      olap_data->olap_index()->num_rows(),
                      merged_rows(), filted_rows(), new_olap_index->num_rows());
    }

DIRECTLY_PROCESS_ERR:
    SAFE_DELETE(writer);
    _row_block_allocator->release(new_row_block);
    return result;
}

SchemaChangeWithSorting::SchemaChangeWithSorting(SmartOLAPTable olap_table,
                                                 const RowBlockChanger& row_block_changer,
                                                 size_t memory_limitation) :
        _olap_table(olap_table),
        _row_block_changer(row_block_changer),
        _memory_limitation(memory_limitation),
        _row_block_allocator(NULL) {
    // 每次SchemaChange做外排的时候，会写一些临时版本（比如999,1000,1001），为避免Cache冲突，临时
    // 版本进行2个处理：
    // 1. 随机值作为VersionHash
    // 2. 版本号取一个BIG NUMBER加上当前正在进行SchemaChange的版本号
    _temp_delta_versions.first = (1 << 28);
    _temp_delta_versions.second = (1 << 28);
    // TODO(zyh): remove the magic number
}

SchemaChangeWithSorting::~SchemaChangeWithSorting() {
    OLAP_LOG_DEBUG("~SchemaChangeWithSorting()");
    SAFE_DELETE(_row_block_allocator);
}

bool SchemaChangeWithSorting::process(IData* olap_data, OLAPIndex* new_olap_index) {
    if (NULL == _row_block_allocator) {
        if (NULL == (_row_block_allocator = new(nothrow) RowBlockAllocator(
                        _olap_table->tablet_schema(), _memory_limitation))) {
            OLAP_LOG_FATAL("failed to malloc RowBlockAllocator. [size=%ld]",
                           sizeof(RowBlockAllocator));
            return false;
        }
    }

    bool result = true;
    DataFileType data_file_type = new_olap_index->table()->data_file_type();
    bool null_supported = true;

    RowBlock* ref_row_block = NULL;
    OLAPStatus res = olap_data->get_first_row_block(&ref_row_block);

    if (OLAP_SUCCESS != res) {
        if (olap_data->eof()) {
            OLAP_LOG_DEBUG("src delta is empty, create an empty version instead.");
            res = create_init_version(
                    new_olap_index->table()->tablet_id(),
                    new_olap_index->table()->schema_hash(),
                    new_olap_index->version(),
                    new_olap_index->version_hash(),
                    new_olap_index);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to create init version. [res=%d]", res);
                result = false;
            }
        } else {
            OLAP_LOG_WARNING("failed to get first row block.");
            result = false;
        }

        return result;
    }

    RowBlockSorter row_block_sorter(_row_block_allocator);

    // for internal sorting
    RowBlock* new_row_block = NULL;
    vector<RowBlock*> row_block_arr;

    // for external sorting
    vector<OLAPIndex*> olap_index_arr;

    _temp_delta_versions.first = _temp_delta_versions.second;

    // Reset filted_rows and merged_rows statistic
    reset_merged_rows();
    reset_filted_rows();

    while (NULL != ref_row_block) {
        if (OLAP_SUCCESS != _row_block_allocator->allocate(
                    &new_row_block, ref_row_block->row_block_info().row_num, 
                    data_file_type, null_supported)) {
            OLAP_LOG_WARNING("failed to allocate RowBlock.");
            result = false;
            goto SORTING_PROCESS_ERR;
        }

        if (NULL == new_row_block) {
            if (row_block_arr.size() < 1) {
                OLAP_LOG_WARNING("Memory limitation is too small for Schema Change. "
                                 "[memory_limitation=%ld]",
                                 _memory_limitation);
                return false;
            }

            // enter here while memory limitation is reached.
            OLAPIndex* olap_index = NULL;

            if (!_internal_sorting(row_block_arr,
                                   Version(_temp_delta_versions.second,
                                           _temp_delta_versions.second),
                                   &olap_index)) {
                OLAP_LOG_WARNING("failed to sorting internally.");
                result = false;
                goto SORTING_PROCESS_ERR;
            }

            olap_index_arr.push_back(olap_index);

            for (vector<RowBlock*>::iterator it = row_block_arr.begin();
                    it != row_block_arr.end(); ++it) {
                _row_block_allocator->release(*it);
            }

            row_block_arr.clear();

            // increase temp version
            ++_temp_delta_versions.second;
            continue;
        }

        uint64_t filted_rows = 0;
        if (!_row_block_changer.change_row_block(
                olap_data->data_file_type(),
                *ref_row_block,
                olap_data->version().second,
                new_row_block,
                &filted_rows)) {
            OLAP_LOG_WARNING("failed to change data in row block.");
            result = false;
            goto SORTING_PROCESS_ERR;
        }
        add_filted_rows(filted_rows);

        if (new_row_block->row_block_info().row_num > 0) {
            if (!row_block_sorter.sort(&new_row_block)) {
                OLAP_LOG_WARNING("failed to sort row block.");
                result = false;
                OLAP_GOTO(SORTING_PROCESS_ERR);
            }

            row_block_arr.push_back(new_row_block);
        } else {
            _row_block_allocator->release(new_row_block);
            new_row_block = NULL;
        }

        olap_data->get_next_row_block(&ref_row_block);
    }

    if (!row_block_arr.empty()) {
        // enter here while memory limitation is reached.
        OLAPIndex* olap_index = NULL;

        if (!_internal_sorting(row_block_arr,
                               Version(_temp_delta_versions.second, _temp_delta_versions.second),
                               &olap_index)) {
            OLAP_LOG_WARNING("failed to sorting internally.");
            result = false;
            goto SORTING_PROCESS_ERR;
        }

        olap_index_arr.push_back(olap_index);

        for (vector<RowBlock*>::iterator it = row_block_arr.begin();
                it != row_block_arr.end(); ++it) {
            _row_block_allocator->release(*it);
        }

        row_block_arr.clear();

        // increase temp version
        ++_temp_delta_versions.second;
    }

    // TODO(zyh): 如果_temp_delta_versions只有一个，不需要再外排
    if (!_external_sorting(olap_index_arr, new_olap_index)) {
        OLAP_LOG_WARNING("failed to sorting externally.");
        result = false;
        goto SORTING_PROCESS_ERR;
    }

    if (olap_data->data_file_type() == COLUMN_ORIENTED_FILE) {
        reset_filted_rows();
        add_filted_rows(olap_data->get_filted_rows());
    }

    // Check row num changes
    if (config::row_nums_check) {
        if (olap_data->olap_index()->num_rows()
            != new_olap_index->num_rows() + merged_rows() + filted_rows()) {
            OLAP_LOG_WARNING("fail to check row num! "
                             "[source_rows=%lu merged_rows=%lu filted_rows=%lu new_index_rows=%lu]",
                             olap_data->olap_index()->num_rows(),
                             merged_rows(), filted_rows(), new_olap_index->num_rows());
            result = false;
        }
    } else {
        OLAP_LOG_INFO("all row nums. "
                      "[source_rows=%lu merged_rows=%lu filted_rows=%lu new_index_rows=%lu]",
                      olap_data->olap_index()->num_rows(),
                      merged_rows(), filted_rows(), new_olap_index->num_rows());
    }

SORTING_PROCESS_ERR:
    for (vector<OLAPIndex*>::iterator it = olap_index_arr.begin();
            it != olap_index_arr.end(); ++it) {
        (*it)->delete_all_files();
        SAFE_DELETE(*it);
    }

    for (vector<RowBlock*>::iterator it = row_block_arr.begin();
            it != row_block_arr.end(); ++it) {
        _row_block_allocator->release(*it);
    }

    row_block_arr.clear();
    return result;
}

bool SchemaChangeWithSorting::_internal_sorting(const vector<RowBlock*>& row_block_arr,
                                                const Version& temp_delta_versions,
                                                OLAPIndex** temp_olap_index) {
    IWriter* writer = NULL;
    uint64_t merged_rows = 0;
    RowBlockMerger merger(_olap_table);

    (*temp_olap_index) = new(nothrow) OLAPIndex(_olap_table.get(),
                                                temp_delta_versions,
                                                rand(),
                                                false,
                                                0,
                                                0);
    if (NULL == (*temp_olap_index)) {
        OLAP_LOG_WARNING("failed to malloc OLAPIndex. [size=%ld]", sizeof(OLAPIndex));
        goto INTERNAL_SORTING_ERR;
    }

    OLAP_LOG_DEBUG("init writer. [table='%s' block_row_size=%lu]",
                   _olap_table->full_name().c_str(),
                   _olap_table->num_rows_per_row_block());

    writer = IWriter::create(_olap_table, *temp_olap_index, false);
    if (NULL == writer) {
        OLAP_LOG_WARNING("failed to create writer.");
        goto INTERNAL_SORTING_ERR;
    }

    if (OLAP_SUCCESS != writer->init()) {
        OLAP_LOG_WARNING("failed to init writer.");
        goto INTERNAL_SORTING_ERR;
    }

    if (!merger.merge(row_block_arr, writer, &merged_rows)) {
        OLAP_LOG_WARNING("failed to merge row blocks.");
        goto INTERNAL_SORTING_ERR;
    }
    add_merged_rows(merged_rows);

    if (OLAP_SUCCESS != (*temp_olap_index)->load()) {
        OLAP_LOG_WARNING("failed to reload olap index.");
        goto INTERNAL_SORTING_ERR;
    }

    SAFE_DELETE(writer);
    return true;

INTERNAL_SORTING_ERR:
    SAFE_DELETE(writer);

    (*temp_olap_index)->delete_all_files();
    SAFE_DELETE(*temp_olap_index);
    return false;
}

bool SchemaChangeWithSorting::_external_sorting(
        vector<OLAPIndex*>& src_olap_index_arr,
        OLAPIndex* dest_olap_index) {
    Merger merger(_olap_table, dest_olap_index, READER_ALTER_TABLE);

    uint64_t merged_rows = 0;
    uint64_t filted_rows = 0;
    vector<IData*> olap_data_arr;

    for (vector<OLAPIndex*>::iterator it = src_olap_index_arr.begin();
            it != src_olap_index_arr.end(); ++it) {
        IData* olap_data = IData::create(*it);
        if (NULL == olap_data) {
            OLAP_LOG_WARNING("fail to create IData.");
            goto EXTERNAL_SORTING_ERR;
        }

        olap_data_arr.push_back(olap_data);

        if (OLAP_SUCCESS != olap_data->init()) {
            OLAP_LOG_WARNING("fail to initial olap data. [version='%d-%d' table='%s']",
                             (*it)->version().first,
                             (*it)->version().second,
                             (*it)->table()->full_name().c_str());
            goto EXTERNAL_SORTING_ERR;
        }
    }

    if (OLAP_SUCCESS != merger.merge(
            olap_data_arr, false, &merged_rows, &filted_rows)) {
        OLAP_LOG_WARNING("fail to merge deltas. [table='%s' version='%d-%d']",
                         _olap_table->full_name().c_str(),
                         dest_olap_index->version().first,
                         dest_olap_index->version().second);
        goto EXTERNAL_SORTING_ERR;
    }
    add_merged_rows(merged_rows);
    add_filted_rows(filted_rows);

    if (OLAP_SUCCESS != dest_olap_index->load()) {
        OLAP_LOG_WARNING("fail to reload index. [table='%s' version='%d-%d']",
                         _olap_table->full_name().c_str(),
                         dest_olap_index->version().first,
                         dest_olap_index->version().second);
        goto EXTERNAL_SORTING_ERR;
    }

    for (vector<IData*>::iterator it = olap_data_arr.begin();
            it != olap_data_arr.end(); ++it) {
        SAFE_DELETE(*it);
    }

    return true;

EXTERNAL_SORTING_ERR:
    for (vector<IData*>::iterator it = olap_data_arr.begin();
            it != olap_data_arr.end(); ++it) {
        SAFE_DELETE(*it);
    }

    dest_olap_index->delete_all_files();
    return false;
}

OLAPStatus SchemaChangeHandler::clear_schema_change_single_info(
        TTabletId tablet_id,
        SchemaHash schema_hash,
        AlterTabletType* alter_table_type,
        bool only_one,
        bool check_only) {
    SmartOLAPTable olap_table = OLAPEngine::get_instance()->get_table(tablet_id, schema_hash);
    return clear_schema_change_single_info(olap_table, alter_table_type, only_one, check_only);
}

OLAPStatus SchemaChangeHandler::clear_schema_change_single_info(
        SmartOLAPTable olap_table,
        AlterTabletType* type,
        bool only_one,
        bool check_only) {
    OLAPStatus res = OLAP_SUCCESS;

    if (NULL == olap_table.get()) {
        return res;
    }

    vector<Version> versions_to_be_changed;
    if (olap_table->get_schema_change_request(NULL,
                                              NULL,
                                              &versions_to_be_changed,
                                              NULL)) {
        if (versions_to_be_changed.size() != 0) {
            OLAP_LOG_WARNING("schema change is not allowed now, "
                             "until previous schema change is done. [table='%s']",
                             olap_table->full_name().c_str());
            return OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED;
        }
    }

    if (!check_only) {
        OLAP_LOG_DEBUG("broke old schema change chain");
        olap_table->clear_schema_change_request();
    }

    return res;
}

OLAPStatus SchemaChangeHandler::_check_and_clear_schema_change_info(
        SmartOLAPTable olap_table,
        const TAlterTabletReq& request) {
    // check for schema change chain ( A->B)
    // broken old relation if a chain was found and there is no version to be changed
    // so, there is no relation between A & B any more
    // including: alter_table, split_table, rollup_table
    OLAPStatus res = OLAP_SUCCESS;
    TTabletId tablet_id;
    TSchemaHash schema_hash;
    vector<Version> versions_to_be_changed;
    AlterTabletType type;

    // checkes schema change & rollup
    olap_table->obtain_header_rdlock();
    bool ret = olap_table->get_schema_change_request(
            &tablet_id, &schema_hash, &versions_to_be_changed, &type);
    olap_table->release_header_lock();
    if (!ret) {
        return res;
    }

    if (versions_to_be_changed.size() != 0) {
        OLAP_LOG_WARNING("schema change is not allowed now, "
                         "until previous schema change is done");
        return OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED;
    }

    if (tablet_id == request.new_tablet_req.tablet_id
            && schema_hash == request.new_tablet_req.tablet_schema.schema_hash) {
        OLAP_LOG_INFO("schema change task for specified tablet has already finished. "
                      "tablet_id=%ld schema_hash=%d", tablet_id, schema_hash);
        return res;
    }

    // clear schema change info of current tablet
    {
        AutoRWLock auto_lock(olap_table->get_header_lock_ptr(), false);
        res = clear_schema_change_single_info(
                olap_table->tablet_id(), olap_table->schema_hash(), &type, true, false);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to clear schema change info. [res=%d full_name='%s']",
                             res, olap_table->full_name().c_str());
            return res;
        }

        res = olap_table->save_header();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to save tablet header. [res=%d, full_name='%s']",
                             res, olap_table->full_name().c_str());
            return res;
        }
    }

    // clear schema change info of related tablet
    SmartOLAPTable tablet = OLAPEngine::get_instance()->get_table(
            tablet_id, schema_hash);
    if (tablet.get() == NULL) {
        OLAP_LOG_WARNING("get null tablet! [tablet_id=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    {
        AutoRWLock auto_lock(tablet->get_header_lock_ptr(), false);
        res = clear_schema_change_single_info(
                tablet_id, schema_hash, &type, true, false);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to clear schema change info. [res=%d full_name='%s']",
                             res, tablet->full_name().c_str());
            return res;
        }

        res = tablet->save_header();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to save tablet header. [res=%d, full_name='%s']",
                             res, tablet->full_name().c_str());
            return res;
        }
    }

    return res;
}

OLAPStatus SchemaChangeHandler::process_alter_table(
        AlterTabletType type,
        const TAlterTabletReq& request) {
    OLAPStatus res = OLAP_SUCCESS;
    OLAP_LOG_INFO("begin to validate alter tablet request.");

    // 1. Lock schema_change_lock util schema change info is stored in table header
    if (!OLAPEngine::get_instance()->try_schema_change_lock(request.base_tablet_id)) {
        OLAP_LOG_WARNING("failed to obtain schema change lock. [res=%d table=%ld]",
                         res, request.base_tablet_id);
        return OLAP_ERR_TRY_LOCK_FAILED;
    }

    // 2. Get base table
    SmartOLAPTable ref_olap_table = OLAPEngine::get_instance()->get_table(
            request.base_tablet_id, request.base_schema_hash);
    if (ref_olap_table.get() == NULL) {
        OLAP_LOG_WARNING("fail to find base table. [base_table=%ld base_schema_hash=%d]",
                         request.base_tablet_id, request.base_schema_hash);
        OLAPEngine::get_instance()->release_schema_change_lock(request.base_tablet_id);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // 3. Check if history schema change information exist,
    //    if exist, it will be cleaned only when all delta versions converted
    res = _check_and_clear_schema_change_info(ref_olap_table, request);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to check and clear schema change info. [table='%s']",
                         ref_olap_table->full_name().c_str());
        OLAPEngine::get_instance()->release_schema_change_lock(request.base_tablet_id);
        return res;
    }

    // 4. return failed if new table already exist in OLAPEngine.
    SmartOLAPTable new_tablet = OLAPEngine::get_instance()->get_table(
            request.new_tablet_req.tablet_id, request.new_tablet_req.tablet_schema.schema_hash);
    if (new_tablet.get() != NULL) {
        res = OLAP_SUCCESS;
    } else {
        res = _do_alter_table(type, ref_olap_table, request);
    }

    OLAPEngine::get_instance()->release_schema_change_lock(request.base_tablet_id);

    return res;
}

OLAPStatus SchemaChangeHandler::_do_alter_table(
        AlterTabletType type,
        SmartOLAPTable ref_olap_table,
        const TAlterTabletReq& request) {
    OLAPStatus res = OLAP_SUCCESS;
    SmartOLAPTable new_olap_table;
    string base_root_path = ref_olap_table->storage_root_path_name();
    OLAP_LOG_INFO("begin to do alter tablet job. new table[%d]",
                  request.new_tablet_req.tablet_id);

    // 1. Create new table and register into OLAPEngine
    res = _create_new_olap_table(ref_olap_table,
                                 request.new_tablet_req,
                                 &base_root_path,
                                 &new_olap_table);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to create new olap table. [table=%ld]",
                         request.new_tablet_req.tablet_id);
        return res;
    }

    // 2. Get version_to_be_changed and store into table header
    ref_olap_table->obtain_push_lock();
    ref_olap_table->obtain_header_wrlock();
    new_olap_table->obtain_header_wrlock();

    vector<Version> versions_to_be_changed;
    vector<IData*> olap_data_arr;
    // delete handlers for new olap table
    DeleteHandler delete_handler;
    do {
        // inherit cumulative_layer_point from ref_olap_table
        new_olap_table->set_cumulative_layer_point(ref_olap_table->cumulative_layer_point());

        // get history versions to be changed
        res = _get_versions_to_be_changed(ref_olap_table, versions_to_be_changed);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to get version to be changed. [res=%d]", res);
            break;
        }

        // store schema change information into table header
        res = _save_schema_change_info(type,
                                       ref_olap_table,
                                       new_olap_table,
                                       versions_to_be_changed);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to save schema change info. [res=%d]", res);
            break;
        }

        // acquire data sources correspond to history versions
        ref_olap_table->acquire_data_sources_by_versions(
                versions_to_be_changed, &olap_data_arr);
        if (olap_data_arr.size() < 1) {
            OLAP_LOG_WARNING("fail to acquire all data sources."
                             "[version_num=%d data_source_num=%d]",
                             versions_to_be_changed.size(),
                             olap_data_arr.size());
            res = OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS;
            break;
        }

        // init one delete handler
        int32_t end_version = -1;
        for (size_t i = 0; i < olap_data_arr.size(); ++i) {
            if (olap_data_arr[i]->version().second > end_version) {
                end_version = olap_data_arr[i]->version().second;
            }
        }

        res = delete_handler.init(ref_olap_table, end_version);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("init delete handler failed. [table=%s; end_version=%d]",
                             ref_olap_table->full_name().c_str(), end_version);

            // release delete handlers which have been inited successfully.
            delete_handler.finalize();
            break;
        }
    } while (0);

    new_olap_table->release_header_lock();
    ref_olap_table->release_header_lock();
    ref_olap_table->release_push_lock();

    if (res == OLAP_SUCCESS) {
        // 3. Generate alter job
        SchemaChangeParams sc_params;
        sc_params.alter_table_type = type;
        sc_params.ref_olap_table = ref_olap_table;
        sc_params.new_olap_table = new_olap_table;
        sc_params.ref_olap_data_arr = olap_data_arr;
        sc_params.delete_handler = delete_handler;


        // 4. Update schema change status of ref_olap_table and new_olap_tables
        new_olap_table->set_schema_change_status(ALTER_TABLE_RUNNING,
                                                 ref_olap_table->schema_hash(),
                                                 versions_to_be_changed.back().second);
        ref_olap_table->set_schema_change_status(ALTER_TABLE_RUNNING,
                                                 new_olap_table->schema_hash(),
                                                 versions_to_be_changed.back().second);

        // add tid to cgroup
        CgroupsMgr::apply_system_cgroup();

        // process the job : special for query table split key
        OLAP_LOG_TRACE("starts to alter table. [new_table='%s' ref_table='%s']",
                       sc_params.new_olap_table->full_name().c_str(),
                       sc_params.ref_olap_table->full_name().c_str());

        if ((res = _alter_table(&sc_params)) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("failed to alter table. [request='%s']",
                             sc_params.debug_message.c_str());
        }

        OLAP_LOG_TRACE("schema change thread completed the job. [request='%s']",
                       sc_params.debug_message.c_str());
    } else {
        // Delete olap table when submit alter table failed.
        OLAPEngine::get_instance()->drop_table(
                new_olap_table->tablet_id(), new_olap_table->schema_hash());
    }

    OLAP_LOG_WARNING("finish to generate alter tablet job. [res=%d]", res);
    return res;
}

OLAPStatus SchemaChangeHandler::_create_new_olap_table(
        const SmartOLAPTable ref_olap_table,
        const TCreateTabletReq& request,
        const string* ref_root_path,
        SmartOLAPTable* out_new_olap_table) {
    OLAPStatus res = OLAP_SUCCESS;
    OLAPTable* new_olap_table = NULL;
    bool is_table_added = false;

    // 1. Lock to ensure that all _create_new_olap_table operation execute in serial
    static MutexLock create_table_lock;
    create_table_lock.lock();

    do {
        // 2. Create table with only header, no deltas
        OLAPTable* new_olap_table = OLAPEngine::get_instance()->create_table(
                request, ref_root_path, true, ref_olap_table);
        if (new_olap_table == NULL) {
            OLAP_LOG_WARNING("failed to create table. [table=%ld xml_path=%d]",
                             request.tablet_id,
                             request.tablet_schema.schema_hash);
            res = OLAP_ERR_INPUT_PARAMETER_ERROR;
            break;
        }

        // 有可能出现以下2种特殊情况：
        // 1. 因为操作系统时间跳变，导致新生成的表的creation_time小于旧表的creation_time时间
        // 2. 因为olap engine代码中统一以秒为单位，所以如果2个操作(比如create一个表,
        //    然后立即alter该表)之间的时间间隔小于1s，则alter得到的新表和旧表的creation_time会相同
        //
        // 当出现以上2种情况时，为了能够区分alter得到的新表和旧表，这里把新表的creation_time设置为
        // 旧表的creation_time加1
        if (new_olap_table->creation_time() <= ref_olap_table->creation_time()) {
            OLAP_LOG_WARNING("new table's creation time is less than or equal to old table"
                             "[new_table_creation_time=%ld; old_table_creation_time=%ld]",
                             new_olap_table->creation_time(),
                             ref_olap_table->creation_time());
            int64_t new_creation_time = ref_olap_table->creation_time() + 1;
            new_olap_table->set_creation_time(new_creation_time);
        }

        // 3. Add table to OlapEngine will make it visiable to user
        res = OLAPEngine::get_instance()->add_table(
                request.tablet_id,
                request.tablet_schema.schema_hash,
                new_olap_table);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("failed to add table to OLAPEngine. [res=%d table='%s']",
                             res, new_olap_table->full_name().c_str());
            break;
        }
        is_table_added = true;

        // 4. Register table into OLAPRootPath, so that we can manage table from
        // the perspective of root path.
        // Example: unregister all tables when a bad disk found.
        res = OLAPRootPath::get_instance()->register_table_into_root_path(
                new_olap_table);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to register table into root path. "
                             "[root_path='%s' table='%s']",
                             new_olap_table->storage_root_path_name().c_str(),
                             new_olap_table->full_name().c_str());
            break;
        }

        SmartOLAPTable olap_table;
        olap_table = OLAPEngine::get_instance()->get_table(
                request.tablet_id, request.tablet_schema.schema_hash);
        if (olap_table.get() == NULL) {
            OLAP_LOG_WARNING("failed to get table from OLAPEngine. [table=%ld schema_hash=%d]",
                             request.tablet_id,
                             request.tablet_schema.schema_hash);
            res = OLAP_ERR_OTHER_ERROR;
            break;
        }

        // 5. Copy selectivity information
        res = _copy_table_attributes(ref_olap_table, olap_table);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("failed to copy table attributes");
            break;
        }

        if (out_new_olap_table != NULL) {
            *out_new_olap_table = olap_table;
        }
    } while (0);

    if (res != OLAP_SUCCESS) {
        if (is_table_added) {
            res = OLAPEngine::get_instance()->drop_table(
                    request.tablet_id, request.tablet_schema.schema_hash);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to drop table when create table failed. "
                                 "[res=%d table=%s]",
                                 res, new_olap_table->full_name().c_str());
            }
        } else if (NULL != new_olap_table) {
            new_olap_table->delete_all_files();
            SAFE_DELETE(new_olap_table);
        }
    }

    create_table_lock.unlock();
    return res;
}

OLAPStatus SchemaChangeHandler::schema_version_convert(
        SmartOLAPTable src_olap_table,
        SmartOLAPTable dest_olap_table,
        vector<OLAPIndex*>* ref_olap_indices,
        vector<OLAPIndex*>* new_olap_indices) {
    if (NULL == new_olap_indices) {
        OLAP_LOG_WARNING("new_olap_index is NULL.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    OLAPStatus res = OLAP_SUCCESS;
    OLAP_LOG_INFO("begin to convert delta version for schema changing. "
                  "[src_tablet='%s' dest_tablet='%s']",
                  src_olap_table->full_name().c_str(),
                  dest_olap_table->full_name().c_str());

    // a. 解析Alter请求，转换成内部的表示形式
    // 不使用DELETE_DATA命令指定的删除条件
    RowBlockChanger rb_changer(dest_olap_table->tablet_schema(), src_olap_table);
    bool sc_sorting = false;
    bool sc_directly = false;

    if (OLAP_SUCCESS != (res = _parse_request(src_olap_table,
                                              dest_olap_table,
                                              &rb_changer,
                                              &sc_sorting,
                                              &sc_directly))) {
        OLAP_LOG_WARNING("failed to parse the request. [res=%d]", res);
        return res;
    }

    // NOTE split_table如果使用row_block，会导致原block变小
    // 但由于历史数据在后续base/cumulative后还是会变成正常，故用directly也可以
    // b. 生成历史数据转换器
    SchemaChange* sc_procedure = NULL;
    if (true == sc_sorting) {
        size_t memory_limitation = config::memory_limiation_per_thread_for_schema_change;
        OLAP_LOG_INFO("doing schema change with sorting.");
        sc_procedure = new(nothrow) SchemaChangeWithSorting(
                                dest_olap_table,
                                rb_changer,
                                memory_limitation * 1024 * 1024 * 1024);
    } else if (true == sc_directly || src_olap_table->data_file_type() == OLAP_DATA_FILE) {
        OLAP_LOG_INFO("doing schema change directly.");
        sc_procedure = new(nothrow) SchemaChangeDirectly(
                                dest_olap_table, rb_changer);
    } else {
        OLAP_LOG_INFO("doing linked schema change.");
        sc_procedure = new(nothrow) LinkedSchemaChange(
                                src_olap_table,
                                dest_olap_table);
    }

    if (NULL == sc_procedure) {
        OLAP_LOG_FATAL("failed to malloc SchemaChange. [size=%ld]",
                       sizeof(SchemaChangeWithSorting));
        return OLAP_ERR_MALLOC_ERROR;
    }

    // c. 转换数据
    IData* olap_data = NULL;
    for (vector<OLAPIndex*>::iterator it = ref_olap_indices->begin();
            it != ref_olap_indices->end(); ++it) {
        IData* olap_data = IData::create(*it);
        if (NULL == olap_data) {
            OLAP_LOG_WARNING("fail to create IData.");
            res = OLAP_ERR_MALLOC_ERROR;
            goto SCHEMA_VERSION_CONVERT_ERR;
        }

        olap_data->init();

        OLAPIndex* new_olap_index = new(nothrow) OLAPIndex(
                                            dest_olap_table.get(),
                                            olap_data->version(),
                                            olap_data->version_hash(),
                                            olap_data->delete_flag(),
                                            0,
                                            olap_data->max_timestamp());

        if (NULL == new_olap_index) {
            OLAP_LOG_FATAL("failed to malloc OLAPIndex. [size=%ld]", sizeof(OLAPIndex));
            res = OLAP_ERR_MALLOC_ERROR;
            goto SCHEMA_VERSION_CONVERT_ERR;
        }

        new_olap_indices->push_back(new_olap_index);

        if (!sc_procedure->process(olap_data, new_olap_index)) {
            OLAP_LOG_WARNING("failed to process the version. [version='%d-%d']",
                             (*it)->version().first,
                             (*it)->version().second);
            res = OLAP_ERR_INPUT_PARAMETER_ERROR;
            goto SCHEMA_VERSION_CONVERT_ERR;
        }

        SAFE_DELETE(olap_data);
    }

    SAFE_DELETE(sc_procedure);
    SAFE_DELETE(olap_data);

    return res;

SCHEMA_VERSION_CONVERT_ERR:
    while (!new_olap_indices->empty()) {
        OLAPIndex* olap_index = new_olap_indices->back();
        olap_index->delete_all_files();
        SAFE_DELETE(olap_index);
        new_olap_indices->pop_back();
    }

    SAFE_DELETE(sc_procedure);
    SAFE_DELETE(olap_data);
    return res;
}

OLAPStatus SchemaChangeHandler::_get_versions_to_be_changed(
        SmartOLAPTable ref_olap_table,
        vector<Version>& versions_to_be_changed) {
    int32_t request_version = 0;
    const FileVersionMessage* latest_version = ref_olap_table->latest_version();
    if (latest_version != NULL) {
        request_version = latest_version->end_version() - 1;
    } else {
        OLAP_LOG_WARNING("Table has no version. [path='%s']",
                         ref_olap_table->full_name().c_str());
        return OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS;
    }

    // 最新版本的delta可以被重导覆盖，因此计算获取的路径中，
    // 必须包含最新版本的delta
    if (request_version >= 0) {
        vector<Version> span_versions;
        ref_olap_table->select_versions_to_span(Version(0, request_version), &span_versions);

        // get all version list
        vector<VersionEntity> all_versions;
        ref_olap_table->list_version_entities(&all_versions);
        if (0 == all_versions.size()) {
            OLAP_LOG_WARNING("there'is no any version in the table. [table='%s']",
                             ref_olap_table->full_name().c_str());
            return OLAP_ERR_VERSION_NOT_EXIST;
        }

        for (uint32_t i = 0; i < span_versions.size(); i++) {
            versions_to_be_changed.push_back(span_versions[i]);
        }
    }
    versions_to_be_changed.push_back(
            Version(latest_version->start_version(), latest_version->end_version()));

    return OLAP_SUCCESS;
}

// 创建新olap-table时，从ref-table中拷贝selectivity信息
OLAPStatus SchemaChangeHandler::_copy_table_attributes(
        SmartOLAPTable ref_olap_table,
        SmartOLAPTable new_olap_table) {
    OLAPStatus res = OLAP_SUCCESS;

    vector<uint32_t> ref_selectivities;

    if (OLAP_SUCCESS != (res = ref_olap_table->get_selectivities(&ref_selectivities))) {
        OLAP_LOG_WARNING("failed to get selectivities from ref_table. [table='%s']",
                         ref_olap_table->full_name().c_str());
        return res;
    }

    // TODO(lijiao) : 这样的selectivity是正确的么？
    vector<uint32_t> new_selectivities(new_olap_table->tablet_schema().size(), 1);

    for (size_t i = 0; i < new_olap_table->tablet_schema().size(); ++i) {
        new_selectivities[i] = i < ref_selectivities.size() ?
                               ref_selectivities[i] : new_selectivities[i - 1];
    }

    new_olap_table->set_selectivities(new_selectivities);

    return res;
}


// 增加A->(B|C|...) 的schema_change信息
OLAPStatus SchemaChangeHandler::_save_schema_change_info(
        AlterTabletType alter_table_type,
        SmartOLAPTable ref_olap_table,
        SmartOLAPTable new_olap_table,
        const vector<Version>& versions_to_be_changed) {
    OLAPStatus res = OLAP_SUCCESS;

    // 1. 在新表和旧表中添加schema change标志
    ref_olap_table->clear_schema_change_request();
    ref_olap_table->set_schema_change_request(new_olap_table->tablet_id(),
                                              new_olap_table->schema_hash(),
                                              versions_to_be_changed,
                                              alter_table_type);
    new_olap_table->set_schema_change_request(ref_olap_table->tablet_id(),
                                              ref_olap_table->schema_hash(),
                                              vector<Version>(),  // empty versions
                                              alter_table_type);

    // save new olap table header :只有一个父ref table
    res = new_olap_table->save_header();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_FATAL("fail to save new table header. [res=%d table='%s']",
                       res, new_olap_table->full_name().c_str());
        return res;
    }

    res = ref_olap_table->save_header();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_FATAL("fail to save ref table header. [res=%d table='%s']",
                       res, ref_olap_table->full_name().c_str());
        return res;
    }

    return res;
}

// @static
OLAPStatus SchemaChangeHandler::_alter_table(SchemaChangeParams* sc_params) {
    OLAPStatus res = OLAP_SUCCESS;
    OLAP_LOG_INFO("begin to process alter table job. "
                  "[ref_olap_table='%s' new_olap_table='%s']",
                  sc_params->ref_olap_table->full_name().c_str(),
                  sc_params->new_olap_table->full_name().c_str());

    // find end version
    int32_t end_version = -1;
    for (size_t i = 0; i < sc_params->ref_olap_data_arr.size(); ++i) {
        if (sc_params->ref_olap_data_arr[i]->version().second > end_version) {
            end_version = sc_params->ref_olap_data_arr[i]->version().second;
        }
    }

    // change中增加了filter信息，在_parse_request中会设置filter的column信息
    // 并在每次row block的change时，过滤一些数据
    RowBlockChanger rb_changer(sc_params->new_olap_table->tablet_schema(),
                               sc_params->ref_olap_table,
                               sc_params->delete_handler);

    bool sc_sorting = false;
    bool sc_directly = false;
    SchemaChange* sc_procedure = NULL;

    // a. 解析Alter请求，转换成内部的表示形式
    res = _parse_request(sc_params->ref_olap_table,
                         sc_params->new_olap_table,
                         &rb_changer,
                         &sc_sorting,
                         &sc_directly);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to parse the request. [res=%d]", res);
        goto PROCESS_ALTER_EXIT;
    }

    // b. 生成历史数据转换器
    if (true == sc_sorting) {
        size_t memory_limitation = config::memory_limiation_per_thread_for_schema_change;
        OLAP_LOG_INFO("doing schema change with sorting.");
        sc_procedure = new(nothrow) SchemaChangeWithSorting(
                               sc_params->new_olap_table,
                               rb_changer,
                               memory_limitation * 1024 * 1024 * 1024);
    } else if (true == sc_directly
               || sc_params->ref_olap_table->data_file_type() == OLAP_DATA_FILE) {
        OLAP_LOG_INFO("doing schema change directly.");
        sc_procedure = new(nothrow) SchemaChangeDirectly(
                sc_params->new_olap_table, rb_changer);
    } else {
        OLAP_LOG_INFO("doing linked schema change.");
        sc_procedure = new(nothrow) LinkedSchemaChange(
                                sc_params->ref_olap_table,
                                sc_params->new_olap_table);
    }

    if (NULL == sc_procedure) {
        OLAP_LOG_WARNING("failed to malloc SchemaChange. [size=%ld]",
                         sizeof(SchemaChangeWithSorting));
        res = OLAP_ERR_MALLOC_ERROR;
        goto PROCESS_ALTER_EXIT;
    }

    // c. 转换历史数据
    for (vector<IData*>::iterator it = sc_params->ref_olap_data_arr.end() - 1;
            it >= sc_params->ref_olap_data_arr.begin(); --it) {
        OLAP_LOG_TRACE("begin to convert a history delta. [version='%d-%d']",
                       (*it)->version().first, (*it)->version().second);

        // set status for monitor
        // 只要有一个new_table为running，ref table就设置为running
        // NOTE 如果第一个sub_table先fail，这里会继续按正常走
        sc_params->ref_olap_table->set_schema_change_status(
                ALTER_TABLE_RUNNING,
                sc_params->new_olap_table->schema_hash(),
                -1);
        sc_params->new_olap_table->set_schema_change_status(
                ALTER_TABLE_RUNNING,
                sc_params->ref_olap_table->schema_hash(),
                (*it)->version().second);

        // we create a new delta with the same version as the IData processing currently.
        OLAPIndex* new_olap_index = new(nothrow) OLAPIndex(
                                            sc_params->new_olap_table.get(),
                                            (*it)->version(),
                                            (*it)->version_hash(),
                                            (*it)->delete_flag(),
                                            0,
                                            (*it)->max_timestamp());

        if (new_olap_index == NULL) {
            OLAP_LOG_WARNING("failed to malloc OLAPIndex. [size=%ld]", sizeof(OLAPIndex));
            res = OLAP_ERR_MALLOC_ERROR;
            goto PROCESS_ALTER_EXIT;
        }

        (*it)->set_delete_handler(sc_params->delete_handler);
        int del_ret = (*it)->delete_pruning_filter();
        if (DEL_SATISFIED == del_ret) {
            OLAP_LOG_DEBUG("filter delta in schema change: %d, %d",
                           (*it)->version().first, (*it)->version().second);
            res = sc_procedure->create_init_version(
                                                    new_olap_index->table()->tablet_id(),
                                                    new_olap_index->table()->schema_hash(),
                                                    new_olap_index->version(),
                                                    new_olap_index->version_hash(),
                                                    new_olap_index);
            sc_procedure->add_filted_rows((*it)->num_rows());
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to create init version. [res=%d]", res);
                res = OLAP_ERR_INPUT_PARAMETER_ERROR;
                OLAP_GOTO(PROCESS_ALTER_EXIT);
            }
        } else if (DEL_PARTIAL_SATISFIED == del_ret) {
            OLAP_LOG_DEBUG("filter delta partially in schema change: %d, %d",
                           (*it)->version().first, (*it)->version().second);
            (*it)->set_delete_status(DEL_PARTIAL_SATISFIED);
        } else {
            OLAP_LOG_DEBUG("not filter delta in schema change: %d, %d",
                           (*it)->version().first, (*it)->version().second);
            (*it)->set_delete_status(DEL_NOT_SATISFIED);
        }

        if (DEL_SATISFIED != del_ret && !sc_procedure->process(*it, new_olap_index)) {
            //if del_ret is DEL_SATISFIED, the new delta version has already been created in new_olap_table
            OLAP_LOG_WARNING("failed to process the version. [version='%d-%d']",
                             (*it)->version().first, (*it)->version().second);
            new_olap_index->delete_all_files();
            SAFE_DELETE(new_olap_index);

            res = OLAP_ERR_INPUT_PARAMETER_ERROR;
            goto PROCESS_ALTER_EXIT;
        }

        // 将新版本的数据加入header
        // 为了防止死锁的出现，一定要先锁住旧表，再锁住新表
        sc_params->new_olap_table->obtain_push_lock();
        sc_params->ref_olap_table->obtain_header_wrlock();
        sc_params->new_olap_table->obtain_header_wrlock();

        if (!sc_params->new_olap_table->has_version((*it)->version())) {
            // register version
            res = sc_params->new_olap_table->register_data_source(new_olap_index);
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("failed to register new version. [table='%s' version='%d-%d']",
                                 sc_params->new_olap_table->full_name().c_str(),
                                 (*it)->version().first,
                                 (*it)->version().second);
                new_olap_index->delete_all_files();
                SAFE_DELETE(new_olap_index);

                sc_params->new_olap_table->release_header_lock();
                sc_params->ref_olap_table->release_header_lock();

                goto PROCESS_ALTER_EXIT;
            }

            OLAP_LOG_DEBUG("register new version. [table='%s' version='%d-%d']",
                           sc_params->new_olap_table->full_name().c_str(),
                           (*it)->version().first,
                           (*it)->version().second);
        } else {
            OLAP_LOG_WARNING("version already exist, version revert occured. "
                             "[table='%s' version='%d-%d']",
                             sc_params->new_olap_table->full_name().c_str(),
                             (*it)->version().first, (*it)->version().second);
            new_olap_index->delete_all_files();
            SAFE_DELETE(new_olap_index);
        }

        // 保存header
        if (OLAP_SUCCESS != sc_params->new_olap_table->save_header()) {
            OLAP_LOG_FATAL("fail to save header. [res=%d table='%s']",
                           res, sc_params->new_olap_table->full_name().c_str());
        }

        // XXX: 此处需要验证ref_olap_data_arr中最后一个版本是否与new_olap_table的header中记录的最
        //      后一个版本相同。然后还要注意一致性问题。
        if (!sc_params->ref_olap_table->remove_last_schema_change_version(
                    sc_params->new_olap_table)) {
            OLAP_LOG_WARNING("failed to remove the last version did schema change.");

            sc_params->new_olap_table->release_header_lock();
            sc_params->ref_olap_table->release_header_lock();

            res = OLAP_ERR_INPUT_PARAMETER_ERROR;
            goto PROCESS_ALTER_EXIT;
        }

        // 保存header
        if (OLAP_SUCCESS != sc_params->ref_olap_table->save_header()) {
            OLAP_LOG_FATAL("failed to save header. [table='%s']",
                           sc_params->new_olap_table->full_name().c_str());
        }

        sc_params->new_olap_table->release_header_lock();
        sc_params->ref_olap_table->release_header_lock();
        sc_params->new_olap_table->release_push_lock();

        OLAP_LOG_TRACE("succeed to convert a history version. [version='%d-%d']",
                       (*it)->version().first,
                       (*it)->version().second);

        // 释放IData
        vector<IData*> olap_data_to_be_released(it, it + 1);
        sc_params->ref_olap_table->release_data_sources(&olap_data_to_be_released);

        it = sc_params->ref_olap_data_arr.erase(it); // after erasing, it will point to end()
    }

    // XXX: 此时应该不取消SchemaChange状态，因为新Delta还要转换成新旧Schema的版本

PROCESS_ALTER_EXIT:
    if (res == OLAP_SUCCESS) {
        Version test_version(0, end_version);
        res = sc_params->new_olap_table->test_version(test_version);
    }

    if (res == OLAP_SUCCESS) {
        // ref的状态只有2个new table都完成后，才能设置为done
        sc_params->ref_olap_table->obtain_header_rdlock();
        res = clear_schema_change_single_info(sc_params->ref_olap_table, NULL, false, true);
        sc_params->ref_olap_table->release_header_lock();

        if (OLAP_SUCCESS == res) {
            sc_params->ref_olap_table->set_schema_change_status(
                    ALTER_TABLE_DONE,
                    sc_params->new_olap_table->schema_hash(),
                    -1);
        } else {
            res = OLAP_SUCCESS;
        }

        sc_params->new_olap_table->set_schema_change_status(
                ALTER_TABLE_DONE,
                sc_params->ref_olap_table->schema_hash(),
                -1);
        OLAP_LOG_DEBUG("set alter table job status. [status=%d]",
                       sc_params->ref_olap_table->schema_change_status().status);
    } else {
        sc_params->ref_olap_table->set_schema_change_status(
                ALTER_TABLE_FAILED,
                sc_params->new_olap_table->schema_hash(),
                -1);

        sc_params->new_olap_table->set_schema_change_status(
                ALTER_TABLE_FAILED,
                sc_params->ref_olap_table->schema_hash(),
                -1);
        OLAP_LOG_DEBUG("set alter table job status. [status=%d]",
                       sc_params->ref_olap_table->schema_change_status().status);
    }

    sc_params->ref_olap_table->release_data_sources(&(sc_params->ref_olap_data_arr));
    SAFE_DELETE(sc_procedure);

    OLAP_LOG_INFO("finish to process alter table job. [res=%d]", res);
    return res;
}

// @static
// 分析column的mapping以及filter key的mapping
OLAPStatus SchemaChangeHandler::_parse_request(SmartOLAPTable ref_olap_table,
                                               SmartOLAPTable new_olap_table,
                                               RowBlockChanger* rb_changer,
                                               bool* sc_sorting,
                                               bool* sc_directly) {
    OLAPStatus res = OLAP_SUCCESS;

    // set column mapping
    for (int i = 0, new_schema_size = new_olap_table->tablet_schema().size();
            i < new_schema_size; ++i) {
        const FieldInfo& new_column_schema = new_olap_table->tablet_schema()[i];
        const string& column_name = new_column_schema.name;
        ColumnMapping* column_mapping = rb_changer->get_mutable_column_mapping(i);

        if (new_column_schema.has_referenced_column) {
            int32_t column_index = ref_olap_table->get_field_index(
                                       new_column_schema.referenced_column);

            if (column_index < 0) {
                OLAP_LOG_WARNING("referenced column was missing. "
                                 "[column='%s' referenced_column='%s']",
                                 column_name.c_str(),
                                 new_column_schema.referenced_column.c_str());
                return OLAP_ERR_CE_CMD_PARAMS_ERROR;
            }

            column_mapping->ref_column = column_index;
            OLAP_LOG_DEBUG("A column refered to existed column will be added after schema changing."
                           "[column='%s' ref_column='%s']",
                           column_name.c_str(),
                           new_column_schema.referenced_column.c_str());
            continue;
        }

        int32_t column_index = ref_olap_table->get_field_index(column_name);
        if (column_index >= 0) {
            column_mapping->ref_column = column_index;
            continue;
        }

        // 新加列走这里
        //if (new_column_schema.is_allow_null || new_column_schema.has_default_value) {
        {
            column_mapping->ref_column = -1;

            if (i < ref_olap_table->num_short_key_fields()) {
                *sc_directly = true;
            }

            if (OLAP_SUCCESS != (res = _init_column_mapping(
                                         column_mapping,
                                         new_column_schema,
                                         new_column_schema.default_value))) {
                return res;
            }

            OLAP_LOG_TRACE("A column with default value will be added after schema chaning. "
                           "[column='%s' default_value='%s']",
                           column_name.c_str(),
                           new_column_schema.default_value.c_str());
            continue;
        }


        // XXX: 只有DROP COLUMN时，遇到新Schema转旧Schema时会进入这里。
        column_mapping->ref_column = -1;

        if (OLAP_SUCCESS != (res = _init_column_mapping(
                                       column_mapping,
                                       new_column_schema,
                                       ""))) {
            return res;
        }

        OLAP_LOG_DEBUG("A new schema delta is converted while droping column. "
                       "Droped column will be assigned as '0' for the older schema. "
                       "[column='%s']",
                       column_name.c_str());
    }

    // Check if re-aggregation is needed.
    *sc_sorting = false;
    // 若Key列的引用序列出现乱序，则需要重排序
    int num_default_value = 0;

    for (int i = 0, new_schema_size = new_olap_table->num_key_fields();
            i < new_schema_size; ++i) {
        ColumnMapping* column_mapping = rb_changer->get_mutable_column_mapping(i);

        if (column_mapping->ref_column < 0) {
            num_default_value++;
            continue;
        }

        if (column_mapping->ref_column != i - num_default_value) {
            *sc_sorting = true;
            return OLAP_SUCCESS;
        }
    }

    if (ref_olap_table->num_short_key_fields() != new_olap_table->num_short_key_fields()) {
        // the number of short_keys changed, can't do linked schema change
        *sc_directly = true;
        return OLAP_SUCCESS;
    }

    const RowFields& ref_table_schema = ref_olap_table->tablet_schema();
    const RowFields& new_table_schema = new_olap_table->tablet_schema();
    for (size_t i = 0; i < new_olap_table->num_fields(); ++i) {
        ColumnMapping* column_mapping = rb_changer->get_mutable_column_mapping(i);
        if (column_mapping->ref_column < 0) {
            continue;
        } else {
            if (new_table_schema[i].type != ref_table_schema[column_mapping->ref_column].type) {
                *sc_directly = true;
                return OLAP_SUCCESS;
            } else if (
                (new_table_schema[i].type == ref_table_schema[column_mapping->ref_column].type)
                    && (new_table_schema[i].length 
                        != ref_table_schema[column_mapping->ref_column].length)) {
                *sc_directly = true;
                return OLAP_SUCCESS;

            } else if (new_table_schema[i].is_bf_column != ref_table_schema[i].is_bf_column) {
                *sc_directly = true;
                return OLAP_SUCCESS;
            }
        }
    }

    if (ref_olap_table->delete_data_conditions_size() != 0){
        //there exists delete condtion in header, can't do linked schema change
        *sc_directly = true;
    }

    if (ref_olap_table->data_file_type() != new_olap_table->data_file_type()) {
        //if change the table from row-oriented to column-oriented, or versus 
        *sc_directly = true;
    }

    return OLAP_SUCCESS;
}

OLAPStatus SchemaChangeHandler::_init_column_mapping(ColumnMapping* column_mapping,
                                                     const FieldInfo& column_schema,
                                                     const std::string& value) {
    column_mapping->default_value = Field::create(column_schema);

    if (column_mapping->default_value == NULL) {
        return OLAP_ERR_MALLOC_ERROR;
    }

    if (!column_mapping->default_value->allocate()) {
        OLAP_LOG_WARNING("failed to init Field. [column='%s']", column_schema.name.c_str());
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    if (true == column_schema.is_allow_null && value.length() == 0) {
        column_mapping->default_value->set_null();
    } else {
        column_mapping->default_value->from_string(value);
    }

    return OLAP_SUCCESS;
}

OLAPStatus SchemaChange::create_init_version(
        TTabletId tablet_id,
        SchemaHash schema_hash,
        Version version,
        VersionHash version_hash,
        OLAPIndex* olap_index) {
    OLAP_LOG_DEBUG("begin to create init version. [begin=%d end=%d]",
                   version.first, version.second);

    SmartOLAPTable table;
    IWriter* writer = NULL;
    OLAPStatus res = OLAP_SUCCESS;

    do {
        if (version.first > version.second) {
            OLAP_LOG_WARNING("begin should not larger than end. [begin=%d end=%d]",
                             version.first, version.second);
            res = OLAP_ERR_INPUT_PARAMETER_ERROR;
            break;
        }

        // Get olap table and generate new index
        table = OLAPEngine::get_instance()->get_table(tablet_id, schema_hash);
        if (table.get() == NULL) {
            OLAP_LOG_WARNING("fail to find table. [table=%ld]", tablet_id);
            res = OLAP_ERR_TABLE_NOT_FOUND;
            break;
        }

        // Create writer, which write nothing to table, to generate empty data file
        writer = IWriter::create(table, olap_index, false);
        if (writer == NULL) {
            OLAP_LOG_WARNING("fail to create writer. [table=%s]", table->full_name().c_str());
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        res = writer->init();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init writer. [table=%s]", table->full_name().c_str());
            break;
        }

        res = writer->finalize();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to finalize writer. [table=%s]", table->full_name().c_str());
            break;
        }

        // Load new index and add to table
        res = olap_index->load();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to load new index. [table=%s]", table->full_name().c_str());
            break;
        }
    } while (0);

    OLAP_LOG_DEBUG("create init version end. [res=%d]", res);
    SAFE_DELETE(writer);
    return res;
}

}  // namespace palo

