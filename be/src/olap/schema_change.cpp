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

#include "olap/merger.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/wrapper_field.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/rowset/alpha_rowset_writer.h"
#include "common/resource_tls.h"
#include "agent/cgroups_mgr.h"

using std::deque;
using std::list;
using std::nothrow;
using std::pair;
using std::string;
using std::stringstream;
using std::vector;

namespace doris {

bool version_entity_sorter(const VersionEntity& a, const VersionEntity& b) {
    if (a.version.first != b.version.first) {
        return a.version.first < b.version.first;
    } else {
        return a.version.second < b.version.second;
    }
}

RowBlockChanger::RowBlockChanger(const TabletSchema& tablet_schema,
                                 const TabletSharedPtr &ref_tablet) {
    _schema_mapping.resize(tablet_schema.num_columns());
}

RowBlockChanger::RowBlockChanger(const TabletSchema& tablet_schema,
                                 const TabletSharedPtr& ref_tablet,
                                 const DeleteHandler& delete_handler) {
    _schema_mapping.resize(tablet_schema.num_columns());
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

#define TYPE_REINTERPRET_CAST(FromType, ToType) \
{ \
    size_t row_num = ref_block.row_block_info().row_num; \
    for (size_t row = 0, mutable_row = 0; row < row_num; ++row) { \
        if (is_data_left_vec[row] != 0) { \
            char* ref_ptr = ref_block.field_ptr(row, ref_column); \
            char* new_ptr = mutable_block->field_ptr(mutable_row++, i); \
            *new_ptr = *ref_ptr; \
            *(ToType*)(new_ptr + 1) = *(FromType*)(ref_ptr + 1); \
        } \
    } \
    break; \
}

#define LARGEINT_REINTERPRET_CAST(FromType, ToType) \
{ \
    size_t row_num = ref_block.row_block_info().row_num; \
    for (size_t row = 0, mutable_row = 0; row < row_num; ++row) { \
        if (is_data_left_vec[row] != 0) { \
            char* ref_ptr = ref_block.field_ptr(row, ref_column); \
            char* new_ptr = mutable_block->field_ptr(mutable_row++, i); \
            *new_ptr = *ref_ptr; \
            ToType new_value = *(FromType*)(ref_ptr + 1); \
            memcpy(new_ptr + 1, &new_value, sizeof(ToType)); \
        } \
    } \
    break; \
}

#define CONVERT_FROM_TYPE(from_type) \
{ \
    switch (mutable_block->tablet_schema().column(i).type()) {\
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
                         ref_block.tablet_schema().column(ref_column).type(), \
                         mutable_block->tablet_schema().column(i).type()); \
        return false; \
    } \
    break; \
}

#define ASSIGN_DEFAULT_VALUE(length) \
    case length: { \
        for (size_t row = 0; row < ref_block.row_block_info().row_num; ++row) { \
            memcpy(buf, _schema_mapping[i].default_value->ptr(), length); \
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
    if (mutable_block == NULL) {
        LOG(FATAL) << "mutable block is uninitialized.";
        return false;
    } else if (mutable_block->tablet_schema().num_columns() != _schema_mapping.size()) {
        OLAP_LOG_WARNING("mutable block does not match with schema mapping rules. "
                         "[block_schema_size=%ld, mapping_schema_size=%ld]",
                         mutable_block->tablet_schema().num_columns(),
                         _schema_mapping.size());
        return false;
    }

    if (mutable_block->capacity() < ref_block.row_block_info().row_num) {
        OLAP_LOG_WARNING("mutable block is not large enough for storing the changed block. "
                         "[mutable_block_size=%ld, ref_block_size=%u]",
                         mutable_block->capacity(),
                         ref_block.row_block_info().row_num);
        return false;
    }

    mutable_block->clear();

    RowCursor write_helper;
    if (write_helper.init(mutable_block->tablet_schema()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init rowcursor.");
        return false;
    }

    RowCursor read_helper;
    if (read_helper.init(ref_block.tablet_schema()) != OLAP_SUCCESS) {
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
        ref_block.get_row(row_index, &read_helper);

        // filter data according to delete conditions specified in DeleteData command
        if (is_data_left_vec[row_index] == 1) {
            if (_delete_handler.is_filter_data(data_version, read_helper)) {
                is_data_left_vec[row_index] = 0;
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

    MemPool* mem_pool = mutable_block->mem_pool();
    // b. 根据前面的过滤信息，只对还标记为1的处理
    for (size_t i = 0, len = mutable_block->tablet_schema().num_columns(); !filter_all && i < len; ++i) {
        int32_t ref_column = _schema_mapping[i].ref_column;

        if (_schema_mapping[i].ref_column >= 0) {
            // new column will be assigned as referenced column
            // check if the type of new column is equal to the older's.
            if (mutable_block->tablet_schema().column(i).type()
                    == ref_block.tablet_schema().column(ref_column).type()) {
                // 效率低下，也可以直接计算变长域拷贝，但仍然会破坏封装
                for (size_t row_index = 0, new_row_index = 0; 
                        row_index < ref_block.row_block_info().row_num; ++row_index) {
                    // 不需要的row，每次处理到这个row时就跳过
                    if (need_filter_data && is_data_left_vec[row_index] == 0) {
                        continue;
                    }

                    // 指定新的要写入的row index（不同于读的row_index）
                    mutable_block->get_row(new_row_index++, &write_helper);
                    ref_block.get_row(row_index, &read_helper);

                    if (true == read_helper.is_null(ref_column)) {
                        write_helper.set_null(i);
                    } else {
                        const Field* field_to_read = read_helper.get_field_by_index(ref_column);
                        if (NULL == field_to_read) {
                            OLAP_LOG_WARNING("faile to get ref field.[index=%d]", ref_column);
                            return false;
                        }
                        
                        write_helper.set_not_null(i);
                        if (mutable_block->tablet_schema()[i].type == OLAP_FIELD_TYPE_CHAR) {
                            // if modify length of CHAR type, the size of slice should be equal
                            // to new length.
                            Slice* src = (Slice*)(field_to_read->get_ptr(read_helper.get_buf()));
                            size_t size = mutable_block->tablet_schema()[i].length;
                            char* buf = reinterpret_cast<char*>(mem_pool->allocate(size));
                            memset(buf, 0, size);
                            size_t copy_size = (size < src->size) ? size : src->size;
                            memcpy(buf, src->data, copy_size);
                            Slice dst(buf, size);
                            write_helper.set_field_content(i, reinterpret_cast<char*>(&dst), mem_pool);
                        } else {
                            char* src = field_to_read->get_ptr(read_helper.get_buf());
                            write_helper.set_field_content(i, src, mem_pool);
                        }
                    }
                }

                // 从ref_column 写入 i列。
            } else if (mutable_block->tablet_schema().column(i).type() == OLAP_FIELD_TYPE_VARCHAR
                       && ref_block.tablet_schema().column(ref_column).type() == OLAP_FIELD_TYPE_CHAR) {
                // 效率低下，也可以直接计算变长域拷贝，但仍然会破坏封装
                for (size_t row_index = 0, new_row_index = 0;
                        row_index < ref_block.row_block_info().row_num; ++row_index) {
                    // 不需要的row，每次处理到这个row时就跳过
                    if (need_filter_data && is_data_left_vec[row_index] == 0) {
                        continue;
                    }

                    // 指定新的要写入的row index（不同于读的row_index）
                    mutable_block->get_row(new_row_index++, &write_helper);

                    ref_block.get_row(row_index, &read_helper);

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
                        int p = ref_block.tablet_schema().column(ref_column).length() - 1;
                        Slice* slice = reinterpret_cast<Slice*>(field_to_read->get_ptr(read_helper.get_buf()));
                        char* buf = slice->data;
                        while (p >= 0 && buf[p] == '\0') {
                            p--;
                        }
                        slice->size = p + 1;
                        write_helper.set_field_content(i, buf, mem_pool);
                    }
                }

                // 从ref_column 写入 i列。
            } else {
                // copy and alter the field
                // 此处可以暂时不动，新类型暂时不涉及类型转换
                switch (ref_block.tablet_schema().column(ref_column).type()) {
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
                                     ref_block.tablet_schema().column(ref_column).type());
                    return false;
                }

                if (mutable_block->tablet_schema().column(i).type() <
                        ref_block.tablet_schema().column(ref_column).type()) {
                    VLOG(3) << "type degraded while altering column. "
                            << "column=" << mutable_block->tablet_schema().column(i).name()
                            << ", origin_type=" << ref_block.tablet_schema().column(ref_column).type()
                            << ", alter_type=" << mutable_block->tablet_schema().column(i).type();
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

                mutable_block->get_row(new_row_index++, &write_helper);

                if (_schema_mapping[i].default_value->is_null()) {
                    write_helper.set_null(i);
                } else {
                    write_helper.set_not_null(i);
                    write_helper.set_field_content(
                        i, _schema_mapping[i].default_value->ptr(), mem_pool);
                }
            }
        }
    }

    // NOTE 当前mutable_block的内存row_num还是和ref一样多
    //  （其实在init时就可以重新init成少的，filter留下的new_row_num）
    // 在split_table时，可能会出现因为过滤导致没有数据
    mutable_block->finalize(new_row_num);
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

    if (_swap_row_block == NULL || _swap_row_block->capacity() < row_num) {
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

    RowCursor helper_row;
    auto res = helper_row.init(_swap_row_block->tablet_schema());
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "row cursor init failed.res:" << res;
        return false;
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

        (*row_block)->get_row(i, row_cursor_list[i]);
    }

    // Must use 'std::' because this class has a function whose name is sort too
    std::stable_sort(row_cursor_list.begin(), row_cursor_list.end(), _row_cursor_comparator);

    // copy the results sorted to temp row block.
    _swap_row_block->clear();
    for (size_t i = 0; i < row_cursor_list.size(); ++i) {
        _swap_row_block->get_row(i, &helper_row);
        if (helper_row.copy(*row_cursor_list[i], _swap_row_block->mem_pool()) != OLAP_SUCCESS) {
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

RowBlockAllocator::RowBlockAllocator(const TabletSchema& tablet_schema,
                                     size_t memory_limitation) :
        _tablet_schema(tablet_schema),
        _memory_allocated(0),
        _memory_limitation(memory_limitation) {
    _row_len = 0;
    _row_len = tablet_schema.row_size();

    VLOG(3) << "RowBlockAllocator(). row_len=" << _row_len;
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
        VLOG(3) << "RowBlockAllocator::alocate() memory exceeded. "
                << "m_memory_allocated=" << _memory_allocated;
        *row_block = NULL;
        return OLAP_SUCCESS;
    }

    // TODO(lijiao) : 为什么舍弃原有的m_row_block_buffer
    *row_block = new(nothrow) RowBlock(&_tablet_schema);

    if (*row_block == NULL) {
        LOG(WARNING) << "failed to malloc RowBlock. size=" << sizeof(RowBlock);
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
    VLOG(3) << "RowBlockAllocator::allocate() this=" << this
            << ", num_rows=" << num_rows
            << ", m_memory_allocated=" << _memory_allocated
            << ", row_block_addr=" << *row_block;
    return res;
}

void RowBlockAllocator::release(RowBlock* row_block) {
    if (row_block == NULL) {
        OLAP_LOG_WARNING("null row block released.");
        return;
    }

    _memory_allocated -= row_block->capacity() * _row_len;

    VLOG(3) << "RowBlockAllocator::release() this=" << this
            << ", num_rows=" << row_block->capacity()
            << ", m_memory_allocated=" << _memory_allocated
            << ", row_block_addr=" << row_block;
    delete row_block;
}

RowBlockMerger::RowBlockMerger(TabletSharedPtr tablet) : _tablet(tablet) {}

RowBlockMerger::~RowBlockMerger() {}

bool RowBlockMerger::merge(
        const vector<RowBlock*>& row_block_arr,
        RowsetWriterSharedPtr rowset_writer,
        uint64_t* merged_rows) {
    uint64_t tmp_merged_rows = 0;
    RowCursor row_cursor;
    if (row_cursor.init(_tablet->tablet_schema()) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init row cursor.";
        goto MERGE_ERR;
    }

    _make_heap(row_block_arr);

    // TODO: for now, string type in rowblock is not allocated
    // memory during init procedure. So, copying content
    // in row_cursor to rowblock is necessary
    // That's not very memory-efficient!

    while (_heap.size() > 0) {
        row_cursor.allocate_memory_for_string_type(_tablet->tablet_schema(), rowset_writer->mem_pool());

        row_cursor.agg_init(*(_heap.top().row_cursor));

        if (!_pop_heap()) {
            goto MERGE_ERR;
        }

        if (KeysType::DUP_KEYS == _tablet->keys_type()) {
            rowset_writer->add_row(&row_cursor);
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
        rowset_writer->add_row(&row_cursor);
    }
    if (rowset_writer->flush() != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to finalizing writer.";
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
            LOG(FATAL) << "failed to malloc RowCursor. size=" << sizeof(RowCursor);
            return false;
        }

        if (element.row_cursor->init(element.row_block->tablet_schema()) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("failed to init row cursor.");
            SAFE_DELETE(element.row_cursor);
            return false;
        }

        element.row_block->get_row(element.row_block_index, element.row_cursor);

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

    element.row_block->get_row(element.row_block_index, element.row_cursor);

    _heap.push(element);
    return true;
}

LinkedSchemaChange::LinkedSchemaChange(
        TabletSharedPtr base_tablet, TabletSharedPtr new_tablet) :
        _base_tablet(base_tablet),
        _new_tablet(new_tablet) { }

bool LinkedSchemaChange::process(
        RowsetReaderSharedPtr rowset_reader,
        RowsetWriterSharedPtr new_rowset_writer,
        TabletSharedPtr tablet) {
    OLAPStatus status = new_rowset_writer->add_rowset(rowset_reader->rowset());
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to reload index. "
                     << "[tablet='"<< _new_tablet->full_name() << "'"
                     << " version='%d" << new_rowset_writer->version().first
                     << "-" << new_rowset_writer->version().second << "']";
        return false;
    }

    return true;
}

SchemaChangeDirectly::SchemaChangeDirectly(
        TabletSharedPtr tablet,
        const RowBlockChanger& row_block_changer) :
        _tablet(tablet),
        _row_block_changer(row_block_changer),
        _row_block_allocator(NULL),
        _src_cursor(NULL) {}

SchemaChangeDirectly::~SchemaChangeDirectly() {
    VLOG(3) << "~SchemaChangeDirectly()";
    SAFE_DELETE(_row_block_allocator);
    SAFE_DELETE(_src_cursor);
}

bool SchemaChangeDirectly::_write_row_block(RowsetWriterSharedPtr rowset_writer, RowBlock* row_block) {
    for (uint32_t i = 0; i < row_block->row_block_info().row_num; i++) {
        row_block->get_row(i, _src_cursor);
        if (OLAP_SUCCESS != rowset_writer->add_row(_src_cursor)) {
            LOG(WARNING) << "fail to attach writer";
            return false;
        }
    }

    return true;
}

bool SchemaChangeDirectly::process(RowsetReaderSharedPtr rowset_reader, RowsetWriterSharedPtr rowset_writer,
                                   TabletSharedPtr tablet) {
    DataFileType data_file_type = tablet->data_file_type();
    bool null_supported = true;

    if (NULL == _row_block_allocator) {
        if (NULL == (_row_block_allocator =
                         new(nothrow) RowBlockAllocator(_tablet->tablet_schema(), 0))) {
            LOG(FATAL) << "failed to malloc RowBlockAllocator. size=" << sizeof(RowBlockAllocator);
            return false;
        }
    }

    if (NULL == _src_cursor) {
        _src_cursor = new(nothrow) RowCursor();
        if (NULL == _src_cursor) {
            OLAP_LOG_WARNING("fail to allocate row cursor.");
            return false;
        }

        if (OLAP_SUCCESS != _src_cursor->init(_tablet->tablet_schema())) {
            OLAP_LOG_WARNING("fail to init row cursor.");
            return false;
        }
    }

    RowBlock* ref_row_block = NULL;
    bool need_create_empty_version = false;
    OLAPStatus res = OLAP_SUCCESS;
    if (!rowset_reader->rowset()->empty()) {
        int num_rows = rowset_reader->rowset()->num_rows();
        if (num_rows == 0) {
            // actually, the rowset is empty
            need_create_empty_version = true;
        }
    } else {
        need_create_empty_version = true;
    }

    if (need_create_empty_version) {
        res = rowset_writer->flush();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "create empty version for schema change failed."
                << "version=" << rowset_writer->version().first << "-" << rowset_writer->version().second;
            return false;
        }
        return true;
    }

    VLOG(3) << "init writer. tablet=" << _tablet->full_name()
        << "block_row_size=" << _tablet->num_rows_per_row_block();
    bool result = true;
    RowBlock* new_row_block = NULL;

    // Reset filted_rows and merged_rows statistic
    reset_merged_rows();
    reset_filted_rows();

    while (NULL != ref_row_block) {
        // 注意这里强制分配和旧块等大的块(小了可能会存不下)
        if (NULL == new_row_block
                || new_row_block->capacity() < ref_row_block->row_block_info().row_num) {
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
        if (!_row_block_changer.change_row_block(data_file_type,
                                                 *ref_row_block,
                                                 rowset_reader->version().second,
                                                 new_row_block,
                                                 &filted_rows)) {
            OLAP_LOG_WARNING("failed to change data in row block.");
            result = false;
            goto DIRECTLY_PROCESS_ERR;
        }
        add_filted_rows(filted_rows);

        if (!_write_row_block(rowset_writer, new_row_block)) {
            OLAP_LOG_WARNING("failed to write row block.");
            result = false;
            goto DIRECTLY_PROCESS_ERR;
        }

        rowset_reader->next_block(&ref_row_block);

    }

    if (OLAP_SUCCESS != rowset_writer->flush()) {
        result = false;
        goto DIRECTLY_PROCESS_ERR;
    }

    add_filted_rows(rowset_reader->get_filtered_rows());

    // Check row num changes
    if (config::row_nums_check) {
        if (rowset_reader->num_rows()
            != rowset_writer->num_rows() + merged_rows() + filted_rows()) {
            LOG(FATAL) << "fail to check row num! "
                       << "source_rows=" << rowset_reader->num_rows()
                       << ", merged_rows=" << merged_rows()
                       << ", filted_rows=" << filted_rows()
                       << ", new_index_rows=" << rowset_writer->num_rows();
            result = false;
        }
    } else {
        LOG(INFO) << "all row nums. source_rows=" << rowset_reader->num_rows()
                  << ", merged_rows=" << merged_rows()
                  << ", filted_rows=" << filted_rows()
                  << ", new_index_rows=" << rowset_writer->num_rows();
    }

DIRECTLY_PROCESS_ERR:
    _row_block_allocator->release(new_row_block);
    return result;
}

SchemaChangeWithSorting::SchemaChangeWithSorting(TabletSharedPtr tablet,
                                                 const RowBlockChanger& row_block_changer,
                                                 size_t memory_limitation) :
        _tablet(tablet),
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
    VLOG(3) << "~SchemaChangeWithSorting()";
    SAFE_DELETE(_row_block_allocator);
}

bool SchemaChangeWithSorting::process(
            RowsetReaderSharedPtr rowset_reader,
            RowsetWriterSharedPtr new_rowset_writer,
            TabletSharedPtr tablet) {
    if (NULL == _row_block_allocator) {
        if (NULL == (_row_block_allocator = new(nothrow) RowBlockAllocator(
                        _tablet->tablet_schema(), _memory_limitation))) {
            LOG(FATAL) << "failed to malloc RowBlockAllocator. size=" << sizeof(RowBlockAllocator);
            return false;
        }
    }

    DataFileType data_file_type = tablet->data_file_type();
    bool null_supported = true;

    RowBlock* ref_row_block = NULL;
    bool need_create_empty_version = false;
    OLAPStatus res = OLAP_SUCCESS;
    RowsetSharedPtr rowset = rowset_reader->rowset();
    if (!rowset->empty()) {
        if (!rowset_reader->has_next()) {
            need_create_empty_version = true;
        }
    } else {
        need_create_empty_version = true;
    }

    if (need_create_empty_version) {
        res = new_rowset_writer->flush();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "create empty version for schema change failed."
                << "version=" << new_rowset_writer->version().first
                << "-" << new_rowset_writer->version().second;
            return false;
        }
        return true;
    }


    bool result = true;
    RowBlockSorter row_block_sorter(_row_block_allocator);

    // for internal sorting
    RowBlock* new_row_block = NULL;
    vector<RowBlock*> row_block_arr;

    // for external sorting
    vector<RowsetSharedPtr> src_rowsets;

    _temp_delta_versions.first = _temp_delta_versions.second;

    // Reset filtered_rows and merged_rows statistic
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
            RowsetSharedPtr rowset;
            if (!_internal_sorting(row_block_arr,
                                   Version(_temp_delta_versions.second,
                                           _temp_delta_versions.second),
                                    rowset_reader->version_hash(),
                                   &rowset)) {
                LOG(WARNING) << "failed to sorting internally.";
                result = false;
                goto SORTING_PROCESS_ERR;
            }

            src_rowsets.push_back(rowset);

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
                data_file_type,
                *ref_row_block,
                rowset_reader->version().second,
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

        res = rowset_reader->next_block(&ref_row_block);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to get next row block.";
            result = false;
            OLAP_GOTO(SORTING_PROCESS_ERR);
        }
    }

    if (!row_block_arr.empty()) {
        // enter here while memory limitation is reached.
        RowsetSharedPtr rowset = NULL;

        if (!_internal_sorting(row_block_arr,
                               Version(_temp_delta_versions.second, _temp_delta_versions.second),
                               rowset_reader->version_hash(),
                               &rowset)) {
            OLAP_LOG_WARNING("failed to sorting internally.");
            result = false;
            goto SORTING_PROCESS_ERR;
        }

        src_rowsets.push_back(rowset);

        for (vector<RowBlock*>::iterator it = row_block_arr.begin();
                it != row_block_arr.end(); ++it) {
            _row_block_allocator->release(*it);
        }

        row_block_arr.clear();

        // increase temp version
        ++_temp_delta_versions.second;
    }

    // TODO(zyh): 如果_temp_delta_versions只有一个，不需要再外排
    if (!_external_sorting(src_rowsets, new_rowset_writer, tablet)) {
        OLAP_LOG_WARNING("failed to sorting externally.");
        result = false;
        goto SORTING_PROCESS_ERR;
    }

    add_filted_rows(rowset_reader->get_filtered_rows());

    // Check row num changes
    if (config::row_nums_check) {
        if (rowset_reader->num_rows()
            != new_rowset_writer->num_rows() + merged_rows() + filted_rows()) {
            LOG(WARNING) << "fail to check row num! "
                         << "[source_rows=" << rowset_reader->num_rows()
                         << " merged_rows=" << merged_rows()
                         << " filted_rows=" << filted_rows()
                         << " new_index_rows=" << new_rowset_writer->num_rows() << "]";
            result = false;
        }
    } else {
        LOG(INFO) << "all row nums. source_rows=" << rowset_reader->num_rows()
                  << ", merged_rows=" << merged_rows()
                  << ", filted_rows=" << filted_rows()
                  << ", new_index_rows=" << new_rowset_writer->num_rows();
    }

SORTING_PROCESS_ERR:
    for (vector<RowsetSharedPtr>::iterator it = src_rowsets.begin();
            it != src_rowsets.end(); ++it) {
        (*it)->remove();
    }

    for (vector<RowBlock*>::iterator it = row_block_arr.begin();
            it != row_block_arr.end(); ++it) {
        _row_block_allocator->release(*it);
    }

    row_block_arr.clear();
    return result;
}

bool SchemaChangeWithSorting::_internal_sorting(const vector<RowBlock*>& row_block_arr,
                                                const Version& version,
                                                const VersionHash version_hash,
                                                RowsetSharedPtr* rowset) {
    uint64_t merged_rows = 0;
    RowBlockMerger merger(_tablet);

    RowsetWriterContextBuilderSharedPtr context_builder(new RowsetWriterContextBuilder());
    RowsetId rowset_id = 0;
    OLAPStatus status = RowsetIdGenerator::instance()->get_next_id(_tablet->data_dir(), &rowset_id);
    if (status == OLAP_SUCCESS) {
        LOG(WARNING) << "get next rowset id failed";
        return false;
    }
    context_builder->set_rowset_id(rowset_id)
                   .set_tablet_id(_tablet->tablet_id())
                   .set_partition_id(_tablet->partition_id())
                   .set_tablet_schema_hash(_tablet->schema_hash())
                   .set_rowset_type(ALPHA_ROWSET)
                   .set_rowset_path_prefix(_tablet->tablet_path())
                   .set_tablet_schema(&(_tablet->tablet_schema()))
                   .set_rowset_state(VISIBLE)
                   .set_version(version)
                   .set_version_hash(version_hash);
    
    RowsetWriterContext context = context_builder->build();
    RowsetWriterSharedPtr rowset_writer(new AlphaRowsetWriter());
    if (rowset_writer == nullptr) {
        LOG(WARNING) << "new rowset builder failed";
        return false;
    }
    VLOG(3) << "init rowset builder. tablet=" << _tablet->full_name()
            << ", block_row_size=" << _tablet->num_rows_per_row_block();
    rowset_writer->init(context);

    if (!merger.merge(row_block_arr, rowset_writer, &merged_rows)) {
        LOG(WARNING) << "failed to merge row blocks.";
        goto INTERNAL_SORTING_ERR;
    }
    add_merged_rows(merged_rows);

    *rowset = rowset_writer->build();

    if (OLAP_SUCCESS != (*rowset)->init()) {
        LOG(WARNING) << "failed to reload olap index.";
        goto INTERNAL_SORTING_ERR;
    }

    return true;

INTERNAL_SORTING_ERR:
    rowset_writer->release();
    return false;
}

bool SchemaChangeWithSorting::_external_sorting(
        vector<RowsetSharedPtr>& src_rowsets,
        RowsetWriterSharedPtr rowset_writer,
        TabletSharedPtr tablet) {
    Merger merger(_tablet, rowset_writer, READER_ALTER_TABLE);

    uint64_t merged_rows = 0;
    uint64_t filted_rows = 0;
    vector<RowsetReaderSharedPtr> rs_readers;

    for (vector<RowsetSharedPtr>::iterator it = src_rowsets.begin();
            it != src_rowsets.end(); ++it) {
        RowsetReaderSharedPtr rowset_reader = (*it)->create_reader();
        if (rowset_reader == nullptr) {
            LOG(WARNING) << "fail to create rowset reader.";
            goto EXTERNAL_SORTING_ERR;
        }
        OLAPStatus status = rowset_reader->init(nullptr);
        if (status == OLAP_SUCCESS) {
            LOG(WARNING) << "fail to initial olap data."
                         << " [version='%d" << (*it)->version().first
                         << "-" << (*it)->version().second
                         << "' tablet='" << tablet->full_name() <<"']";
            goto EXTERNAL_SORTING_ERR;
        }
        rs_readers.push_back(rowset_reader);

        
    }

    if (OLAP_SUCCESS != merger.merge(rs_readers, &merged_rows, &filted_rows)) {
        OLAP_LOG_WARNING("fail to merge deltas. [tablet='%s' version='%d-%d']",
                         _tablet->full_name().c_str(),
                         rowset_writer->version().first,
                         rowset_writer->version().second);
        goto EXTERNAL_SORTING_ERR;
    }
    add_merged_rows(merged_rows);
    add_filted_rows(filted_rows);

    return true;

EXTERNAL_SORTING_ERR:
    rowset_writer->release();
    return false;
}

// MOVE TO TABLET and then add rdlock to schema change request
OLAPStatus SchemaChangeHandler::_check_and_clear_schema_change_info(
        TabletSharedPtr tablet,
        const TAlterTabletReq& request) {
    // check for schema change chain ( A->B)
    // broken old relation if a chain was found and there is no version to be changed
    // so, there is no relation between A & B any more
    // including: alter_tablet, split_tablet, rollup_tablet
    OLAPStatus res = OLAP_SUCCESS;
    TTabletId tablet_id;
    TSchemaHash schema_hash;
    vector<Version> versions_to_be_changed;
    AlterTabletType type;

    // checkes schema change & rollup
    tablet->obtain_header_rdlock();
    bool ret = tablet->get_schema_change_request(
            &tablet_id, &schema_hash, &versions_to_be_changed, &type);
    tablet->release_header_lock();
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
        LOG(INFO) << "schema change task for specified tablet has already finished. "
                  << "tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
        return res;
    }

    // clear schema change info of current tablet
    {
        WriteLock wrlock(tablet->get_header_lock_ptr());
        res = tablet->clear_schema_change_info(&type, true, false);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to clear schema change info. [res=%d full_name='%s']",
                             res, tablet->full_name().c_str());
            return res;
        }

        res = tablet->save_tablet_meta();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to save tablet header. [res=%d, full_name='%s']",
                             res, tablet->full_name().c_str());
            return res;
        }
    }

    // clear schema change info of related tablet
    TabletSharedPtr related_tablet = TabletManager::instance()->get_tablet(
            tablet_id, schema_hash);
    if (related_tablet.get() == NULL) {
        OLAP_LOG_WARNING("get null tablet! [tablet_id=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    {
        WriteLock wrlock(related_tablet->get_header_lock_ptr());
        res = related_tablet->clear_schema_change_info(&type, true, false);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to clear schema change info. [res=%d full_name='%s']",
                             res, related_tablet->full_name().c_str());
            return res;
        }

        res = related_tablet->save_tablet_meta();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to save related_tablet header. [res=%d, full_name='%s']",
                             res, related_tablet->full_name().c_str());
            return res;
        }
    }

    return res;
}

OLAPStatus SchemaChangeHandler::process_alter_tablet(
        AlterTabletType type,
        const TAlterTabletReq& request) {
    OLAPStatus res = OLAP_SUCCESS;
    LOG(INFO) << "begin to validate alter tablet request.";

    // 1. Lock schema_change_lock util schema change info is stored in tablet header
    if (!TabletManager::instance()->try_schema_change_lock(request.base_tablet_id)) {
        OLAP_LOG_WARNING("failed to obtain schema change lock. [res=%d tablet=%ld]",
                         res, request.base_tablet_id);
        return OLAP_ERR_TRY_LOCK_FAILED;
    }

    // 2. Get base tablet
    TabletSharedPtr ref_tablet = TabletManager::instance()->get_tablet(
            request.base_tablet_id, request.base_schema_hash);
    if (ref_tablet.get() == NULL) {
        OLAP_LOG_WARNING("fail to find base tablet. [base_tablet=%ld base_schema_hash=%d]",
                         request.base_tablet_id, request.base_schema_hash);
        TabletManager::instance()->release_schema_change_lock(request.base_tablet_id);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // 3. Check if history schema change information exist,
    //    if exist, it will be cleaned only when all delta versions converted
    res = _check_and_clear_schema_change_info(ref_tablet, request);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to check and clear schema change info. [tablet='%s']",
                         ref_tablet->full_name().c_str());
        TabletManager::instance()->release_schema_change_lock(request.base_tablet_id);
        return res;
    }

    // 4. return failed if new tablet already exist in StorageEngine.
    TabletSharedPtr new_tablet = TabletManager::instance()->get_tablet(
            request.new_tablet_req.tablet_id, request.new_tablet_req.tablet_schema.schema_hash);
    if (new_tablet.get() != NULL) {
        res = OLAP_SUCCESS;
    } else {
        res = _do_alter_tablet(type, ref_tablet, request);
    }

    TabletManager::instance()->release_schema_change_lock(request.base_tablet_id);

    return res;
}

OLAPStatus SchemaChangeHandler::_do_alter_tablet(
        AlterTabletType type,
        TabletSharedPtr ref_tablet,
        const TAlterTabletReq& request) {
    OLAPStatus res = OLAP_SUCCESS;
    TabletSharedPtr new_tablet;
    string base_root_path = ref_tablet->storage_root_path_name();

    LOG(INFO) << "begin to do alter tablet job. new_tablet_id=" << request.new_tablet_req.tablet_id;
    // 1. Create new tablet and register into StorageEngine
    res = _create_new_tablet(ref_tablet,
                                 request.new_tablet_req,
                                 &base_root_path,
                                 &new_tablet);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to create new tablet. [tablet=%ld]",
                         request.new_tablet_req.tablet_id);
        return res;
    }

    // set schema change status temporarily,
    // after waiting transactions to finish, will calculate versions again
    vector<Version> tmp_versions_to_be_changed;
    tmp_versions_to_be_changed.push_back(Version(-1, -1));
    ref_tablet->obtain_push_lock();
    ref_tablet->obtain_header_wrlock();
    new_tablet->obtain_header_wrlock();
    res = _save_schema_change_info(type, ref_tablet, new_tablet, tmp_versions_to_be_changed);
    new_tablet->release_header_lock();
    ref_tablet->release_header_lock();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to save schema change info before waiting transactions. "
                         "[base=%s new=%s res=%d]", ref_tablet->full_name().c_str(),
                         new_tablet->full_name().c_str(), res);
        ref_tablet->release_push_lock();
        TabletManager::instance()->drop_tablet(
                new_tablet->tablet_id(), new_tablet->schema_hash());
        return res;
    }

    // get current transactions
    int64_t partition_id;
    std::set<int64_t> transaction_ids;
    TxnManager::instance()->get_tablet_related_txns(ref_tablet, &partition_id, &transaction_ids);
    ref_tablet->release_push_lock();

    // wait transactions to publish version
    int num = 0;
    while (!transaction_ids.empty()) {
        VLOG(3) << "wait transactions when schema change. tablet=" << ref_tablet->full_name()
                << ", transaction_size=" << transaction_ids.size();
        num++;
        if (num % 100 == 0) {
            for (int64_t transaction_id : transaction_ids) {
                LOG(INFO) << "transaction_id is waiting by schema_change: " << transaction_id;
            }
        }
        sleep(1);
        // erase finished transaction
        vector<int64_t> finished_transactions;
        for (int64_t transaction_id : transaction_ids) {
            if (!TxnManager::instance()->has_txn(
                partition_id, transaction_id,
                ref_tablet->tablet_id(), ref_tablet->schema_hash())) {
                finished_transactions.push_back(transaction_id);
            }
        }
        for (int64_t transaction_id : finished_transactions) {
            transaction_ids.erase(transaction_id);
            VLOG(3) << "transaction finished when schema change is waiting. "
                    << "tablet=" << ref_tablet->full_name()
                    << ", transaction_id=" << transaction_id
                    << ", transaction_size=" << transaction_ids.size();
        }
    }

    // 2. Get version_to_be_changed and store into tablet header
    ref_tablet->obtain_push_lock();
    ref_tablet->obtain_header_wrlock();
    new_tablet->obtain_header_wrlock();

    // before calculating version_to_be_changed,
    // remove all data from new tablet, prevent to rewrite data(those double pushed when wait)
    VLOG(3) << "begin to remove all data from new tablet to prevent rewrite. "
            << "new_tablet=" << new_tablet->full_name();

    vector<Version> versions_to_be_changed;
    vector<RowsetReaderSharedPtr> rs_readers;
    // delete handlers for new tablet
    DeleteHandler delete_handler;
    do {
        // inherit cumulative_layer_point from ref_tablet
        new_tablet->set_cumulative_layer_point(ref_tablet->cumulative_layer_point());

        // get history versions to be changed
        res = _get_versions_to_be_changed(ref_tablet, versions_to_be_changed);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to get version to be changed. [res=%d]", res);
            break;
        }

        // store schema change information into tablet header
        res = _save_schema_change_info(type,
                                       ref_tablet,
                                       new_tablet,
                                       versions_to_be_changed);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to save schema change info. [res=%d]", res);
            break;
        }

        // acquire data sources correspond to history versions
        ref_tablet->capture_rs_readers(versions_to_be_changed, &rs_readers);
        if (rs_readers.size() < 1) {
            LOG(WARNING) << "fail to acquire all data sources. "
                         << "version_num=" << versions_to_be_changed.size()
                         << ", data_source_num=" << rs_readers.size();
            res = OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS;
            break;
        }

        // init one delete handler
        int32_t end_version = -1;
        for (size_t i = 0; i < rs_readers.size(); ++i) {
            if (rs_readers[i]->version().second > end_version) {
                end_version = rs_readers[i]->version().second;
            }
        }

        res = delete_handler.init(new_tablet->tablet_schema(), new_tablet->delete_predicates(), end_version);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("init delete handler failed. [tablet=%s; end_version=%d]",
                             ref_tablet->full_name().c_str(), end_version);

            // release delete handlers which have been inited successfully.
            delete_handler.finalize();
            break;
        }
    } while (0);

    new_tablet->release_header_lock();
    ref_tablet->release_header_lock();
    ref_tablet->release_push_lock();

    if (res == OLAP_SUCCESS) {
        // 3. Generate alter job
        SchemaChangeParams sc_params;
        sc_params.alter_tablet_type = type;
        sc_params.ref_tablet = ref_tablet;
        sc_params.new_tablet = new_tablet;
        sc_params.ref_rowset_readers = rs_readers;
        sc_params.delete_handler = delete_handler;


        // 4. Update schema change status of ref_tablet and new_tablets
        new_tablet->set_schema_change_status(ALTER_TABLE_RUNNING,
                                                 ref_tablet->schema_hash(),
                                                 versions_to_be_changed.back().second);
        ref_tablet->set_schema_change_status(ALTER_TABLE_RUNNING,
                                                 new_tablet->schema_hash(),
                                                 versions_to_be_changed.back().second);

        // add tid to cgroup
        CgroupsMgr::apply_system_cgroup();

        // process the job : special for query tablet split key
        VLOG(10) << "starts to alter tablet. "
                 << "old_tablet=" << sc_params.ref_tablet->full_name()
                 << ", new_tablet=" << sc_params.new_tablet->full_name();

        if ((res = _alter_tablet(&sc_params)) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("failed to alter tablet. [request='%s']",
                             sc_params.debug_message.c_str());
        }

        VLOG(10) << "schema change thread completed the job. "
                 << "request=" << sc_params.debug_message;
    } else {
        // Delete tablet when submit alter tablet failed.
        TabletManager::instance()->drop_tablet(
                new_tablet->tablet_id(), new_tablet->schema_hash());
    }

    OLAP_LOG_WARNING("finish to generate alter tablet job. [res=%d]", res);
    return res;
}

OLAPStatus SchemaChangeHandler::_create_new_tablet(
        const TabletSharedPtr ref_tablet,
        const TCreateTabletReq& request,
        const string* ref_root_path,
        TabletSharedPtr* out_new_tablet) {
    OLAPStatus res = OLAP_SUCCESS;
    Tablet* new_tablet = NULL;
    bool is_tablet_added = false;

    // 1. Lock to ensure that all _create_new_tablet operation execute in serial
    static Mutex create_tablet_lock;
    create_tablet_lock.lock();

    do {
        // 2. Create tablet with only header, no deltas
        TabletSharedPtr new_tablet = StorageEngine::instance()->create_tablet(request, true, ref_tablet);
        if (new_tablet == NULL) {
            OLAP_LOG_WARNING("failed to create tablet. [tablet=%ld xml_path=%d]",
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
        if (new_tablet->creation_time() <= ref_tablet->creation_time()) {
            OLAP_LOG_WARNING("new tablet's creation time is less than or equal to old tablet"
                             "[new_tablet_creation_time=%ld; old_tablet_creation_time=%ld]",
                             new_tablet->creation_time(),
                             ref_tablet->creation_time());
            int64_t new_creation_time = ref_tablet->creation_time() + 1;
            new_tablet->set_creation_time(new_creation_time);
        }

        // 3. Add tablet to StorageEngine will make it visiable to user
        res = TabletManager::instance()->add_tablet(
                request.tablet_id,
                request.tablet_schema.schema_hash,
                new_tablet, 
                false);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("failed to add tablet to StorageEngine. [res=%d tablet='%s']",
                             res, new_tablet->full_name().c_str());
            break;
        }
        is_tablet_added = true;

        // 4. Register tablet into store, so that we can manage tablet from
        // the perspective of root path.
        // Example: unregister all tables when a bad disk found.
        res = new_tablet->register_tablet_into_dir();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to register tablet into root path. "
                             "[root_path='%s' tablet='%s']",
                             new_tablet->storage_root_path_name().c_str(),
                             new_tablet->full_name().c_str());
            break;
        }

        TabletSharedPtr tablet;
        tablet = TabletManager::instance()->get_tablet(
                request.tablet_id, request.tablet_schema.schema_hash);
        if (tablet.get() == NULL) {
            OLAP_LOG_WARNING("failed to get tablet from StorageEngine. [tablet=%ld schema_hash=%d]",
                             request.tablet_id,
                             request.tablet_schema.schema_hash);
            res = OLAP_ERR_OTHER_ERROR;
            break;
        }

        if (out_new_tablet != NULL) {
            *out_new_tablet = tablet;
        }
    } while (0);

    if (res != OLAP_SUCCESS) {
        if (is_tablet_added) {
            res = TabletManager::instance()->drop_tablet(
                    request.tablet_id, request.tablet_schema.schema_hash);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to drop tablet when create tablet failed. res=" << res
                    << ", tablet=" << request.tablet_id
                    << ":" << request.tablet_schema.schema_hash;
            }
        } else if (NULL != new_tablet) {
            new_tablet->delete_all_files();
        }
    }

    create_tablet_lock.unlock();
    return res;
}

OLAPStatus SchemaChangeHandler::schema_version_convert(
        TabletSharedPtr src_tablet,
        TabletSharedPtr dest_tablet,
        std::vector<RowsetSharedPtr>* old_rowsets,
        std::vector<RowsetSharedPtr>* new_rowsets) {
    if (NULL == new_rowsets) {
        OLAP_LOG_WARNING("new rowsets vector is NULL.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    OLAPStatus res = OLAP_SUCCESS;
    LOG(INFO) << "begin to convert delta version for schema changing. "
              << "old_tablet=" << src_tablet->full_name()
              << ", dest_tablet=" << dest_tablet->full_name();

    // a. 解析Alter请求，转换成内部的表示形式
    // 不使用DELETE_DATA命令指定的删除条件
    RowBlockChanger rb_changer(dest_tablet->tablet_schema(), src_tablet);
    bool sc_sorting = false;
    bool sc_directly = false;

    if (OLAP_SUCCESS != (res = _parse_request(src_tablet,
                                              dest_tablet,
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
        size_t memory_limitation = config::memory_limitation_per_thread_for_schema_change;
        LOG(INFO) << "doing schema change with sorting.";
        sc_procedure = new(nothrow) SchemaChangeWithSorting(
                                dest_tablet,
                                rb_changer,
                                memory_limitation * 1024 * 1024 * 1024);
    } else if (true == sc_directly) {
        LOG(INFO) << "doing schema change directly.";
        sc_procedure = new(nothrow) SchemaChangeDirectly(
                                dest_tablet, rb_changer);
    } else {
        LOG(INFO) << "doing linked schema change.";
        sc_procedure = new(nothrow) LinkedSchemaChange(
                                src_tablet,
                                dest_tablet);
    }

    if (NULL == sc_procedure) {
        LOG(FATAL) << "failed to malloc SchemaChange. size=" << sizeof(SchemaChangeWithSorting);
        return OLAP_ERR_MALLOC_ERROR;
    }

    // c. 转换数据
    for (vector<RowsetSharedPtr>::iterator it = old_rowsets->begin();
            it != old_rowsets->end(); ++it) {
        RowsetReaderSharedPtr rowset_reader = (*it)->create_reader();
        rowset_reader->init(nullptr);

        RowsetWriterContextBuilder context_builder;
        RowsetId rowset_id = 0;
        RowsetIdGenerator::instance()->get_next_id(dest_tablet->data_dir(), &rowset_id);
        if ((*it)->is_pending()) {
            PUniqueId load_id;
            load_id.set_hi(0);
            load_id.set_lo(0);
            context_builder.set_rowset_id(rowset_id)
                           .set_tablet_id(dest_tablet->tablet_id())
                           .set_partition_id(dest_tablet->partition_id())
                           .set_tablet_schema_hash(dest_tablet->schema_hash())
                           .set_rowset_type(ALPHA_ROWSET)
                           .set_rowset_path_prefix(dest_tablet->tablet_path())
                           .set_tablet_schema(&(dest_tablet->tablet_schema()))
                           .set_rowset_state(PREPARED)
                           .set_txn_id((*it)->txn_id())
                           .set_load_id(load_id);
        } else {
            context_builder.set_rowset_id(rowset_id)
                           .set_tablet_id(dest_tablet->tablet_id())
                           .set_partition_id(dest_tablet->partition_id())
                           .set_tablet_schema_hash(dest_tablet->schema_hash())
                           .set_rowset_type(ALPHA_ROWSET)
                           .set_rowset_path_prefix(dest_tablet->tablet_path())
                           .set_tablet_schema(&(dest_tablet->tablet_schema()))
                           .set_rowset_state(VISIBLE)
                           .set_version((*it)->version())
                           .set_version_hash((*it)->version_hash());
        }
        RowsetWriterContext context = context_builder.build();
        RowsetWriterSharedPtr rowset_writer(new AlphaRowsetWriter());
        rowset_writer->init(context);

        if (!sc_procedure->process(rowset_reader, rowset_writer, dest_tablet)) {
            if ((*it)->is_pending()) {
                LOG(WARNING) << "failed to process the transaction when schema change. "
                             << "[tablet='" << dest_tablet->full_name() << "'"
                             << " transaction="<< (*it)->txn_id() << "]";
            } else {
                LOG(WARNING) << "failed to process the version. "
                             << "[version='" << (*it)->version().first
                             << "-" << (*it)->version().second << "']";
            }
            rowset_writer->release();
            res = OLAP_ERR_INPUT_PARAMETER_ERROR;
            goto SCHEMA_VERSION_CONVERT_ERR;
        }
        RowsetSharedPtr new_rowset = rowset_writer->build();
        if (new_rowset == nullptr) {
            LOG(WARNING) << "build rowset failed.";
            res = OLAP_ERR_MALLOC_ERROR;
            goto SCHEMA_VERSION_CONVERT_ERR;
        }
        new_rowsets->push_back(new_rowset);
    }

    SAFE_DELETE(sc_procedure);

    return res;

SCHEMA_VERSION_CONVERT_ERR:
    while (!new_rowsets->empty()) {
        RowsetSharedPtr new_rowset = new_rowsets->back();
        new_rowset->remove();
        new_rowsets->pop_back();
    }

    SAFE_DELETE(sc_procedure);
    return res;
}

OLAPStatus SchemaChangeHandler::_get_versions_to_be_changed(
        TabletSharedPtr ref_tablet,
        vector<Version>& versions_to_be_changed) {
    int32_t request_version = 0;
    RowsetSharedPtr rowset = ref_tablet->rowset_with_max_version();
    if (rowset != NULL) {
        request_version = rowset->version().second - 1;
    } else {
        LOG(WARNING) << "Table has no version. path=" << ref_tablet->full_name();
        return OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS;
    }

    // 最新版本的delta可以被重导覆盖，因此计算获取的路径中，
    // 必须包含最新版本的delta
    if (request_version >= 0) {
        vector<Version> span_versions;
        ref_tablet->capture_consistent_versions(Version(0, request_version), &span_versions);

        // get all version list
        vector<VersionEntity> all_versions;
        ref_tablet->list_version_entities(&all_versions);
        if (0 == all_versions.size()) {
            LOG(WARNING) << "there'is no any version in the tablet. tablet=" << ref_tablet->full_name();
            return OLAP_ERR_VERSION_NOT_EXIST;
        }

        for (uint32_t i = 0; i < span_versions.size(); i++) {
            versions_to_be_changed.push_back(span_versions[i]);
        }
    }
    versions_to_be_changed.push_back(
            Version(rowset->version().first, rowset->version().second));

    return OLAP_SUCCESS;
}

// 增加A->(B|C|...) 的schema_change信息
OLAPStatus SchemaChangeHandler::_save_schema_change_info(
        AlterTabletType alter_tablet_type,
        TabletSharedPtr ref_tablet,
        TabletSharedPtr new_tablet,
        const vector<Version>& versions_to_be_changed) {

    // check new tablet exists,
    // prevent to set base's status after new's dropping (clear base's status)
    if (TabletManager::instance()->get_tablet(
            new_tablet->tablet_id(), new_tablet->schema_hash()).get() == NULL) {
        OLAP_LOG_WARNING("fail to find tablet before saving status. [tablet='%s']",
                         new_tablet->full_name().c_str());
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    OLAPStatus res = OLAP_SUCCESS;

    // 1. 在新表和旧表中添加schema change标志
    ref_tablet->clear_schema_change_request();
    ref_tablet->set_schema_change_request(new_tablet->tablet_id(),
                                              new_tablet->schema_hash(),
                                              versions_to_be_changed,
                                              alter_tablet_type);
    new_tablet->set_schema_change_request(ref_tablet->tablet_id(),
                                              ref_tablet->schema_hash(),
                                              vector<Version>(),  // empty versions
                                              alter_tablet_type);

    // save new tablet header :只有一个父ref tablet
    res = new_tablet->save_tablet_meta();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save new tablet header. res=" << res
                   << ", tablet=" << new_tablet->full_name();
        return res;
    }

    res = ref_tablet->save_tablet_meta();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save ref tablet header. res=" << res
                   << ", tablet=" << ref_tablet->full_name().c_str();
        return res;
    }

    return res;
}

// @static
OLAPStatus SchemaChangeHandler::_alter_tablet(SchemaChangeParams* sc_params) {
    OLAPStatus res = OLAP_SUCCESS;
    LOG(INFO) << "begin to process alter tablet job. "
              << "old_tablet=" << sc_params->ref_tablet->full_name()
              << ", new_tablet=" << sc_params->new_tablet->full_name();

    // find end version
    int32_t end_version = -1;
    for (size_t i = 0; i < sc_params->ref_rowset_readers.size(); ++i) {
        if (sc_params->ref_rowset_readers[i]->version().second > end_version) {
            end_version = sc_params->ref_rowset_readers[i]->version().second;
        }
    }

    // change中增加了filter信息，在_parse_request中会设置filter的column信息
    // 并在每次row block的change时，过滤一些数据
    RowBlockChanger rb_changer(sc_params->new_tablet->tablet_schema(),
                               sc_params->ref_tablet,
                               sc_params->delete_handler);

    bool sc_sorting = false;
    bool sc_directly = false;
    SchemaChange* sc_procedure = NULL;

    // a. 解析Alter请求，转换成内部的表示形式
    res = _parse_request(sc_params->ref_tablet,
                         sc_params->new_tablet,
                         &rb_changer,
                         &sc_sorting,
                         &sc_directly);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to parse the request. [res=%d]", res);
        goto PROCESS_ALTER_EXIT;
    }

    // b. 生成历史数据转换器
    if (true == sc_sorting) {
        size_t memory_limitation = config::memory_limitation_per_thread_for_schema_change;
        LOG(INFO) << "doing schema change with sorting.";
        sc_procedure = new(nothrow) SchemaChangeWithSorting(
                               sc_params->new_tablet,
                               rb_changer,
                               memory_limitation * 1024 * 1024 * 1024);
    } else if (true == sc_directly) {
        LOG(INFO) << "doing schema change directly.";
        sc_procedure = new(nothrow) SchemaChangeDirectly(
                sc_params->new_tablet, rb_changer);
    } else {
        LOG(INFO) << "doing linked schema change.";
        sc_procedure = new(nothrow) LinkedSchemaChange(
                                sc_params->ref_tablet,
                                sc_params->new_tablet);
    }

    if (NULL == sc_procedure) {
        OLAP_LOG_WARNING("failed to malloc SchemaChange. [size=%ld]",
                         sizeof(SchemaChangeWithSorting));
        res = OLAP_ERR_MALLOC_ERROR;
        goto PROCESS_ALTER_EXIT;
    }

    // c. 转换历史数据
    for (vector<RowsetReaderSharedPtr>::iterator it = sc_params->ref_rowset_readers.end() - 1;
            it >= sc_params->ref_rowset_readers.begin(); --it) {
        VLOG(10) << "begin to convert a history delta. "
                 << "version=" << (*it)->version().first << "-" << (*it)->version().second;

        // set status for monitor
        // 只要有一个new_table为running，ref table就设置为running
        // NOTE 如果第一个sub_table先fail，这里会继续按正常走
        sc_params->ref_tablet->set_schema_change_status(
                ALTER_TABLE_RUNNING,
                sc_params->new_tablet->schema_hash(),
                -1);
        sc_params->new_tablet->set_schema_change_status(
                ALTER_TABLE_RUNNING,
                sc_params->ref_tablet->schema_hash(),
                (*it)->version().second);

        RowsetId rowset_id = 0;
        TabletSharedPtr new_tablet = sc_params->new_tablet;
        res = RowsetIdGenerator::instance()->get_next_id(sc_params->new_tablet->data_dir(), &rowset_id);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "generate next id failed";
            goto PROCESS_ALTER_EXIT;
        }

        RowsetWriterContextBuilder context_builder;
        context_builder.set_rowset_id(rowset_id)
                       .set_tablet_id(new_tablet->tablet_id())
                       .set_tablet_schema_hash(new_tablet->schema_hash())
                       .set_partition_id(new_tablet->partition_id())
                       .set_rowset_type(ALPHA_ROWSET)
                       .set_rowset_path_prefix(new_tablet->tablet_path())
                       .set_tablet_schema(&(new_tablet->tablet_schema()))
                       .set_rowset_state(VISIBLE)
                       .set_version((*it)->version())
                       .set_version_hash((*it)->version_hash());
        RowsetWriterContext builder_context = context_builder.build();

        RowsetWriterSharedPtr rowset_writer(new AlphaRowsetWriter());
        OLAPStatus status = rowset_writer->init(builder_context);
        if (status != OLAP_SUCCESS) {
            res = OLAP_ERR_ROWSET_BUILDER_INIT;
            goto PROCESS_ALTER_EXIT;
        }

        if (!sc_procedure->process(*it, rowset_writer, sc_params->new_tablet)) {
            LOG(WARNING) << "failed to process the version. "
                         << " version=" << (*it)->version().first
                         << "-" << (*it)->version().second;
            rowset_writer->release();

            res = OLAP_ERR_INPUT_PARAMETER_ERROR;
            goto PROCESS_ALTER_EXIT;
        }

        // 将新版本的数据加入header
        // 为了防止死锁的出现，一定要先锁住旧表，再锁住新表
        sc_params->new_tablet->obtain_push_lock();
        sc_params->ref_tablet->obtain_header_wrlock();
        sc_params->new_tablet->obtain_header_wrlock();

        if (!sc_params->new_tablet->has_version((*it)->version())) {
            // register version
            RowsetSharedPtr new_rowset = rowset_writer->build();
            res = sc_params->new_tablet->add_rowset(new_rowset);
            if (OLAP_SUCCESS != res) {
                LOG(WARNING) << "failed to register new version. "
                             << " tablet='%s'" << sc_params->new_tablet->full_name()
                             << " version='" << (*it)->version().first
                             << "-" << (*it)->version().second << "'";
                new_rowset->remove();

                sc_params->new_tablet->release_header_lock();
                sc_params->ref_tablet->release_header_lock();

                goto PROCESS_ALTER_EXIT;
            }

            VLOG(3) << "register new version. tablet=" << sc_params->new_tablet->full_name()
                    << ", version=" << (*it)->version().first << "-" << (*it)->version().second;
        } else {
            LOG(WARNING) << "version already exist, version revert occured. "
                         << "[tablet=" << sc_params->new_tablet->full_name()
                         << " version='" << (*it)->version().first
                         << "-" << (*it)->version().second << "']";
            rowset_writer->release();
        }

        // 保存header
        if (OLAP_SUCCESS != sc_params->new_tablet->save_tablet_meta()) {
            LOG(FATAL) << "fail to save header. res=" << res
                       << ", tablet=" << sc_params->new_tablet->full_name();
        }

        // XXX: 此处需要验证ref_rowset_readers中最后一个版本是否与new_tablet的header中记录的最
        //      后一个版本相同。然后还要注意一致性问题。
        if (!sc_params->ref_tablet->remove_last_schema_change_version(
                    sc_params->new_tablet)) {
            LOG(WARNING) << "failed to remove the last version did schema change.";

            sc_params->new_tablet->release_header_lock();
            sc_params->ref_tablet->release_header_lock();

            res = OLAP_ERR_INPUT_PARAMETER_ERROR;
            goto PROCESS_ALTER_EXIT;
        }

        // 保存header
        if (OLAP_SUCCESS != sc_params->ref_tablet->save_tablet_meta()) {
            LOG(FATAL) << "failed to save header. tablet=" << sc_params->new_tablet->full_name();
        }

        sc_params->new_tablet->release_header_lock();
        sc_params->ref_tablet->release_header_lock();
        sc_params->new_tablet->release_push_lock();

        VLOG(10) << "succeed to convert a history version."
            << ", version=" << (*it)->version().first << "-" << (*it)->version().second;

        // 释放RowsetReader
        (*it)->close();
    }

    // XXX: 此时应该不取消SchemaChange状态，因为新Delta还要转换成新旧Schema的版本

PROCESS_ALTER_EXIT:
    if (res == OLAP_SUCCESS) {
        Version test_version(0, end_version);
        res = sc_params->new_tablet->test_version(test_version);
    }

    if (res == OLAP_SUCCESS) {
        // ref的状态只有2个new table都完成后，才能设置为done
        res = sc_params->ref_tablet->clear_schema_change_info(NULL, false, true);

        if (OLAP_SUCCESS == res) {
            sc_params->ref_tablet->set_schema_change_status(
                    ALTER_TABLE_FINISHED,
                    sc_params->new_tablet->schema_hash(),
                    -1);
        } else {
            res = OLAP_SUCCESS;
        }

        sc_params->new_tablet->set_schema_change_status(
                ALTER_TABLE_FINISHED,
                sc_params->ref_tablet->schema_hash(),
                -1);
        VLOG(3) << "set alter tablet job status. "
                << "status=" << sc_params->ref_tablet->schema_change_status().status;
    } else {
        sc_params->ref_tablet->set_schema_change_status(
                ALTER_TABLE_FAILED,
                sc_params->new_tablet->schema_hash(),
                -1);

        sc_params->new_tablet->set_schema_change_status(
                ALTER_TABLE_FAILED,
                sc_params->ref_tablet->schema_hash(),
                -1);
        VLOG(3) << "set alter tablet job status. "
                << "status=" << sc_params->ref_tablet->schema_change_status().status;
    }

    for (auto& rs_reader : sc_params->ref_rowset_readers) {
        rs_reader->close();
    }
    SAFE_DELETE(sc_procedure);

    LOG(INFO) << "finish to process alter tablet job. res=" << res;
    return res;
}

// @static
// 分析column的mapping以及filter key的mapping
OLAPStatus SchemaChangeHandler::_parse_request(TabletSharedPtr ref_tablet,
                                               TabletSharedPtr new_tablet,
                                               RowBlockChanger* rb_changer,
                                               bool* sc_sorting,
                                               bool* sc_directly) {
    OLAPStatus res = OLAP_SUCCESS;

    // set column mapping
    for (int i = 0, new_schema_size = new_tablet->tablet_schema().num_columns();
            i < new_schema_size; ++i) {
        const TabletColumn& new_column = new_tablet->tablet_schema().column(i);
        const string& column_name = new_column.name();
        ColumnMapping* column_mapping = rb_changer->get_mutable_column_mapping(i);

        if (new_column.has_reference_column()) {
            int32_t column_index = ref_tablet->field_index(new_column.referenced_column());

            if (column_index < 0) {
                LOG(WARNING) << "referenced column was missing. "
                             << "[column=" << column_name
                             << " referenced_column=" << column_index << "]";
                return OLAP_ERR_CE_CMD_PARAMS_ERROR;
            }

            column_mapping->ref_column = column_index;
            VLOG(3) << "A column refered to existed column will be added after schema changing."
                    << "column=" << column_name << ", ref_column=" << column_index;
            continue;
        }

        int32_t column_index = ref_tablet->field_index(column_name);
        if (column_index >= 0) {
            column_mapping->ref_column = column_index;
            continue;
        }

        // 新加列走这里
        //if (new_column_schema.is_allow_null || new_column_schema.has_default_value) {
        {
            column_mapping->ref_column = -1;

            if (i < ref_tablet->num_short_key_columns()) {
                *sc_directly = true;
            }

            if (OLAP_SUCCESS != (res = _init_column_mapping(
                                         column_mapping,
                                         new_column,
                                         new_column.default_value()))) {
                return res;
            }

            VLOG(10) << "A column with default value will be added after schema chaning. "
                     << "column=" << column_name
                     << ", default_value=" << new_column.default_value();
            continue;
        }


        // XXX: 只有DROP COLUMN时，遇到新Schema转旧Schema时会进入这里。
        column_mapping->ref_column = -1;

        if (OLAP_SUCCESS != (res = _init_column_mapping(
                                       column_mapping,
                                       new_column,
                                       ""))) {
            return res;
        }

        VLOG(3) << "A new schema delta is converted while droping column. "
                << "Droped column will be assigned as '0' for the older schema. "
                << "column=" << column_name;
    }

    // Check if re-aggregation is needed.
    *sc_sorting = false;
    // 若Key列的引用序列出现乱序，则需要重排序
    int num_default_value = 0;

    for (int i = 0, new_schema_size = new_tablet->num_key_columns();
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

    if (ref_tablet->num_short_key_columns() != new_tablet->num_short_key_columns()) {
        // the number of short_keys changed, can't do linked schema change
        *sc_directly = true;
        return OLAP_SUCCESS;
    }

    const TabletSchema& ref_tablet_schema = ref_tablet->tablet_schema();
    const TabletSchema& new_tablet_schema = new_tablet->tablet_schema();
    for (size_t i = 0; i < new_tablet->num_columns(); ++i) {
        ColumnMapping* column_mapping = rb_changer->get_mutable_column_mapping(i);
        if (column_mapping->ref_column < 0) {
            continue;
        } else {
            if (new_tablet_schema.column(i).type() != ref_tablet_schema.column(column_mapping->ref_column).type()) {
                *sc_directly = true;
                return OLAP_SUCCESS;
            } else if (
                (new_tablet_schema.column(i).type() == ref_tablet_schema.column(column_mapping->ref_column).type())
                    && (new_tablet_schema.column(i).length()
                        != ref_tablet_schema.column(column_mapping->ref_column).length())) {
                *sc_directly = true;
                return OLAP_SUCCESS;

            } else if (new_tablet_schema.column(i).is_bf_column() != ref_tablet_schema.column(i).is_bf_column()) {
                *sc_directly = true;
                return OLAP_SUCCESS;
            }
        }
    }

    if (ref_tablet->delete_data_conditions_size() != 0){
        //there exists delete condtion in header, can't do linked schema change
        *sc_directly = true;
    }

    if (ref_tablet->data_file_type() != new_tablet->data_file_type()) {
        //if change the tablet from row-oriented to column-oriented, or versus 
        *sc_directly = true;
    }

    return OLAP_SUCCESS;
}

OLAPStatus SchemaChangeHandler::_init_column_mapping(ColumnMapping* column_mapping,
                                                     const TabletColumn& column_schema,
                                                     const std::string& value) {
    column_mapping->default_value = WrapperField::create(column_schema);

    if (column_mapping->default_value == NULL) {
        return OLAP_ERR_MALLOC_ERROR;
    }

    if (true == column_schema.is_nullable() && value.length() == 0) {
        column_mapping->default_value->set_null();
    } else {
        column_mapping->default_value->from_string(value);
    }

    return OLAP_SUCCESS;
}

}  // namespace doris
