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

#include "olap/olap_data.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

// #include <ul_string.h>

#include "olap/olap_engine.h"
#include "olap/olap_index.h"
#include "olap/olap_table.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/utils.h"
#include "runtime/runtime_state.h"
#include "runtime/mem_tracker.h"
#include "util/mem_util.hpp"

using std::exception;
using std::lower_bound;
using std::nothrow;
using std::string;
using std::upper_bound;
using std::vector;

namespace palo {

OLAPData::OLAPData(OLAPIndex* index) :
        IData(OLAP_DATA_FILE, index),
        _olap_table(NULL),
        _is_pickled(true),
        _session_status(NULL),
        _row_block_broker(NULL),
        _write_descriptor(NULL) {
    _olap_table = index->table();
}

OLAPData::~OLAPData() {
    pickle();

    SAFE_DELETE(_session_status);

    // Close file to release resources
    if (_write_descriptor) {
        SAFE_DELETE_ARRAY(_write_descriptor->packed_buffer);

        // File would be closed in deconstruction function
        SAFE_DELETE(_write_descriptor);
    }
}

OLAPStatus OLAPData::init() {
    return unpickle();
}

void OLAPData::set_conjuncts(std::vector<ExprContext*>* query_conjuncts, 
                             std::vector<ExprContext*>* delete_conjuncts) {
    _row_block_broker->set_conjuncts(query_conjuncts, delete_conjuncts);
}

OLAPStatus OLAPData::get_first_row_block(RowBlock** row_block,
                                     const char** packed_row_block,
                                     uint32_t* packed_row_block_size) {
    OLAPStatus res = OLAP_SUCCESS;
    (row_block == NULL || ((*row_block) = NULL));

    set_eof(false);
    
    if (!_row_block_broker) {
        OLAP_LOG_FATAL("using pickled olap data is forbidden.");
        return OLAP_ERR_NOT_INITED;
    }

    RowBlockPosition row_block_pos;
    res = olap_index()->find_first_row_block(&row_block_pos);
    if (res == OLAP_ERR_INDEX_EOF) {
        // 为了防止出错，这里还是为rowBlock设下NULL，否则当前一个block被析构，
        // 下一个版本恰好又是NULL，这个指针就可能变成野指针。
        (row_block == NULL || ((*row_block) = NULL));
        set_eof(true);
        return res;
    } else if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Fail to find first row block with OLAPIndex.");
        return res;
    }

    res = _row_block_broker->change_to(row_block_pos, _profile);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Fail to get row block. "
                         "[segment=%d, block_size=%d, data_offset=%d, index_offset=%d]",
                         row_block_pos.segment,
                         row_block_pos.block_size,
                         row_block_pos.data_offset,
                         row_block_pos.index_offset);
        _check_io_error(res);
        return res;
    }

    (row_block == NULL || (*row_block = _row_block_broker->row_block()));
    (packed_row_block == NULL || (*packed_row_block = _row_block_broker->packed_row_block()));
    (packed_row_block_size == NULL
         || (*packed_row_block_size = _row_block_broker->packed_row_block_size()));

    return res;
}

OLAPStatus OLAPData::get_next_row_block(RowBlock** row_block,
                                    const char** packed_row_block,
                                    uint32_t* packed_row_block_size) {
    OLAPStatus res = OLAP_SUCCESS;
    set_eof(false);

    if (!_row_block_broker) {
        OLAP_LOG_FATAL("using pickled olap data is forbidden.");
        return OLAP_ERR_NOT_INITED;
    }

    RowBlockPosition row_block_pos = _row_block_broker->position();
    res = olap_index()->find_next_row_block(&row_block_pos, eof_ptr());
    if (eof()) {
        OLAP_LOG_DEBUG("Got EOF from OLAPIndex. [segment=%d, data_offset=%d]",
                       row_block_pos.segment,
                       row_block_pos.data_offset);
        // 当到达eof的时候不需要把结果带出来
        (row_block == NULL || (*row_block = NULL));
        (packed_row_block == NULL || (*packed_row_block = NULL));
        (packed_row_block_size == NULL || (*packed_row_block_size = 0));

        return res;
    }

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Fail to get next row block. "
                         "[res=%d, segment=%d, data_offset=%d, index_offset=%d]",
                         res,
                         row_block_pos.segment,
                         row_block_pos.data_offset,
                         row_block_pos.index_offset);
        return res;
    }

    // 块位置越界有两种情况
    // 1 block_pos大于end_block_pos
    // 2 block_pos等于end_block_pos，但end_row_index为0，即end_key在
    // 某个block的首行
    if (_row_block_broker->get_set_end_row_flag()
            && (row_block_pos > _row_block_broker->end_block_position()
                    || (_row_block_broker->end_row_index() == 0
                            && row_block_pos == _row_block_broker->end_block_position()))) {
        set_eof(true);
        OLAP_LOG_TRACE("Over the end row block. [segment=%d, data_offset=%d, index_offset=%d]",
                       row_block_pos.segment,
                       row_block_pos.data_offset,
                       row_block_pos.index_offset);

        return OLAP_ERR_INDEX_EOF;
    }
    res = _row_block_broker->change_to(row_block_pos, _profile);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Fail to get row block. "
                         "[segment=%d, block_size=%d, data_offset=%d, index_offset=%d]",
                         row_block_pos.segment,
                         row_block_pos.block_size,
                         row_block_pos.data_offset,
                         row_block_pos.index_offset);

        _check_io_error(res);
        return res;
    }

    (row_block == NULL || (*row_block = _row_block_broker->row_block()));
    (packed_row_block == NULL || (*packed_row_block = _row_block_broker->packed_row_block()));
    (packed_row_block_size == NULL
         || (*packed_row_block_size = _row_block_broker->packed_row_block_size()));

    return res;
}

OLAPStatus OLAPData::get_next_row_block(RowBlock** row_block) {
    return get_next_row_block(row_block, NULL, NULL);
}

OLAPStatus OLAPData::get_first_row_block(RowBlock** row_block) {
    return get_first_row_block(row_block, NULL, NULL);
}

RowBlock* OLAPData::seek_and_get_row_block(const RowBlockPosition& position) {
    set_eof(false);

    if (!_row_block_broker) {
        OLAP_LOG_FATAL("using pickled OLAPData is forbidden.");
        return NULL;
    }

    OLAPStatus res = OLAP_SUCCESS;
    if ((res = _row_block_broker->change_to(position, _profile)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Fail to get row block. "
                         "[segment=%d, block_size=%d, data_offset=%d, index_offset=%d]",
                         position.segment,
                         position.block_size,
                         position.data_offset,
                         position.index_offset);

        _check_io_error(res);
        return NULL;
    }

    return _row_block_broker->row_block();
}

const RowCursor* OLAPData::get_first_row() {
    set_eof(false);

    if (!_row_block_broker) {
        OLAP_LOG_FATAL("using pickled olap data is forbidden.");
        return NULL;
    }

    RowBlockPosition row_block_pos;
    OLAPStatus res = olap_index()->find_first_row_block(&row_block_pos);
    if (res == OLAP_ERR_INDEX_EOF) {
        set_eof(true);
        return NULL;
    } else if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Fail to find first row block with OLAPIndex.");
        return NULL;
    }

    OLAP_LOG_DEBUG("RowBlockPosition='%s'", row_block_pos.to_string().c_str());

    if ((res = _row_block_broker->change_to(row_block_pos, _profile)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Fail to get row block. "
                         "[segment=%d, block_size=%d, data_offset=%d, index_offset=%d]",
                         row_block_pos.segment,
                         row_block_pos.block_size,
                         row_block_pos.data_offset,
                         row_block_pos.index_offset);

        _check_io_error(res);
        return NULL;
    }

    return _row_block_broker->first();
}

const RowCursor* OLAPData::get_current_row() {
    set_eof(false);

    if (!_row_block_broker) {
        OLAP_LOG_FATAL("using pickled OLAPData is forbidden.");
        return NULL;
    }

    // 这里没有强限制,必须在调用其他的获取get row的方法之后才能使用此方法
    return _row_block_broker->current();
}

const RowCursor* OLAPData::get_next_row() {
    set_eof(false);

    if (!_row_block_broker) {
        OLAP_LOG_FATAL("using pickled olap data is forbidden.");
        return NULL;
    }

    bool end_of_row_block = false;
    const RowCursor* row_cursor = _row_block_broker->next(&end_of_row_block);
    if (row_cursor) {
        return row_cursor;
    }

    if (end_of_row_block) {
        // get_next_row_block传了三个NULL,不需要其返回结果
        if (get_next_row_block(NULL, NULL, NULL) != OLAP_SUCCESS && eof() == false) {
            OLAP_LOG_WARNING("fail to get next row block.");
            return NULL;
        } else if (eof() == true){
            OLAP_LOG_DEBUG("get next row block got eof.");
            return NULL;
        }

        if (_row_block_broker != NULL) {
            return _row_block_broker->first();
        }
    }

    return NULL;
}

const RowCursor* OLAPData::find_row(const RowCursor& key, bool find_last_key, bool is_end_key) {
    set_eof(false);

    if (!_row_block_broker) {
        OLAP_LOG_FATAL("using pickled OLAPData is forbidden.");
        return NULL;
    }

    OlapStopWatch time_watch;

    OLAPStatus res = OLAP_SUCCESS;
    RowCursor helper_cursor;
    if (helper_cursor.init(olap_index()->short_key_fields()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Init helper_cursor fail.");
        return NULL;
    }

    // 取得RowBlock范围的起点和终点，在上面做二分
    // 若find_last_key==false, index找到最后一个<key的block,
    // 或者说是第一个可能包含>=key的block, 即二分的左边界
    // 若find_last_key==true, index找到第一个包含>key的block,
    // 或者说是最后一个可能包含>=key的block, 即二分的右边界
    // 二分完成后，first_pos与last_pos指向同一个RowBlock
    RowBlockPosition start_position;
    RowBlockPosition end_position;
    res = olap_index()->find_row_block(key, &helper_cursor, false,  &start_position);
    if (res == OLAP_ERR_INDEX_EOF) {
        set_eof(true);
        return NULL;
    } else if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Find row block failed. [res=%d]", res);
        return NULL;
    }

    res = olap_index()->find_row_block(key, &helper_cursor, true, &end_position);
    if (res == OLAP_ERR_INDEX_EOF) {
        set_eof(true);
        return NULL;
    } else if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("find row block failed. [res=%d]", res);
        return NULL;
    }

    if (key.field_count() > _olap_table->num_short_key_fields()) {
        // helper rowcursor for OLAPDataComparator
        RowCursor data_helper_cursor;
        if ((res = data_helper_cursor.init(_olap_table->tablet_schema())) != OLAP_SUCCESS) {
            OLAP_LOG_FATAL("fail to init row cursor. [res=%d]", res);
            return NULL;
        }

        // 调整start_position
        uint32_t distance = olap_index()->compute_distance(start_position, end_position);
        BinarySearchIterator it_start(0u);
        BinarySearchIterator it_end(distance + 1);
        BinarySearchIterator it_result(0u);
        OLAPDataComparator comparator(start_position, 
                                      this, 
                                      olap_index(), 
                                      &data_helper_cursor);

        try {
            if (!find_last_key) {
                it_result = lower_bound(it_start, it_end, key, comparator);
            } else {
                it_result = upper_bound(it_start, it_end, key, comparator);
            }

            OLAP_LOG_DEBUG("get result iterator. [offset=%u start_pos='%s']",
                           *it_result,
                           start_position.to_string().c_str());
        } catch (exception& e) {
            OLAP_LOG_FATAL("exception happens when doing seek. [e.what='%s']", e.what());
            return NULL;
        }

        // 如果没有找到就肯定没有, 并且一定是eof了
        // 如果找到了, 那么找到的block里一定有满足条件的记录
        if (*it_result == *it_end) {
            OLAP_LOG_DEBUG("key isnot in current data file.");
            set_eof(true);
            return NULL;
        }

        // 设置_position
        res = olap_index()->advance_row_block(*it_result, &start_position);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to advance row block. "
                             "[res=%d start_position='%s' offset=%u]",
                             res, start_position.to_string().c_str(), *it_result);
            return NULL;
        }
    }

    bool eof = false;
    bool data_eof = false;
    const RowCursor* row_cursor = NULL;

    while (end_position >= start_position && !data_eof) {
        // 根据pos取到对应的row_block
        if ((res = _row_block_broker->change_to(start_position, _profile)) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("Fail to get row block. "
                             "[segment=%d, block_size=%d, data_offset=%d, index_offset=%d]",
                             start_position.segment,
                             start_position.block_size,
                             start_position.data_offset,
                             start_position.index_offset);

            _check_io_error(res);
            return NULL;
        }

        // eof代表这一块找完了，仍然没有发现key，但也可能是找到了endkey，也就是说
        // 这个数据中没有需要的key。
        row_cursor = _row_block_broker->find_row(key, find_last_key, &eof);
        if (row_cursor != NULL || (eof && _row_block_broker->is_end_block())) {
            break;
        }

        res = olap_index()->find_next_row_block(&start_position, &data_eof);
        if (res != OLAP_SUCCESS) {
            break;
        }
    }

    OLAPNoticeInfo::add_seek_count();
    OLAPNoticeInfo::add_seek_time_us(time_watch.get_elapse_time_us());

    if (row_cursor) {
        set_eof(false);
        return row_cursor;
    } else if (eof || data_eof) {
        // 此处找不到，是由于设置了end_key，超找超过了end_key对应行
        OLAP_LOG_TRACE("key can't be found, Search over end_key![key=%s]", key.to_string().c_str());
        set_eof(true);
        return NULL;
    } else {
        // 走到这个分支只能说明前面的某个地方有bug
        OLAP_LOG_FATAL("find null row but data isnot eof! [res=%d]", res);
        return NULL;
    }
}

OLAPStatus OLAPData::pickle() {
    if (_is_pickled) {
        return OLAP_SUCCESS;
    }

    if (_row_block_broker != NULL) {
        // 保存现场
        if (_row_block_broker->row_block() != NULL) {
            if (_session_status == NULL) {
                _session_status = new(nothrow) SessionStatus();
                if (_session_status == NULL) {
                    OLAP_LOG_FATAL("fail to malloc SessionStatus. [size=%ld]",
                                   sizeof(SessionStatus));
                    return OLAP_ERR_MALLOC_ERROR;
                }
            }

            _session_status->position = _row_block_broker->position();
            _session_status->row_index = _row_block_broker->row_index();
            _session_status->end_block_position = _row_block_broker->end_block_position();
            _session_status->end_row_index = _row_block_broker->end_row_index();
            _session_status->is_set_end_row = _row_block_broker->get_set_end_row_flag();
        }

        SAFE_DELETE(_row_block_broker);
    }

    _is_pickled = true;
    return OLAP_SUCCESS;
}

OLAPStatus OLAPData::unpickle() {
    OLAPStatus res = OLAP_SUCCESS;

    if (!_is_pickled) {
        return OLAP_SUCCESS;
    }

    _row_block_broker = new(nothrow) RowBlockBroker(_olap_table, olap_index(), _runtime_state);
    if (_row_block_broker == NULL) {
        OLAP_LOG_FATAL("fail to malloc RowBlockBroker. [size=%ld]", sizeof(RowBlockBroker));
        return OLAP_ERR_MALLOC_ERROR;
    }

    res = _row_block_broker->init();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_FATAL("fail to init RowBlockBroker. [res=%d]", res);
        SAFE_DELETE(_row_block_broker);
        return OLAP_ERR_INIT_FAILED;
    }

    // 恢复现场
    if (_session_status != NULL) {
        _row_block_broker->set_end_row(_session_status->end_block_position,
                                       _session_status->end_row_index);
        _row_block_broker->set_end_row_flag(_session_status->is_set_end_row);

        res = _row_block_broker->change_to(_session_status->position, _profile);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to get row block. "
                             "[res=%d segment=%d block_size=%d data_offset=%d index_offset=%d]",
                             res,
                             _session_status->position.segment,
                             _session_status->position.block_size,
                             _session_status->position.data_offset,
                             _session_status->position.index_offset);

            SAFE_DELETE(_row_block_broker);
            _check_io_error(res);
            return OLAP_ERR_DATA_ROW_BLOCK_ERROR;
        }

        _row_block_broker->get_row(_session_status->row_index);
    }

    _is_pickled = false;
    return OLAP_SUCCESS;
}

OLAPStatus OLAPData::add_segment() {
    // 在这里没做是否封口(finalize_segment)的检查,如果没有封口再调用add_segment会有问题
    OLAPStatus res = OLAP_SUCCESS;
    string file_name;
    OLAPDataHeaderMessage* data_header = NULL;

    if (_write_descriptor == NULL) {
        if ((_write_descriptor = new(nothrow) WriteDescriptor()) == NULL) {
            OLAP_LOG_FATAL("fail to malloc FileHandler. [size=%ld]", sizeof(FileHandler));
            res = OLAP_ERR_MALLOC_ERROR;
            goto ADD_SEGMENT_ERR;
        }

        _write_descriptor->packed_buffer =
                new(nothrow) char[OLAP_DEFAULT_MAX_PACKED_ROW_BLOCK_SIZE];
        if (_write_descriptor->packed_buffer == NULL) {
            OLAP_LOG_FATAL("fail to malloc write buffer. [size=%ld]",
                           sizeof(OLAP_DEFAULT_MAX_PACKED_ROW_BLOCK_SIZE));
            res = OLAP_ERR_MALLOC_ERROR;
            goto ADD_SEGMENT_ERR;
        }

        memset(_write_descriptor->packed_buffer, 0, OLAP_DEFAULT_MAX_PACKED_ROW_BLOCK_SIZE);
        _write_descriptor->segment = 0;
    } else {
        ++_write_descriptor->segment;
    }

    data_header = _write_descriptor->file_header.mutable_message();
    data_header->set_segment(_write_descriptor->segment);

    // file for new segment
    file_name = _olap_table->construct_data_file_path(olap_index()->version(),
                                                      olap_index()->version_hash(),
                                                      _write_descriptor->segment);
    res = _write_descriptor->file_handle.open_with_mode(
            file_name, O_CREAT | O_EXCL | O_WRONLY , S_IRUSR | S_IWUSR);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Fail to open file. [file_name=%s]", file_name.c_str());
        goto ADD_SEGMENT_ERR;
    }

    // 准备FileHeader
    res = _write_descriptor->file_header.prepare(&(_write_descriptor->file_handle));
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_FATAL("write file header error. [res=%d err=%m]", res);
        goto ADD_SEGMENT_ERR;
    }

    // 跳过FileHeader
    if (_write_descriptor->file_handle.seek(_write_descriptor->file_header.size(),
                                            SEEK_SET) == -1) {
        OLAP_LOG_FATAL("lseek header file error. [err=%m]");
        res = OLAP_ERR_IO_ERROR;
        goto ADD_SEGMENT_ERR;
    }

    // 初始化checksum
    _write_descriptor->checksum = CRC32_INIT;
    return res;

ADD_SEGMENT_ERR:
    if (_write_descriptor) {
        if (_write_descriptor->packed_buffer) {
            SAFE_DELETE_ARRAY(_write_descriptor->packed_buffer);
        }

        SAFE_DELETE(_write_descriptor);
        _write_descriptor = NULL;
    }

    _check_io_error(res);

    return res;
}

OLAPStatus OLAPData::add_packed_row_block(const RowBlock* row_block,
                                      const char* packed_row_block,
                                      uint32_t packed_row_block_size,
                                      uint32_t* start_data_offset,
                                      uint32_t* end_data_offset) {
    if (!_write_descriptor) {
        OLAP_LOG_WARNING("segment should be added before.");
        return OLAP_ERR_NOT_INITED;
    }

    OLAPStatus res = OLAP_SUCCESS;
    RowBlockHeaderV2 row_block_header;

    memory_copy(_write_descriptor->packed_buffer, packed_row_block, packed_row_block_size);

    RowBlockInfo rb_info = row_block->row_block_info();
    // 返回RowBlock起始位置的Offset
    off_t offset = _write_descriptor->file_handle.tell();
    if (offset == -1) {
        OLAP_LOG_WARNING("fail to tell file. [err=%m]");
        res = OLAP_ERR_IO_ERROR;
        goto ADD_PACKED_ROW_BLOCK_ERROR;
    }

    (start_data_offset == NULL || (*start_data_offset = static_cast<uint32_t>(offset)));

    // 更新RowBlockHeader
    row_block_header.packed_len = static_cast<uint32_t>(packed_row_block_size);
    row_block_header.num_rows = rb_info.row_num;
    row_block_header.checksum = rb_info.checksum;
    //新增内容，包括魔数，版本和未打包内容的大小，便于后续解压
    row_block_header.magic_num = 0;
    row_block_header.version = 1;
    row_block_header.unpacked_len = row_block->used_buf_len();
    
    res = _write_descriptor->file_handle.write(&row_block_header, sizeof(row_block_header));
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to dump row block header. [size=%lu]", sizeof(row_block_header));
        goto ADD_PACKED_ROW_BLOCK_ERROR;
    }
    
    res = _write_descriptor->file_handle.write(_write_descriptor->packed_buffer,
                                               packed_row_block_size);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to dump row block data. [size=%u]", packed_row_block_size);
        goto ADD_PACKED_ROW_BLOCK_ERROR;
    }

    // 更新SegmentHeader的校验码，计算RowBlock数据部分
    _write_descriptor->checksum = olap_crc32(_write_descriptor->checksum,
                                             _write_descriptor->packed_buffer,
                                             packed_row_block_size);
    // 返回RowBlock结束位置的Offset
    offset = _write_descriptor->file_handle.tell();
    if (offset == -1) {
        res = OLAP_ERR_IO_ERROR;
        goto ADD_PACKED_ROW_BLOCK_ERROR;
    }

    (end_data_offset == NULL || (*end_data_offset = static_cast<uint32_t>(offset)));

    return OLAP_SUCCESS;

ADD_PACKED_ROW_BLOCK_ERROR:
    _check_io_error(res);

    return res;
}

OLAPStatus OLAPData::add_row_block(const RowBlock& row_block,
                               uint32_t* start_data_offset,
                               uint32_t* end_data_offset) {
    if (!_write_descriptor) {
        OLAP_LOG_WARNING("segment should be added before.");
        return OLAP_ERR_NOT_INITED;
    }

    OLAPStatus res = OLAP_SUCCESS;
    RowBlockHeaderV2 row_block_header;
    size_t packed_size = 0;
    RowBlockInfo rb_info = row_block.row_block_info();

    // 返回RowBlock起始位置的Offset
    off_t offset = _write_descriptor->file_handle.tell();
    if (offset == -1) {
        res = OLAP_ERR_IO_ERROR;
        goto ADD_ROW_BLOCK_ERROR;
    }

    (start_data_offset == NULL || (*start_data_offset = static_cast<uint32_t>(offset)));

    // 使用LZO1C-99压缩RowBlock
    if (row_block.compress(_write_descriptor->packed_buffer,
                           OLAP_DEFAULT_MAX_PACKED_ROW_BLOCK_SIZE,
                           &packed_size,
                           OLAP_COMP_STORAGE) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Fail to compress row block.");
        return OLAP_ERR_COMPRESS_ERROR;
    }

    // 更新RowBlockHeader
    row_block_header.packed_len = static_cast<uint32_t>(packed_size);
    row_block_header.num_rows = rb_info.row_num;
    row_block_header.checksum = rb_info.checksum;

    // 新增
    row_block_header.magic_num = 0;
    row_block_header.version = 1;
    row_block_header.unpacked_len = row_block.used_buf_len();
    
    res = _write_descriptor->file_handle.write(&row_block_header, sizeof(row_block_header));
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Fail to dump row block header. [size=%lu]", sizeof(row_block_header));
        return res;
    }
    
    res = _write_descriptor->file_handle.write(_write_descriptor->packed_buffer, packed_size);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Fail to dump row block data. [size=%lu]", packed_size);
        return res;
    }

    // 更新SegmentHeader的校验码，计算RowBlock数据部分
    _write_descriptor->checksum = olap_crc32(_write_descriptor->checksum,
                                             _write_descriptor->packed_buffer,
                                             packed_size);
    // 返回RowBlock结束位置的Offset
    offset = _write_descriptor->file_handle.tell();
    if (offset == -1) {
        res = OLAP_ERR_IO_ERROR;
        goto ADD_ROW_BLOCK_ERROR;
    }

    (end_data_offset == NULL || (*end_data_offset = static_cast<uint32_t>(offset)));

    return OLAP_SUCCESS;

ADD_ROW_BLOCK_ERROR:
    _check_io_error(res);

    return res;
}

OLAPStatus OLAPData::finalize_segment(uint32_t* data_offset) {
    if (!_write_descriptor) {
        OLAP_LOG_WARNING("segment should be added before.");
        return OLAP_ERR_NOT_INITED;
    }

    OLAPStatus res;
    off_t file_length = _write_descriptor->file_handle.tell();
    if (file_length == -1) {
        res = OLAP_ERR_IO_ERROR;
        goto FINALIZE_SEGMENT_ERROR;
    }

    // 返回写入的总大小
    (data_offset == NULL || (*data_offset = static_cast<uint32_t>(file_length)));

    _write_descriptor->file_header.set_file_length(file_length);
    _write_descriptor->file_header.set_checksum(_write_descriptor->checksum);

    // 写入更新之后的FileHeader
    res = _write_descriptor->file_header.serialize(&(_write_descriptor->file_handle));
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_FATAL("write file header error. [err=%m]");
        goto FINALIZE_SEGMENT_ERROR;
    }

    OLAP_LOG_DEBUG("finalize_segment. [file_name='%s' file_size=%ld]",
                   _write_descriptor->file_handle.file_name().c_str(),
                   file_length);
    
    res = _write_descriptor->file_handle.close();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_FATAL("close file header error. [res=%d err=%m]", res);
        goto FINALIZE_SEGMENT_ERROR;
    }

    return OLAP_SUCCESS;

FINALIZE_SEGMENT_ERROR:
    _check_io_error(res);

    return res;
}

void OLAPData::sync() {
    if (_write_descriptor->file_handle.sync() == -1) {
        OLAP_LOG_WARNING("fail to sync file.[err=%m]");
        _check_io_error(OLAP_ERR_IO_ERROR);
    }
}

OLAPStatus OLAPData::set_end_key(const RowCursor* end_key, bool find_last_end_key) {
    _row_block_broker->set_end_row_flag(false);

    if (end_key == NULL) {
        return OLAP_SUCCESS;
    }

    if (find_row(*end_key, find_last_end_key, true) == NULL) {
        OLAP_LOG_TRACE("End key can't be found, Search until EOF![end_key='%s']",
                       end_key->to_string().c_str());
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    OLAP_LOG_TRACE("end key in block %s row %d",
                   _row_block_broker->position().to_string().c_str(),
                   _row_block_broker->row_index());

    _row_block_broker->set_end_row(_row_block_broker->position(), _row_block_broker->row_index());
    _row_block_broker->set_end_row_flag(true);

    return OLAP_SUCCESS;
}

void OLAPData::_check_io_error(OLAPStatus res) {
    if (is_io_error(res)) {
        _olap_table->set_io_error();
    }
}

OLAPData::RowBlockBroker::RowBlockBroker(
        OLAPTable* olap_table, OLAPIndex* olap_index, RuntimeState* runtime_state) :
        _file_handler(),
        _row_block_pos(),
        _read_buffer(NULL),
        _packed_row_block_size(0),
        _row_block_header_size(0),
        _unpacked_len(0),
        _row_block(NULL),
        _num_rows(0),
        _row_index(0),
        _end_block_position(),
        _end_row_index(0),
        _is_set_end_row(false),
        _olap_table(olap_table),
        _olap_index(olap_index),
        _query_conjunct_ctxs(NULL),
        _delete_conjunct_ctxs(NULL),
        _is_end_block(false),
        _runtime_state(runtime_state) {
    if (_olap_index != NULL) {
        _olap_index->acquire();
    }

    _row_block_header_size = sizeof(RowBlockHeaderV2);
    _data_read_buf_size = OLAP_DEFAULT_DATA_READ_BUF_SIZE;
}

OLAPData::RowBlockBroker::~RowBlockBroker() {
    if (_olap_index != NULL) {
        _olap_index->release();
    }

    this->release();

    if (_runtime_state != NULL && _read_buffer != NULL) {
        MemTracker::update_limits(-1 * _data_read_buf_size, _runtime_state->mem_trackers());
    }

    SAFE_DELETE_ARRAY(_read_buffer);
}

OLAPStatus OLAPData::RowBlockBroker::init() {
    OLAPStatus res = OLAP_SUCCESS;

    if ((res = _row_cursor.init(_olap_table->tablet_schema())) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init row cursor. [res=%d]", res);
        return OLAP_ERR_INIT_FAILED;
    }

    _read_buffer = new(nothrow) char[_data_read_buf_size];
    if (_read_buffer == NULL) {
        OLAP_LOG_FATAL("fail to malloc _packed_row_block. [size=%lu]", _data_read_buf_size);
        return OLAP_ERR_MALLOC_ERROR;
    }

    if (_runtime_state != NULL) {
        MemTracker::update_limits(_data_read_buf_size, _runtime_state->mem_trackers());
        if (MemTracker::limit_exceeded(*_runtime_state->mem_trackers())) {
            return OLAP_ERR_FETCH_MEMORY_EXCEEDED;
        }
    }

    return OLAP_SUCCESS;
}

const RowCursor* OLAPData::RowBlockBroker::first() {
    _row_index = 0;
    if (_row_block->get_row_to_read(_row_index, &_row_cursor) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get row from row block. "
                         "[segment=%d data_offset=%d _row_index=%d]",
                         _row_block_pos.segment,
                         _row_block_pos.data_offset,
                         _row_index);
        return NULL;
    }

    return &_row_cursor;
}

const RowCursor* OLAPData::RowBlockBroker::last() {
    _row_index = _num_rows - 1;

    if (_row_block->get_row_to_read(_row_index, &_row_cursor) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get row from row block. "
                         "[segment=%d data_offset=%d _row_index=%d]",
                         _row_block_pos.segment,
                         _row_block_pos.data_offset,
                         _row_index);
        return NULL;
    }

    return &_row_cursor;
}

const RowCursor* OLAPData::RowBlockBroker::next(bool* end_of_row_block) {
    if (++_row_index >= _num_rows) {
        (end_of_row_block == NULL || (*end_of_row_block = true));
        return NULL;
    }

    (end_of_row_block == NULL || (*end_of_row_block = false));

    if (_row_block->get_row_to_read(_row_index, &_row_cursor) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get row from row block. "
                         "[segment=%d data_offset=%d _row_index=%d]",
                         _row_block_pos.segment,
                         _row_block_pos.data_offset,
                         _row_index);
        return NULL;
    }

    return &_row_cursor;
}

const RowCursor* OLAPData::RowBlockBroker::find_row(const RowCursor& key,
                                                    bool find_last_key,
                                                    bool* end_of_row_block) {
    if (_row_block->find_row(key, find_last_key, &_row_index) != OLAP_SUCCESS) {
        OLAP_LOG_TRACE("fail to find row from row block. [key='%s']", key.to_string().c_str());
        return NULL;
    }

    if (_row_index >= _num_rows) {
        (end_of_row_block == NULL || (*end_of_row_block = true));
        return NULL;
    }

    if (_row_block->get_row_to_read(_row_index, &_row_cursor) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get row from row block. "
                         "[segment=%d, data_offset=%d, _row_index=%d]",
                         _row_block_pos.segment,
                         _row_block_pos.data_offset,
                         _row_index);
        return NULL;
    }

    (end_of_row_block == NULL || (*end_of_row_block = false));
    
    return &_row_cursor;
}

const RowCursor* OLAPData::RowBlockBroker::get_row(uint32_t row_index) {
    _row_index = row_index;
    if (_row_index >= _num_rows) {
        return NULL;
    }

    if (_row_block->get_row_to_read(_row_index, &_row_cursor) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get row from row block."
                         "[segment=%d, data_offset=%d, _row_index=%d]",
                         _row_block_pos.segment,
                         _row_block_pos.data_offset,
                         _row_index);
        return NULL;
    }

    return &_row_cursor;
}

const RowCursor* OLAPData::RowBlockBroker::current() {
    if (_row_index >= _num_rows) {
        return NULL;
    }

    if (_row_block == NULL) {
        OLAP_LOG_WARNING("didn't have row block.");
        return NULL;
    }

    return &_row_cursor;
}

OLAPStatus OLAPData::RowBlockBroker::change_to(
        const RowBlockPosition& row_block_pos, RuntimeProfile* profile) {
    OLAPStatus res = OLAP_SUCCESS;
    RuntimeProfile::Counter* read_data_timer = NULL;
    if (profile != NULL) {
        read_data_timer = profile->get_counter("ReadDataTime");    
    }
    SCOPED_TIMER(read_data_timer);

    // 先将持有的row_block释放
    this->release();

    _row_block_pos = row_block_pos;

    if ((res = _get_row_block(row_block_pos)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get row block.[res=%d]", res);
        return res;
    }

    // TODO(hujie01) 考虑删除条件的版本
    // _row_block->restore();
    
    if (_is_set_end_row && (_row_block_pos == _end_block_position)) {
        _num_rows = _end_row_index;
        _is_end_block = true;
    } else {
        _num_rows = _row_block->row_num();
        _is_end_block = false;
    }

    _row_index = 0;

    return OLAP_SUCCESS;
}

OLAPStatus OLAPData::RowBlockBroker::release() {
    if (_runtime_state != NULL && _row_block != NULL) {
        MemTracker::update_limits(-1 * _row_block->buf_len(), _runtime_state->mem_trackers());
    }
    SAFE_DELETE(_row_block);
    _row_block = NULL;
    _num_rows = 0;
    _row_index = 0;

    return OLAP_SUCCESS;
}

OLAPStatus OLAPData::RowBlockBroker::_get_row_block(const RowBlockPosition& row_block_pos) {
    uint32_t num_rows = 0;
    uint32_t packed_len = 0;
    uint32_t checksum = 0;
    OLAPStatus res = OLAP_SUCCESS;
    RowBlockInfo row_block_info;
    string file_name;
    RowBlockHeaderV2* row_block_header = NULL;

    if (row_block_pos.block_size > _data_read_buf_size) {
        uint64_t max_packed_row_block_size = config::max_packed_row_block_size;

        // if max_unpacked_row_block_size not set or block_size still larger than
        // user specified max_unpacked_row_block_size
        if (max_packed_row_block_size == 0
                || row_block_pos.block_size > max_packed_row_block_size) {
            OLAP_LOG_WARNING("row block size lager than buf. [block_size=%d]",
                             row_block_pos.block_size);
            return OLAP_ERR_ROWBLOCK_READ_INFO_ERROR;
        }

        SAFE_DELETE_ARRAY(_read_buffer);

        _read_buffer = new(nothrow) char[row_block_pos.block_size + sizeof(RowBlockHeaderV2)];
        if (_read_buffer == NULL) {
            if (_runtime_state != NULL) {
                MemTracker::update_limits(-1 * _data_read_buf_size, _runtime_state->mem_trackers());
            }
            OLAP_LOG_WARNING("malloc for read buffer failed. size=%u", row_block_pos.block_size);
            return OLAP_ERR_MALLOC_ERROR;
        }

        if (_runtime_state != NULL) {
            MemTracker::update_limits(
                    row_block_pos.block_size - _data_read_buf_size, _runtime_state->mem_trackers());
            _data_read_buf_size = row_block_pos.block_size;
            if (MemTracker::limit_exceeded(*_runtime_state->mem_trackers())) {
                return OLAP_ERR_FETCH_MEMORY_EXCEEDED;
            }
        }
    }

    file_name = _olap_table->construct_data_file_path(_olap_index->version(),
                                                      _olap_index->version_hash(),
                                                      row_block_pos.segment);

    if ((res = _file_handler.open_with_cache(file_name, O_RDONLY)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to open file. [file_name=%s]", file_name.c_str());
        goto GET_ROW_BLOCK_ERROR;
    }
    
    res = _file_handler.pread(_read_buffer, row_block_pos.block_size, row_block_pos.data_offset);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to read block from file. [offset=%d, size=%d, buf_size=%d]",
                         row_block_pos.data_offset,
                         row_block_pos.block_size,
                         row_block_pos.block_size);
        goto GET_ROW_BLOCK_ERROR;
    }

    // 处理row_block header, 这地方比较丑，先写，后边和filehelper一起优化
    row_block_header = reinterpret_cast<RowBlockHeaderV2*>(_read_buffer);
    if (row_block_header->magic_num != 0) {
        RowBlockHeader* old_row_block_header = reinterpret_cast<RowBlockHeader*>(_read_buffer);
        num_rows = old_row_block_header->num_rows;
        packed_len = old_row_block_header->packed_len;
        checksum = old_row_block_header->checksum;
        _unpacked_len = _olap_table->get_row_size() * num_rows;
        _row_block_header_size = sizeof(RowBlockHeader);
    } else {
        num_rows = row_block_header->num_rows;
        packed_len = row_block_header->packed_len;
        checksum = row_block_header->checksum;
        _unpacked_len = row_block_header->unpacked_len;
        _row_block_header_size = sizeof(RowBlockHeaderV2);
    }

    if (_row_block_pos.block_size != packed_len + _row_block_header_size) {
        OLAP_LOG_WARNING("row block len on disk not match in index. "
                         "[block_size_in_index=%u block_size_in_row_block_header=%u]",
                         _row_block_pos.block_size,
                         packed_len + _row_block_header_size);
        res = OLAP_ERR_ROWBLOCK_READ_INFO_ERROR;
        goto GET_ROW_BLOCK_ERROR;
    }

    if (num_rows < 1) {
        OLAP_LOG_WARNING("row block is empty. [num_rows=%d]", num_rows);
        res = OLAP_ERR_ROWBLOCK_READ_INFO_ERROR;
        goto GET_ROW_BLOCK_ERROR;
    }

    _packed_row_block_size = packed_len;

    // 创建新的row_block
    row_block_info.checksum = checksum;
    row_block_info.row_num = num_rows;
    row_block_info.unpacked_len = _unpacked_len;
    row_block_info.data_file_type = OLAP_DATA_FILE;
    row_block_info.null_supported = _olap_index->get_null_supported(0);

    if ((_row_block = new(nothrow) RowBlock(_olap_table->tablet_schema())) == NULL) {
        OLAP_LOG_FATAL("fail to malloc RowBlock. [size=%ld]", sizeof(RowBlock));
        res = OLAP_ERR_MALLOC_ERROR;
        goto GET_ROW_BLOCK_ERROR;
    }

    if ((res = _row_block->init(row_block_info)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init row block. [res=%d]", res);
        goto GET_ROW_BLOCK_ERROR;
    }

    if (_runtime_state != NULL) {
        MemTracker::update_limits(_row_block->buf_len(), _runtime_state->mem_trackers());
        if (MemTracker::limit_exceeded(*_runtime_state->mem_trackers())) {
            res = OLAP_ERR_FETCH_MEMORY_EXCEEDED;
            OLAP_GOTO(GET_ROW_BLOCK_ERROR);
        }
    }

    res = _row_block->decompress(_read_buffer + _row_block_header_size,
                                 packed_len,
                                 OLAP_COMP_STORAGE);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to decompress the row block. [res=%d, packed_len=%d]",
                         res,
                         packed_len);
        goto GET_ROW_BLOCK_ERROR;
    }

    // 过滤删除条件
    if (_delete_conjunct_ctxs != NULL) {
        res = _row_block->eval_conjuncts(*_delete_conjunct_ctxs);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to eval delete conjuncts for row block. [res=%d]", res);
            goto GET_ROW_BLOCK_ERROR;
        }
        // 保存删除条件执行后的结果，以便存入Cache备用
        _row_block->backup();
    }

    // 过滤查询条件
    if (_query_conjunct_ctxs != NULL) {
        res = _row_block->eval_conjuncts(*_query_conjunct_ctxs);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to eval query conjuncts for row block. [res=%d]", res);
            goto GET_ROW_BLOCK_ERROR;
        }
    }

    return OLAP_SUCCESS;

GET_ROW_BLOCK_ERROR:
    if (_runtime_state != NULL && _row_block != NULL) {
        MemTracker::update_limits(-1 * _row_block->buf_len(), _runtime_state->mem_trackers());
    }
    SAFE_DELETE(_row_block);
    return res;
}

}  // namespace palo
