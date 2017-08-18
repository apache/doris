// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/olap_index.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <fstream>

#include "olap/olap_data.h"
#include "olap/olap_table.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/utils.h"

using std::ifstream;
using std::string;
using std::vector;

namespace palo {
#define TABLE_PARAM_VALIDATE() \
    do { \
        if (!_index_loaded) { \
            OLAP_LOG_WARNING("fail to find, index is not loaded. [table=%ld schema_hash=%d]", \
                    _table->tablet_id(), \
                    _table->schema_hash()); \
            return OLAP_ERR_NOT_INITED; \
        } \
    } while (0);

#define POS_PARAM_VALIDATE(pos) \
    do { \
        if (NULL == pos) { \
            OLAP_LOG_WARNING("fail to find, NULL position parameter."); \
            return OLAP_ERR_INPUT_PARAMETER_ERROR; \
        } \
    } while (0);

#define SLICE_PARAM_VALIDATE(slice) \
    do { \
        if (NULL == slice) { \
            OLAP_LOG_WARNING("fail to find, NULL slice parameter."); \
            return OLAP_ERR_INPUT_PARAMETER_ERROR; \
        } \
    } while (0);

OLAPIndex::OLAPIndex(OLAPTable* table,
                     Version version,
                     VersionHash version_hash,
                     bool delete_flag,
                     uint32_t num_segments,
                     time_t max_timestamp) :
        _table(table),
        _version(version),
        _delete_flag(delete_flag),
        _max_timestamp(max_timestamp),
        _num_segments(num_segments),
        _version_hash(version_hash),
        _current_num_rows_per_row_block(0),
        _inited_column_statistics(false),
        _column_statistics(_table->num_key_fields(), std::pair<Field *, Field *>(NULL, NULL)) {
    const RowFields& tablet_schema = _table->tablet_schema();
    _short_key_length = 0;
    _short_key_buf = NULL;

    //_short_key_length += (_table->num_short_key_fields() + 7) >> 3;
    for (size_t i = 0; i < _table->num_short_key_fields(); ++i) {
        _short_key_info_list.push_back(tablet_schema[i]);
        _short_key_length += tablet_schema[i].index_length + sizeof(bool);
    }

    _index_loaded = false;
    _ref_count = 0;
    _header_file_name = _table->header_file_name();
}

OLAPIndex::~OLAPIndex() {
    delete [] _short_key_buf;
    _current_file_handler.close();

    if (_inited_column_statistics) {
            for (size_t i = 0; i < _column_statistics.size(); ++i) {
            SAFE_DELETE(_column_statistics[i].first);
            SAFE_DELETE(_column_statistics[i].second);
        }
    }
    
    _seg_pb_map.clear();
}

void OLAPIndex::acquire() {
    atomic_inc(&_ref_count);
}

int64_t OLAPIndex::ref_count() {
    return _ref_count;
}

void OLAPIndex::release() {
    atomic_dec(&_ref_count);
}

bool OLAPIndex::is_in_use() {
    return _ref_count > 0;
}

// you can not use OLAPIndex after delete_all_files(), or else unknown behavior occurs.
void OLAPIndex::delete_all_files() {
    for (uint32_t seg_id = 0; seg_id < _num_segments; ++seg_id) {
        // get full path for one segment
        string index_path = _construct_index_file_path(_version, _version_hash, seg_id);
        string data_path = _construct_data_file_path(_version, _version_hash, seg_id);

        if (remove(index_path.c_str()) != 0) {
            OLAP_LOG_WARNING("fail to delete index file. [err='%m' path='%s']", index_path.c_str());
        }

        if (remove(data_path.c_str()) != 0) {
            OLAP_LOG_WARNING("fail to delete data file. [err='%m' path='%s']", data_path.c_str());
        }
    }
}

OLAPStatus OLAPIndex::set_column_statistics(
        std::vector<std::pair<Field *, Field *> > &column_statistics) {
    if (_inited_column_statistics) {
        return OLAP_SUCCESS;
    }
    
    if (column_statistics.size() != _column_statistics.size()) {
        OLAP_LOG_WARNING("fail to set delta pruning![column statistics size=%d:%d]",
                _column_statistics.size(), column_statistics.size());
        return OLAP_ERR_INDEX_DELTA_PRUNING;
    }

    for (size_t i = 0; i < _column_statistics.size(); ++i) {
        _column_statistics[i].first = Field::create(_table->tablet_schema()[i]);
        if (_column_statistics[i].first == NULL) {
            OLAP_LOG_FATAL("fail to create column statistics field. [field_id=%lu]", i);
            return OLAP_ERR_MALLOC_ERROR;
        }
        if (!_column_statistics[i].first->allocate()) {
            OLAP_LOG_FATAL("fail to allocate column statistics field. [field_id=%lu]", i);
            return OLAP_ERR_MALLOC_ERROR;
        }
        
        _column_statistics[i].second = Field::create(_table->tablet_schema()[i]);
        if (_column_statistics[i].second == NULL) {
            OLAP_LOG_FATAL("fail to create column statistics field. [field_id=%lu]", i);
            return OLAP_ERR_MALLOC_ERROR;
        }
        if (!_column_statistics[i].second->allocate()) {
            OLAP_LOG_FATAL("fail to allocate column statistics field. [field_id=%lu]", i);
            return OLAP_ERR_MALLOC_ERROR;
        }
    }

    for (size_t i = 0; i < _column_statistics.size(); ++i) {
        _column_statistics[i].first->copy(column_statistics[i].first);
        _column_statistics[i].second->copy(column_statistics[i].second);
    }

    _inited_column_statistics = true;

    return OLAP_SUCCESS;
}

OLAPStatus OLAPIndex::set_column_statistics_from_string(
            std::vector<std::pair<std::string, std::string> > &column_statistics_string,
            std::vector<bool> &has_null_flags) {
    if (_inited_column_statistics) {
        return OLAP_SUCCESS;
    }
    
    if (column_statistics_string.size() != _column_statistics.size()) {
        OLAP_LOG_WARNING("fail to set delta pruning![column statistics size=%d:%d]",
                _column_statistics.size(), column_statistics_string.size());
        return OLAP_ERR_INDEX_DELTA_PRUNING;
    }

    for (size_t i = 0; i < _column_statistics.size(); ++i) {
        _column_statistics[i].first = Field::create(_table->tablet_schema()[i]);
        if (_column_statistics[i].first == NULL) {
            OLAP_LOG_FATAL("fail to create column statistics field. [field_id=%lu]", i);
            return OLAP_ERR_MALLOC_ERROR;
        }
        if (!_column_statistics[i].first->allocate()) {
            OLAP_LOG_FATAL("fail to allocate column statistics field. [field_id=%lu]", i);
            return OLAP_ERR_MALLOC_ERROR;
        }
        
        _column_statistics[i].second = Field::create(_table->tablet_schema()[i]);
        if (_column_statistics[i].second == NULL) {
            OLAP_LOG_FATAL("fail to create column statistics field. [field_id=%lu]", i);
            return OLAP_ERR_MALLOC_ERROR;
        }
        if (!_column_statistics[i].second->allocate()) {
            OLAP_LOG_FATAL("fail to allocate column statistics field. [field_id=%lu]", i);
            return OLAP_ERR_MALLOC_ERROR;
        }
    }

    OLAPStatus res = OLAP_SUCCESS;
    for (size_t i = 0; i < _column_statistics.size(); ++i) {
        res = _column_statistics[i].first->from_string(
                const_cast<const char*>(column_statistics_string[i].first.c_str()));
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to init field from string.[string=%s]",
                    column_statistics_string[i].first.c_str());
            return res;
        }
        if (has_null_flags[i]) {
            //[min, max] -> [NULL, max]
            _column_statistics[i].first->set_null();
        }
        res = _column_statistics[i].second->from_string(
                const_cast<const char*>(column_statistics_string[i].second.c_str()));
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to init field from string.[string=%s]",
                    column_statistics_string[i].second.c_str());
            return res;
        }
    }

    _inited_column_statistics = true;

    return OLAP_SUCCESS;
}

OLAPStatus OLAPIndex::load() {
    OLAPStatus res = OLAP_ERR_INDEX_LOAD_ERROR;
    boost::lock_guard<boost::mutex> guard(_index_load_lock);

    if (_index_loaded) {
        return OLAP_SUCCESS;
    }

    if (_num_segments == 0) {
        OLAP_LOG_WARNING("fail to load index, segments number is 0.");
        return res;
    }

    if (_index.init(_short_key_length, _table->num_short_key_fields(), 
            &_short_key_info_list) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to create MemIndex. [num_segment=%d]", _num_segments);
        return res;
    }

    // for each segment
    for (uint32_t seg_id = 0; seg_id < _num_segments; ++seg_id) {
        if (COLUMN_ORIENTED_FILE == _table->data_file_type()) {
            string seg_path = _table->construct_data_file_path(_version, _version_hash, seg_id);
            if (OLAP_SUCCESS != (res = load_pb(seg_path.c_str(), seg_id))) {
                OLAP_LOG_WARNING("faile to load pb structures. [seg_path='%s']", seg_path.c_str());
                _check_io_error(res);
                return res;
            }
        }

        // get full path for one segment
        string path = _table->construct_index_file_path(_version, _version_hash, seg_id);
        if ((res = _index.load_segment(path.c_str(), &_current_num_rows_per_row_block))
                != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to load segment. [path='%s']", path.c_str());
            _check_io_error(res);
            return res;
        }
    }

    _index_loaded = true;

    return OLAP_SUCCESS;
}

OLAPStatus OLAPIndex::load_pb(const char* file, uint32_t seg_id) {
    OLAPStatus res = OLAP_SUCCESS;

    FileHeader<column_file::ColumnDataHeaderMessage> seg_file_header;
    FileHandler seg_file_handler;
    res = seg_file_handler.open_with_cache(file, O_RDONLY);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("failed to open segment file. [err=%d, file=%s]", res, file);
        return res;
    }

    res = seg_file_header.unserialize(&seg_file_handler);
    if (OLAP_SUCCESS != res) {
        seg_file_handler.close();
        OLAP_LOG_WARNING("fail to unserialize header. [err=%d, path='%s']", res, file);
        return res;
    }

    _seg_pb_map[seg_id] = seg_file_header;
    seg_file_handler.close();
    return OLAP_SUCCESS;
}

bool OLAPIndex::index_loaded() {
    return _index_loaded;
}

OLAPStatus OLAPIndex::validate() {
    OLAPStatus res = OLAP_SUCCESS;

    for (uint32_t seg_id = 0; seg_id < _num_segments; ++seg_id) {
        FileHeader<OLAPIndexHeaderMessage, OLAPIndexFixedHeader> index_file_header;
        FileHeader<OLAPDataHeaderMessage> data_file_header;

        // get full path for one segment
        string index_path = _table->construct_index_file_path(_version, _version_hash, seg_id);
        string data_path = _table->construct_data_file_path(_version, _version_hash, seg_id);

        // 检查index文件头
        if ((res = index_file_header.validate(index_path)) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("validate index file error. [file='%s']", index_path.c_str());
            _check_io_error(res);
            return res;
        }

        // 检查data文件头
        if ((res = data_file_header.validate(data_path)) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("validate data file error. [file='%s']", data_path.c_str());
            _check_io_error(res);
            return res;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPIndex::find_row_block(const RowCursor& key,
                                 RowCursor* helper_cursor,
                                 bool find_last,
                                 RowBlockPosition* pos) const {
    TABLE_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(pos);

    // 将这部分逻辑从memindex移出来，这样可以复用find。
    OLAPIndexOffset offset = _index.find(key, helper_cursor, find_last);
    if (offset.offset > 0) {
        offset.offset = offset.offset - 1;
    } else {
        offset.offset = 0;
    }

    if (find_last) {
        OLAPIndexOffset next_offset = _index.next(offset);
        if (!(next_offset == _index.end())) {
            offset = next_offset;
        }
    }

    return _index.get_row_block_position(offset, pos);
}

OLAPStatus OLAPIndex::find_short_key(const RowCursor& key,
                                 RowCursor* helper_cursor,
                                 bool find_last,
                                 RowBlockPosition* pos) const {
    TABLE_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(pos);

    // 由于find会从前一个segment找起，如果前一个segment中恰好没有该key，
    // 就用前移后移来移动segment的位置.
    OLAPIndexOffset offset = _index.find(key, helper_cursor, find_last);
    if (offset.offset > 0) {
        offset.offset = offset.offset - 1;

        OLAPIndexOffset next_offset = _index.next(offset);
        if (!(next_offset == _index.end())) {
            offset = next_offset;
        }
    }

    OLAP_LOG_DEBUG("[seg='%d', offset='%d']", offset.segment, offset.offset);
    return _index.get_row_block_position(offset, pos);
}

OLAPStatus OLAPIndex::get_row_block_entry(const RowBlockPosition& pos, Slice* entry) const {
    TABLE_PARAM_VALIDATE();
    SLICE_PARAM_VALIDATE(entry);
    
    return _index.get_entry(_index.get_offset(pos), entry);
}

OLAPStatus OLAPIndex::find_first_row_block(RowBlockPosition* position) const {
    TABLE_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(position);
    
    return _index.get_row_block_position(_index.find_first(), position);
}

OLAPStatus OLAPIndex::find_last_row_block(RowBlockPosition* position) const {
    TABLE_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(position);
    
    return _index.get_row_block_position(_index.find_last(), position);
}

OLAPStatus OLAPIndex::find_next_row_block(RowBlockPosition* pos, bool* eof) const {
    TABLE_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(pos);
    POS_PARAM_VALIDATE(eof);

    OLAPIndexOffset current = _index.get_offset(*pos);
    *eof = false;

    OLAPIndexOffset next = _index.next(current);
    if (next == _index.end()) {
        *eof = true;
        return OLAP_ERR_INDEX_EOF;
    }

    return _index.get_row_block_position(next, pos);
}

OLAPStatus OLAPIndex::find_mid_point(const RowBlockPosition& low,
                                 const RowBlockPosition& high,
                                 RowBlockPosition* output,
                                 uint32_t* dis) const {
    *dis = compute_distance(low, high);
    if (*dis >= _index.count()) {
        return OLAP_ERR_INDEX_EOF;
    } else {
        *output = low;
        if (advance_row_block(*dis / 2, output) != OLAP_SUCCESS) {
            return OLAP_ERR_INDEX_EOF;
        }

        return OLAP_SUCCESS;
    }
}

OLAPStatus OLAPIndex::find_prev_point(
        const RowBlockPosition& current, RowBlockPosition* prev) const {
    OLAPIndexOffset current_offset = _index.get_offset(current);
    OLAPIndexOffset prev_offset = _index.prev(current_offset);

    return _index.get_row_block_position(prev_offset, prev);
}

OLAPStatus OLAPIndex::advance_row_block(int64_t num_row_blocks, RowBlockPosition* position) const {
    TABLE_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(position);

    OLAPIndexOffset off = _index.get_offset(*position);
    iterator_offset_t absolute_offset = _index.get_absolute_offset(off) + num_row_blocks;
    if (absolute_offset >= _index.count()) {
        return OLAP_ERR_INDEX_EOF;
    }

    return _index.get_row_block_position(_index.get_relative_offset(absolute_offset), position);
}

// PRECONDITION position1 < position2
uint32_t OLAPIndex::compute_distance(const RowBlockPosition& position1,
                                     const RowBlockPosition& position2) const {
    iterator_offset_t offset1 = _index.get_absolute_offset(_index.get_offset(position1));
    iterator_offset_t offset2 = _index.get_absolute_offset(_index.get_offset(position2));
    
    return offset2 > offset1 ? offset2 - offset1 : 0;
}

OLAPStatus OLAPIndex::add_segment() {
    // 打开文件
    ++_num_segments;
    OLAPStatus res = OLAP_SUCCESS;
    OLAPIndexHeaderMessage* index_header = NULL;
    
    string file_path = 
            _table->construct_index_file_path(version(), version_hash(), _num_segments - 1);
    res = _current_file_handler.open_with_mode(
            file_path.c_str(), O_CREAT | O_EXCL | O_WRONLY, S_IRUSR | S_IWUSR);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("can not create file. [file_path='%s' err=%m]", file_path.c_str());
        _check_io_error(res);
        return res;
    }

    // 构造Proto格式的Header
    index_header = _file_header.mutable_message();
    index_header->set_start_version(_version.first);
    index_header->set_end_version(_version.second);
    index_header->set_cumulative_version_hash(_version_hash);
    index_header->set_segment(_num_segments - 1);
    index_header->set_num_rows_per_block(_table->num_rows_per_row_block());
    index_header->set_delete_flag(_delete_flag);
    index_header->set_null_supported(true);

    // 准备FileHeader
    if ((res = _file_header.prepare(&_current_file_handler)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("write file header error. [err=%m]");
        _check_io_error(res);
        return res;
    }

    // 跳过FileHeader
    if (_current_file_handler.seek(_file_header.size(), SEEK_SET) == -1) {
        OLAP_LOG_WARNING("lseek header file error. [err=%m]");
        res = OLAP_ERR_IO_ERROR;
        _check_io_error(res);
        return res;
    }

    // 分配一段存储short key的内存, 初始化index_row
    if (_short_key_buf == NULL) {
        _short_key_buf = new(std::nothrow) char[_short_key_length];
        if (_short_key_buf == NULL) {
            OLAP_LOG_WARNING("malloc short_key_buf error.");
            return OLAP_ERR_MALLOC_ERROR;
        }

        if (_current_index_row.init(_table->tablet_schema()) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("init _current_index_row fail.");
            return OLAP_ERR_INIT_FAILED;
        }
    }

    // 初始化checksum
    _checksum = ADLER32_INIT;
    return OLAP_SUCCESS;
}

OLAPStatus OLAPIndex::add_row_block(const RowBlock& row_block, const uint32_t data_offset) {
    // get first row of the row_block to distill index item.
    if (row_block.get_row_to_read(0, &_current_index_row) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("get first row in row_block fail.");
        return OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION;
    }

    return add_short_key(_current_index_row, data_offset);
}

OLAPStatus OLAPIndex::add_short_key(const RowCursor& short_key, const uint32_t data_offset) {
    // 将short key的内容写入_short_key_buf
    OLAPStatus res = OLAP_SUCCESS;
    size_t offset = 0;

    //short_key.write_null_array(_short_key_buf);
    //offset += short_key.get_num_null_byte();
    for (size_t i = 0; i < _short_key_info_list.size(); i++) {
        short_key.write_index_by_index(i, _short_key_buf + offset);
        offset += short_key.get_field_size(i);
    }

    // 写入Short Key对应的数据
    if ((res = _current_file_handler.write(_short_key_buf, _short_key_length)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("write short key failed. [err=%m]");
        _check_io_error(res);
        return res;
    }

    // 写入对应的数据文件偏移量
    if ((res = _current_file_handler.write(&data_offset, sizeof(data_offset))) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("write data_offset failed. [err=%m]");
        _check_io_error(res);
        return res;
    }

    _checksum = olap_adler32(_checksum, _short_key_buf, _short_key_length);
    _checksum = olap_adler32(_checksum,
                             reinterpret_cast<const char*>(&data_offset),
                             sizeof(data_offset));
    return OLAP_SUCCESS;
}

OLAPStatus OLAPIndex::finalize_segment(uint32_t data_segment_size, int64_t num_rows) {
    // 准备FileHeader
    OLAPStatus res = OLAP_SUCCESS;

    int file_length = _current_file_handler.tell();
    if (file_length == -1) {
        OLAP_LOG_WARNING("get file_length error. [err=%m]");
        _check_io_error(res);
        return OLAP_ERR_IO_ERROR;
    }

    _file_header.set_file_length(file_length);
    _file_header.set_checksum(_checksum);
    _file_header.mutable_extra()->data_length = data_segment_size;
    _file_header.mutable_extra()->num_rows = num_rows;

    // 写入更新之后的FileHeader
    if ((res = _file_header.serialize(&_current_file_handler)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("write file header error. [err=%m]");
        _check_io_error(res);
        return res;
    }

    OLAP_LOG_DEBUG("finalize_segment. [file_name='%s' file_length=%d]",
                   _current_file_handler.file_name().c_str(),
                   file_length);

    if ((res = _current_file_handler.close()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("close file error. [err=%m]");
        _check_io_error(res);
        return res;
    }

    return OLAP_SUCCESS;
}

void OLAPIndex::sync() {
    if (_current_file_handler.sync() == -1) {
        OLAP_LOG_WARNING("fail to sync file.[err=%m]");
        _table->set_io_error();
    }
}

VersionHash OLAPIndex::version_hash() const {
    return _version_hash;
}

void OLAPIndex::_check_io_error(OLAPStatus res) {
    if (is_io_error(res)) {
        _table->set_io_error();
    }
}

uint64_t OLAPIndex::num_index_entries() const {
    return _index.count();
}

MemIndex::~MemIndex() {
    _num_entries = 0;
    for (vector<SegmentMetaInfo>::iterator it = _meta.begin(); it != _meta.end(); ++it) {
        free(it->buffer.data);
        it->buffer.data = NULL;
        it->buffer.length = 0;
    }
}

OLAPStatus MemIndex::load_segment(const char* file, size_t *current_num_rows_per_row_block) {
    OLAPStatus res = OLAP_SUCCESS;

    SegmentMetaInfo meta;
    OLAPIndexHeaderMessage pb;
    uint32_t adler_checksum = 0;
    uint32_t num_entries = 0;

    if (file == NULL) {
        res = OLAP_ERR_INPUT_PARAMETER_ERROR;
        OLAP_LOG_WARNING("load segment for loading index error. [file=%s; res=%d]", file, res);
        return res;
    }

    FileHandler file_handler;
    if ((res = file_handler.open_with_cache(file, O_RDONLY)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to open index file. [file='%s']", file);
        OLAP_LOG_WARNING("load segment for loading index error. [file=%s; res=%d]", file, res);
        return res;
    }

    if ((res = meta.file_header.unserialize(&file_handler)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to read index file header. [file='%s']", file);
        OLAP_LOG_WARNING("load segment for loading index error. [file=%s; res=%d]", file, res);
        file_handler.close();
        return res;
    }

    // 允许索引内容为空
    // 索引长度必须为索引项长度的整数倍
    meta.buffer.length = meta.file_header.file_length() - meta.file_header.size();
    bool null_supported = false;
    //null_supported是为了兼容之前没有NULL字节的数据。
    //目前索引里面都加入了NULL的标志位，entry length都算了NULL标志位构成的bytes
    //对于没有标志位的索引，读取数据之后需要对每个字段补齐这部分。
    if (false == meta.file_header.message().has_null_supported()) {
        null_supported = false;
    } else {
        null_supported = meta.file_header.message().null_supported();
    }
    size_t num_short_key_fields = short_key_num();
    bool is_align = false;
    if (false == null_supported) {
        is_align = (0 == meta.buffer.length % (entry_length() - num_short_key_fields));
    } else {
        is_align = (0 == meta.buffer.length % entry_length());
    }
    if (false == is_align) {
        res = OLAP_ERR_INDEX_LOAD_ERROR;
        OLAP_LOG_WARNING("fail to load_segment, buffer length is not correct.");
        OLAP_LOG_WARNING("load segment for loading index error. [file=%s; res=%d]", file, res);
        file_handler.close();
        return res;
    }
    if (false == null_supported) {
        num_entries = meta.buffer.length / (entry_length() - num_short_key_fields);
        meta.buffer.data = reinterpret_cast<char*>(
                calloc(meta.buffer.length + num_entries * num_short_key_fields, 1));
    } else {
        num_entries = meta.buffer.length / entry_length();
        meta.buffer.data = reinterpret_cast<char*>(calloc(meta.buffer.length, 1));
    }

    if (meta.buffer.data == NULL) {
        res = OLAP_ERR_MALLOC_ERROR;
        OLAP_LOG_WARNING("load segment for loading index error. [file=%s; res=%d]", file, res);
        file_handler.close();
        return res;
    }

    // 读取索引内容
    // 为了启动加速，此处可使用mmap方式。
    if (file_handler.pread(meta.buffer.data,
                           meta.buffer.length,
                           meta.file_header.size()) != OLAP_SUCCESS) {
        res = OLAP_ERR_IO_ERROR;
        OLAP_LOG_WARNING("load segment for loading index error. [file=%s; res=%d]", file, res);
        file_handler.close();
        free(meta.buffer.data);
        return res;
    }

    // calculate the total size of all segments
    if (false == null_supported) {
        _index_size += meta.file_header.file_length() + num_entries * num_short_key_fields;
    } else {
        _index_size += meta.file_header.file_length();
    }
    _data_size += meta.file_header.extra().data_length;
    _num_rows += meta.file_header.extra().num_rows;

    // checksum validation
    adler_checksum = olap_adler32(ADLER32_INIT, meta.buffer.data, meta.buffer.length);
    if (adler_checksum != meta.file_header.checksum()) {
        res = OLAP_ERR_INDEX_CHECKSUM_ERROR;
        OLAP_LOG_WARNING("checksum validation error.");
        OLAP_LOG_WARNING("load segment for loading index error. [file=%s; res=%d]", file, res);
        file_handler.close();
        free(meta.buffer.data);
        return res;
    }

    if (false == null_supported) {
        meta.buffer.length += num_entries * num_short_key_fields;
        const RowFields& tablet_schema = short_key_fields();
        size_t src = meta.buffer.length - num_entries * num_short_key_fields;
        size_t dest = meta.buffer.length;
        for (size_t i = 0; i < num_entries; ++i) {
            memmove(meta.buffer.data + dest - sizeof(data_file_offset_t),
                    meta.buffer.data + src - sizeof(data_file_offset_t),
                    sizeof(data_file_offset_t));
            dest = dest - sizeof(data_file_offset_t);
            src = src - sizeof(data_file_offset_t);
            for (size_t j = num_short_key_fields; j > 0; --j) {
                size_t index_length = tablet_schema[j-1].index_length;
                memmove(meta.buffer.data + dest - index_length, 
                        meta.buffer.data + src - index_length, index_length);
                *(meta.buffer.data + dest - index_length - 1) &= 0;
                dest = dest - index_length - 1;
                src = src - index_length;
            }
        }
    }

    meta.range.first = _num_entries;
    meta.range.last = meta.range.first + num_entries;
    _num_entries = meta.range.last;
    _meta.push_back(meta);

    (current_num_rows_per_row_block == NULL
         || (*current_num_rows_per_row_block = meta.file_header.message().num_rows_per_block())); 

    file_handler.close();

    return OLAP_SUCCESS;
}

OLAPStatus MemIndex::init(size_t short_key_len, size_t short_key_num, RowFields* fields) {
    if (fields == NULL) {
        OLAP_LOG_WARNING("fail to init MemIndex, NULL short key fields.");
        return OLAP_ERR_INDEX_LOAD_ERROR;
    }

    _key_length = short_key_len;
    _key_num = short_key_num;
    _fields = fields;

    return OLAP_SUCCESS;
}

// Find and return the IndexOffset of the element prior to the first element which
// is key's lower_bound, or upper_bound if key exists, or return the last element in MemIndex
// This process is consists of two phases of binary search.
// Here try to find the first segment which range covers k,
// and find the index item inside the segment previously found.
//
// There 're a little more detail of the binary search.
// The semantics here is to guarantee there's no
// omissions for given k, consider the following case:
// [4, offset] ---------------------> [(4, xxxx), (4, xxxy), (5, xxxx), (5, xxxy)]
// [5, offset] ---------------------> [(5, yyyy), (5, yyyx), (6, ...)]
// because of our sparse index, the first item which short key equals 5(5, xxxx) is indexed
// by shortkey 4 in the first index item, if we want to find the first key not less than 6, we
// should return the first index instead the second.
const OLAPIndexOffset MemIndex::find(const RowCursor& k,
                                     RowCursor* helper_cursor,
                                     bool find_last) const {
    if (begin() == end()) {
        return begin();
    }

    OLAPIndexOffset offset;
    BinarySearchIterator it;
    BinarySearchIterator seg_beg(0);
    BinarySearchIterator seg_fin(segment_count());

    try {
        SegmentComparator seg_comparator(this, helper_cursor);

        // first step, binary search for the correct segment
        if (!find_last) {
            it = std::lower_bound(seg_beg, seg_fin, k, seg_comparator);
        } else {
            it = std::upper_bound(seg_beg, seg_fin, k, seg_comparator);
        }

        iterator_offset_t off = 0;
        if (*it > 0) {
            off = *it - 1;
        }

        // set segment id
        offset.segment = off;
        IndexComparator index_comparator(this, helper_cursor);
        // second step, binary search index item in given segment
        BinarySearchIterator index_beg(0);
        BinarySearchIterator index_fin(_meta[off].count());

        if (index_comparator.set_segment_id(off) != OLAP_SUCCESS) {
            throw "index of of range";
        }

        if (!find_last) {
            it = std::lower_bound(index_beg, index_fin, k, index_comparator);
        } else {
            it = std::upper_bound(index_beg, index_fin, k, index_comparator);
        }

        offset.offset = *it;
        OLAP_LOG_DEBUG("show real offset iterator value. [off=%u]", *it);
        OLAP_LOG_DEBUG("show result offset. [seg_off=%u off=%u]", offset.segment, offset.offset);
    } catch (...) {
        OLAP_LOG_WARNING("fail to compare value in memindex. [cursor='%s' find_last=%d]",
                         k.to_string().c_str(),
                         find_last);
        return end();
    }

    return offset;
}

const OLAPIndexOffset MemIndex::next(const OLAPIndexOffset& pos) const {
    OLAPIndexOffset off;

    if (pos.segment >= segment_count()) {
        return end();
    } else if (pos.segment == segment_count() - 1) {
        if (pos.offset >= _meta[pos.segment].count() - 1) {
            return end();
        } else {
            off.segment = pos.segment;
            off.offset = pos.offset + 1;
            return off;
        }
    } else {
        if (pos.offset >= _meta[pos.segment].count() - 1) {
            off.segment = pos.segment + 1;
            off.offset = 0;
        } else {
            off.segment = pos.segment;
            off.offset = pos.offset + 1;
        }

        return off;
    }
}

const OLAPIndexOffset MemIndex::prev(const OLAPIndexOffset& pos) const {
    OLAPIndexOffset off;

    if (pos.offset == 0) {
        if (pos.segment == 0) {
            off = pos;
        } else {
            off.segment = pos.segment - 1;
            off.offset = _meta[off.segment].count() - 1;
        }
    } else {
        off.segment = pos.segment;
        off.offset = pos.offset - 1;
    }

    return off;
}

const OLAPIndexOffset MemIndex::get_offset(const RowBlockPosition& pos) const {
    uint32_t file_header_size = _meta[pos.segment].file_header.size();
    if (pos.segment >= segment_count()
            || pos.index_offset > file_header_size + _meta[pos.segment].buffer.length
            || (pos.index_offset - file_header_size) % entry_length() != 0) {
        return end();
    }

    OLAPIndexOffset off;
    off.segment = pos.segment;
    off.offset = (pos.index_offset - _meta[pos.segment].file_header.size()) / entry_length();

    return off;
}

OLAPStatus MemIndex::get_entry(const OLAPIndexOffset& pos, Slice* slice) const {
    if (pos.segment >= segment_count() || pos.offset >= _meta[pos.segment].count()) {
        return OLAP_ERR_INDEX_EOF;
    }

    slice->length = entry_length();
    slice->data = _meta[pos.segment].buffer.data + pos.offset * entry_length();

    return OLAP_SUCCESS;
}

OLAPStatus MemIndex::get_row_block_position(
        const OLAPIndexOffset& pos, RowBlockPosition* rbp) const {
    if (empty()) {
        return OLAP_ERR_INDEX_EOF;
    }

    if (pos.segment >= segment_count() || pos.offset >= _meta[pos.segment].count()) {
        OLAP_LOG_WARNING("fail to get RowBlockPosition from OLAPIndexOffset. "
                         "[IndexOffse={segment=%u offset=%u} segment_count=%lu items_count=%lu]",
                         pos.segment,
                         pos.offset,
                         segment_count(),
                         pos.segment < segment_count() ? _meta[pos.segment].count() : 0);
        return OLAP_ERR_INDEX_EOF;
    }

    rbp->segment = pos.segment;
    rbp->data_offset = *reinterpret_cast<uint32_t*>(
                           _meta[pos.segment].buffer.data +
                           pos.offset * entry_length() + short_key_length());
    rbp->index_offset = _meta[pos.segment].file_header.size() + pos.offset * entry_length();

    if (pos.offset == _meta[pos.segment].count() - 1) {
        rbp->block_size = _meta[pos.segment].file_header.extra().data_length - rbp->data_offset;
    } else {
        uint32_t next_offset = *reinterpret_cast<uint32_t*>(
                                   _meta[pos.segment].buffer.data +
                                   (pos.offset + 1) * entry_length() + short_key_length());
        rbp->block_size = next_offset - rbp->data_offset;
    }

    return OLAP_SUCCESS;
}

const OLAPIndexOffset MemIndex::get_relative_offset(iterator_offset_t absolute_offset) const {
    int begin = 0;
    int end = segment_count() - 1;
    OLAPIndexOffset offset(0, 0);

    while (begin <= end) {
        size_t mid = (begin + end) / 2;
        if (absolute_offset >= _meta[mid].range.last) {
            begin = mid + 1;
        } else if (absolute_offset < _meta[mid].range.first) {
            end = mid - 1;
        } else {
            offset.segment = mid;
            break;
        }
    }

    // 这里不考虑没有找到的情况
    offset.offset = absolute_offset - _meta[offset.segment].range.first;
    return offset;
}

OLAPUnusedIndex::OLAPUnusedIndex() {

}

OLAPUnusedIndex::~OLAPUnusedIndex() {
    clear();
}

void OLAPUnusedIndex::start_delete_unused_index() {
    _mutex.lock();

    for (unused_index_list_t::iterator it = _unused_index_list.begin();
            it != _unused_index_list.end();) {
        if (!(*it)->is_in_use()) {
            OLAP_LOG_TRACE("deleting index succeed, it is in use. [version=%d,%d version_hash=%lu]",
                           (*it)->version().first,
                           (*it)->version().second,
                           (*it)->version_hash());
            (*it)->delete_all_files();
            SAFE_DELETE(*it);
            it = _unused_index_list.erase(it);
        } else {
            OLAP_LOG_TRACE("fail to delete index, it is in use. [version=%d,%d version_hash=%lu]",
                           (*it)->version().first, (*it)->version().second,
                           (*it)->version_hash());
            ++it;
        }
    }

    _mutex.unlock();
}

void OLAPUnusedIndex::add_unused_index(OLAPIndex* olap_index) {
    _mutex.lock();

    unused_index_list_t::iterator iter = find(_unused_index_list.begin(),
                                              _unused_index_list.end(),
                                              olap_index);
    if (iter == _unused_index_list.end()) {
        _unused_index_list.push_back(olap_index);
    }

    _mutex.unlock();
}

}  // namespace palo
