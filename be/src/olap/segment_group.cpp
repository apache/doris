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

#include "olap/segment_group.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <fstream>

#include "olap/column_data.h"
#include "olap/olap_table.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/utils.h"
#include "olap/column_mapping.h"

using std::ifstream;
using std::string;
using std::vector;

namespace doris {

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

SegmentGroup::SegmentGroup(OLAPTable* table, Version version, VersionHash version_hash,
                     bool delete_flag, int32_t segment_group_id, int32_t num_segments)
      : _table(table),
        _version(version),
        _version_hash(version_hash),
        _delete_flag(delete_flag),
        _segment_group_id(segment_group_id),
        _num_segments(num_segments) {
    _index_loaded = false;
    _ref_count = 0;
    _is_pending = false;
    _partition_id = 0;
    _transaction_id = 0;
    _short_key_length = 0;
    _new_short_key_length = 0;
    _short_key_buf = nullptr;
    _file_created = false;
    _new_segment_created = false;
    _empty = false;

    const RowFields& tablet_schema = _table->tablet_schema();
    for (size_t i = 0; i < _table->num_short_key_fields(); ++i) {
        _short_key_info_list.push_back(tablet_schema[i]);
        _short_key_length += tablet_schema[i].index_length + 1;// 1 for null byte
        if (tablet_schema[i].type == OLAP_FIELD_TYPE_CHAR ||
            tablet_schema[i].type == OLAP_FIELD_TYPE_VARCHAR) {
            _new_short_key_length += sizeof(Slice) + 1;
        } else {
            _new_short_key_length += tablet_schema[i].index_length + 1;
        }
    }
}

SegmentGroup::SegmentGroup(OLAPTable* table, bool delete_flag,
                     int32_t segment_group_id, int32_t num_segments, bool is_pending,
                     TPartitionId partition_id, TTransactionId transaction_id)
    : _table(table), _delete_flag(delete_flag),
      _segment_group_id(segment_group_id), _num_segments(num_segments),
      _is_pending(is_pending), _partition_id(partition_id),
      _transaction_id(transaction_id)
{
    _version = {-1, -1};
    _version_hash = 0;
    _load_id.set_hi(0);
    _load_id.set_lo(0);
    _index_loaded = false;
    _ref_count = 0;
    _short_key_length = 0;
    _new_short_key_length = 0;
    _short_key_buf = NULL;
    _file_created = false;
    _new_segment_created = false;
    _empty = false;

    const RowFields& tablet_schema = _table->tablet_schema();
    for (size_t i = 0; i < _table->num_short_key_fields(); ++i) {
        _short_key_info_list.push_back(tablet_schema[i]);
        _short_key_length += tablet_schema[i].index_length + 1;// 1 for null byte
        if (tablet_schema[i].type == OLAP_FIELD_TYPE_CHAR ||
            tablet_schema[i].type == OLAP_FIELD_TYPE_VARCHAR) {
            _new_short_key_length += sizeof(Slice) + 1;
        } else {
            _new_short_key_length += tablet_schema[i].index_length + 1;
        }
    }
}

SegmentGroup::~SegmentGroup() {
    delete [] _short_key_buf;
    _current_file_handler.close();

    for (size_t i = 0; i < _column_statistics.size(); ++i) {
        SAFE_DELETE(_column_statistics[i].first);
        SAFE_DELETE(_column_statistics[i].second);
    }
    _seg_pb_map.clear();
}

string SegmentGroup::construct_index_file_path(int32_t segment_group_id, int32_t segment) const {
    if (_is_pending) {
        return _table->construct_pending_index_file_path(_transaction_id, _segment_group_id, segment);
    } else {
        return _table->construct_index_file_path(_version, _version_hash, _segment_group_id, segment);
    }
}

string SegmentGroup::construct_data_file_path(int32_t segment_group_id, int32_t segment) const {
    if (_is_pending) {
        return _table->construct_pending_data_file_path(_transaction_id, segment_group_id, segment);
    } else {
        return _table->construct_data_file_path(_version, _version_hash, segment_group_id, segment);
    }
}

void SegmentGroup::publish_version(Version version, VersionHash version_hash) {
    _version = version;
    _version_hash = version_hash;
}

void SegmentGroup::acquire() {
    atomic_inc(&_ref_count);
}

int64_t SegmentGroup::ref_count() {
    return _ref_count;
}

void SegmentGroup::release() {
    atomic_dec(&_ref_count);
}

bool SegmentGroup::is_in_use() {
    return _ref_count > 0;
}

// you can not use SegmentGroup after delete_all_files(), or else unknown behavior occurs.
void SegmentGroup::delete_all_files() {
    if (!_file_created) { return; }
    for (uint32_t seg_id = 0; seg_id < _num_segments; ++seg_id) {
        // get full path for one segment
        string index_path = construct_index_file_path(_segment_group_id, seg_id);
        string data_path = construct_data_file_path(_segment_group_id, seg_id);

        if (remove(index_path.c_str()) != 0) {
            char errmsg[64];
            LOG(WARNING) << "fail to delete index file. [err='" << strerror_r(errno, errmsg, 64)
                         << "' path='" << index_path << "']";
        }

        if (remove(data_path.c_str()) != 0) {
            char errmsg[64];
            LOG(WARNING) << "fail to delete data file. [err='" << strerror_r(errno, errmsg, 64)
                         << "' path='" << data_path << "']";
        }
    }
}


OLAPStatus SegmentGroup::add_column_statistics_for_linked_schema_change(
        const std::vector<std::pair<WrapperField*, WrapperField*>>& column_statistic_fields,
        const SchemaMapping& schema_mapping) {
    //When add rollup table, the base table index maybe empty
    if (column_statistic_fields.size() == 0) {
        return OLAP_SUCCESS;
    }

    //1 for LinkedSchemaChange, the rollup table keys order is the same as base table
    //2 when user add a new key column to base table, _table->num_key_fields() size will
    // greater than _column_statistics size
    int num_new_keys = 0;
    for (size_t i = 0; i < _table->num_key_fields(); ++i) {
        const FieldInfo& column_schema = _table->tablet_schema()[i];

        WrapperField* first = WrapperField::create(column_schema);
        DCHECK(first != NULL) << "failed to allocate memory for field: " << i;

        WrapperField* second = WrapperField::create(column_schema);
        DCHECK(second != NULL) << "failed to allocate memory for field: " << i;

        //for new key column, use default value to fill into column_statistics
        if (schema_mapping[i].ref_column == -1) {
            num_new_keys++;

            first->copy(schema_mapping[i].default_value);
            second->copy(schema_mapping[i].default_value);
        } else {
            first->copy(column_statistic_fields[i - num_new_keys].first);
            second->copy(column_statistic_fields[i - num_new_keys].second);
        }

        _column_statistics.push_back(std::make_pair(first, second));
    }
    return OLAP_SUCCESS;
}

OLAPStatus SegmentGroup::add_column_statistics(
        const std::vector<std::pair<WrapperField*, WrapperField*>>& column_statistic_fields) {
    DCHECK(column_statistic_fields.size() == _table->num_key_fields());
    for (size_t i = 0; i < column_statistic_fields.size(); ++i) {
        WrapperField* first = WrapperField::create(_table->tablet_schema()[i]);
        DCHECK(first != NULL) << "failed to allocate memory for field: " << i;
        first->copy(column_statistic_fields[i].first);

        WrapperField* second = WrapperField::create(_table->tablet_schema()[i]);
        DCHECK(second != NULL) << "failed to allocate memory for field: " << i;
        second->copy(column_statistic_fields[i].second);

        _column_statistics.push_back(std::make_pair(first, second));
    }
    return OLAP_SUCCESS;
}

OLAPStatus SegmentGroup::add_column_statistics(
        std::vector<std::pair<std::string, std::string> > &column_statistic_strings,
        std::vector<bool> &null_vec) {
    DCHECK(column_statistic_strings.size() == _table->num_key_fields());
    for (size_t i = 0; i < column_statistic_strings.size(); ++i) {
        WrapperField* first = WrapperField::create(_table->tablet_schema()[i]);
        DCHECK(first != NULL) << "failed to allocate memory for field: " << i ;
        RETURN_NOT_OK(first->from_string(column_statistic_strings[i].first));
        if (null_vec[i]) {
            //[min, max] -> [NULL, max]
            first->set_null();
        }
        WrapperField* second = WrapperField::create(_table->tablet_schema()[i]);
        DCHECK(first != NULL) << "failed to allocate memory for field: " << i ;
        RETURN_NOT_OK(second->from_string(column_statistic_strings[i].second));
        _column_statistics.push_back(std::make_pair(first, second));
    }
    return OLAP_SUCCESS;
}

OLAPStatus SegmentGroup::load() {
    if (_empty) {
        return OLAP_SUCCESS;
    }
    OLAPStatus res = OLAP_ERR_INDEX_LOAD_ERROR;
    boost::lock_guard<boost::mutex> guard(_index_load_lock);

    if (_index_loaded) {
        return OLAP_SUCCESS;
    }

    if (_num_segments == 0) {
        OLAP_LOG_WARNING("fail to load index, segments number is 0.");
        return res;
    }

    if (_index.init(_short_key_length, _new_short_key_length,
                    _table->num_short_key_fields(), &_short_key_info_list) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to create MemIndex. [num_segment=%d]", _num_segments);
        return res;
    }

    // for each segment
    for (uint32_t seg_id = 0; seg_id < _num_segments; ++seg_id) {
        if (COLUMN_ORIENTED_FILE == _table->data_file_type()) {
            string seg_path = construct_data_file_path(_segment_group_id, seg_id);
            if (OLAP_SUCCESS != (res = load_pb(seg_path.c_str(), seg_id))) {
                LOG(WARNING) << "failed to load pb structures. [seg_path='" << seg_path << "']";
                _check_io_error(res);
                return res;
            }
        }

        // get full path for one segment
        string path = construct_index_file_path(_segment_group_id, seg_id);
        if ((res = _index.load_segment(path.c_str(), &_current_num_rows_per_row_block))
                != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to load segment. [path='" << path << "']";
            _check_io_error(res);
            return res;
        }
    }

    _delete_flag = _index.delete_flag();
    _index_loaded = true;
    _file_created = true;

    return OLAP_SUCCESS;
}

OLAPStatus SegmentGroup::load_pb(const char* file, uint32_t seg_id) {
    OLAPStatus res = OLAP_SUCCESS;

    FileHeader<ColumnDataHeaderMessage> seg_file_header;
    FileHandler seg_file_handler;
    res = seg_file_handler.open(file, O_RDONLY);
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

bool SegmentGroup::index_loaded() {
    return _index_loaded;
}

OLAPStatus SegmentGroup::validate() {
    if (_empty) {
        return OLAP_SUCCESS;
    }

    OLAPStatus res = OLAP_SUCCESS;
    for (uint32_t seg_id = 0; seg_id < _num_segments; ++seg_id) {
        FileHeader<OLAPIndexHeaderMessage, OLAPIndexFixedHeader> index_file_header;
        FileHeader<OLAPDataHeaderMessage> data_file_header;

        // get full path for one segment
        string index_path = construct_index_file_path(_segment_group_id, seg_id);
        string data_path = construct_data_file_path(_segment_group_id, seg_id);

        // 检查index文件头
        if ((res = index_file_header.validate(index_path)) != OLAP_SUCCESS) {
            LOG(WARNING) << "validate index file error. [file='" << index_path << "']";
            _check_io_error(res);
            return res;
        }

        // 检查data文件头
        if ((res = data_file_header.validate(data_path)) != OLAP_SUCCESS) {
            LOG(WARNING) << "validate data file error. [file='" << data_path << "']";
            _check_io_error(res);
            return res;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus SegmentGroup::find_row_block(const RowCursor& key,
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

OLAPStatus SegmentGroup::find_short_key(const RowCursor& key,
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

    VLOG(3) << "seg=" << offset.segment << ", offset=" << offset.offset;
    return _index.get_row_block_position(offset, pos);
}

OLAPStatus SegmentGroup::get_row_block_entry(const RowBlockPosition& pos, EntrySlice* entry) const {
    TABLE_PARAM_VALIDATE();
    SLICE_PARAM_VALIDATE(entry);
    
    return _index.get_entry(_index.get_offset(pos), entry);
}

OLAPStatus SegmentGroup::find_first_row_block(RowBlockPosition* position) const {
    TABLE_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(position);
    
    return _index.get_row_block_position(_index.find_first(), position);
}

OLAPStatus SegmentGroup::find_last_row_block(RowBlockPosition* position) const {
    TABLE_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(position);
    
    return _index.get_row_block_position(_index.find_last(), position);
}

OLAPStatus SegmentGroup::find_next_row_block(RowBlockPosition* pos, bool* eof) const {
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

OLAPStatus SegmentGroup::find_mid_point(const RowBlockPosition& low,
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

OLAPStatus SegmentGroup::find_prev_point(
        const RowBlockPosition& current, RowBlockPosition* prev) const {
    OLAPIndexOffset current_offset = _index.get_offset(current);
    OLAPIndexOffset prev_offset = _index.prev(current_offset);

    return _index.get_row_block_position(prev_offset, prev);
}

OLAPStatus SegmentGroup::advance_row_block(int64_t num_row_blocks, RowBlockPosition* position) const {
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
uint32_t SegmentGroup::compute_distance(const RowBlockPosition& position1,
                                     const RowBlockPosition& position2) const {
    iterator_offset_t offset1 = _index.get_absolute_offset(_index.get_offset(position1));
    iterator_offset_t offset2 = _index.get_absolute_offset(_index.get_offset(position2));

    return offset2 > offset1 ? offset2 - offset1 : 0;
}

OLAPStatus SegmentGroup::add_segment() {
    // 打开文件
    ++_num_segments;

    OLAPIndexHeaderMessage* index_header = NULL;
    // 构造Proto格式的Header
    index_header = _file_header.mutable_message();
    index_header->set_start_version(_version.first);
    index_header->set_end_version(_version.second);
    index_header->set_cumulative_version_hash(_version_hash);
    index_header->set_segment(_num_segments - 1);
    index_header->set_num_rows_per_block(_table->num_rows_per_row_block());
    index_header->set_delete_flag(_delete_flag);
    index_header->set_null_supported(true);

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

OLAPStatus SegmentGroup::add_row_block(const RowBlock& row_block, const uint32_t data_offset) {
    // get first row of the row_block to distill index item.
    row_block.get_row(0, &_current_index_row);
    return add_short_key(_current_index_row, data_offset);
}

OLAPStatus SegmentGroup::add_short_key(const RowCursor& short_key, const uint32_t data_offset) {
    OLAPStatus res = OLAP_SUCCESS;
    if (!_new_segment_created) {
        string file_path = construct_index_file_path(_segment_group_id, _num_segments - 1);
        res = _current_file_handler.open_with_mode(
                        file_path.c_str(), O_CREAT | O_EXCL | O_WRONLY, S_IRUSR | S_IWUSR);
        if (res != OLAP_SUCCESS) {
            char errmsg[64];
            LOG(WARNING) << "can not create file. [file_path='" << file_path
                << "' err='" << strerror_r(errno, errmsg, 64) << "']";
            _check_io_error(res);
            return res;
        }
        _file_created = true;
        _new_segment_created = true;

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
    }

    // 将short key的内容写入_short_key_buf
    size_t offset = 0;

    //short_key.write_null_array(_short_key_buf);
    //offset += short_key.get_num_null_byte();
    for (size_t i = 0; i < _short_key_info_list.size(); i++) {
        short_key.write_index_by_index(i, _short_key_buf + offset);
        offset += short_key.get_index_size(i) + 1;
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

OLAPStatus SegmentGroup::finalize_segment(uint32_t data_segment_size, int64_t num_rows) {
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

    VLOG(3) << "finalize_segment. file_name=" << _current_file_handler.file_name()
            << ", file_length=" << file_length;

    if ((res = _current_file_handler.close()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("close file error. [err=%m]");
        _check_io_error(res);
        return res;
    }

    _new_segment_created = false;
    return OLAP_SUCCESS;
}

void SegmentGroup::sync() {
    if (_current_file_handler.sync() == -1) {
        OLAP_LOG_WARNING("fail to sync file.[err=%m]");
        _table->set_io_error();
    }
}

void SegmentGroup::_check_io_error(OLAPStatus res) {
    if (is_io_error(res)) {
        _table->set_io_error();
    }
}

uint64_t SegmentGroup::num_index_entries() const {
    return _index.count();
}

}
