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

#include "olap/rowset/segment_group.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <fstream>
#include <sstream>

#include "olap/column_mapping.h"
#include "olap/data_dir.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/rowset/column_data.h"
#include "olap/schema.h"
#include "olap/storage_engine.h"
#include "olap/utils.h"
#include "olap/wrapper_field.h"
#include "util/file_utils.h"
#include "util/stack_util.h"

using std::ifstream;
using std::string;
using std::vector;

namespace doris {

#define SEGMENT_GROUP_PARAM_VALIDATE()                                                   \
    do {                                                                                 \
        if (!_index_loaded) {                                                            \
            OLAP_LOG_WARNING("fail to find, index is not loaded. [segment_group_id=%d]", \
                             _segment_group_id);                                         \
            return Status::OLAPInternalError(OLAP_ERR_NOT_INITED);                       \
        }                                                                                \
    } while (0);

#define POS_PARAM_VALIDATE(pos)                                               \
    do {                                                                      \
        if (nullptr == pos) {                                                 \
            OLAP_LOG_WARNING("fail to find, nullptr position parameter.");    \
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR); \
        }                                                                     \
    } while (0);

#define SLICE_PARAM_VALIDATE(slice)                                           \
    do {                                                                      \
        if (nullptr == slice) {                                               \
            OLAP_LOG_WARNING("fail to find, nullptr slice parameter.");       \
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR); \
        }                                                                     \
    } while (0);

SegmentGroup::SegmentGroup(int64_t tablet_id, const RowsetId& rowset_id, const TabletSchema* schema,
                           const std::string& rowset_path_prefix, Version version, bool delete_flag,
                           int32_t segment_group_id, int32_t num_segments)
        : _tablet_id(tablet_id),
          _rowset_id(rowset_id),
          _schema(schema),
          _rowset_path_prefix(rowset_path_prefix),
          _version(version),
          _delete_flag(delete_flag),
          _segment_group_id(segment_group_id),
          _num_segments(num_segments) {
    _index_loaded = false;
    _ref_count = 0;
    _is_pending = false;
    _partition_id = 0;
    _txn_id = 0;
    _short_key_length = 0;
    _new_short_key_length = 0;
    _short_key_buf = nullptr;
    _new_segment_created = false;
    _empty = false;

    for (size_t i = 0; i < _schema->num_short_key_columns(); ++i) {
        const TabletColumn& column = _schema->column(i);
        _short_key_columns.push_back(column);
        _short_key_length += column.index_length() + 1; // 1 for null byte
        if (column.type() == OLAP_FIELD_TYPE_CHAR || column.type() == OLAP_FIELD_TYPE_VARCHAR ||
            column.type() == OLAP_FIELD_TYPE_STRING) {
            _new_short_key_length += sizeof(Slice) + 1;
        } else {
            _new_short_key_length += column.index_length() + 1;
        }
    }
}

SegmentGroup::SegmentGroup(int64_t tablet_id, const RowsetId& rowset_id, const TabletSchema* schema,
                           const std::string& rowset_path_prefix, bool delete_flag,
                           int32_t segment_group_id, int32_t num_segments, bool is_pending,
                           TPartitionId partition_id, TTransactionId transaction_id)
        : _tablet_id(tablet_id),
          _rowset_id(rowset_id),
          _schema(schema),
          _rowset_path_prefix(rowset_path_prefix),
          _delete_flag(delete_flag),
          _segment_group_id(segment_group_id),
          _num_segments(num_segments),
          _is_pending(is_pending),
          _partition_id(partition_id),
          _txn_id(transaction_id) {
    _version = {-1, -1};
    _load_id.set_hi(0);
    _load_id.set_lo(0);
    _index_loaded = false;
    _ref_count = 0;
    _short_key_length = 0;
    _new_short_key_length = 0;
    _short_key_buf = nullptr;
    _new_segment_created = false;
    _empty = false;

    for (size_t i = 0; i < _schema->num_short_key_columns(); ++i) {
        const TabletColumn& column = _schema->column(i);
        _short_key_columns.push_back(column);
        _short_key_length += column.index_length() + 1; // 1 for null byte
        if (column.type() == OLAP_FIELD_TYPE_CHAR || column.type() == OLAP_FIELD_TYPE_VARCHAR ||
            column.type() == OLAP_FIELD_TYPE_STRING) {
            _new_short_key_length += sizeof(Slice) + 1;
        } else {
            _new_short_key_length += column.index_length() + 1;
        }
    }
}

SegmentGroup::~SegmentGroup() {
    delete[] _short_key_buf;
    _current_file_handler.close();

    for (size_t i = 0; i < _zone_maps.size(); ++i) {
        SAFE_DELETE(_zone_maps[i].first);
        SAFE_DELETE(_zone_maps[i].second);
    }
    _seg_pb_map.clear();
}

std::string SegmentGroup::_construct_file_name(int32_t segment_id, const string& suffix) const {
    // during convert from old files, the segment group id == -1, but we want to convert
    // it to 0
    int32_t tmp_sg_id = 0;
    if (_segment_group_id > 0) {
        tmp_sg_id = _segment_group_id;
    }
    std::string file_name = _rowset_id.to_string() + "_" + std::to_string(tmp_sg_id) + "_" +
                            std::to_string(segment_id) + suffix;
    return file_name;
}

std::string SegmentGroup::_construct_file_name(const RowsetId& rowset_id, int32_t segment_id,
                                               const string& suffix) const {
    std::string file_name = rowset_id.to_string() + "_" + std::to_string(_segment_group_id) + "_" +
                            std::to_string(segment_id) + suffix;
    return file_name;
}

std::string SegmentGroup::construct_index_file_path(const std::string& snapshot_path,
                                                    int32_t segment_id) const {
    std::string file_path = snapshot_path;
    file_path.append("/");
    file_path.append(_construct_file_name(segment_id, ".idx"));
    return file_path;
}

std::string SegmentGroup::construct_index_file_path(int32_t segment_id) const {
    return construct_index_file_path(_rowset_path_prefix, segment_id);
}

std::string SegmentGroup::construct_data_file_path(const std::string& snapshot_path,
                                                   int32_t segment_id) const {
    std::string file_path = snapshot_path;
    file_path.append("/");
    file_path.append(_construct_file_name(segment_id, ".dat"));
    return file_path;
}

std::string SegmentGroup::construct_data_file_path(int32_t segment_id) const {
    return construct_data_file_path(_rowset_path_prefix, segment_id);
}

void SegmentGroup::acquire() {
    ++_ref_count;
}

int64_t SegmentGroup::ref_count() {
    return _ref_count;
}

void SegmentGroup::release() {
    --_ref_count;
}

bool SegmentGroup::is_in_use() {
    return _ref_count > 0;
}

// you can not use SegmentGroup after delete_all_files(), or else unknown behavior occurs.
bool SegmentGroup::delete_all_files() {
    bool success = true;
    if (_empty) {
        return success;
    }
    for (uint32_t seg_id = 0; seg_id < _num_segments; ++seg_id) {
        // get full path for one segment
        string index_path = construct_index_file_path(seg_id);
        string data_path = construct_data_file_path(seg_id);

        VLOG_NOTICE << "delete index file. path=" << index_path;
        if (remove(index_path.c_str()) != 0) {
            // if the errno is not ENOENT, log the error msg.
            // ENOENT stands for 'No such file or directory'
            if (errno != ENOENT) {
                char errmsg[64];
                LOG(WARNING) << "fail to delete index file. err=" << strerror_r(errno, errmsg, 64)
                             << ", path=" << index_path;
                success = false;
            }
        }

        VLOG_NOTICE << "delete data file. path=" << data_path;
        if (remove(data_path.c_str()) != 0) {
            if (errno != ENOENT) {
                char errmsg[64];
                LOG(WARNING) << "fail to delete data file. err=" << strerror_r(errno, errmsg, 64)
                             << ", path=" << data_path;
                success = false;
            }
        }
    }
    return success;
}

Status SegmentGroup::add_zone_maps_for_linked_schema_change(
        const std::vector<std::pair<WrapperField*, WrapperField*>>& zone_map_fields,
        const SchemaMapping& schema_mapping) {
    //When add rollup tablet, the base tablet index maybe empty
    if (zone_map_fields.size() == 0) {
        return Status::OK();
    }

    // 1. rollup tablet get_num_zone_map_columns() will less than base tablet zone_map_fields.size().
    //    For LinkedSchemaChange, the rollup tablet keys order is the same as base tablet
    // 2. adding column to existed table, get_num_zone_map_columns() will larger than
    //    zone_map_fields.size()
    int zonemap_col_num = get_num_zone_map_columns();
    CHECK(zonemap_col_num <= schema_mapping.size())
            << zonemap_col_num << " vs. " << schema_mapping.size();

    for (size_t i = 0; i < zonemap_col_num; ++i) {
        // in duplicate/unique table update from 0.11 to 0.12, zone map index may be missed and may not a new column.
        if (_schema->keys_type() != AGG_KEYS && schema_mapping[i].ref_column != -1 &&
            schema_mapping[i].ref_column >= zone_map_fields.size()) {
            // the sequence of columns in _zone_maps and _schema must be consistent, so here
            // process should not add missed zonemap and we break the loop.
            break;
        }
        const TabletColumn& column = _schema->column(i);

        // nullptr is checked in olap_cond.cpp eval, note: When we apply column statistic, Field can be nullptr when type is Varchar.
        WrapperField* first = nullptr;
        WrapperField* second = nullptr;

        // when this is no ref_column (add new column), fill default value
        if (schema_mapping[i].ref_column == -1 ||
            schema_mapping[i].ref_column >= zone_map_fields.size()) {
            // ref_column == -1 means this is a new column.
            // for new column, use default value to fill into column_statistics
            if (schema_mapping[i].default_value != nullptr) {
                first = WrapperField::create(column);
                DCHECK(first != nullptr) << "failed to allocate memory for field: " << i;
                first->copy(schema_mapping[i].default_value);
                second = WrapperField::create(column);
                DCHECK(second != nullptr) << "failed to allocate memory for field: " << i;
                second->copy(schema_mapping[i].default_value);
            }
        } else {
            WrapperField* wfirst = zone_map_fields[schema_mapping[i].ref_column].first;
            WrapperField* wsecond = zone_map_fields[schema_mapping[i].ref_column].second;

            if (wfirst != nullptr) {
                first = WrapperField::create(column);
                DCHECK(first != nullptr) << "failed to allocate memory for field: " << i;
                first->copy(wfirst);
            }

            if (wsecond != nullptr) {
                second = WrapperField::create(column);
                DCHECK(second != nullptr) << "failed to allocate memory for field: " << i;
                second->copy(wsecond);
            }
        }

        // first and second can be nullptr, because when type is Varchar then default_value and zone_map_fields in old column
        // can be nullptr,  and it is checked in olap_cond.cpp eval function.
        _zone_maps.push_back(std::make_pair(first, second));
    }

    return Status::OK();
}

Status SegmentGroup::add_zone_maps(
        const std::vector<std::pair<WrapperField*, WrapperField*>>& zone_map_fields) {
    DCHECK(_empty || zone_map_fields.size() == get_num_zone_map_columns());
    for (size_t i = 0; i < zone_map_fields.size(); ++i) {
        const TabletColumn& column = _schema->column(i);
        WrapperField* first = WrapperField::create(column);
        DCHECK(first != nullptr) << "failed to allocate memory for field: " << i;
        first->copy(zone_map_fields[i].first);

        WrapperField* second = WrapperField::create(column);
        DCHECK(second != nullptr) << "failed to allocate memory for field: " << i;
        second->copy(zone_map_fields[i].second);

        _zone_maps.push_back(std::make_pair(first, second));
    }
    return Status::OK();
}

Status SegmentGroup::add_zone_maps(
        std::vector<std::pair<std::string, std::string>>& zone_map_strings,
        std::vector<bool>& null_vec) {
    DCHECK(_empty || zone_map_strings.size() <= get_num_zone_map_columns());
    for (size_t i = 0; i < zone_map_strings.size(); ++i) {
        const TabletColumn& column = _schema->column(i);
        WrapperField* first = WrapperField::create(column);
        DCHECK(first != nullptr) << "failed to allocate memory for field: " << i;
        RETURN_NOT_OK(first->from_string(zone_map_strings[i].first));
        if (null_vec[i]) {
            //[min, max] -> [nullptr, max]
            first->set_null();
        }
        WrapperField* second = WrapperField::create(column);
        DCHECK(first != nullptr) << "failed to allocate memory for field: " << i;
        RETURN_NOT_OK(second->from_string(zone_map_strings[i].second));
        _zone_maps.push_back(std::make_pair(first, second));
    }
    return Status::OK();
}

Status SegmentGroup::load(bool use_cache) {
    if (_empty) {
        _index_loaded = true;
        return Status::OK();
    }
    Status res = Status::OLAPInternalError(OLAP_ERR_INDEX_LOAD_ERROR);
    std::lock_guard<std::mutex> guard(_index_load_lock);

    if (_index_loaded) {
        return Status::OK();
    }

    if (_num_segments == 0) {
        LOG(WARNING) << "fail to load index, segments number is 0.";
        return res;
    }

    if (_index.init(_short_key_length, _new_short_key_length, _schema->num_short_key_columns(),
                    &_short_key_columns) != Status::OK()) {
        LOG(WARNING) << "fail to create MemIndex. num_segment=" << _num_segments;
        return res;
    }

    // for each segment
    for (uint32_t seg_id = 0; seg_id < _num_segments; ++seg_id) {
        string seg_path = construct_data_file_path(seg_id);
        if (!(res = load_pb(seg_path.c_str(), seg_id))) {
            LOG(WARNING) << "failed to load pb structures. [seg_path='" << seg_path << "']";

            return res;
        }

        // get full path for one segment
        std::string path = construct_index_file_path(seg_id);
        if ((res = _index.load_segment(path.c_str(), &_current_num_rows_per_row_block,
                                       use_cache)) != Status::OK()) {
            LOG(WARNING) << "fail to load segment. [path='" << path << "']";

            return res;
        }
    }

    _delete_flag = _index.delete_flag();
    _index_loaded = true;

    return Status::OK();
}

Status SegmentGroup::load_pb(const char* file, uint32_t seg_id) {
    Status res = Status::OK();

    FileHeader<ColumnDataHeaderMessage> seg_file_header;
    FileHandler seg_file_handler;
    res = seg_file_handler.open(file, O_RDONLY);
    if (!res.ok()) {
        LOG(WARNING) << "failed to open segment file. err=" << res << ", file=" << file;
        return res;
    }

    res = seg_file_header.unserialize(&seg_file_handler);
    if (!res.ok()) {
        seg_file_handler.close();
        LOG(WARNING) << "fail to unserialize header. err=" << res << ", path=" << file;
        return res;
    }

    _seg_pb_map[seg_id] = seg_file_header;
    seg_file_handler.close();
    return Status::OK();
}

bool SegmentGroup::index_loaded() {
    return _index_loaded;
}

Status SegmentGroup::validate() {
    if (_empty) {
        return Status::OK();
    }

    Status res = Status::OK();
    for (uint32_t seg_id = 0; seg_id < _num_segments; ++seg_id) {
        FileHeader<OLAPIndexHeaderMessage, OLAPIndexFixedHeader> index_file_header;
        FileHeader<OLAPDataHeaderMessage> data_file_header;

        // get full path for one segment
        string index_path = construct_index_file_path(seg_id);
        string data_path = construct_data_file_path(seg_id);

        // 检查index文件头
        if ((res = index_file_header.validate(index_path)) != Status::OK()) {
            LOG(WARNING) << "validate index file error. [file='" << index_path << "']";
            return res;
        }

        // 检查data文件头
        if ((res = data_file_header.validate(data_path)) != Status::OK()) {
            LOG(WARNING) << "validate data file error. [file='" << data_path << "']";
            return res;
        }
    }

    return Status::OK();
}

bool SegmentGroup::check() {
    // if the segment group is converted from old files, _empty == false but _num_segments == 0
    if (_empty && (_num_segments > 0 || !zero_num_rows())) {
        LOG(WARNING) << "invalid num segments for empty segment group, _num_segments:"
                     << _num_segments << ",num rows:" << num_rows();
        return false;
    }
    return true;
}

Status SegmentGroup::find_short_key(const RowCursor& key, RowCursor* helper_cursor, bool find_last,
                                    RowBlockPosition* pos) const {
    SEGMENT_GROUP_PARAM_VALIDATE();
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

    VLOG_NOTICE << "seg=" << offset.segment << ", offset=" << offset.offset;
    return _index.get_row_block_position(offset, pos);
}

Status SegmentGroup::get_row_block_entry(const RowBlockPosition& pos, EntrySlice* entry) const {
    SEGMENT_GROUP_PARAM_VALIDATE();
    SLICE_PARAM_VALIDATE(entry);

    return _index.get_entry(_index.get_offset(pos), entry);
}

Status SegmentGroup::find_first_row_block(RowBlockPosition* position) const {
    SEGMENT_GROUP_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(position);

    return _index.get_row_block_position(_index.find_first(), position);
}

Status SegmentGroup::find_last_row_block(RowBlockPosition* position) const {
    SEGMENT_GROUP_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(position);

    return _index.get_row_block_position(_index.find_last(), position);
}

Status SegmentGroup::find_next_row_block(RowBlockPosition* pos, bool* eof) const {
    SEGMENT_GROUP_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(pos);
    POS_PARAM_VALIDATE(eof);

    OLAPIndexOffset current = _index.get_offset(*pos);
    *eof = false;

    OLAPIndexOffset next = _index.next(current);
    if (next == _index.end()) {
        *eof = true;
        return Status::OLAPInternalError(OLAP_ERR_INDEX_EOF);
    }

    return _index.get_row_block_position(next, pos);
}

Status SegmentGroup::find_mid_point(const RowBlockPosition& low, const RowBlockPosition& high,
                                    RowBlockPosition* output, uint32_t* dis) const {
    *dis = compute_distance(low, high);
    if (*dis >= _index.count()) {
        return Status::OLAPInternalError(OLAP_ERR_INDEX_EOF);
    } else {
        *output = low;
        if (advance_row_block(*dis / 2, output) != Status::OK()) {
            return Status::OLAPInternalError(OLAP_ERR_INDEX_EOF);
        }

        return Status::OK();
    }
}

Status SegmentGroup::find_prev_point(const RowBlockPosition& current,
                                     RowBlockPosition* prev) const {
    OLAPIndexOffset current_offset = _index.get_offset(current);
    OLAPIndexOffset prev_offset = _index.prev(current_offset);

    return _index.get_row_block_position(prev_offset, prev);
}

Status SegmentGroup::advance_row_block(int64_t num_row_blocks, RowBlockPosition* position) const {
    SEGMENT_GROUP_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(position);

    OLAPIndexOffset off = _index.get_offset(*position);
    iterator_offset_t absolute_offset = _index.get_absolute_offset(off) + num_row_blocks;
    if (absolute_offset >= _index.count()) {
        return Status::OLAPInternalError(OLAP_ERR_INDEX_EOF);
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

Status SegmentGroup::add_segment() {
    // 打开文件
    ++_num_segments;

    OLAPIndexHeaderMessage* index_header = nullptr;
    // 构造Proto格式的Header
    index_header = _file_header.mutable_message();
    index_header->set_start_version(_version.first);
    index_header->set_end_version(_version.second);
    // Version hash is useless but it is a required field in header message pb
    index_header->set_cumulative_version_hash(0);
    index_header->set_segment(_num_segments - 1);
    index_header->set_num_rows_per_block(_schema->num_rows_per_row_block());
    index_header->set_delete_flag(_delete_flag);
    index_header->set_null_supported(true);

    // 分配一段存储short key的内存, 初始化index_row
    if (_short_key_buf == nullptr) {
        _short_key_buf = new (std::nothrow) char[_short_key_length];
        if (_short_key_buf == nullptr) {
            OLAP_LOG_WARNING("malloc short_key_buf error.");
            return Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
        }

        memset(_short_key_buf, 0, _short_key_length);
        if (_current_index_row.init(*_schema) != Status::OK()) {
            OLAP_LOG_WARNING("init _current_index_row fail.");
            return Status::OLAPInternalError(OLAP_ERR_INIT_FAILED);
        }
    }

    // 初始化checksum
    _checksum = ADLER32_INIT;
    return Status::OK();
}

Status SegmentGroup::add_row_block(const RowBlock& row_block, const uint32_t data_offset) {
    // get first row of the row_block to distill index item.
    row_block.get_row(0, &_current_index_row);
    return add_short_key(_current_index_row, data_offset);
}

Status SegmentGroup::add_short_key(const RowCursor& short_key, const uint32_t data_offset) {
    Status res = Status::OK();
    if (!_new_segment_created) {
        string file_path = construct_index_file_path(_num_segments - 1);
        StorageEngine* engine = StorageEngine::instance();
        if (engine != nullptr) {
            std::filesystem::path tablet_path(_rowset_path_prefix);
            std::filesystem::path data_dir_path =
                    tablet_path.parent_path().parent_path().parent_path().parent_path();
            std::string data_dir_string = data_dir_path.string();
            DataDir* data_dir = engine->get_store(data_dir_string);
            data_dir->add_pending_ids(ROWSET_ID_PREFIX + _rowset_id.to_string());
        }
        res = _current_file_handler.open_with_mode(file_path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                                                   S_IRUSR | S_IWUSR);
        if (!res.ok()) {
            char errmsg[64];
            LOG(WARNING) << "can not create file. file_path=" << file_path << ", err='"
                         << strerror_r(errno, errmsg, 64);
            return res;
        }
        _new_segment_created = true;

        // 准备FileHeader
        if ((res = _file_header.prepare(&_current_file_handler)) != Status::OK()) {
            OLAP_LOG_WARNING("write file header error. [err=%m]");
            return res;
        }

        // 跳过FileHeader
        if (_current_file_handler.seek(_file_header.size(), SEEK_SET) == -1) {
            OLAP_LOG_WARNING("lseek header file error. [err=%m]");
            res = Status::OLAPInternalError(OLAP_ERR_IO_ERROR);
            return res;
        }
    }

    // 将short key的内容写入_short_key_buf
    size_t offset = 0;

    //short_key.write_null_array(_short_key_buf);
    //offset += short_key.get_num_null_byte();
    for (size_t i = 0; i < _short_key_columns.size(); i++) {
        short_key.write_index_by_index(i, _short_key_buf + offset);
        offset += short_key.get_index_size(i) + 1;
    }

    // 写入Short Key对应的数据
    if ((res = _current_file_handler.write(_short_key_buf, _short_key_length)) != Status::OK()) {
        OLAP_LOG_WARNING("write short key failed. [err=%m]");

        return res;
    }

    // 写入对应的数据文件偏移量
    if ((res = _current_file_handler.write(&data_offset, sizeof(data_offset))) != Status::OK()) {
        OLAP_LOG_WARNING("write data_offset failed. [err=%m]");
        return res;
    }

    _checksum = olap_adler32(_checksum, _short_key_buf, _short_key_length);
    _checksum = olap_adler32(_checksum, reinterpret_cast<const char*>(&data_offset),
                             sizeof(data_offset));
    return Status::OK();
}

Status SegmentGroup::finalize_segment(uint32_t data_segment_size, int64_t num_rows) {
    // 准备FileHeader
    Status res = Status::OK();

    int file_length = _current_file_handler.tell();
    if (file_length == -1) {
        LOG(WARNING) << "get file_length error. err=" << Errno::no()
                     << ", _new_segment_created=" << _new_segment_created;
        return Status::OLAPInternalError(OLAP_ERR_IO_ERROR);
    }

    _file_header.set_file_length(file_length);
    _file_header.set_checksum(_checksum);
    _file_header.mutable_extra()->data_length = data_segment_size;
    _file_header.mutable_extra()->num_rows = num_rows;

    // 写入更新之后的FileHeader
    if ((res = _file_header.serialize(&_current_file_handler)) != Status::OK()) {
        OLAP_LOG_WARNING("write file header error. [err=%m]");

        return res;
    }

    VLOG_NOTICE << "finalize_segment. file_name=" << _current_file_handler.file_name()
                << ", file_length=" << file_length;

    if ((res = _current_file_handler.close()) != Status::OK()) {
        OLAP_LOG_WARNING("close file error. [err=%m]");

        return res;
    }

    _new_segment_created = false;
    return Status::OK();
}

uint64_t SegmentGroup::num_index_entries() const {
    return _index.count();
}

size_t SegmentGroup::current_num_rows_per_row_block() const {
    return _current_num_rows_per_row_block;
}

const TabletSchema& SegmentGroup::get_tablet_schema() {
    return *_schema;
}

int SegmentGroup::get_num_zone_map_columns() {
    return _schema->num_key_columns();
}

int SegmentGroup::get_num_key_columns() {
    return _schema->num_key_columns();
}

int SegmentGroup::get_num_short_key_columns() {
    return _schema->num_short_key_columns();
}

size_t SegmentGroup::get_num_rows_per_row_block() {
    return _schema->num_rows_per_row_block();
}

std::string SegmentGroup::rowset_path_prefix() {
    return _rowset_path_prefix;
}

int64_t SegmentGroup::get_tablet_id() {
    return _tablet_id;
}

Status SegmentGroup::copy_files_to(const std::string& dir) {
    if (_empty) {
        return Status::OK();
    }
    for (int segment_id = 0; segment_id < _num_segments; segment_id++) {
        std::string dest_data_file = construct_data_file_path(dir, segment_id);
        if (FileUtils::check_exist(dest_data_file)) {
            LOG(WARNING) << "file already exists:" << dest_data_file;
            return Status::OLAPInternalError(OLAP_ERR_FILE_ALREADY_EXIST);
        }
        std::string data_file_to_copy = construct_data_file_path(segment_id);
        if (!FileUtils::copy_file(data_file_to_copy, dest_data_file).ok()) {
            LOG(WARNING) << "fail to copy data file. from=" << data_file_to_copy
                         << ", to=" << dest_data_file << ", errno=" << Errno::no();
            return Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
        }
        std::string dest_index_file = construct_index_file_path(dir, segment_id);
        if (FileUtils::check_exist(dest_index_file)) {
            LOG(WARNING) << "file already exists:" << dest_index_file;
            return Status::OLAPInternalError(OLAP_ERR_FILE_ALREADY_EXIST);
        }
        std::string index_file_to_copy = construct_index_file_path(segment_id);
        if (!FileUtils::copy_file(index_file_to_copy, dest_index_file).ok()) {
            LOG(WARNING) << "fail to copy index file. from=" << index_file_to_copy
                         << ", to=" << dest_index_file << ", errno=" << Errno::no();
            return Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
        }
    }
    return Status::OK();
}

// when convert from old files, remove existing files
// convert from old files in 2 cases:
//  case 1: clone from old version be
//  case 2: upgrade to new version be
Status SegmentGroup::convert_from_old_files(const std::string& snapshot_path,
                                            std::vector<std::string>* success_links) {
    if (_empty) {
        // the segment group is empty, it does not have files, just return
        return Status::OK();
    }
    for (int segment_id = 0; segment_id < _num_segments; segment_id++) {
        std::string new_data_file_name = construct_data_file_path(_rowset_path_prefix, segment_id);
        // if file exist should remove it because same file name does not mean same data
        if (FileUtils::check_exist(new_data_file_name)) {
            LOG(INFO) << "file already exist, remove it. file=" << new_data_file_name;
            RETURN_WITH_WARN_IF_ERROR(FileUtils::remove(new_data_file_name),
                                      Status::OLAPInternalError(OLAP_ERR_CANNOT_CREATE_DIR),
                                      "remove path failed. path=" + new_data_file_name);
        }
        std::string old_data_file_name = construct_old_data_file_path(snapshot_path, segment_id);
        if (link(old_data_file_name.c_str(), new_data_file_name.c_str()) != 0) {
            LOG(WARNING) << "fail to create hard link. from=" << old_data_file_name
                         << ", to=" << new_data_file_name << ", errno=" << Errno::no();
            return Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
        } else {
            VLOG_NOTICE << "link data file from " << old_data_file_name << " to "
                        << new_data_file_name << " successfully";
        }
        success_links->push_back(new_data_file_name);
        std::string new_index_file_name =
                construct_index_file_path(_rowset_path_prefix, segment_id);
        if (FileUtils::check_exist(new_index_file_name)) {
            LOG(INFO) << "file already exist, remove it. file=" << new_index_file_name;

            RETURN_WITH_WARN_IF_ERROR(FileUtils::remove(new_index_file_name),
                                      Status::OLAPInternalError(OLAP_ERR_CANNOT_CREATE_DIR),
                                      "remove path failed. path=" + new_index_file_name);
        }
        std::string old_index_file_name = construct_old_index_file_path(snapshot_path, segment_id);
        if (link(old_index_file_name.c_str(), new_index_file_name.c_str()) != 0) {
            LOG(WARNING) << "fail to create hard link. from=" << old_index_file_name
                         << ", to=" << new_index_file_name << ", errno=" << Errno::no();
            return Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
        } else {
            VLOG_NOTICE << "link index file from " << old_index_file_name << " to "
                        << new_index_file_name << " successfully";
        }
        success_links->push_back(new_index_file_name);
    }
    return Status::OK();
}

Status SegmentGroup::convert_to_old_files(const std::string& snapshot_path,
                                          std::vector<std::string>* success_links) {
    if (_empty) {
        return Status::OK();
    }
    for (int segment_id = 0; segment_id < _num_segments; segment_id++) {
        std::string new_data_file_name = construct_data_file_path(_rowset_path_prefix, segment_id);
        std::string old_data_file_name = construct_old_data_file_path(snapshot_path, segment_id);
        if (!FileUtils::check_exist(old_data_file_name)) {
            if (link(new_data_file_name.c_str(), old_data_file_name.c_str()) != 0) {
                LOG(WARNING) << "fail to create hard link. from=" << new_data_file_name << ", "
                             << "to=" << old_data_file_name << ", "
                             << "errno=" << Errno::no();
                return Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
            }
            success_links->push_back(old_data_file_name);
        }
        VLOG_NOTICE << "create hard link. from=" << new_data_file_name << ", "
                    << "to=" << old_data_file_name;
        std::string new_index_file_name =
                construct_index_file_path(_rowset_path_prefix, segment_id);
        std::string old_index_file_name = construct_old_index_file_path(snapshot_path, segment_id);
        if (!FileUtils::check_exist(old_index_file_name)) {
            if (link(new_index_file_name.c_str(), old_index_file_name.c_str()) != 0) {
                LOG(WARNING) << "fail to create hard link. from=" << new_index_file_name << ", "
                             << "to=" << old_index_file_name << ", "
                             << "errno=" << Errno::no();
                return Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
            }
            success_links->push_back(old_index_file_name);
        }
        VLOG_NOTICE << "create hard link. from=" << new_index_file_name << ", "
                    << "to=" << old_index_file_name;
    }
    return Status::OK();
}

Status SegmentGroup::remove_old_files(std::vector<std::string>* links_to_remove) {
    for (int segment_id = 0; segment_id < _num_segments; segment_id++) {
        std::string old_data_file_name =
                construct_old_data_file_path(_rowset_path_prefix, segment_id);
        if (FileUtils::check_exist(old_data_file_name)) {
            RETURN_WITH_WARN_IF_ERROR(FileUtils::remove(old_data_file_name),
                                      Status::OLAPInternalError(OLAP_ERR_CANNOT_CREATE_DIR),
                                      "remove path failed. path" + old_data_file_name);

            links_to_remove->push_back(old_data_file_name);
        }
        std::string old_index_file_name =
                construct_old_index_file_path(_rowset_path_prefix, segment_id);
        if (FileUtils::check_exist(old_index_file_name)) {
            RETURN_WITH_WARN_IF_ERROR(FileUtils::remove(old_index_file_name),
                                      Status::OLAPInternalError(OLAP_ERR_CANNOT_CREATE_DIR),
                                      "remove path failed. path" + old_index_file_name);

            links_to_remove->push_back(old_index_file_name);
        }
        // if segment group id == 0, it maybe convert from old files which do not have segment group id in file path
        if (_segment_group_id == 0) {
            old_data_file_name = _construct_err_sg_data_file_path(_rowset_path_prefix, segment_id);
            if (FileUtils::check_exist(old_data_file_name)) {
                RETURN_WITH_WARN_IF_ERROR(FileUtils::remove(old_data_file_name),
                                          Status::OLAPInternalError(OLAP_ERR_CANNOT_CREATE_DIR),
                                          "remove path failed. path" + old_data_file_name);
                links_to_remove->push_back(old_data_file_name);
            }
            old_index_file_name =
                    _construct_err_sg_index_file_path(_rowset_path_prefix, segment_id);
            if (FileUtils::check_exist(old_index_file_name)) {
                RETURN_WITH_WARN_IF_ERROR(FileUtils::remove(old_index_file_name),
                                          Status::OLAPInternalError(OLAP_ERR_CANNOT_CREATE_DIR),
                                          "remove path failed. path" + old_index_file_name);

                links_to_remove->push_back(old_index_file_name);
            }
        }
    }
    std::string pending_delta_path = _rowset_path_prefix + PENDING_DELTA_PREFIX;
    if (FileUtils::check_exist(pending_delta_path)) {
        LOG(INFO) << "remove pending delta path:" << pending_delta_path;
        RETURN_WITH_WARN_IF_ERROR(FileUtils::remove_all(pending_delta_path),
                                  Status::OLAPInternalError(OLAP_ERR_CANNOT_CREATE_DIR),
                                  "remove path failed. path" + pending_delta_path);
    }
    return Status::OK();
}

Status SegmentGroup::link_segments_to_path(const std::string& dest_path,
                                           const RowsetId& rowset_id) {
    if (dest_path.empty()) {
        LOG(WARNING) << "dest path is empty, return error";
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }
    for (int segment_id = 0; segment_id < _num_segments; segment_id++) {
        std::string data_file_name = _construct_file_name(rowset_id, segment_id, ".dat");
        std::string new_data_file_path = dest_path + "/" + data_file_name;
        if (!FileUtils::check_exist(new_data_file_path)) {
            std::string origin_data_file_path =
                    construct_data_file_path(_rowset_path_prefix, segment_id);
            if (link(origin_data_file_path.c_str(), new_data_file_path.c_str()) != 0) {
                LOG(WARNING) << "fail to create hard link. from=" << origin_data_file_path
                             << ", to=" << new_data_file_path << ", error=" << strerror(errno);
                return Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
            }
        }
        std::string index_file_name = _construct_file_name(rowset_id, segment_id, ".idx");
        std::string new_index_file_path = dest_path + "/" + index_file_name;
        if (!FileUtils::check_exist(new_index_file_path)) {
            std::string origin_idx_file_path =
                    construct_index_file_path(_rowset_path_prefix, segment_id);
            if (link(origin_idx_file_path.c_str(), new_index_file_path.c_str()) != 0) {
                LOG(WARNING) << "fail to create hard link. from=" << origin_idx_file_path
                             << ", to=" << new_index_file_path << ", error=" << strerror(errno);
                return Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
            }
        }
    }
    return Status::OK();
}

std::string SegmentGroup::construct_old_index_file_path(const std::string& path_prefix,
                                                        int32_t segment_id) const {
    if (_is_pending) {
        return _construct_old_pending_file_path(path_prefix, segment_id, ".idx");
    } else {
        return _construct_old_file_path(path_prefix, segment_id, ".idx");
    }
}

std::string SegmentGroup::construct_old_data_file_path(const std::string& path_prefix,
                                                       int32_t segment_id) const {
    if (_is_pending) {
        return _construct_old_pending_file_path(path_prefix, segment_id, ".dat");
    } else {
        return _construct_old_file_path(path_prefix, segment_id, ".dat");
    }
}

std::string SegmentGroup::_construct_err_sg_index_file_path(const std::string& path_prefix,
                                                            int32_t segment_id) const {
    if (_is_pending) {
        return _construct_old_pending_file_path(path_prefix, segment_id, ".idx");
    } else {
        return _construct_err_sg_file_path(path_prefix, segment_id, ".idx");
    }
}

std::string SegmentGroup::_construct_err_sg_data_file_path(const std::string& path_prefix,
                                                           int32_t segment_id) const {
    if (_is_pending) {
        return _construct_old_pending_file_path(path_prefix, segment_id, ".dat");
    } else {
        return _construct_err_sg_file_path(path_prefix, segment_id, ".dat");
    }
}

std::string SegmentGroup::_construct_old_pending_file_path(const std::string& path_prefix,
                                                           int32_t segment_id,
                                                           const std::string& suffix) const {
    std::stringstream file_path;
    file_path << path_prefix << "/" << PENDING_DELTA_PREFIX << "/" << _txn_id << "_"
              << _segment_group_id << "_" << segment_id << suffix;
    return file_path.str();
}

std::string SegmentGroup::_construct_old_file_path(const std::string& path_prefix,
                                                   int32_t segment_id,
                                                   const std::string& suffix) const {
    char file_path[OLAP_MAX_PATH_LEN];
    if (_segment_group_id == -1) {
        snprintf(file_path, sizeof(file_path), "%s/%ld_%ld_%ld_%d%s", path_prefix.c_str(),
                 _tablet_id, _version.first, _version.second, segment_id, suffix.c_str());
    } else {
        snprintf(file_path, sizeof(file_path), "%s/%ld_%ld_%ld_%d_%d%s", path_prefix.c_str(),
                 _tablet_id, _version.first, _version.second, _segment_group_id, segment_id,
                 suffix.c_str());
    }

    return file_path;
}

// construct file path for sg_id == -1
std::string SegmentGroup::_construct_err_sg_file_path(const std::string& path_prefix,
                                                      int32_t segment_id,
                                                      const std::string& suffix) const {
    char file_path[OLAP_MAX_PATH_LEN];
    snprintf(file_path, sizeof(file_path), "%s/%ld_%ld_%ld_%d%s", path_prefix.c_str(), _tablet_id,
             _version.first, _version.second, segment_id, suffix.c_str());

    return file_path;
}

} // namespace doris
