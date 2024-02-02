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

#include "olap/rowid_conversion.h"
namespace doris {
RowIdConversion::RowIdConversion() = default;
RowIdConversion::~RowIdConversion() {
    if (_file_reader.get() != nullptr) {
        auto st = _file_reader->close();
        if (!st.ok()) {
            LOG(WARNING) << "fail to close file " << _file_name;
        }
        st = io::global_local_filesystem()->delete_file(_file_name);
        if (!st.ok()) {
            LOG(WARNING) << "fail to delete file " << _file_name;
        }
    }
}

// resize segment rowid map to its rows num
void RowIdConversion::init_segment_map(const RowsetId& src_rowset_id,
                                       const std::vector<uint32_t>& num_rows) {
    for (size_t i = 0; i < num_rows.size(); i++) {
        uint32_t id = _segments_rowid_map.size();
        _segment_to_id_map.emplace(std::pair<RowsetId, uint32_t> {src_rowset_id, i}, id);
        _id_to_segment_map.emplace_back(src_rowset_id, i);
        _segments_rowid_map.emplace_back(std::vector<std::pair<uint32_t, uint32_t>>(
                num_rows[i], std::pair<uint32_t, uint32_t>(UINT32_MAX, UINT32_MAX)));
        _count += num_rows[i];
    }
}

// add row id to the map
void RowIdConversion::add(const std::vector<RowLocation>& rss_row_ids,
                          const std::vector<uint32_t>& dst_segments_num_row) {
    for (auto& item : rss_row_ids) {
        if (item.row_id == -1) {
            continue;
        }
        uint32_t id = _segment_to_id_map.at(
                std::pair<RowsetId, uint32_t> {item.rowset_id, item.segment_id});
        if (_cur_dst_segment_id < dst_segments_num_row.size() &&
            _cur_dst_segment_rowid >= dst_segments_num_row[_cur_dst_segment_id]) {
            _cur_dst_segment_id++;
            _cur_dst_segment_rowid = 0;
        }
        _segments_rowid_map[id][item.row_id] =
                std::pair<uint32_t, uint32_t> {_cur_dst_segment_id, _cur_dst_segment_rowid++};
    }
}

void RowIdConversion::set_file_name(std::string file_name) {
    _file_name = file_name;
}

Status RowIdConversion::create_file_name(TabletSharedPtr tablet, ReaderType reader_type) {
    std::stringstream file_path_ss;
    file_path_ss << tablet->tablet_path() << "/rowid_conversion_" << tablet->tablet_id();
    if (reader_type == ReaderType::READER_BASE_COMPACTION) {
        file_path_ss << "_base";
    } else if (reader_type == ReaderType::READER_CUMULATIVE_COMPACTION ||
               reader_type == ReaderType::READER_SEGMENT_COMPACTION) {
        file_path_ss << "_cumu";
    } else if (reader_type == ReaderType::READER_COLD_DATA_COMPACTION) {
        file_path_ss << "_cold";
    } else {
        DCHECK(false);
        return Status::InternalError("unknown reader type");
    }
    file_path_ss << ".XXXXXX";
    std::string file_path = file_path_ss.str();
    LOG(INFO) << "rowid_conversion path: " << file_path;
    _file_name = file_path;
    return Status::OK();
}

Status RowIdConversion::save_to_file_if_necessary(TabletSharedPtr tablet, ReaderType reader_type) {
#ifndef BE_TEST
    if (count() > config::rowid_conversion_persistence_threshold_count) {
        RETURN_IF_ERROR(create_file_name(tablet, reader_type));
        RETURN_IF_ERROR(save_to_file());
        clear_segments_rowid_map();
        RETURN_IF_ERROR(open_file());
    }
#endif
    return Status::OK();
}

Status RowIdConversion::save_to_file() {
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(_file_name, &exists));
    if (exists) {
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(_file_name));
    }
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(_file_name, &_file_writer));
    size_t total_size = 0;
    total_size += _id_to_segment_map.size() * sizeof(uint64_t);
    total_size += _count * sizeof(uint32_t) * 3;
    size_t offset = 0;
    size_t segment_index = 0;
    uint8_t size_buf[sizeof(uint64_t)];
    uint8_t record_buf[sizeof(uint32_t)];
    for (auto segment : _segments_rowid_map) {
        _id_to_pos_map.emplace(segment_index, offset);
        uint64_t size = segment.size();
        memcpy(size_buf, &size, sizeof(size));
        RETURN_IF_ERROR(_file_writer->append({size_buf, sizeof(size)}));
        offset += sizeof(size);
        uint32_t src_row_index = 0;
        for (auto iter = segment.begin(); iter != segment.end(); ++iter) {
            memcpy(record_buf, &src_row_index, sizeof(src_row_index));
            RETURN_IF_ERROR(_file_writer->append({record_buf, sizeof(src_row_index)}));
            offset += sizeof(src_row_index);
            memcpy(record_buf, &iter->first, sizeof(iter->first));
            RETURN_IF_ERROR(_file_writer->append({record_buf, sizeof(iter->first)}));
            offset += sizeof(iter->first);
            memcpy(record_buf, &iter->second, sizeof(iter->second));
            RETURN_IF_ERROR(_file_writer->append({record_buf, sizeof(iter->second)}));
            offset += sizeof(iter->second);
            src_row_index++;
        }
        segment_index++;
    }
    CHECK(offset == total_size);
    RETURN_IF_ERROR(_file_writer->close());

    return Status::OK();
}

Status RowIdConversion::open_file() {
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(_file_name, &_file_reader));
    _read_from_file = true;
    return Status::OK();
}

void RowIdConversion::clear_segments_rowid_map() {
    _segments_rowid_map.clear();
}

// get destination RowLocation
// return non-zero if the src RowLocation does not exist
int RowIdConversion::get(const RowLocation& src, RowLocation* dst) const {
    auto iter = _segment_to_id_map.find({src.rowset_id, src.segment_id});
    if (iter == _segment_to_id_map.end()) {
        return -1;
    }
    uint32_t dst_segment_id = UINT32_MAX;
    uint32_t dst_rowid = UINT32_MAX;
    if (_read_from_file) {
        size_t bytes_read = 0;
        auto pos = _id_to_pos_map.at(iter->second);
        uint64_t rows_size = sizeof(uint64_t);
        uint8_t rows_buf[rows_size];
        auto st = _file_reader->read_at(pos, {rows_buf, rows_size}, &bytes_read);
        if (!st.ok()) {
            LOG(WARNING) << "fail to read rows record,file=" << _file_name << ",at pos=" << pos;
            return -1;
        }
        pos += rows_size;
        size_t rows;
        memcpy(&rows, rows_buf, sizeof(uint64_t));
        if (src.row_id >= rows) {
            return -1;
        }
        uint32_t record_size = sizeof(uint32_t) * 3;
        uint8_t record_buf[record_size];
        st = _file_reader->read_at(pos + record_size * src.row_id, {record_buf, record_size},
                                   &bytes_read);
        if (!st.ok()) {
            LOG(WARNING) << "fail to read triplet record file=" << _file_name
                         << ",at pos=" << pos + record_size * src.row_id;
            return -1;
        }
        uint32_t src_row_index;
        memcpy(&src_row_index, record_buf, sizeof(uint32_t));
        CHECK(src_row_index == src.row_id);
        memcpy(&dst_segment_id, record_buf + sizeof(uint32_t), sizeof(uint32_t));
        memcpy(&dst_rowid, record_buf + sizeof(uint32_t) * 2, sizeof(uint32_t));
    } else {
        const auto& rowid_map = _segments_rowid_map[iter->second];
        if (src.row_id >= rowid_map.size()) {
            return -1;
        }
        dst_segment_id = rowid_map[src.row_id].first;
        dst_rowid = rowid_map[src.row_id].second;
    }
    if (dst_segment_id == UINT32_MAX && dst_rowid == UINT32_MAX) {
        return -1;
    }

    dst->rowset_id = _dst_rowst_id;
    dst->segment_id = dst_segment_id;
    dst->row_id = dst_rowid;
    return 0;
}

uint64_t RowIdConversion::count() {
    return _count;
}

} // namespace doris