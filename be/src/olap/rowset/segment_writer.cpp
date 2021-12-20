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

#include "olap/rowset/segment_writer.h"

#include "olap/data_dir.h"
#include "olap/file_helper.h"
#include "olap/out_stream.h"
#include "olap/rowset/column_writer.h"
#include "olap/storage_engine.h"
#include "olap/utils.h"

namespace doris {

SegmentWriter::SegmentWriter(const std::string& file_name, SegmentGroup* segment_group,
                             uint32_t stream_buffer_size, CompressKind compress_kind,
                             double bloom_filter_fpp)
        : _file_name(file_name),
          _segment_group(segment_group),
          _stream_buffer_size(stream_buffer_size),
          _compress_kind(compress_kind),
          _bloom_filter_fpp(bloom_filter_fpp),
          _stream_factory(nullptr),
          _row_count(0),
          _block_count(0) {}

SegmentWriter::~SegmentWriter() {
    SAFE_DELETE(_stream_factory);

    for (std::vector<ColumnWriter*>::iterator it = _root_writers.begin(); it != _root_writers.end();
         ++it) {
        SAFE_DELETE(*it);
    }
}

OLAPStatus SegmentWriter::init(uint32_t write_mbytes_per_sec) {
    OLAPStatus res = OLAP_SUCCESS;
    // 创建factory
    _stream_factory = new (std::nothrow) OutStreamFactory(_compress_kind, _stream_buffer_size);

    if (nullptr == _stream_factory) {
        OLAP_LOG_WARNING("fail to allocate out stream factory");
        return OLAP_ERR_MALLOC_ERROR;
    }

    // 创建writer
    for (uint32_t i = 0; i < _segment_group->get_tablet_schema().num_columns(); i++) {
        ColumnWriter* writer = ColumnWriter::create(
                i, _segment_group->get_tablet_schema(), _stream_factory,
                _segment_group->get_num_rows_per_row_block(), _bloom_filter_fpp);

        if (nullptr == writer) {
            OLAP_LOG_WARNING("fail to create writer");
            return OLAP_ERR_MALLOC_ERROR;
        } else {
            _root_writers.push_back(writer);
        }

        res = writer->init();
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to initialize ColumnWriter. [res=%d]", res);
            return res;
        }
    }

    _write_mbytes_per_sec = write_mbytes_per_sec;

    return OLAP_SUCCESS;
}

OLAPStatus SegmentWriter::write_batch(RowBlock* block, RowCursor* cursor, bool is_finalize) {
    DCHECK(block->row_block_info().row_num == _segment_group->get_num_rows_per_row_block() ||
           is_finalize)
            << "write block not empty, num_rows=" << block->row_block_info().row_num
            << ", table_num_rows=" << _segment_group->get_num_rows_per_row_block();
    OLAPStatus res = OLAP_SUCCESS;
    for (auto col_writer : _root_writers) {
        res = col_writer->write_batch(block, cursor);
        if (OLAP_UNLIKELY(res != OLAP_SUCCESS)) {
            OLAP_LOG_WARNING("fail to write row. [res=%d]", res);
            return res;
        }
        res = col_writer->create_row_index_entry();
        if (OLAP_UNLIKELY(res != OLAP_SUCCESS)) {
            OLAP_LOG_WARNING("fail to create row index. [res=%d]", res);
            return res;
        }
    }
    _row_count += block->row_block_info().row_num;
    ++_block_count;
    return res;
}

uint64_t SegmentWriter::estimate_segment_size() {
    uint64_t result = 0;

    for (std::map<StreamName, OutStream*>::const_iterator it = _stream_factory->streams().begin();
         it != _stream_factory->streams().end(); ++it) {
        result += it->second->get_total_buffer_size();
    }

    for (std::vector<ColumnWriter*>::iterator it = _root_writers.begin(); it != _root_writers.end();
         ++it) {
        result += (*it)->estimate_buffered_memory();
    }

    return result;
}

OLAPStatus SegmentWriter::_make_file_header(ColumnDataHeaderMessage* file_header) {
    OLAPStatus res = OLAP_SUCCESS;
    file_header->set_number_of_rows(_row_count);
    file_header->set_compress_kind(_compress_kind);
    file_header->set_stream_buffer_size(_stream_buffer_size);
    // TODO. 之前没设置
    file_header->set_magic_string("COLUMN DATA");
    file_header->set_version(1);
    file_header->set_num_rows_per_block(_segment_group->get_num_rows_per_row_block());

    // check if has bloom filter columns
    bool has_bf_column = false;
    uint32_t bf_hash_function_num = 0;
    uint32_t bf_bit_num = 0;
    for (std::vector<ColumnWriter*>::iterator it = _root_writers.begin(); it != _root_writers.end();
         ++it) {
        (*it)->get_bloom_filter_info(&has_bf_column, &bf_hash_function_num, &bf_bit_num);
        if (has_bf_column) {
            file_header->set_bf_hash_function_num(bf_hash_function_num);
            file_header->set_bf_bit_num(bf_bit_num);
            break;
        }
    }

    for (std::vector<ColumnWriter*>::iterator it = _root_writers.begin(); it != _root_writers.end();
         ++it) {
        // ColumnWriter::finalize will set:
        //   * column_unique_id
        //   * column_type
        //   * column_encoding
        //   * zone_maps
        res = (*it)->finalize(file_header);

        if (OLAP_UNLIKELY(OLAP_SUCCESS != res)) {
            OLAP_LOG_WARNING("fail to finalize row writer. [res=%d]", res);
            return res;
        }
    }

    uint64_t index_length = 0;
    uint64_t data_length = 0;

    for (std::map<StreamName, OutStream*>::const_iterator it = _stream_factory->streams().begin();
         it != _stream_factory->streams().end(); ++it) {
        OutStream* stream = it->second;

        // 如果这个流没有被终止，flush
        if (!stream->is_suppressed()) {
            if (OLAP_SUCCESS != (res = stream->flush())) {
                OLAP_LOG_WARNING("fail to flush out stream. [res=%d]", res);
                return res;
            }
        } else {
            //如果被suspend，目前也就是present流，不写入信息
            continue;
        }

        StreamInfoMessage* stream_info = file_header->add_stream_info();
        stream_info->set_length(stream->get_stream_length());
        stream_info->set_column_unique_id(it->first.unique_column_id());
        stream_info->set_kind(it->first.kind());

        if (it->first.kind() == StreamInfoMessage::ROW_INDEX ||
            it->first.kind() == StreamInfoMessage::BLOOM_FILTER) {
            index_length += stream->get_stream_length();
        } else {
            data_length += stream->get_stream_length();
        }

        VLOG_TRACE << "stream id=" << it->first.unique_column_id() << ", type=" << it->first.kind()
                   << ", length=" << stream->get_stream_length();
    }

    file_header->set_index_length(index_length);
    file_header->set_data_length(data_length);
    return res;
}

// 之前所有的数据都缓存在内存里, 现在创建文件, 写入数据
OLAPStatus SegmentWriter::finalize(uint32_t* segment_file_size) {
    OLAPStatus res = OLAP_SUCCESS;
    FileHandler file_handle;
    FileHeader<ColumnDataHeaderMessage> file_header;
    StorageEngine* engine = StorageEngine::instance();
    DataDir* data_dir = nullptr;
    if (engine != nullptr) {
        std::filesystem::path tablet_path = std::string_view(_segment_group->rowset_path_prefix());
        std::filesystem::path data_dir_path =
                tablet_path.parent_path().parent_path().parent_path().parent_path();
        std::string data_dir_string = data_dir_path.string();
        data_dir = engine->get_store(data_dir_string);
        if (LIKELY(data_dir != nullptr)) {
            data_dir->add_pending_ids(ROWSET_ID_PREFIX + _segment_group->rowset_id().to_string());
        } else {
            LOG(WARNING) << "data dir not found. [data_dir=" << data_dir_string << "]";
            return OLAP_ERR_CANNOT_CREATE_DIR;
        }
    }
    if (OLAP_SUCCESS != (res = file_handle.open_with_mode(_file_name, O_CREAT | O_EXCL | O_WRONLY,
                                                          S_IRUSR | S_IWUSR))) {
        LOG(WARNING) << "fail to open file. [file_name=" << _file_name << "]";
        return res;
    }

    res = _make_file_header(file_header.mutable_message());
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to make file header. [res=%d]", res);
        return res;
    }

    // check disk capacity
    if (data_dir != nullptr && data_dir->reach_capacity_limit((int64_t)file_header.file_length())) {
        return OLAP_ERR_DISK_REACH_CAPACITY_LIMIT;
    }

    if (OLAP_SUCCESS != (res = file_handle.open_with_mode(_file_name, O_CREAT | O_EXCL | O_WRONLY,
                                                          S_IRUSR | S_IWUSR))) {
        LOG(WARNING) << "fail to open file. [file_name=" << _file_name << "]";
        return res;
    }

    res = file_header.prepare(&file_handle);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("write file header error. [err=%m]");
        return res;
    }

    // 跳过FileHeader
    if (-1 == file_handle.seek(file_header.size(), SEEK_SET)) {
        OLAP_LOG_WARNING("lseek header file error. [err=%m]");
        return OLAP_ERR_IO_ERROR;
    }

    uint32_t checksum = CRC32_INIT;

    // 写入数据
    for (std::map<StreamName, OutStream*>::const_iterator it = _stream_factory->streams().begin();
         it != _stream_factory->streams().end(); ++it) {
        OutStream* stream = it->second;

        // 输出没有被掐掉的流
        if (!stream->is_suppressed()) {
            checksum = stream->crc32(checksum);
            VLOG_TRACE << "stream id=" << it->first.unique_column_id()
                       << ", type=" << it->first.kind();
            res = stream->write_to_file(&file_handle, _write_mbytes_per_sec);
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to write stream to file. [res=%d]", res);
                return res;
            }
        }
    }

    uint64_t file_length = file_handle.tell();
    file_header.set_file_length(file_length);
    file_header.set_checksum(checksum);
    *segment_file_size = file_length;

    // 写入更新之后的FileHeader
    res = file_header.serialize(&file_handle);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("write file header error. [err=%m]");
        return res;
    }

    res = file_handle.close();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to close file. [err=%m]");
        return res;
    }

    return res;
}

} // namespace doris
