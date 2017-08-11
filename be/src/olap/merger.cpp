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

#include "olap/merger.h"

#include <memory>
#include <vector>

#include "olap/i_data.h"
#include "olap/olap_define.h"
#include "olap/olap_index.h"
#include "olap/olap_table.h"
#include "olap/reader.h"
#include "olap/row_cursor.h"
#include "olap/writer.h"

using std::list;
using std::string;
using std::unique_ptr;
using std::vector;


namespace palo {

Merger::Merger(SmartOLAPTable table, OLAPIndex* index, ReaderType type) : 
        _table(table),
        _index(index),
        _reader_type(type),
        _row_count(0),
        _uniq_keys(table->num_key_fields(), 1),
        _selectivities(table->num_key_fields(), 1) {}

OLAPStatus Merger::merge(
        const vector<IData*>& olap_data_arr,
        bool use_simple_merge,
        uint64_t* merged_rows,
        uint64_t* filted_rows) {
    if (use_simple_merge && _check_simple_merge(olap_data_arr)) {
        *merged_rows = 0;
        *filted_rows = 0;
        return _create_hard_link();
    } else {
        return _merge(olap_data_arr, merged_rows, filted_rows);
    }
}

bool Merger::_check_simple_merge(const vector<IData*>& olap_data_arr) {
    bool res = false;
    vector<Version> versions;
    int32_t no_empty_file_num = 0;

    // set init value. When all the version is empty, the first version is the merge base version.
    _simple_merge_version = olap_data_arr[0]->version();

    _table->obtain_header_rdlock();

    for (vector<IData*>::const_iterator it = olap_data_arr.begin();
            it != olap_data_arr.end(); ++it) {
        if (!(*it)->empty()) {
            _simple_merge_version = (*it)->version();
            no_empty_file_num++;

            if (no_empty_file_num > 1) {
                goto EXIT;
            }
        } else if (_table->is_delete_data_version((*it)->version())) {
            goto EXIT;
        }
    }

    if (1 <= no_empty_file_num) {
        res = true;
    }

EXIT:
    _table->release_header_lock();
    return res;
}

OLAPStatus Merger::_create_hard_link() {
    OLAPStatus res = OLAP_SUCCESS;
    _table->obtain_header_rdlock();
    VersionEntity version_entity = _table->get_version_entity_by_version(_simple_merge_version);
    _table->release_header_lock();
    list<string> new_files;

    for (uint32_t i = 0; i < version_entity.num_segments; i++) {
        string new_index_path = _table->construct_index_file_path(
                                     _index->version(), _index->version_hash(), i);

        string old_index_path = _table->construct_index_file_path(
                                     version_entity.version, version_entity.version_hash, i);

        if (0 != link(old_index_path.c_str(), new_index_path.c_str())) {
            OLAP_LOG_WARNING("fail to create hard link. [old_path=%s] to [new_path=%s] [%m]",
                    old_index_path.c_str(),
                    new_index_path.c_str());
            res = OLAP_ERR_OS_ERROR;
            goto EXIT;
        }

        new_files.push_back(new_index_path);

        string new_data_path = _table->construct_data_file_path(
                                    _index->version(), _index->version_hash(), i);

        string old_data_path = _table->construct_data_file_path(
                                    version_entity.version, version_entity.version_hash, i);

        if (0 != link(old_data_path.c_str(), new_data_path.c_str())) {
            OLAP_LOG_WARNING("fail to create hard link. from [path=%s] to [path=%s] [%m]",
                    old_data_path.c_str(),
                    new_data_path.c_str());
            res = OLAP_ERR_OS_ERROR;
            goto EXIT;
        }

        new_files.push_back(new_data_path);
    }

    _index->set_num_segments(version_entity.num_segments);

    if (version_entity.column_statistics.size() != 0) {
        _index->set_column_statistics(version_entity.column_statistics);
    }

EXIT:

    if (res != OLAP_SUCCESS && new_files.size() != 0) {
        for (list<string>::iterator it = new_files.begin();
                it != new_files.end(); ++it) {
            if (0 != remove(it->c_str())) {
                OLAP_LOG_WARNING("fail to remove linked file.[file='%s']", it->c_str());
            }
        }
    }

    return res;
}

OLAPStatus Merger::_merge(
        const vector<IData*>& olap_data_arr,
        uint64_t* merged_rows,
        uint64_t* filted_rows) {
    // Create and initiate reader for scanning and multi-merging specified
    // OLAPDatas.
    Reader reader;
    ReaderParams reader_params;
    reader_params.olap_table = _table;
    reader_params.reader_type = _reader_type;
    reader_params.olap_data_arr = olap_data_arr;

    if (_reader_type == READER_BASE_EXPANSION) {
        reader_params.version = _index->version();
    }

    if (OLAP_SUCCESS != reader.init(reader_params)) {
        OLAP_LOG_WARNING("fail to initiate reader. [table='%s']",
                _table->full_name().c_str());
        return OLAP_ERR_INIT_FAILED;
    }

    // create and initiate writer for generating new index and data files.
    unique_ptr<IWriter> writer(IWriter::create(_table, _index, false));

    if (NULL == writer) {
        OLAP_LOG_WARNING("fail to allocate writer.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    if (OLAP_SUCCESS != writer->init()) {
        OLAP_LOG_WARNING("fail to initiate writer. [table='%s']",
                _table->full_name().c_str());
        return OLAP_ERR_INIT_FAILED;
    }

    bool has_error = false;
    // We calculate selectivities only when base expansioning.
    bool need_calculate_selectivities = (_index->version().first == 0);
    RowCursor row_cursor;

    if (OLAP_SUCCESS != row_cursor.init(_table->tablet_schema())) {
        OLAP_LOG_WARNING("fail to init row cursor.");
        has_error = true;
    }

    RowCursor last_row;

    if (OLAP_SUCCESS != last_row.init(_table->tablet_schema())) {
        OLAP_LOG_WARNING("fail to init row cursor.");
        has_error = true;
    }

    bool eof = false;
    int64_t raw_rows_read = 0;

    // The following procedure would last for long time, half of one day, etc.
    while (!has_error) {
        // Attach row cursor to the memory position of the row block being
        // written in writer.
        if (OLAP_SUCCESS != writer->attached_by(&row_cursor)) {
            OLAP_LOG_WARNING("attach row failed. [table='%s']",
                    _table->full_name().c_str());
            has_error = true;
            break;
        }

        // Read one row into row_cursor
        OLAPStatus res = reader.next_row_with_aggregation(&row_cursor, &raw_rows_read, &eof);

        if (OLAP_SUCCESS == res && eof) {
            OLAP_LOG_DEBUG("reader read to the end.");
            break;
        } else if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("reader read failed.");
            has_error = true;
            break;
        }

        // Goto next row position in the row block being written
        writer->next(row_cursor);

        if (need_calculate_selectivities) {
            // Calculate statistics while base expansion
            if (0 != _row_count) {
                size_t first_diff_id = 0;

                if (OLAP_SUCCESS != last_row.get_first_different_column_id(
                        row_cursor, &first_diff_id)) {
                    OLAP_LOG_WARNING("fail to get_first_different_column_id.");
                    has_error = true;
                    break;
                }

                for (size_t i = first_diff_id; i < _uniq_keys.size(); ++i) {
                    ++_uniq_keys[i];
                }
            }

            // set last row for next comapration.
            if (OLAP_SUCCESS != last_row.copy(row_cursor)) {
                OLAP_LOG_WARNING("fail to copy last row.");
                has_error = true;
                break;
            }
        }

        ++_row_count;
    }

    if (OLAP_SUCCESS != writer->finalize()) {
        OLAP_LOG_WARNING("fail to finalize writer. [table='%s']",
                _table->full_name().c_str());
        has_error = true;
    }

    if (need_calculate_selectivities) {
        for (size_t i = 0; i < _uniq_keys.size(); ++i) {
            _selectivities[i]
                = static_cast<uint32_t>(_row_count / _uniq_keys[i]);
        }
    }

    if (!has_error) {
        *merged_rows = reader.merged_rows();
        *filted_rows = reader.filted_rows();
    }

    return has_error ? OLAP_ERR_OTHER_ERROR : OLAP_SUCCESS;
}


}  // namespace palo
