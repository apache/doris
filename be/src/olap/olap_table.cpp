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

#include "olap/olap_table.h"

#include <ctype.h>
#include <pthread.h>
#include <stdio.h>

#include <algorithm>
#include <map>
#include <set>

#include <boost/filesystem.hpp>

#include "olap/field.h"
#include "olap/i_data.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_engine.h"
#include "olap/olap_index.h"
#include "olap/olap_rootpath.h"
#include "olap/reader.h"
#include "olap/row_cursor.h"


using std::map;
using std::nothrow;
using std::set;
using std::sort;
using std::string;
using std::stringstream;
using std::vector;
using boost::filesystem::path;

namespace palo {

OLAPTable* OLAPTable::create_from_header_file(
        TTabletId tablet_id, TSchemaHash schema_hash, const string& header_file) {
    OLAPHeader* olap_header = NULL;
    OLAPTable* olap_table = NULL;

    olap_header = new(nothrow) OLAPHeader(header_file);
    if (olap_header == NULL) {
        OLAP_LOG_WARNING("fail to malloc OLAPHeader.");
        return NULL;
    }

    if (olap_header->load() != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to load header. [header_file=%s]", header_file.c_str());
        delete olap_header;
        return NULL;
    }

    if (olap_header->data_file_type() == OLAP_DATA_FILE) {
        if (config::default_num_rows_per_data_block != olap_header->num_rows_per_data_block()) {
            olap_header->set_num_rows_per_data_block(config::default_num_rows_per_data_block);
            olap_header->save();
        }
    }

    olap_table = new(nothrow) OLAPTable(olap_header);
    if (olap_table == NULL) {
        OLAP_LOG_WARNING("fail to validate table. [header_file=%s]", header_file.c_str());
        delete olap_header;
        return NULL;
    }

    olap_table->_tablet_id = tablet_id;
    olap_table->_schema_hash = schema_hash;
    stringstream full_name_stream;
    full_name_stream << tablet_id << "." << schema_hash;
    olap_table->_full_name = full_name_stream.str();

    return olap_table;
}

OLAPTable::OLAPTable(OLAPHeader* header) :
        _header(header),
        _is_dropped(false),
        _num_fields(0),
        _num_null_fields(0),
        _num_key_fields(0),
        _id(0),
        _is_loaded(false) {
    if (header == NULL) {
        return;  // for convenience of mock test.
    }

    for (int i = 0; i < header->column_size(); i++) {
        FieldInfo field_info;
        field_info.name = header->column(i).name();
        field_info.type = FieldInfo::get_field_type_by_string(header->column(i).type());
        field_info.aggregation = FieldInfo::get_aggregation_type_by_string(
                                     header->column(i).aggregation());
        field_info.length = header->column(i).length();
        field_info.is_key = header->column(i).is_key();

        if (header->column(i).has_default_value()) {
            field_info.has_default_value = true;
            field_info.set_default_value(header->column(i).default_value().c_str());
        } else {
            field_info.has_default_value = false;
        }

        if (header->column(i).has_referenced_column()) {
            field_info.has_referenced_column = true;
            field_info.referenced_column = header->column(i).referenced_column();
        } else {
            field_info.has_referenced_column = false;
        }

        if (header->column(i).has_index_length() || header->column(i).index_length() != 0) {
            field_info.index_length = header->column(i).index_length();
        } else {
            field_info.index_length = field_info.length;
        }

        if (header->column(i).has_precision()) {
            field_info.precision = header->column(i).precision();
        }

        if (header->column(i).has_frac()) {
            field_info.frac = header->column(i).frac();
        }

        if (header->column(i).has_unique_id()) {
            field_info.unique_id = header->column(i).unique_id();
        } else {
            // 该表不支持unique id, 分配一个unique id
            field_info.unique_id = static_cast<uint32_t>(i);
        }

        for (int j = 0; i < header->column(i).sub_column_size(); j++) {
            field_info.sub_columns.push_back(header->column(i).sub_column(j));
        }

        field_info.is_root_column = header->column(i).is_root_column();
        if (header->column(i).has_is_allow_null()) {
            field_info.is_allow_null = header->column(i).is_allow_null();
        } else {
            field_info.is_allow_null = false;
        }

        field_info.is_bf_column = header->column(i).is_bf_column();

        _tablet_schema.push_back(field_info);
        // field name --> field position in full row.
        _field_index_map[field_info.name] = i;
        _field_sizes.push_back(field_info.length);
        _num_fields++;
        if (true == field_info.is_allow_null) {
            _num_null_fields++;
        }

        if (field_info.is_key) {
            _num_key_fields++;
        }
    }

    if (header->file_version_size() > 0) {
        int32_t start_version = 0;
        int32_t end_version = 0;

        // 获取所有可以查询的有效版本
        for (int i = 0; i < header->file_version_size(); i++) {
            Version version(header->file_version(i).start_version(),
                            header->file_version(i).end_version());
            if (version.first == 0 && version.second > 0) {
                start_version = version.second;
            } else if (version.first == version.second && version.second > end_version) {
                end_version = version.second;
            }
        }

        // 考虑一个Delta都没有的特殊情况
        if (end_version == 0) {
            end_version = start_version;
        }
    }

    _set_storage_root_path_name();
}

OLAPTable::~OLAPTable() {
    if (_header == NULL) {
        return;  // for convenience of mock test.
    }

    // ensure that there is nobody using OLAPTable, like acquiring OLAPData(OLAPIndex)
    obtain_header_wrlock();
    for (version_olap_index_map_t::iterator it = _data_sources.begin();
            it != _data_sources.end(); ++it) {
        SAFE_DELETE(it->second);
        it->second = NULL;
    }
    _data_sources.clear();
    release_header_lock();

    path path_name(_header->file_name());
    SAFE_DELETE(_header);

    // 移动数据目录
    if (_is_dropped) {
        path table_path = path_name.parent_path();
        if (move_to_trash(table_path, table_path) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to delete table. [table_path=%s]", table_path.c_str());
        }
    }
}

OLAPStatus OLAPTable::load() {
    OLAPStatus res = OLAP_SUCCESS;
    AutoMutexLock l(&_load_lock);

    path header_file_path(_header->file_name());
    string one_schema_root = header_file_path.parent_path().string();
    set<string> files;
    set<string> index_files;
    set<string> data_files;

    if (_is_loaded) {
        goto EXIT;
    }

    res = dir_walk(one_schema_root, NULL, &files);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to walk schema root dir. [res=%d root='%s']",
                         res, one_schema_root.c_str());
        goto EXIT;
    }

    res = load_indices();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_FATAL("fail to load indices. [res=%d table='%s']", res, _full_name.c_str());
        goto EXIT;
    }

    // delete unused files
    obtain_header_rdlock();
    list_index_files(&index_files);
    list_data_files(&data_files);
    if (remove_unused_files(one_schema_root,
                            files,
                            header_file_path.filename().string(),
                            index_files,
                            data_files) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to remove unused files. [root='%s']", one_schema_root.c_str());
    }
    release_header_lock();

    _is_loaded = true;

EXIT:
    if (res != OLAP_SUCCESS) {
        OLAPEngine::get_instance()->drop_table(tablet_id(), schema_hash());
    }

    return res;
}

OLAPStatus OLAPTable::load_indices() {
    OLAPStatus res = OLAP_SUCCESS;
    obtain_header_rdlock();
    OLAPHeader* header = _header;
    OLAP_LOG_DEBUG("begin to load indices. [version_size=%d table='%s']",
                   header->file_version_size(),
                   full_name().c_str());

    for (int i = 0; i < header->file_version_size(); i++) {
        Version version;
        version.first = header->file_version(i).start_version();
        version.second = header->file_version(i).end_version();
        OLAPIndex* index = new(nothrow) OLAPIndex(this,
                                                  version,
                                                  header->file_version(i).version_hash(),
                                                  false,
                                                  header->file_version(i).num_segments(),
                                                  header->file_version(i).creation_time());
        if (index == NULL) {
            OLAP_LOG_WARNING("fail to create olap index. [version='%d-%d' table='%s']",
                             version.first,
                             version.second,
                             full_name().c_str());
            release_header_lock();
            return OLAP_ERR_MALLOC_ERROR;
        }

        // 在校验和加载索引前把index放到data-source，以防止加载索引失败造成内存泄露
        _data_sources[version] = index;
        // 判断index是否正常, 在所有版本的都检查完成之后才加载所有版本的index
        if (index->validate() != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to validate index. [version='%d-%d' version_hash=%ld]",
                             version.first,
                             version.second,
                             header->file_version(i).version_hash());
            // 现在只要一个index没有被正确加载,整个table加载失败
            release_header_lock();
            return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
        }

        if (header->file_version(i).has_delta_pruning()) {
            if (_num_key_fields != header->file_version(i).delta_pruning().column_pruning_size()) {
                OLAP_LOG_WARNING("column pruning size is error."
                        "[column_pruning_size=%d, num_key_fields=%d]",
                        header->file_version(i).delta_pruning().column_pruning_size(),
                        _num_key_fields);
                release_header_lock();
                return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
            }

            std::vector<std::pair<std::string, std::string> > \
                    column_statistics_string(_num_key_fields);
            std::vector<bool> null_flags(_num_key_fields);
            for (size_t j = 0; j < _num_key_fields; ++j) {
                ColumnPruning column_pruning = 
                        header->file_version(i).delta_pruning().column_pruning(j);
                column_statistics_string[j].first = column_pruning.min();
                column_statistics_string[j].second = column_pruning.max();
                if (column_pruning.has_null_flag()) {
                    null_flags[j] = column_pruning.null_flag();
                } else {
                    null_flags[j] = false;
                }
            }
            
            res = index->set_column_statistics_from_string(column_statistics_string, null_flags);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to set column statistics. [res=%d]", res);
                release_header_lock();
                return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
            }
        }
    }

    for (version_olap_index_map_t::const_iterator it = _data_sources.begin();
            it != _data_sources.end(); ++it) {
        Version version = it->first;
        OLAPIndex* index = it->second;

        if ((res = index->load()) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to load index. [version='%d-%d' version_hash=%ld]",
                             version.first,
                             version.second,
                             index->version_hash());
            // 现在只要一个index没有被正确加载,整个table加载失败
            release_header_lock();
            return res;
        }

        OLAP_LOG_DEBUG("load OLAPIndex success. "
                       "[version='%d-%d' version_hash=%ld num_segments=%d table='%s']",
                       version.first, version.second,
                       index->version_hash(),
                       index->num_segments(),
                       full_name().c_str());
    }

    // check if it was doing schema change.
    // TODO(zyh)
    release_header_lock();
    return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::select_versions_to_span( const Version& version,
                                           vector<Version>* span_versions) const {
    OLAPStatus res = _header->select_versions_to_span(version, span_versions);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to generate shortest version path. [version='%d-%d' table='%s']",
                         version.first,
                         version.second,
                         full_name().c_str());
    }
    return res;
}

void OLAPTable::acquire_data_sources(const Version& version, vector<IData*>* sources) const {
    vector<Version> span_versions;

    if (_header->select_versions_to_span(version, &span_versions) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to generate shortest version path. [version='%d-%d' table='%s']",
                         version.first,
                         version.second,
                         full_name().c_str());
        return;
    }

    acquire_data_sources_by_versions(span_versions, sources);
    return;
}

void OLAPTable::acquire_data_sources_by_versions(const vector<Version>& version_list,
                                                 vector<IData*>* sources) const {
    if (sources == NULL) {
        OLAP_LOG_WARNING("output parameter for data sources is null. [table='%s']",
                         full_name().c_str());
        return;
    }

    // first clear the output vector, please do not put any OLAPData
    // into this vector, it may be cause memory leak.
    sources->clear();

    for (vector<Version>::const_iterator it1 = version_list.begin();
            it1 != version_list.end(); ++it1) {
        version_olap_index_map_t::const_iterator it2 = _data_sources.find(*it1);
        if (it2 == _data_sources.end()) {
            OLAP_LOG_WARNING("fail to find OLAPIndex for version. [version='%d-%d' table='%s']",
                             it1->first,
                             it1->second,
                             full_name().c_str());
            release_data_sources(sources);
            return;
        }

        OLAPIndex* olap_index = it2->second;
        IData* olap_data = IData::create(olap_index);
        if (olap_data == NULL) {
            OLAP_LOG_WARNING("fail to malloc Data. [version='%d-%d' table='%s']",
                             it1->first,
                             it1->second,
                             full_name().c_str());
            release_data_sources(sources);
            return;
        }

        sources->push_back(olap_data);

        if (olap_data->init() != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to initial olap data. [version='%d-%d' table='%s']",
                             it1->first,
                             it1->second,
                             full_name().c_str());
            release_data_sources(sources);
            return;
        }
    }
}

OLAPStatus OLAPTable::release_data_sources(vector<IData*>* data_sources) const {
    if (data_sources == NULL) {
        OLAP_LOG_WARNING("parameter data_sources is null. [table='%s']", full_name().c_str());
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    for (vector<IData*>::iterator it = data_sources->begin(); it != data_sources->end(); ++it) {
        delete(*it);
        *it = NULL;
    }

    // clear data_sources vector
    data_sources->clear();
    return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::register_data_source(OLAPIndex* index) {
    OLAPStatus res = OLAP_SUCCESS;

    if (index == NULL) {
        OLAP_LOG_WARNING("parameter index is null. [table='%s']", full_name().c_str());
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    Version version = index->version();
    if (_data_sources.find(version) != _data_sources.end()) {
        OLAP_LOG_WARNING("olap index for version exists. [version='%d-%d' table='%s']",
                         version.first,
                         version.second,
                         full_name().c_str());
        return OLAP_ERR_TABLE_VERSION_DUPLICATE_ERROR;
    }

    // add a reference to the data source in the header file
    if (index->has_column_statistics()) {
        res = _header->add_version(
                version, index->version_hash(), index->num_segments(), index->max_timestamp(), 
                index->index_size(), index->data_size(), index->num_rows(),
                &index->get_column_statistics());
    } else {
        res = _header->add_version(
                version, index->version_hash(), index->num_segments(), index->max_timestamp(), 
                index->index_size(), index->data_size(), index->num_rows());
    }
    
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to add version to olap header. [version='%d-%d' table='%s']",
                         version.first,
                         version.second,
                         full_name().c_str());
        return res;
    }

    // put the new index into _data_sources.
    // 由于对header的操作可能失败，因此对_data_sources要放在这里
    _data_sources[version] = index;

    OLAP_LOG_DEBUG("succeed to register data source. "
                   "[version='%d-%d' version_hash=%ld num_segments=%d table='%s']",
                   version.first,
                   version.second,
                   index->version_hash(),
                   index->num_segments(),
                   full_name().c_str());
    
    return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::unregister_data_source(const Version& version, OLAPIndex** index) {
    OLAPStatus res = OLAP_SUCCESS;
    version_olap_index_map_t::iterator it = _data_sources.find(version);
    if (it == _data_sources.end()) {
        OLAP_LOG_WARNING("olap index for version does not exists. [version='%d-%d' table='%s']",
                         version.first,
                         version.second,
                         full_name().c_str());
        return OLAP_ERR_VERSION_NOT_EXIST;
    }

    // delete a reference to the data source in the header file
    if ((res = _header->delete_version(version)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to delete version from olap header. [version='%d-%d' table='%s']",
                         version.first,
                         version.second,
                         full_name().c_str());
        return res;
    }

    *index = it->second;
    _data_sources.erase(it);

    OLAP_LOG_DEBUG("unregister data source success. "
                   "[version='%d-%d' version_hash=%ld num_segments=%d table='%s']",
                   version.first,
                   version.second,
                   (*index)->version_hash(),
                   (*index)->num_segments(),
                   full_name().c_str());

    return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::replace_data_sources(const vector<Version>* old_versions,
                                       const vector<OLAPIndex*>* new_data_sources,
                                       vector<OLAPIndex*>* old_data_sources) {
    OLAPStatus res = OLAP_SUCCESS;

    if (old_versions == NULL || new_data_sources == NULL) {
        OLAP_LOG_WARNING("parameter old_versions or new_data_sources is null. [table='%s']",
                         full_name().c_str());
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    old_data_sources->clear();

    // check old version existed
    for (vector<Version>::const_iterator it = old_versions->begin();
            it != old_versions->end(); ++it) {
        version_olap_index_map_t::iterator data_source_it = _data_sources.find(*it);
        if (data_source_it == _data_sources.end()) {
            OLAP_LOG_WARNING("olap index for version does not exists. [version='%d-%d' table='%s']",
                             it->first,
                             it->second,
                             full_name().c_str());
            return OLAP_ERR_VERSION_NOT_EXIST;
        }
    }

    // check new versions not existed
    for (vector<OLAPIndex*>::const_iterator it = new_data_sources->begin();
            it != new_data_sources->end(); ++it) {
        if (_data_sources.find((*it)->version()) != _data_sources.end()) {
            bool to_be_deleted = false;

            for (vector<Version>::const_iterator old_it = old_versions->begin();
                    old_it != old_versions->end(); ++old_it) {
                if (*old_it == (*it)->version()) {
                    to_be_deleted = true;
                    break;
                }
            }

            if (!to_be_deleted) {
                OLAP_LOG_WARNING("olap index for version exists. [version='%d-%d' table='%s']",
                                 (*it)->version().first,
                                 (*it)->version().second,
                                 full_name().c_str());
                return OLAP_ERR_TABLE_VERSION_DUPLICATE_ERROR;
            }
        }
    }

    // update versions
    for (vector<Version>::const_iterator it = old_versions->begin();
            it != old_versions->end(); ++it) {
        version_olap_index_map_t::iterator data_source_it = _data_sources.find(*it);
        if (data_source_it != _data_sources.end()) {
            old_data_sources->push_back(data_source_it->second);
            _data_sources.erase(data_source_it);
        }

        // 删除失败会导致脏数据
        if ((res = _header->delete_version(*it)) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to delete version from olap header.[version='%d-%d' table='%s']",
                             it->first,
                             it->second,
                             full_name().c_str());
            return res;
        }

        OLAP_LOG_TRACE("delete version from olap header.[version='%d-%d' table='%s']",
                       it->first,
                       it->second,
                       full_name().c_str());
    }

    for (vector<OLAPIndex*>::const_iterator it = new_data_sources->begin();
            it != new_data_sources->end(); ++it) {
        _data_sources[(*it)->version()] = *it;

        // 新增失败会导致脏数据
        if ((*it)->has_column_statistics()) {
            res = _header->add_version(
                    (*it)->version(), (*it)->version_hash(), (*it)->num_segments(), 
                    (*it)->max_timestamp(), (*it)->index_size(), (*it)->data_size(),
                    (*it)->num_rows(), &(*it)->get_column_statistics());
        } else {
            res = _header->add_version(
                    (*it)->version(), (*it)->version_hash(), (*it)->num_segments(), 
                    (*it)->max_timestamp(), (*it)->index_size(), (*it)->data_size(),
                    (*it)->num_rows());
        }
        
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to add version to olap header.[version='%d-%d' table='%s']",
                             (*it)->version().first,
                             (*it)->version().second,
                             full_name().c_str());
            return res;
        }

        OLAP_LOG_TRACE("add version to olap header.[version='%d-%d' table='%s']",
                       (*it)->version().first,
                       (*it)->version().second,
                       full_name().c_str());
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::compute_all_versions_hash(const vector<Version>& versions,
                                                VersionHash* version_hash) const {
    if (version_hash == NULL) {
        OLAP_LOG_WARNING("invalid parameter: 'new_version_hash' is null.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
    *version_hash = 0L;

    for (vector<Version>::const_iterator version_index = versions.begin();
            version_index != versions.end(); ++version_index) {
        version_olap_index_map_t::const_iterator temp = _data_sources.find(*version_index);
        if (temp == _data_sources.end()) {
            OLAP_LOG_WARNING("fail to find OLAPIndex."
                             "[start_version=%d; end_version=%d]",
                             version_index->first,
                             version_index->second);
            return OLAP_ERR_TABLE_VERSION_INDEX_MISMATCH_ERROR;
        }

        *version_hash ^= temp->second->version_hash();
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::get_selectivities(vector<uint32_t>* selectivities) {
    // num_rows and selectivities are calculated when loading and base expansioning.
    if (selectivities == NULL) {
        OLAP_LOG_WARNING("parameter num_rows or selectivity is null.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    for (int i = 0; i < _header->selectivity_size(); ++i) {
        selectivities->push_back(_header->selectivity(i));
    }

    return OLAP_SUCCESS;
}

void OLAPTable::set_selectivities(const vector<uint32_t>& selectivities) {
    _header->clear_selectivity();

    for (size_t i = 0; i < selectivities.size(); ++i) {
        _header->add_selectivity(selectivities[i]);
    }
}

OLAPIndex* OLAPTable::_get_largest_index() {
    OLAPIndex* largest_index = NULL;
    size_t largest_index_sizes = 0;

    for (version_olap_index_map_t::iterator it = _data_sources.begin();
            it != _data_sources.end(); ++it) {
        // use index of base file as target index when base is not empty,
        // or try to find the biggest index.
        if (!it->second->empty() && it->second->index_size() > largest_index_sizes) {
            largest_index = it->second;
            largest_index_sizes = it->second->index_size();
        }
    }

    return largest_index;
}

OLAPStatus OLAPTable::split_range(
        const vector<string>& start_key_strings,
        const vector<string>& end_key_strings,
        uint64_t request_block_row_count,
        std::vector<std::vector<std::string>>* ranges) {
    if (ranges == NULL) {
        OLAP_LOG_WARNING("parameter end_row is null.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    Slice entry;
    RowCursor start_key;
    RowCursor end_key;
    RowCursor helper_cursor;
    RowBlockPosition start_pos;
    RowBlockPosition end_pos;
    RowBlockPosition step_pos;

    // 此helper用于辅助查找，注意它的内容不能拿来使用，是不可预知的，仅作为辅助使用
    if (helper_cursor.init(_tablet_schema, num_short_key_fields()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to parse strings to key with RowCursor type.");
        return OLAP_ERR_INVALID_SCHEMA;
    }

    // 如果有startkey，用startkey初始化；反之则用minkey初始化
    if (start_key_strings.size() > 0) {
        if (start_key.init_keys(_tablet_schema, start_key_strings) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to initial key strings with RowCursor type.");
            return OLAP_ERR_INIT_FAILED;
        }

        if (start_key.from_string(start_key_strings) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("init end key failed");
            return OLAP_ERR_INVALID_SCHEMA;
        }
    } else {
        if (start_key.init(_tablet_schema, num_short_key_fields()) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to initial key strings with RowCursor type.");
            return OLAP_ERR_INIT_FAILED;
        }

        start_key.build_min_key();
    }

    // 和startkey一样处理，没有则用maxkey初始化
    if (end_key_strings.size() > 0) {
        if (OLAP_SUCCESS != end_key.init_keys(_tablet_schema, end_key_strings)) {
            OLAP_LOG_WARNING("fail to parse strings to key with RowCursor type.");
            return OLAP_ERR_INVALID_SCHEMA;
        }

        if (end_key.from_string(end_key_strings) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("init end key failed");
            return OLAP_ERR_INVALID_SCHEMA;
        }
    } else {
        if (end_key.init(_tablet_schema, num_short_key_fields()) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to initial key strings with RowCursor type.");
            return OLAP_ERR_INIT_FAILED;
        }

        end_key.build_max_key();
    }

    AutoRWLock auto_lock(get_header_lock_ptr(), true);
    OLAPIndex* base_index = _get_largest_index();

    // 如果找不到合适的index，就直接返回startkey，endkey
    if (base_index == NULL) {
        OLAP_LOG_DEBUG("there is no base file now, may be tablet is empty.");
        // it may be right if the table is empty, so we return success.
        ranges->push_back(start_key.to_string_vector());
        ranges->push_back(end_key.to_string_vector());
        return OLAP_SUCCESS;
    }

    uint64_t expected_rows = request_block_row_count
            / base_index->current_num_rows_per_row_block();
    if (expected_rows == 0) {
        OLAP_LOG_WARNING("expected_rows less than 1. [request_block_row_count = '%d']",
                         request_block_row_count);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // 找到startkey对应的起始位置
    if (base_index->find_short_key(start_key, &helper_cursor, false, &start_pos) != OLAP_SUCCESS) {
        if (base_index->find_first_row_block(&start_pos) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to get first block pos");
            return OLAP_ERR_TABLE_INDEX_FIND_ERROR;
        }
    }

    step_pos = start_pos;
    OLAP_LOG_DEBUG("start post = %d, %d", start_pos.segment, start_pos.index_offset);

    //find last row_block is end_key is given, or using last_row_block
    if (base_index->find_short_key(end_key, &helper_cursor, false, &end_pos) != OLAP_SUCCESS) {
        if (base_index->find_last_row_block(&end_pos) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail find last row block.");
            return OLAP_ERR_TABLE_INDEX_FIND_ERROR;
        }
    }

    OLAP_LOG_DEBUG("end post = %d, %d", end_pos.segment, end_pos.index_offset);

    //get rows between first and last
    OLAPStatus res = OLAP_SUCCESS;
    RowCursor cur_start_key;
    RowCursor last_start_key;

    if (cur_start_key.init(_tablet_schema, num_short_key_fields()) != OLAP_SUCCESS
            || last_start_key.init(_tablet_schema, num_short_key_fields()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init cursor");
        return OLAP_ERR_INIT_FAILED;
    }

    if (base_index->get_row_block_entry(start_pos, &entry) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("get block entry failed.");
        return OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION;
    }

    cur_start_key.attach(entry.data, entry.length);
    last_start_key.copy(cur_start_key);
    // start_key是last start_key, 但返回的实际上是查询层给出的key
    ranges->push_back(start_key.to_string_vector());

    while (end_pos > step_pos) {
        res = base_index->advance_row_block(expected_rows, &step_pos);
        if (res == OLAP_ERR_INDEX_EOF || !(end_pos > step_pos)) {
            break;
        } else if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("advance_row_block failed.");
            return OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION;
        }

        if (base_index->get_row_block_entry(step_pos, &entry) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("get block entry failed.");
            return OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION;
        }
        cur_start_key.attach(entry.data, entry.length);

        if (cur_start_key.cmp(last_start_key) != 0) {
            ranges->push_back(cur_start_key.to_string_vector()); // end of last section
            ranges->push_back(cur_start_key.to_string_vector()); // start a new section
            last_start_key.copy(cur_start_key);
        }
    }

    ranges->push_back(end_key.to_string_vector());

    return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::_get_block_pos(const vector<string>& key_strings,
                                 bool is_start_key,
                                 OLAPIndex* base_index,
                                 bool find_last,
                                 RowBlockPosition* pos) {
    if (key_strings.size() == 0) {
        if (is_start_key) {
            if (base_index->find_first_row_block(pos) != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to find first row block.");
                return OLAP_ERR_TABLE_INDEX_FIND_ERROR;
            }

            OLAP_LOG_DEBUG("get first row block. [pos='%s']", pos->to_string().c_str());
        } else {
            if (base_index->find_last_row_block(pos) != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to find last row block.");
                return OLAP_ERR_TABLE_INDEX_FIND_ERROR;
            }

            OLAP_LOG_DEBUG("get last row block. [pos='%s']", pos->to_string().c_str());
        }

        return OLAP_SUCCESS;
    }

    RowCursor key;
    if (key.init(_tablet_schema, key_strings.size()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to initial key strings with RowCursor type.");
        return OLAP_ERR_INIT_FAILED;
    }
    if (key.from_string(key_strings) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to parse strings to key with RowCursor type.");
        return OLAP_ERR_INVALID_SCHEMA;
    }

    RowCursor helper_cursor;
    if (helper_cursor.init(_tablet_schema, num_short_key_fields()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init helper cursor.");
        return OLAP_ERR_INIT_FAILED;
    }

    OLAP_LOG_DEBUG("show num_short_key_field. [num=%lu]", num_short_key_fields());

    // get the row block position.
    if (base_index->find_row_block(key, &helper_cursor, find_last, pos) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to find row block. [key='%s']", key.to_string().c_str());
        return OLAP_ERR_TABLE_INDEX_FIND_ERROR;
    }

    return OLAP_SUCCESS;
}

void OLAPTable::list_data_files(set<string>* file_names) const {
    _list_files_with_suffix("dat", file_names);
}

void OLAPTable::list_index_files(set<string>* file_names) const {
    _list_files_with_suffix("idx", file_names);
}

void OLAPTable::_list_files_with_suffix(const string& file_suffix, set<string>* file_names) const {
    if (file_names == NULL) {
        OLAP_LOG_WARNING("parameter filenames is null. [table='%s']", full_name().c_str());
        return;
    }

    file_names->clear();

    for (version_olap_index_map_t::const_iterator it = _data_sources.begin();
            it != _data_sources.end(); ++it) {
        // every data segment has its file name.
        OLAPIndex* index = it->second;
        for (uint32_t i = 0; i < index->num_segments(); ++i) {
            file_names->insert(basename(construct_file_path(_header->file_name(),
                                                            index->version(),
                                                            index->version_hash(),
                                                            i,
                                                            file_suffix).c_str()));
        }
    }
}

bool OLAPTable::has_version(const Version& version) const {
    return (_data_sources.find(version) != _data_sources.end());
}

void OLAPTable::list_versions(vector<Version>* versions) const {
    if (versions == NULL) {
        OLAP_LOG_WARNING("parameter versions is null.");
        return;
    }

    versions->clear();

    // versions vector is not sorted.
    version_olap_index_map_t::const_iterator it;
    for (it = _data_sources.begin(); it != _data_sources.end(); ++it) {
        versions->push_back(it->first);
    }
}

void OLAPTable::list_version_entities(vector<VersionEntity>* version_entities) const {
    if (version_entities == NULL) {
        OLAP_LOG_WARNING("parameter versions is null.");
        return;
    }

    version_entities->clear();

    // version_entities vector is not sorted.
    version_olap_index_map_t::const_iterator it;
    for (it = _data_sources.begin(); it != _data_sources.end(); ++it) {
        if (it->second->has_column_statistics()) {
            version_entities->push_back(VersionEntity(
                    it->first,
                    it->second->version_hash(),
                    it->second->num_segments(),
                    it->second->ref_count(),
                    it->second->num_rows(),
                    it->second->data_size(),
                    it->second->index_size(),
                    it->second->empty(),
                    it->second->get_column_statistics()));
        } else {
            version_entities->push_back(VersionEntity(
                    it->first,
                    it->second->version_hash(),
                    it->second->num_segments(),
                    it->second->ref_count(),
                    it->second->num_rows(),
                    it->second->data_size(),
                    it->second->index_size(),
                    it->second->empty()));
        }
    }
}

void OLAPTable::delete_all_files() {
    // Release resources like memory and disk space.
    // we have to call list_versions first, or else error occurs when
    // removing hash_map item and iterating hash_map concurrently.
    vector<Version> versions;
    list_versions(&versions);

    // remove indices and data files, release related resources.
    for (vector<Version>::const_iterator it = versions.begin(); it != versions.end(); ++it) {
        OLAPIndex* data_source = NULL;
        if (unregister_data_source(*it, &data_source) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to unregister data source for version. [start=%d end=%d]",
                             it->first,
                             it->second);
            return;
        }

        data_source->delete_all_files();
        delete data_source;
    }

    // remove olap header file, _header object will be delete in OLAPTable.destructor
    if (remove_parent_dir(_header->file_name()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to delete header file and directory. [header_path='%s']",
                         _header->file_name().c_str());
    }
}

string OLAPTable::construct_index_file_path(const Version& version,
                                            VersionHash version_hash,
                                            uint32_t segment) const {
    return construct_file_path(_header->file_name(), version, version_hash, segment, "idx");
}
string OLAPTable::construct_data_file_path(const Version& version,
                                           VersionHash version_hash,
                                           uint32_t segment) const {
    return construct_file_path(_header->file_name(), version, version_hash, segment, "dat");
}
string OLAPTable::construct_file_path(const string& header_path,
                                      const Version& version,
                                      VersionHash version_hash,
                                      uint32_t segment,
                                      const string& suffix) {
    string file_path_prefix = header_path;
    size_t header_file_name_len = header_path.length();

    // header file must be ended with ".hdr"
    if (header_file_name_len <= 4) {
        OLAP_LOG_WARNING("invalid header file name. [file='%s']", header_path.c_str());
        return "";
    }

    file_path_prefix.erase(header_file_name_len - 4, 4);
    char file_path[OLAP_MAX_PATH_LEN];
    snprintf(file_path,
             sizeof(file_path),
             "%s_%d_%d_%ld_%u.%s",
             file_path_prefix.c_str(),
             version.first,
             version.second,
             version_hash,
             segment,
             suffix.c_str());

    return file_path;
}

string OLAPTable::construct_file_name(const Version& version,
                                      VersionHash version_hash,
                                      uint32_t segment,
                                      const string& suffix) {
    char file_name[OLAP_MAX_PATH_LEN];
    snprintf(file_name, sizeof(file_name),
             "%ld_%d_%d_%d_%ld_%u.%s",
             _tablet_id,
             _schema_hash,
             version.first,
             version.second,
             version_hash,
             segment,
             suffix.c_str());

    return file_name;
}

int32_t OLAPTable::get_field_index(const string& field_name) const {
    field_index_map_t::const_iterator res_iterator = _field_index_map.find(field_name);
    if (res_iterator == _field_index_map.end()) {
        OLAP_LOG_WARNING("invalid field name. [name='%s']", field_name.c_str());
        return -1;
    }

    return res_iterator->second;
}

size_t OLAPTable::get_field_size(const string& field_name) const {
    field_index_map_t::const_iterator res_iterator = _field_index_map.find(field_name);
    if (res_iterator == _field_index_map.end()) {
        OLAP_LOG_WARNING("invalid field name. [name='%s']", field_name.c_str());
        return 0;
    }

    if (static_cast<size_t>(res_iterator->second) >= _field_sizes.size()) {
        OLAP_LOG_WARNING("invalid field index. [name='%s']", field_name.c_str());
        return 0;
    }

    return _field_sizes[res_iterator->second];
}

size_t OLAPTable::get_return_column_size(const string& field_name) const {
    field_index_map_t::const_iterator res_iterator = _field_index_map.find(field_name);
    if (res_iterator == _field_index_map.end()) {
        OLAP_LOG_WARNING("invalid field name. [name='%s']", field_name.c_str());
        return 0;
    }

    if (static_cast<size_t>(res_iterator->second) >= _field_sizes.size()) {
        OLAP_LOG_WARNING("invalid field index. [name='%s']", field_name.c_str());
        return 0;
    }

    if (_tablet_schema[res_iterator->second].type == OLAP_FIELD_TYPE_VARCHAR ||
            _tablet_schema[res_iterator->second].type == OLAP_FIELD_TYPE_HLL) {
        return 0;
    }

    return _field_sizes[res_iterator->second];
}


size_t OLAPTable::get_row_size() const {
    size_t size = 0u;
    vector<int32_t>::const_iterator it;
    for (it = _field_sizes.begin(); it != _field_sizes.end(); ++it) {
        size += *it;
    }
    size += (_num_fields + 7) / 8;

    return size;
}

int64_t OLAPTable::get_data_size() const {
    int64_t total_size = 0;
    for (const FileVersionMessage& version : _header->file_version()) {
        total_size += version.data_size();
    }

    return total_size;
}

int64_t OLAPTable::get_num_rows() const {
    int64_t num_rows = 0;
    for (const FileVersionMessage& version : _header->file_version()) {
        num_rows += version.num_rows();
    }

    return num_rows;
}

bool OLAPTable::is_load_delete_version(Version version) {
    version_olap_index_map_t::iterator it = _data_sources.find(version);
    return it->second->delete_flag();
}

bool OLAPTable::is_schema_changing() {
    bool is_schema_changing = false;

    obtain_header_rdlock();
    if (_header->has_schema_change_status()) {
        is_schema_changing = true;
    }
    release_header_lock();

    return is_schema_changing;
}

bool OLAPTable::get_schema_change_request(TTabletId* tablet_id,
                                          SchemaHash* schema_hash,
                                          vector<Version>* versions_to_be_changed,
                                          AlterTabletType* alter_table_type) const {
    if (!_header->has_schema_change_status()) {
        return false;
    }

    const SchemaChangeStatusMessage& schema_change_status = _header->schema_change_status();

    (tablet_id == NULL || (*tablet_id = schema_change_status.related_tablet_id()));
    (schema_hash == NULL || (*schema_hash = schema_change_status.related_schema_hash()));
    (alter_table_type == NULL || (*alter_table_type =
            static_cast<AlterTabletType>(schema_change_status.schema_change_type())));

    if (versions_to_be_changed != NULL) {
        versions_to_be_changed->clear();
        for (int i = 0, len = schema_change_status.versions_to_be_changed_size(); i < len; ++i) {
            const FileVersionMessage& version = schema_change_status.versions_to_be_changed(i);
            versions_to_be_changed->push_back(
                    Version(version.start_version(), version.end_version()));
        }
    }

    return true;
}

void OLAPTable::set_schema_change_request(TTabletId tablet_id,
                                          TSchemaHash schema_hash,
                                          const vector<Version>& versions_to_be_changed,
                                          const AlterTabletType alter_table_type) {
    clear_schema_change_request();

    SchemaChangeStatusMessage* schema_change_status = _header->mutable_schema_change_status();
    schema_change_status->set_related_tablet_id(tablet_id);
    schema_change_status->set_related_schema_hash(schema_hash);

    vector<Version>::const_iterator it;
    for (it = versions_to_be_changed.begin(); it != versions_to_be_changed.end(); ++it) {
        FileVersionMessage* version = schema_change_status->add_versions_to_be_changed();
        version->set_num_segments(0);
        version->set_start_version(it->first);
        version->set_end_version(it->second);
        version->set_version_hash(0);
        version->set_max_timestamp(0);
        version->set_index_size(0);
        version->set_data_size(0);
        version->set_creation_time(0);
    }

    schema_change_status->set_schema_change_type(alter_table_type);
}

bool OLAPTable::remove_last_schema_change_version(SmartOLAPTable new_olap_table) {
    if (!_header->has_schema_change_status()) {
        return false;
    }

    if (_header->has_schema_change_status()) {
        SchemaChangeStatusMessage* schema_change_status = _header->mutable_schema_change_status();
        ::google::protobuf::RepeatedPtrField<FileVersionMessage>* versions_to_be_changed
            = schema_change_status->mutable_versions_to_be_changed();

        if (versions_to_be_changed->size() > 0) {
            versions_to_be_changed->RemoveLast();
        }
    }

    return true;
}

void OLAPTable::clear_schema_change_request() {
    OLAP_LOG_INFO("clear schema change status. [tablet='%s']", _full_name.c_str());
    _header->clear_schema_change_status();
}

void OLAPTable::set_io_error() {
    OLAP_LOG_WARNING("io error occur.[tablet_full_name='%s', root_path_name='%s']",
                     _full_name.c_str(),
                     _storage_root_path.c_str());
    OLAPRootPath::get_instance()->set_root_path_used_stat(_storage_root_path, false);
}

bool OLAPTable::is_used() {
    bool is_used = false;

    if (OLAPRootPath::get_instance()->get_root_path_used_stat(_storage_root_path, &is_used)
            == OLAP_SUCCESS) {
        return is_used;
    } else {
        return false;
    }
}

void OLAPTable::_set_storage_root_path_name() {
    // sample: storage_root_path/DATA_PREFIX/shard/tablet_id/schema_hash/xxxx.hdr
    path header_file_path(_header->file_name());
    path schema_path = header_file_path.parent_path();
    path table_path = schema_path.parent_path();
    path db_path = table_path.parent_path();

    _storage_root_path = db_path.parent_path().parent_path().string();
}

VersionEntity OLAPTable::get_version_entity_by_version(Version version) {
    if (_data_sources[version]->has_column_statistics()) {
        return VersionEntity(version,
                _data_sources[version]->version_hash(),
                _data_sources[version]->num_segments(),
                _data_sources[version]->ref_count(),
                _data_sources[version]->num_rows(),
                _data_sources[version]->data_size(),
                _data_sources[version]->index_size(),
                _data_sources[version]->empty(),
                _data_sources[version]->get_column_statistics());
    } else {
        return VersionEntity(version,
                _data_sources[version]->version_hash(),
                _data_sources[version]->num_segments(),
                _data_sources[version]->ref_count(),
                _data_sources[version]->num_rows(),
                _data_sources[version]->data_size(),
                _data_sources[version]->index_size(),
                _data_sources[version]->empty());
    }
}

OLAPStatus OLAPTable::test_version(const Version& version) {
    vector<Version> span_versions;
    obtain_header_rdlock();
    OLAPStatus res = _header->select_versions_to_span(version, &span_versions);
    release_header_lock();

    return res;
}

}  // namespace palo
