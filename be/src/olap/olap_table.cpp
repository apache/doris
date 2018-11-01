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
#include "olap/rowset.h"
#include "olap/reader.h"
#include "olap/store.h"
#include "olap/row_cursor.h"
#include "util/defer_op.h"
#include "olap/olap_header_manager.h"
#include "olap/olap_engine.h"
#include "olap/utils.h"
#include "olap/writer.h"

using std::pair;
using std::map;
using std::nothrow;
using std::set;
using std::sort;
using std::string;
using std::stringstream;
using std::vector;
using boost::filesystem::path;

namespace palo {

OLAPTablePtr OLAPTable::create_from_header_file(
        TTabletId tablet_id, TSchemaHash schema_hash,
        const string& header_file, OlapStore* store) {
    OLAPHeader* olap_header = NULL;
    olap_header = new(nothrow) OLAPHeader(header_file);
    if (olap_header == NULL) {
        LOG(WARNING) << "fail to malloc OLAPHeader.";
        return NULL;
    }

    if (olap_header->load_and_init() != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to load header. header_file=" << header_file;
        delete olap_header;
        return NULL;
    }

    // add new fields
    olap_header->set_tablet_id(tablet_id);
    olap_header->set_schema_hash(schema_hash);
    path header_file_path(header_file);
    std::string shard_path = header_file_path.parent_path().parent_path().parent_path().string();
    std::string shard_str = shard_path.substr(shard_path.find_last_of('/') + 1);
    uint64_t shard = stol(shard_str);
    olap_header->set_shard(shard);

    // save header info to kv db
    // header key format: tablet_id + "_" + schema_hash
    OLAPStatus s = OlapHeaderManager::save(store, tablet_id, schema_hash, olap_header);
    if (s != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to save header to db. [header_file=%s]", header_file.c_str());
        delete olap_header;
        return NULL;
    }
    return create_from_header(olap_header, store);
}

OLAPTablePtr OLAPTable::create_from_header(
        OLAPHeader* header,
        OlapStore* store) {
    auto olap_table = std::make_shared<OLAPTable>(header, store);
    if (olap_table == NULL) {
        LOG(WARNING) << "fail to malloc a table.";
        return nullptr;
    }

    return olap_table;
}

OLAPTable::OLAPTable(OLAPHeader* header, OlapStore* store) :
        _header(header),
        _is_dropped(false),
        _num_fields(0),
        _num_null_fields(0),
        _num_key_fields(0),
        _id(0),
        _store(store),
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

    _num_rows_per_row_block = header->num_rows_per_data_block();
    _compress_kind = header->compress_kind();
    std::stringstream tablet_path_stream;
    _tablet_id =  header->tablet_id();
    _schema_hash = header->schema_hash();
    tablet_path_stream << store->path() << DATA_PREFIX << "/" << header->shard();
    tablet_path_stream << "/" << _tablet_id << "/" << _schema_hash;
    _tablet_path = tablet_path_stream.str();
    _storage_root_path = store->path();
    _full_name = std::to_string(header->tablet_id()) + "." + std::to_string(header->schema_hash());
}

OLAPTable::~OLAPTable() {
    if (_header == NULL) {
        return;  // for convenience of mock test.
    }

    // ensure that there is nobody using OLAPTable, like acquiring OLAPData(Rowset)
    obtain_header_wrlock();
    for (auto& it : _data_sources) {
        for (Rowset* rowset : it.second) {
            SAFE_DELETE(rowset);
        }
    }
    _data_sources.clear();

    // clear the transactions in memory
    for (auto& it : _pending_data_sources) {
        // false means can't remove the transaction from header, also prevent the loading of tablet
        for (Rowset* rowset : it.second) {
            OLAPEngine::get_instance()->delete_transaction(
                    rowset->partition_id(), rowset->transaction_id(),
                    _tablet_id, _schema_hash, false);
            SAFE_DELETE(rowset);
        }
    }
    _pending_data_sources.clear();
    release_header_lock();

    SAFE_DELETE(_header);

    // 移动数据目录
    if (_is_dropped) {
        LOG(INFO) << "drop table:" << full_name() << ", tablet path:" << _tablet_path;
        path table_path(_tablet_path);
        std::string header_path = _tablet_path + "/" + std::to_string(_tablet_id) + ".hdr";
        OLAPStatus s = OlapHeaderManager::dump_header(_store, _tablet_id, _schema_hash, header_path);
        LOG(INFO) << "dump header to path:" << header_path << ", status:" << s;
        LOG(INFO) << "start to remove tablet header:" << full_name();
        s = OlapHeaderManager::remove(_store, _tablet_id, _schema_hash);
        LOG(INFO) << "finish remove tablet header:" << full_name() << ", res:" << s;
        if (move_to_trash(table_path, table_path) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to delete table. [table_path=" << _tablet_path << "]";
        }
        LOG(INFO) << "finish drop table:" << full_name();
    }
}

OLAPStatus OLAPTable::load() {
    OLAPStatus res = OLAP_SUCCESS;
    MutexLock l(&_load_lock);

    string one_schema_root = _tablet_path;
    set<string> files;
    set<string> index_files;
    set<string> data_files;

    if (_is_loaded) {
        goto EXIT;
    }

    res = dir_walk(one_schema_root, NULL, &files);
    // Disk Failure will triggered delete file in disk.
    // IOError will drop object. File only deleted upon restart.
    // TODO. Tablet should has a state to report to FE, delete tablet
    //         request will get from FE.      
    if (res == OLAP_ERR_DISK_FAILURE) {
        LOG(WARNING) << "fail to walk schema root dir." 
                     << "res=" << res << ", root=" << one_schema_root;
        goto EXIT;
    } else if (res != OLAP_SUCCESS) {
        OLAPEngine::get_instance()->drop_table(tablet_id(), schema_hash(), true);
        return res;
    }
    res = load_indices();

    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to load indices. [res=" << res << " table='" << _full_name << "']";
        goto EXIT;
    }

    // delete unused files
    obtain_header_rdlock();
    list_index_files(&index_files);
    list_data_files(&data_files);
    if (remove_unused_files(one_schema_root,
                            files,
                            "",
                            index_files,
                            data_files) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to remove unused files. [root='" << one_schema_root << "']";
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
    ReadLock rdlock(&_header_lock);
    OLAPHeader* header = _header;
    VLOG(3) << "begin to load indices. table=" << full_name() << ", "
        << "version_size=" << header->file_delta_size();

    for (int delta_id = 0; delta_id < header->delta_size(); ++delta_id) {
        const PDelta& delta = header->delta(delta_id);
        Version version;
        version.first = delta.start_version();
        version.second = delta.end_version();
        for (int j = 0; j < delta.rowset_size(); ++j) {
            const PRowSet& prowset = delta.rowset(j);
            Rowset* rowset = new Rowset(this, version, delta.version_hash(),
                                        false, prowset.rowset_id(), prowset.num_segments());
            if (rowset == nullptr) {
                LOG(WARNING) << "fail to create olap rowset. [version='" << version.first
                    << "-" << version.second << "' table='" << full_name() << "']";
                return OLAP_ERR_MALLOC_ERROR;
            }

            if (prowset.has_empty()) {
                rowset->set_empty(prowset.empty());
            }
            // 在校验和加载索引前把rowset放到data-source，以防止加载索引失败造成内存泄露
            _data_sources[version].push_back(rowset);

            // 判断rowset是否正常, 在所有版本的都检查完成之后才加载所有版本的rowset
            if (rowset->validate() != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to validate rowset. [version='%d-%d' version_hash=%ld]",
                                 version.first,
                                 version.second,
                                 header->delta(delta_id).version_hash());
                // 现在只要一个rowset没有被正确加载,整个table加载失败
                return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
            }

            if (prowset.column_pruning_size() != 0) {
                size_t column_pruning_size = prowset.column_pruning_size();
                if (_num_key_fields != column_pruning_size) {
                    LOG(ERROR) << "column pruning size is error."
                        << "column_pruning_size=" << column_pruning_size << ", "
                        << "num_key_fields=" << _num_key_fields;
                    return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
                }
                std::vector<std::pair<std::string, std::string> > \
                    column_statistic_strings(_num_key_fields);
                std::vector<bool> null_vec(_num_key_fields);
                for (size_t j = 0; j < _num_key_fields; ++j) {
                    ColumnPruning column_pruning = prowset.column_pruning(j);
                    column_statistic_strings[j].first = column_pruning.min();
                    column_statistic_strings[j].second = column_pruning.max();
                    if (column_pruning.has_null_flag()) {
                        null_vec[j] = column_pruning.null_flag();
                    } else {
                        null_vec[j] = false;
                    }
                }
                RETURN_NOT_OK(rowset->add_column_statistics(column_statistic_strings, null_vec));
            }
        }
    }

    for (version_olap_index_map_t::const_iterator it = _data_sources.begin();
            it != _data_sources.end(); ++it) {
        Version version = it->first;
        for (Rowset* rowset : it->second) {
            if ((res = rowset->load()) != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to load rowset. version=" << version.first << "-" << version.second << ", "
                             << "version_hash=" << rowset->version_hash();
                // 现在只要一个rowset没有被正确加载,整个table加载失败
                return res;
            }

            VLOG(3) << "load Rowset success. table=" << full_name() << ", "
                    << "version=" << version.first << "-" << version.second << ", "
                    << "version_hash=" << rowset->version_hash() << ", "
                    << "num_segments=" << rowset->num_segments();
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::save_header() {
    OLAPStatus res = OlapHeaderManager::save(_store, _tablet_id, _schema_hash, _header);
    if (res != OLAP_SUCCESS) {
       LOG(WARNING) << "fail to save header. [res=" << res << " root=" << _storage_root_path << "]";
    }

    return res;
}

OLAPStatus OLAPTable::select_versions_to_span( const Version& version,
                                           vector<Version>* span_versions) const {
    OLAPStatus res = _header->select_versions_to_span(version, span_versions);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to generate shortest version path. [version='" << version.first
                     << "-" << version.second << "' table='" << full_name() << "']";
    }
    return res;
}

void OLAPTable::acquire_data_sources(const Version& version, vector<IData*>* sources) const {
    vector<Version> span_versions;

    if (_header->select_versions_to_span(version, &span_versions) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to generate shortest version path. [version='" << version.first
                     << "-" << version.second << "' table='" << full_name() << "']";
        return;
    }

    acquire_data_sources_by_versions(span_versions, sources);
    return;
}

void OLAPTable::acquire_data_sources_by_versions(const vector<Version>& version_list,
                                                 vector<IData*>* sources) const {
    if (sources == NULL) {
        LOG(WARNING) << "output parameter for data sources is null. table=" << full_name();
        return;
    }

    // first clear the output vector, please do not put any OLAPData
    // into this vector, it may be cause memory leak.
    sources->clear();

    for (vector<Version>::const_iterator it1 = version_list.begin();
            it1 != version_list.end(); ++it1) {
        version_olap_index_map_t::const_iterator it2 = _data_sources.find(*it1);
        if (it2 == _data_sources.end()) {
            LOG(WARNING) << "fail to find Rowset for version. [version='" << it1->first
                         << "-" << it1->second << "' table='" << full_name() << "']";
            release_data_sources(sources);
            return;
        }

        for (Rowset* rowset : it2->second) {
            IData* olap_data = IData::create(rowset);
            if (olap_data == NULL) {
                LOG(WARNING) << "fail to malloc Data. [version='" << it1->first
                    << "-" << it1->second << "' table='" << full_name() << "']";
                release_data_sources(sources);
                return;
            }

            sources->push_back(olap_data);

            if (olap_data->init() != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to initial olap data. [version='" << it1->first
                    << "-" << it1->second << "' table='" << full_name() << "']";
                release_data_sources(sources);
                return;
            }
        }
    }
}

OLAPStatus OLAPTable::release_data_sources(vector<IData*>* data_sources) const {
    if (data_sources == NULL) {
        LOG(WARNING) << "parameter data_sources is null. [table='" << full_name() << "']";
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    for (auto data : *data_sources) {
        delete data;
    }

    // clear data_sources vector
    data_sources->clear();
    return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::register_data_source(const std::vector<Rowset*>& index_vec) {
    OLAPStatus res = OLAP_SUCCESS;

    if (index_vec.empty()) {
        LOG(WARNING) << "parameter rowset is null."
                     << "table=" << full_name();
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    for (Rowset* rowset : index_vec) {
        Version version = rowset->version();
        const std::vector<KeyRange>* column_statistics = nullptr;
        if (rowset->has_column_statistics()) {
            column_statistics = &rowset->get_column_statistics();
        }
        res = _header->add_version(version, rowset->version_hash(), rowset->rowset_id(),
                                   rowset->num_segments(), rowset->index_size(), rowset->data_size(),
                                   rowset->num_rows(), rowset->empty(), column_statistics);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to add version to olap header. table=" << full_name() << ", "
                         << "version=" << version.first << "-" << version.second;
            return res;
        }

        // put the new rowset into _data_sources.
        // 由于对header的操作可能失败，因此对_data_sources要放在这里
        _data_sources[version].push_back(rowset);
        VLOG(3) << "succeed to register data source. table=" << full_name() << ", "
                << "version=" << version.first << "-" << version.second << ", "
                << "version_hash=" << rowset->version_hash() << ", "
                << "rowset_id=" << rowset->rowset_id() << ", "
                << "num_segments=" << rowset->num_segments();
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::unregister_data_source(const Version& version, std::vector<Rowset*>* index_vec) {
    OLAPStatus res = OLAP_SUCCESS;
    version_olap_index_map_t::iterator it = _data_sources.find(version);
    if (it == _data_sources.end()) {
        LOG(WARNING) << "olap rowset for version does not exists. [version='" << version.first
                     << "-" << version.second << "' table='" << full_name() << "']";
        return OLAP_ERR_VERSION_NOT_EXIST;
    }

    // delete a reference to the data source in the header file
    if ((res = _header->delete_version(version)) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to delete version from olap header. [version='" << version.first
                     << "-" << version.second << "' table='" << full_name() << "']";
        return res;
    }

    *index_vec = it->second;
    _data_sources.erase(it);
    return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::add_pending_version(int64_t partition_id, int64_t transaction_id,
                                        const std::vector<string>* delete_conditions) {
   WriteLock wrlock(&_header_lock);
   OLAPStatus res = _header->add_pending_version(partition_id, transaction_id, delete_conditions);
   if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to add pending delta to header."
                     << "table=" << full_name() << ", "
                     << "transaction_id=" << transaction_id;
        return res;
   }
   res = save_header();
   if (res != OLAP_SUCCESS) {
       _header->delete_pending_delta(transaction_id);
       LOG(FATAL) << "fail to save header when add pending rowset. [table=" << full_name()
           << " transaction_id=" << transaction_id << "]";
       return res;
   }
   return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::add_pending_rowset(Rowset* rowset) {
    if (rowset == nullptr) {
        LOG(WARNING) << "parameter rowset is null. [table=" << full_name() << "]";
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    int64_t transaction_id = rowset->transaction_id();
    obtain_header_wrlock();
    OLAPStatus res = OLAP_SUCCESS;

    // add to header
    const std::vector<KeyRange>* column_statistics = nullptr;
    if (rowset->has_column_statistics()) {
        column_statistics = &(rowset->get_column_statistics());
    }
    res = _header->add_pending_rowset(transaction_id, rowset->num_segments(),
                                      rowset->rowset_id(), rowset->load_id(),
                                      rowset->empty(), column_statistics);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to add pending rowset to header. [table=" << full_name()
                     << " transaction_id=" << transaction_id << "]";
        release_header_lock();
        return res;
    }

    // save header
    res = save_header();
    if (res != OLAP_SUCCESS) {
        _header->delete_pending_delta(transaction_id);
        LOG(FATAL) << "fail to save header when add pending rowset. [table=" << full_name()
                   << " transaction_id=" << transaction_id << "]";
        release_header_lock();
        return res;
    }

    // add to data sources
    _pending_data_sources[transaction_id].push_back(rowset);
    release_header_lock();
    VLOG(3) << "add pending data to tablet successfully."
            << "table=" << full_name() << ", transaction_id=" << transaction_id;

    return res;
}

int32_t OLAPTable::current_pending_rowset_id(int64_t transaction_id) {
    ReadLock rdlock(&_header_lock);
    int32_t rowset_id = -1;
    if (_pending_data_sources.find(transaction_id) != _pending_data_sources.end()) {
        for (Rowset* rowset : _pending_data_sources[transaction_id]) {
            if (rowset->rowset_id() > rowset_id) {
                rowset_id = rowset->rowset_id();
            }
        }
    }
    return rowset_id;
}

OLAPStatus OLAPTable::add_pending_data(Rowset* rowset, const std::vector<TCondition>* delete_conditions) {
    if (rowset == nullptr) {
        LOG(WARNING) << "parameter rowset is null. table=" << full_name(); 
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    obtain_header_wrlock();
    int64_t transaction_id = rowset->transaction_id();
    if (_pending_data_sources.find(transaction_id) != _pending_data_sources.end()) {
        LOG(WARNING) << "find pending data existed when add to tablet. [table=" << full_name()
                     << " transaction_id=" << transaction_id << "]";
        release_header_lock();
        return OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST;
    }
    OLAPStatus res = OLAP_SUCCESS;

    // if push for delete, construct sub conditions
    vector<string> condition_strs;
    if (delete_conditions != nullptr) {
        DeleteConditionHandler del_cond_handler;
        for (const TCondition& condition : *delete_conditions) {
            condition_strs.push_back(del_cond_handler.construct_sub_conditions(condition));
        }
    }

    if (!condition_strs.empty()) {
        res = _header->add_pending_version(rowset->partition_id(), transaction_id, &condition_strs);
    } else {
        res = _header->add_pending_version(rowset->partition_id(), transaction_id, nullptr);
    }
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to add pending delta to header."
                     << "table=" << full_name() << ", "
                     << "transaction_id=" << transaction_id;
        release_header_lock();
        return res;
    }

    // add to header
    const std::vector<KeyRange>* column_statistics = nullptr;
    if (rowset->has_column_statistics()) {
        column_statistics = &(rowset->get_column_statistics());
    }
    res = _header->add_pending_rowset(transaction_id, rowset->num_segments(),
                                      rowset->rowset_id(), rowset->load_id(),
                                      rowset->empty(), column_statistics);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to add pending rowset to header. [table=" << full_name()
                     << " transaction_id=" << transaction_id << "]";
        release_header_lock();
        return res;
    }

    // save header
    res = save_header();
    if (res != OLAP_SUCCESS) {
        _header->delete_pending_delta(transaction_id);
        LOG(FATAL) << "fail to save header when add pending rowset. [table=" << full_name()
                   << " transaction_id=" << transaction_id << "]";
        release_header_lock();
        return res;
    }

    // add to data sources
    _pending_data_sources[transaction_id].push_back(rowset);
    release_header_lock();
    VLOG(3) << "add pending data to tablet successfully."
            << "table=" << full_name() << ", transaction_id=" << transaction_id;
    return res;

}

bool OLAPTable::has_pending_data(int64_t transaction_id) {
    ReadLock rdlock(&_header_lock);
    return _pending_data_sources.find(transaction_id) != _pending_data_sources.end();
}

void OLAPTable::delete_pending_data(int64_t transaction_id) {
    obtain_header_wrlock();

    auto it = _pending_data_sources.find(transaction_id);
    if (it == _pending_data_sources.end()) {
        release_header_lock();
        return;
    }

    // delete from data sources
    for (Rowset* rowset : it->second) {
        rowset->release();
        OLAPEngine::get_instance()->add_unused_index(rowset);
    }
    _pending_data_sources.erase(it);

    // delete from header
    _header->delete_pending_delta(transaction_id);

    // save header
    if (save_header() != OLAP_SUCCESS) {
        LOG(FATAL) << "failed to save header when delete pending data. [table=" << full_name()
                   << " transaction_id=" << transaction_id << "]";
    }

    release_header_lock();
    LOG(INFO) << "delete pending data from tablet. [table=" << full_name()
              << " transaction_id=" << transaction_id << "]";

}

void OLAPTable::get_expire_pending_data(vector<int64_t>* transaction_ids) {
    time_t now = time(NULL);
    ReadLock rdlock(&_header_lock);

    for (auto& it : _header->pending_delta()) {
        double diff = difftime(now, it.creation_time());
        if (diff >= config::pending_data_expire_time_sec) {
            transaction_ids->push_back(it.transaction_id());
            VLOG(3) << "find expire pending data. table=" << full_name() << ", "
                    << "transaction_id=" << it.transaction_id() << " exist_sec=" << diff;
        }
    }
}

void OLAPTable::load_pending_data() {
    LOG(INFO) << "begin to load pending_data. table=" << full_name() << ", "
              << "pending_delta size=" << _header->pending_delta_size();
    MutexLock load_lock(&_load_lock);

    // if a olap rowset loads failed, delete it from header
    std::set<int64_t> error_pending_data;

    for (const PPendingDelta& pending_delta : _header->pending_delta()) {
        for (const PPendingRowSet& pending_rowset : pending_delta.pending_rowset()) {
            Rowset* rowset = new Rowset(this, false, pending_rowset.pending_rowset_id(),
                                        pending_rowset.num_segments(), true,
                                        pending_delta.partition_id(), pending_delta.transaction_id());
            DCHECK(rowset != nullptr);
            rowset->set_load_id(pending_rowset.load_id());
            if (pending_rowset.has_empty()) {
                rowset->set_empty(pending_rowset.empty());
            }
            _pending_data_sources[rowset->transaction_id()].push_back(rowset);

            if (rowset->validate() != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to validate rowset when load pending data."
                             << "table=" << full_name() << ", "
                             << "transaction_id=" << rowset->transaction_id();
                error_pending_data.insert(rowset->transaction_id());
                break;
            }

            if (_num_key_fields != pending_rowset.column_pruning_size()) {
                LOG(WARNING) << "column pruning size is error when load pending data."
                             << "column_pruning_size=" << pending_rowset.column_pruning_size() << ", "
                             << "num_key_fields=" << _num_key_fields;
                error_pending_data.insert(rowset->transaction_id());
                break;
            }
            std::vector<std::pair<std::string, std::string>> column_statistics_string(_num_key_fields);
            std::vector<bool> null_vec(_num_key_fields);
            for (size_t j = 0; j < _num_key_fields; ++j) {
                ColumnPruning column_pruning = pending_rowset.column_pruning(j);
                column_statistics_string[j].first = column_pruning.min();
                column_statistics_string[j].second = column_pruning.max();
                if (column_pruning.has_null_flag()) {
                    null_vec[j] = column_pruning.null_flag();
                } else {
                    null_vec[j] = false;
                }
            }

            if (rowset->add_column_statistics(column_statistics_string, null_vec) != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to set column statistics when load pending data";
                error_pending_data.insert(pending_delta.transaction_id());
                break;
            }

            if (rowset->load() != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to load rowset when load pending data."
                    << "table=" << full_name() << ", transaction_id=" << pending_delta.transaction_id();
                error_pending_data.insert(pending_delta.transaction_id());
                break;
            }

            OLAPStatus add_status = OLAPEngine::get_instance()->add_transaction(
                    pending_delta.partition_id(), pending_delta.transaction_id(),
                    _tablet_id, _schema_hash, pending_rowset.load_id());

            if (add_status != OLAP_SUCCESS) {
                LOG(WARNING) << "find transaction exists in engine when load pending data. [table=" << full_name()
                    << " transaction_id=" << pending_delta.transaction_id() << "]";
                error_pending_data.insert(pending_delta.transaction_id());
                break;
            }
        }

        if (error_pending_data.find(pending_delta.transaction_id()) != error_pending_data.end()) {
            continue;
        }

        VLOG(3) << "load pending data successfully. table=" << full_name() << ", "
                << "partition_id=" << pending_delta.partition_id() << ", "
                << "transaction_id=" << pending_delta.transaction_id();
    }

    LOG(INFO) << "finish to load pending data. table=" << full_name() << ", "
              << "error_data_size=" << error_pending_data.size();

    for (int64_t error_data : error_pending_data) {
        delete_pending_data(error_data);
    }
}

// 1. need to replace local data if same version existed
// 2. move pending data to version data
// 3. move pending data to incremental data, it won't be merged, so we can do incremental clone
OLAPStatus OLAPTable::publish_version(int64_t transaction_id, Version version,
                                      VersionHash version_hash) {
    WriteLock wrlock(&_header_lock);
    if (_pending_data_sources.find(transaction_id) == _pending_data_sources.end()) {
        LOG(WARNING) << "pending data not exists in tablet, not finished or deleted."
                     << "table=" << full_name() << ", "
                     << "transaction_id=" << transaction_id;
        return OLAP_ERR_TRANSACTION_NOT_EXIST;
    }
    RETURN_NOT_OK(_handle_existed_version(transaction_id, version, version_hash));
    std::vector<Rowset*> index_vec;
    vector<string> linked_files;
    OLAPStatus res = OLAP_SUCCESS;
    for (Rowset* rowset : _pending_data_sources[transaction_id]) {
        int32_t rowset_id = rowset->rowset_id();
        for (int32_t seg_id = 0; seg_id < rowset->num_segments(); ++seg_id) {
            std::string pending_index_path = rowset->construct_index_file_path(rowset_id, seg_id);
            std::string index_path = construct_index_file_path(version, version_hash, rowset_id, seg_id);
            res = _create_hard_link(pending_index_path, index_path, &linked_files);
            if (res != OLAP_SUCCESS) { remove_files(linked_files); return res; }

            std::string pending_data_path = rowset->construct_data_file_path(rowset_id, seg_id);
            std::string data_path = construct_data_file_path(version, version_hash, rowset_id, seg_id);
            res = _create_hard_link(pending_data_path, data_path, &linked_files);
            if (res != OLAP_SUCCESS) { remove_files(linked_files); return res; }
        }

        rowset->publish_version(version, version_hash);
        index_vec.push_back(rowset);
    }

    res = register_data_source(index_vec);
    if (res != OLAP_SUCCESS) { remove_files(linked_files); return res; }

    const PPendingDelta* pending_delta = _header->get_pending_delta(transaction_id);
    if (pending_delta->has_delete_condition()) {
        const DeleteConditionMessage& delete_condition = pending_delta->delete_condition();
        _header->add_delete_condition(delete_condition, version.first);
    }

    // add incremental version, if failed, ignore it
    res = _add_incremental_data(index_vec, transaction_id, version, version_hash);
    VLOG(3) << "finish to add incremental version. res=" << res << ", "
            << "table=" << full_name() << ", "
            << "transaction_id=" << transaction_id << ", "
            << "version=" << version.first << "-" << version.second;

    // save header
    res = save_header();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save header when publish version. res=" << res << ", "
                   << "table=" << full_name() << ", "
                   << "transaction_id=" << transaction_id;
        std::vector<Rowset*> delete_index_vec;
        // if failed, clear new data
        unregister_data_source(version, &delete_index_vec);
        _delete_incremental_data(version, version_hash);
        remove_files(linked_files);
        return res;
    }

    _header->delete_pending_delta(transaction_id);
    res = save_header();
    if (res != OLAP_SUCCESS) {
        remove_files(linked_files);
        LOG(FATAL) << "fail to save header when publish version. res=" << res << ", "
                   << "table=" << full_name() << ", "
                   << "transaction_id=" << transaction_id;
        return res;
    }
    for (Rowset* rowset : _pending_data_sources[transaction_id]) {
        rowset->delete_all_files();
        rowset->set_pending_finished();
    }
    _pending_data_sources.erase(transaction_id);

    return res;
}

// 1. if version is same and version_hash different, delete local data, save header
// 2. if version_hash is same or version is merged, publish success, delete transaction, save header
OLAPStatus OLAPTable::_handle_existed_version(int64_t transaction_id, const Version& version,
                                              const VersionHash& version_hash) {
    const PDelta* existed_delta = nullptr;
    for (int i = 0; i < file_delta_size(); ++i) {
        const PDelta* delta = _header->get_delta(i);
        if (version.first >= delta->start_version()
            && version.second <= delta->end_version()) {
            existed_delta = delta;
        }

    }

    if (existed_delta == nullptr) {
        return OLAP_SUCCESS;
    }

    OLAPStatus res = OLAP_SUCCESS;
    // if version is same and version_hash different, delete local data
    if (existed_delta->start_version() == version.first
        && existed_delta->end_version() == version.second
        && existed_delta->version_hash() != version_hash) {
        LOG(INFO) << "version_hash is different when publish version, delete local data. [table=" << full_name()
                  << " transaction_id=" << transaction_id << "]";
        // remove delete condition if current type is PUSH_FOR_DELETE,
        // this occurs when user cancel delete_data soon after submit it
        bool push_for_delete = false;
        res = is_push_for_delete(transaction_id, &push_for_delete);
        if (res != OLAP_SUCCESS) {
            return res;
        } else if (!push_for_delete) {
            DeleteConditionHandler del_cond_handler;
            OLAPTablePtr olap_table_ptr =
                OLAPEngine::get_instance()->get_table(_tablet_id, _schema_hash);
            if (olap_table_ptr.get() != nullptr) {
                del_cond_handler.delete_cond(olap_table_ptr, version.first, false);
            }
        }
        // delete local data
        //Rowset *existed_index = NULL;
        std::vector<Rowset*> existed_index_vec;
        _delete_incremental_data(version, version_hash);
        res = unregister_data_source(version, &existed_index_vec);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to unregister data when publish version. [table=" << full_name()
                         << " version=" << version.first << "-" << version.second << " res=" << res << "]";
            return res;
        }
        // save header
        res = save_header();
        if (res != OLAP_SUCCESS) {
            LOG(FATAL) << "fail to save header when unregister data. [tablet=" << full_name()
                       << " transaction_id=" << transaction_id << "]";
        }
        // use OLAPEngine to delete this rowset
        if (!existed_index_vec.empty()) {
            OLAPEngine *unused_index = OLAPEngine::get_instance();
            for (Rowset* rowset : existed_index_vec) {
                unused_index->add_unused_index(rowset);
            }
        }
    // if version_hash is same or version is merged, publish success
    } else {
        LOG(INFO) << "version_hash is same when publish version, publish success. [table=" << full_name()
                  << " transaction_id=" << transaction_id << "]";
        res = OLAP_ERR_PUSH_VERSION_ALREADY_EXIST;
    }
    return res;
}

OLAPStatus OLAPTable::_add_incremental_data(std::vector<Rowset*>& index_vec, int64_t transaction_id,
                                            const Version& version, const VersionHash& version_hash) {
    if (index_vec.empty()) {
        LOG(WARNING) << "no parameter when add incremental data. table=" << full_name();
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // create incremental rowset's dir
    std::string dir_path = construct_incremental_delta_dir_path();
    OLAPStatus res = OLAP_SUCCESS;
    if (!check_dir_existed(dir_path)) {
        res = create_dirs(dir_path);
        if (res != OLAP_SUCCESS && !check_dir_existed(dir_path)) {
            LOG(WARNING) << "fail to create rowset dir. table=" << full_name() << ", "
                         << " transaction_id=" << transaction_id;
            return res;
        }
    }
    std::vector<std::string> linked_files;
    for (Rowset* rowset : index_vec) {
        for (int32_t seg_id = 0; seg_id < rowset->num_segments(); ++seg_id) {
            int32_t rowset_id = rowset->rowset_id();
            std::string index_path = rowset->construct_index_file_path(rowset_id, seg_id);
            std::string incremental_index_path =
                construct_incremental_index_file_path(version, version_hash, rowset_id, seg_id);
            res = _create_hard_link(index_path, incremental_index_path, &linked_files);
            if (res != OLAP_SUCCESS) { remove_files(linked_files); return res; }

            std::string data_path = rowset->construct_data_file_path(rowset_id, seg_id);
            std::string incremental_data_path =
                construct_incremental_data_file_path(version, version_hash, rowset_id, seg_id);
            res = _create_hard_link(data_path, incremental_data_path, &linked_files);
            if (res != OLAP_SUCCESS) { remove_files(linked_files); return res; }
        }

        const std::vector<KeyRange>* column_statistics = nullptr;
        if (rowset->has_column_statistics()) {
            column_statistics = &(rowset->get_column_statistics());
        }
        res = _header->add_incremental_version(
                rowset->version(), rowset->version_hash(),
                rowset->rowset_id(), rowset->num_segments(),
                rowset->index_size(), rowset->data_size(),
                rowset->num_rows(), rowset->empty(), column_statistics);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to add incremental data. res=" << res << ", "
                         << "table=" << full_name() << ", "
                         << "transaction_id=" << transaction_id << ", "
                         << "version=" << version.first << "-" << version.second;
            remove_files(linked_files);
            return res;
        }
    }

    return res;
}

void OLAPTable::delete_expire_incremental_data() {
    time_t now = time(NULL);
    std::vector<std::pair<Version, VersionHash>> expire_versions;
    WriteLock wrlock(&_header_lock);
    for (auto& it : _header->incremental_delta()) {
        double diff = difftime(now, it.creation_time());
        if (diff >= config::incremental_delta_expire_time_sec) {
            Version version(it.start_version(), it.end_version());
            expire_versions.push_back(std::make_pair(version, it.version_hash()));
            VLOG(3) << "find expire incremental rowset. tablet=" << full_name() << ", "
                    << "version=" << it.start_version() << "-" << it.end_version() << ", "
                    << "exist_sec=" << diff;
        }
    }
    for (auto& it : expire_versions) {
        _delete_incremental_data(it.first, it.second);
        VLOG(3) << "delete expire incremental data. table=" << full_name() << ", "
                << "version=" << it.first.first << "-" << it.first.second;
    }

    if (save_header() != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save header when delete expire incremental data."
                   << "table=" << full_name();
    }
}

void OLAPTable::_delete_incremental_data(const Version& version, const VersionHash& version_hash) {
    const PDelta* incremental_delta = get_incremental_delta(version);
    if (incremental_delta == nullptr) { return; }

    vector<string> files_to_delete;
    for (const PRowSet& prowset : incremental_delta->rowset()) {
        int32_t rowset_id = prowset.rowset_id();
        for (int seg_id = 0; seg_id < prowset.num_segments(); seg_id++) {
            std::string incremental_index_path =
                construct_incremental_index_file_path(version, version_hash, rowset_id, seg_id);
            files_to_delete.emplace_back(incremental_index_path);

            std::string incremental_data_path =
                construct_incremental_data_file_path(version, version_hash, rowset_id, seg_id);
            files_to_delete.emplace_back(incremental_data_path);
        }
    }

    remove_files(files_to_delete);
    _header->delete_incremental_delta(version);
    VLOG(3) << "delete incremental data. table=" << full_name() << ", "
            << "version=" << version.first << "-" << version.second;
}

void OLAPTable::get_missing_versions_with_header_locked(
        int64_t until_version, std::vector<Version>* missing_versions) const {
    DCHECK(until_version > 0) << "invalid until_version: " << until_version;
    std::list<Version> existing_versions;
    for (int i = 0; i < _header->file_delta_size(); ++i) {
        const PDelta* delta = _header->get_delta(i);
        existing_versions.emplace_back(delta->start_version(), delta->end_version());
    }

    // sort the existing versions in ascending order
    existing_versions.sort([](const Version& a, const Version& b) {
        // simple because 2 versions are certainly not overlapping
        return a.first < b.first;
    });

    // find the missing version until until_version
    int64_t last_version = -1;
    for (const Version& version : existing_versions) {
        if (version.first > last_version + 1) {
            for (int64_t i = last_version + 1; i < version.first; ++i) {
                missing_versions->emplace_back(i, i);
            }
        }
        last_version = version.second;
        if (until_version <= last_version) {
            break;
        }
    }
    for (int64_t i = last_version + 1; i <= until_version; ++i) {
        missing_versions->emplace_back(i, i);
    }
}

const PDelta* OLAPTable::least_complete_version(
    const vector<Version>& missing_versions) const {

    const PDelta* least_delta = nullptr;
    if (!missing_versions.empty()) {
        Version version = missing_versions.front();
        for (int i = 0; i < _header->file_delta_size(); ++i) {
            const PDelta* delta = _header->get_delta(i);
            if (delta->end_version() == version.first - 1) {
                LOG(INFO) << "find least complete version. table=" << full_name() << ", "
                    << "version=" << delta->start_version() << "-" << delta->end_version() << ", "
                    << "version_hash=" << delta->version_hash() << ", "
                    << "first_missing_version=" << version.first << "-" << version.second;
                least_delta = delta;
                break;
            }
        }
    } else {
        least_delta = lastest_version();
    }

    return least_delta;
}

OLAPStatus OLAPTable::is_push_for_delete(
    int64_t transaction_id, bool* is_push_for_delete) const {

    const PPendingDelta* pending_delta = _header->get_pending_delta(transaction_id);
    if (pending_delta == nullptr) {
        LOG(WARNING) << "pending rowset not found when check push for delete. [table=" << full_name()
                     << " transaction_id=" << transaction_id << "]";
        return OLAP_ERR_TRANSACTION_NOT_EXIST;
    }
    *is_push_for_delete  = pending_delta->has_delete_condition();
    return OLAP_SUCCESS;
}

Rowset* OLAPTable::_construct_index_from_version(const PDelta* delta, int32_t rowset_id) {
    VLOG(3) << "begin to construct rowset from version."
            << "table=" << full_name() << ", "
            << "version=" << delta->start_version() << "-" << delta->end_version() << ", "
            << "version_hash=" << delta->version_hash();
    Version version(delta->start_version(), delta->end_version());
    const PRowSet* prowset = nullptr;
    if (rowset_id == -1) {
        // Previous FileVersionMessage will be convert to PDelta and PRowset.
        // In PRowset, this is rowset_id is set to minus one.
        // When to get it, should used rowset + 1 as index.
        prowset = &(delta->rowset().Get(rowset_id + 1));
    } else {
        prowset = &(delta->rowset().Get(rowset_id));
    }
    Rowset* rowset = new Rowset(this, version, delta->version_hash(),
                                false, rowset_id, prowset->num_segments());
    if (prowset->has_empty()) {
        rowset->set_empty(prowset->empty());
    }
    DCHECK(rowset != nullptr) << "malloc error when construct rowset."
            << "table=" << full_name() << ", "
            << "version=" << version.first << "-" << version.second << ", "
            << "version_hash=" << delta->version_hash();
    OLAPStatus res = rowset->validate();
    if (res != OLAP_SUCCESS) {
        SAFE_DELETE(rowset);
        return nullptr;
    }

    if (_num_key_fields != prowset->column_pruning_size()) {
        LOG(WARNING) << "column pruning size error, " << "table=" << full_name() << ", "
                << "version=" << version.first << "-" << version.second << ", "
                << "version_hash=" << delta->version_hash() << ", "
                << "column_pruning_size=" << prowset->column_pruning_size() << ", "
                << "num_key_fields=" << _num_key_fields;
        SAFE_DELETE(rowset);
        return nullptr;
    }
    vector<pair<string, string>> column_statistic_strings(_num_key_fields);
    std::vector<bool> null_vec(_num_key_fields);
    for (size_t j = 0; j < _num_key_fields; ++j) {
        ColumnPruning column_pruning = prowset->column_pruning(j);
        column_statistic_strings[j].first = column_pruning.min();
        column_statistic_strings[j].second = column_pruning.max();
        if (column_pruning.has_null_flag()) {
            null_vec[j] = column_pruning.null_flag();
        } else {
            null_vec[j] = false;
        }
    }

    res = rowset->add_column_statistics(column_statistic_strings, null_vec);
    if (res != OLAP_SUCCESS) {
        SAFE_DELETE(rowset);
        return nullptr;
    }

    res = rowset->load();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to load rowset. res=" << res << ", "
                     << "table=" << full_name() << ", "
                     << "version=" << version.first << "-" << version.second << ", "
                     << "version_hash=" << delta->version_hash();
        SAFE_DELETE(rowset);
        return nullptr;
    }

    VLOG(3) << "finish to construct rowset from version."
            << "table=" << full_name() << ", "
            << "version=" << version.first << "-" << version.second;
    return rowset;
}

OLAPStatus OLAPTable::_create_hard_link(const string& from, const string& to,
                                        vector<string>* linked_success_files) {
    if (link(from.c_str(), to.c_str()) != 0) {
        LOG(WARNING) << "fail to create hard link. from=" << from << ", "
                     << "to=" << to << ", " << "errno=" << Errno::no();
        return OLAP_ERR_OS_ERROR;
    }
    linked_success_files->push_back(to);
    VLOG(3) << "success to create hard link. [from=" << from << " to=" << to << "]";
    return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::clone_data(const OLAPHeader& clone_header,
                                 const vector<const PDelta*>& clone_deltas,
                                 const vector<Version>& versions_to_delete) {
    LOG(INFO) << "begin to clone data to tablet. table=" << full_name() << ", "
              << "clone_versions_size=" << clone_deltas.size() << ", "
              << "versions_to_delete_size=" << versions_to_delete.size();
    OLAPStatus res = OLAP_SUCCESS;
    version_olap_index_map_t tmp_data_sources;

    do {
        // load new local header to operate on
        OLAPHeader new_local_header;
        OlapHeaderManager::get_header(_store, _tablet_id, _schema_hash, &new_local_header);

        // delete versions from new local header
        for (const Version& version : versions_to_delete) {
            res = new_local_header.delete_version(version);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to delete version from new local header. [table=" << full_name()
                             << " version=" << version.first << "-" << version.second << "]";
                break;
            }
            LOG(INFO) << "delete version from new local header when clone. [table='" << full_name()
                      << "', version=" << version.first << "-" << version.second << "]";
        }

        if (res != OLAP_SUCCESS) {
            break;
        }

        for (const PDelta* clone_delta : clone_deltas) {
            Version version(clone_delta->start_version(),
                            clone_delta->end_version());

            // construct new rowset
            for (const PRowSet& prowset : clone_delta->rowset()) {
                Rowset* tmp_index = _construct_index_from_version(clone_delta, prowset.rowset_id());
                if (tmp_index == NULL) {
                    LOG(WARNING) << "fail to construct rowset when clone data. table=" << full_name() << ", "
                        << "version=" << version.first << "-" << version.second << ", "
                        << "version_hash=" << clone_delta->version_hash();
                    res = OLAP_ERR_INDEX_LOAD_ERROR;
                    break;
                }

                tmp_data_sources[version].push_back(tmp_index);

                // add version to new local header
                const std::vector<KeyRange>* column_statistics = nullptr;
                if (tmp_index->has_column_statistics()) {
                    column_statistics = &(tmp_index->get_column_statistics());
                }
                res = new_local_header.add_version(version, tmp_index->version_hash(),
                                                   tmp_index->rowset_id(),
                                                   tmp_index->num_segments(),
                                                   tmp_index->index_size(),
                                                   tmp_index->data_size(),
                                                   tmp_index->num_rows(),
                                                   tmp_index->empty(),
                                                   column_statistics);
                if (res != OLAP_SUCCESS) {
                    LOG(WARNING) << "fail to add version to new local header when clone."
                                 << "res=" << res << ", "
                                 << "table=" << full_name() << ", "
                                 << "version=" << version.first << "-" << version.second << ", "
                                 << "version_hash=" << clone_delta->version_hash();
                    break;
                }
            }

            if (res != OLAP_SUCCESS) { break; }

            // add delete conditions to new local header, if it exists in clone_header
            if (version.first == version.second) {
                for (google::protobuf::RepeatedPtrField<DeleteConditionMessage>::const_iterator it
                     = clone_header.delete_data_conditions().begin();
                     it != clone_header.delete_data_conditions().end(); ++it) {
                    if (it->version() == version.first) {
                        // add it
                        new_local_header.add_delete_condition(*it, version.first);
                        LOG(INFO) << "add delete condition when clone. [table=" << full_name()
                                  << " version=" << it->version() << "]";
                        break;
                    }
                }
            }
        }

        if (res != OLAP_SUCCESS) {
            break;
        }
        VLOG(3) << "load indices successfully when clone. table=" << full_name() << ", "
                << "add_versions_size=" << clone_deltas.size() << ", " 
                << "new_indices_size=" << tmp_data_sources.size();
        // save and reload header
        res = OlapHeaderManager::save(_store, _tablet_id, _schema_hash, &new_local_header);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to save new local header when clone. res:" << res;
            break;
        }
        res = OlapHeaderManager::get_header(_store, _tablet_id, _schema_hash, _header);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to reload original header when clone. [table=" << full_name()
                         << " res=" << res << "]";
            break;
        }

    } while (0);

    // if success, update local data sources
    if (res == OLAP_SUCCESS) {

        // delete local data source
        for (const Version& version_to_delete : versions_to_delete) {
            version_olap_index_map_t::iterator it = _data_sources.find(version_to_delete);
            if (it != _data_sources.end()) {
                std::vector<Rowset*> index_to_delete_vec = it->second;
                _data_sources.erase(it);
                OLAPEngine* unused_index = OLAPEngine::get_instance();
                for (Rowset* rowset : index_to_delete_vec) {
                    unused_index->add_unused_index(rowset);
                }
            }
        }

        // add new data source
        for (auto& it : tmp_data_sources) {
            for (Rowset* rowset : it.second) {
                _data_sources[rowset->version()].push_back(rowset);
            }
        }

    // clear tmp indices if failed
    } else {
        for (auto& it : tmp_data_sources) {
            for (Rowset* rowset : it.second) {
                SAFE_DELETE(rowset);
            }
        }
    }

    LOG(INFO) << "finish to clone data to tablet. res=" << res << ", "
              << "table=" << full_name() << ", "
              << "clone_versions_size=" << clone_deltas.size();
    return res;
}

OLAPStatus OLAPTable::replace_data_sources(const vector<Version>* old_versions,
                                       const vector<Rowset*>* new_data_sources,
                                       vector<Rowset*>* old_data_sources) {
    OLAPStatus res = OLAP_SUCCESS;

    if (old_versions == NULL || new_data_sources == NULL) {
        LOG(WARNING) << "parameter old_versions or new_data_sources is null. table=" << full_name();
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    old_data_sources->clear();

    // check old version existed
    for (vector<Version>::const_iterator it = old_versions->begin();
            it != old_versions->end(); ++it) {
        version_olap_index_map_t::iterator data_source_it = _data_sources.find(*it);
        if (data_source_it == _data_sources.end()) {
            LOG(WARNING) << "olap rowset for version does not exists. [version='" << it->first
                         << "-" << it->second << "' table='" << full_name() << "']";
            return OLAP_ERR_VERSION_NOT_EXIST;
        }
    }

    // check new versions not existed
    for (vector<Rowset*>::const_iterator it = new_data_sources->begin();
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
                LOG(WARNING) << "olap rowset for version exists. [version='" << (*it)->version().first
                             << "-" << (*it)->version().second << "' table='" << full_name() << "']";
                return OLAP_ERR_TABLE_VERSION_DUPLICATE_ERROR;
            }
        }
    }

    // update versions
    for (vector<Version>::const_iterator it = old_versions->begin();
            it != old_versions->end(); ++it) {
        version_olap_index_map_t::iterator data_source_it = _data_sources.find(*it);
        if (data_source_it != _data_sources.end()) {
            for (Rowset* rowset : data_source_it->second) {
                old_data_sources->push_back(rowset);
            }
            _data_sources.erase(data_source_it);
        }

        // 删除失败会导致脏数据
        if ((res = _header->delete_version(*it)) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to delete version from olap header.[version='" << it->first
                         << "-" << it->second << "' table='" << full_name() << "']";
            return res;
        }

        VLOG(3) << "delete version from olap header. table=" << full_name() << ", "
                << "version=" << it->first << "-" << it->second;
    }

    for (vector<Rowset*>::const_iterator it = new_data_sources->begin();
            it != new_data_sources->end(); ++it) {
        _data_sources[(*it)->version()].push_back(*it);

        // 新增失败会导致脏数据
        const std::vector<KeyRange>* column_statistics = nullptr;
        if ((*it)->has_column_statistics()) {
            column_statistics = &((*it)->get_column_statistics());
        }
        res = _header->add_version((*it)->version(), (*it)->version_hash(),
                                   (*it)->rowset_id(), (*it)->num_segments(),
                                   (*it)->index_size(), (*it)->data_size(),
                                   (*it)->num_rows(), (*it)->empty(), column_statistics);

        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to add version to olap header.[version='" << (*it)->version().first
                         << "-" << (*it)->version().second << "' table='" << full_name() << "']";
            return res;
        }

        VLOG(3) << "add version to olap header. table=" << full_name() << ", "
                << "version=" << (*it)->version().first << "-" << (*it)->version().second;
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
            OLAP_LOG_WARNING("fail to find Rowset."
                             "[start_version=%d; end_version=%d]",
                             version_index->first,
                             version_index->second);
            return OLAP_ERR_TABLE_VERSION_INDEX_MISMATCH_ERROR;
        }

        *version_hash ^= temp->second[0]->version_hash();
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::merge_header(const OLAPHeader& hdr, int to_version) {
    obtain_header_wrlock();
    DeferOp release_lock(std::bind<void>(&OLAPTable::release_header_lock, this));

    const PDelta* base_version = _header->get_base_version();
    if (base_version->end_version() != to_version) {
        return OLAP_ERR_VERSION_NOT_EXIST;
    }

    // delete old base version
    Version base = { base_version->start_version(), base_version->end_version() };
    OLAPStatus st = _header->delete_version(base);
    if (st != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to delete version from header" << ", "
            << "version=" << base_version->start_version() << ", "
            << base_version->end_version();
        return st;
    }
    VLOG(3) << "finished to delete version from header"
            << "version=" << base_version->start_version() << "-"
            << base_version->end_version();


    // add new versions
    for (int i = 0; i < hdr.file_delta_size(); ++i) {
        const PDelta* delta = hdr.get_delta(i);
        if (delta->end_version() > to_version) {
            break;
        }
        Version version = { delta->start_version(), delta->end_version() };
        VersionHash v_hash = delta->version_hash();
        for (int j = 0; j < delta->rowset_size(); ++j) {
            const PRowSet& rowset = delta->rowset(j);
            st = _header->add_version(version, v_hash, rowset.rowset_id(),
                                       rowset.num_segments(), rowset.index_size(), rowset.data_size(),
                                       rowset.num_rows(), rowset.empty(), nullptr);
            if (st != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to add version to header" << ", "
                    << "version=" << version.first << "-" << version.second;
                return st;
            }
        }
    }
    st = _header->save();
    if (st != OLAP_SUCCESS) {
       LOG(FATAL) << "failed to save header when merging. tablet:" <<  _tablet_id;
       return st;
    }

    VLOG(3) << "finished to merge header to version:" << to_version << "-" << to_version;
    return OLAP_SUCCESS;
}

Rowset* OLAPTable::_get_largest_index() {
    Rowset* largest_index = NULL;
    size_t largest_index_sizes = 0;

    for (auto& it : _data_sources) {
        // use rowset of base file as target rowset when base is not empty,
        // or try to find the biggest rowset.
        for (Rowset* rowset : it.second) {
            if (rowset->empty() || rowset->zero_num_rows()) {
                continue;
            }
            if (rowset->index_size() > largest_index_sizes) {
                largest_index = rowset;
                largest_index_sizes = rowset->index_size();
            }
        }
    }

    return largest_index;
}

OLAPStatus OLAPTable::split_range(
        const OlapTuple& start_key_strings,
        const OlapTuple& end_key_strings,
        uint64_t request_block_row_count,
        std::vector<OlapTuple>* ranges) {
    if (ranges == NULL) {
        OLAP_LOG_WARNING("parameter end_row is null.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    EntrySlice entry;
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
        if (start_key.init_scan_key(_tablet_schema, start_key_strings.values()) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to initial key strings with RowCursor type.");
            return OLAP_ERR_INIT_FAILED;
        }

        if (start_key.from_tuple(start_key_strings) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("init end key failed");
            return OLAP_ERR_INVALID_SCHEMA;
        }
    } else {
        if (start_key.init(_tablet_schema, num_short_key_fields()) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to initial key strings with RowCursor type.");
            return OLAP_ERR_INIT_FAILED;
        }

        start_key.allocate_memory_for_string_type(_tablet_schema);
        start_key.build_min_key();
    }

    // 和startkey一样处理，没有则用maxkey初始化
    if (end_key_strings.size() > 0) {
        if (OLAP_SUCCESS != end_key.init_scan_key(_tablet_schema, end_key_strings.values())) {
            OLAP_LOG_WARNING("fail to parse strings to key with RowCursor type.");
            return OLAP_ERR_INVALID_SCHEMA;
        }

        if (end_key.from_tuple(end_key_strings) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("init end key failed");
            return OLAP_ERR_INVALID_SCHEMA;
        }
    } else {
        if (end_key.init(_tablet_schema, num_short_key_fields()) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to initial key strings with RowCursor type.");
            return OLAP_ERR_INIT_FAILED;
        }

        end_key.allocate_memory_for_string_type(_tablet_schema);
        end_key.build_max_key();
    }

    ReadLock rdlock(get_header_lock_ptr());
    Rowset* base_index = _get_largest_index();

    // 如果找不到合适的rowset，就直接返回startkey，endkey
    if (base_index == NULL) {
        OLAP_LOG_DEBUG("there is no base file now, may be tablet is empty.");
        // it may be right if the table is empty, so we return success.
        ranges->emplace_back(start_key.to_tuple());
        ranges->emplace_back(end_key.to_tuple());
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

    cur_start_key.attach(entry.data);
    last_start_key.allocate_memory_for_string_type(_tablet_schema);
    last_start_key.copy_without_pool(cur_start_key);
    // start_key是last start_key, 但返回的实际上是查询层给出的key
    ranges->emplace_back(start_key.to_tuple());

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
        cur_start_key.attach(entry.data);

        if (cur_start_key.cmp(last_start_key) != 0) {
            ranges->emplace_back(cur_start_key.to_tuple()); // end of last section
            ranges->emplace_back(cur_start_key.to_tuple()); // start a new section
            last_start_key.copy_without_pool(cur_start_key);
        }
    }

    ranges->emplace_back(end_key.to_tuple());

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
        LOG(WARNING) << "parameter filenames is null. [table='" << full_name() << "']";
        return;
    }

    file_names->clear();

    stringstream prefix_stream;
    prefix_stream << _tablet_path << "/" << _tablet_id;
    string tablet_path_prefix = prefix_stream.str();
    for (auto& it : _data_sources) {
        // every data segment has its file name.
        for (Rowset* rowset : it.second) {
            for (int32_t seg_id = 0; seg_id < rowset->num_segments(); ++seg_id) {
                file_names->insert(basename(construct_file_path(tablet_path_prefix,
                                                                rowset->version(),
                                                                rowset->version_hash(),
                                                                rowset->rowset_id(),
                                                                seg_id,
                                                                file_suffix).c_str()));
            }
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
        const std::vector<Rowset*>& index_vec = it->second;
        VersionEntity version_entity(it->first, index_vec[0]->version_hash());
        for (Rowset* rowset : index_vec) {
            const std::vector<KeyRange>* column_statistics = nullptr;
            if (rowset->has_column_statistics()) {
                column_statistics = &(rowset->get_column_statistics());
            }
            RowSetEntity rowset_entity(rowset->rowset_id(), rowset->num_segments(),
                              rowset->num_rows(), rowset->data_size(),
                              rowset->index_size(), rowset->empty(), column_statistics);
            version_entity.add_rowset_entity(rowset_entity);
        }
        version_entities->push_back(version_entity);
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
        std::vector<Rowset*> index_vec;
        if (unregister_data_source(*it, &index_vec) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to unregister version."
                         << "version=" << it->first << "-" << it->second;
            return;
        }

        for (Rowset* rowset : index_vec) {
            rowset->delete_all_files();
            delete rowset;
        }
    }

    // remove olap header file, _header object will be delete in OLAPTable.destructor
    if (remove_parent_dir(_tablet_path) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to delete header file and directory. header_path=" << _tablet_path;
    }
}

string OLAPTable::construct_index_file_path(const Version& version,
                                            VersionHash version_hash,
                                            int32_t rowset_id,
                                            int32_t segment) const {
    stringstream prefix_stream;
    prefix_stream << _tablet_path << "/" << _tablet_id;
    string tablet_path_prefix = prefix_stream.str();
    return construct_file_path(tablet_path_prefix, version, version_hash, rowset_id, segment, "idx");
}
string OLAPTable::construct_data_file_path(const Version& version,
                                           VersionHash version_hash,
                                           int32_t rowset_id,
                                           int32_t segment) const {
    stringstream prefix_stream;
    prefix_stream << _tablet_path << "/" << _tablet_id;
    string tablet_path_prefix = prefix_stream.str();
    return construct_file_path(tablet_path_prefix, version, version_hash, rowset_id, segment, "dat");
}
string OLAPTable::construct_file_path(const string& tablet_path_prefix,
                                      const Version& version,
                                      VersionHash version_hash,
                                      int32_t rowset_id, int32_t segment,
                                      const string& suffix) {
    char file_path[OLAP_MAX_PATH_LEN];
    if (rowset_id == -1) {
        snprintf(file_path,
                 sizeof(file_path),
                 "%s_%ld_%ld_%ld_%d.%s",
                 tablet_path_prefix.c_str(),
                 version.first,
                 version.second,
                 version_hash,
                 segment,
                 suffix.c_str());
    } else {
        snprintf(file_path,
                 sizeof(file_path),
                 "%s_%ld_%ld_%ld_%d_%d.%s",
                 tablet_path_prefix.c_str(),
                 version.first,
                 version.second,
                 version_hash,
                 rowset_id, segment,
                 suffix.c_str());
    }

    return file_path;
}

string OLAPTable::construct_incremental_delta_dir_path() const {
    stringstream rowset_dir_path;
    rowset_dir_path << _tablet_path << INCREMENTAL_DELTA_PREFIX;

    return rowset_dir_path.str();
}
string OLAPTable::construct_incremental_index_file_path(Version version, VersionHash version_hash,
                                                  int32_t rowset_id, int32_t segment) const {
    string rowset_dir_path = construct_incremental_delta_dir_path();
    stringstream rowset_file_path;
    rowset_file_path << rowset_dir_path << "/"
                    << construct_file_name(version, version_hash, rowset_id, segment, "idx");
    return rowset_file_path.str();
}
string OLAPTable::construct_incremental_data_file_path(Version version, VersionHash version_hash,
                                                  int32_t rowset_id, int32_t segment) const {
    string rowset_dir_path = construct_incremental_delta_dir_path();
    stringstream rowset_file_path;
    rowset_file_path << rowset_dir_path << "/"
                    << construct_file_name(version, version_hash, rowset_id, segment, "dat");
    return rowset_file_path.str();
}
string OLAPTable::construct_pending_data_dir_path() const {
    return _tablet_path + PENDING_DELTA_PREFIX;
}
string OLAPTable::construct_pending_index_file_path(TTransactionId transaction_id,
                                                    int32_t rowset_id, int32_t segment) const {
    string dir_path = construct_pending_data_dir_path();
    stringstream file_path;
    file_path << dir_path << "/"
                          << transaction_id << "_"
                          << rowset_id << "_" << segment << ".idx";

    return file_path.str();
}
string OLAPTable::construct_pending_data_file_path(TTransactionId transaction_id,
                                                   int32_t rowset_id, int32_t segment) const {
    string dir_path = construct_pending_data_dir_path();
    stringstream file_path;
    file_path << dir_path << "/"
                          << transaction_id << "_"
                          << rowset_id << "_" << segment << ".dat";

    return file_path.str();
}

string OLAPTable::construct_file_name(const Version& version,
                                      VersionHash version_hash,
                                      int32_t rowset_id, int32_t segment,
                                      const string& suffix) const {
    char file_name[OLAP_MAX_PATH_LEN];
    snprintf(file_name, sizeof(file_name),
             "%ld_%ld_%ld_%ld_%d_%d.%s",
             _tablet_id,
             version.first,
             version.second,
             version_hash,
             rowset_id,
             segment,
             suffix.c_str());

    return file_name;
}

string OLAPTable::construct_dir_path() const {
    return _tablet_path;
}

int32_t OLAPTable::get_field_index(const string& field_name) const {
    field_index_map_t::const_iterator res_iterator = _field_index_map.find(field_name);
    if (res_iterator == _field_index_map.end()) {
        LOG(WARNING) << "invalid field name. [name='" << field_name << "']";
        return -1;
    }

    return res_iterator->second;
}

size_t OLAPTable::get_field_size(const string& field_name) const {
    field_index_map_t::const_iterator res_iterator = _field_index_map.find(field_name);
    if (res_iterator == _field_index_map.end()) {
        LOG(WARNING) << "invalid field name. [name='" << field_name << "']";
        return 0;
    }

    if (static_cast<size_t>(res_iterator->second) >= _field_sizes.size()) {
        LOG(WARNING) << "invalid field rowset. [name='" << field_name << "']";
        return 0;
    }

    return _field_sizes[res_iterator->second];
}

size_t OLAPTable::get_return_column_size(const string& field_name) const {
    field_index_map_t::const_iterator res_iterator = _field_index_map.find(field_name);
    if (res_iterator == _field_index_map.end()) {
        LOG(WARNING) << "invalid field name. [name='" << field_name << "']";
        return 0;
    }

    if (static_cast<size_t>(res_iterator->second) >= _field_sizes.size()) {
        LOG(WARNING) << "invalid field rowset. [name='" << field_name << "']";
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
    for (const PDelta& delta : _header->delta()) {
        for (const PRowSet& prowset : delta.rowset()) {
            total_size += prowset.data_size();
        }
    }

    return total_size;
}

int64_t OLAPTable::get_num_rows() const {
    int64_t num_rows = 0;
    for (const PDelta& delta : _header->delta()) {
        for (const PRowSet& prowset : delta.rowset()) {
            num_rows += prowset.num_rows();
        }
    }

    return num_rows;
}

bool OLAPTable::is_load_delete_version(Version version) {
    version_olap_index_map_t::iterator it = _data_sources.find(version);
    return it->second[0]->delete_flag();
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
                                          vector<Version>* versions_to_changed,
                                          AlterTabletType* alter_table_type) const {
    if (!_header->has_schema_change_status()) {
        return false;
    }

    const SchemaChangeStatusMessage& schema_change_status = _header->schema_change_status();

    (tablet_id == NULL || (*tablet_id = schema_change_status.related_tablet_id()));
    (schema_hash == NULL || (*schema_hash = schema_change_status.related_schema_hash()));
    (alter_table_type == NULL || (*alter_table_type =
            static_cast<AlterTabletType>(schema_change_status.schema_change_type())));

    if (versions_to_changed != NULL) {
        versions_to_changed->clear();
        for (int i = 0, len = schema_change_status.versions_to_changed_size(); i < len; ++i) {
            const PDelta& version = schema_change_status.versions_to_changed(i);
            versions_to_changed->push_back(
                    Version(version.start_version(), version.end_version()));
        }
    }

    return true;
}

void OLAPTable::set_schema_change_request(TTabletId tablet_id,
                                          TSchemaHash schema_hash,
                                          const vector<Version>& versions_to_changed,
                                          const AlterTabletType alter_table_type) {
    clear_schema_change_request();

    SchemaChangeStatusMessage* schema_change_status = _header->mutable_schema_change_status();
    schema_change_status->set_related_tablet_id(tablet_id);
    schema_change_status->set_related_schema_hash(schema_hash);

    vector<Version>::const_iterator it;
    for (it = versions_to_changed.begin(); it != versions_to_changed.end(); ++it) {
        PDelta* version = schema_change_status->add_versions_to_changed();
        version->set_start_version(it->first);
        version->set_end_version(it->second);
        version->set_version_hash(0);
        version->set_creation_time(0);
        //version->set_index_size(0);
        //version->set_data_size(0);
        //version->set_num_segments(0);
    }

    schema_change_status->set_schema_change_type(alter_table_type);
}

bool OLAPTable::remove_last_schema_change_version(OLAPTablePtr new_olap_table) {
    if (!_header->has_schema_change_status()) {
        return false;
    }

    if (_header->has_schema_change_status()) {
        SchemaChangeStatusMessage* schema_change_status = _header->mutable_schema_change_status();
        ::google::protobuf::RepeatedPtrField<PDelta>* versions_to_changed
            = schema_change_status->mutable_versions_to_changed();

        if (versions_to_changed->size() > 0) {
            versions_to_changed->RemoveLast();
        }
    }

    return true;
}

void OLAPTable::clear_schema_change_request() {
    LOG(INFO) << "clear schema change status. [tablet='" << _full_name << "']";
    _header->clear_schema_change_status();
}

void OLAPTable::set_io_error() {
    OLAP_LOG_WARNING("io error occur.[tablet_full_name='%s', root_path_name='%s']",
                     _full_name.c_str(),
                     _storage_root_path.c_str());
    OLAPEngine::get_instance()->set_store_used_flag(_storage_root_path, false);
}

bool OLAPTable::is_used() {
    return _store->is_used();
}

VersionEntity OLAPTable::get_version_entity_by_version(const Version& version) {
    std::vector<Rowset*>& index_vec = _data_sources[version];
    VersionEntity version_entity(version, index_vec[0]->version_hash());
    for (Rowset* rowset : index_vec) {
        const std::vector<KeyRange>* column_statistics = nullptr;
        if (rowset->has_column_statistics()) {
            column_statistics = &(rowset->get_column_statistics());
        }
        RowSetEntity rowset_entity(rowset->rowset_id(), rowset->num_segments(),
                          rowset->num_rows(), rowset->data_size(),
                          rowset->index_size(), rowset->empty(), column_statistics);
        version_entity.add_rowset_entity(rowset_entity);
    }
    return version_entity;
}

size_t OLAPTable::get_version_index_size(const Version& version) {
    std::vector<Rowset*>& index_vec = _data_sources[version];
    size_t index_size = 0;
    for (Rowset* rowset : index_vec) {
        index_size += rowset->index_size();
    }
    return index_size;
}

size_t OLAPTable::get_version_data_size(const Version& version) {
    std::vector<Rowset*>& index_vec = _data_sources[version];
    size_t data_size = 0;
    for (Rowset* rowset : index_vec) {
        data_size += rowset->data_size();
    }
    return data_size;
}

OLAPStatus OLAPTable::recover_tablet_until_specfic_version(
            const int64_t& until_version, const int64_t& version_hash) {
    std::vector<Version> missing_versions;
    {
        ReadLock rdlock(&_header_lock);
        get_missing_versions_with_header_locked(until_version, &missing_versions);
    }

    std::vector<Rowset*> rowset_vec;
    OLAPStatus res = OLAP_SUCCESS;
    for (Version& missing_version : missing_versions) {
        Rowset* rowset = new Rowset(this, missing_version, version_hash, false, 0, 0);
        rowset->set_empty(true);
        IWriter* writer = IWriter::create(std::shared_ptr<OLAPTable>(this), rowset, true);
        if (res != OLAP_SUCCESS) { break; }

        res = writer->finalize();
        if (res != OLAP_SUCCESS) { break; }
        rowset_vec.push_back(rowset);
    }

    if (res != OLAP_SUCCESS) {
        for (Rowset* rowset : rowset_vec) {
            rowset->delete_all_files();
            SAFE_DELETE(rowset);
        }
    } else {
        for (Rowset* rowset : rowset_vec) {
            rowset->load();
        }
    }

    {
        WriteLock wrlock(&_header_lock);
        RETURN_NOT_OK(register_data_source(rowset_vec));
        RETURN_NOT_OK(save_header());
    }
    return OLAP_SUCCESS;
}

OLAPStatus OLAPTable::test_version(const Version& version) {
    vector<Version> span_versions;
    obtain_header_rdlock();
    OLAPStatus res = _header->select_versions_to_span(version, &span_versions);
    release_header_lock();

    return res;
}

}  // namespace palo
