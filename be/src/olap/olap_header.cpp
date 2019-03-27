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

#include "olap/olap_header.h"

#include <algorithm>
#include <fstream>
#include <queue>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "olap/field.h"
#include "olap/wrapper_field.h"
#include "olap/file_helper.h"
#include "olap/utils.h"

using google::protobuf::RepeatedPtrField;
using std::ifstream;
using std::ios;
using std::list;
using std::make_pair;
using std::ofstream;
using std::queue;
using std::sort;
using std::string;
using std::stringstream;
using std::unordered_map;
using std::vector;

namespace doris {
// related static functions of version graph

// Construct version graph(using adjacency list) from header's information.
static OLAPStatus construct_version_graph(
        const RepeatedPtrField<PDelta>& versions_in_header,
        vector<Vertex>* version_graph,
        unordered_map<int, int>* vertex_helper_map);

// Clear version graph and vertex_helper_map, release memory hold by version_graph.
static OLAPStatus clear_version_graph(vector<Vertex>* version_graph,
                                  unordered_map<int, int>* vertex_helper_map);

// Add version to graph, it is called near the end of add_version
static OLAPStatus add_version_to_graph(const Version& version,
                                   vector<Vertex>* version_graph,
                                   unordered_map<int, int>* vertex_helper_map);

// Delete version from graph, it is called near the end of delete_version
static OLAPStatus delete_version_from_graph(
        const RepeatedPtrField<PDelta>& versions_in_header,
        const Version& version,
        vector<Vertex>* version_graph,
        unordered_map<int, int>* vertex_helper_map);

// Add vertex to graph, if vertex already exists, still return SUCCESS.
static OLAPStatus add_vertex_to_graph(int vertex_value,
                                  vector<Vertex>* version_graph,
                                  unordered_map<int, int>* vertex_helper_map);

OLAPHeader::~OLAPHeader() {
    // Release memory of version graph.
    clear_version_graph(&_version_graph, &_vertex_helper_map);
    Clear();
}

void OLAPHeader::change_file_version_to_delta() {
    // convert FileVersionMessage to PDelta and PSegmentGroup in initialization.
    // FileVersionMessage is used in previous code, and PDelta and PSegmentGroup
    // is used in streaming load branch.
    for (int i = 0; i < file_version_size(); ++i) {
        PDelta* delta = add_delta();
        _convert_file_version_to_delta(file_version(i), delta);
    }

    clear_file_version();
}

OLAPStatus OLAPHeader::init() {
    clear_version_graph(&_version_graph, &_vertex_helper_map);
    if (construct_version_graph(delta(),
                                &_version_graph,
                                &_vertex_helper_map) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to construct version graph.");
        return OLAP_ERR_OTHER_ERROR;
    }
    if (_file_name == "") {
        stringstream file_stream;
        file_stream << tablet_id() << ".hdr";
        _file_name = file_stream.str();
    }
    return OLAP_SUCCESS;
}

OLAPStatus OLAPHeader::load_and_init() {
    // check the tablet_path is not empty
    if (_file_name == "") {
        LOG(WARNING) << "file_path is empty for header";
        return OLAP_ERR_DIR_NOT_EXIST;
    }

    FileHeader<OLAPHeaderMessage> file_header;
    FileHandler file_handler;

    if (file_handler.open(_file_name.c_str(), O_RDONLY) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to open index file. [file='" << _file_name << "']";
        return OLAP_ERR_IO_ERROR;
    }

    // In file_header.unserialize(), it validates file length, signature, checksum of protobuf.
    if (file_header.unserialize(&file_handler) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to unserialize header. [path='" << _file_name << "']";
        return OLAP_ERR_PARSE_PROTOBUF_ERROR;
    }

    try {
        CopyFrom(file_header.message());
    } catch (...) {
        LOG(WARNING) << "fail to copy protocol buffer object. [path='" << _file_name << "']";
        return OLAP_ERR_PARSE_PROTOBUF_ERROR;
    }

    if (file_version_size() != 0) {
        // convert FileVersionMessage to PDelta and PSegmentGroup in initialization.
        for (int i = 0; i < file_version_size(); ++i) {
            PDelta* delta = add_delta();
            _convert_file_version_to_delta(file_version(i), delta);
        }

        clear_file_version();
        OLAPStatus res = save();
        if (res != OLAP_SUCCESS) {
            LOG(FATAL) << "failed to remove file version in initialization";
        }
    }
    return init();
}

OLAPStatus OLAPHeader::load_for_check() {
    FileHeader<OLAPHeaderMessage> file_header;
    FileHandler file_handler;

    if (file_handler.open(_file_name.c_str(), O_RDONLY) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to open index file. [file='%s']", _file_name.c_str());
        return OLAP_ERR_IO_ERROR;
    }

    // In file_header.unserialize(), it validates file length, signature, checksum of protobuf.
    if (file_header.unserialize(&file_handler) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to unserialize header. [path='%s']", _file_name.c_str());
        return OLAP_ERR_PARSE_PROTOBUF_ERROR;
    }

    try {
        CopyFrom(file_header.message());
    } catch (...) {
        OLAP_LOG_WARNING("fail to copy protocol buffer object. [path='%s']", _file_name.c_str());
        return OLAP_ERR_PARSE_PROTOBUF_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPHeader::save() {
    return save(_file_name);
}

OLAPStatus OLAPHeader::save(const string& file_path) {
    // check the tablet_path is not empty
    if (file_path == "") {
        LOG(WARNING) << "file_path is empty for header";
        return OLAP_ERR_DIR_NOT_EXIST;
    }

    FileHeader<OLAPHeaderMessage> file_header;
    FileHandler file_handler;

    if (file_handler.open_with_mode(file_path.c_str(),
            O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to open header file. [file='" << file_path << "']";
        return OLAP_ERR_IO_ERROR;
    }

    try {
        file_header.mutable_message()->CopyFrom(*this);
    } catch (...) {
        LOG(WARNING) << "fail to copy protocol buffer object. [path='" << file_path << "']";
        return OLAP_ERR_OTHER_ERROR;
    }

    if (file_header.prepare(&file_handler) != OLAP_SUCCESS
            || file_header.serialize(&file_handler) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to serialize to file header. [path='" << file_path << "']";
        return OLAP_ERR_SERIALIZE_PROTOBUF_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPHeader::add_version(Version version, VersionHash version_hash,
                int32_t segment_group_id, int32_t num_segments,
                int64_t index_size, int64_t data_size, int64_t num_rows,
                bool empty, const std::vector<KeyRange>* column_statistics) {
    // Check whether version is valid.
    if (version.first > version.second) {
        LOG(WARNING) << "the version is not valid."
                     << "version=" << version.first << "-" << version.second;
        return OLAP_ERR_HEADER_ADD_VERSION;
    }

    int delta_id = -1;
    for (int i = 0; i < delta_size(); ++i) {
        if (delta(i).start_version() == version.first
            && delta(i).end_version() == version.second) {
            for (const PSegmentGroup& segment_group : delta(i).segment_group()) {
                if (segment_group.segment_group_id() == segment_group_id) {
                    LOG(WARNING) << "the version is existed."
                        << "version=" << version.first << "-"
                        << version.second;
                    return OLAP_ERR_HEADER_ADD_VERSION;
                }
            }
            delta_id = i;
            break;
        }
    }

    // if segment_group_id is greater or equal than zero, it is used
    // to streaming load

    // Try to add version to protobuf.
    PDelta* new_delta = nullptr;
    try {
        if (segment_group_id == -1 || delta_id == -1) {
            // snapshot will use segment_group_id which equals minus one
            new_delta = add_delta();
            new_delta->set_start_version(version.first);
            new_delta->set_end_version(version.second);
            new_delta->set_version_hash(version_hash);
            new_delta->set_creation_time(time(NULL));
        } else {
            new_delta = const_cast<PDelta*>(&delta(delta_id));
        }
        PSegmentGroup* new_segment_group = new_delta->add_segment_group();
        new_segment_group->set_segment_group_id(segment_group_id);
        new_segment_group->set_num_segments(num_segments);
        new_segment_group->set_index_size(index_size);
        new_segment_group->set_data_size(data_size);
        new_segment_group->set_num_rows(num_rows);
        new_segment_group->set_empty(empty);
        if (NULL != column_statistics) {
            for (size_t i = 0; i < column_statistics->size(); ++i) {
                ColumnPruning *column_pruning =
                    new_segment_group->add_column_pruning();
                column_pruning->set_min(column_statistics->at(i).first->to_string());
                column_pruning->set_max(column_statistics->at(i).second->to_string());
                column_pruning->set_null_flag(column_statistics->at(i).first->is_null());
            }
        }
    } catch (...) {
        OLAP_LOG_WARNING("add file version to protobf error");
        return OLAP_ERR_HEADER_ADD_VERSION;
    }

    if (add_version_to_graph(version, &_version_graph, &_vertex_helper_map) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to add version to graph. [version='%d-%d']",
                         version.first,
                         version.second);
        return OLAP_ERR_HEADER_ADD_VERSION;
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPHeader::add_pending_version(
        int64_t partition_id, int64_t transaction_id,
        const std::vector<string>* delete_conditions) {
    for (int i = 0; i < pending_delta_size(); ++i) {
        if (pending_delta(i).transaction_id() == transaction_id) {
            LOG(WARNING) << "pending delta already exists in header."
                         << "transaction_id: " << transaction_id;
            return OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST;
        }
    }

    try {
        PPendingDelta* new_pending_delta = add_pending_delta();
        new_pending_delta->set_partition_id(partition_id);
        new_pending_delta->set_transaction_id(transaction_id);
        new_pending_delta->set_creation_time(time(NULL));

        if (delete_conditions != nullptr) {
            DeleteConditionMessage* del_cond = new_pending_delta->mutable_delete_condition();
            del_cond->set_version(0);
            for (const string& condition : *delete_conditions) {
                del_cond->add_sub_conditions(condition);
                LOG(INFO) << "store one sub-delete condition. condition=" << condition
                          << ", transaction_id=" << transaction_id;
            }
        }

    } catch (...) {
        LOG(WARNING) << "fail to add pending segment_group to header protobf";
        return OLAP_ERR_HEADER_ADD_PENDING_DELTA;
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPHeader::add_pending_segment_group(
        int64_t transaction_id, int32_t num_segments,
        int32_t pending_segment_group_id, const PUniqueId& load_id,
        bool empty, const std::vector<KeyRange>* column_statistics) {

    int32_t delta_id = 0;
    for (int32_t i = 0; i < pending_delta_size(); ++i) {
        const PPendingDelta& delta = pending_delta(i);
        if (delta.transaction_id() == transaction_id) {
            delta_id = i;
            for (int j = 0; j < delta.pending_segment_group_size(); ++j) {
                const PPendingSegmentGroup& pending_segment_group = delta.pending_segment_group(j);
                if (pending_segment_group.pending_segment_group_id() == pending_segment_group_id) {
                    LOG(WARNING) << "pending segment_group already exists in header."
                        << "transaction_id:" << transaction_id
                        << ", pending_segment_group_id: " << pending_segment_group_id;
                    return OLAP_ERR_HEADER_ADD_PENDING_DELTA;
                }
            }
        }
    }

    try {
        PPendingSegmentGroup* new_pending_segment_group
            = const_cast<PPendingDelta&>(pending_delta(delta_id)).add_pending_segment_group();
        new_pending_segment_group->set_pending_segment_group_id(pending_segment_group_id);
        new_pending_segment_group->set_num_segments(num_segments);
        new_pending_segment_group->mutable_load_id()->set_hi(load_id.hi());
        new_pending_segment_group->mutable_load_id()->set_lo(load_id.lo());
        new_pending_segment_group->set_empty(empty);
        if (NULL != column_statistics) {
            for (size_t i = 0; i < column_statistics->size(); ++i) {
                ColumnPruning *column_pruning =
                    new_pending_segment_group->add_column_pruning();
                column_pruning->set_min(column_statistics->at(i).first->to_string());
                column_pruning->set_max(column_statistics->at(i).second->to_string());
                column_pruning->set_null_flag(column_statistics->at(i).first->is_null());
            }
        }
    } catch (...) {
        OLAP_LOG_WARNING("fail to add pending segment_group to protobf");
        return OLAP_ERR_HEADER_ADD_PENDING_DELTA;
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPHeader::add_incremental_version(Version version, VersionHash version_hash,
                int32_t segment_group_id, int32_t num_segments,
                int64_t index_size, int64_t data_size, int64_t num_rows,
                bool empty, const std::vector<KeyRange>* column_statistics) {
    // Check whether version is valid.
    if (version.first != version.second) {
        OLAP_LOG_WARNING("the incremental version is not valid. [version=%d]", version.first);
        return OLAP_ERR_HEADER_ADD_INCREMENTAL_VERSION;
    }

    // Check whether the version is existed.
    int32_t delta_id = 0;
    for (int i = 0; i < incremental_delta_size(); ++i) {
        const PDelta& incre_delta = incremental_delta(i);
        if (incre_delta.start_version() == version.first) {
            delta_id = i;
            for (int j = 0; j < incre_delta.segment_group_size(); ++j) {
                const PSegmentGroup& incremental_segment_group = incre_delta.segment_group(j);
                if (incremental_segment_group.segment_group_id() == segment_group_id) {
                    LOG(WARNING) << "segment_group already exists in header."
                        << "version: " << version.first << "-" << version.second << ","
                        << "segment_group_id: " << segment_group_id;
                    return OLAP_ERR_HEADER_ADD_PENDING_DELTA;
                }
            }
        }
    }

    // Try to add version to protobuf.
    try {
        PDelta* new_incremental_delta = nullptr;
        if (segment_group_id == 0) {
            new_incremental_delta = add_incremental_delta();
            new_incremental_delta->set_start_version(version.first);
            new_incremental_delta->set_end_version(version.second);
            new_incremental_delta->set_version_hash(version_hash);
            new_incremental_delta->set_creation_time(time(NULL));
        } else {
            new_incremental_delta = const_cast<PDelta*>(&incremental_delta(delta_id));
        }
        PSegmentGroup* new_incremental_segment_group = new_incremental_delta->add_segment_group();
        new_incremental_segment_group->set_segment_group_id(segment_group_id);
        new_incremental_segment_group->set_num_segments(num_segments);
        new_incremental_segment_group->set_index_size(index_size);
        new_incremental_segment_group->set_data_size(data_size);
        new_incremental_segment_group->set_num_rows(num_rows);
        new_incremental_segment_group->set_empty(empty);
        if (NULL != column_statistics) {
            for (size_t i = 0; i < column_statistics->size(); ++i) {
                ColumnPruning *column_pruning =
                    new_incremental_segment_group->add_column_pruning();
                column_pruning->set_min(column_statistics->at(i).first->to_string());
                column_pruning->set_max(column_statistics->at(i).second->to_string());
                column_pruning->set_null_flag(column_statistics->at(i).first->is_null());
            }
        }
    } catch (...) {
        OLAP_LOG_WARNING("add incremental version to protobf error");
        return OLAP_ERR_HEADER_ADD_INCREMENTAL_VERSION;
    }

    return OLAP_SUCCESS;
}

void OLAPHeader::add_delete_condition(const DeleteConditionMessage& delete_condition,
                                      int64_t version) {
    // check whether condition exist
    DeleteConditionMessage* del_cond = NULL;
    int i = 0;
    for (; i < delete_data_conditions_size(); i++) {
        DeleteConditionMessage temp = delete_data_conditions().Get(i);
        if (temp.version() == version) {
            break;
        }
    }

    // clear existed condition
    if (i < delete_data_conditions_size()) {
        del_cond = mutable_delete_data_conditions(i);
        del_cond->clear_sub_conditions();
    } else {
        del_cond = add_delete_data_conditions();
        del_cond->set_version(version);
    }

    for (const string& condition : delete_condition.sub_conditions()) {
        del_cond->add_sub_conditions(condition);
    }
    LOG(INFO) << "add delete condition. version=" << version;
}

void OLAPHeader::delete_cond_by_version(const Version& version) {
    DCHECK(version.first == version.second);
    google::protobuf::RepeatedPtrField<DeleteConditionMessage>* delete_conditions
            = mutable_delete_data_conditions();
    int index = 0;
    for (; index < delete_conditions->size(); ++index) {
        const DeleteConditionMessage& temp = delete_conditions->Get(index);
        if (temp.version() == version.first) {
            // log delete condtion
            string del_cond_str;
            const RepeatedPtrField<string>& sub_conditions = temp.sub_conditions();

            for (int i = 0; i != sub_conditions.size(); ++i) {
                del_cond_str += sub_conditions.Get(i) + ";";
            }

            LOG(INFO) << "delete one condition. version=" << temp.version()
                      << ", condition=" << del_cond_str;

            // remove delete condition from PB
            delete_conditions->SwapElements(index, delete_conditions->size() - 1);
            delete_conditions->RemoveLast();
        }
    }
}

bool OLAPHeader::is_delete_data_version(Version version) {
    if (version.first != version.second) {
        return false;
    }

    google::protobuf::RepeatedPtrField<DeleteConditionMessage>::const_iterator it;
    it = delete_data_conditions().begin();
    for (; it != delete_data_conditions().end(); ++it) {
        if (it->version() == version.first) {
            return true;
        }
    }

    return false;
}

const PPendingDelta* OLAPHeader::get_pending_delta(int64_t transaction_id) const {
    for (int i = 0; i < pending_delta_size(); i++) {
        if (pending_delta(i).transaction_id() == transaction_id) {
            return &pending_delta(i);
        }
    }
    return nullptr;
}

const PPendingSegmentGroup* OLAPHeader::get_pending_segment_group(int64_t transaction_id,
        int32_t pending_segment_group_id) const {
    for (int i = 0; i < pending_delta_size(); i++) {
        if (pending_delta(i).transaction_id() == transaction_id) {
            const PPendingDelta& delta = pending_delta(i);
            for (int j = 0; j < delta.pending_segment_group_size(); ++j) {
                const PPendingSegmentGroup& pending_segment_group = delta.pending_segment_group(j);
                if (pending_segment_group.pending_segment_group_id() == pending_segment_group_id) {
                    return &pending_segment_group;
                }
            }
        }
    }
    return nullptr;
}

const PDelta* OLAPHeader::get_incremental_version(Version version) const {
    for (int i = 0; i < incremental_delta_size(); i++) {
        if (incremental_delta(i).start_version() == version.first
            && incremental_delta(i).end_version() == version.second) {
            return &incremental_delta(i);
        }
    }
    return nullptr;
}

OLAPStatus OLAPHeader::delete_version(Version version) {
    // Find the version that need to be deleted.
    int index = -1;
    for (int i = 0; i < delta_size(); ++i) {
        if (delta(i).start_version() == version.first
                && delta(i).end_version() == version.second) {
            index = i;
            break;
        }
    }

    // Delete version from protobuf.
    if (index != -1) {
        RepeatedPtrField<PDelta>* version_ptr = mutable_delta();
        for (int i = index; i < delta_size() - 1; ++i) {
            version_ptr->SwapElements(i, i + 1);
        }

        version_ptr->RemoveLast();
    }

    // Atomic delete is not supported now.
    if (delete_version_from_graph(delta(), version,
                                  &_version_graph,
                                  &_vertex_helper_map) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to delete version from graph. [version='%d-%d']",
                         version.first,
                         version.second);
        return OLAP_ERR_HEADER_DELETE_VERSION;
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPHeader::delete_all_versions() {
    clear_file_version();
    clear_delta();
    clear_pending_delta();
    clear_incremental_delta();
    clear_version_graph(&_version_graph, &_vertex_helper_map);

    if (construct_version_graph(delta(),
                                &_version_graph,
                                &_vertex_helper_map) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to construct version graph.");
        return OLAP_ERR_OTHER_ERROR;
    }
    return OLAP_SUCCESS;
}

void OLAPHeader::delete_pending_delta(int64_t transaction_id) {
    int index = -1;
    for (int i = 0; i < pending_delta_size(); ++i) {
        if (pending_delta(i).transaction_id() == transaction_id) {
            index = i;
            break;
        }
    }

    if (index != -1) {
        RepeatedPtrField<PPendingDelta>* pending_delta_ptr = mutable_pending_delta();
        for (int i = index; i < pending_delta_size() - 1; ++i) {
            pending_delta_ptr->SwapElements(i, i + 1);
        }

        pending_delta_ptr->RemoveLast();
    }
}

void OLAPHeader::delete_incremental_delta(Version version) {
    int index = -1;
    for (int i = 0; i < incremental_delta_size(); ++i) {
        if (incremental_delta(i).start_version() == version.first
             && incremental_delta(i).end_version() == version.second) {
            index = i;
            break;
        }
    }

    if (index != -1) {
        RepeatedPtrField<PDelta>* version_ptr = mutable_incremental_delta();
        for (int i = index; i < incremental_delta_size() - 1; ++i) {
            version_ptr->SwapElements(i, i + 1);
        }

        version_ptr->RemoveLast();
    }
}

// This function is called when base-compaction, cumulative-compaction, quering.
// we use BFS algorithm to get the shortest version path.
OLAPStatus OLAPHeader::select_versions_to_span(const Version& target_version,
                                           vector<Version>* span_versions) {
    if (target_version.first > target_version.second) {
        OLAP_LOG_WARNING("invalid param target_version. [start_version_id=%d end_version_id=%d]",
                         target_version.first,
                         target_version.second);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (span_versions == NULL) {
        OLAP_LOG_WARNING("param span_versions is NULL.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // bfs_queue's element is vertex_index.
    queue<int> bfs_queue;
    // predecessor[i] means the predecessor of vertex_index 'i'.
    vector<int> predecessor(_version_graph.size());
    // visited[int]==true means it had entered bfs_queue.
    vector<bool> visited(_version_graph.size());
    // [start_vertex_value, end_vertex_value)
    int start_vertex_value = target_version.first;
    int end_vertex_value = target_version.second + 1;
    // -1 is invalid vertex index.
    int start_vertex_index = -1;
    // -1 is valid vertex index.
    int end_vertex_index = -1;
    // Sometimes, the version path can not have reverse version even you set
    // _support_reverse_version to be true.
    bool can_support_reverse = _support_reverse_version;

    // Check schema to see if we can support reverse version in the version
    // path. If the aggregation type of any value column is SUM, then we can
    // not support reverse version.
    for (int i = 0; can_support_reverse && i < column_size(); ++i) {
        if (column(i).is_key() == false && column(i).aggregation().compare("SUM") != 0) {
            can_support_reverse = false;
        }
    }

    for (size_t i = 0; i < _version_graph.size(); ++i) {
        if (_version_graph[i].value == start_vertex_value) {
            start_vertex_index = i;
        }
        if (_version_graph[i].value == end_vertex_value) {
            end_vertex_index = i;
        }
    }

    if (start_vertex_index < 0 || end_vertex_index < 0) {
        OLAP_LOG_WARNING("fail to find version in version list. "
                         "[start_version_id=%d end_version_id=%d tmp_start=%d tmp_end=%d]",
                         target_version.first,
                         target_version.second,
                         start_vertex_index,
                         end_vertex_index);
        return OLAP_ERR_VERSION_NOT_EXIST;
    }

    for (int i = 0; i < static_cast<int>(_version_graph.size()); ++i) {
        visited[i] = false;
    }

    bfs_queue.push(start_vertex_index);
    visited[start_vertex_index] = true;
    // The predecessor of root is itself.
    predecessor[start_vertex_index] = start_vertex_index;

    while (bfs_queue.empty() == false && visited[end_vertex_index] == false) {
        int top_vertex_index = bfs_queue.front();
        bfs_queue.pop();

        for (list<int>::const_iterator it = _version_graph[top_vertex_index].edges->begin();
                it != _version_graph[top_vertex_index].edges->end(); ++it) {
            if (visited[*it] == false) {
                // If we don't support reverse version in the path, and start vertex
                // value is larger than the end vertex value, we skip this edge.
                if (can_support_reverse == false
                        && _version_graph[top_vertex_index].value > _version_graph[*it].value) {
                    continue;
                }

                visited[*it] = true;
                predecessor[*it] = top_vertex_index;
                bfs_queue.push(*it);
            }
        }
    }

    if (visited[end_vertex_index] == false) {
        OLAP_LOG_WARNING("fail to find path to end_version in version list. "
                         "[start_version_id=%d end_version_id=%d]",
                         target_version.first,
                         target_version.second);
        return OLAP_ERR_VERSION_NOT_EXIST;
    }

    vector<int> reversed_path;
    int tmp_vertex_index = end_vertex_index;
    reversed_path.push_back(tmp_vertex_index);

    // For start_vertex_index, its predecessor must be itself.
    while (predecessor[tmp_vertex_index] != tmp_vertex_index) {
        tmp_vertex_index = predecessor[tmp_vertex_index];
        reversed_path.push_back(tmp_vertex_index);
    }

    // Make span_versions from reversed_path.
    stringstream shortest_path_for_debug;
    for (int path_id = reversed_path.size() - 1; path_id > 0; --path_id) {
        int tmp_start_vertex_value = _version_graph[reversed_path[path_id]].value;
        int tmp_end_vertex_value = _version_graph[reversed_path[path_id - 1]].value;

        // tmp_start_vertex_value mustn't be equal to tmp_end_vertex_value
        if (tmp_start_vertex_value <= tmp_end_vertex_value) {
            span_versions->push_back(make_pair(tmp_start_vertex_value, tmp_end_vertex_value - 1));
        } else {
            span_versions->push_back(make_pair(tmp_end_vertex_value, tmp_start_vertex_value - 1));
        }

        shortest_path_for_debug << (*span_versions)[span_versions->size() - 1].first << '-'
                                << (*span_versions)[span_versions->size() - 1].second << ' ';
    }

    VLOG(10) << "calculated shortest path. "
             << "version=" << target_version.first << "-" << target_version.second
             << "path=" << shortest_path_for_debug.str();

    return OLAP_SUCCESS;
}

const PDelta* OLAPHeader::get_lastest_delta_version() const {
    if (delta_size() == 0) {
        return nullptr;
    }

    const PDelta* max_delta = nullptr;
    for (int i = delta_size() - 1; i >= 0; --i) {
        if (delta(i).start_version() == delta(i).end_version()) {
            if (max_delta == nullptr) {
                max_delta = &delta(i);
            } else if (delta(i).start_version() > max_delta->start_version()) {
                max_delta = &delta(i);
            }
        }
    }
    if (max_delta != nullptr) {
        LOG(INFO) << "max_delta:" << max_delta->start_version() << ","
            << max_delta->end_version();
    }
    return max_delta;
}

const PDelta* OLAPHeader::get_lastest_version() const {
    if (delta_size() == 0) {
        return nullptr;
    }

    const PDelta* max_delta = nullptr;
    for (int i = delta_size() - 1; i >= 0; --i) {
        if (max_delta == nullptr) {
            max_delta = &delta(i);
        } else if (delta(i).end_version() > max_delta->end_version()) {
            max_delta = &delta(i);
        } else if (delta(i).end_version() == max_delta->end_version()
                   && delta(i).start_version() == delta(i).end_version()) {
            max_delta = &delta(i);
        }
    }
    return max_delta;
}

Version OLAPHeader::get_latest_version() const {
    auto delta = get_lastest_version();
    return {delta->start_version(), delta->end_version()};
}

const PDelta* OLAPHeader::get_delta(int index) const {
    if (delta_size() == 0) {
        return nullptr;
    }

    return &delta(index);
}

void OLAPHeader::_convert_file_version_to_delta(const FileVersionMessage& version,
                                                PDelta* delta) {
    delta->set_start_version(version.start_version());
    delta->set_end_version(version.end_version());
    delta->set_version_hash(version.version_hash());
    delta->set_creation_time(version.creation_time());

    PSegmentGroup* segment_group = delta->add_segment_group();
    segment_group->set_segment_group_id(-1);
    segment_group->set_num_segments(version.num_segments());
    segment_group->set_index_size(version.index_size());
    segment_group->set_data_size(version.data_size());
    segment_group->set_num_rows(version.num_rows());
    if (version.has_delta_pruning()) {
        for (int i = 0; i < version.delta_pruning().column_pruning_size(); ++i) {
            ColumnPruning* column_pruning = segment_group->add_column_pruning();
            *column_pruning = version.delta_pruning().column_pruning(i);
        }
    }
}

const uint32_t OLAPHeader::get_cumulative_compaction_score() const{
    uint32_t score = 0;
    bool base_version_exists = false;
    const int32_t point = cumulative_layer_point();
    for (int i = delta_size() - 1; i >= 0; --i) {
        if (delta(i).start_version() >= point) {
            score++;
        }
        if (delta(i).start_version() == 0) {
            base_version_exists = true;
        }
    }
    score = score < config::cumulative_compaction_num_singleton_deltas ? 0 : score;

    // base不存在可能是tablet正在做alter table，先不选它，设score=0
    return base_version_exists ? score : 0;
}

const uint32_t OLAPHeader::get_base_compaction_score() const{
    uint32_t score = 0;
    const int32_t point = cumulative_layer_point();
    bool base_version_exists = false;
    for (int i = delta_size() - 1; i >= 0; --i) {
        if (delta(i).end_version() < point) {
            score++;
        }
        if (delta(i).start_version() == 0) {
            base_version_exists = true;
        }
    }
    score = score < config::base_compaction_num_cumulative_deltas ? 0 : score;

    // base不存在可能是tablet正在做alter table，先不选它，设score=0
    return base_version_exists ? score : 0;
}

const OLAPStatus OLAPHeader::version_creation_time(const Version& version,
                                                   int64_t* creation_time) const {
    if (delta_size() == 0) {
        return OLAP_ERR_VERSION_NOT_EXIST;
    }

    for (int i = delta_size() - 1; i >= 0; --i) {
        const PDelta& temp = delta(i);
        if (temp.start_version() == version.first && temp.end_version() == version.second) {
            *creation_time = temp.creation_time();
            return OLAP_SUCCESS;
        }
    }

    return OLAP_ERR_VERSION_NOT_EXIST;
}

// Related static functions about version graph.

#define CHECK_GRAPH_PARAMS(param1, param2) \
    if (param1 == NULL || param2 == NULL) { \
        OLAP_LOG_WARNING("invalid graph parameters."); \
        return OLAP_ERR_INPUT_PARAMETER_ERROR; \
    }

// Construct version graph(using adjacency list) from header's information.
static OLAPStatus construct_version_graph(
        const RepeatedPtrField<PDelta>& versions_in_header,
        vector<Vertex>* version_graph,
        unordered_map<int, int>* vertex_helper_map) {
    if (versions_in_header.size() == 0) {
        VLOG(3) << "there is no version in the header.";
        return OLAP_SUCCESS;
    }

    CHECK_GRAPH_PARAMS(version_graph, vertex_helper_map);
    // Distill vertex values from versions in OLAPHeader.
    vector<int> vertex_values;
    vertex_values.reserve(2 * versions_in_header.size());

    for (int i = 0; i < versions_in_header.size(); ++i) {
        vertex_values.push_back(versions_in_header.Get(i).start_version());
        vertex_values.push_back(versions_in_header.Get(i).end_version() + 1);
        VLOG(3) << "added two vertex_values. "
                << "version=" << versions_in_header.Get(i).start_version()
                << "-" << versions_in_header.Get(i).end_version() + 1;
    }

    sort(vertex_values.begin(), vertex_values.end());

    // Clear vertex_helper_map and version graph.
    version_graph->clear();

    // Items in vertex_values are sorted, but not unique.
    // we choose unique items in vertex_values to create vertexes.
    int last_vertex_value = -1;
    for (size_t i = 0; i < vertex_values.size(); ++i) {
        if (i != 0 && vertex_values[i] == last_vertex_value) {
            continue;
        }

        // Add vertex to graph.
        if (add_vertex_to_graph(vertex_values[i],
                                version_graph,
                                vertex_helper_map) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to add vertex to version graph. [vertex_value=%d]",
                             vertex_values[i]);
            return OLAP_ERR_OTHER_ERROR;
        }

        last_vertex_value = vertex_values[i];
    }

    // Create edges for version graph according to OLAPHeader's versions.
    for (int i = 0; i < versions_in_header.size(); ++i) {
        // Versions in header are unique.
        // We ensure vertex_helper_map has its start_version.
        int start_vertex_index = (*vertex_helper_map)[versions_in_header.Get(i).start_version()];
        int end_vertex_index = (*vertex_helper_map)[versions_in_header.Get(i).end_version() + 1];
        // Add one edge from start_version to end_version.
        list<int>* edges = (*version_graph)[start_vertex_index].edges;
        edges->insert(edges->begin(), end_vertex_index);
        // Add reverse edge from end_version to start_version.
        list<int>* r_edges = (*version_graph)[end_vertex_index].edges;
        r_edges->insert(r_edges->begin(), start_vertex_index);
    }

    return OLAP_SUCCESS;
}

// Clear version graph and vertex_helper_map, release memory hold by version_graph.
static OLAPStatus clear_version_graph(vector<Vertex>* version_graph,
                                  unordered_map<int, int>* vertex_helper_map) {
    CHECK_GRAPH_PARAMS(version_graph, vertex_helper_map);

    // Release memory of version graph.
    vertex_helper_map->clear();
    for (vector<Vertex>::iterator it = version_graph->begin();
            it != version_graph->end(); ++it) {
        SAFE_DELETE(it->edges);
    }
    version_graph->clear();

    return OLAP_SUCCESS;
}

// Add version to graph, it is called near the end of add_version
static OLAPStatus add_version_to_graph(const Version& version,
                                   vector<Vertex>* version_graph,
                                   unordered_map<int, int>* vertex_helper_map) {
    CHECK_GRAPH_PARAMS(version_graph, vertex_helper_map);
    // Add version.first as new vertex of version graph if not exist.
    int start_vertex_value = version.first;
    int end_vertex_value = version.second + 1;

    // Add vertex to graph.
    if (add_vertex_to_graph(start_vertex_value, version_graph, vertex_helper_map) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to add vertex to version graph. [vertex=%d]", start_vertex_value);
        return OLAP_ERR_OTHER_ERROR;
    }

    if (add_vertex_to_graph(end_vertex_value, version_graph, vertex_helper_map) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to add vertex to version graph. [vertex=%d]", end_vertex_value);
        return OLAP_ERR_OTHER_ERROR;
    }

    int start_vertex_index = (*vertex_helper_map)[start_vertex_value];
    int end_vertex_index = (*vertex_helper_map)[end_vertex_value];

    // We assume this version is new version, so we just add two edges
    // into version graph. add one edge from start_version to end_version
    list<int>* edges = (*version_graph)[start_vertex_index].edges;
    edges->insert(edges->begin(), end_vertex_index);

    // We add reverse edge(from end_version to start_version) to graph in spite
    // that _support_reverse_version is false.
    list<int>* r_edges = (*version_graph)[end_vertex_index].edges;
    r_edges->insert(r_edges->begin(), start_vertex_index);

    return OLAP_SUCCESS;
}

// Delete version from graph, it is called near the end of delete_version
static OLAPStatus delete_version_from_graph(
        const RepeatedPtrField<PDelta>& versions_in_header,
        const Version& version,
        vector<Vertex>* version_graph,
        unordered_map<int, int>* vertex_helper_map) {
    CHECK_GRAPH_PARAMS(version_graph, vertex_helper_map);
    int start_vertex_value = version.first;
    int end_vertex_value = version.second + 1;

    if (vertex_helper_map->find(start_vertex_value) == vertex_helper_map->end()) {
        OLAP_LOG_WARNING("vertex for version.first does not exists. [version='%d-%d']",
                         version.first,
                         version.second);
        return OLAP_ERR_VERSION_NOT_EXIST;
    }

    if (vertex_helper_map->find(end_vertex_value) == vertex_helper_map->end()) {
        OLAP_LOG_WARNING("vertex for version.second+1 does not exists. [version='%d-%d']",
                         version.first,
                         version.second);
        return OLAP_ERR_VERSION_NOT_EXIST;
    }

    int start_vertex_index = (*vertex_helper_map)[start_vertex_value];
    int end_vertex_index = (*vertex_helper_map)[end_vertex_value];
    // Remove edge and its reverse edge.
    (*version_graph)[start_vertex_index].edges->remove(end_vertex_index);
    (*version_graph)[end_vertex_index].edges->remove(start_vertex_index);

    // We should reconstruct version graph if the ratio of isolated vertexes
    // reaches RATIO_OF_ISOLATED_VERTEX = 30%. The last version may be treated
    // as isolated vertex(if no reverse edge), but it doesn't matter.
    int num_isolated_vertex = 0;
    for (vector<Vertex>::const_iterator it = version_graph->begin();
            it != version_graph->end(); ++it) {
        if (it->edges->size() == 0) {
            ++num_isolated_vertex;
        }
    }

    // If the number of isolated vertex reaches this ratio, reconstruct the
    // version graph.
    // ratio of isolated vertex in version graph
    const static double RATIO_OF_ISOLATED_VERTEX = 0.3;

    if (num_isolated_vertex > 1 + static_cast<int>(RATIO_OF_ISOLATED_VERTEX
                                                   * version_graph->size())) {
        VLOG(3) << "the number of isolated vertexes reaches specified ratio,"
                << "reconstruct version graph. num_isolated_vertex=" << num_isolated_vertex
                << ", num_vertex=" << version_graph->size();

        // Release memory of version graph.
        for (vector<Vertex>::iterator it = version_graph->begin();
                it != version_graph->end(); ++it) {
            delete it->edges;
            it->edges = NULL;
        }

        // We do not use swap pointer technique to avoid version_graph
        // construction failue.
        clear_version_graph(version_graph, vertex_helper_map);

        // Reconstruct version graph.
        if (construct_version_graph(versions_in_header,
                                    version_graph,
                                    vertex_helper_map) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("reconstruct version graph fail.");
            return OLAP_ERR_OTHER_ERROR;
        }
    }

    return OLAP_SUCCESS;
}

// Add vertex to graph, if vertex already exists, still return SUCCESS.
static OLAPStatus add_vertex_to_graph(int vertex_value,
                                  vector<Vertex>* version_graph,
                                  unordered_map<int, int>* vertex_helper_map) {
    CHECK_GRAPH_PARAMS(version_graph, vertex_helper_map);

    // Vertex with vertex_value already exists.
    if (vertex_helper_map->find(vertex_value) != vertex_helper_map->end()) {
        VLOG(3) << "vertex with vertex value already exists. value=" << vertex_value;
        return OLAP_SUCCESS;
    }

    list<int>* edges = new(std::nothrow) list<int>();
    if (edges == NULL) {
        OLAP_LOG_WARNING("fail to malloc edge list.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    Vertex vertex = {vertex_value, edges};
    version_graph->push_back(vertex);
    (*vertex_helper_map)[vertex_value] = version_graph->size() - 1;
    return OLAP_SUCCESS;
}

const PDelta* OLAPHeader::get_base_version() const {
    if (delta_size() == 0) {
        return nullptr;
    }

    for (int i = 0; i < delta_size(); ++i) {
        if (delta(i).start_version() == 0) {
            return &delta(i);
        }
    }

    return nullptr;
}

}  // namespace doris
