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

namespace palo {
// related static functions of version graph

// Construct version graph(using adjacency list) from header's information.
static OLAPStatus construct_version_graph(
        const RepeatedPtrField<FileVersionMessage>& versions_in_header,
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
        const RepeatedPtrField<FileVersionMessage>& versions_in_header,
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

OLAPStatus OLAPHeader::load() {
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

    clear_version_graph(&_version_graph, &_vertex_helper_map);

    if (construct_version_graph(file_version(),
                                &_version_graph,
                                &_vertex_helper_map) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to construct version graph.");
        return OLAP_ERR_OTHER_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPHeader::save() {
    return save(_file_name);
}

OLAPStatus OLAPHeader::save(const string& file_path) {
    FileHeader<OLAPHeaderMessage> file_header;
    FileHandler file_handler;

    if (file_handler.open_with_mode(file_path.c_str(),
            O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to open header file. [file='%s']", file_path.c_str());
        return OLAP_ERR_IO_ERROR;
    }

    try {
        file_header.mutable_message()->CopyFrom(*this);
    } catch (...) {
        OLAP_LOG_WARNING("fail to copy protocol buffer object. [path='%s']", file_path.c_str());
        return OLAP_ERR_OTHER_ERROR;
    }

    if (file_header.prepare(&file_handler) != OLAP_SUCCESS
            || file_header.serialize(&file_handler) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to serialize to file header. [path='%s']", file_path.c_str());
        return OLAP_ERR_SERIALIZE_PROTOBUF_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPHeader::add_version(
        Version version,
        VersionHash version_hash,
        uint32_t num_segments,
        time_t max_timestamp,
        int64_t index_size,
        int64_t data_size,
        int64_t num_rows,
        const std::vector<std::pair<WrapperField*, WrapperField*>>* column_statistics) {
    // Check whether version is valid.
    if (version.first > version.second) {
        OLAP_LOG_WARNING("the version is not valid. [version='%d,%d']",
                         version.first,
                         version.second);
        return OLAP_ERR_HEADER_ADD_VERSION;
    }

    // Check whether the version is existed.
    for (int i = 0; i < file_version_size(); ++i) {
        if (file_version(i).start_version() == version.first
                && file_version(i).end_version() == version.second) {
            OLAP_LOG_WARNING("the version is existed. [version='%d,%d']",
                             version.first,
                             version.second);
            return OLAP_ERR_HEADER_ADD_VERSION;
        }
    }

    // Try to add version to protobuf.
    try {
        FileVersionMessage* new_version = add_file_version();
        new_version->set_num_segments(num_segments);
        new_version->set_start_version(version.first);
        new_version->set_end_version(version.second);
        new_version->set_version_hash(version_hash);
        new_version->set_max_timestamp(max_timestamp);
        new_version->set_index_size(index_size);
        new_version->set_data_size(data_size);
        new_version->set_num_rows(num_rows);
        new_version->set_creation_time(time(NULL));
        if (NULL != column_statistics) {
            for (size_t i = 0; i < column_statistics->size(); ++i) {
                ColumnPruning *column_pruning = 
                        new_version->mutable_delta_pruning()->add_column_pruning();
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

OLAPStatus OLAPHeader::delete_version(Version version) {
    // Find the version that need to be deleted.
    int index = -1;
    for (int i = 0; i < file_version_size(); ++i) {
        if (file_version(i).start_version() == version.first
                && file_version(i).end_version() == version.second) {
            index = i;
            break;
        }
    }

    // Delete version from protobuf.
    if (index != -1) {
        RepeatedPtrField<FileVersionMessage >* version_ptr = mutable_file_version();
        for (int i = index; i < file_version_size() - 1; ++i) {
            version_ptr->SwapElements(i, i + 1);
        }

        version_ptr->RemoveLast();
    }

    // Atomic delete is not supported now.
    if (delete_version_from_graph(file_version(),
                                  version,
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
    clear_version_graph(&_version_graph, &_vertex_helper_map);

    if (construct_version_graph(file_version(),
                                &_version_graph,
                                &_vertex_helper_map) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to construct version graph.");
        return OLAP_ERR_OTHER_ERROR;
    }

    return OLAP_SUCCESS;
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

    OLAP_LOG_TRACE("calculated shortest path. [version='%d-%d' path='%s']",
                   target_version.first,
                   target_version.second,
                   shortest_path_for_debug.str().c_str());

    return OLAP_SUCCESS;
}

const FileVersionMessage* OLAPHeader::get_lastest_delta_version() const {
    if (file_version_size() == 0) {
        return NULL;
    }

    const FileVersionMessage* max_version = NULL;
    for (int i = file_version_size() - 1; i >= 0; --i) {
        if (file_version(i).start_version() == file_version(i).end_version()) {
            if (max_version == NULL) {
                max_version = &file_version(i);
            } else if (file_version(i).start_version() > max_version->start_version()) {
                max_version = &file_version(i);
            }
        }
    }

    return max_version;
}

const FileVersionMessage* OLAPHeader::get_latest_version() const {
    if (file_version_size() == 0) {
        return NULL;
    }

    const FileVersionMessage* max_version = NULL;
    for (int i = file_version_size() - 1; i >= 0; --i) {
        if (max_version == NULL) {
            max_version = &file_version(i);
        } else if (file_version(i).end_version() > max_version->end_version()) {
            max_version = &file_version(i);
        } else if (file_version(i).end_version() == max_version->end_version()
                       && file_version(i).start_version() == file_version(i).end_version()) {
            max_version = &file_version(i);
        }
    }

    return max_version;
}

const uint32_t OLAPHeader::get_compaction_nice_estimate() const{
    uint32_t nice = 0;
    bool base_version_exists = false;
    const int32_t point = cumulative_layer_point();
    for (int i = file_version_size() - 1; i >= 0; --i) {
        if (file_version(i).start_version() >= point) {
            nice++;
        }
        if (file_version(i).start_version() == 0) {
            base_version_exists = true;
        }
    }
    nice = nice < config::cumulative_compaction_num_singleton_deltas ? 0 : nice;

    // base不存在可能是tablet正在做alter table，先不选它，设nice=0
    return base_version_exists ? nice : 0;
}

const OLAPStatus OLAPHeader::version_creation_time(const Version& version,
                                                   int64_t* creation_time) const {
    if (0 == file_version_size()) {
        return OLAP_ERR_VERSION_NOT_EXIST;
    }

    for (int i = file_version_size() - 1; i >= 0; --i) {
        const FileVersionMessage& temp = file_version(i);
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
        const RepeatedPtrField<FileVersionMessage>& versions_in_header,
        vector<Vertex>* version_graph,
        unordered_map<int, int>* vertex_helper_map) {
    if (versions_in_header.size() == 0) {
        OLAP_LOG_DEBUG("there is no version in the header.");
        return OLAP_SUCCESS;
    }

    CHECK_GRAPH_PARAMS(version_graph, vertex_helper_map);
    // Distill vertex values from versions in OLAPHeader.
    vector<int> vertex_values;
    vertex_values.reserve(2 * versions_in_header.size());

    for (int i = 0; i < versions_in_header.size(); ++i) {
        vertex_values.push_back(versions_in_header.Get(i).start_version());
        vertex_values.push_back(versions_in_header.Get(i).end_version() + 1);
        OLAP_LOG_DEBUG("added two vertex_values. [version='%d-%d']",
                       versions_in_header.Get(i).start_version(),
                       versions_in_header.Get(i).end_version() + 1);
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
        const RepeatedPtrField<FileVersionMessage>& versions_in_header,
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
        OLAP_LOG_DEBUG("the number of isolated vertexes reaches specified ratio,"
                       "reconstruct version graph. [num_isolated_vertex=%d num_vertex=%lu]",
                       num_isolated_vertex,
                       version_graph->size());

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
        OLAP_LOG_DEBUG("vertex with vertex value already exists. [value=%d]", vertex_value);
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

}  // namespace palo
