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

#include "olap/rowset_graph.h"
#include "common/logging.h"
#include <queue>

namespace doris {

OLAPStatus RowsetGraph::construct_rowset_graph(const std::vector<RowsetMetaSharedPtr>& rs_metas) {
    if (rs_metas.empty()) {
        VLOG(3) << "there is no version in the header.";
        return OLAP_SUCCESS;
    }

    // Distill vertex values from versions in TabletMeta.
    std::vector<int64_t> vertex_values;
    vertex_values.reserve(2 * rs_metas.size());

    for (size_t i = 0; i < rs_metas.size(); ++i) {
        vertex_values.push_back(rs_metas[i]->start_version());
        vertex_values.push_back(rs_metas[i]->end_version() + 1);
    }

    sort(vertex_values.begin(), vertex_values.end());

    // Items in vertex_values are sorted, but not unique.
    // we choose unique items in vertex_values to create vertexes.
    int64_t last_vertex_value = -1;
    for (size_t i = 0; i < vertex_values.size(); ++i) {
        if (i != 0 && vertex_values[i] == last_vertex_value) {
            continue;
        }

        // Add vertex to graph.
        OLAPStatus status = _add_vertex_to_graph(vertex_values[i]);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to add vertex to version graph. vertex=" << vertex_values[i];
            return status;
        }

        last_vertex_value = vertex_values[i];
    }

    // Create edges for version graph according to TabletMeta's versions.
    for (size_t i = 0; i < rs_metas.size(); ++i) {
        // Versions in header are unique.
        // We ensure _vertex_index_map has its start_version.
        int64_t start_vertex_index = _vertex_index_map[rs_metas[i]->start_version()];
        int64_t end_vertex_index = _vertex_index_map[rs_metas[i]->end_version() + 1];
        // Add one edge from start_version to end_version.
        std::list<int64_t>* edges = _version_graph[start_vertex_index].edges;
        edges->insert(edges->begin(), end_vertex_index);
        // Add reverse edge from end_version to start_version.
        std::list<int64_t>* r_edges = _version_graph[end_vertex_index].edges;
        r_edges->insert(r_edges->begin(), start_vertex_index);
    }
    return OLAP_SUCCESS;
}

OLAPStatus RowsetGraph::reconstruct_rowset_graph(const std::vector<RowsetMetaSharedPtr>& rs_metas) {
    for (auto& vertex : _version_graph) {
        SAFE_DELETE(vertex.edges);
    }
    _version_graph.clear();
    _vertex_index_map.clear();
    return construct_rowset_graph(rs_metas);
}

OLAPStatus RowsetGraph::add_version_to_graph(const Version& version) {
    // Add version.first as new vertex of version graph if not exist.
    int64_t start_vertex_value = version.first;
    int64_t end_vertex_value = version.second + 1;

    // Add vertex to graph.
    OLAPStatus status = _add_vertex_to_graph(start_vertex_value);
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to add vertex to version graph. vertex=" << start_vertex_value;
        return status;
    }

    status = _add_vertex_to_graph(end_vertex_value);
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to add vertex to version graph. vertex=" << end_vertex_value;
        return status;
    }

    int64_t start_vertex_index = _vertex_index_map[start_vertex_value];
    int64_t end_vertex_index = _vertex_index_map[end_vertex_value];

    // We assume this version is new version, so we just add two edges
    // into version graph. add one edge from start_version to end_version
    std::list<int64_t>* edges = _version_graph[start_vertex_index].edges;
    edges->insert(edges->begin(), end_vertex_index);

    // We add reverse edge(from end_version to start_version) to graph
    std::list<int64_t>* r_edges = _version_graph[end_vertex_index].edges;
    r_edges->insert(r_edges->begin(), start_vertex_index);

    return OLAP_SUCCESS;
}

OLAPStatus RowsetGraph::delete_version_from_graph(const Version& version) {
    int64_t start_vertex_value = version.first;
    int64_t end_vertex_value = version.second + 1;

    if (_vertex_index_map.find(start_vertex_value) == _vertex_index_map.end()
          || _vertex_index_map.find(end_vertex_value) == _vertex_index_map.end()) {
        LOG(WARNING) << "vertex for version does not exists. "
                     << "version=" << version.first << "-" << version.second;
        return OLAP_ERR_HEADER_DELETE_VERSION;
    }

    int64_t start_vertex_index = _vertex_index_map[start_vertex_value];
    int64_t end_vertex_index = _vertex_index_map[end_vertex_value];
    // Remove edge and its reverse edge.
    _version_graph[start_vertex_index].edges->remove(end_vertex_index);
    _version_graph[end_vertex_index].edges->remove(start_vertex_index);

    return OLAP_SUCCESS;
}

OLAPStatus RowsetGraph::_add_vertex_to_graph(int64_t vertex_value) {
    // Vertex with vertex_value already exists.
    if (_vertex_index_map.find(vertex_value) != _vertex_index_map.end()) {
        VLOG(3) << "vertex with vertex value already exists. value=" << vertex_value;
        return OLAP_SUCCESS;
    }

    std::list<int64_t>* edges = new std::list<int64_t>();
    if (edges == nullptr) {
        LOG(WARNING) << "fail to malloc edge list.";
        return OLAP_ERR_OTHER_ERROR;
    }

    Vertex vertex = {vertex_value, edges};
    _version_graph.push_back(vertex);
    _vertex_index_map[vertex_value] = _version_graph.size() - 1;
    return OLAP_SUCCESS;
}

OLAPStatus RowsetGraph::capture_consistent_versions(
                            const Version& spec_version,
                            std::vector<Version>* version_path) const {
    if (spec_version.first > spec_version.second) {
        LOG(WARNING) << "invalid specfied version. "
                     << "spec_version=" << spec_version.first << "-" << spec_version.second;
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (version_path == nullptr) {
        LOG(WARNING) << "param version_path is nullptr.";
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // bfs_queue's element is vertex_index.
    std::queue<int64_t> bfs_queue;
    // predecessor[i] means the predecessor of vertex_index 'i'.
    std::vector<int64_t> predecessor(_version_graph.size());
    // visited[int64_t]==true means it had entered bfs_queue.
    std::vector<bool> visited(_version_graph.size());
    // [start_vertex_value, end_vertex_value)
    int64_t start_vertex_value = spec_version.first;
    int64_t end_vertex_value = spec_version.second + 1;
    // -1 is invalid vertex index.
    int64_t start_vertex_index = -1;
    // -1 is valid vertex index.
    int64_t end_vertex_index = -1;

    for (size_t i = 0; i < _version_graph.size(); ++i) {
        if (_version_graph[i].value == start_vertex_value) {
            start_vertex_index = i;
        }
        if (_version_graph[i].value == end_vertex_value) {
            end_vertex_index = i;
        }
    }

    if (start_vertex_index < 0 || end_vertex_index < 0) {
        LOG(WARNING) << "fail to find path in version_graph. "
                     << "spec_version: " << spec_version.first << "-" << spec_version.second;
        return OLAP_ERR_VERSION_NOT_EXIST;
    }

    for (size_t i = 0; i < _version_graph.size(); ++i) {
        visited[i] = false;
    }

    bfs_queue.push(start_vertex_index);
    visited[start_vertex_index] = true;
    // The predecessor of root is itself.
    predecessor[start_vertex_index] = start_vertex_index;

    while (bfs_queue.empty() == false && visited[end_vertex_index] == false) {
        int64_t top_vertex_index = bfs_queue.front();
        bfs_queue.pop();
        auto it = _version_graph[top_vertex_index].edges->begin();
        for (; it != _version_graph[top_vertex_index].edges->end(); ++it) {
            if (visited[*it] == false) {
                // If we don't support reverse version in the path, and start vertex
                // value is larger than the end vertex value, we skip this edge.
                if (_version_graph[top_vertex_index].value > _version_graph[*it].value) {
                    continue;
                }

                visited[*it] = true;
                predecessor[*it] = top_vertex_index;
                bfs_queue.push(*it);
            }
        }
    }

    if (!visited[end_vertex_index]) {
        LOG(WARNING) << "fail to find path in version_graph. "
                     << "spec_version: " << spec_version.first << "-" << spec_version.second;
        return OLAP_ERR_VERSION_NOT_EXIST; 
    }

    std::vector<int64_t> reversed_path;
    int64_t tmp_vertex_index = end_vertex_index;
    reversed_path.push_back(tmp_vertex_index);

    // For start_vertex_index, its predecessor must be itself.
    while (predecessor[tmp_vertex_index] != tmp_vertex_index) {
        tmp_vertex_index = predecessor[tmp_vertex_index];
        reversed_path.push_back(tmp_vertex_index);
    }

    // Make version_path from reversed_path.
    std::stringstream shortest_path_for_debug;
    for (size_t path_id = reversed_path.size() - 1; path_id > 0; --path_id) {
        int64_t tmp_start_vertex_value = _version_graph[reversed_path[path_id]].value;
        int64_t tmp_end_vertex_value = _version_graph[reversed_path[path_id - 1]].value;

        // tmp_start_vertex_value mustn't be equal to tmp_end_vertex_value
        if (tmp_start_vertex_value <= tmp_end_vertex_value) {
            version_path->push_back(std::make_pair(tmp_start_vertex_value, tmp_end_vertex_value - 1));
        } else {
            version_path->push_back(std::make_pair(tmp_end_vertex_value, tmp_start_vertex_value - 1));
        }

        shortest_path_for_debug << (*version_path)[version_path->size() - 1].first << '-'
            << (*version_path)[version_path->size() - 1].second << ' ';
    }

    VLOG(3) << "success to find path for spec_version. "
            << "spec_version=" << spec_version.first << "-" << spec_version.second
            << ", path=" << shortest_path_for_debug.str();

    return OLAP_SUCCESS;
}

}  // namespace doris
