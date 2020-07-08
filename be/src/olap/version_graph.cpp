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

#include "olap/version_graph.h"

#include <memory>
#include <queue>

#include "common/logging.h"

namespace doris {

void TimestampedVersionTracker::_construct_versioned_tracker(
        const std::vector<RowsetMetaSharedPtr>& rs_metas,
        const std::vector<RowsetMetaSharedPtr>& expired_snapshot_rs_metas) {
    int64_t max_version = 0;

    std::vector<RowsetMetaSharedPtr> all_rs_metas(rs_metas);
    all_rs_metas.insert(all_rs_metas.end(), expired_snapshot_rs_metas.begin(),
                        expired_snapshot_rs_metas.end());
    // construct the roset graph
    _version_graph.reconstruct_version_graph(all_rs_metas, &max_version);

    // version -> rowsetmeta
    // fill main_path which are included not merged rowsets
    std::unordered_map<Version, RowsetMetaSharedPtr, HashOfVersion> main_path;
    for (auto& rs_meta : rs_metas) {
        main_path[rs_meta->version()] = rs_meta;
    }
    // fill other_path which are included merged rowsets
    typedef std::shared_ptr<std::map<int64_t, RowsetMetaSharedPtr>> Edges;
    std::map<int64_t, Edges> other_path;

    for (auto& rs_meta : expired_snapshot_rs_metas) {
        if (other_path.find(rs_meta->start_version()) == other_path.end()) {
            other_path[rs_meta->start_version()] =
                    Edges(new std::map<int64_t, RowsetMetaSharedPtr>());
        }
        other_path[rs_meta->start_version()]->insert(
                std::pair<int64_t, RowsetMetaSharedPtr>(rs_meta->end_version(), rs_meta));
    }

    auto iter = other_path.begin();
    for (; iter != other_path.end(); iter++) {
        Edges edges = iter->second;
        auto min_begin_version = iter->first;

        while (!edges->empty()) {
            PathVersionListSharedPtr path_version_ptr(new TimestampedVersionPathContainer());
            // 1. find a path, begin from min_begin_version
            auto min_end_version = edges->begin()->first;
            auto min_rs_meta = edges->begin()->second;
            // tracker the first
            TimestampedVersionSharedPtr tracker_ptr(new TimestampedVersion(
                    Version(min_begin_version, min_end_version), min_rs_meta->creation_time()));
            path_version_ptr->add_timestamped_version(tracker_ptr);
            // 1.1 do loop, find next start to make a path
            auto tmp_start_version = min_end_version + 1;
            do {
                int64_t tmp_end_version = -1;
                int64_t create_time = -1;
                // 1.2 the next must in other_path, find from other_path
                auto tmp_edge_iter = other_path.find(tmp_start_version);
                if (tmp_edge_iter == other_path.end() || tmp_edge_iter->second->empty()) {
                    break;
                }
                // 1.3 record this version to make a tracker, put into path_version
                auto next_rs_meta_iter = tmp_edge_iter->second->begin();
                tmp_end_version = next_rs_meta_iter->first;
                create_time = next_rs_meta_iter->second->creation_time();
                TimestampedVersionSharedPtr tracker_ptr(new TimestampedVersion(
                        Version(tmp_start_version, tmp_end_version), create_time));
                path_version_ptr->add_timestamped_version(tracker_ptr);
                // 1.4 judge if this path finish
                auto max_end_version = tmp_end_version;
                // find from other_path
                if (edges->find(max_end_version) != edges->end()) {
                    break;
                }
                // find from main_path
                if (main_path.find(Version(min_begin_version, max_end_version)) !=
                    main_path.end()) {
                    break;
                }
                // 1.5 do next
                tmp_start_version = tmp_end_version + 1;

            } while (true);
            // 1.6 add path_version to map
            _expired_snapshot_rs_path_map[_next_path_version++] = path_version_ptr;

            // 2 remove this path from other_path
            std::vector<TimestampedVersionSharedPtr>& timestamped_versions = path_version_ptr->timestamped_versions();
            auto path_iter = timestamped_versions.begin();
            for (; path_iter != timestamped_versions.end(); path_iter++) {
                const Version& version = (*path_iter)->version();
                other_path[version.first]->erase(version.second);
            }
        }
    }
    other_path.clear();
    main_path.clear();
    LOG(INFO) << _get_current_path_map_str();

}

void TimestampedVersionTracker::construct_versioned_tracker(
        const std::vector<RowsetMetaSharedPtr>& rs_metas,
        const std::vector<RowsetMetaSharedPtr>& expired_snapshot_rs_metas) {
    if (rs_metas.empty()) {
        VLOG(3) << "there is no version in the header.";
        return;
    }

    _construct_versioned_tracker(rs_metas, expired_snapshot_rs_metas);
}

void TimestampedVersionTracker::reconstruct_versioned_tracker(
        const std::vector<RowsetMetaSharedPtr>& rs_metas,
        const std::vector<RowsetMetaSharedPtr>& expired_snapshot_rs_metas) {
    if (rs_metas.empty()) {
        VLOG(3) << "there is no version in the header.";
        return;
    }
    _expired_snapshot_rs_path_map.clear();
    _next_path_version = 1;

    _construct_versioned_tracker(rs_metas, expired_snapshot_rs_metas);
}

void TimestampedVersionTracker::add_version(const Version& version) {
    _version_graph.add_version_to_graph(version);
}

void TimestampedVersionTracker::add_expired_path_version(
        const std::vector<RowsetMetaSharedPtr>& expired_snapshot_rs_metas) {
    if (expired_snapshot_rs_metas.empty()) {
        VLOG(3) << "there is no version in the expired_snapshot_rs_metas.";
        return;
    }

    PathVersionListSharedPtr ptr(new TimestampedVersionPathContainer());
    for (auto rs : expired_snapshot_rs_metas) {
        TimestampedVersionSharedPtr vt_ptr(new TimestampedVersion(rs->version(), rs->creation_time()));
        ptr->add_timestamped_version(vt_ptr);
    }

    std::vector<TimestampedVersionSharedPtr>& timestamped_versions = ptr->timestamped_versions();
    sort(timestamped_versions.begin(), timestamped_versions.end());
    _expired_snapshot_rs_path_map[_next_path_version] = ptr;
    _next_path_version++;
}

// Capture consistent versions from graph
OLAPStatus TimestampedVersionTracker::capture_consistent_versions(
        const Version& spec_version, std::vector<Version>* version_path) const {
    return _version_graph.capture_consistent_versions(spec_version, version_path);
}

void TimestampedVersionTracker::capture_expired_path_version(
        int64_t expired_snapshot_sweep_endtime, std::vector<int64_t>* path_version_vec) const {
    std::unordered_map<int64_t, PathVersionListSharedPtr>::const_iterator iter =
            _expired_snapshot_rs_path_map.begin();

    while (iter != _expired_snapshot_rs_path_map.end()) {
        int64_t max_create_time = iter->second->max_create_time();
        if (max_create_time <= expired_snapshot_sweep_endtime) {
            int64_t path_version = iter->first;
            path_version_vec->push_back(path_version);
        }
        iter++;
    }
}

void TimestampedVersionTracker::fetch_path_version(int64_t path_version,
                                                std::vector<Version>& version_path) {
    if (_expired_snapshot_rs_path_map.count(path_version) == 0) {
        VLOG(3) << "path version " << path_version << " does not exist!";
        return;
    }

    PathVersionListSharedPtr ptr = _expired_snapshot_rs_path_map[path_version];

    std::vector<TimestampedVersionSharedPtr>& timestamped_versions = ptr->timestamped_versions();
    std::vector<TimestampedVersionSharedPtr>::iterator iter = timestamped_versions.begin();
    while (iter != timestamped_versions.end()) {
        version_path.push_back((*iter)->version());
        iter++;
    }
}

void TimestampedVersionTracker::fetch_and_delete_path_version(int64_t path_version,
                                                           std::vector<Version>& version_path) {
    if (_expired_snapshot_rs_path_map.count(path_version) == 0) {
        VLOG(3) << "path version " << path_version << " does not exist!";
        return;
    }

    LOG(INFO) << _get_current_path_map_str();
    fetch_path_version(path_version, version_path);

    _expired_snapshot_rs_path_map.erase(path_version);

    for (auto& version : version_path) {
        _version_graph.delete_version_from_graph(version);
    }
}

std::string TimestampedVersionTracker::_get_current_path_map_str() {

    std::stringstream tracker_info;
    tracker_info << "current expired next_path_version " << _next_path_version << std::endl;

    std::unordered_map<int64_t, PathVersionListSharedPtr>::const_iterator iter =
            _expired_snapshot_rs_path_map.begin();
    while (iter != _expired_snapshot_rs_path_map.end()) {
        
        tracker_info << "current expired path_version " << iter->first;
        std::vector<TimestampedVersionSharedPtr>& timestamped_versions = iter->second->timestamped_versions();
        std::vector<TimestampedVersionSharedPtr>::iterator version_path_iter = timestamped_versions.begin();
        int64_t max_create_time = -1;
        while (version_path_iter != timestamped_versions.end()) {
            if (max_create_time < (*version_path_iter)->get_create_time()) {
                max_create_time = (*version_path_iter)->get_create_time();
            }
            tracker_info << " -> [";
            tracker_info << (*version_path_iter)->version().first;
            tracker_info << ",";
            tracker_info << (*version_path_iter)->version().second;
            tracker_info << "]";

            version_path_iter++;
        }

        tracker_info << std::endl;
        iter++;
    }
    return tracker_info.str();
}

void TimestampedVersionPathContainer::add_timestamped_version(TimestampedVersionSharedPtr version) {
    // compare and refresh _max_create_time
    if (version->get_create_time() > _max_create_time) {
        _max_create_time = version->get_create_time();
    }
    _timestamped_versions_container.push_back(version);
}

inline std::vector<TimestampedVersionSharedPtr>& TimestampedVersionPathContainer::timestamped_versions() {
    return _timestamped_versions_container;
}

void VersionGraph::construct_version_graph(const std::vector<RowsetMetaSharedPtr>& rs_metas,
                                         int64_t* max_version) {
    if (rs_metas.empty()) {
        VLOG(3) << "there is no version in the header.";
        return;
    }

    // Distill vertex values from versions in TabletMeta.
    std::vector<int64_t> vertex_values;
    vertex_values.reserve(2 * rs_metas.size());

    for (size_t i = 0; i < rs_metas.size(); ++i) {
        vertex_values.push_back(rs_metas[i]->start_version());
        vertex_values.push_back(rs_metas[i]->end_version() + 1);
        if ( max_version != nullptr and *max_version < rs_metas[i]->end_version()) {
            *max_version = rs_metas[i]->end_version();
        }
    }
    std::sort(vertex_values.begin(), vertex_values.end());

    // Items in vertex_values are sorted, but not unique.
    // we choose unique items in vertex_values to create vertexes.
    int64_t last_vertex_value = -1;
    for (size_t i = 0; i < vertex_values.size(); ++i) {
        if (i != 0 && vertex_values[i] == last_vertex_value) {
            continue;
        }

        // Add vertex to graph.
        _add_vertex_to_graph(vertex_values[i]);
        last_vertex_value = vertex_values[i];
    }
    // Create edges for version graph according to TabletMeta's versions.
    for (size_t i = 0; i < rs_metas.size(); ++i) {
        // Versions in header are unique.
        // We ensure _vertex_index_map has its start_version.
        int64_t start_vertex_index = _vertex_index_map[rs_metas[i]->start_version()];
        int64_t end_vertex_index = _vertex_index_map[rs_metas[i]->end_version() + 1];
        // Add one edge from start_version to end_version.
        _version_graph[start_vertex_index].edges.push_front(end_vertex_index);
        // Add reverse edge from end_version to start_version.
        _version_graph[end_vertex_index].edges.push_front(start_vertex_index);

    }

}

void VersionGraph::reconstruct_version_graph(const std::vector<RowsetMetaSharedPtr>& rs_metas,
                                           int64_t* max_version) {
    _version_graph.clear();
    _vertex_index_map.clear();

    construct_version_graph(rs_metas, max_version);
}

void VersionGraph::add_version_to_graph(const Version& version) {
    // Add version.first as new vertex of version graph if not exist.
    int64_t start_vertex_value = version.first;
    int64_t end_vertex_value = version.second + 1;

    // Add vertex to graph.
    _add_vertex_to_graph(start_vertex_value);
    _add_vertex_to_graph(end_vertex_value);

    int64_t start_vertex_index = _vertex_index_map[start_vertex_value];
    int64_t end_vertex_index = _vertex_index_map[end_vertex_value];

    // We assume this version is new version, so we just add two edges
    // into version graph. add one edge from start_version to end_version
    _version_graph[start_vertex_index].edges.push_front(end_vertex_index);

    // We add reverse edge(from end_version to start_version) to graph
    _version_graph[end_vertex_index].edges.push_front(start_vertex_index);
}

OLAPStatus VersionGraph::delete_version_from_graph(const Version& version) {
    int64_t start_vertex_value = version.first;
    int64_t end_vertex_value = version.second + 1;

    if (_vertex_index_map.find(start_vertex_value) == _vertex_index_map.end() ||
        _vertex_index_map.find(end_vertex_value) == _vertex_index_map.end()) {
        LOG(WARNING) << "vertex for version does not exists. "
                     << "version=" << version.first << "-" << version.second;
        return OLAP_ERR_HEADER_DELETE_VERSION;
    }

    int64_t start_vertex_index = _vertex_index_map[start_vertex_value];
    int64_t end_vertex_index = _vertex_index_map[end_vertex_value];
    // Remove edge and its reverse edge.
    _version_graph[start_vertex_index].edges.remove(end_vertex_index);
    _version_graph[end_vertex_index].edges.remove(start_vertex_index);

    return OLAP_SUCCESS;
}

void VersionGraph::_add_vertex_to_graph(int64_t vertex_value) {
    // Vertex with vertex_value already exists.
    if (_vertex_index_map.find(vertex_value) != _vertex_index_map.end()) {
        VLOG(3) << "vertex with vertex value already exists. value=" << vertex_value;
        return;
    }

    _version_graph.emplace_back(Vertex(vertex_value));
    _vertex_index_map[vertex_value] = _version_graph.size() - 1;
}

OLAPStatus VersionGraph::capture_consistent_versions(const Version& spec_version,
                                                    std::vector<Version>* version_path) const {
    if (spec_version.first > spec_version.second) {
        LOG(WARNING) << "invalid specfied version. "
                     << "spec_version=" << spec_version.first << "-" << spec_version.second;
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
        for (const auto& it : _version_graph[top_vertex_index].edges) {
            if (visited[it] == false) {
                // If we don't support reverse version in the path, and start vertex
                // value is larger than the end vertex value, we skip this edge.
                if (_version_graph[top_vertex_index].value > _version_graph[it].value) {
                    continue;
                }

                visited[it] = true;
                predecessor[it] = top_vertex_index;
                bfs_queue.push(it);
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

    if (version_path != nullptr) {
        // Make version_path from reversed_path.
        std::stringstream shortest_path_for_debug;
        for (size_t path_id = reversed_path.size() - 1; path_id > 0; --path_id) {
            int64_t tmp_start_vertex_value = _version_graph[reversed_path[path_id]].value;
            int64_t tmp_end_vertex_value = _version_graph[reversed_path[path_id - 1]].value;

            // tmp_start_vertex_value mustn't be equal to tmp_end_vertex_value
            if (tmp_start_vertex_value <= tmp_end_vertex_value) {
                version_path->emplace_back(tmp_start_vertex_value, tmp_end_vertex_value - 1);
            } else {
                version_path->emplace_back(tmp_end_vertex_value, tmp_start_vertex_value - 1);
            }

            shortest_path_for_debug << (*version_path)[version_path->size() - 1] << ' ';
        }
        VLOG(10) << "success to find path for spec_version. spec_version=" << spec_version
                 << ", path=" << shortest_path_for_debug.str();
    }

    return OLAP_SUCCESS;
}

} // namespace doris
