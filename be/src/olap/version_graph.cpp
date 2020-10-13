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
#include <cctz/time_zone.h>

#include "common/logging.h"
#include "util/time.h"

namespace doris {

void TimestampedVersionTracker::_construct_versioned_tracker(const std::vector<RowsetMetaSharedPtr>& rs_metas) {
    int64_t max_version = 0;

    // construct the rowset graph
    _version_graph.reconstruct_version_graph(rs_metas, &max_version);
}

void TimestampedVersionTracker::construct_versioned_tracker(const std::vector<RowsetMetaSharedPtr>& rs_metas) {
    if (rs_metas.empty()) {
        VLOG(3) << "there is no version in the header.";
        return;
    }
    _stale_version_path_map.clear();
    _next_path_id = 1;
    _construct_versioned_tracker(rs_metas);
}

void TimestampedVersionTracker::construct_versioned_tracker(
        const std::vector<RowsetMetaSharedPtr>& rs_metas,
        const std::vector<RowsetMetaSharedPtr>& stale_metas) {

    if (rs_metas.empty()) {
        VLOG(3) << "there is no version in the header.";
        return;
    }
    _stale_version_path_map.clear();
    _next_path_id = 1;
    _construct_versioned_tracker(rs_metas);

    // init _stale_version_path_map
    _init_stale_version_path_map(rs_metas, stale_metas);
}

void TimestampedVersionTracker::_init_stale_version_path_map(
        const std::vector<RowsetMetaSharedPtr>& rs_metas,
        const std::vector<RowsetMetaSharedPtr>& stale_metas) {

    if (stale_metas.empty()) {
        return;
    }

    // sort stale meta by version diff (second version - first version)
    std::list<RowsetMetaSharedPtr> sorted_stale_metas;
    for (auto& rs : stale_metas) {
        sorted_stale_metas.emplace_back(rs);
    }

    // 1. Sort the existing rowsets by version in ascending order
    sorted_stale_metas.sort([](const RowsetMetaSharedPtr& a, const RowsetMetaSharedPtr& b) {
        // compare by version diff between version.first and version.second
        int64_t a_diff = a->version().second - a->version().first;
        int64_t b_diff = b->version().second - b->version().first;

        int diff = a_diff - b_diff;
        if (diff < 0) {
            return true;
        }
        else if (diff > 0) {
            return false;
        }
        // when the version diff is equal, compare rowset create time
        return a->creation_time() < b->creation_time();
    });

    // first_version -> (second_version -> rowset_meta)
    std::unordered_map<int64_t, std::unordered_map<int64_t, RowsetMetaSharedPtr>> stale_map;

    // 2. generate stale path from stale_metas. traverse sorted_stale_metas and each time add stale_meta to stale_map.
    // when a stale path in stale_map can replace stale_meta in sorted_stale_metas, stale_map remove rowset_metas of a stale path
    // and add the path to _stale_version_path_map.
    for(auto& stale_meta:sorted_stale_metas) {
        std::vector<RowsetMetaSharedPtr> stale_path;
        // 2.1 find a path in stale_map can replace current stale_meta version
        bool r = _find_path_from_stale_map(stale_map, stale_meta->start_version(), stale_meta->end_version(), &stale_path);

        // 2.2 add version to version_graph
        Version stale_meta_version = stale_meta->version();
        add_version(stale_meta_version);
        
        // 2.3 find the path
        if (r) {
            // add the path to _stale_version_path_map
            add_stale_path_version(stale_path);
            // remove stale_path from stale_map
            for (auto stale_item:stale_path) {
                stale_map[stale_item->start_version()].erase(stale_item->end_version());
            
                if (stale_map[stale_item->start_version()].empty()) {
                    stale_map.erase(stale_item->start_version());
                }
            }
        }

        // 2.4 add stale_meta to stale_map
        auto start_iter = stale_map.find(stale_meta->start_version());
        if (start_iter != stale_map.end()) {
            start_iter->second[stale_meta->end_version()] = stale_meta;
        } else {
            std::unordered_map<int64_t, RowsetMetaSharedPtr> item;
            item[stale_meta->end_version()] = stale_meta;
            stale_map[stale_meta->start_version()] = std::move(item);
        }
    }

    // 3. generate stale path from rs_metas
    for(auto& stale_meta:rs_metas) {
        std::vector<RowsetMetaSharedPtr> stale_path;
        // 3.1 find a path in stale_map can replace current stale_meta version
        bool r = _find_path_from_stale_map(stale_map, stale_meta->start_version(), stale_meta->end_version(), &stale_path);
        
        // 3.2 find the path 
        if (r) {
            // add the path to _stale_version_path_map
            add_stale_path_version(stale_path);
            // remove stale_path from stale_map
            for (auto stale_item:stale_path) {
                stale_map[stale_item->start_version()].erase(stale_item->end_version());
            
                if (stale_map[stale_item->start_version()].empty()) {
                    stale_map.erase(stale_item->start_version());
                }
            }
        }
    }

    // 4. process remain stale rowset_meta in stale_map
    auto map_iter = stale_map.begin();
    while (map_iter != stale_map.end()) {
        auto second_iter = map_iter->second.begin();
        while(second_iter != map_iter->second.end()) {
            // each remain stale rowset_meta generate a stale path
            std::vector<RowsetMetaSharedPtr> stale_path;
            stale_path.push_back(second_iter->second);
            add_stale_path_version(stale_path);

            second_iter++;
        }
        map_iter++;
    }
}

bool TimestampedVersionTracker::_find_path_from_stale_map(
        const std::unordered_map<int64_t, std::unordered_map<int64_t, RowsetMetaSharedPtr>>& stale_map,
        int64_t first_version, int64_t second_version, std::vector<RowsetMetaSharedPtr>* stale_path) {

    auto first_iter = stale_map.find(first_version);
    // if first_version not in stale_map, there is no path.
    if (first_iter == stale_map.end()) {
        return false;
    }
    auto& second_version_map = first_iter->second;
    auto second_iter = second_version_map.find(second_version);
    // if second_version in stale_map, find a path.
    if (second_iter != second_version_map.end()) {
        auto row_meta = second_iter->second;
        // add rowset to path
        stale_path->push_back(row_meta);
        return true;
    }

    // traverse the first version map to backtracking _find_path_from_stale_map
    auto map_iter = second_version_map.begin();
    while (map_iter != second_version_map.end()) {
        // the version greater than second_version, we can't find path in stale_map
        if (map_iter->first > second_version) {
            map_iter++;
            continue;
        }
        // backtracking _find_path_from_stale_map find from map_iter->first + 1 to second_version
        stale_path->push_back(map_iter->second);
        bool r = _find_path_from_stale_map(stale_map, map_iter->first + 1, second_version, stale_path);
        if (r) {
            return true;
        }
        // there is no path in current version, pop and continue
        stale_path->pop_back();
        map_iter++;
    }

    return false;
}

void TimestampedVersionTracker::get_stale_version_path_json_doc(rapidjson::Document& path_arr) {

    auto path_arr_iter = _stale_version_path_map.begin();

    // do loop version path 
    while (path_arr_iter != _stale_version_path_map.end()) {

        auto path_id = path_arr_iter->first;
        auto path_version_path = path_arr_iter->second;

        rapidjson::Document item;
        item.SetObject();
        // add path_id to item
        auto path_id_str = std::to_string(path_id);
        rapidjson::Value path_id_value;
        path_id_value.SetString(path_id_str.c_str(), path_id_str.length(), path_arr.GetAllocator());
        item.AddMember("path id", path_id_value, path_arr.GetAllocator());

        // add max create time to item
        auto time_zone = cctz::local_time_zone();

        auto tp = std::chrono::system_clock::from_time_t(path_version_path->max_create_time());
        auto create_time_str = cctz::format("%Y-%m-%d %H:%M:%S %z", tp, time_zone);

        rapidjson::Value create_time_value;
        create_time_value.SetString(create_time_str.c_str(), create_time_str.length(), path_arr.GetAllocator());
        item.AddMember("last create time", create_time_value, path_arr.GetAllocator());

        // add path list to item
        std::stringstream path_list_stream;
        path_list_stream << path_id_str;
        auto path_list_ptr = path_version_path->timestamped_versions();
        auto path_list_iter = path_list_ptr.begin();
        while (path_list_iter != path_list_ptr.end()) {
            path_list_stream << " -> ";
            path_list_stream << "[";
            path_list_stream << (*path_list_iter)->version().first;
            path_list_stream << "-";
            path_list_stream << (*path_list_iter)->version().second;
            path_list_stream << "]";
            path_list_iter++;
        }
        std::string path_list = path_list_stream.str();
        rapidjson::Value path_list_value;
        path_list_value.SetString(path_list.c_str(), path_list.length(), path_arr.GetAllocator());
        item.AddMember("path list", path_list_value, path_arr.GetAllocator());
        
        // add item to path_arr
        path_arr.PushBack(item, path_arr.GetAllocator());

        path_arr_iter++;
    }
}

void TimestampedVersionTracker::recover_versioned_tracker(const std::map<int64_t, PathVersionListSharedPtr>& stale_version_path_map) {
    
    auto _path_map_iter = stale_version_path_map.begin();
    // recover stale_version_path_map
    while (_path_map_iter != stale_version_path_map.end()) {
        // add PathVersionListSharedPtr to map
        _stale_version_path_map[_path_map_iter->first] = _path_map_iter->second;

        std::vector<TimestampedVersionSharedPtr>& timestamped_versions = _path_map_iter->second->timestamped_versions();
        std::vector<TimestampedVersionSharedPtr>::iterator version_path_iter = timestamped_versions.begin();
        while (version_path_iter != timestamped_versions.end()) {
            // add version to _version_graph
            _version_graph.add_version_to_graph((*version_path_iter)->version());
            version_path_iter++;
        }
        _path_map_iter++;
    }
    LOG(INFO) <<"recover_versioned_tracker current map info "<< _get_current_path_map_str();
}

void TimestampedVersionTracker::add_version(const Version& version) {
    _version_graph.add_version_to_graph(version);
}

void TimestampedVersionTracker::add_stale_path_version(
        const std::vector<RowsetMetaSharedPtr>& stale_rs_metas) {
    if (stale_rs_metas.empty()) {
        VLOG(3) << "there is no version in the stale_rs_metas.";
        return;
    }

    PathVersionListSharedPtr ptr(new TimestampedVersionPathContainer());
    for (auto rs : stale_rs_metas) {
        TimestampedVersionSharedPtr vt_ptr(new TimestampedVersion(rs->version(), rs->creation_time()));
        ptr->add_timestamped_version(vt_ptr);
    }

    std::vector<TimestampedVersionSharedPtr>& timestamped_versions = ptr->timestamped_versions();

    struct TimestampedVersionPtrCompare {
        bool operator () (const TimestampedVersionSharedPtr ptr1, const TimestampedVersionSharedPtr ptr2) {
            return ptr1->version().first < ptr2->version().first;
        }
    };
    sort(timestamped_versions.begin(), timestamped_versions.end(), TimestampedVersionPtrCompare());
    _stale_version_path_map[_next_path_id] = ptr;
    _next_path_id++;
}

// Capture consistent versions from graph
OLAPStatus TimestampedVersionTracker::capture_consistent_versions(
        const Version& spec_version, std::vector<Version>* version_path) const {
    return _version_graph.capture_consistent_versions(spec_version, version_path);
}

void TimestampedVersionTracker::capture_expired_paths(
        int64_t stale_sweep_endtime, std::vector<int64_t>* path_version_vec) const {

    std::map<int64_t, PathVersionListSharedPtr>::const_iterator iter =
            _stale_version_path_map.begin();

    while (iter != _stale_version_path_map.end()) {
        int64_t max_create_time = iter->second->max_create_time();
        if (max_create_time <= stale_sweep_endtime) {
            int64_t path_version = iter->first;
            path_version_vec->push_back(path_version);
        }
        iter++;
    }
    
}

PathVersionListSharedPtr TimestampedVersionTracker::fetch_path_version_by_id(int64_t path_id) {
    if (_stale_version_path_map.count(path_id) == 0) {
        VLOG(3) << "path version " << path_id << " does not exist!";
        return nullptr;
    }

    return _stale_version_path_map[path_id];
}

PathVersionListSharedPtr TimestampedVersionTracker::fetch_and_delete_path_by_id(int64_t path_id) {
    if (_stale_version_path_map.count(path_id) == 0) {
        VLOG(3) << "path version " << path_id << " does not exist!";
        return nullptr;
    }

    LOG(INFO) << _get_current_path_map_str();
    PathVersionListSharedPtr ptr = fetch_path_version_by_id(path_id);

    _stale_version_path_map.erase(path_id);

    for (auto& version : ptr->timestamped_versions()) {
        _version_graph.delete_version_from_graph(version->version());
    }
    return ptr;
}

std::string TimestampedVersionTracker::_get_current_path_map_str() {

    std::stringstream tracker_info;
    tracker_info << "current expired next_path_id " << _next_path_id << std::endl;

    std::map<int64_t, PathVersionListSharedPtr>::const_iterator iter =
            _stale_version_path_map.begin();
    while (iter != _stale_version_path_map.end()) {
        
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

std::vector<TimestampedVersionSharedPtr>& TimestampedVersionPathContainer::timestamped_versions() {
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
    // When there are same versions in edges, just remove the first version.
    auto start_edges_iter = _version_graph[start_vertex_index].edges.begin();
    while (start_edges_iter != _version_graph[start_vertex_index].edges.end()) {
        if (*start_edges_iter == end_vertex_index) {
            _version_graph[start_vertex_index].edges.erase(start_edges_iter);
            break;
        }
        start_edges_iter++;
    }

    auto end_edges_iter = _version_graph[end_vertex_index].edges.begin();
    while (end_edges_iter != _version_graph[end_vertex_index].edges.end()) {
        if (*end_edges_iter == start_vertex_index) {
            _version_graph[end_vertex_index].edges.erase(end_edges_iter);
            break;
        }
        end_edges_iter++;
    }

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
        LOG(WARNING) << "invalid specified version. "
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
