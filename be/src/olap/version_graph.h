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

#pragma once

#include <rapidjson/document.h>
#include <stdint.h>

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_meta.h"

namespace doris {

template <typename R>
concept RowsetMetaSPtrRange = std::same_as<std::ranges::range_value_t<R>, RowsetMetaSharedPtr> &&
                              std::ranges::sized_range<R> && std::ranges::forward_range<R>;
/// VersionGraph class which is implemented to build and maintain total versions of rowsets.
/// This class use adjacency-matrix represent rowsets version and links. A vertex is a version
/// and a link is the _version object of a rowset (from start version to end version + 1).
/// Use this class, when given a spec_version, we can get a version path which is the shortest path
/// in the graph.
class VersionGraph {
public:
    /// Use rs_metas to construct the graph including vertex and edges, and return the
    /// max_version in metas.
    void construct_version_graph(RowsetMetaSPtrRange auto&& rs_metas, int64_t* max_version);
    void construct_version_graph(const RowsetMetaMapContainer& rs_metas, int64_t* max_version);
    /// Reconstruct the graph, begin construction the vertex vec and edges list will be cleared.
    void reconstruct_version_graph(RowsetMetaSPtrRange auto&&, int64_t* max_version);
    void reconstruct_version_graph(const RowsetMetaMapContainer& rs_metas, int64_t* max_version);
    /// Add a version to this graph, graph will add the version and edge in version.
    void add_version_to_graph(const Version& version);
    /// Delete a version from graph. Notice that this del operation only remove this edges and
    /// remain the vertex.
    Status delete_version_from_graph(const Version& version);
    /// Given a spec_version, this method can find a version path which is the shortest path
    /// in the graph. The version paths are added to version_path as return info.
    Status capture_consistent_versions(const Version& spec_version,
                                       std::vector<Version>* version_path) const;

    Status capture_consistent_versions_prefer_cache(
            const Version& spec_version, std::vector<Version>& version_path,
            const std::function<bool(int64_t, int64_t)>& validator) const;

    // Given a start, this method can find a version path which satisfy the following conditions:
    // 1. all edges satisfy the conditions specified by `validator` in the graph.
    // 2. the destination version is as far as possible.
    // 3. the path is the shortest path.
    // The version paths are added to version_path as return info.
    // If this version not in main version, version_path can be included expired rowset.
    // NOTE: this method may return edges which is in stale path
    //
    // @param validator: Function that takes (start_version, end_version) representing a rowset
    //                   and returns true if the rowset should be included in the path, false to skip it
    Status capture_consistent_versions_with_validator(
            const Version& spec_version, std::vector<Version>& version_path,
            const std::function<bool(int64_t, int64_t)>& validator) const;

    // Capture consistent versions with validator for merge-on-write (MOW) tables.
    // Similar to capture_consistent_versions_with_validator but with special handling for MOW tables.
    // For MOW tables, newly generated delete bitmap marks will be on the rowsets which are in newest layout.
    // So we can only capture rowsets which are in newest data layout to ensure data correctness.
    //
    // @param validator: Function that takes (start_version, end_version) representing a rowset
    //                   and returns true if the rowset is warmed up, false if not warmed up
    Status capture_consistent_versions_with_validator_mow(
            const Version& spec_version, std::vector<Version>& version_path,
            const std::function<bool(int64_t, int64_t)>& validator) const;

    // See comment of TimestampedVersionTracker's get_orphan_vertex_ratio();
    double get_orphan_vertex_ratio();

    std::string debug_string() const;

private:
    /// Private method add a version to graph.
    void _add_vertex_to_graph(int64_t vertex_value);

    // OLAP version contains two parts, [start_version, end_version]. In order
    // to construct graph, the OLAP version has two corresponding vertex, one
    // vertex's value is version.start_version, the other is
    // version.end_version + 1.
    // Use adjacency list to describe version graph.
    // In order to speed up the version capture, vertex's edges are sorted by version in descending order.
    std::vector<Vertex> _version_graph;

    // vertex value --> vertex_index of _version_graph
    // It is easy to find vertex index according to vertex value.
    std::unordered_map<int64_t, int64_t> _vertex_index_map;
};

/// TimestampedVersion class which is implemented to maintain multi-version path of rowsets.
/// This compaction info of a rowset includes start version, end version and the create time.
class TimestampedVersion {
public:
    /// TimestampedVersion construction function. Use rowset version and create time to build a TimestampedVersion.
    TimestampedVersion(const Version& version, int64_t create_time)
            : _version(version.first, version.second), _create_time(create_time) {}

    ~TimestampedVersion() {}

    /// Return the rowset version of TimestampedVersion record.
    Version version() const { return _version; }
    /// Return the rowset create_time of TimestampedVersion record.
    int64_t get_create_time() { return _create_time; }

    /// Compare two version trackers.
    bool operator!=(const TimestampedVersion& rhs) const { return _version != rhs._version; }

    /// Compare two version trackers.
    bool operator==(const TimestampedVersion& rhs) const { return _version == rhs._version; }

    /// Judge if a tracker contains the other.
    bool contains(const TimestampedVersion& other) const {
        return _version.contains(other._version);
    }

private:
    Version _version;
    int64_t _create_time;
};

using TimestampedVersionSharedPtr = std::shared_ptr<TimestampedVersion>;

/// TimestampedVersionPathContainer class is used to maintain a path timestamped version path
/// and record the max create time in a path version. Once a timestamped version is added, the max_create_time
/// will compare with the version timestamp and be refreshed.
class TimestampedVersionPathContainer {
public:
    /// TimestampedVersionPathContainer construction function, max_create_time is assigned to 0.
    TimestampedVersionPathContainer() : _max_create_time(0) {}

    /// Return the max create time in a path version.
    int64_t max_create_time() { return _max_create_time; }

    /// Add a timestamped version to timestamped_versions_container. Once a timestamped version is added,
    /// the max_create_time will compare with the version timestamp and be refreshed.
    void add_timestamped_version(TimestampedVersionSharedPtr version);

    /// Return the timestamped_versions_container as const type.
    std::vector<TimestampedVersionSharedPtr>& timestamped_versions();

private:
    std::vector<TimestampedVersionSharedPtr> _timestamped_versions_container;
    int64_t _max_create_time;
};

using PathVersionListSharedPtr = std::shared_ptr<TimestampedVersionPathContainer>;

/// TimestampedVersionTracker class is responsible to track all rowsets version links of a tablet.
/// This class not only records the graph of all versions, but also records the paths which will be removed
/// after the path is expired.
class TimestampedVersionTracker {
public:
    /// Construct rowsets version tracker by main path rowset meta.
    void construct_versioned_tracker(RowsetMetaSPtrRange auto&& rs_metas);
    void construct_versioned_tracker(const RowsetMetaMapContainer& rs_metas);

    /// Construct rowsets version tracker by main path rowset meta and stale rowset meta.
    void construct_versioned_tracker(RowsetMetaSPtrRange auto&& rs_metas,
                                     RowsetMetaSPtrRange auto&& stale_metas);
    void construct_versioned_tracker(const RowsetMetaMapContainer& rs_metas,
                                     const RowsetMetaMapContainer& stale_metas);

    /// Recover rowsets version tracker from stale version path map. When delete operation fails, the
    /// tracker can be recovered from deleted stale_version_path_map.
    void recover_versioned_tracker(
            const std::map<int64_t, PathVersionListSharedPtr>& stale_version_path_map);

    /// Add a version to tracker, this version is a new version rowset, not merged rowset.
    void add_version(const Version& version);

    void delete_version(const Version& version) {
        static_cast<void>(_version_graph.delete_version_from_graph(version));
    }

    /// Add a version path with stale_rs_metas, this versions in version path
    /// are merged rowsets.  These rowsets are tracked and removed after they are expired.
    /// TabletManager sweep these rowsets using tracker by timing.
    void add_stale_path_version(const std::vector<RowsetMetaSharedPtr>& stale_rs_metas);

    /// Given a spec_version, this method can find a version path which is the shortest path
    /// in the graph. The version paths are added to version_path as return info.
    /// If this version not in main version, version_path can be included expired rowset.
    Status capture_consistent_versions(const Version& spec_version,
                                       std::vector<Version>* version_path) const;

    Status capture_consistent_versions_prefer_cache(
            const Version& spec_version, std::vector<Version>& version_path,
            const std::function<bool(int64_t, int64_t)>& validator) const;

    // Given a start, this method can find a version path which satisfy the following conditions:
    // 1. all edges satisfy the conditions specified by `validator` in the graph.
    // 2. the destination version is as far as possible.
    // 3. the path is the shortest path.
    // The version paths are added to version_path as return info.
    // If this version not in main version, version_path can be included expired rowset.
    // NOTE: this method may return edges which is in stale path
    //
    // @param validator: Function that takes (start_version, end_version) representing a rowset
    //                   and returns true if the rowset should be included in the path, false to skip it
    Status capture_consistent_versions_with_validator(
            const Version& spec_version, std::vector<Version>& version_path,
            const std::function<bool(int64_t, int64_t)>& validator) const;

    // Capture consistent versions with validator for merge-on-write (MOW) tables.
    // Similar to capture_consistent_versions_with_validator but with special handling for MOW tables.
    // For MOW tables, newly generated delete bitmap marks will be on the rowsets which are in newest layout.
    // So we can only capture rowsets which are in newest data layout to ensure data correctness.
    //
    // @param validator: Function that takes (start_version, end_version) representing a rowset
    //                   and returns true if the rowset is warmed up, false if not warmed up
    Status capture_consistent_versions_with_validator_mow(
            const Version& spec_version, std::vector<Version>& version_path,
            const std::function<bool(int64_t, int64_t)>& validator) const;

    /// Capture all expired path version.
    /// When the last rowset create time of a path greater than expired time  which can be expressed
    /// "now() - tablet_rowset_stale_sweep_time_sec" , this path will be remained.
    /// Otherwise, this path will be added to path_version.
    void capture_expired_paths(int64_t stale_sweep_endtime,
                               std::vector<int64_t>* path_version) const;

    /// Fetch all versions with a path_version.
    PathVersionListSharedPtr fetch_path_version_by_id(int64_t path_id);

    /// Fetch all versions with a path_version, at the same time remove this path from the tracker.
    /// Next time, fetch this path, it will return empty.
    PathVersionListSharedPtr fetch_and_delete_path_by_id(int64_t path_id);

    /// Print all expired version path in a tablet.
    std::string get_current_path_map_str();

    /// Get json document of _stale_version_path_map. Fill the path_id and version_path
    /// list in the document. The parameter path arr is used as return variable.
    void get_stale_version_path_json_doc(rapidjson::Document& path_arr);

    // Return proportion of orphan vertex in VersionGraph's _version_graph.
    // If a vertex is no longer the starting point of any edge, then this vertex is defined as orphan vertex
    double get_orphan_vertex_ratio();

    std::string debug_string() const;

private:
    /// Construct rowsets version tracker with main path rowset meta.
    void _construct_versioned_tracker(RowsetMetaSPtrRange auto&& rs_metas);

    /// init stale_version_path_map by main path rowset meta and stale rowset meta.
    void _init_stale_version_path_map(RowsetMetaSPtrRange auto&& rs_metas,
                                      RowsetMetaSPtrRange auto&& stale_metas);

    /// find a path in stale_map from first_version to second_version, stale_path is used as result.
    bool _find_path_from_stale_map(
            const std::unordered_map<int64_t, std::unordered_map<int64_t, RowsetMetaSharedPtr>>&
                    stale_map,
            int64_t first_version, int64_t second_version,
            std::vector<RowsetMetaSharedPtr>* stale_path);

private:
    // This variable records the id of path version which will be dispatched to next path version,
    // it is not persisted.
    int64_t _next_path_id = 1;

    // path_version -> list of path version,
    // This variable is used to maintain the map from path version and it's all version.
    std::map<int64_t, PathVersionListSharedPtr> _stale_version_path_map;

    VersionGraph _version_graph;
};

void VersionGraph::reconstruct_version_graph(RowsetMetaSPtrRange auto&& rs_metas,
                                             int64_t* max_version) {
    _version_graph.clear();
    _vertex_index_map.clear();

    construct_version_graph(rs_metas, max_version);
}

void VersionGraph::construct_version_graph(RowsetMetaSPtrRange auto&& rs_metas,
                                           int64_t* max_version) {
    if (std::ranges::empty(rs_metas)) {
        VLOG_NOTICE << "there is no version in the header.";
        return;
    }

    // Distill vertex values from versions in TabletMeta.
    std::vector<int64_t> vertex_values;
    vertex_values.reserve(2 * std::ranges::size(rs_metas));

    for (const auto& rs : rs_metas) {
        vertex_values.push_back(rs->start_version());
        vertex_values.push_back(rs->end_version() + 1);
        if (max_version != nullptr and *max_version < rs->end_version()) {
            *max_version = rs->end_version();
        }
    }
    std::sort(vertex_values.begin(), vertex_values.end());

    // Items in `vertex_values` are sorted, but not unique.
    // we choose unique items in `vertex_values` to create vertexes.
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
    for (const auto& rs : rs_metas) {
        // Versions in header are unique.
        // We ensure `_vertex_index_map` has its `start_version`.
        int64_t start_vertex_index = _vertex_index_map[rs->start_version()];
        int64_t end_vertex_index = _vertex_index_map[rs->end_version() + 1];
        // Add one edge from `start_version` to `end_version`.
        _version_graph[start_vertex_index].edges.push_front(end_vertex_index);
        // Add reverse edge from `end_version` to `start_version`.
        _version_graph[end_vertex_index].edges.push_front(start_vertex_index);
    }

    // Sort edges by version in descending order.
    for (auto& vertex : _version_graph) {
        vertex.edges.sort([this](const int& vertex_idx_a, const int& vertex_idx_b) {
            return _version_graph[vertex_idx_a].value > _version_graph[vertex_idx_b].value;
        });
    }
}

void TimestampedVersionTracker::_construct_versioned_tracker(RowsetMetaSPtrRange auto&& rs_metas) {
    int64_t max_version = 0;

    // construct the rowset graph
    _version_graph.reconstruct_version_graph(rs_metas, &max_version);
}

void TimestampedVersionTracker::construct_versioned_tracker(RowsetMetaSPtrRange auto&& rs_metas) {
    if (std::ranges::empty(rs_metas)) {
        VLOG_NOTICE << "there is no version in the header.";
        return;
    }
    _stale_version_path_map.clear();
    _next_path_id = 1;
    _construct_versioned_tracker(rs_metas);
}

void TimestampedVersionTracker::construct_versioned_tracker(
        RowsetMetaSPtrRange auto&& rs_metas, RowsetMetaSPtrRange auto&& stale_metas) {
    if (std::ranges::empty(rs_metas)) {
        VLOG_NOTICE << "there is no version in the header.";
        return;
    }
    _stale_version_path_map.clear();
    _next_path_id = 1;
    _construct_versioned_tracker(rs_metas);

    // Init `_stale_version_path_map`.
    _init_stale_version_path_map(rs_metas, stale_metas);
}

void TimestampedVersionTracker::_init_stale_version_path_map(
        RowsetMetaSPtrRange auto&& rs_metas, RowsetMetaSPtrRange auto&& stale_metas) {
    if (std::ranges::empty(stale_metas)) {
        return;
    }

    // Sort stale meta by version diff (second version - first version).
    std::list<RowsetMetaSharedPtr> sorted_stale_metas;
    for (const auto& rs : stale_metas) {
        sorted_stale_metas.emplace_back(rs);
    }

    // 1. sort the existing rowsets by version in ascending order.
    sorted_stale_metas.sort([](const RowsetMetaSharedPtr& a, const RowsetMetaSharedPtr& b) {
        // Compare by version diff between `version.first` and `version.second`.
        int64_t a_diff = a->version().second - a->version().first;
        int64_t b_diff = b->version().second - b->version().first;

        int64_t diff = a_diff - b_diff;
        if (diff < 0) {
            return true;
        } else if (diff > 0) {
            return false;
        }
        // When the version diff is equal, compare the rowset`s stale time
        return a->stale_at() < b->stale_at();
    });

    // first_version -> (second_version -> rowset_meta)
    std::unordered_map<int64_t, std::unordered_map<int64_t, RowsetMetaSharedPtr>> stale_map;

    // 2. generate stale path from stale_metas. traverse sorted_stale_metas and each time add stale_meta to stale_map.
    // when a stale path in stale_map can replace stale_meta in sorted_stale_metas, stale_map remove rowset_metas of a stale path
    // and add the path to `_stale_version_path_map`.
    for (auto& stale_meta : sorted_stale_metas) {
        std::vector<RowsetMetaSharedPtr> stale_path;
        // 2.1 find a path in `stale_map` can replace current `stale_meta` version.
        bool r = _find_path_from_stale_map(stale_map, stale_meta->start_version(),
                                           stale_meta->end_version(), &stale_path);

        // 2.2 add version to `version_graph`.
        Version stale_meta_version = stale_meta->version();
        add_version(stale_meta_version);

        // 2.3 find the path.
        if (r) {
            // Add the path to `_stale_version_path_map`.
            add_stale_path_version(stale_path);
            // Remove `stale_path` from `stale_map`.
            for (auto stale_item : stale_path) {
                stale_map[stale_item->start_version()].erase(stale_item->end_version());

                if (stale_map[stale_item->start_version()].empty()) {
                    stale_map.erase(stale_item->start_version());
                }
            }
        }

        // 2.4 add `stale_meta` to `stale_map`.
        auto start_iter = stale_map.find(stale_meta->start_version());
        if (start_iter != stale_map.end()) {
            start_iter->second[stale_meta->end_version()] = stale_meta;
        } else {
            std::unordered_map<int64_t, RowsetMetaSharedPtr> item;
            item[stale_meta->end_version()] = stale_meta;
            stale_map[stale_meta->start_version()] = std::move(item);
        }
    }

    // 3. generate stale path from `rs_metas`.
    for (const auto& stale_meta : rs_metas) {
        std::vector<RowsetMetaSharedPtr> stale_path;
        // 3.1 find a path in stale_map can replace current `stale_meta` version.
        bool r = _find_path_from_stale_map(stale_map, stale_meta->start_version(),
                                           stale_meta->end_version(), &stale_path);

        // 3.2 find the path.
        if (r) {
            // Add the path to `_stale_version_path_map`.
            add_stale_path_version(stale_path);
            // Remove `stale_path` from `stale_map`.
            for (auto stale_item : stale_path) {
                stale_map[stale_item->start_version()].erase(stale_item->end_version());

                if (stale_map[stale_item->start_version()].empty()) {
                    stale_map.erase(stale_item->start_version());
                }
            }
        }
    }

    // 4. process remain stale `rowset_meta` in `stale_map`.
    auto map_iter = stale_map.begin();
    while (map_iter != stale_map.end()) {
        auto second_iter = map_iter->second.begin();
        while (second_iter != map_iter->second.end()) {
            // Each remain stale `rowset_meta` generate a stale path.
            std::vector<RowsetMetaSharedPtr> stale_path;
            stale_path.push_back(second_iter->second);
            add_stale_path_version(stale_path);

            second_iter++;
        }
        map_iter++;
    }
}

} // namespace doris
