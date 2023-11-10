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

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_meta.h"

namespace doris {
/// VersionGraph class which is implemented to build and maintain total versions of rowsets.
/// This class use adjacency-matrix represent rowsets version and links. A vertex is a version
/// and a link is the _version object of a rowset (from start version to end version + 1).
/// Use this class, when given a spec_version, we can get a version path which is the shortest path
/// in the graph.
class VersionGraph {
public:
    /// Use rs_metas to construct the graph including vertex and edges, and return the
    /// max_version in metas.
    void construct_version_graph(const std::vector<RowsetMetaSharedPtr>& rs_metas,
                                 int64_t* max_version);
    /// Reconstruct the graph, begin construction the vertex vec and edges list will be cleared.
    void reconstruct_version_graph(const std::vector<RowsetMetaSharedPtr>& rs_metas,
                                   int64_t* max_version);
    /// Add a version to this graph, graph will add the version and edge in version.
    void add_version_to_graph(const Version& version);
    /// Delete a version from graph. Notice that this del operation only remove this edges and
    /// remain the vertex.
    Status delete_version_from_graph(const Version& version);
    /// Given a spec_version, this method can find a version path which is the shortest path
    /// in the graph. The version paths are added to version_path as return info.
    Status capture_consistent_versions(const Version& spec_version,
                                       std::vector<Version>* version_path) const;

    // See comment of TimestampedVersionTracker's get_orphan_vertex_ratio();
    double get_orphan_vertex_ratio();

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
    void construct_versioned_tracker(const std::vector<RowsetMetaSharedPtr>& rs_metas);

    /// Construct rowsets version tracker by main path rowset meta and stale rowset meta.
    void construct_versioned_tracker(const std::vector<RowsetMetaSharedPtr>& rs_metas,
                                     const std::vector<RowsetMetaSharedPtr>& stale_metas);

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

private:
    /// Construct rowsets version tracker with main path rowset meta.
    void _construct_versioned_tracker(const std::vector<RowsetMetaSharedPtr>& rs_metas);

    /// init stale_version_path_map by main path rowset meta and stale rowset meta.
    void _init_stale_version_path_map(const std::vector<RowsetMetaSharedPtr>& rs_metas,
                                      const std::vector<RowsetMetaSharedPtr>& stale_metas);

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

} // namespace doris
