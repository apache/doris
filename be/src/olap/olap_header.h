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

#ifndef DORIS_BE_SRC_OLAP_OLAP_HEADER_H
#define DORIS_BE_SRC_OLAP_OLAP_HEADER_H

#include <list>
#include <string>
#include <unordered_map>
#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/Types_types.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"

namespace doris {
// Class for managing olap table header.
class OLAPHeader : public OLAPHeaderMessage {
public:
    explicit OLAPHeader() :
            _support_reverse_version(false) {}

    // for compatible header file
    explicit OLAPHeader(const std::string& file_name) :
            _file_name(file_name),
            _support_reverse_version(false) {}

    virtual ~OLAPHeader();

    // Loads the header from disk and init, returning true on success.
    // In load_and_init(), we will validate olap header file, which mainly include
    // tablet schema, delta version and so on.
    OLAPStatus load_and_init();
    OLAPStatus load_for_check();

    // Saves the header to disk, returning true on success.
    OLAPStatus save();
    OLAPStatus save(const std::string& file_path);

    OLAPStatus init();

    // Return the file name of the heade.
    std::string file_name() const {
        return _file_name;
    }

    // Adds a new version to the header. Do not use the proto's
    // add_version() directly.
    OLAPStatus add_version(Version version, VersionHash version_hash,
                           int32_t segment_group_id, int32_t num_segments,
                           int64_t index_size, int64_t data_size, int64_t num_rows,
                           bool empty, const std::vector<KeyRange>* column_statistics);

    OLAPStatus add_pending_version(int64_t partition_id, int64_t transaction_id,
                                 const std::vector<std::string>* delete_conditions);
    OLAPStatus add_pending_segment_group(int64_t transaction_id, int32_t num_segments,
                                  int32_t pending_segment_group_id, const PUniqueId& load_id,
                                  bool empty, const std::vector<KeyRange>* column_statistics);

    // add incremental segment_group into header like "9-9" "10-10", for incremental cloning
    OLAPStatus add_incremental_version(Version version, VersionHash version_hash,
                                       int32_t segment_group_id, int32_t num_segments,
                                       int64_t index_size, int64_t data_size, int64_t num_rows,
                                       bool empty, const std::vector<KeyRange>* column_statistics);

    void add_delete_condition(const DeleteConditionMessage& delete_condition, int64_t version);
    void delete_cond_by_version(const Version& version);
    bool is_delete_data_version(Version version);

    const PPendingDelta* get_pending_delta(int64_t transaction_id) const;
    const PPendingSegmentGroup* get_pending_segment_group(int64_t transaction_id, int32_t pending_segment_group_id) const;
    const PDelta* get_incremental_version(Version version) const;

    // Deletes a version from the header.
    OLAPStatus delete_version(Version version);
    OLAPStatus delete_all_versions();
    void delete_pending_delta(int64_t transaction_id);
    void delete_incremental_delta(Version version);

    // Constructs a canonical file name (without path) for the header.
    // eg "DailyUnitStats_PRIMARY.hdr"
    std::string construct_file_name() const {
        return std::string(basename(_file_name.c_str()));
    }

    // In order to prevent reverse version to appear in the shortest version
    // path, you can call set_reverse_version(false) although schema can
    // support reverse version in the path.
    void set_reverse_version(bool support_reverse_version) {
        _support_reverse_version = support_reverse_version;
    }

    // Try to select the least number of data files that can span the
    // target_version and append these data versions to the span_versions.
    // Return false if the target_version cannot be spanned.
    virtual OLAPStatus select_versions_to_span(const Version& target_version,
                                           std::vector<Version>* span_versions);

    const PDelta* get_lastest_delta_version() const;
    const PDelta* get_lastest_version() const;
    Version get_latest_version() const;
    const PDelta* get_delta(int index) const;
    const PDelta* get_base_version() const;
    const uint32_t get_cumulative_compaction_score() const;
    const uint32_t get_base_compaction_score() const;
    const OLAPStatus version_creation_time(const Version& version, int64_t* creation_time) const;

    int file_delta_size() const {
        return delta_size();
    }
    void change_file_version_to_delta();
private:
    void _convert_file_version_to_delta(const FileVersionMessage& version, PDelta* delta);

    // full path of olap header file
    std::string _file_name;

    // If the aggregation types of all value columns in the schema are SUM,
    // select_versions_to_span can return reverse version in the shortest
    // version path. one can set _support_reverse_version to be false in
    // order to prevent reverse version to appear in the shortest version path.
    // Its default value is false.
    bool _support_reverse_version;

    // OLAP version contains two parts, [start_version, end_version]. In order
    // to construct graph, the OLAP version has two corresponding vertex, one
    // vertex's value is version.start_version, the other is
    // version.end_version + 1.
    // Use adjacency list to describe version graph.
    std::vector<Vertex> _version_graph;

    // vertex value --> vertex_index of _version_graph
    // It is easy to find vertex index according to vertex value.
    std::unordered_map<int, int> _vertex_helper_map;
    
    DISALLOW_COPY_AND_ASSIGN(OLAPHeader);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_HEADER_H
