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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_GRAPH_H
#define DORIS_BE_SRC_OLAP_ROWSET_GRAPH_H

#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset_meta.h"

namespace doris {

class RowsetGraph {
public:
    OLAPStatus construct_rowset_graph(const std::vector<RowsetMetaSharedPtr>& rs_metas);
    OLAPStatus reconstruct_rowset_graph(const std::vector<RowsetMetaSharedPtr>& rs_metas);
    OLAPStatus add_version_to_graph(const Version& version);
    OLAPStatus delete_version_from_graph(const Version& version);
    OLAPStatus capture_consistent_versions(const Version& spec_version,
                                           std::vector<Version>* version_path) const;
private:
    OLAPStatus _add_vertex_to_graph(int64_t vertex_value);

    // OLAP version contains two parts, [start_version, end_version]. In order
    // to construct graph, the OLAP version has two corresponding vertex, one
    // vertex's value is version.start_version, the other is
    // version.end_version + 1.
    // Use adjacency list to describe version graph.
    std::vector<Vertex> _version_graph;

    // vertex value --> vertex_index of _version_graph
    // It is easy to find vertex index according to vertex value.
    std::unordered_map<int64_t, int64_t> _vertex_index_map;
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_ROWSET_GRAPH_H
