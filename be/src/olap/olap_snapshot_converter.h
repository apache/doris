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

#ifndef DORIS_BE_SRC_OLAP_OLAP_SNAPSHOT_CONVERTER_H
#define DORIS_BE_SRC_OLAP_OLAP_SNAPSHOT_CONVERTER_H

#include <string>
#include <map>
#include <functional>

#include "gen_cpp/olap_file.pb.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/tablet_schema.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/delete_handler.h"

using std::ifstream;
using std::string;
using std::vector;

namespace doris {


class OlapSnapshotConverter {
public:
    // convert tablet meta pb to olap header
    // only consider alpha rowset not other rowsets
    OLAPStatus to_olap_header(const TabletMetaPB& tablet_meta_pb, OLAPHeaderMessage* olap_header);

    // convert olap header to tablet meta pb, convert delta to rowsetmetapb
    // pending delta is not in tablet meta any more, so that convert pending delta to rowset and add it to pending rowsets
    // as a return value
    OLAPStatus to_tablet_meta_pb(const OLAPHeaderMessage& olap_header, TabletMetaPB* tablet_meta_pb, 
                                 vector<RowsetMetaPB>* pending_rowsets);

    OLAPStatus convert_to_pdelta(const RowsetMetaPB& rowset_meta_pb, PDelta* delta);

    OLAPStatus convert_to_rowset_meta(const PDelta& delta, const RowsetId& rowset_id, int64_t tablet_id, 
        int32_t schema_hash, RowsetMetaPB* rowset_meta_pb);

    OLAPStatus convert_to_rowset_meta(const PPendingDelta& pending_delta, const RowsetId& rowset_id, 
        int64_t tablet_id, int32_t schema_hash, RowsetMetaPB* rowset_meta_pb);

    OLAPStatus to_column_pb(const ColumnMessage& column_msg, ColumnPB* column_pb);

    OLAPStatus to_column_msg(const ColumnPB& column_pb, ColumnMessage* column_msg);

    // only convert schema change msg to alter tablet pb, not the other side because snapshot does not need
    // schema change status while restart and upgrade need schema change status
    OLAPStatus to_alter_tablet_pb(const SchemaChangeStatusMessage& schema_change_msg, AlterTabletPB* alter_tablet_pb);

    // from olap header to tablet meta
    OLAPStatus to_new_snapshot(const OLAPHeaderMessage& olap_header, const string& old_data_path_prefix, 
        const string& new_data_path_prefix, TabletMetaPB* tablet_meta_pb,
        vector<RowsetMetaPB>* pending_rowsets, bool is_startup);

    // from tablet meta to olap header
    OLAPStatus to_old_snapshot(const TabletMetaPB& tablet_meta_pb, string& new_data_path_prefix,
        string& old_data_path_prefix, OLAPHeaderMessage* olap_header);
    
    OLAPStatus save(const string& file_path, const OLAPHeaderMessage& olap_header);

private:
    void _modify_old_segment_group_id(RowsetMetaPB& rowset_meta);
};

}

#endif // DORIS_BE_SRC_OLAP_OLAP_SNAPSHOT_CONVERTER_H
