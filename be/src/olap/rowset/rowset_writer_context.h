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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_WRITER_CONTEXT_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_WRITER_CONTEXT_H

#include "gen_cpp/olap_file.pb.h"
#include "olap/data_dir.h"
#include "olap/tablet_schema.h"

namespace doris {

class RowsetWriterContextBuilder;
using RowsetWriterContextBuilderSharedPtr = std::shared_ptr<RowsetWriterContextBuilder>;

struct RowsetWriterContext {
    RowsetWriterContext()
            : tablet_id(0),
              tablet_schema_hash(0),
              partition_id(0),
              rowset_type(ALPHA_ROWSET),
              tablet_schema(nullptr),
              rowset_state(PREPARED),
              version(Version(0, 0)),
              txn_id(0),
              tablet_uid(0, 0),
              segments_overlap(OVERLAP_UNKNOWN) {
        load_id.set_hi(0);
        load_id.set_lo(0);
    }
    RowsetId rowset_id;
    int64_t tablet_id;
    int64_t tablet_schema_hash;
    int64_t partition_id;
    RowsetTypePB rowset_type;
    FilePathDesc path_desc;
    const TabletSchema* tablet_schema;
    // PREPARED/COMMITTED for pending rowset
    // VISIBLE for non-pending rowset
    RowsetStatePB rowset_state;
    // properties for non-pending rowset
    Version version;

    // properties for pending rowset
    int64_t txn_id;
    PUniqueId load_id;
    TabletUid tablet_uid;
    // indicate whether the data among segments is overlapping.
    // default is OVERLAP_UNKNOWN.
    SegmentsOverlapPB segments_overlap;
    // segment file use uint32 to represent row number, therefore the maximum is UINT32_MAX.
    // the default is set to INT32_MAX to avoid overflow issue when casting from uint32_t to int.
    // test cases can change this value to control flush timing
    uint32_t max_rows_per_segment = INT32_MAX;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_WRITER_CONTEXT_H
