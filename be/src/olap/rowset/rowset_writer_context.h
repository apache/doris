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

#include "gen_cpp/olap_file.pb.h"
#include "olap/data_dir.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
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
              fs(nullptr),
              tablet_schema(nullptr),
              rowset_state(PREPARED),
              version(Version(0, 0)),
              txn_id(0),
              tablet_uid(0, 0),
              segments_overlap(OVERLAP_UNKNOWN) {
        load_id.set_hi(0);
        load_id.set_lo(0);
    }

    static RowsetWriterContext create(const Version& version, TabletSharedPtr new_tablet,
                                      RowsetTypePB new_rowset_type,
                                      SegmentsOverlapPB segments_overlap) {
        RowsetWriterContext context;
        context.rowset_id = StorageEngine::instance()->next_rowset_id();
        context.tablet_uid = new_tablet->tablet_uid();
        context.tablet_id = new_tablet->tablet_id();
        context.partition_id = new_tablet->partition_id();
        context.tablet_schema_hash = new_tablet->schema_hash();
        context.rowset_type = new_rowset_type;
        context.rowset_dir = new_tablet->tablet_path();
        context.tablet_schema = new_tablet->tablet_schema();
        context.data_dir = new_tablet->data_dir();
        context.rowset_state = VISIBLE;
        context.version = version;
        context.segments_overlap = segments_overlap;

        return context;
    }

    RowsetId rowset_id;
    int64_t tablet_id;
    int64_t tablet_schema_hash;
    int64_t partition_id;
    RowsetTypePB rowset_type;
    io::FileSystemSPtr fs = nullptr;
    std::string rowset_dir = "";
    TabletSchemaSPtr tablet_schema;
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
    // not owned, point to the data dir of this rowset
    // for checking disk capacity when write data to disk.
    // ATTN: not support for RowsetConvertor.
    // (because it hard to refactor, and RowsetConvertor will be deprecated in future)
    DataDir* data_dir = nullptr;

    int64_t oldest_write_timestamp;
    int64_t newest_write_timestamp;
    bool enable_unique_key_merge_on_write = false;
};

} // namespace doris
