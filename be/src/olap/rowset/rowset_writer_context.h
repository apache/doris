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
    RowsetWriterContext() :
        rowset_id(0),
        tablet_id(0),
        tablet_schema_hash(0),
        partition_id(0),
        rowset_type(ALPHA_ROWSET),
        rowset_path_prefix(""),
        tablet_schema(nullptr),
        rowset_state(PREPARED),
        data_dir(nullptr),
        version(Version(0, 0)),
        version_hash(0),
        txn_id(0) {
        load_id.set_hi(0);
        load_id.set_lo(0);
        tablet_uid.hi = 0;
        tablet_uid.lo = 0;
    }
    int64_t rowset_id;
    int64_t tablet_id;
    int64_t tablet_schema_hash;
    int64_t partition_id;
    RowsetTypePB rowset_type;
    std::string rowset_path_prefix;
    const TabletSchema* tablet_schema;
    // PREPARED/COMMITTED for pending rowset
    // VISIBLE for non-pending rowset
    RowsetStatePB rowset_state;
    DataDir* data_dir;
    // properties for non-pending rowset
    Version version;
    VersionHash version_hash;

    // properties for pending rowset
    int64_t txn_id;
    PUniqueId load_id;
    TabletUid tablet_uid;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_WRITER_CONTEXT_H
