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
};

class RowsetWriterContextBuilder {
public:
    RowsetWriterContextBuilder() {
        _rowset_writer_context.rowset_id = 0;
        _rowset_writer_context.tablet_id = 0;
        _rowset_writer_context.tablet_schema_hash = 0;
        _rowset_writer_context.partition_id = 0;
        _rowset_writer_context.rowset_type = ALPHA_ROWSET;
        _rowset_writer_context.rowset_path_prefix = "";
        _rowset_writer_context.tablet_schema = nullptr;
        _rowset_writer_context.rowset_state = PREPARED;
        _rowset_writer_context.data_dir = nullptr;
        _rowset_writer_context.version = Version(0,0);
        _rowset_writer_context.version_hash = 0;
        _rowset_writer_context.txn_id = 0;
        _rowset_writer_context.load_id.set_hi(0);
        _rowset_writer_context.load_id.set_lo(0);
    }

    RowsetWriterContextBuilder& set_rowset_id(int64_t rowset_id) {
        _rowset_writer_context.rowset_id = rowset_id;
        return *this;
    }

    RowsetWriterContextBuilder& set_tablet_id(int64_t tablet_id) {
        _rowset_writer_context.tablet_id = tablet_id;
        return *this;
    }

    RowsetWriterContextBuilder& set_tablet_schema_hash(int64_t tablet_schema_hash) {
        _rowset_writer_context.tablet_schema_hash = tablet_schema_hash;
        return *this;
    }
    
    RowsetWriterContextBuilder& set_partition_id(int64_t partition_id) {
        _rowset_writer_context.partition_id = partition_id;
        return *this;
    }

    RowsetWriterContextBuilder& set_rowset_type(RowsetTypePB rowset_type) {
        _rowset_writer_context.rowset_type = rowset_type;
        return *this;
    }

    RowsetWriterContextBuilder& set_rowset_path_prefix(const std::string& rowset_path_prefix) {
        _rowset_writer_context.rowset_path_prefix = rowset_path_prefix;
        return *this;
    }

    RowsetWriterContextBuilder& set_tablet_schema(const TabletSchema* tablet_schema) {
        _rowset_writer_context.tablet_schema = tablet_schema;
        return *this;
    }

    RowsetWriterContextBuilder& set_rowset_state(RowsetStatePB rowset_state) {
        _rowset_writer_context.rowset_state = rowset_state;
        return *this;
    }

    RowsetWriterContextBuilder& set_data_dir(DataDir* data_dir) {
        _rowset_writer_context.data_dir = data_dir;
        return *this;
    }

    RowsetWriterContextBuilder& set_version(Version version) {
        _rowset_writer_context.version = version;
        return *this;
    }

    RowsetWriterContextBuilder& set_version_hash(VersionHash version_hash) {
        _rowset_writer_context.version_hash = version_hash;
        return *this;
    }

    RowsetWriterContextBuilder& set_txn_id(int64_t txn_id) {
        _rowset_writer_context.txn_id = txn_id;
        return *this;
    }

    RowsetWriterContextBuilder& set_load_id(PUniqueId load_id) {
        _rowset_writer_context.load_id.set_hi(load_id.hi());
        _rowset_writer_context.load_id.set_lo(load_id.lo());
        return *this;
    }

    RowsetWriterContext build() {
        return _rowset_writer_context;
    }

private:
    RowsetWriterContext _rowset_writer_context;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_WRITER_CONTEXT_H
