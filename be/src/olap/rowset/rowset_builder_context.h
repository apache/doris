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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_BUILDER_CONTEXT_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_BUILDER_CONTEXT_H

#include "gen_cpp/olap_file.pb.h"

namespace doris {

struct RowsetBuilderContext {
    int64_t rowset_id;
    int64_t tablet_id;
    int64_t tablet_schema_hash;
    int64_t partition_id;
    RowsetTypePB rowset_type;
    std::string rowset_path_prefix;
    RowFields tablet_schema;
    size_t num_key_fields;
    size_t num_short_key_fields;
    size_t num_rows_per_row_block;
    CompressKind compress_kind;
    double bloom_filter_fpp;
    // PREPARING/COMMITTED for pending rowset
    // VISIBLE for non-pending rowset
    RowsetStatePB rowset_state;
    // properties for non-pending rowset
    Version version;
    VersionHash version_hash;

    // properties for pending rowset
    int64_t txn_id;
    PUniqueId load_id;
};

class RowsetBuilderContextBuilder {
public:
    RowsetBuilderContextBuilder& set_rowset_id(int64_t rowset_id) {
        _rowset_builder_context.rowset_id = rowset_id;
        return *this;
    }

    RowsetBuilderContextBuilder& set_tablet_id(int64_t tablet_id) {
        _rowset_builder_context.tablet_id = tablet_id;
        return *this;
    }

    RowsetBuilderContextBuilder& set_tablet_schema_hash(int64_t tablet_schema_hash) {
        _rowset_builder_context.tablet_schema_hash = tablet_schema_hash;
        return *this;
    }
    
    RowsetBuilderContextBuilder& set_partition_id(int64_t partition_id) {
        _rowset_builder_context.partition_id = partition_id;
        return *this;
    }

    RowsetBuilderContextBuilder& set_rowset_type(RowsetTypePB rowset_type) {
        _rowset_builder_context.rowset_type = rowset_type;
        return *this;
    }

    RowsetBuilderContextBuilder& set_rowset_path_prefix(const std::string& rowset_path_prefix) {
        _rowset_builder_context.rowset_path_prefix = rowset_path_prefix;
        return *this;
    }

    RowsetBuilderContextBuilder& set_tablet_schema(const RowFields& tablet_schema) {
        _rowset_builder_context.tablet_schema = tablet_schema;
        return *this;
    }

    RowsetBuilderContextBuilder& set_num_key_fields(size_t num_key_fields) {
        _rowset_builder_context.num_key_fields = num_key_fields;
        return *this;
    }

    RowsetBuilderContextBuilder& set_num_short_key_fields(size_t num_short_key_fields) {
        _rowset_builder_context.num_short_key_fields = num_short_key_fields;
        return *this;
    }

    RowsetBuilderContextBuilder& set_num_rows_per_row_block(size_t num_rows_per_row_block) {
        _rowset_builder_context.num_rows_per_row_block = num_rows_per_row_block;
        return *this;
    }

    RowsetBuilderContextBuilder& set_compress_kind(CompressKind compress_kind) {
        _rowset_builder_context.compress_kind = compress_kind;
        return *this;
    }

    RowsetBuilderContextBuilder& set_bloom_filter_fpp(double bloom_filter_fpp) {
        _rowset_builder_context.bloom_filter_fpp = bloom_filter_fpp;
        return *this;
    }

    RowsetBuilderContextBuilder& set_rowset_state(RowsetStatePB rowset_state) {
        _rowset_builder_context.rowset_state = rowset_state;
        return *this;
    }

    RowsetBuilderContextBuilder& set_version(Version version) {
        _rowset_builder_context.version = version;
        return *this;
    }

    RowsetBuilderContextBuilder& set_version_hash(VersionHash version_hash) {
        _rowset_builder_context.version_hash = version_hash;
        return *this;
    }

    RowsetBuilderContextBuilder& set_txn_id(int64_t txn_id) {
        _rowset_builder_context.txn_id = txn_id;
        return *this;
    }

    RowsetBuilderContextBuilder& set_load_id(PUniqueId load_id) {
        _rowset_builder_context.load_id.set_hi(load_id.hi());
        _rowset_builder_context.load_id.set_lo(load_id.lo());
        return *this;
    }

    RowsetBuilderContext build() {
        return _rowset_builder_context;
    }

private:
    RowsetBuilderContext _rowset_builder_context;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_BUILDER_CONTEXT_H