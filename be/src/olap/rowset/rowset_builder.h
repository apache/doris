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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_BUILDER_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_BUILDER_H

#include "olap/rowset/rowset.h"
#include "olap/new_status.h"
#include "olap/schema.h"
#include "olap/row_block.h"
#include "gen_cpp/types.pb.h"
#include "runtime/mem_pool.h"

namespace doris {

class Rowset;

struct RowsetBuilderContext {
    int64_t partition_id;
    int64_t tablet_id;
    int64_t tablet_schema_hash;
    int64_t rowset_id;
    RowsetTypePB rowset_type;
    std::string rowset_path_prefix;
    RowFields tablet_schema;
    size_t num_key_fields;
    size_t num_short_key_fields;
    size_t num_rows_per_row_block;
    CompressKind compress_kind;
    double bloom_filter_fpp;
};

class RowsetBuilder {
public:
    virtual ~RowsetBuilder() { }
    
    virtual OLAPStatus init(const RowsetBuilderContext& rowset_builder_context) = 0;
    virtual void set_txn_id(const int64_t& txn_id) = 0;
    virtual void set_load_id(const PUniqueId& load_id) = 0; 
    virtual void set_version(const Version& version) = 0; 
    virtual void set_version_hash(const VersionHash& version_hash) = 0;

    // add a row to rowset
    virtual OLAPStatus add_row(RowCursor* row_block) = 0;

    virtual OLAPStatus flush() = 0;

    // get a rowset
    virtual std::shared_ptr<Rowset> build() = 0;
    virtual MemPool* mem_pool();
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_BUILDER_H
