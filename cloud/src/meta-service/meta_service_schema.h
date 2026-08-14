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

#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <string>
#include <string_view>

namespace doris::cloud {
class Transaction;
struct ValueBuf;

// Compares only the row TTL policy carried by two tablet schemas. Schema versions and ordinary
// column/index evolution are intentionally ignored so callers can fence metadata replacement
// against the currently active TTL policy.
bool check_row_ttl_policy_compatible(const doris::TabletSchemaCloudPB& schema,
                                     const doris::TabletSchemaCloudPB& saved_schema,
                                     std::string* reason);

bool check_tablet_schema_compatible(const doris::TabletSchemaCloudPB& schema,
                                    const doris::TabletSchemaCloudPB& saved_schema,
                                    std::string* reason);

// Returns the stable schema stored in schema KV after variant extension columns, indexes and
// sparse children have been externalized by write_schema_dict().
doris::TabletSchemaCloudPB normalize_tablet_schema_for_schema_kv(
        const doris::TabletSchemaCloudPB& schema);

// Normalizes legacy FE type spellings before a tablet-level schema is detached into schema KV.
void normalize_tablet_schema_column_types(doris::TabletSchemaCloudPB* schema);

// Read-only compatibility checks. A missing key is compatible because the subsequent write will
// create it; an existing key must contain the same schema and row TTL policy.
void check_schema_kv(MetaServiceCode& code, std::string& msg, Transaction* txn,
                     std::string_view schema_key, const doris::TabletSchemaCloudPB& schema);

void check_versioned_schema_kv(MetaServiceCode& code, std::string& msg, Transaction* txn,
                               std::string_view schema_key,
                               const doris::TabletSchemaCloudPB& schema);

void put_schema_kv(MetaServiceCode& code, std::string& msg, Transaction* txn,
                   std::string_view schema_key, const doris::TabletSchemaCloudPB& schema);

void put_versioned_schema_kv(MetaServiceCode& code, std::string& msg, Transaction* txn,
                             std::string_view schema_key, const doris::TabletSchemaCloudPB& schema);

// Return true if parse success
[[nodiscard]] bool parse_schema_value(const ValueBuf& buf, doris::TabletSchemaCloudPB* schema);

// Writes schema dictionary metadata to RowsetMetaCloudPB
void write_schema_dict(MetaServiceCode& code, std::string& msg, const std::string& instance_id,
                       Transaction* txn, RowsetMetaCloudPB* rowset_meta);

// Read schema from dictionary metadata, modified to rowset_metas
void read_schema_dict(MetaServiceCode& code, std::string& msg, const std::string& instance_id,
                      int64_t index_id, Transaction* txn,
                      google::protobuf::RepeatedPtrField<doris::RowsetMetaCloudPB>* rsp_metas,
                      SchemaCloudDictionary* rsp_dict,
                      GetRowsetRequest::SchemaOp schema_op = GetRowsetRequest::FILL_WITH_DICT);

} // namespace doris::cloud
