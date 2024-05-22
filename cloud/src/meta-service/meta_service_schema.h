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

namespace doris::cloud {
class Transaction;
struct ValueBuf;

void put_schema_kv(MetaServiceCode& code, std::string& msg, Transaction* txn,
                   std::string_view schema_key, const doris::TabletSchemaCloudPB& schema);

// Return true if parse success
[[nodiscard]] bool parse_schema_value(const ValueBuf& buf, doris::TabletSchemaCloudPB* schema);

// Writes schema dictionary metadata to RowsetMetaCloudPB
void write_schema_dict(MetaServiceCode& code, std::string& msg, const std::string& instance_id,
                       Transaction* txn, RowsetMetaCloudPB* rowset_meta);

// Read schema from dictionary metadata, modified to rowset_metas
void read_schema_from_dict(MetaServiceCode& code, std::string& msg, const std::string& instance_id,
                           int64_t index_id, Transaction* txn,
                           google::protobuf::RepeatedPtrField<RowsetMetaCloudPB>* rowset_metas);

} // namespace doris::cloud
