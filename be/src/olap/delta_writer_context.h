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

#include <brpc/stream.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>

#include <vector>

namespace doris {

class TupleDescriptor;
class SlotDescriptor;
class OlapTableSchemaParam;

struct WriteRequest {
    int64_t tablet_id = 0;
    int32_t schema_hash = 0;
    int64_t txn_id = 0;
    int64_t txn_expiration = 0; // For cloud mode
    int64_t index_id = 0;
    int64_t partition_id = 0;
    PUniqueId load_id;
    TupleDescriptor* tuple_desc = nullptr;
    // slots are in order of tablet's schema
    const std::vector<SlotDescriptor*>* slots = nullptr;
    std::shared_ptr<OlapTableSchemaParam> table_schema_param = nullptr;
    bool is_high_priority = false;
    bool write_file_cache = false;
    std::string storage_vault_id;
};

} // namespace doris
