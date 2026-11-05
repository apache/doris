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

#include "format/table/paimon/paimon_table_descriptor.h"

#include <paimon_rust/paimon.h>

#include <vector>

namespace doris::paimon {

Status validate_table_descriptor(const TPaimonTableDesc& descriptor) {
    if (!descriptor.__isset.table_path || descriptor.table_path.empty()) {
        return Status::InvalidArgument("Paimon table descriptor missing table_path");
    }
    if (!descriptor.__isset.schema_json || descriptor.schema_json.empty()) {
        return Status::InvalidArgument("Paimon table descriptor missing schema_json");
    }
    if (!descriptor.__isset.database_name || descriptor.database_name.empty()) {
        return Status::InvalidArgument("Paimon table descriptor missing database_name");
    }
    if (!descriptor.__isset.table_name || descriptor.table_name.empty()) {
        return Status::InvalidArgument("Paimon table descriptor missing table_name");
    }
    if (!descriptor.__isset.branch || descriptor.branch.empty()) {
        return Status::InvalidArgument("Paimon table descriptor missing branch");
    }
    if (!descriptor.__isset.storage_options) {
        return Status::InvalidArgument("Paimon table descriptor missing storage_options");
    }
    return Status::OK();
}

paimon_result_get_table open_table_from_descriptor(const TPaimonTableDesc& descriptor) {
    std::vector<paimon_option> options;
    options.reserve(descriptor.storage_options.size());
    for (const auto& [key, value] : descriptor.storage_options) {
        options.push_back({key.c_str(), value.c_str()});
    }
    return paimon_table_from_schema_json(
            descriptor.table_path.c_str(), descriptor.schema_json.c_str(),
            descriptor.database_name.c_str(), descriptor.table_name.c_str(),
            descriptor.branch.c_str(), options.empty() ? nullptr : options.data(), options.size());
}

} // namespace doris::paimon
