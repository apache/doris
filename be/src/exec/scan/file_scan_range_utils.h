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

#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {

inline Status validate_file_scan_support(const TFileScanRangeParams& params,
                                         const TFileRangeDesc* range = nullptr) {
    // Deprecated table markers must never fall through to plain file reads: that would lose
    // Hive transaction visibility and LakeSoul merge semantics, including in metadata counts.
    auto validate_table = [](const TTableFormatFileDesc& table) -> Status {
        if (table.table_format_type == "transactional_hive") {
            return Status::NotSupported("Hive transactional table reads are no longer supported");
        }
        if (table.table_format_type == "lakesoul") {
            return Status::NotSupported("LakeSoul reads are no longer supported");
        }
        return Status::OK();
    };
    if (params.__isset.table_format_params) {
        RETURN_IF_ERROR(validate_table(params.table_format_params));
    }
    if (range != nullptr && range->__isset.table_format_params) {
        RETURN_IF_ERROR(validate_table(range->table_format_params));
    }
    const auto format = range != nullptr && range->__isset.format_type ? range->format_type
                                                                       : params.format_type;
    if (format == TFileFormatType::FORMAT_AVRO) {
        return Status::NotSupported("Avro file reads are no longer supported");
    }
    return Status::OK();
}

inline bool can_ignore_not_found_file(const TFileRangeDesc& range, bool ignore_not_found) {
    // Iceberg ranges belong to a committed snapshot, not a stale directory listing. Skipping a
    // missing data or delete file would silently return an incomplete or incorrect snapshot.
    return ignore_not_found && !(range.__isset.table_format_params &&
                                 range.table_format_params.table_format_type == "iceberg");
}

} // namespace doris
