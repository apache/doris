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

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/tablet/tablet_schema.h"
#include "util/string_util.h"

namespace doris {

inline Status parse_predicate_lm_stage1_cols_to_column_ids(const std::string& cols,
                                                          const TabletSchemaSPtr& tablet_schema,
                                                          std::vector<ColumnId>* column_ids) {
    column_ids->clear();
    if (cols.empty()) {
        return Status::OK();
    }

    std::vector<std::string> parts = doris::split(cols, ",");
    std::vector<std::string> missing;
    missing.reserve(parts.size());

    for (const auto& part : parts) {
        std::string_view name_sv = doris::trim(std::string_view(part));
        if (name_sv.empty()) {
            continue;
        }

        if (name_sv.size() >= 2 && name_sv.front() == '`' && name_sv.back() == '`') {
            name_sv = name_sv.substr(1, name_sv.size() - 2);
            name_sv = doris::trim(name_sv);
        }
        if (name_sv.empty()) {
            continue;
        }

        std::string name(name_sv);

        int32_t cid = tablet_schema->field_index(name);
        if (cid < 0) {
            cid = tablet_schema->field_index(doris::to_lower(name));
        }

        if (cid < 0) {
            missing.emplace_back(std::move(name));
            continue;
        }

        column_ids->push_back(static_cast<ColumnId>(cid));
    }

    if (!missing.empty()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                "predicate_lm_stage1_cols contains non-existing columns: {}",
                doris::join(missing, ","));
    }

    std::sort(column_ids->begin(), column_ids->end());
    column_ids->erase(std::unique(column_ids->begin(), column_ids->end()), column_ids->end());
    return Status::OK();
}

} // namespace doris
