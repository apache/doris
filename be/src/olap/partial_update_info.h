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

#include "olap/tablet_schema.h"

namespace doris {

struct PartialUpdateInfo {
    void init(const TabletSchema& tablet_schema, bool partial_update,
              const std::set<string>& partial_update_cols, bool is_strict_mode) {
        is_partial_update = partial_update;
        partial_update_input_columns = partial_update_cols;
        missing_cids.clear();
        update_cids.clear();
        for (auto i = 0; i < tablet_schema.num_columns(); ++i) {
            auto tablet_column = tablet_schema.column(i);
            if (!partial_update_input_columns.contains(tablet_column.name())) {
                missing_cids.emplace_back(i);
                if (!tablet_column.has_default_value() && !tablet_column.is_nullable()) {
                    can_insert_new_rows_in_partial_update = false;
                }
            } else {
                update_cids.emplace_back(i);
            }
        }
        this->is_strict_mode = is_strict_mode;
    }

    bool is_partial_update {false};
    std::set<std::string> partial_update_input_columns;
    std::vector<uint32_t> missing_cids;
    std::vector<uint32_t> update_cids;
    // if key not exist in old rowset, use default value or null value for the unmentioned cols
    // to generate a new row, only available in non-strict mode
    bool can_insert_new_rows_in_partial_update {true};
    bool is_strict_mode {false};
};
} // namespace doris
