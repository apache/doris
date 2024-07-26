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
              const std::set<string>& partial_update_cols, bool is_strict_mode,
              int64_t timestamp_ms, const std::string& timezone,
              const std::string& auto_increment_column) {
        is_partial_update = partial_update;
        partial_update_input_columns = partial_update_cols;

        this->timestamp_ms = timestamp_ms;
        this->timezone = timezone;
        missing_cids.clear();
        update_cids.clear();
        for (auto i = 0; i < tablet_schema.num_columns(); ++i) {
            auto tablet_column = tablet_schema.column(i);
            if (!partial_update_input_columns.contains(tablet_column.name())) {
                missing_cids.emplace_back(i);
                if (!tablet_column.has_default_value() && !tablet_column.is_nullable() &&
                    tablet_schema.auto_increment_column() != tablet_column.name()) {
                    can_insert_new_rows_in_partial_update = false;
                }
            } else {
                update_cids.emplace_back(i);
            }
            if (auto_increment_column == tablet_column.name()) {
                is_schema_contains_auto_inc_column = true;
            }
        }
        this->is_strict_mode = is_strict_mode;
        is_input_columns_contains_auto_inc_column =
                is_partial_update && partial_update_input_columns.contains(auto_increment_column);
        _generate_default_values_for_missing_cids(tablet_schema);
    }

private:
    void _generate_default_values_for_missing_cids(const TabletSchema& tablet_schema) {
        for (auto i = 0; i < missing_cids.size(); ++i) {
            auto cur_cid = missing_cids[i];
            const auto& column = tablet_schema.column(cur_cid);
            if (column.has_default_value()) {
                std::string default_value;
                if (UNLIKELY(tablet_schema.column(cur_cid).type() ==
                                     FieldType::OLAP_FIELD_TYPE_DATETIMEV2 &&
                             to_lower(tablet_schema.column(cur_cid).default_value())
                                             .find(to_lower("CURRENT_TIMESTAMP")) !=
                                     std::string::npos)) {
                    DateV2Value<DateTimeV2ValueType> dtv;
                    dtv.from_unixtime(timestamp_ms / 1000, timezone);
                    default_value = dtv.debug_string();
                } else if (UNLIKELY(tablet_schema.column(cur_cid).type() ==
                                            FieldType::OLAP_FIELD_TYPE_DATEV2 &&
                                    to_lower(tablet_schema.column(cur_cid).default_value())
                                                    .find(to_lower("CURRENT_DATE")) !=
                                            std::string::npos)) {
                    DateV2Value<DateV2ValueType> dv;
                    dv.from_unixtime(timestamp_ms / 1000, timezone);
                    default_value = dv.debug_string();
                } else {
                    default_value = tablet_schema.column(cur_cid).default_value();
                }
                default_values.emplace_back(default_value);
            } else {
                // place an empty string here
                default_values.emplace_back();
            }
        }
        CHECK_EQ(missing_cids.size(), default_values.size());
    }

public:
    bool is_partial_update {false};
    std::set<std::string> partial_update_input_columns;
    std::vector<uint32_t> missing_cids;
    std::vector<uint32_t> update_cids;
    // if key not exist in old rowset, use default value or null value for the unmentioned cols
    // to generate a new row, only available in non-strict mode
    bool can_insert_new_rows_in_partial_update {true};
    bool is_strict_mode {false};
    int64_t timestamp_ms {0};
    std::string timezone;
    bool is_input_columns_contains_auto_inc_column = false;
    bool is_schema_contains_auto_inc_column = false;

    // default values for missing cids
    std::vector<std::string> default_values;
};
} // namespace doris
