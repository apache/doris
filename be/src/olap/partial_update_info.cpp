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

#include "olap/partial_update_info.h"

#include <gen_cpp/olap_file.pb.h>

#include "olap/tablet_schema.h"

namespace doris {

void PartialUpdateInfo::init(const TabletSchema& tablet_schema, bool partial_update,
                             const std::set<string>& partial_update_cols, bool is_strict_mode,
                             int64_t timestamp_ms, const std::string& timezone,
                             const std::string& auto_increment_column, int64_t cur_max_version) {
    is_partial_update = partial_update;
    partial_update_input_columns = partial_update_cols;
    max_version_in_flush_phase = cur_max_version;
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

void PartialUpdateInfo::to_pb(PartialUpdateInfoPB* partial_update_info_pb) const {
    partial_update_info_pb->set_is_partial_update(is_partial_update);
    partial_update_info_pb->set_max_version_in_flush_phase(max_version_in_flush_phase);
    for (const auto& col : partial_update_input_columns) {
        partial_update_info_pb->add_partial_update_input_columns(col);
    }
    for (auto cid : missing_cids) {
        partial_update_info_pb->add_missing_cids(cid);
    }
    for (auto cid : update_cids) {
        partial_update_info_pb->add_update_cids(cid);
    }
    partial_update_info_pb->set_can_insert_new_rows_in_partial_update(
            can_insert_new_rows_in_partial_update);
    partial_update_info_pb->set_is_strict_mode(is_strict_mode);
    partial_update_info_pb->set_timestamp_ms(timestamp_ms);
    partial_update_info_pb->set_timezone(timezone);
    partial_update_info_pb->set_is_input_columns_contains_auto_inc_column(
            is_input_columns_contains_auto_inc_column);
    partial_update_info_pb->set_is_schema_contains_auto_inc_column(
            is_schema_contains_auto_inc_column);
    for (const auto& value : default_values) {
        partial_update_info_pb->add_default_values(value);
    }
}

void PartialUpdateInfo::from_pb(PartialUpdateInfoPB* partial_update_info_pb) {
    is_partial_update = partial_update_info_pb->is_partial_update();
    max_version_in_flush_phase = partial_update_info_pb->has_max_version_in_flush_phase()
                                         ? partial_update_info_pb->max_version_in_flush_phase()
                                         : -1;
    partial_update_input_columns.clear();
    for (const auto& col : partial_update_info_pb->partial_update_input_columns()) {
        partial_update_input_columns.insert(col);
    }
    missing_cids.clear();
    for (auto cid : partial_update_info_pb->missing_cids()) {
        missing_cids.push_back(cid);
    }
    update_cids.clear();
    for (auto cid : partial_update_info_pb->update_cids()) {
        update_cids.push_back(cid);
    }
    can_insert_new_rows_in_partial_update =
            partial_update_info_pb->can_insert_new_rows_in_partial_update();
    is_strict_mode = partial_update_info_pb->is_strict_mode();
    timestamp_ms = partial_update_info_pb->timestamp_ms();
    timezone = partial_update_info_pb->timezone();
    is_input_columns_contains_auto_inc_column =
            partial_update_info_pb->is_input_columns_contains_auto_inc_column();
    is_schema_contains_auto_inc_column =
            partial_update_info_pb->is_schema_contains_auto_inc_column();
    default_values.clear();
    for (const auto& value : partial_update_info_pb->default_values()) {
        default_values.push_back(value);
    }
}

std::string PartialUpdateInfo::summary() const {
    return fmt::format(
            "update_cids={}, missing_cids={}, is_strict_mode={}, max_version_in_flush_phase={}",
            update_cids.size(), missing_cids.size(), is_strict_mode, max_version_in_flush_phase);
}

void PartialUpdateInfo::_generate_default_values_for_missing_cids(
        const TabletSchema& tablet_schema) {
    for (unsigned int cur_cid : missing_cids) {
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
} // namespace doris
