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

#include "olap/base_tablet.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"

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

Status PartialUpdateInfo::handle_non_strict_mode_not_found_error(
        const TabletSchema& tablet_schema) {
    if (!can_insert_new_rows_in_partial_update) {
        std::string error_column;
        for (auto cid : missing_cids) {
            const TabletColumn& col = tablet_schema.column(cid);
            if (!col.has_default_value() && !col.is_nullable() &&
                !(tablet_schema.auto_increment_column() == col.name())) {
                error_column = col.name();
                break;
            }
        }
        return Status::Error<ErrorCode::INVALID_SCHEMA, false>(
                "the unmentioned column `{}` should have default value or be nullable "
                "for "
                "newly inserted rows in non-strict mode partial update",
                error_column);
    }
    return Status::OK();
}

void PartialUpdateInfo::_generate_default_values_for_missing_cids(
        const TabletSchema& tablet_schema) {
    for (unsigned int cur_cid : missing_cids) {
        const auto& column = tablet_schema.column(cur_cid);
        if (column.has_default_value()) {
            std::string default_value;
            if (UNLIKELY(column.type() == FieldType::OLAP_FIELD_TYPE_DATETIMEV2 &&
                         to_lower(column.default_value()).find(to_lower("CURRENT_TIMESTAMP")) !=
                                 std::string::npos)) {
                DateV2Value<DateTimeV2ValueType> dtv;
                dtv.from_unixtime(timestamp_ms / 1000, timezone);
                default_value = dtv.debug_string();
            } else if (UNLIKELY(column.type() == FieldType::OLAP_FIELD_TYPE_DATEV2 &&
                                to_lower(column.default_value()).find(to_lower("CURRENT_DATE")) !=
                                        std::string::npos)) {
                DateV2Value<DateV2ValueType> dv;
                dv.from_unixtime(timestamp_ms / 1000, timezone);
                default_value = dv.debug_string();
            } else {
                default_value = column.default_value();
            }
            default_values.emplace_back(default_value);
        } else {
            // place an empty string here
            default_values.emplace_back();
        }
    }
    CHECK_EQ(missing_cids.size(), default_values.size());
}

void PartialUpdateReadPlan::prepare_to_read(const RowLocation& row_location, size_t pos) {
    plan[row_location.rowset_id][row_location.segment_id].emplace_back(row_location.row_id, pos);
}

// read columns by read plan
// read_index: ori_pos-> block_idx
Status PartialUpdateReadPlan::read_columns_by_plan(
        const TabletSchema& tablet_schema, const std::vector<uint32_t> cids_to_read,
        const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset, vectorized::Block& block,
        std::map<uint32_t, uint32_t>* read_index, const signed char* __restrict skip_map) const {
    bool has_row_column = tablet_schema.has_row_store_for_all_columns();
    auto mutable_columns = block.mutate_columns();
    size_t read_idx = 0;
    for (const auto& [rowset_id, segment_row_mappings] : plan) {
        for (const auto& [segment_id, mappings] : segment_row_mappings) {
            auto rowset_iter = rsid_to_rowset.find(rowset_id);
            CHECK(rowset_iter != rsid_to_rowset.end());
            std::vector<uint32_t> rids;
            for (auto [rid, pos] : mappings) {
                if (skip_map && skip_map[pos]) {
                    continue;
                }
                rids.emplace_back(rid);
                (*read_index)[pos] = read_idx++;
            }
            if (has_row_column) {
                auto st = doris::BaseTablet::fetch_value_through_row_column(
                        rowset_iter->second, tablet_schema, segment_id, rids, cids_to_read, block);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to fetch value through row column";
                    return st;
                }
                continue;
            }
            for (size_t cid = 0; cid < mutable_columns.size(); ++cid) {
                TabletColumn tablet_column = tablet_schema.column(cids_to_read[cid]);
                auto st = doris::BaseTablet::fetch_value_by_rowids(
                        rowset_iter->second, segment_id, rids, tablet_column, mutable_columns[cid]);
                // set read value to output block
                if (!st.ok()) {
                    LOG(WARNING) << "failed to fetch value";
                    return st;
                }
            }
        }
    }
    block.set_columns(std::move(mutable_columns));
    return Status::OK();
}

Status PartialUpdateReadPlan::fill_missing_columns(
        RowsetWriterContext* rowset_ctx, const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
        const TabletSchema& tablet_schema, vectorized::Block& full_block,
        const std::vector<bool>& use_default_or_null_flag, bool has_default_or_nullable,
        const size_t& segment_start_pos, const vectorized::Block* block) const {
    auto mutable_full_columns = full_block.mutate_columns();
    // create old value columns
    const auto& missing_cids = rowset_ctx->partial_update_info->missing_cids;
    auto old_value_block = tablet_schema.create_block_by_cids(missing_cids);
    CHECK_EQ(missing_cids.size(), old_value_block.columns());

    // record real pos, key is input line num, value is old_block line num
    std::map<uint32_t, uint32_t> read_index;
    RETURN_IF_ERROR(read_columns_by_plan(tablet_schema, missing_cids, rsid_to_rowset,
                                         old_value_block, &read_index, nullptr));

    const auto* delete_sign_column_data = BaseTablet::get_delete_sign_column_data(old_value_block);

    // build default value columns
    auto default_value_block = old_value_block.clone_empty();
    if (has_default_or_nullable || delete_sign_column_data != nullptr) {
        RETURN_IF_ERROR(BaseTablet::generate_default_value_block(
                tablet_schema, missing_cids, rowset_ctx->partial_update_info->default_values,
                old_value_block, default_value_block));
    }
    auto mutable_default_value_columns = default_value_block.mutate_columns();

    // fill all missing value from mutable_old_columns, need to consider default value and null value
    for (auto idx = 0; idx < use_default_or_null_flag.size(); idx++) {
        // `use_default_or_null_flag[idx] == false` doesn't mean that we should read values from the old row
        // for the missing columns. For example, if a table has sequence column, the rows with DELETE_SIGN column
        // marked will not be marked in delete bitmap(see https://github.com/apache/doris/pull/24011), so it will
        // be found in Tablet::lookup_row_key() and `use_default_or_null_flag[idx]` will be false. But we should not
        // read values from old rows for missing values in this occasion. So we should read the DELETE_SIGN column
        // to check if a row REALLY exists in the table.
        auto pos_in_old_block = read_index[idx + segment_start_pos];
        if (use_default_or_null_flag[idx] || (delete_sign_column_data != nullptr &&
                                              delete_sign_column_data[pos_in_old_block] != 0)) {
            for (auto i = 0; i < missing_cids.size(); ++i) {
                // if the column has default value, fill it with default value
                // otherwise, if the column is nullable, fill it with null value
                const auto& tablet_column = tablet_schema.column(missing_cids[i]);
                auto& missing_col = mutable_full_columns[missing_cids[i]];
                // clang-format off
                if (tablet_column.has_default_value()) {
                    missing_col->insert_from(*mutable_default_value_columns[i].get(), 0);
                } else if (tablet_column.is_nullable()) {
                    auto* nullable_column =
                            assert_cast<vectorized::ColumnNullable*, TypeCheckOnRelease::DISABLE>(missing_col.get());
                    nullable_column->insert_null_elements(1);
                } else if (tablet_schema.auto_increment_column() == tablet_column.name()) {
                    const auto& column =
                            *DORIS_TRY(rowset_ctx->tablet_schema->column(tablet_column.name()));
                    DCHECK(column.type() == FieldType::OLAP_FIELD_TYPE_BIGINT);
                    auto* auto_inc_column =
                            assert_cast<vectorized::ColumnInt64*, TypeCheckOnRelease::DISABLE>(missing_col.get());
                    auto_inc_column->insert(
                            (assert_cast<const vectorized::ColumnInt64*, TypeCheckOnRelease::DISABLE>(
                                     block->get_by_name("__PARTIAL_UPDATE_AUTO_INC_COLUMN__").column.get()))->get_element(idx));
                } else {
                    // If the control flow reaches this branch, the column neither has default value
                    // nor is nullable. It means that the row's delete sign is marked, and the value
                    // columns are useless and won't be read. So we can just put arbitary values in the cells
                    missing_col->insert_default();
                }
                // clang-format on
            }
            continue;
        }
        for (auto i = 0; i < missing_cids.size(); ++i) {
            mutable_full_columns[missing_cids[i]]->insert_from(
                    *old_value_block.get_columns_with_type_and_name()[i].column.get(),
                    pos_in_old_block);
        }
    }
    return Status::OK();
}

} // namespace doris
