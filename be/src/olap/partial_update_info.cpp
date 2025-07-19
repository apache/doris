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

#include <cstdint>

#include "common/consts.h"
#include "common/logging.h"
#include "olap/base_tablet.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/vertical_segment_writer.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "util/bitmap_value.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h" // IWYU pragma: keep
#include "vec/olap/olap_data_convertor.h"

namespace doris {
#include "common/compile_check_begin.h"
Status PartialUpdateInfo::init(int64_t tablet_id, int64_t txn_id, const TabletSchema& tablet_schema,
                               UniqueKeyUpdateModePB unique_key_update_mode,
                               PartialUpdateNewRowPolicyPB policy,
                               const std::set<std::string>& partial_update_cols,
                               bool is_strict_mode_, int64_t timestamp_ms_, int32_t nano_seconds_,
                               const std::string& timezone_,
                               const std::string& auto_increment_column,
                               int32_t sequence_map_col_uid, int64_t cur_max_version) {
    partial_update_mode = unique_key_update_mode;
    partial_update_new_key_policy = policy;
    partial_update_input_columns = partial_update_cols;
    max_version_in_flush_phase = cur_max_version;
    sequence_map_col_unqiue_id = sequence_map_col_uid;
    timestamp_ms = timestamp_ms_;
    nano_seconds = nano_seconds_;
    timezone = timezone_;
    missing_cids.clear();
    update_cids.clear();

    if (partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS) {
        // partial_update_cols should include all key columns
        for (std::size_t i {0}; i < tablet_schema.num_key_columns(); i++) {
            const auto key_col = tablet_schema.column(i);
            if (!partial_update_cols.contains(key_col.name())) {
                auto msg = fmt::format(
                        "Unable to do partial update on shadow index's tablet, tablet_id={}, "
                        "txn_id={}. Missing key column {}.",
                        tablet_id, txn_id, key_col.name());
                LOG_WARNING(msg);
                return Status::Aborted<false>(msg);
            }
        }
    }

    for (auto i = 0; i < tablet_schema.num_columns(); ++i) {
        if (partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS) {
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
        } else {
            // in flexible partial update, missing cids is all non sort keys' cid
            if (i >= tablet_schema.num_key_columns()) {
                missing_cids.emplace_back(i);
            }
        }
    }
    is_strict_mode = is_strict_mode_;
    is_input_columns_contains_auto_inc_column =
            is_fixed_partial_update() &&
            partial_update_input_columns.contains(auto_increment_column);
    _generate_default_values_for_missing_cids(tablet_schema);
    return Status::OK();
}

void PartialUpdateInfo::to_pb(PartialUpdateInfoPB* partial_update_info_pb) const {
    partial_update_info_pb->set_partial_update_mode(partial_update_mode);
    partial_update_info_pb->set_partial_update_new_key_policy(partial_update_new_key_policy);
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
    partial_update_info_pb->set_nano_seconds(nano_seconds);
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
    if (!partial_update_info_pb->has_partial_update_mode()) {
        // for backward compatibility
        if (partial_update_info_pb->is_partial_update()) {
            partial_update_mode = UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS;
        } else {
            partial_update_mode = UniqueKeyUpdateModePB::UPSERT;
        }
    } else {
        partial_update_mode = partial_update_info_pb->partial_update_mode();
    }
    if (partial_update_info_pb->has_partial_update_new_key_policy()) {
        partial_update_new_key_policy = partial_update_info_pb->partial_update_new_key_policy();
    }
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
    if (partial_update_info_pb->has_nano_seconds()) {
        nano_seconds = partial_update_info_pb->nano_seconds();
    }
    default_values.clear();
    for (const auto& value : partial_update_info_pb->default_values()) {
        default_values.push_back(value);
    }
}

std::string PartialUpdateInfo::summary() const {
    std::string mode;
    switch (partial_update_mode) {
    case UniqueKeyUpdateModePB::UPSERT:
        mode = "upsert";
        break;
    case UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS:
        mode = "fixed partial update";
        break;
    case UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS:
        mode = "flexible partial update";
        break;
    }
    return fmt::format(
            "mode={}, update_cids={}, missing_cids={}, is_strict_mode={}, "
            "max_version_in_flush_phase={}",
            mode, update_cids.size(), missing_cids.size(), is_strict_mode,
            max_version_in_flush_phase);
}

Status PartialUpdateInfo::handle_new_key(const TabletSchema& tablet_schema,
                                         const std::function<std::string()>& line,
                                         BitmapValue* skip_bitmap) {
    switch (partial_update_new_key_policy) {
    case doris::PartialUpdateNewRowPolicyPB::APPEND: {
        if (partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS) {
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
                        "for newly inserted rows in non-strict mode partial update",
                        error_column);
            }
        } else if (partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS) {
            DCHECK(skip_bitmap != nullptr);
            bool can_insert_new_row {true};
            std::string error_column;
            for (auto cid : missing_cids) {
                const TabletColumn& col = tablet_schema.column(cid);
                if (skip_bitmap->contains(col.unique_id()) && !col.has_default_value() &&
                    !col.is_nullable() && !col.is_auto_increment()) {
                    error_column = col.name();
                    can_insert_new_row = false;
                    break;
                }
            }
            if (!can_insert_new_row) {
                return Status::Error<ErrorCode::INVALID_SCHEMA, false>(
                        "the unmentioned column `{}` should have default value or be "
                        "nullable for newly inserted rows in non-strict mode flexible partial "
                        "update",
                        error_column);
            }
        }
    } break;
    case doris::PartialUpdateNewRowPolicyPB::ERROR: {
        return Status::Error<ErrorCode::NEW_ROWS_IN_PARTIAL_UPDATE, false>(
                "Can't append new rows in partial update when partial_update_new_key_behavior is "
                "ERROR. Row with key=[{}] is not in table.",
                line());
    } break;
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
                auto pos = to_lower(column.default_value()).find('(');
                if (pos == std::string::npos) {
                    DateV2Value<DateTimeV2ValueType> dtv;
                    dtv.from_unixtime(timestamp_ms / 1000, timezone);
                    default_value = dtv.debug_string();
                } else {
                    int precision = std::stoi(column.default_value().substr(pos + 1));
                    DateV2Value<DateTimeV2ValueType> dtv;
                    dtv.from_unixtime(timestamp_ms / 1000, nano_seconds, timezone, precision);
                    default_value = dtv.debug_string();
                }
            } else if (UNLIKELY(column.type() == FieldType::OLAP_FIELD_TYPE_DATEV2 &&
                                to_lower(column.default_value()).find(to_lower("CURRENT_DATE")) !=
                                        std::string::npos)) {
                DateV2Value<DateV2ValueType> dv;
                dv.from_unixtime(timestamp_ms / 1000, timezone);
                default_value = dv.debug_string();
            } else if (UNLIKELY(column.type() == FieldType::OLAP_FIELD_TYPE_BITMAP &&
                                to_lower(column.default_value()).find(to_lower("BITMAP_EMPTY")) !=
                                        std::string::npos)) {
                BitmapValue v = BitmapValue {};
                default_value = v.to_string();
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

bool FixedReadPlan::empty() const {
    return plan.empty();
}

void FixedReadPlan::prepare_to_read(const RowLocation& row_location, size_t pos) {
    plan[row_location.rowset_id][row_location.segment_id].emplace_back(row_location.row_id, pos);
}

// read columns by read plan
// read_index: ori_pos-> block_idx
Status FixedReadPlan::read_columns_by_plan(
        const TabletSchema& tablet_schema, std::vector<uint32_t> cids_to_read,
        const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset, vectorized::Block& block,
        std::map<uint32_t, uint32_t>* read_index, bool force_read_old_delete_signs,
        const signed char* __restrict cur_delete_signs) const {
    if (force_read_old_delete_signs) {
        // always read delete sign column from historical data
        if (const vectorized::ColumnWithTypeAndName* old_delete_sign_column =
                    block.try_get_by_name(DELETE_SIGN);
            old_delete_sign_column == nullptr) {
            auto del_col_cid = tablet_schema.field_index(DELETE_SIGN);
            cids_to_read.emplace_back(del_col_cid);
            block.swap(tablet_schema.create_block_by_cids(cids_to_read));
        }
    }
    bool has_row_column = tablet_schema.has_row_store_for_all_columns();
    auto mutable_columns = block.mutate_columns();
    uint32_t read_idx = 0;
    for (const auto& [rowset_id, segment_row_mappings] : plan) {
        for (const auto& [segment_id, mappings] : segment_row_mappings) {
            auto rowset_iter = rsid_to_rowset.find(rowset_id);
            CHECK(rowset_iter != rsid_to_rowset.end());
            std::vector<uint32_t> rids;
            for (auto [rid, pos] : mappings) {
                if (cur_delete_signs && cur_delete_signs[pos]) {
                    continue;
                }
                rids.emplace_back(rid);
                (*read_index)[static_cast<uint32_t>(pos)] = read_idx++;
            }
            if (has_row_column) {
                auto st = BaseTablet::fetch_value_through_row_column(
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

Status FixedReadPlan::fill_missing_columns(
        RowsetWriterContext* rowset_ctx, const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
        const TabletSchema& tablet_schema, vectorized::Block& full_block,
        const std::vector<bool>& use_default_or_null_flag, bool has_default_or_nullable,
        uint32_t segment_start_pos, const vectorized::Block* block) const {
    auto mutable_full_columns = full_block.mutate_columns();
    // create old value columns
    const auto& missing_cids = rowset_ctx->partial_update_info->missing_cids;
    bool have_input_seq_column = false;
    if (tablet_schema.has_sequence_col()) {
        const std::vector<uint32_t>& including_cids = rowset_ctx->partial_update_info->update_cids;
        have_input_seq_column =
                (std::find(including_cids.cbegin(), including_cids.cend(),
                           tablet_schema.sequence_col_idx()) != including_cids.cend());
    }

    auto old_value_block = tablet_schema.create_block_by_cids(missing_cids);
    CHECK_EQ(missing_cids.size(), old_value_block.columns());

    // segment pos to write -> rowid to read in old_value_block
    std::map<uint32_t, uint32_t> read_index;
    RETURN_IF_ERROR(read_columns_by_plan(tablet_schema, missing_cids, rsid_to_rowset,
                                         old_value_block, &read_index, true, nullptr));

    const auto* old_delete_signs = BaseTablet::get_delete_sign_column_data(old_value_block);
    DCHECK(old_delete_signs != nullptr);
    // build default value columns
    auto default_value_block = old_value_block.clone_empty();
    RETURN_IF_ERROR(BaseTablet::generate_default_value_block(
            tablet_schema, missing_cids, rowset_ctx->partial_update_info->default_values,
            old_value_block, default_value_block));
    auto mutable_default_value_columns = default_value_block.mutate_columns();

    // fill all missing value from mutable_old_columns, need to consider default value and null value
    for (auto idx = 0; idx < use_default_or_null_flag.size(); idx++) {
        auto segment_pos = idx + segment_start_pos;
        auto pos_in_old_block = read_index[segment_pos];

        for (auto i = 0; i < missing_cids.size(); ++i) {
            // if the column has default value, fill it with default value
            // otherwise, if the column is nullable, fill it with null value
            const auto& tablet_column = tablet_schema.column(missing_cids[i]);
            auto& missing_col = mutable_full_columns[missing_cids[i]];

            bool should_use_default = use_default_or_null_flag[idx];
            if (!should_use_default) {
                bool old_row_delete_sign =
                        (old_delete_signs != nullptr && old_delete_signs[pos_in_old_block] != 0);
                if (old_row_delete_sign) {
                    if (!tablet_schema.has_sequence_col()) {
                        should_use_default = true;
                    } else if (have_input_seq_column || (!tablet_column.is_seqeunce_col())) {
                        // to keep the sequence column value not decreasing, we should read values of seq column
                        // from old rows even if the old row is deleted when the input don't specify the sequence column, otherwise
                        // it may cause the merge-on-read based compaction to produce incorrect results
                        should_use_default = true;
                    }
                }
            }

            if (should_use_default) {
                // clang-format off
                if (tablet_column.has_default_value()) {
                    missing_col->insert_from(*mutable_default_value_columns[i], 0);
                } else if (tablet_column.is_nullable()) {
                    auto* nullable_column = assert_cast<vectorized::ColumnNullable*, TypeCheckOnRelease::DISABLE>(missing_col.get());
                    nullable_column->insert_many_defaults(1);
                } else if (tablet_schema.auto_increment_column() == tablet_column.name()) {
                    const auto& column = *DORIS_TRY(rowset_ctx->tablet_schema->column(tablet_column.name()));
                    DCHECK(column.type() == FieldType::OLAP_FIELD_TYPE_BIGINT);
                    auto* auto_inc_column =
                            assert_cast<vectorized::ColumnInt64*, TypeCheckOnRelease::DISABLE>(missing_col.get());
                    auto_inc_column->insert(vectorized::Field::create_field<TYPE_BIGINT>(
assert_cast<const vectorized::ColumnInt64*, TypeCheckOnRelease::DISABLE>(
block->get_by_name(BeConsts::PARTIAL_UPDATE_AUTO_INC_COL).column.get())->get_element(idx)));
                } else {
                    // If the control flow reaches this branch, the column neither has default value
                    // nor is nullable. It means that the row's delete sign is marked, and the value
                    // columns are useless and won't be read. So we can just put arbitary values in the cells
                    missing_col->insert(tablet_column.get_vec_type()->get_default());
                }
                // clang-format on
            } else {
                missing_col->insert_from(*old_value_block.get_by_position(i).column,
                                         pos_in_old_block);
            }
        }
    }
    full_block.set_columns(std::move(mutable_full_columns));
    return Status::OK();
}

void FlexibleReadPlan::prepare_to_read(const RowLocation& row_location, size_t pos,
                                       const BitmapValue& skip_bitmap) {
    if (!use_row_store) {
        for (uint64_t col_uid : skip_bitmap) {
            plan[row_location.rowset_id][row_location.segment_id][static_cast<uint32_t>(col_uid)]
                    .emplace_back(row_location.row_id, pos);
        }
    } else {
        row_store_plan[row_location.rowset_id][row_location.segment_id].emplace_back(
                row_location.row_id, pos);
    }
}

Status FlexibleReadPlan::read_columns_by_plan(
        const TabletSchema& tablet_schema,
        const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
        vectorized::Block& old_value_block,
        std::map<uint32_t, std::map<uint32_t, uint32_t>>* read_index) const {
    auto mutable_columns = old_value_block.mutate_columns();

    // cid -> next rid to fill in block
    std::map<uint32_t, uint32_t> next_read_idx;
    for (uint32_t cid {0}; cid < tablet_schema.num_columns(); cid++) {
        next_read_idx[cid] = 0;
    }

    for (const auto& [rowset_id, segment_mappings] : plan) {
        for (const auto& [segment_id, uid_mappings] : segment_mappings) {
            for (const auto& [col_uid, mappings] : uid_mappings) {
                auto rowset_iter = rsid_to_rowset.find(rowset_id);
                CHECK(rowset_iter != rsid_to_rowset.end());
                auto cid = tablet_schema.field_index(col_uid);
                DCHECK_NE(cid, -1);
                DCHECK_GE(cid, tablet_schema.num_key_columns());
                std::vector<uint32_t> rids;
                for (auto [rid, pos] : mappings) {
                    rids.emplace_back(rid);
                    (*read_index)[cid][static_cast<uint32_t>(pos)] = next_read_idx[cid]++;
                }

                TabletColumn tablet_column = tablet_schema.column(cid);
                auto idx = cid - tablet_schema.num_key_columns();
                RETURN_IF_ERROR(doris::BaseTablet::fetch_value_by_rowids(
                        rowset_iter->second, segment_id, rids, tablet_column,
                        mutable_columns[idx]));
            }
        }
    }
    // !!!ATTENTION!!!: columns in block may have different size because every row has different columns to update
    old_value_block.set_columns(std::move(mutable_columns));
    return Status::OK();
}

Status FlexibleReadPlan::read_columns_by_plan(
        const TabletSchema& tablet_schema, const std::vector<uint32_t>& cids_to_read,
        const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
        vectorized::Block& old_value_block, std::map<uint32_t, uint32_t>* read_index) const {
    DCHECK(use_row_store);
    uint32_t read_idx = 0;
    for (const auto& [rowset_id, segment_row_mappings] : row_store_plan) {
        for (const auto& [segment_id, mappings] : segment_row_mappings) {
            auto rowset_iter = rsid_to_rowset.find(rowset_id);
            CHECK(rowset_iter != rsid_to_rowset.end());
            std::vector<uint32_t> rids;
            for (auto [rid, pos] : mappings) {
                rids.emplace_back(rid);
                (*read_index)[static_cast<uint32_t>(pos)] = read_idx++;
            }
            auto st = BaseTablet::fetch_value_through_row_column(rowset_iter->second, tablet_schema,
                                                                 segment_id, rids, cids_to_read,
                                                                 old_value_block);
            if (!st.ok()) {
                LOG(WARNING) << "failed to fetch value through row column";
                return st;
            }
        }
    }
    return Status::OK();
}

Status FlexibleReadPlan::fill_non_primary_key_columns(
        RowsetWriterContext* rowset_ctx, const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
        const TabletSchema& tablet_schema, vectorized::Block& full_block,
        const std::vector<bool>& use_default_or_null_flag, bool has_default_or_nullable,
        uint32_t segment_start_pos, uint32_t block_start_pos, const vectorized::Block* block,
        std::vector<BitmapValue>* skip_bitmaps) const {
    auto mutable_full_columns = full_block.mutate_columns();

    // missing_cids are all non sort key columns' cids
    const auto& non_sort_key_cids = rowset_ctx->partial_update_info->missing_cids;
    auto old_value_block = tablet_schema.create_block_by_cids(non_sort_key_cids);
    CHECK_EQ(non_sort_key_cids.size(), old_value_block.columns());

    if (!use_row_store) {
        RETURN_IF_ERROR(fill_non_primary_key_columns_for_column_store(
                rowset_ctx, rsid_to_rowset, tablet_schema, non_sort_key_cids, old_value_block,
                mutable_full_columns, use_default_or_null_flag, has_default_or_nullable,
                segment_start_pos, block_start_pos, block, skip_bitmaps));
    } else {
        RETURN_IF_ERROR(fill_non_primary_key_columns_for_row_store(
                rowset_ctx, rsid_to_rowset, tablet_schema, non_sort_key_cids, old_value_block,
                mutable_full_columns, use_default_or_null_flag, has_default_or_nullable,
                segment_start_pos, block_start_pos, block, skip_bitmaps));
    }
    full_block.set_columns(std::move(mutable_full_columns));
    return Status::OK();
}

Status FlexibleReadPlan::fill_non_primary_key_columns_for_column_store(
        RowsetWriterContext* rowset_ctx, const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
        const TabletSchema& tablet_schema, const std::vector<uint32_t>& non_sort_key_cids,
        vectorized::Block& old_value_block, vectorized::MutableColumns& mutable_full_columns,
        const std::vector<bool>& use_default_or_null_flag, bool has_default_or_nullable,
        uint32_t segment_start_pos, uint32_t block_start_pos, const vectorized::Block* block,
        std::vector<BitmapValue>* skip_bitmaps) const {
    auto* info = rowset_ctx->partial_update_info.get();
    int32_t seq_col_unique_id = -1;
    if (tablet_schema.has_sequence_col()) {
        seq_col_unique_id = tablet_schema.column(tablet_schema.sequence_col_idx()).unique_id();
    }
    // cid -> segment pos to write -> rowid to read in old_value_block
    std::map<uint32_t, std::map<uint32_t, uint32_t>> read_index;
    RETURN_IF_ERROR(
            read_columns_by_plan(tablet_schema, rsid_to_rowset, old_value_block, &read_index));
    // !!!ATTENTION!!!: columns in old_value_block may have different size because every row has different columns to update

    const auto* delete_sign_column_data = BaseTablet::get_delete_sign_column_data(old_value_block);
    // build default value columns
    auto default_value_block = old_value_block.clone_empty();
    if (has_default_or_nullable || delete_sign_column_data != nullptr) {
        RETURN_IF_ERROR(BaseTablet::generate_default_value_block(
                tablet_schema, non_sort_key_cids, info->default_values, old_value_block,
                default_value_block));
    }

    auto fill_one_cell = [&tablet_schema, &read_index, info](
                                 const TabletColumn& tablet_column, uint32_t cid,
                                 vectorized::MutableColumnPtr& new_col,
                                 const vectorized::IColumn& default_value_col,
                                 const vectorized::IColumn& old_value_col,
                                 const vectorized::IColumn& cur_col, std::size_t block_pos,
                                 uint32_t segment_pos, bool skipped, bool row_has_sequence_col,
                                 bool use_default, const signed char* delete_sign_column_data) {
        if (skipped) {
            DCHECK(cid != tablet_schema.skip_bitmap_col_idx());
            DCHECK(cid != tablet_schema.version_col_idx());
            DCHECK(!tablet_column.is_row_store_column());

            if (!use_default) {
                if (delete_sign_column_data != nullptr) {
                    bool old_row_delete_sign = false;
                    if (auto it = read_index[tablet_schema.delete_sign_idx()].find(segment_pos);
                        it != read_index[tablet_schema.delete_sign_idx()].end()) {
                        old_row_delete_sign = (delete_sign_column_data[it->second] != 0);
                    }

                    if (old_row_delete_sign) {
                        if (!tablet_schema.has_sequence_col()) {
                            use_default = true;
                        } else if (row_has_sequence_col ||
                                   (!tablet_column.is_seqeunce_col() &&
                                    (tablet_column.unique_id() != info->sequence_map_col_uid()))) {
                            // to keep the sequence column value not decreasing, we should read values of seq column(and seq map column)
                            // from old rows even if the old row is deleted when the input don't specify the sequence column, otherwise
                            // it may cause the merge-on-read based compaction to produce incorrect results
                            use_default = true;
                        }
                    }
                }
            }
            if (!use_default && tablet_column.is_on_update_current_timestamp()) {
                use_default = true;
            }
            if (use_default) {
                if (tablet_column.has_default_value()) {
                    new_col->insert_from(default_value_col, 0);
                } else if (tablet_column.is_nullable()) {
                    assert_cast<vectorized::ColumnNullable*, TypeCheckOnRelease::DISABLE>(
                            new_col.get())
                            ->insert_many_defaults(1);
                } else if (tablet_column.is_auto_increment()) {
                    // In flexible partial update, the skip bitmap indicates whether a cell
                    // is specified in the original load, so the generated auto-increment value is filled
                    // in current block in place if needed rather than using a seperate column to
                    // store the generated auto-increment value in fixed partial update
                    new_col->insert_from(cur_col, block_pos);
                } else {
                    new_col->insert(tablet_column.get_vec_type()->get_default());
                }
            } else {
                auto pos_in_old_block = read_index.at(cid).at(segment_pos);
                new_col->insert_from(old_value_col, pos_in_old_block);
            }
        } else {
            new_col->insert_from(cur_col, block_pos);
        }
    };

    // fill all non sort key columns from mutable_old_columns, need to consider default value and null value
    for (std::size_t i {0}; i < non_sort_key_cids.size(); i++) {
        auto cid = non_sort_key_cids[i];
        const auto& tablet_column = tablet_schema.column(cid);
        auto col_uid = tablet_column.unique_id();
        for (auto idx = 0; idx < use_default_or_null_flag.size(); idx++) {
            auto segment_pos = segment_start_pos + idx;
            auto block_pos = block_start_pos + idx;

            fill_one_cell(tablet_column, cid, mutable_full_columns[cid],
                          *default_value_block.get_by_position(i).column,
                          *old_value_block.get_by_position(i).column,
                          *block->get_by_position(cid).column, block_pos, segment_pos,
                          skip_bitmaps->at(block_pos).contains(col_uid),
                          tablet_schema.has_sequence_col()
                                  ? !skip_bitmaps->at(block_pos).contains(seq_col_unique_id)
                                  : false,
                          use_default_or_null_flag[idx], delete_sign_column_data);
        }
    }
    return Status::OK();
}

Status FlexibleReadPlan::fill_non_primary_key_columns_for_row_store(
        RowsetWriterContext* rowset_ctx, const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
        const TabletSchema& tablet_schema, const std::vector<uint32_t>& non_sort_key_cids,
        vectorized::Block& old_value_block, vectorized::MutableColumns& mutable_full_columns,
        const std::vector<bool>& use_default_or_null_flag, bool has_default_or_nullable,
        uint32_t segment_start_pos, uint32_t block_start_pos, const vectorized::Block* block,
        std::vector<BitmapValue>* skip_bitmaps) const {
    auto* info = rowset_ctx->partial_update_info.get();
    int32_t seq_col_unique_id = -1;
    if (tablet_schema.has_sequence_col()) {
        seq_col_unique_id = tablet_schema.column(tablet_schema.sequence_col_idx()).unique_id();
    }
    // segment pos to write -> rowid to read in old_value_block
    std::map<uint32_t, uint32_t> read_index;
    RETURN_IF_ERROR(read_columns_by_plan(tablet_schema, non_sort_key_cids, rsid_to_rowset,
                                         old_value_block, &read_index));

    const auto* delete_sign_column_data = BaseTablet::get_delete_sign_column_data(old_value_block);
    // build default value columns
    auto default_value_block = old_value_block.clone_empty();
    if (has_default_or_nullable || delete_sign_column_data != nullptr) {
        RETURN_IF_ERROR(BaseTablet::generate_default_value_block(
                tablet_schema, non_sort_key_cids, info->default_values, old_value_block,
                default_value_block));
    }

    auto fill_one_cell = [&tablet_schema, info](const TabletColumn& tablet_column, uint32_t cid,
                                                vectorized::MutableColumnPtr& new_col,
                                                const vectorized::IColumn& default_value_col,
                                                const vectorized::IColumn& old_value_col,
                                                const vectorized::IColumn& cur_col,
                                                std::size_t block_pos, bool skipped,
                                                bool row_has_sequence_col, bool use_default,
                                                const signed char* delete_sign_column_data,
                                                uint32_t pos_in_old_block) {
        if (skipped) {
            DCHECK(cid != tablet_schema.skip_bitmap_col_idx());
            DCHECK(cid != tablet_schema.version_col_idx());
            DCHECK(!tablet_column.is_row_store_column());
            if (!use_default) {
                if (delete_sign_column_data != nullptr) {
                    bool old_row_delete_sign = (delete_sign_column_data[pos_in_old_block] != 0);
                    if (old_row_delete_sign) {
                        if (!tablet_schema.has_sequence_col()) {
                            use_default = true;
                        } else if (row_has_sequence_col ||
                                   (!tablet_column.is_seqeunce_col() &&
                                    (tablet_column.unique_id() != info->sequence_map_col_uid()))) {
                            // to keep the sequence column value not decreasing, we should read values of seq column(and seq map column)
                            // from old rows even if the old row is deleted when the input don't specify the sequence column, otherwise
                            // it may cause the merge-on-read based compaction to produce incorrect results
                            use_default = true;
                        }
                    }
                }
            }

            if (!use_default && tablet_column.is_on_update_current_timestamp()) {
                use_default = true;
            }
            if (use_default) {
                if (tablet_column.has_default_value()) {
                    new_col->insert_from(default_value_col, 0);
                } else if (tablet_column.is_nullable()) {
                    assert_cast<vectorized::ColumnNullable*, TypeCheckOnRelease::DISABLE>(
                            new_col.get())
                            ->insert_many_defaults(1);
                } else if (tablet_column.is_auto_increment()) {
                    // In flexible partial update, the skip bitmap indicates whether a cell
                    // is specified in the original load, so the generated auto-increment value is filled
                    // in current block in place if needed rather than using a seperate column to
                    // store the generated auto-increment value in fixed partial update
                    new_col->insert_from(cur_col, block_pos);
                } else {
                    new_col->insert(tablet_column.get_vec_type()->get_default());
                }
            } else {
                new_col->insert_from(old_value_col, pos_in_old_block);
            }
        } else {
            new_col->insert_from(cur_col, block_pos);
        }
    };

    // fill all non sort key columns from mutable_old_columns, need to consider default value and null value
    for (std::size_t i {0}; i < non_sort_key_cids.size(); i++) {
        auto cid = non_sort_key_cids[i];
        const auto& tablet_column = tablet_schema.column(cid);
        auto col_uid = tablet_column.unique_id();
        for (auto idx = 0; idx < use_default_or_null_flag.size(); idx++) {
            auto segment_pos = segment_start_pos + idx;
            auto block_pos = block_start_pos + idx;
            auto pos_in_old_block = read_index[segment_pos];

            fill_one_cell(tablet_column, cid, mutable_full_columns[cid],
                          *default_value_block.get_by_position(i).column,
                          *old_value_block.get_by_position(i).column,
                          *block->get_by_position(cid).column, block_pos,
                          skip_bitmaps->at(block_pos).contains(col_uid),
                          tablet_schema.has_sequence_col()
                                  ? !skip_bitmaps->at(block_pos).contains(seq_col_unique_id)
                                  : false,
                          use_default_or_null_flag[idx], delete_sign_column_data, pos_in_old_block);
        }
    }
    return Status::OK();
}

BlockAggregator::BlockAggregator(segment_v2::VerticalSegmentWriter& vertical_segment_writer)
        : _writer(vertical_segment_writer), _tablet_schema(*_writer._tablet_schema) {}

void BlockAggregator::merge_one_row(vectorized::MutableBlock& dst_block,
                                    vectorized::Block* src_block, int rid,
                                    BitmapValue& skip_bitmap) {
    for (size_t cid {_tablet_schema.num_key_columns()}; cid < _tablet_schema.num_columns(); cid++) {
        if (cid == _tablet_schema.skip_bitmap_col_idx()) {
            auto& cur_skip_bitmap =
                    assert_cast<vectorized::ColumnBitmap*>(dst_block.mutable_columns()[cid].get())
                            ->get_data()
                            .back();
            const auto& new_row_skip_bitmap =
                    assert_cast<vectorized::ColumnBitmap*>(
                            src_block->get_by_position(cid).column->assume_mutable().get())
                            ->get_data()[rid];
            cur_skip_bitmap &= new_row_skip_bitmap;
            continue;
        }
        if (!skip_bitmap.contains(_tablet_schema.column(cid).unique_id())) {
            dst_block.mutable_columns()[cid]->pop_back(1);
            dst_block.mutable_columns()[cid]->insert_from(*src_block->get_by_position(cid).column,
                                                          rid);
        }
    }
    VLOG_DEBUG << fmt::format("merge a row, after merge, output_block.rows()={}, state: {}",
                              dst_block.rows(), _state.to_string());
}

void BlockAggregator::append_one_row(vectorized::MutableBlock& dst_block,
                                     vectorized::Block* src_block, int rid) {
    dst_block.add_row(src_block, rid);
    _state.rows++;
    VLOG_DEBUG << fmt::format("append a new row, after append, output_block.rows()={}, state: {}",
                              dst_block.rows(), _state.to_string());
}

void BlockAggregator::remove_last_n_rows(vectorized::MutableBlock& dst_block, int n) {
    if (n > 0) {
        for (size_t cid {0}; cid < _tablet_schema.num_columns(); cid++) {
            DCHECK_GE(dst_block.mutable_columns()[cid]->size(), n);
            dst_block.mutable_columns()[cid]->pop_back(n);
        }
    }
}

void BlockAggregator::append_or_merge_row(vectorized::MutableBlock& dst_block,
                                          vectorized::Block* src_block, int rid,
                                          BitmapValue& skip_bitmap, bool have_delete_sign) {
    if (have_delete_sign) {
        // remove all the previous batched rows
        remove_last_n_rows(dst_block, _state.rows);
        _state.rows = 0;
        _state.has_row_with_delete_sign = true;

        append_one_row(dst_block, src_block, rid);
    } else {
        if (_state.should_merge()) {
            merge_one_row(dst_block, src_block, rid, skip_bitmap);
        } else {
            append_one_row(dst_block, src_block, rid);
        }
    }
};

Status BlockAggregator::aggregate_rows(
        vectorized::MutableBlock& output_block, vectorized::Block* block, int start, int end,
        std::string key, std::vector<BitmapValue>* skip_bitmaps, const signed char* delete_signs,
        vectorized::IOlapColumnDataAccessor* seq_column,
        const std::vector<RowsetSharedPtr>& specified_rowsets,
        std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches) {
    VLOG_DEBUG << fmt::format("merge rows in range=[{}-{})", start, end);
    if (end - start == 1) {
        output_block.add_row(block, start);
        VLOG_DEBUG << fmt::format("append a row directly, rid={}", start);
        return Status::OK();
    }

    auto seq_col_unique_id = _tablet_schema.column(_tablet_schema.sequence_col_idx()).unique_id();
    auto delete_sign_col_unique_id =
            _tablet_schema.column(_tablet_schema.delete_sign_idx()).unique_id();

    _state.reset();

    RowLocation loc;
    RowsetSharedPtr rowset;
    std::string previous_encoded_seq_value {};
    Status st = _writer._tablet->lookup_row_key(
            key, &_tablet_schema, false, specified_rowsets, &loc, _writer._mow_context->max_version,
            segment_caches, &rowset, true, &previous_encoded_seq_value);
    int pos = start;
    bool is_expected_st = (st.is<ErrorCode::KEY_NOT_FOUND>() || st.ok());
    DCHECK(is_expected_st || st.is<ErrorCode::MEM_LIMIT_EXCEEDED>())
            << "[BlockAggregator::aggregate_rows] unexpected error status while lookup_row_key:"
            << st;
    if (!is_expected_st) {
        return st;
    }

    std::string cur_seq_val;
    if (st.ok()) {
        for (pos = start; pos < end; pos++) {
            auto& skip_bitmap = skip_bitmaps->at(pos);
            bool row_has_sequence_col = (!skip_bitmap.contains(seq_col_unique_id));
            // Discard all the rows whose seq value is smaller than previous_encoded_seq_value.
            if (row_has_sequence_col) {
                std::string seq_val {};
                _writer._encode_seq_column(seq_column, pos, &seq_val);
                if (Slice {seq_val}.compare(Slice {previous_encoded_seq_value}) < 0) {
                    continue;
                }
                cur_seq_val = std::move(seq_val);
                break;
            }
            cur_seq_val = std::move(previous_encoded_seq_value);
            break;
        }
    } else {
        pos = start;
        auto& skip_bitmap = skip_bitmaps->at(pos);
        bool row_has_sequence_col = (!skip_bitmap.contains(seq_col_unique_id));
        if (row_has_sequence_col) {
            std::string seq_val {};
            // for rows that don't specify seqeunce col, seq_val will be encoded to minial value
            _writer._encode_seq_column(seq_column, pos, &seq_val);
            cur_seq_val = std::move(seq_val);
        } else {
            cur_seq_val.clear();
            RETURN_IF_ERROR(_writer._generate_encoded_default_seq_value(
                    _tablet_schema, *_writer._opts.rowset_ctx->partial_update_info, &cur_seq_val));
        }
    }

    for (int rid {pos}; rid < end; rid++) {
        auto& skip_bitmap = skip_bitmaps->at(rid);
        bool row_has_sequence_col = (!skip_bitmap.contains(seq_col_unique_id));
        bool have_delete_sign =
                (!skip_bitmap.contains(delete_sign_col_unique_id) && delete_signs[rid] != 0);
        if (!row_has_sequence_col) {
            append_or_merge_row(output_block, block, rid, skip_bitmap, have_delete_sign);
        } else {
            std::string seq_val {};
            _writer._encode_seq_column(seq_column, rid, &seq_val);
            if (Slice {seq_val}.compare(Slice {cur_seq_val}) >= 0) {
                append_or_merge_row(output_block, block, rid, skip_bitmap, have_delete_sign);
                cur_seq_val = std::move(seq_val);
            } else {
                VLOG_DEBUG << fmt::format(
                        "skip rid={} becasue its seq value is lower than the previous", rid);
            }
        }
    }
    return Status::OK();
};

Status BlockAggregator::aggregate_for_sequence_column(
        vectorized::Block* block, int num_rows,
        const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns,
        vectorized::IOlapColumnDataAccessor* seq_column,
        const std::vector<RowsetSharedPtr>& specified_rowsets,
        std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches) {
    DCHECK_EQ(block->columns(), _tablet_schema.num_columns());
    // the process logic here is the same as MemTable::_aggregate_for_flexible_partial_update_without_seq_col()
    // after this function, there will be at most 2 rows for a specified key
    std::vector<BitmapValue>* skip_bitmaps =
            &(assert_cast<vectorized::ColumnBitmap*>(
                      block->get_by_position(_tablet_schema.skip_bitmap_col_idx())
                              .column->assume_mutable()
                              .get())
                      ->get_data());
    const auto* delete_signs = BaseTablet::get_delete_sign_column_data(*block, num_rows);

    auto filtered_block = _tablet_schema.create_block();
    vectorized::MutableBlock output_block =
            vectorized::MutableBlock::build_mutable_block(&filtered_block);

    int same_key_rows {0};
    std::string previous_key {};
    for (int block_pos {0}; block_pos < num_rows; block_pos++) {
        std::string key = _writer._full_encode_keys(key_columns, block_pos);
        if (block_pos > 0 && previous_key == key) {
            same_key_rows++;
        } else {
            if (same_key_rows > 0) {
                RETURN_IF_ERROR(aggregate_rows(output_block, block, block_pos - same_key_rows,
                                               block_pos, std::move(previous_key), skip_bitmaps,
                                               delete_signs, seq_column, specified_rowsets,
                                               segment_caches));
            }
            same_key_rows = 1;
        }
        previous_key = std::move(key);
    }
    if (same_key_rows > 0) {
        RETURN_IF_ERROR(aggregate_rows(output_block, block, num_rows - same_key_rows, num_rows,
                                       std::move(previous_key), skip_bitmaps, delete_signs,
                                       seq_column, specified_rowsets, segment_caches));
    }

    block->swap(output_block.to_block());
    return Status::OK();
}

Status BlockAggregator::fill_sequence_column(vectorized::Block* block, size_t num_rows,
                                             const FixedReadPlan& read_plan,
                                             std::vector<BitmapValue>& skip_bitmaps) {
    DCHECK(_tablet_schema.has_sequence_col());
    std::vector<uint32_t> cids {static_cast<uint32_t>(_tablet_schema.sequence_col_idx())};
    auto seq_col_unique_id = _tablet_schema.column(_tablet_schema.sequence_col_idx()).unique_id();

    auto seq_col_block = _tablet_schema.create_block_by_cids(cids);
    auto tmp_block = _tablet_schema.create_block_by_cids(cids);
    std::map<uint32_t, uint32_t> read_index;
    RETURN_IF_ERROR(read_plan.read_columns_by_plan(_tablet_schema, cids, _writer._rsid_to_rowset,
                                                   seq_col_block, &read_index, false));

    auto new_seq_col_ptr = tmp_block.get_by_position(0).column->assume_mutable();
    const auto& old_seq_col_ptr = *seq_col_block.get_by_position(0).column;
    const auto& cur_seq_col_ptr = *block->get_by_position(_tablet_schema.sequence_col_idx()).column;
    for (uint32_t block_pos {0}; block_pos < num_rows; block_pos++) {
        if (read_index.contains(block_pos)) {
            new_seq_col_ptr->insert_from(old_seq_col_ptr, read_index[block_pos]);
            skip_bitmaps[block_pos].remove(seq_col_unique_id);
        } else {
            new_seq_col_ptr->insert_from(cur_seq_col_ptr, block_pos);
        }
    }
    block->replace_by_position(_tablet_schema.sequence_col_idx(), std::move(new_seq_col_ptr));
    return Status::OK();
}

Status BlockAggregator::aggregate_for_insert_after_delete(
        vectorized::Block* block, size_t num_rows,
        const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns,
        const std::vector<RowsetSharedPtr>& specified_rowsets,
        std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches) {
    DCHECK_EQ(block->columns(), _tablet_schema.num_columns());
    // there will be at most 2 rows for a specified key in block when control flow reaches here
    // after this function, there will not be duplicate rows in block

    std::vector<BitmapValue>* skip_bitmaps =
            &(assert_cast<vectorized::ColumnBitmap*>(
                      block->get_by_position(_tablet_schema.skip_bitmap_col_idx())
                              .column->assume_mutable()
                              .get())
                      ->get_data());
    const auto* delete_signs = BaseTablet::get_delete_sign_column_data(*block, num_rows);

    auto filter_column = vectorized::ColumnUInt8::create(num_rows, 1);
    auto* __restrict filter_map = filter_column->get_data().data();
    std::string previous_key {};
    bool previous_has_delete_sign {false};
    int duplicate_rows {0};
    int32_t delete_sign_col_unique_id =
            _tablet_schema.column(_tablet_schema.delete_sign_idx()).unique_id();
    auto seq_col_unique_id =
            (_tablet_schema.sequence_col_idx() != -1)
                    ? _tablet_schema.column(_tablet_schema.sequence_col_idx()).unique_id()
                    : -1;
    FixedReadPlan read_plan;
    for (size_t block_pos {0}; block_pos < num_rows; block_pos++) {
        size_t delta_pos = block_pos;
        auto& skip_bitmap = skip_bitmaps->at(block_pos);
        std::string key = _writer._full_encode_keys(key_columns, delta_pos);
        bool have_delete_sign =
                (!skip_bitmap.contains(delete_sign_col_unique_id) && delete_signs[block_pos] != 0);
        if (delta_pos > 0 && previous_key == key) {
            // !!ATTENTION!!: We can only remove the row with delete sign if there is a insert with the same key after this row.
            // If there is only a row with delete sign, we should keep it and can't remove it from block, because
            // compaction will not use the delete bitmap when reading data. So there may still be rows with delete sign
            // in later process
            DCHECK(previous_has_delete_sign);
            DCHECK(!have_delete_sign);
            ++duplicate_rows;
            RowLocation loc;
            RowsetSharedPtr rowset;
            Status st = _writer._tablet->lookup_row_key(
                    key, &_tablet_schema, false, specified_rowsets, &loc,
                    _writer._mow_context->max_version, segment_caches, &rowset, true);
            bool is_expected_st = (st.is<ErrorCode::KEY_NOT_FOUND>() || st.ok());
            DCHECK(is_expected_st || st.is<ErrorCode::MEM_LIMIT_EXCEEDED>())
                    << "[BlockAggregator::aggregate_for_insert_after_delete] unexpected error "
                       "status while lookup_row_key:"
                    << st;
            if (!is_expected_st) {
                return st;
            }

            Slice previous_seq_slice {};
            if (st.ok()) {
                if (_tablet_schema.has_sequence_col()) {
                    // if the insert row doesn't specify the sequence column, we need to
                    // read the historical's sequence column value so that we don't need
                    // to handle seqeunce column in append_block_with_flexible_content()
                    // for this row
                    bool row_has_sequence_col = (!skip_bitmap.contains(seq_col_unique_id));
                    if (!row_has_sequence_col) {
                        read_plan.prepare_to_read(loc, block_pos);
                        _writer._rsid_to_rowset.emplace(rowset->rowset_id(), rowset);
                    }
                }
                // delete the existing row
                _writer._mow_context->delete_bitmap->add(
                        {loc.rowset_id, loc.segment_id, DeleteBitmap::TEMP_VERSION_COMMON},
                        loc.row_id);
            }
            // and remove the row with delete sign from the current block
            filter_map[block_pos - 1] = 0;
        }
        previous_has_delete_sign = have_delete_sign;
        previous_key = std::move(key);
    }
    if (duplicate_rows > 0) {
        if (!read_plan.empty()) {
            // fill sequence column value for some rows
            RETURN_IF_ERROR(fill_sequence_column(block, num_rows, read_plan, *skip_bitmaps));
        }
        RETURN_IF_ERROR(filter_block(block, num_rows, std::move(filter_column), duplicate_rows,
                                     "__filter_insert_after_delete_col__"));
    }
    return Status::OK();
}

Status BlockAggregator::filter_block(vectorized::Block* block, size_t num_rows,
                                     vectorized::MutableColumnPtr filter_column, int duplicate_rows,
                                     std::string col_name) {
    auto num_cols = block->columns();
    block->insert(
            {std::move(filter_column), std::make_shared<vectorized::DataTypeUInt8>(), col_name});
    RETURN_IF_ERROR(vectorized::Block::filter_block(block, num_cols, num_cols));
    DCHECK_EQ(num_cols, block->columns());
    size_t merged_rows = num_rows - block->rows();
    if (duplicate_rows != merged_rows) {
        auto msg = fmt::format(
                "filter_block_for_flexible_partial_update {}: duplicate_rows != merged_rows, "
                "duplicate_keys={}, merged_rows={}, num_rows={}, mutable_block->rows()={}",
                col_name, duplicate_rows, merged_rows, num_rows, block->rows());
        DCHECK(false) << msg;
        return Status::InternalError<false>(msg);
    }
    return Status::OK();
}

Status BlockAggregator::convert_pk_columns(
        vectorized::Block* block, size_t row_pos, size_t num_rows,
        std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns) {
    key_columns.clear();
    for (uint32_t cid {0}; cid < _tablet_schema.num_key_columns(); cid++) {
        RETURN_IF_ERROR(_writer._olap_data_convertor->set_source_content_with_specifid_column(
                block->get_by_position(cid), row_pos, num_rows, cid));
        auto [status, column] = _writer._olap_data_convertor->convert_column_data(cid);
        if (!status.ok()) {
            return status;
        }
        key_columns.push_back(column);
    }
    return Status::OK();
}

Status BlockAggregator::convert_seq_column(vectorized::Block* block, size_t row_pos,
                                           size_t num_rows,
                                           vectorized::IOlapColumnDataAccessor*& seq_column) {
    seq_column = nullptr;
    if (_tablet_schema.has_sequence_col()) {
        auto seq_col_idx = _tablet_schema.sequence_col_idx();
        RETURN_IF_ERROR(_writer._olap_data_convertor->set_source_content_with_specifid_column(
                block->get_by_position(seq_col_idx), row_pos, num_rows, seq_col_idx));
        auto [status, column] = _writer._olap_data_convertor->convert_column_data(seq_col_idx);
        if (!status.ok()) {
            return status;
        }
        seq_column = column;
    }
    return Status::OK();
};

Status BlockAggregator::aggregate_for_flexible_partial_update(
        vectorized::Block* block, size_t num_rows,
        const std::vector<RowsetSharedPtr>& specified_rowsets,
        std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches) {
    std::vector<vectorized::IOlapColumnDataAccessor*> key_columns {};
    vectorized::IOlapColumnDataAccessor* seq_column {nullptr};

    RETURN_IF_ERROR(convert_pk_columns(block, 0, num_rows, key_columns));
    RETURN_IF_ERROR(convert_seq_column(block, 0, num_rows, seq_column));

    // 1. merge duplicate rows when table has sequence column
    // When there are multiple rows with the same keys in memtable, some of them specify specify the sequence column,
    // some of them don't. We can't do the de-duplication in memtable because we don't know the historical data. We must
    // de-duplicate them here.
    if (_tablet_schema.has_sequence_col()) {
        RETURN_IF_ERROR(aggregate_for_sequence_column(block, static_cast<int>(num_rows),
                                                      key_columns, seq_column, specified_rowsets,
                                                      segment_caches));
    }

    // 2. merge duplicate rows and handle insert after delete
    if (block->rows() != num_rows) {
        num_rows = block->rows();
        // data in block has changed, should re-encode key columns, sequence column
        _writer._olap_data_convertor->clear_source_content();
        RETURN_IF_ERROR(convert_pk_columns(block, 0, num_rows, key_columns));
        RETURN_IF_ERROR(convert_seq_column(block, 0, num_rows, seq_column));
    }
    RETURN_IF_ERROR(aggregate_for_insert_after_delete(block, num_rows, key_columns,
                                                      specified_rowsets, segment_caches));
    return Status::OK();
}

} // namespace doris
