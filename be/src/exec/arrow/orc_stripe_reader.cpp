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

#include "exec/arrow/orc_stripe_reader.h"

#include <arrow/io/memory.h>
#include <arrow/status.h>
#include <exprs/expr_context.h>
#include <parquet/file_reader.h>

#include <memory>
#include <orc/Statistics.hh>

#include "exec/arrow/arrow_range.h"

namespace doris {

StripeReader::StripeReader(const std::vector<ExprContext*>& conjunct_ctxs, ORCReaderWrap* parent)
        : _conjunct_ctxs(conjunct_ctxs), _parent(parent) {}

StripeReader::~StripeReader() {
    _slot_conjuncts.clear();
    _filter_group.clear();
}

Status StripeReader::init_filter_groups(const TupleDescriptor* tuple_desc,
                                        const std::map<std::string, int>& map_column,
                                        const std::vector<int>& include_column_ids) {
    std::unordered_set<int> column_ids(include_column_ids.begin(), include_column_ids.end());
    _init_conjuncts(tuple_desc, map_column, column_ids);
    auto* raw_reader = _parent->getReader()->GetRawORCReader();

    int total_group = _parent->getReader()->NumberOfStripes();
    int num_rows = _parent->getReader()->NumberOfRows();
    _parent->statistics()->total_groups = total_group;
    _parent->statistics()->total_rows = num_rows;

    int32_t filtered_num_row_groups = 0;
    int64_t filtered_num_rows = 0;
    int64_t filtered_total_byte_size = 0;
    bool update_statistics = false;
    for (int row_group_id = 0; row_group_id < total_group; row_group_id++) {
        auto stripe_stats = raw_reader->getStripeStatistics(row_group_id);
        const orc::Statistics* statistics = stripe_stats.get();
        if (!statistics) {
            continue;
        }
        for (SlotId slot_id = 0; slot_id < tuple_desc->slots().size(); slot_id++) {
            const std::string& col_name = tuple_desc->slots()[slot_id]->col_name();
            auto col_iter = map_column.find(col_name);
            if (col_iter == map_column.end()) {
                continue;
            }
            int parquet_col_id = col_iter->second;
            if (column_ids.end() == column_ids.find(parquet_col_id)) {
                // Column not exist in parquet file
                continue;
            }
            auto slot_iter = _slot_conjuncts.find(slot_id);
            if (slot_iter == _slot_conjuncts.end()) {
                continue;
            }
            const orc::ColumnStatistics* col_stats =
                    statistics->getColumnStatistics(parquet_col_id + 1);
            if (!col_stats || col_stats->hasNull()) {
                continue;
            }
            bool need_filter = false;
            if (const auto* int_stats =
                        dynamic_cast<const orc::IntegerColumnStatistics*>(col_stats)) {
                IntegerArrowRange range(int_stats->getMinimum(), int_stats->getMaximum());
                need_filter = range.determine_filter_row_group(slot_iter->second);
            } else if (const auto* double_stats =
                               dynamic_cast<const orc::DoubleColumnStatistics*>(col_stats)) {
                DoubleArrowRange range(double_stats->getMinimum(), double_stats->getMaximum());
                need_filter = range.determine_filter_row_group(slot_iter->second);
            } else if (const auto* string_stats =
                               dynamic_cast<const orc::StringColumnStatistics*>(col_stats)) {
                StringArrowRange range(string_stats->getMinimum(), string_stats->getMaximum());
                need_filter = range.determine_filter_row_group(slot_iter->second);
            } else if (const auto* timestamp_stats =
                               dynamic_cast<const orc::TimestampColumnStatistics*>(col_stats)) {
                DateTimeArrowRange range(timestamp_stats->getMinimum(),
                                         timestamp_stats->getMaximum());
                need_filter = range.determine_filter_row_group(slot_iter->second);
            } else if (const auto* date_stats =
                               dynamic_cast<const orc::DateColumnStatistics*>(col_stats)) {
                DateArrowRange range(date_stats->getMinimum(), date_stats->getMaximum());
                need_filter = range.determine_filter_row_group(slot_iter->second);
            }

            if (need_filter) {
                update_statistics = true;
                filtered_num_row_groups++;
                // filtered_num_rows for each stripe can't be got from orc statics
                // filtered_total_byte_size for each stripe can't be got from orc statics
                VLOG_DEBUG << "Filter row group id: " << row_group_id;
                _filter_group.emplace(row_group_id);
                break;
            }
        }
    }
    if (update_statistics) {
        _parent->statistics()->filtered_row_groups = filtered_num_row_groups;
        _parent->statistics()->filtered_rows = filtered_num_rows;
        _parent->statistics()->filtered_total_bytes = filtered_total_byte_size;
        VLOG_DEBUG << "Orc file: "
                   << ", Num of read row group: " << total_group
                   << ", and num of skip row group: " << filtered_num_row_groups;
    }
    return Status::OK();
}

void StripeReader::_init_conjuncts(const TupleDescriptor* tuple_desc,
                                   const std::map<std::string, int>& map_column,
                                   const std::unordered_set<int>& include_column_ids) {
    if (tuple_desc->slots().empty()) {
        return;
    }
    for (int i = 0; i < tuple_desc->slots().size(); i++) {
        auto col_iter = map_column.find(tuple_desc->slots()[i]->col_name());
        if (col_iter == map_column.end()) {
            continue;
        }
        int parquet_col_id = col_iter->second;
        if (include_column_ids.end() == include_column_ids.find(parquet_col_id)) {
            continue;
        }
        for (int conj_idx = 0; conj_idx < _conjunct_ctxs.size(); conj_idx++) {
            Expr* conjunct = _conjunct_ctxs[conj_idx]->root();
            if (conjunct->get_num_children() == 0) {
                continue;
            }
            Expr* raw_slot = conjunct->get_child(0);
            if (TExprNodeType::SLOT_REF != raw_slot->node_type()) {
                continue;
            }
            SlotRef* slot_ref = (SlotRef*)raw_slot;
            SlotId conjunct_slot_id = slot_ref->slot_id();
            if (conjunct_slot_id == tuple_desc->slots()[i]->id()) {
                // Get conjuncts by conjunct_slot_id
                auto iter = _slot_conjuncts.find(conjunct_slot_id);
                if (_slot_conjuncts.end() == iter) {
                    std::vector<ExprContext*> conjuncts;
                    conjuncts.emplace_back(_conjunct_ctxs[conj_idx]);
                    _slot_conjuncts.emplace(std::make_pair(conjunct_slot_id, conjuncts));
                } else {
                    std::vector<ExprContext*> conjuncts = iter->second;
                    conjuncts.emplace_back(_conjunct_ctxs[conj_idx]);
                }
            }
        }
    }
}

} // namespace doris
