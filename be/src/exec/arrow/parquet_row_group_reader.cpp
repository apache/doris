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

#include "exec/arrow/parquet_row_group_reader.h"
#include <exprs/expr_context.h>
#include <parquet/encoding.h>

namespace doris {

    RowGroupReader::RowGroupReader(const std::vector<ExprContext*>& conjunct_ctxs,
                                   std::shared_ptr<parquet::FileMetaData>& file_metadata)
            : _conjunct_ctxs(conjunct_ctxs), _file_metadata(file_metadata) {}

    RowGroupReader::~RowGroupReader() {
        _slot_conjuncts.clear();
        _filter_group.clear();
    }

    Status RowGroupReader::init_filter_groups(const std::vector<SlotDescriptor*>& tuple_slot_descs,
                                              const std::map<std::string, int>& map_column,
                                              const std::vector<int>& include_column_ids) {
        std::unordered_set<int> parquet_column_ids(include_column_ids.begin(), include_column_ids.end());
        _init_conjuncts(tuple_slot_descs, map_column, parquet_column_ids);
        int total_group = _file_metadata->num_row_groups();
        for (int row_group_id = 0; row_group_id < total_group; row_group_id++) {
            auto row_group_meta = _file_metadata->RowGroup(row_group_id);
            for (SlotId slot_id = 0; slot_id < tuple_slot_descs.size(); slot_id++) {
                auto col_iter = map_column.find(tuple_slot_descs[slot_id]->col_name());
                if (col_iter == map_column.end()) {
                    continue;
                }
                int parquet_col_id = col_iter->second;
                if (parquet_column_ids.end() == parquet_column_ids.find(parquet_col_id)) {
                    // Column not exist in parquet file
                    continue;
                }
                auto statistic = row_group_meta->ColumnChunk(parquet_col_id)->statistics();
                if (!statistic->HasMinMax()) {
                    continue;
                }
                // Min-max of statistic is plain-encoded value
                std::string min = statistic->EncodeMin();
                std::string max = statistic->EncodeMax();
                LOG(INFO) << "Stat min:" << parquet::FormatStatValue(statistic->physical_type(), min);
                LOG(INFO) << "Stat max:" << parquet::FormatStatValue(statistic->physical_type(), max);
                bool need_filter = false;
                Status st = _determine_filter_row_group(_slot_conjuncts.at(slot_id), min, max, &need_filter);
                if (!st.ok()) {
                    return st;
                }
                if (need_filter) {
                     LOG(INFO) << "Filter row group id: " << row_group_id;
                    _filter_group.emplace(row_group_id);
                }
            }
        }
        return Status::OK();
    }

    void RowGroupReader::_init_conjuncts(const std::vector<SlotDescriptor*>& tuple_slot_descs,
                                         const std::map<std::string, int>& map_column,
                                         const std::unordered_set<int>& include_column_ids) {
        for (int i = 0; i < tuple_slot_descs.size(); i++) {
            int parquet_col_id = map_column.at(tuple_slot_descs[i]->col_name());
            if (include_column_ids.end() == include_column_ids.find(parquet_col_id)) {
                continue;
            }
            for (int conj_idx = 0; conj_idx < _conjunct_ctxs.size(); conj_idx++) {
                Expr* conjunct = _conjunct_ctxs[conj_idx]->root();
                if (TExprNodeType::SLOT_REF != conjunct->get_child(0)->node_type()) {
                    continue;
                }
                SlotRef* slot_ref = (SlotRef*) (conjunct->get_child(0));
                SlotId slot_id = slot_ref->slot_id();
                if (slot_ref->slot_id() == tuple_slot_descs[i]->id()) {
                    if (_slot_conjuncts.end() == _slot_conjuncts.find(slot_id)) {
                        std::vector<ExprContext*> conjuncts;
                        conjuncts.emplace_back(_conjunct_ctxs[conj_idx]);
                        _slot_conjuncts.emplace(std::make_pair(slot_id, conjuncts));
                    } else {
                        std::vector<ExprContext*> conjuncts = _slot_conjuncts.at(slot_id);
                        conjuncts.emplace_back(_conjunct_ctxs[conj_idx]);
                    }
                }
            }
        }
    }

    Status RowGroupReader::_determine_filter_row_group(const std::vector<ExprContext*>& conjuncts,
                                                       const std::string& encoded_min, const std::string& encoded_max,
                                                       bool* need_filter) {
        const char* min_bytes = encoded_min.data();
        const char* max_bytes = encoded_max.data();
        for (int i = 0; i < conjuncts.size(); i++) {
            Expr* expr = conjuncts[i]->root();
            void* value = conjuncts[i]->get_value(nullptr);
            if (TExprNodeType::BINARY_PRED == expr->node_type()) {
                _eval_binary_predicate(expr, value, min_bytes, max_bytes, need_filter);
            } else if (TExprNodeType::IN_PRED == expr->node_type()) {
                _eval_in_predicate(expr, min_bytes, max_bytes, need_filter);
            }
        }
        return Status::OK();
    }

    Status RowGroupReader::_eval_binary_predicate(const Expr* conjunct, void* value,
                                                  const char* min_bytes, const char* max_bytes, bool* need_filter) {
        auto conjunct_type = conjunct->get_child(1)->type().type;
        // LOG(INFO) << "conjunct type " << conjunct_type->debug_string();
        switch (conjunct->op()) {
            case TExprOpcode::EQ:
                *need_filter = _eval_eq(conjunct_type, value, min_bytes, max_bytes);
                break;
            case TExprOpcode::NE:
                break;
            case TExprOpcode::GT:
                break;
            case TExprOpcode::GE:
                break;
            case TExprOpcode::LT:
                break;
            case TExprOpcode::LE:
                break;
            default:
                break;
        }
        return Status::OK();
    }

    bool RowGroupReader::_eval_eq(PrimitiveType conjunct_type, void* value,
                                  const char* min_bytes, const char* max_bytes) {
        switch (conjunct_type) {
            case TYPE_TINYINT:
                break;
            case TYPE_SMALLINT:
                break;
            case TYPE_INT: {
                const int32_t min = reinterpret_cast<const int32_t*>(min_bytes)[0];
                const int32_t max = reinterpret_cast<const int32_t*>(max_bytes)[0];
                int32_t* conjunct_val = (int32_t*) value;
                if (*conjunct_val < min || *conjunct_val > max) {
                    return true;
                }
                break;
            }
            case TYPE_BIGINT: {
                const int64_t min = reinterpret_cast<const int64_t*>(min_bytes)[0];
                const int64_t max = reinterpret_cast<const int64_t*>(max_bytes)[0];
                int64_t* conjunct_val = (int64_t*)value;
                if (*conjunct_val < min || *conjunct_val > max) {
                    return true;
                }
                break;
            }
            case TYPE_STRING:
                break;
            case TYPE_DATE:
                break;
            case TYPE_DATETIME:
//            case TYPE_TIME:
                break;
            default:
                return false;
        }
        return false;
    }


    Status RowGroupReader::_eval_in_predicate(const Expr *conjunct, const std::string &min,
                                       const std::string &max, bool *need_filter) {
//        switch (op_type) {
//            case TExprOpcode::FILTER_IN:
//
//                break;
//            case TExprOpcode::FILTER_NOT_IN:
//
//                break;
//            default:
//                break;
//        }
        return Status();
    }
}