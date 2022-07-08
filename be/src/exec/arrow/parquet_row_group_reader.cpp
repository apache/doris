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
#include <exprs/in_predicate.h>
#include <parquet/encoding.h>

#include <cstring>

#define _PLAIN_DECODE(T, value, min_bytes, max_bytes, out_value, out_min, out_max) \
    const T out_min = reinterpret_cast<const T*>(min_bytes)[0];                    \
    const T out_max = reinterpret_cast<const T*>(max_bytes)[0];                    \
    T out_value = *((T*)value);

#define _PLAIN_DECODE_SINGLE(T, value, bytes, conjunct_value, out) \
    const T out = reinterpret_cast<const T*>(bytes)[0];            \
    T conjunct_value = *((T*)value);

#define _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max) \
    if (conjunct_value < min || conjunct_value > max) {    \
        return true;                                       \
    }

#define _FILTER_GROUP_BY_GT_PRED(conjunct_value, max) \
    if (max <= conjunct_value) {                      \
        return true;                                  \
    }

#define _FILTER_GROUP_BY_GE_PRED(conjunct_value, max) \
    if (max < conjunct_value) {                       \
        return true;                                  \
    }

#define _FILTER_GROUP_BY_LT_PRED(conjunct_value, min) \
    if (min >= conjunct_value) {                      \
        return true;                                  \
    }

#define _FILTER_GROUP_BY_LE_PRED(conjunct_value, min) \
    if (min > conjunct_value) {                       \
        return true;                                  \
    }

#define _FILTER_GROUP_BY_IN(T, in_pred_values, min_bytes, max_bytes) \
    std::vector<T> in_values;                                        \
    for (auto val : in_pred_values) {                                \
        T value = reinterpret_cast<T*>(val)[0];                      \
        in_values.emplace_back(value);                               \
    }                                                                \
    if (in_values.empty()) {                                         \
        return false;                                                \
    }                                                                \
    std::sort(in_values.begin(), in_values.end());                   \
    T in_min = in_values.front();                                    \
    T in_max = in_values.back();                                     \
    const T group_min = reinterpret_cast<const T*>(min_bytes)[0];    \
    const T group_max = reinterpret_cast<const T*>(max_bytes)[0];    \
    if (in_max < group_min || in_min > group_max) {                  \
        return true;                                                 \
    }

namespace doris {

RowGroupReader::RowGroupReader(RuntimeProfile* profile,
                               const std::vector<ExprContext*>& conjunct_ctxs,
                               std::shared_ptr<parquet::FileMetaData>& file_metadata,
                               ParquetReaderWrap* parent)
        : _conjunct_ctxs(conjunct_ctxs),
          _file_metadata(file_metadata),
          _profile(profile),
          _parent(parent) {}

RowGroupReader::~RowGroupReader() {
    _slot_conjuncts.clear();
    _filter_group.clear();
}

Status RowGroupReader::init_filter_groups(const TupleDescriptor* tuple_desc,
                                          const std::map<std::string, int>& map_column,
                                          const std::vector<int>& include_column_ids) {
    std::unordered_set<int> parquet_column_ids(include_column_ids.begin(),
                                               include_column_ids.end());
    _init_conjuncts(tuple_desc, map_column, parquet_column_ids);
    int total_group = _file_metadata->num_row_groups();
    _parent->statistics()->total_groups = total_group;
    _parent->statistics()->total_rows = _file_metadata->num_rows();

    int32_t filtered_num_row_groups = 0;
    int64_t filtered_num_rows = 0;
    int64_t filtered_total_byte_size = 0;
    bool update_statistics = false;
    for (int row_group_id = 0; row_group_id < total_group; row_group_id++) {
        auto row_group_meta = _file_metadata->RowGroup(row_group_id);
        for (SlotId slot_id = 0; slot_id < tuple_desc->slots().size(); slot_id++) {
            const std::string& col_name = tuple_desc->slots()[slot_id]->col_name();
            auto col_iter = map_column.find(col_name);
            if (col_iter == map_column.end()) {
                continue;
            }
            int parquet_col_id = col_iter->second;
            if (parquet_column_ids.end() == parquet_column_ids.find(parquet_col_id)) {
                // Column not exist in parquet file
                continue;
            }
            auto slot_iter = _slot_conjuncts.find(slot_id);
            if (slot_iter == _slot_conjuncts.end()) {
                continue;
            }
            auto statistic = row_group_meta->ColumnChunk(parquet_col_id)->statistics();
            if (!statistic->HasMinMax()) {
                continue;
            }
            // Min-max of statistic is plain-encoded value
            const std::string& min = statistic->EncodeMin();
            const std::string& max = statistic->EncodeMax();

            bool group_need_filter = _determine_filter_row_group(slot_iter->second, min, max);
            if (group_need_filter) {
                update_statistics = true;
                filtered_num_row_groups++;
                filtered_num_rows += row_group_meta->num_rows();
                filtered_total_byte_size += row_group_meta->total_byte_size();
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
        VLOG_DEBUG << "Parquet file: " << _file_metadata->schema()->name()
                   << ", Num of read row group: " << total_group
                   << ", and num of skip row group: " << filtered_num_row_groups;
    }
    return Status::OK();
}

void RowGroupReader::_init_conjuncts(const TupleDescriptor* tuple_desc,
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

bool RowGroupReader::_determine_filter_row_group(const std::vector<ExprContext*>& conjuncts,
                                                 const std::string& encoded_min,
                                                 const std::string& encoded_max) {
    const char* min_bytes = encoded_min.data();
    const char* max_bytes = encoded_max.data();
    bool need_filter = false;
    for (int i = 0; i < conjuncts.size(); i++) {
        Expr* conjunct = conjuncts[i]->root();
        if (TExprNodeType::BINARY_PRED == conjunct->node_type()) {
            _eval_binary_predicate(conjuncts[i], min_bytes, max_bytes, need_filter);
        } else if (TExprNodeType::IN_PRED == conjunct->node_type()) {
            _eval_in_predicate(conjuncts[i], min_bytes, max_bytes, need_filter);
        }
    }
    return need_filter;
}

void RowGroupReader::_eval_binary_predicate(ExprContext* ctx, const char* min_bytes,
                                            const char* max_bytes, bool& need_filter) {
    Expr* conjunct = ctx->root();
    Expr* expr = conjunct->get_child(1);
    if (expr == nullptr) {
        return;
    }
    // supported conjunct example: slot_ref < 123, slot_ref > func(123), ..
    auto conjunct_type = expr->type().type;
    void* conjunct_value = ctx->get_value(expr, nullptr);
    switch (conjunct->op()) {
    case TExprOpcode::EQ:
        need_filter = _eval_eq(conjunct_type, conjunct_value, min_bytes, max_bytes);
        break;
    case TExprOpcode::NE:
        break;
    case TExprOpcode::GT:
        need_filter = _eval_gt(conjunct_type, conjunct_value, max_bytes);
        break;
    case TExprOpcode::GE:
        need_filter = _eval_ge(conjunct_type, conjunct_value, max_bytes);
        break;
    case TExprOpcode::LT:
        need_filter = _eval_lt(conjunct_type, conjunct_value, min_bytes);
        break;
    case TExprOpcode::LE:
        need_filter = _eval_le(conjunct_type, conjunct_value, min_bytes);
        break;
    default:
        break;
    }
}

void RowGroupReader::_eval_in_predicate(ExprContext* ctx, const char* min_bytes,
                                        const char* max_bytes, bool& need_filter) {
    Expr* conjunct = ctx->root();
    std::vector<void*> in_pred_values;
    const InPredicate* pred = static_cast<const InPredicate*>(conjunct);
    HybridSetBase::IteratorBase* iter = pred->hybrid_set()->begin();
    // TODO: process expr: in(func(123),123)
    while (iter->has_next()) {
        if (nullptr == iter->get_value()) {
            return;
        }
        in_pred_values.emplace_back(const_cast<void*>(iter->get_value()));
        iter->next();
    }
    auto conjunct_type = conjunct->get_child(1)->type().type;
    switch (conjunct->op()) {
    case TExprOpcode::FILTER_IN:
        need_filter = _eval_in_val(conjunct_type, in_pred_values, min_bytes, max_bytes);
        break;
    //  case TExprOpcode::FILTER_NOT_IN:
    default:
        need_filter = false;
    }
}

bool RowGroupReader::_eval_in_val(PrimitiveType conjunct_type, std::vector<void*> in_pred_values,
                                  const char* min_bytes, const char* max_bytes) {
    switch (conjunct_type) {
    case TYPE_TINYINT: {
        _FILTER_GROUP_BY_IN(int8_t, in_pred_values, min_bytes, max_bytes)
        break;
    }
    case TYPE_SMALLINT: {
        _FILTER_GROUP_BY_IN(int16_t, in_pred_values, min_bytes, max_bytes)
        break;
    }
    case TYPE_INT: {
        _FILTER_GROUP_BY_IN(int32_t, in_pred_values, min_bytes, max_bytes)
        break;
    }
    case TYPE_BIGINT: {
        _FILTER_GROUP_BY_IN(int64_t, in_pred_values, min_bytes, max_bytes)
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_DATE:
    case TYPE_DATETIME: {
        std::vector<const char*> in_values;
        for (auto val : in_pred_values) {
            const char* value = ((std::string*)val)->c_str();
            in_values.emplace_back(value);
        }
        if (in_values.empty()) {
            return false;
        }
        std::sort(in_values.begin(), in_values.end());
        const char* in_min = in_values.front();
        const char* in_max = in_values.back();
        if (strcmp(in_max, min_bytes) < 0 || strcmp(in_min, max_bytes) > 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}

bool RowGroupReader::_eval_eq(PrimitiveType conjunct_type, void* value, const char* min_bytes,
                              const char* max_bytes) {
    switch (conjunct_type) {
    case TYPE_TINYINT: {
        _PLAIN_DECODE(int16_t, value, min_bytes, max_bytes, conjunct_value, min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_SMALLINT: {
        _PLAIN_DECODE(int16_t, value, min_bytes, max_bytes, conjunct_value, min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_INT: {
        _PLAIN_DECODE(int32_t, value, min_bytes, max_bytes, conjunct_value, min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_BIGINT: {
        _PLAIN_DECODE(int64_t, value, min_bytes, max_bytes, conjunct_value, min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_DOUBLE: {
        _PLAIN_DECODE(double, value, min_bytes, max_bytes, conjunct_value, min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_FLOAT: {
        _PLAIN_DECODE(float, value, min_bytes, max_bytes, conjunct_value, min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_DATE:
    case TYPE_DATETIME: {
        const char* conjunct_value = ((std::string*)value)->c_str();
        if (strcmp(conjunct_value, min_bytes) < 0 || strcmp(conjunct_value, max_bytes) > 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}

bool RowGroupReader::_eval_gt(PrimitiveType conjunct_type, void* value, const char* max_bytes) {
    switch (conjunct_type) {
    case TYPE_TINYINT: {
        _PLAIN_DECODE_SINGLE(int8_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GT_PRED(conjunct_value, max)
        break;
    }
    case TYPE_SMALLINT: {
        _PLAIN_DECODE_SINGLE(int16_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GT_PRED(conjunct_value, max)
        break;
    }
    case TYPE_INT: {
        _PLAIN_DECODE_SINGLE(int32_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GT_PRED(conjunct_value, max)
        break;
    }
    case TYPE_BIGINT: {
        _PLAIN_DECODE_SINGLE(int64_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GT_PRED(conjunct_value, max)
        break;
    }
    case TYPE_DOUBLE: {
        _PLAIN_DECODE_SINGLE(double, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GT_PRED(conjunct_value, max)
        break;
    }
    case TYPE_FLOAT: {
        _PLAIN_DECODE_SINGLE(float, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GT_PRED(conjunct_value, max)
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_DATE:
    case TYPE_DATETIME: {
        //            case TYPE_TIME:
        const char* conjunct_value = ((std::string*)value)->c_str();
        if (strcmp(max_bytes, conjunct_value) <= 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}

bool RowGroupReader::_eval_ge(PrimitiveType conjunct_type, void* value, const char* max_bytes) {
    switch (conjunct_type) {
    case TYPE_TINYINT: {
        _PLAIN_DECODE_SINGLE(int8_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GE_PRED(conjunct_value, max)
        break;
    }
    case TYPE_SMALLINT: {
        _PLAIN_DECODE_SINGLE(int16_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GE_PRED(conjunct_value, max)
        break;
    }
    case TYPE_INT: {
        _PLAIN_DECODE_SINGLE(int32_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GE_PRED(conjunct_value, max)
        break;
    }
    case TYPE_BIGINT: {
        _PLAIN_DECODE_SINGLE(int64_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GE_PRED(conjunct_value, max)
        break;
    }
    case TYPE_DOUBLE: {
        _PLAIN_DECODE_SINGLE(double, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GE_PRED(conjunct_value, max)
        break;
    }
    case TYPE_FLOAT: {
        _PLAIN_DECODE_SINGLE(float, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GE_PRED(conjunct_value, max)
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_DATE:
    case TYPE_DATETIME: {
        //            case TYPE_TIME:
        const char* conjunct_value = ((std::string*)value)->c_str();
        if (strcmp(max_bytes, conjunct_value) < 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}

bool RowGroupReader::_eval_lt(PrimitiveType conjunct_type, void* value, const char* min_bytes) {
    switch (conjunct_type) {
    case TYPE_TINYINT: {
        _PLAIN_DECODE_SINGLE(int8_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LT_PRED(conjunct_value, min)
        break;
    }
    case TYPE_SMALLINT: {
        _PLAIN_DECODE_SINGLE(int16_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LT_PRED(conjunct_value, min)
        break;
    }
    case TYPE_INT: {
        _PLAIN_DECODE_SINGLE(int32_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LT_PRED(conjunct_value, min)
        break;
    }
    case TYPE_BIGINT: {
        _PLAIN_DECODE_SINGLE(int64_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LT_PRED(conjunct_value, min)
        break;
    }
    case TYPE_DOUBLE: {
        _PLAIN_DECODE_SINGLE(double, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LT_PRED(conjunct_value, min)
        break;
    }
    case TYPE_FLOAT: {
        _PLAIN_DECODE_SINGLE(float, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LT_PRED(conjunct_value, min)
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_DATE:
    case TYPE_DATETIME: {
        //            case TYPE_TIME:
        const char* conjunct_value = ((std::string*)value)->c_str();
        if (strcmp(min_bytes, conjunct_value) >= 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}

bool RowGroupReader::_eval_le(PrimitiveType conjunct_type, void* value, const char* min_bytes) {
    switch (conjunct_type) {
    case TYPE_TINYINT: {
        _PLAIN_DECODE_SINGLE(int8_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LE_PRED(conjunct_value, min)
        break;
    }
    case TYPE_SMALLINT: {
        _PLAIN_DECODE_SINGLE(int16_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LE_PRED(conjunct_value, min)
        break;
    }
    case TYPE_INT: {
        _PLAIN_DECODE_SINGLE(int32_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LE_PRED(conjunct_value, min)
        break;
    }
    case TYPE_BIGINT: {
        _PLAIN_DECODE_SINGLE(int64_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LE_PRED(conjunct_value, min)
        break;
    }
    case TYPE_DOUBLE: {
        _PLAIN_DECODE_SINGLE(double, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LE_PRED(conjunct_value, min)
        break;
    }
    case TYPE_FLOAT: {
        _PLAIN_DECODE_SINGLE(float, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LE_PRED(conjunct_value, min)
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_DATE:
    case TYPE_DATETIME: {
        //            case TYPE_TIME:
        const char* conjunct_value = ((std::string*)value)->c_str();
        if (strcmp(min_bytes, conjunct_value) > 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}
} // namespace doris