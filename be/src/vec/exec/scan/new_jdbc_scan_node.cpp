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

#include "vec/exec/scan/new_jdbc_scan_node.h"

#include <fmt/format.h>
#include <gen_cpp/PlanNodes_types.h>

#include <boost/algorithm/string/join.hpp>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>

#include "common/logging.h"
#include "common/object_pool.h"
#include "exprs/hybrid_set.h"
#include "runtime/runtime_state.h"
#include "vec/exec/scan/new_jdbc_scanner.h"
#include "vec/exprs/vcast_expr.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vruntimefilter_wrapper.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
class DescriptorTbl;
namespace vectorized {
class VScanner;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
NewJdbcScanNode::NewJdbcScanNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : VScanNode(pool, tnode, descs),
          _table_name(tnode.jdbc_scan_node.table_name),
          _tuple_id(tnode.jdbc_scan_node.tuple_id),
          _query_string(tnode.jdbc_scan_node.query_string),
          _table_type(tnode.jdbc_scan_node.table_type) {
    _output_tuple_id = tnode.jdbc_scan_node.tuple_id;
}

std::string NewJdbcScanNode::get_name() {
    return fmt::format("VNewJdbcScanNode({0})", _table_name);
}

Status NewJdbcScanNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "VNewJdbcScanNode::Prepare";
    RETURN_IF_ERROR(VScanNode::prepare(state));
    return Status::OK();
}

Status NewJdbcScanNode::_init_profile() {
    RETURN_IF_ERROR(VScanNode::_init_profile());
    return Status::OK();
}

Status NewJdbcScanNode::_init_scanners(std::list<VScannerSPtr>* scanners) {
    if (_eos == true) {
        return Status::OK();
    }
    std::unique_ptr<NewJdbcScanner> scanner =
            NewJdbcScanner::create_unique(_state, this, _limit_per_scanner, _tuple_id,
                                          _query_string, _table_type, _state->runtime_profile());
    RETURN_IF_ERROR(scanner->prepare(_state, _conjuncts));
    scanners->push_back(std::move(scanner));
    return Status::OK();
}

Status NewJdbcScanNode::_process_conjuncts() {
    RETURN_IF_ERROR(VScanNode::_process_conjuncts());
    if (_eos) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_process_rf_exprs());
    return Status::OK();
}

Status NewJdbcScanNode::_process_rf_exprs() {
    std::vector<std::string> filters;
    for (auto expr : _rf_vexpr_set) {
        if (typeid_cast<VRuntimeFilterWrapper*>(expr.get())) {
            RETURN_IF_ERROR(_convert_rf_expr_to_sql_str(expr, filters));
        }
    }
    if (!filters.empty()) {
        const std::string filter_str = boost::join(filters, " AND ");
        _append_rf_string(filter_str);
    }
    return Status::OK();
}

Status NewJdbcScanNode::_convert_rf_expr_to_sql_str(const vectorized::VExprSPtr& expr,
                                                    std::vector<std::string>& filters) {
    auto filter_impl = expr->get_impl();
    auto node_type = expr->node_type();
    auto op_code = expr->op();
    if (node_type == TExprNodeType::IN_PRED) {
        // in_predict includes only one SLOT_REF child
        DCHECK(op_code == TExprOpcode::FILTER_IN && filter_impl->get_num_children() == 1);
        auto hybrid_set = filter_impl->get_set_func();
        auto slot_expr = filter_impl->get_child(0);
        auto data_type = slot_expr->type().type;
        if (slot_expr->node_type() == TExprNodeType::CAST_EXPR) {
            data_type = dynamic_cast<const VCastExpr*>(slot_expr.get())
                                ->get_target_type()
                                ->get_type_as_type_descriptor()
                                .type;
            slot_expr = slot_expr->get_child(0);
        }
        if (slot_expr->node_type() != TExprNodeType::SLOT_REF) {
            return Status::OK();
        }
        std::vector<std::string> in_strs;
        if (hybrid_set) {
            auto iter = hybrid_set->begin();
            while (iter->has_next()) {
                if (nullptr == iter->get_value()) {
                    iter->next();
                    continue;
                }
                std::string value_str = _convert_value_data(data_type, iter->get_value());
                if (!value_str.empty()) {
                    in_strs.push_back(value_str);
                }
                iter->next();
            }

            if (!in_strs.empty()) {
                std::string in_pred_str = fmt::format("`{}` IN ({})", slot_expr->expr_name(),
                                                      boost::join(in_strs, ", "));
                filters.push_back(in_pred_str);
            }
        }
    } else if (node_type == TExprNodeType::BINARY_PRED) {
        DCHECK(filter_impl->get_num_children() == 2);
        auto slot_expr = filter_impl->get_child(0);
        auto data_type = slot_expr->type().type;
        if (slot_expr->node_type() == TExprNodeType::CAST_EXPR) {
            data_type = dynamic_cast<const VCastExpr*>(slot_expr.get())
                                ->get_target_type()
                                ->get_type_as_type_descriptor()
                                .type;
            slot_expr = slot_expr->get_child(0);
        }
        if (slot_expr->node_type() != TExprNodeType::SLOT_REF) {
            return Status::OK();
        }
        auto literal = dynamic_cast<const vectorized::VLiteral*>(filter_impl->get_child(1).get());
        const std::string value = (is_string_type(data_type) || is_date_type(data_type))
                                          ? fmt::format("'{}'", literal->value())
                                          : literal->value();
        std::string min_max_pred_str;
        switch (op_code) {
        case TExprOpcode::LE:
            min_max_pred_str = fmt::format("`{}`<={}", slot_expr->expr_name(), value);
            filters.push_back(min_max_pred_str);
            break;
        case TExprOpcode::GE:
            min_max_pred_str = fmt::format("`{}`>={}", slot_expr->expr_name(), value);
            filters.push_back(min_max_pred_str);
            break;
        default:
            LOG(WARNING) << "Invalid opcode for max_min_runtimefilter: " << op_code;
            break;
        }
    }
    return Status::OK();
}

std::string NewJdbcScanNode::_convert_value_data(PrimitiveType type, const void* value) {
    std::string res;
    switch (type) {
    case TYPE_TINYINT: {
        res = std::to_string(*reinterpret_cast<const int8_t*>(value));
        break;
    }
    case TYPE_SMALLINT: {
        res = std::to_string(*reinterpret_cast<const int16_t*>(value));
        break;
    }
    case TYPE_INT: {
        res = std::to_string(*reinterpret_cast<const int32_t*>(value));
        break;
    }
    case TYPE_BIGINT: {
        res = std::to_string(*reinterpret_cast<const int64_t*>(value));
        break;
    }
    case TYPE_LARGEINT: {
        res = LargeIntValue::to_string(*reinterpret_cast<const int128_t*>(value));
        break;
    }
    case TYPE_FLOAT: {
        res = std::to_string(*reinterpret_cast<const float*>(value));
        break;
    }
    case TYPE_DOUBLE: {
        res = std::to_string(*reinterpret_cast<const double*>(value));
        break;
    }
    case TYPE_DECIMALV2: {
        res = reinterpret_cast<const DecimalV2Value*>(value)->to_string();
        break;
    }
    case TYPE_DECIMAL32: {
        res = std::to_string(*reinterpret_cast<const int32_t*>(value));
        break;
    }
    case TYPE_DECIMAL64: {
        res = std::to_string(*reinterpret_cast<const int64_t*>(value));
        break;
    }
    case TYPE_DECIMAL128I: {
        res = LargeIntValue::to_string(*reinterpret_cast<const int128_t*>(value));
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        const StringRef* min_string_value = reinterpret_cast<const StringRef*>(value);
        // Wrapped in single quotation marks
        res = fmt::format("'{}'", std::string(min_string_value->data, min_string_value->size));
        break;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        char convert_buffer[64];
        int len = reinterpret_cast<const VecDateTimeValue*>(value)->to_buffer(convert_buffer);
        res = fmt::format("'{}'", std::string(convert_buffer, len));
        break;
    }
    case TYPE_DATEV2: {
        char convert_buffer[64];
        int len = reinterpret_cast<const DateV2Value<DateV2ValueType>*>(value)->to_buffer(
                convert_buffer);
        res = fmt::format("'{}'", std::string(convert_buffer, len));
        break;
    }
    case TYPE_DATETIMEV2: {
        char convert_buffer[64];
        int len = reinterpret_cast<const DateV2Value<DateTimeV2ValueType>*>(value)->to_buffer(
                convert_buffer);
        res = fmt::format("'{}'", std::string(convert_buffer, len));
        break;
    }
    default: {
        break;
    }
    }
    return res;
}

void NewJdbcScanNode::_append_rf_string(const std::string filter_str) {
    const std::string where("WHERE");
    bool has_where = false;
    int pos = _query_string.find(where);
    if (pos == std::string::npos) {
        pos = _query_string.find(_table_name);
    } else {
        has_where = true;
    }
    DCHECK_NE(pos, std::string::npos);

    if (has_where) {
        _query_string.insert(pos + where.size(), fmt::format(" {} AND", filter_str));
    } else {
        _query_string.insert(pos + _table_name.size(), fmt::format(" {} {}", where, filter_str));
    }
}
} // namespace doris::vectorized
