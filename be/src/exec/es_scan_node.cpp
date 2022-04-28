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

#include "es_scan_node.h"

#include <gutil/strings/substitute.h>

#include <boost/algorithm/string.hpp>
#include <string>

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/in_predicate.h"
#include "exprs/slot_ref.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "olap/olap_common.h"
#include "olap/utils.h"
#include "runtime/client_cache.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "service/backend_options.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

namespace doris {

// $0 = column type (e.g. INT)
const std::string ERROR_INVALID_COL_DATA =
        "Data source returned inconsistent column data. "
        "Expected value of type $0 based on column metadata. This likely indicates a "
        "problem with the data source library.";
const std::string ERROR_MEM_LIMIT_EXCEEDED =
        "DataSourceScanNode::$0() failed to allocate "
        "$1 bytes for $2.";

EsScanNode::EsScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _tuple_id(tnode.es_scan_node.tuple_id),
          _tuple_desc(nullptr),
          _env(nullptr),
          _scan_range_idx(0) {
    if (tnode.es_scan_node.__isset.properties) {
        _properties = tnode.es_scan_node.properties;
    }
}

EsScanNode::~EsScanNode() {}

Status EsScanNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "EsScanNode::Prepare";

    RETURN_IF_ERROR(ScanNode::prepare(state));
    SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(mem_tracker());
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "es tuple descriptor is null, _tuple_id=" << _tuple_id;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    _env = state->exec_env();

    return Status::OK();
}

Status EsScanNode::open(RuntimeState* state) {
    VLOG_CRITICAL << "EsScanNode::Open";

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(mem_tracker());
    RETURN_IF_ERROR(ExecNode::open(state));

    // TExtOpenParams.row_schema
    std::vector<TExtColumnDesc> cols;
    for (const SlotDescriptor* slot : _tuple_desc->slots()) {
        TExtColumnDesc col;
        col.__set_name(slot->col_name());
        col.__set_type(slot->type().to_thrift());
        cols.emplace_back(std::move(col));
    }
    TExtTableSchema row_schema;
    row_schema.cols = std::move(cols);
    row_schema.__isset.cols = true;

    // TExtOpenParams.predicates
    std::vector<vector<TExtPredicate>> predicates;
    std::vector<int> predicate_to_conjunct;
    for (int i = 0; i < _conjunct_ctxs.size(); ++i) {
        VLOG_CRITICAL << "conjunct: " << _conjunct_ctxs[i]->root()->debug_string();
        std::vector<TExtPredicate> disjuncts;
        if (get_disjuncts(_conjunct_ctxs[i], _conjunct_ctxs[i]->root(), disjuncts)) {
            predicates.emplace_back(std::move(disjuncts));
            predicate_to_conjunct.push_back(i);
        }
    }

    // open every scan range
    std::vector<int> conjunct_accepted_times(_conjunct_ctxs.size(), 0);
    for (int i = 0; i < _scan_ranges.size(); ++i) {
        TEsScanRange& es_scan_range = _scan_ranges[i];

        if (es_scan_range.es_hosts.empty()) {
            std::stringstream ss;
            ss << "es fail to open: hosts empty";
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // TExtOpenParams
        TExtOpenParams params;
        params.__set_query_id(state->query_id());
        _properties["index"] = es_scan_range.index;
        if (es_scan_range.__isset.type) {
            _properties["type"] = es_scan_range.type;
        }
        _properties["shard_id"] = std::to_string(es_scan_range.shard_id);
        params.__set_properties(_properties);
        params.__set_row_schema(row_schema);
        params.__set_batch_size(state->batch_size());
        params.__set_predicates(predicates);
        TExtOpenResult result;

        // choose an es node, local is the first choice
        std::string localhost = BackendOptions::get_localhost();
        bool is_success = false;
        for (int j = 0; j < 2; ++j) {
            for (auto& es_host : es_scan_range.es_hosts) {
                if ((j == 0 && es_host.hostname != localhost) ||
                    (j == 1 && es_host.hostname == localhost)) {
                    continue;
                }
                Status status = open_es(es_host, result, params);
                if (status.ok()) {
                    is_success = true;
                    _addresses.push_back(es_host);
                    _scan_handles.push_back(result.scan_handle);
                    if (result.__isset.accepted_conjuncts) {
                        for (int index : result.accepted_conjuncts) {
                            conjunct_accepted_times[predicate_to_conjunct[index]]++;
                        }
                    }
                    break;
                } else if (status.code() == TStatusCode::ES_SHARD_NOT_FOUND) {
                    // if shard not found, try other nodes
                    LOG(WARNING) << "shard not found on es node: "
                                 << ", address=" << es_host << ", scan_range_idx=" << i
                                 << ", try other nodes";
                } else {
                    LOG(WARNING) << "es open error: scan_range_idx=" << i << ", address=" << es_host
                                 << ", msg=" << status.get_error_msg();
                    return status;
                }
            }
            if (is_success) {
                break;
            }
        }

        if (!is_success) {
            std::stringstream ss;
            ss << "es open error: scan_range_idx=" << i << ", can't find shard on any node";
            return Status::InternalError(ss.str());
        }
    }

    // remove those conjuncts that accepted by all scan ranges
    for (int i = predicate_to_conjunct.size() - 1; i >= 0; i--) {
        int conjunct_index = predicate_to_conjunct[i];
        if (conjunct_accepted_times[conjunct_index] == _scan_ranges.size()) {
            _pushdown_conjunct_ctxs.push_back(*(_conjunct_ctxs.begin() + conjunct_index));
            _conjunct_ctxs.erase(_conjunct_ctxs.begin() + conjunct_index);
        }
    }

    for (int i = 0; i < _conjunct_ctxs.size(); ++i) {
        if (!check_left_conjuncts(_conjunct_ctxs[i]->root())) {
            return Status::InternalError(
                    "esquery could only be executed on es, but could not push down to es");
        }
    }

    return Status::OK();
}

Status EsScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    VLOG_CRITICAL << "EsScanNode::GetNext";

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_SWITCH_TASK_THREAD_LOCAL_EXISTED_MEM_TRACKER(mem_tracker());

    // create tuple
    MemPool* tuple_pool = row_batch->tuple_data_pool();
    int64_t tuple_buffer_size;
    uint8_t* tuple_buffer = nullptr;
    RETURN_IF_ERROR(
            row_batch->resize_and_allocate_tuple_buffer(state, &tuple_buffer_size, &tuple_buffer));
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);

    // get batch
    TExtGetNextResult result;
    RETURN_IF_ERROR(get_next_from_es(result));
    _offsets[_scan_range_idx] += result.rows.num_rows;

    // convert
    VLOG_CRITICAL << "begin to convert: scan_range_idx=" << _scan_range_idx
                  << ", num_rows=" << result.rows.num_rows;
    std::vector<TExtColumnData>& cols = result.rows.cols;
    // indexes of the next non-null value in the row batch, per column.
    std::vector<int> cols_next_val_idx(_tuple_desc->slots().size(), 0);
    for (int row_idx = 0; row_idx < result.rows.num_rows; row_idx++) {
        if (reached_limit()) {
            *eos = true;
            break;
        }
        RETURN_IF_ERROR(materialize_row(tuple_pool, tuple, cols, row_idx, cols_next_val_idx));
        TupleRow* tuple_row = row_batch->get_row(row_batch->add_row());
        tuple_row->set_tuple(0, tuple);
        if (ExecNode::eval_conjuncts(_conjunct_ctxs.data(), _conjunct_ctxs.size(), tuple_row)) {
            row_batch->commit_last_row();
            tuple = reinterpret_cast<Tuple*>(reinterpret_cast<uint8_t*>(tuple) +
                                             _tuple_desc->byte_size());
            ++_num_rows_returned;
        }
    }

    VLOG_CRITICAL << "finish one batch: num_rows=" << row_batch->num_rows();
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    if (result.__isset.eos && result.eos) {
        VLOG_CRITICAL << "es finish one scan_range: scan_range_idx=" << _scan_range_idx;
        ++_scan_range_idx;
    }
    if (_scan_range_idx == _scan_ranges.size()) {
        *eos = true;
    }

    return Status::OK();
}

Status EsScanNode::close(RuntimeState* state) {
    if (is_closed()) return Status::OK();
    VLOG_CRITICAL << "EsScanNode::Close";
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    Expr::close(_pushdown_conjunct_ctxs, state);
    RETURN_IF_ERROR(ExecNode::close(state));
    for (int i = 0; i < _addresses.size(); ++i) {
        TExtCloseParams params;
        params.__set_scan_handle(_scan_handles[i]);
        TExtCloseResult result;

#ifndef BE_TEST
        const TNetworkAddress& address = _addresses[i];
        try {
            Status status;
            ExtDataSourceServiceClientCache* client_cache = _env->extdatasource_client_cache();
            ExtDataSourceServiceConnection client(client_cache, address, 10000, &status);
            if (!status.ok()) {
                LOG(WARNING) << "es create client error: scan_range_idx=" << i
                             << ", address=" << address << ", msg=" << status.get_error_msg();
                return status;
            }

            try {
                VLOG_CRITICAL << "es close param=" << apache::thrift::ThriftDebugString(params);
                client->close(result, params);
            } catch (apache::thrift::transport::TTransportException& e) {
                LOG(WARNING) << "es close retrying, because: " << e.what();
                RETURN_IF_ERROR(client.reopen());
                client->close(result, params);
            }
        } catch (apache::thrift::TException& e) {
            std::stringstream ss;
            ss << "es close error: scan_range_idx=" << i << ", msg=" << e.what();
            LOG(WARNING) << ss.str();
            return Status::ThriftRpcError(ss.str());
        }

        VLOG_CRITICAL << "es close result=" << apache::thrift::ThriftDebugString(result);
        Status status(result.status);
        if (!status.ok()) {
            LOG(WARNING) << "es close error: : scan_range_idx=" << i
                         << ", msg=" << status.get_error_msg();
            return status;
        }
#else
        TStatus status;
        result.__set_status(status);
#endif
    }

    return Status::OK();
}

void EsScanNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "EsScanNode(tupleid=" << _tuple_id;
    *out << ")" << std::endl;

    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->debug_string(indentation_level + 1, out);
    }
}

Status EsScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (int i = 0; i < scan_ranges.size(); ++i) {
        TScanRangeParams scan_range = scan_ranges[i];
        DCHECK(scan_range.scan_range.__isset.es_scan_range);
        TEsScanRange es_scan_range = scan_range.scan_range.es_scan_range;
        _scan_ranges.push_back(es_scan_range);
    }

    _offsets.resize(scan_ranges.size(), 0);
    return Status::OK();
}

Status EsScanNode::open_es(TNetworkAddress& address, TExtOpenResult& result,
                           TExtOpenParams& params) {
    VLOG_CRITICAL << "es open param=" << apache::thrift::ThriftDebugString(params);
#ifndef BE_TEST
    try {
        ExtDataSourceServiceClientCache* client_cache = _env->extdatasource_client_cache();
        Status status;
        ExtDataSourceServiceConnection client(client_cache, address, 10000, &status);
        if (!status.ok()) {
            std::stringstream ss;
            ss << "es create client error: address=" << address
               << ", msg=" << status.get_error_msg();
            return Status::InternalError(ss.str());
        }

        try {
            client->open(result, params);
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG(WARNING) << "es open retrying, because: " << e.what();
            RETURN_IF_ERROR(client.reopen());
            client->open(result, params);
        }
        VLOG_CRITICAL << "es open result=" << apache::thrift::ThriftDebugString(result);
        return Status(result.status);
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "es open error: address=" << address << ", msg=" << e.what();
        return Status::InternalError(ss.str());
    }
#else
    TStatus status;
    result.__set_status(status);
    result.__set_scan_handle("0");
    return Status(status);
#endif
}

// legacy conjuncts must not contain match function
bool EsScanNode::check_left_conjuncts(Expr* conjunct) {
    if (is_match_func(conjunct)) {
        return false;
    } else {
        int num_children = conjunct->get_num_children();
        for (int child_idx = 0; child_idx < num_children; ++child_idx) {
            if (!check_left_conjuncts(conjunct->get_child(child_idx))) {
                return false;
            }
        }
        return true;
    }
}

bool EsScanNode::ignore_cast(SlotDescriptor* slot, Expr* expr) {
    if (slot->type().is_date_type() && expr->type().is_date_type()) {
        return true;
    }
    if (slot->type().is_string_type() && expr->type().is_string_type()) {
        return true;
    }
    return false;
}

bool EsScanNode::get_disjuncts(ExprContext* context, Expr* conjunct,
                               std::vector<TExtPredicate>& disjuncts) {
    if (TExprNodeType::BINARY_PRED == conjunct->node_type()) {
        if (conjunct->children().size() != 2) {
            VLOG_CRITICAL << "get disjuncts fail: number of children is not 2";
            return false;
        }
        SlotRef* slotRef;
        TExprOpcode::type op;
        Expr* expr;
        if (TExprNodeType::SLOT_REF == conjunct->get_child(0)->node_type()) {
            expr = conjunct->get_child(1);
            slotRef = (SlotRef*)(conjunct->get_child(0));
            op = conjunct->op();
        } else if (TExprNodeType::SLOT_REF == conjunct->get_child(1)->node_type()) {
            expr = conjunct->get_child(0);
            slotRef = (SlotRef*)(conjunct->get_child(1));
            op = conjunct->op();
        } else {
            VLOG_CRITICAL << "get disjuncts fail: no SLOT_REF child";
            return false;
        }

        SlotDescriptor* slot_desc = get_slot_desc(slotRef);
        if (slot_desc == nullptr) {
            VLOG_CRITICAL << "get disjuncts fail: slot_desc is null";
            return false;
        }

        TExtLiteral literal;
        if (!to_ext_literal(context, expr, &literal)) {
            VLOG_CRITICAL << "get disjuncts fail: can't get literal, node_type="
                          << expr->node_type();
            return false;
        }

        TExtColumnDesc columnDesc;
        columnDesc.__set_name(slot_desc->col_name());
        columnDesc.__set_type(slot_desc->type().to_thrift());
        TExtBinaryPredicate binaryPredicate;
        binaryPredicate.__set_col(columnDesc);
        binaryPredicate.__set_op(op);
        binaryPredicate.__set_value(std::move(literal));
        TExtPredicate predicate;
        predicate.__set_node_type(TExprNodeType::BINARY_PRED);
        predicate.__set_binary_predicate(binaryPredicate);
        disjuncts.push_back(std::move(predicate));
        return true;
    } else if (is_match_func(conjunct)) {
        // if this is a function call expr and function name is match, then push
        // down it to es
        TExtFunction match_function;
        match_function.__set_func_name(conjunct->fn().name.function_name);
        std::vector<TExtLiteral> query_conditions;

        TExtLiteral literal;
        if (!to_ext_literal(context, conjunct->get_child(1), &literal)) {
            VLOG_CRITICAL << "get disjuncts fail: can't get literal, node_type="
                          << conjunct->get_child(1)->node_type();
            return false;
        }

        query_conditions.push_back(std::move(literal));
        match_function.__set_values(query_conditions);
        TExtPredicate predicate;
        predicate.__set_node_type(TExprNodeType::FUNCTION_CALL);
        predicate.__set_ext_function(match_function);
        disjuncts.push_back(std::move(predicate));
        return true;
    } else if (TExprNodeType::IN_PRED == conjunct->node_type()) {
        // the op code maybe FILTER_NEW_IN, it means there is function in list
        // like col_a in (abs(1))
        if (TExprOpcode::FILTER_IN != conjunct->op() &&
            TExprOpcode::FILTER_NOT_IN != conjunct->op()) {
            return false;
        }
        TExtInPredicate ext_in_predicate;
        std::vector<TExtLiteral> in_pred_values;
        InPredicate* pred = static_cast<InPredicate*>(conjunct);
        ext_in_predicate.__set_is_not_in(pred->is_not_in());
        if (Expr::type_without_cast(pred->get_child(0)) != TExprNodeType::SLOT_REF) {
            return false;
        }

        SlotRef* slot_ref = (SlotRef*)(conjunct->get_child(0));
        SlotDescriptor* slot_desc = get_slot_desc(slot_ref);
        if (slot_desc == nullptr) {
            return false;
        }
        TExtColumnDesc columnDesc;
        columnDesc.__set_name(slot_desc->col_name());
        columnDesc.__set_type(slot_desc->type().to_thrift());
        ext_in_predicate.__set_col(columnDesc);

        if (pred->get_child(0)->type().type != slot_desc->type().type) {
            if (!ignore_cast(slot_desc, pred->get_child(0))) {
                return false;
            }
        }

        HybridSetBase::IteratorBase* iter = pred->hybrid_set()->begin();
        while (iter->has_next()) {
            if (nullptr == iter->get_value()) {
                return false;
            }
            TExtLiteral literal;
            if (!to_ext_literal(slot_desc->type().type, const_cast<void*>(iter->get_value()),
                                &literal)) {
                VLOG_CRITICAL << "get disjuncts fail: can't get literal, node_type="
                              << slot_desc->type().type;
                return false;
            }
            in_pred_values.push_back(literal);
            iter->next();
        }
        ext_in_predicate.__set_values(in_pred_values);
        TExtPredicate predicate;
        predicate.__set_node_type(TExprNodeType::IN_PRED);
        predicate.__set_in_predicate(ext_in_predicate);
        disjuncts.push_back(std::move(predicate));
        return true;
    } else if (TExprNodeType::COMPOUND_PRED == conjunct->node_type()) {
        if (TExprOpcode::COMPOUND_OR != conjunct->op()) {
            VLOG_CRITICAL << "get disjuncts fail: op is not COMPOUND_OR";
            return false;
        }
        if (!get_disjuncts(context, conjunct->get_child(0), disjuncts)) {
            return false;
        }
        if (!get_disjuncts(context, conjunct->get_child(1), disjuncts)) {
            return false;
        }
        return true;
    } else {
        VLOG_CRITICAL << "get disjuncts fail: node type is " << conjunct->node_type()
                      << ", should be BINARY_PRED or COMPOUND_PRED";
        return false;
    }
}

bool EsScanNode::is_match_func(Expr* conjunct) {
    if (TExprNodeType::FUNCTION_CALL == conjunct->node_type() &&
        conjunct->fn().name.function_name == "esquery") {
        return true;
    }
    return false;
}

SlotDescriptor* EsScanNode::get_slot_desc(SlotRef* slotRef) {
    std::vector<SlotId> slot_ids;
    slotRef->get_slot_ids(&slot_ids);
    SlotDescriptor* slot_desc = nullptr;
    for (SlotDescriptor* slot : _tuple_desc->slots()) {
        if (slot->id() == slot_ids[0]) {
            slot_desc = slot;
            break;
        }
    }
    return slot_desc;
}

bool EsScanNode::to_ext_literal(ExprContext* context, Expr* expr, TExtLiteral* literal) {
    switch (expr->node_type()) {
    case TExprNodeType::BOOL_LITERAL:
    case TExprNodeType::INT_LITERAL:
    case TExprNodeType::LARGE_INT_LITERAL:
    case TExprNodeType::FLOAT_LITERAL:
    case TExprNodeType::DECIMAL_LITERAL:
    case TExprNodeType::STRING_LITERAL:
    case TExprNodeType::DATE_LITERAL:
        return to_ext_literal(expr->type().type, context->get_value(expr, nullptr), literal);
    default:
        return false;
    }
}

bool EsScanNode::to_ext_literal(PrimitiveType slot_type, void* value, TExtLiteral* literal) {
    TExprNodeType::type node_type;
    switch (slot_type) {
    case TYPE_BOOLEAN: {
        node_type = (TExprNodeType::BOOL_LITERAL);
        TBoolLiteral bool_literal;
        bool_literal.__set_value(*reinterpret_cast<bool*>(value));
        literal->__set_bool_literal(bool_literal);
        break;
    }

    case TYPE_TINYINT: {
        node_type = (TExprNodeType::INT_LITERAL);
        TIntLiteral int_literal;
        int_literal.__set_value(*reinterpret_cast<int8_t*>(value));
        literal->__set_int_literal(int_literal);
        break;
    }
    case TYPE_SMALLINT: {
        node_type = (TExprNodeType::INT_LITERAL);
        TIntLiteral int_literal;
        int_literal.__set_value(*reinterpret_cast<int16_t*>(value));
        literal->__set_int_literal(int_literal);
        break;
    }
    case TYPE_INT: {
        node_type = (TExprNodeType::INT_LITERAL);
        TIntLiteral int_literal;
        int_literal.__set_value(*reinterpret_cast<int32_t*>(value));
        literal->__set_int_literal(int_literal);
        break;
    }
    case TYPE_BIGINT: {
        node_type = (TExprNodeType::INT_LITERAL);
        TIntLiteral int_literal;
        int_literal.__set_value(*reinterpret_cast<int64_t*>(value));
        literal->__set_int_literal(int_literal);
        break;
    }

    case TYPE_LARGEINT: {
        node_type = (TExprNodeType::LARGE_INT_LITERAL);
        TLargeIntLiteral large_int_literal;
        large_int_literal.__set_value(
                LargeIntValue::to_string(*reinterpret_cast<__int128*>(value)));
        literal->__set_large_int_literal(large_int_literal);
        break;
    }

    case TYPE_FLOAT: {
        node_type = (TExprNodeType::FLOAT_LITERAL);
        TFloatLiteral float_literal;
        float_literal.__set_value(*reinterpret_cast<float*>(value));
        literal->__set_float_literal(float_literal);
        break;
    }
    case TYPE_DOUBLE: {
        node_type = (TExprNodeType::FLOAT_LITERAL);
        TFloatLiteral float_literal;
        float_literal.__set_value(*reinterpret_cast<double*>(value));
        literal->__set_float_literal(float_literal);
        break;
    }

    case TYPE_DATE:
    case TYPE_DATETIME: {
        node_type = (TExprNodeType::DATE_LITERAL);
        const DateTimeValue date_value = *reinterpret_cast<DateTimeValue*>(value);
        char str[MAX_DTVALUE_STR_LEN];
        date_value.to_string(str);
        TDateLiteral date_literal;
        date_literal.__set_value(str);
        literal->__set_date_literal(date_literal);
        break;
    }

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        node_type = (TExprNodeType::STRING_LITERAL);
        TStringLiteral string_literal;
        string_literal.__set_value((reinterpret_cast<StringValue*>(value))->debug_string());
        literal->__set_string_literal(string_literal);
        break;
    }

    default: {
        DCHECK(false) << "Invalid type.";
        return false;
    }
    }
    literal->__set_node_type(node_type);
    return true;
}

Status EsScanNode::get_next_from_es(TExtGetNextResult& result) {
    TExtGetNextParams params;
    params.__set_scan_handle(_scan_handles[_scan_range_idx]);
    params.__set_offset(_offsets[_scan_range_idx]);

    // getNext
    const TNetworkAddress& address = _addresses[_scan_range_idx];
#ifndef BE_TEST
    try {
        Status create_client_status;
        ExtDataSourceServiceClientCache* client_cache = _env->extdatasource_client_cache();
        ExtDataSourceServiceConnection client(client_cache, address, 10000, &create_client_status);
        if (!create_client_status.ok()) {
            LOG(WARNING) << "es create client error: scan_range_idx=" << _scan_range_idx
                         << ", address=" << address
                         << ", msg=" << create_client_status.get_error_msg();
            return create_client_status;
        }

        try {
            VLOG_CRITICAL << "es get_next param=" << apache::thrift::ThriftDebugString(params);
            client->getNext(result, params);
        } catch (apache::thrift::transport::TTransportException& e) {
            std::stringstream ss;
            ss << "es get_next error: scan_range_idx=" << _scan_range_idx << ", msg=" << e.what();
            LOG(WARNING) << ss.str();
            RETURN_IF_ERROR(client.reopen());
            return Status::ThriftRpcError(ss.str());
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "es get_next error: scan_range_idx=" << _scan_range_idx << ", msg=" << e.what();
        LOG(WARNING) << ss.str();
        return Status::ThriftRpcError(ss.str());
    }
#else
    TStatus status;
    result.__set_status(status);
    result.__set_eos(true);
    TExtColumnData col_data;
    std::vector<bool> is_null;
    is_null.push_back(false);
    col_data.__set_is_null(is_null);
    std::vector<int32_t> int_vals;
    int_vals.push_back(1);
    int_vals.push_back(2);
    col_data.__set_int_vals(int_vals);
    std::vector<TExtColumnData> cols;
    cols.push_back(col_data);
    TExtRowBatch rows;
    rows.__set_cols(cols);
    rows.__set_num_rows(2);
    result.__set_rows(rows);
    return Status(status);
#endif

    // check result
    VLOG_CRITICAL << "es get_next result=" << apache::thrift::ThriftDebugString(result);
    Status get_next_status(result.status);
    if (!get_next_status.ok()) {
        LOG(WARNING) << "es get_next error: scan_range_idx=" << _scan_range_idx
                     << ", address=" << address << ", msg=" << get_next_status.get_error_msg();
        return get_next_status;
    }
    if (!result.__isset.rows || !result.rows.__isset.num_rows) {
        std::stringstream ss;
        ss << "es get_next error: scan_range_idx=" << _scan_range_idx
           << ", msg=rows or num_rows not in result";
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

Status EsScanNode::materialize_row(MemPool* tuple_pool, Tuple* tuple,
                                   const std::vector<TExtColumnData>& cols, int row_idx,
                                   std::vector<int>& cols_next_val_idx) {
    tuple->init(_tuple_desc->byte_size());

    for (int i = 0; i < _tuple_desc->slots().size(); ++i) {
        const SlotDescriptor* slot_desc = _tuple_desc->slots()[i];

        if (!slot_desc->is_materialized()) {
            continue;
        }

        void* slot = tuple->get_slot(slot_desc->tuple_offset());
        const TExtColumnData& col = cols[i];

        if (col.is_null[row_idx]) {
            tuple->set_null(slot_desc->null_indicator_offset());
            continue;
        } else {
            tuple->set_not_null(slot_desc->null_indicator_offset());
        }

        int val_idx = cols_next_val_idx[i]++;
        switch (slot_desc->type().type) {
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            if (val_idx >= col.string_vals.size()) {
                return Status::InternalError(strings::Substitute(ERROR_INVALID_COL_DATA, "STRING"));
            }
            const string& val = col.string_vals[val_idx];
            size_t val_size = val.size();
            Status rst;
            char* buffer =
                    reinterpret_cast<char*>(tuple_pool->try_allocate_unaligned(val_size, &rst));
            if (UNLIKELY(buffer == nullptr)) {
                std::string details = strings::Substitute(
                        ERROR_MEM_LIMIT_EXCEEDED, "MaterializeNextRow", val_size, "string slot");
                RETURN_LIMIT_EXCEEDED(tuple_pool->mem_tracker(), nullptr, details, val_size, rst);
            }
            memcpy(buffer, val.data(), val_size);
            reinterpret_cast<StringValue*>(slot)->ptr = buffer;
            reinterpret_cast<StringValue*>(slot)->len = val_size;
            break;
        }
        case TYPE_TINYINT:
            if (val_idx >= col.byte_vals.size()) {
                return Status::InternalError(
                        strings::Substitute(ERROR_INVALID_COL_DATA, "TINYINT"));
            }
            *reinterpret_cast<int8_t*>(slot) = col.byte_vals[val_idx];
            break;
        case TYPE_SMALLINT:
            if (val_idx >= col.short_vals.size()) {
                return Status::InternalError(
                        strings::Substitute(ERROR_INVALID_COL_DATA, "SMALLINT"));
            }
            *reinterpret_cast<int16_t*>(slot) = col.short_vals[val_idx];
            break;
        case TYPE_INT:
            if (val_idx >= col.int_vals.size()) {
                return Status::InternalError(strings::Substitute(ERROR_INVALID_COL_DATA, "INT"));
            }
            *reinterpret_cast<int32_t*>(slot) = col.int_vals[val_idx];
            break;
        case TYPE_BIGINT:
            if (val_idx >= col.long_vals.size()) {
                return Status::InternalError(strings::Substitute(ERROR_INVALID_COL_DATA, "BIGINT"));
            }
            *reinterpret_cast<int64_t*>(slot) = col.long_vals[val_idx];
            break;
        case TYPE_LARGEINT:
            if (val_idx >= col.long_vals.size()) {
                return Status::InternalError(
                        strings::Substitute(ERROR_INVALID_COL_DATA, "LARGEINT"));
            }
            *reinterpret_cast<int128_t*>(slot) = col.long_vals[val_idx];
            break;
        case TYPE_DOUBLE:
            if (val_idx >= col.double_vals.size()) {
                return Status::InternalError(strings::Substitute(ERROR_INVALID_COL_DATA, "DOUBLE"));
            }
            *reinterpret_cast<double*>(slot) = col.double_vals[val_idx];
            break;
        case TYPE_FLOAT:
            if (val_idx >= col.double_vals.size()) {
                return Status::InternalError(strings::Substitute(ERROR_INVALID_COL_DATA, "FLOAT"));
            }
            *reinterpret_cast<float*>(slot) = col.double_vals[val_idx];
            break;
        case TYPE_BOOLEAN:
            if (val_idx >= col.bool_vals.size()) {
                return Status::InternalError(
                        strings::Substitute(ERROR_INVALID_COL_DATA, "BOOLEAN"));
            }
            *reinterpret_cast<int8_t*>(slot) = col.bool_vals[val_idx];
            break;
        case TYPE_DATE:
            if (val_idx >= col.long_vals.size() ||
                !reinterpret_cast<DateTimeValue*>(slot)->from_unixtime(col.long_vals[val_idx],
                                                                       "+08:00")) {
                return Status::InternalError(
                        strings::Substitute(ERROR_INVALID_COL_DATA, "TYPE_DATE"));
            }
            reinterpret_cast<DateTimeValue*>(slot)->cast_to_date();
            break;
        case TYPE_DATETIME: {
            if (val_idx >= col.long_vals.size() ||
                !reinterpret_cast<DateTimeValue*>(slot)->from_unixtime(col.long_vals[val_idx],
                                                                       "+08:00")) {
                return Status::InternalError(
                        strings::Substitute(ERROR_INVALID_COL_DATA, "TYPE_DATETIME"));
            }
            reinterpret_cast<DateTimeValue*>(slot)->set_type(TIME_DATETIME);
            break;
        }
        default:
            DCHECK(false);
        }
    }
    return Status::OK();
}

} // namespace doris
