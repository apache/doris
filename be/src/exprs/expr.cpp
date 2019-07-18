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

#include "exprs/expr.h"

#include <sstream>
#include <vector>
#include <thrift/protocol/TDebugProtocol.h>
#include <llvm/Support/InstIterator.h>

#include "codegen/codegen_anyval.h"
#include "codegen/llvm_codegen.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/anyval_util.h"
#include "exprs/literal.h"
#include "exprs/binary_predicate.h"
#include "exprs/case_expr.h"
#include "exprs/cast_expr.h"
#include "exprs/compound_predicate.h"
#include "exprs/conditional_functions.h"
#include "exprs/in_predicate.h"
#include "exprs/arithmetic_expr.h"
#include "exprs/is_null_predicate.h"
#include "exprs/null_literal.h"
#include "exprs/info_func.h"
#include "exprs/scalar_fn_call.h"
#include "exprs/tuple_is_null_predicate.h"
#include "exprs/slot_ref.h"
#include "exprs/aggregate_functions.h"
#include "exprs/slot_ref.h"
#include "exprs/aggregate_functions.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Data_types.h"
#include "runtime/runtime_state.h"
#include "runtime/raw_value.h"
#include "runtime/user_function_cache.h"
#include "util/debug_util.h"

#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PaloService_types.h"

using llvm::Function;
using llvm::Instruction;
using llvm::CallInst;
using llvm::ConstantInt;
using llvm::Value;
using std::vector;
namespace doris {

const char* Expr::_s_llvm_class_name = "class.doris::Expr";
const char* Expr::_s_get_constant_symbol_prefix = "_ZN4doris4Expr12get_constant";

template<class T>
bool parse_string(const std::string& str, T* val) {
    std::stringstream stream(str);
    stream >> *val;
    return !stream.fail();
}

void init_builtins_dummy() {
    // Call one function from each of the classes to pull all the symbols
    // from that class in.
    // TODO: is there a better way to do this?
    AggregateFunctions::init_null(NULL, NULL);
}

FunctionContext* Expr::register_function_context(
        ExprContext* ctx, RuntimeState* state, int varargs_buffer_size) {
    FunctionContext::TypeDesc return_type = AnyValUtil::column_type_to_type_desc(_type);
    std::vector<FunctionContext::TypeDesc> arg_types;
    for (int i = 0; i < _children.size(); ++i) {
        arg_types.push_back(AnyValUtil::column_type_to_type_desc(_children[i]->_type));
    }
    _fn_context_index = ctx->register_func(state, return_type, arg_types, varargs_buffer_size);
    return ctx->fn_context(_fn_context_index);
}

// No children here
Expr::Expr(const Expr& expr) 
        : _cache_entry(expr._cache_entry),
        _node_type(expr._node_type),
        _opcode(expr._opcode),
        _is_slotref(expr._is_slotref),
        _type(expr._type),
        _output_scale(expr._output_scale),
        _output_column(expr._output_column),
        _fn(expr._fn),
        _fn_context_index(expr._fn_context_index),
        _ir_compute_fn(expr._ir_compute_fn),
        _constant_val(expr._constant_val),
        _vector_compute_fn(expr._vector_compute_fn) {
}

Expr::Expr(const TypeDescriptor& type) :
        _opcode(TExprOpcode::INVALID_OPCODE),
        // _vector_opcode(TExprOpcode::INVALID_OPCODE),
        _is_slotref(false),
        _type(type),
        _output_scale(-1),
        _output_column(-1),
        _fn_context_index(-1),
        _ir_compute_fn(NULL) {
    switch (_type.type) {
    case TYPE_BOOLEAN:
        _node_type = (TExprNodeType::BOOL_LITERAL);
        break;

    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        _node_type = (TExprNodeType::INT_LITERAL);
        break;

    case TYPE_LARGEINT:
        _node_type = (TExprNodeType::LARGE_INT_LITERAL);
        break;

    case TYPE_NULL:
        _node_type = (TExprNodeType::NULL_LITERAL);
        break;

    case TYPE_FLOAT:
    case TYPE_DOUBLE:
        _node_type = (TExprNodeType::FLOAT_LITERAL);
        break;

    case TYPE_DECIMAL:
    case TYPE_DECIMALV2:
        _node_type = (TExprNodeType::DECIMAL_LITERAL);
        break;

    case TYPE_DATE:
    case TYPE_DATETIME:
        _node_type = (TExprNodeType::DATE_LITERAL);
        break;

    case TYPE_TIME:
        _node_type = (TExprNodeType::TIME_LITERAL);
        break;

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
        _node_type = (TExprNodeType::STRING_LITERAL);
        break;

    default:
        DCHECK(false) << "Invalid type.";
    }
}

Expr::Expr(const TypeDescriptor& type, bool is_slotref) :
        _opcode(TExprOpcode::INVALID_OPCODE),
        // _vector_opcode(TExprOpcode::INVALID_OPCODE),
        _is_slotref(is_slotref),
        _type(type),
        _output_scale(-1),
        _output_column(-1),
        _fn_context_index(-1),
        _ir_compute_fn(NULL) {
    if (is_slotref) {
        _node_type = (TExprNodeType::SLOT_REF);
    } else {
        switch (_type.type) {
        case TYPE_BOOLEAN:
            _node_type = (TExprNodeType::BOOL_LITERAL);
            break;

        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
            _node_type = (TExprNodeType::INT_LITERAL);
            break;

        case TYPE_LARGEINT:
            _node_type = (TExprNodeType::LARGE_INT_LITERAL);
            break;

        case TYPE_NULL:
            _node_type = (TExprNodeType::NULL_LITERAL);
            break;

        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            _node_type = (TExprNodeType::FLOAT_LITERAL);
            break;

        case TYPE_DECIMAL:
        case TYPE_DECIMALV2:
            _node_type = (TExprNodeType::DECIMAL_LITERAL);
            break;

        case TYPE_DATETIME:
            _node_type = (TExprNodeType::DATE_LITERAL);
            break;

        case TYPE_TIME:
             _node_type = (TExprNodeType::TIME_LITERAL);
             break;

        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_HLL:
            _node_type = (TExprNodeType::STRING_LITERAL);
            break;

        default:
            DCHECK(false) << "Invalid type.";
        }
    }
}

Expr::Expr(const TExprNode& node) :
        _node_type(node.node_type),
        _opcode(node.__isset.opcode ? node.opcode : TExprOpcode::INVALID_OPCODE),
        // _vector_opcode(
                // node.__isset.vector_opcode ? node.vector_opcode : TExprOpcode::INVALID_OPCODE),
        _is_slotref(false),
        _type(TypeDescriptor::from_thrift(node.type)),
        _output_scale(node.output_scale),
        _output_column(node.__isset.output_column ? node.output_column : -1),
        _fn_context_index(-1),
        _ir_compute_fn(NULL) {
    if (node.__isset.fn) {
        _fn = node.fn;
    }
}

Expr::Expr(const TExprNode& node, bool is_slotref) :
        _node_type(node.node_type),
        _opcode(node.__isset.opcode ? node.opcode : TExprOpcode::INVALID_OPCODE),
        // _vector_opcode(
                // node.__isset.vector_opcode ? node.vector_opcode : TExprOpcode::INVALID_OPCODE),
        _is_slotref(is_slotref),
        _type(TypeDescriptor::from_thrift(node.type)),
        _output_scale(node.output_scale),
        _output_column(node.__isset.output_column ? node.output_column : -1),
        _fn_context_index(-1),
        _ir_compute_fn(NULL) {
    if (node.__isset.fn) {
        _fn = node.fn;
    }
}

Expr::~Expr() {
}

Status Expr::create_expr_tree(ObjectPool* pool, const TExpr& texpr, ExprContext** ctx) {
    // input is empty
    if (texpr.nodes.size() == 0) {
        *ctx = NULL;
        return Status::OK();
    }
    int node_idx = 0;
    Expr* e = NULL;
    Status status = create_tree_from_thrift(pool, texpr.nodes, NULL, &node_idx, &e, ctx);
    if (status.ok() && node_idx + 1 != texpr.nodes.size()) {
        status = Status::InternalError(
            "Expression tree only partially reconstructed. Not all thrift nodes were used.");
    }
    if (!status.ok()) {
        LOG(ERROR) << "Could not construct expr tree.\n" << status.get_error_msg() << "\n"
            << apache::thrift::ThriftDebugString(texpr);
    }
    return status;
}

Status Expr::create_expr_trees(
        ObjectPool* pool,
        const std::vector<TExpr>& texprs,
        std::vector<ExprContext*>* ctxs) {
    ctxs->clear();
    for (int i = 0; i < texprs.size(); ++i) {
        ExprContext* ctx = nullptr;
        RETURN_IF_ERROR(create_expr_tree(pool, texprs[i], &ctx));
        ctxs->push_back(ctx);
    }
    return Status::OK();
}

Status Expr::create_tree_from_thrift(
        ObjectPool* pool,
        const vector<TExprNode>& nodes,
        Expr* parent,
        int* node_idx,
        Expr** root_expr,
        ExprContext** ctx) {
    // propagate error case
    if (*node_idx >= nodes.size()) {
        return Status::InternalError("Failed to reconstruct expression tree from thrift.");
    }
    int num_children = nodes[*node_idx].num_children;
    Expr* expr = NULL;
    RETURN_IF_ERROR(create_expr(pool, nodes[*node_idx], &expr));
    DCHECK(expr != NULL);
    if (parent != NULL) {
        parent->add_child(expr);
    } else {
        DCHECK(root_expr != NULL);
        DCHECK(ctx != NULL);
        *root_expr = expr;
        *ctx = pool->add(new ExprContext(expr));
    }
    for (int i = 0; i < num_children; i++) {
        *node_idx += 1;
        RETURN_IF_ERROR(create_tree_from_thrift(pool, nodes, expr, node_idx, NULL, NULL));
        // we are expecting a child, but have used all nodes
        // this means we have been given a bad tree and must fail
        if (*node_idx >= nodes.size()) {
            return Status::InternalError("Failed to reconstruct expression tree from thrift.");
        }
    }
    return Status::OK();
}

Status Expr::create_expr(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr) {
    switch (texpr_node.node_type) {
    case TExprNodeType::BOOL_LITERAL:
    case TExprNodeType::INT_LITERAL:
    case TExprNodeType::LARGE_INT_LITERAL:
    case TExprNodeType::FLOAT_LITERAL:
    case TExprNodeType::DECIMAL_LITERAL:
    case TExprNodeType::DATE_LITERAL:
    case TExprNodeType::STRING_LITERAL:
    case TExprNodeType::TIME_LITERAL:
        *expr = pool->add(new Literal(texpr_node));
        return Status::OK();
    case TExprNodeType::COMPOUND_PRED:
        switch (texpr_node.opcode) {
        case TExprOpcode::COMPOUND_AND:
            *expr = pool->add(new AndPredicate(texpr_node));
            break;
        case TExprOpcode::COMPOUND_OR:
            *expr = pool->add(new OrPredicate(texpr_node));
            break;
        default:
            *expr = pool->add(new NotPredicate(texpr_node));
            break;
        }
        return Status::OK();
    case TExprNodeType::BINARY_PRED:
        *expr = pool->add(BinaryPredicate::from_thrift(texpr_node));
        return Status::OK();
    case TExprNodeType::NULL_LITERAL:
        *expr = pool->add(new NullLiteral(texpr_node));
        return Status::OK();
    case TExprNodeType::ARITHMETIC_EXPR:
        if (texpr_node.opcode != TExprOpcode::INVALID_OPCODE) {
            *expr = pool->add(ArithmeticExpr::from_thrift(texpr_node));
            return Status::OK();
        }
    case TExprNodeType::CAST_EXPR:
        if (texpr_node.__isset.child_type) {
            *expr = pool->add(CastExpr::from_thrift(texpr_node));
            return Status::OK();
        }
    case TExprNodeType::COMPUTE_FUNCTION_CALL:
    case TExprNodeType::FUNCTION_CALL:
        DCHECK(texpr_node.__isset.fn);
        if (texpr_node.fn.name.function_name == "if") {
            *expr = pool->add(new IfExpr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "nullif") {
            *expr = pool->add(new NullIfExpr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "ifnull") {
            *expr = pool->add(new IfNullExpr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "coalesce") {
            *expr = pool->add(new CoalesceExpr(texpr_node));
        } else {
            *expr = pool->add(new ScalarFnCall(texpr_node));
        }
        return Status::OK();
        //case TExprNodeType::AGG_EXPR: {
        //  if (!texpr_node.__isset.agg_expr) {
        //    return Status::InternalError("Aggregation expression not set in thrift node");
        //  }
        //  *expr = pool->add(new AggregateExpr(texpr_node));
        //  return Status::OK();
        //}

    case TExprNodeType::CASE_EXPR: {
        if (!texpr_node.__isset.case_expr) {
            return Status::InternalError("Case expression not set in thrift node");
        }

        *expr = pool->add(new CaseExpr(texpr_node));
        return Status::OK();
    }

    case TExprNodeType::IN_PRED: {
        switch (texpr_node.opcode) {
        case TExprOpcode::FILTER_IN:
        case TExprOpcode::FILTER_NOT_IN:
            *expr = pool->add(new InPredicate(texpr_node));
            break;
        default:
            *expr = pool->add(new ScalarFnCall(texpr_node));
            break;
        }
        return Status::OK();
    }

    case TExprNodeType::SLOT_REF: {
        if (!texpr_node.__isset.slot_ref) {
            return Status::InternalError("Slot reference not set in thrift node");
        }

        *expr = pool->add(new SlotRef(texpr_node));
        return Status::OK();
    }
    case TExprNodeType::TUPLE_IS_NULL_PRED: {
        *expr = pool->add(new TupleIsNullPredicate(texpr_node));
        return Status::OK();
    }

    case TExprNodeType::INFO_FUNC: {
        *expr = pool->add(new InfoFunc(texpr_node));
        return Status::OK();
    }
#if 0
    case TExprNodeType::FUNCTION_CALL: {
        if (!texpr_node.__isset.fn_call_expr) {
            return Status::InternalError("Udf call not set in thrift node");
        }

        if (texpr_node.fn_call_expr.fn.binary_type == TFunctionBinaryType::HIVE) {
            DCHECK(false);  //temp add, can't get here
            //*expr = pool->Add(new HiveUdfCall(texpr_node));
        } else {
            *expr = pool->add(new NativeUdfExpr(texpr_node));
        }

        return Status::OK();
    }
#endif

    default:
        std::stringstream os;
        os << "Unknown expr node type: " << texpr_node.node_type;
        return Status::InternalError(os.str());
    }
}

struct MemLayoutData {
    int expr_idx;
    int byte_size;
    bool variable_length;

    // TODO: sort by type as well?  Any reason to do this?
    bool operator<(const MemLayoutData& rhs) const {
        // variable_len go at end
        if (this->variable_length && !rhs.variable_length) {
            return false;
        }

        if (!this->variable_length && rhs.variable_length) {
            return true;
        }

        return this->byte_size < rhs.byte_size;
    }
};

int Expr::compute_results_layout(
        const std::vector<Expr*>& exprs,
        std::vector<int>* offsets,
        int* var_result_begin) {
    if (exprs.size() == 0) {
        *var_result_begin = -1;
        return 0;
    }

    std::vector<MemLayoutData> data;
    data.resize(exprs.size());

    // Collect all the byte sizes and sort them
    for (int i = 0; i < exprs.size(); ++i) {
        data[i].expr_idx = i;

        if (exprs[i]->type().type == TYPE_CHAR
                || exprs[i]->type().type == TYPE_VARCHAR) {
            data[i].byte_size = 16;
            data[i].variable_length = true;
        } else if (exprs[i]->type().type == TYPE_DECIMAL) {
            data[i].byte_size = get_byte_size(exprs[i]->type().type);

            // Although the current decimal has a fix-length, for the 
            // same value, it will work out different hash value due to the
            // different memory represent if the variable_length here is set 
            // to false, so we have to keep it.
            data[i].variable_length = true;
        } else {
            data[i].byte_size = get_byte_size(exprs[i]->type().type);
            data[i].variable_length = false;
        }

        DCHECK_NE(data[i].byte_size, 0);
    }

    sort(data.begin(), data.end());

    // Walk the types and store in a packed aligned layout
    int max_alignment = sizeof(int64_t);
    int current_alignment = data[0].byte_size;
    int byte_offset = 0;

    offsets->resize(exprs.size());
    offsets->clear();
    *var_result_begin = -1;

    for (int i = 0; i < data.size(); ++i) {
        DCHECK_GE(data[i].byte_size, current_alignment);

        // Don't align more than word (8-byte) size.  This is consistent with what compilers
        // do.
        if (data[i].byte_size != current_alignment && current_alignment != max_alignment) {
            byte_offset += data[i].byte_size - current_alignment;
            current_alignment = std::min(data[i].byte_size, max_alignment);
            // TODO(zc): fixed decimal align
            if (data[i].byte_size == 40) {
                current_alignment = 4;
            }
        }

        (*offsets)[data[i].expr_idx] = byte_offset;

        if (data[i].variable_length && *var_result_begin == -1) {
            *var_result_begin = byte_offset;
        }

        byte_offset += data[i].byte_size;
    }

    return byte_offset;
}

int Expr::compute_results_layout(
        const std::vector<ExprContext*>& ctxs,
        std::vector<int>* offsets,
        int* var_result_begin) {
    std::vector<Expr*> exprs;
    for (int i = 0; i < ctxs.size(); ++i) {
        exprs.push_back(ctxs[i]->root());
    }
    return compute_results_layout(exprs, offsets, var_result_begin);
}

Status Expr::prepare(
        const std::vector<ExprContext*>& ctxs,
        RuntimeState* state,
        const RowDescriptor& row_desc,
        MemTracker* tracker) {
    for (int i = 0; i < ctxs.size(); ++i) {
        RETURN_IF_ERROR(ctxs[i]->prepare(state, row_desc, tracker));
    }
    return Status::OK();
}

Status Expr::prepare(RuntimeState* state, const RowDescriptor& row_desc,
                     ExprContext* context) {
    DCHECK(_type.type != INVALID_TYPE);
    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->prepare(state, row_desc, context));
    }
    return Status::OK();
}

Status Expr::open(const std::vector<ExprContext*>& ctxs, RuntimeState* state) {
    for (int i = 0; i < ctxs.size(); ++i) {
        RETURN_IF_ERROR(ctxs[i]->open(state));
    }
    return Status::OK();
}

Status Expr::open(
        RuntimeState* state,
        ExprContext* context,
        FunctionContext::FunctionStateScope scope) {
    DCHECK(_type.type != INVALID_TYPE);
    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->open(state, context, scope));
    }
    return Status::OK();
}

void Expr::close(const std::vector<ExprContext*>& ctxs, RuntimeState* state) {
    for (int i = 0; i < ctxs.size(); ++i) {
        ctxs[i]->close(state);
    }
}

void Expr::close(
        RuntimeState* state,
        ExprContext* context,
        FunctionContext::FunctionStateScope scope) {
    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->close(state, context, scope);
    }
    // TODO(zc)
#if 0
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        // This is the final, non-cloned context to close. Clean up the whole Expr.
        if (cache_entry_ != NULL) {
            LibCache::instance()->DecrementUseCount(cache_entry_);
            cache_entry_ = NULL;
        }
    }
#endif
}

Status Expr::clone_if_not_exists(
        const std::vector<ExprContext*>& ctxs,
        RuntimeState* state,
        std::vector<ExprContext*>* new_ctxs) {
    DCHECK(new_ctxs != NULL);
    if (!new_ctxs->empty()) {
        // 'ctxs' was already cloned into '*new_ctxs', nothing to do.
        DCHECK_EQ(new_ctxs->size(), ctxs.size());
        for (int i = 0; i < new_ctxs->size(); ++i) {
            DCHECK((*new_ctxs)[i]->_is_clone);
        }
        return Status::OK();
    }
    new_ctxs->resize(ctxs.size());
    for (int i = 0; i < ctxs.size(); ++i) {
        RETURN_IF_ERROR(ctxs[i]->clone(state, &(*new_ctxs)[i]));
    }
    return Status::OK();
}

std::string Expr::debug_string() const {
    // TODO: implement partial debug string for member vars
    std::stringstream out;
    out << " type=" << _type.debug_string();

    if (_opcode != TExprOpcode::INVALID_OPCODE) {
        out << " opcode=" << _opcode;
    }

    out << " codegen=" << (_ir_compute_fn == NULL ? "false" : "true");

    if (!_children.empty()) {
        out << " children=" << debug_string(_children);
    }

    return out.str();
}

std::string Expr::debug_string(const std::vector<Expr*>& exprs) {
    std::stringstream out;
    out << "[";

    for (int i = 0; i < exprs.size(); ++i) {
        out << (i == 0 ? "" : " ") << exprs[i]->debug_string();
    }

    out << "]";
    return out.str();
}

std::string Expr::debug_string(const std::vector<ExprContext*>& ctxs) {
    std::vector<Expr*> exprs;
    for (int i = 0; i < ctxs.size(); ++i) {
        exprs.push_back(ctxs[i]->root());
    }
    return debug_string(exprs);
}

bool Expr::is_constant() const {
    for (int i = 0; i < _children.size(); ++i) {
        if (!_children[i]->is_constant()) {
            return false;
        }
    }

    return true;
}

bool Expr::is_vectorized() const {
    for (int i = 0; i < _children.size(); ++i) {
        if (!_children[i]->is_vectorized()) {
            return false;
        }
    }

    return true;
}

TExprNodeType::type Expr::type_without_cast(const Expr* expr) {
    if (expr->_opcode == TExprOpcode::CAST) {
        return type_without_cast(expr->_children[0]);
    }
    return expr->_node_type;
}

const Expr* Expr::expr_without_cast(const Expr* expr) {
    if (expr->_opcode == TExprOpcode::CAST) {
        return expr_without_cast(expr->_children[0]);
    }
    return expr;
}

doris_udf::AnyVal* Expr::get_const_val(ExprContext* context) {
    if (!is_constant()) {
        return NULL;
    }
    if (_constant_val.get() != NULL) {
        return _constant_val.get();
    }
    switch (_type.type) {
    case TYPE_BOOLEAN: {
        _constant_val.reset(new BooleanVal(get_boolean_val(context, NULL)));
        break;
    }
    case TYPE_TINYINT: {
        _constant_val.reset(new TinyIntVal(get_tiny_int_val(context, NULL)));
        break;
    }
    case TYPE_SMALLINT: {
        _constant_val.reset(new SmallIntVal(get_small_int_val(context, NULL)));
        break;
    }
    case TYPE_INT: {
        _constant_val.reset(new IntVal(get_int_val(context, NULL)));
        break;
    }
    case TYPE_BIGINT: {
        _constant_val.reset(new BigIntVal(get_big_int_val(context, NULL)));
        break;
    }
    case TYPE_LARGEINT: {
        _constant_val.reset(new LargeIntVal(get_large_int_val(context, NULL)));
        break;
    }
    case TYPE_FLOAT: {
        _constant_val.reset(new FloatVal(get_float_val(context, NULL)));
        break;
    }
    case TYPE_DOUBLE: {
        _constant_val.reset(new DoubleVal(get_double_val(context, NULL)));
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL: {
        _constant_val.reset(new StringVal(get_string_val(context, NULL)));
        break;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        _constant_val.reset(new DateTimeVal(get_datetime_val(context, NULL)));
        break;
    }
    case TYPE_TIME: {
        _constant_val.reset(new TimeVal(get_time_val(context, NULL)));
        break;
    }
    case TYPE_DECIMAL: {
        _constant_val.reset(new DecimalVal(get_decimal_val(context, NULL)));
        break;
    }
    case TYPE_DECIMALV2: {
        _constant_val.reset(new DecimalV2Val(get_decimalv2_val(context, NULL)));
        break;
    }
    case TYPE_NULL: {
        _constant_val.reset(new AnyVal(true));
        break;
    }
    default:
        DCHECK(false) << "Type not implemented: " << type();
    }
    DCHECK(_constant_val.get() != NULL);
    return _constant_val.get();
}

bool Expr::is_bound(std::vector<TupleId>* tuple_ids) const {
    for (int i = 0; i < _children.size(); ++i) {
        if (!_children[i]->is_bound(tuple_ids)) {
            return false;
        }
    }

    return true;
}

int Expr::get_slot_ids(std::vector<SlotId>* slot_ids) const {
    int n = 0;

    for (int i = 0; i < _children.size(); ++i) {
        n += _children[i]->get_slot_ids(slot_ids);
    }

    return n;
}

BooleanVal Expr::get_boolean_val(ExprContext* context, TupleRow* row) {
    return BooleanVal::null(); // (*(bool*)get_value(row));
}

TinyIntVal Expr::get_tiny_int_val(ExprContext* context, TupleRow* row) {
    return TinyIntVal::null(); // (*(int8_t*)get_value(row));
}

SmallIntVal Expr::get_small_int_val(ExprContext* context, TupleRow* row) {
    return SmallIntVal::null(); // (*(int16_t*)get_value(row));
}

IntVal Expr::get_int_val(ExprContext* context, TupleRow* row) {
    return IntVal::null(); // (*(int32_t*)get_value(row));
}

BigIntVal Expr::get_big_int_val(ExprContext* context, TupleRow* row) {
    return BigIntVal::null(); // (*(int64_t*)get_value(row));
}

LargeIntVal Expr::get_large_int_val(ExprContext* context, TupleRow* row) {
    return LargeIntVal::null(); // (*(int64_t*)get_value(row));
}

FloatVal Expr::get_float_val(ExprContext* context, TupleRow* row) {
    return FloatVal::null(); // (*(float*)get_value(row));
}

DoubleVal Expr::get_double_val(ExprContext* context, TupleRow* row) {
    return DoubleVal::null(); // (*(double*)get_value(row));
}

StringVal Expr::get_string_val(ExprContext* context, TupleRow* row) {
    StringVal val;
    // ((StringValue*)get_value(row))->to_string_val(&val);
    return val;
}

TimeVal Expr::get_time_val(ExprContext* context, TupleRow* row) {
    TimeVal val;
    return val;
}

// TODO(zc)
// virtual ArrayVal Expr::GetArrayVal(ExprContext* context, TupleRow*);
DateTimeVal Expr::get_datetime_val(ExprContext* context, TupleRow* row) {
    DateTimeVal val;
    // ((DateTimeValue*)get_value(row))->to_datetime_val(&val);
    return val;
}

DecimalVal Expr::get_decimal_val(ExprContext* context, TupleRow* row) {
    DecimalVal val;
    // ((DecimalValue*)get_value(row))->to_decimal_val(&val);
    return val;
}

DecimalV2Val Expr::get_decimalv2_val(ExprContext* context, TupleRow* row) {
    DecimalV2Val val;
    return val;
}

Status Expr::get_fn_context_error(ExprContext* ctx) {
    if (_fn_context_index != -1) {
        FunctionContext* fn_ctx = ctx->fn_context(_fn_context_index);
        if (fn_ctx->has_error()) {
            return Status::InternalError(fn_ctx->error_msg());
        }
    }
    return Status::OK();
}

llvm::Function* Expr::create_ir_function_prototype(
        LlvmCodeGen* codegen, const std::string& name, llvm::Value* (*args)[2]) {
    llvm::Type* return_type = CodegenAnyVal::get_lowered_type(codegen, type());
    LlvmCodeGen::FnPrototype prototype(codegen, name, return_type);
    prototype.add_argument(
        LlvmCodeGen::NamedVariable(
            "context", codegen->get_ptr_type(ExprContext::_s_llvm_class_name)));
    prototype.add_argument(
        LlvmCodeGen::NamedVariable("row", codegen->get_ptr_type(TupleRow::_s_llvm_class_name)));
    llvm::Function* function = prototype.generate_prototype(NULL, args[0]);
    DCHECK(function != NULL);
    return function;
}

llvm::Function* Expr::get_static_get_val_wrapper(
        const TypeDescriptor& type, LlvmCodeGen* codegen) {
    switch (type.type) {
    case TYPE_BOOLEAN:
        return codegen->get_function(IRFunction::EXPR_GET_BOOLEAN_VAL);
    case TYPE_TINYINT:
        return codegen->get_function(IRFunction::EXPR_GET_TINYINT_VAL);
    case TYPE_SMALLINT:
        return codegen->get_function(IRFunction::EXPR_GET_SMALLINT_VAL);
    case TYPE_INT:
        return codegen->get_function(IRFunction::EXPR_GET_INT_VAL);
    case TYPE_BIGINT:
        return codegen->get_function(IRFunction::EXPR_GET_BIGINT_VAL);
    case TYPE_LARGEINT:
        return codegen->get_function(IRFunction::EXPR_GET_LARGEINT_VAL);
    case TYPE_FLOAT:
        return codegen->get_function(IRFunction::EXPR_GET_FLOAT_VAL);
    case TYPE_DOUBLE:
        return codegen->get_function(IRFunction::EXPR_GET_DOUBLE_VAL);
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return codegen->get_function(IRFunction::EXPR_GET_STRING_VAL);
    case TYPE_DATE:
    case TYPE_DATETIME:
        return codegen->get_function(IRFunction::EXPR_GET_DATETIME_VAL);
    case TYPE_DECIMAL:
        return codegen->get_function(IRFunction::EXPR_GET_DECIMAL_VAL);
    default:
        DCHECK(false) << "Invalid type: " << type.debug_string();
        return NULL;
    }
}

Value* Expr::get_ir_constant(LlvmCodeGen* codegen, ExprConstant c, int i) {
    switch (c) {
    case RETURN_TYPE_SIZE:
        DCHECK_EQ(i, -1);
        return ConstantInt::get(codegen->get_type(TYPE_INT), _type.get_byte_size());
    case ARG_TYPE_SIZE:
        DCHECK_GE(i, 0);
        DCHECK_LT(i, _children.size());
        return ConstantInt::get(
            codegen->get_type(TYPE_INT), _children[i]->_type.get_byte_size());
    default:
        CHECK(false) << "NYI";
        return NULL;
    }
}

int Expr::inline_constants(LlvmCodeGen* codegen, Function* fn) {
    int replaced = 0;
    for (llvm::inst_iterator iter = llvm::inst_begin(fn), end = llvm::inst_end(fn);
            iter != end;) {
        // Increment iter now so we don't mess it up modifying the instrunction below
        Instruction* instr = &*(iter++);

        // Look for call instructions
        if (!llvm::isa<CallInst>(instr)) {
            continue;
        }
        CallInst* call_instr = llvm::cast<CallInst>(instr);
        Function* called_fn = call_instr->getCalledFunction();

        // Look for call to Expr::GetConstant()
        if (called_fn == NULL ||
            called_fn->getName().find(_s_get_constant_symbol_prefix) == std::string::npos) {
            continue;
        }

        // 'c' and 'i' arguments must be constant
        ConstantInt* c_arg = llvm::dyn_cast<ConstantInt>(call_instr->getArgOperand(1));
        ConstantInt* i_arg = llvm::dyn_cast<ConstantInt>(call_instr->getArgOperand(2));
        DCHECK(c_arg != NULL) << "Non-constant 'c' argument to Expr::GetConstant()";
        DCHECK(i_arg != NULL) << "Non-constant 'i' argument to Expr::GetConstant()";

        // Replace the called function with the appropriate constant
        ExprConstant c_val = static_cast<ExprConstant>(c_arg->getSExtValue());
        int i_val = static_cast<int>(i_arg->getSExtValue());
        call_instr->replaceAllUsesWith(get_ir_constant(codegen, c_val, i_val));
        call_instr->eraseFromParent();
        ++replaced;
    }
    return replaced;
}

Status Expr::get_codegend_compute_fn_wrapper(RuntimeState* state, llvm::Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }
    LlvmCodeGen* codegen = NULL;
    RETURN_IF_ERROR(state->get_codegen(&codegen));
    llvm::Function* static_getval_fn = get_static_get_val_wrapper(type(), codegen);

    // Call it passing this as the additional first argument.
    llvm::Value* args[2];
    _ir_compute_fn = create_ir_function_prototype(codegen, "codegen_compute_fn_wrapper", &args);
    llvm::BasicBlock* entry_block =
        llvm::BasicBlock::Create(codegen->context(), "entry", _ir_compute_fn);
    LlvmCodeGen::LlvmBuilder builder(entry_block);
    llvm::Value* this_ptr =
        codegen->cast_ptr_to_llvm_ptr(codegen->get_ptr_type(Expr::_s_llvm_class_name), this);
    llvm::Value* compute_fn_args[] = { this_ptr, args[0], args[1] };
    llvm::Value* ret = CodegenAnyVal::create_call(
        codegen, &builder, static_getval_fn, compute_fn_args, "ret", NULL);
    builder.CreateRet(ret);
    _ir_compute_fn = codegen->finalize_function(_ir_compute_fn);
    *fn = _ir_compute_fn;
    return Status::OK();
}

Expr* Expr::copy(ObjectPool* pool, Expr* old_expr) {
    auto new_expr = old_expr->clone(pool);
    for (auto child : old_expr->_children) {
        auto new_child = copy(pool, child);
        new_expr->_children.push_back(new_child);
    }
    return new_expr;
}

void Expr::assign_fn_ctx_idx(int* next_fn_ctx_idx) {
  _fn_ctx_idx_start = *next_fn_ctx_idx;
  if (has_fn_ctx()) {
    _fn_ctx_idx = *next_fn_ctx_idx;
    ++(*next_fn_ctx_idx);
  }
  for (Expr* child : children()) child->assign_fn_ctx_idx(next_fn_ctx_idx);
  _fn_ctx_idx_end = *next_fn_ctx_idx;
}


Status Expr::create(const TExpr& texpr, const RowDescriptor& row_desc,
    RuntimeState* state, ObjectPool* pool, Expr** scalar_expr,
    MemTracker* tracker) {
  *scalar_expr = nullptr;
  Expr* root;
  RETURN_IF_ERROR(create_expr(pool, texpr.nodes[0], &root));
  RETURN_IF_ERROR(create_tree(texpr, pool, root));
  // TODO pengyubing replace by Init()
  ExprContext* ctx = pool->add(new ExprContext(root));
  // TODO chenhao check node type in ScalarExpr Init()
  Status status = Status::OK();
  if (texpr.nodes[0].node_type != TExprNodeType::CASE_EXPR) {
      status = root->prepare(state, row_desc, ctx);
  }
  if (UNLIKELY(!status.ok())) {
    root->close();
    return status;
  }
  int fn_ctx_idx = 0;
  root->assign_fn_ctx_idx(&fn_ctx_idx);
  *scalar_expr = root;
  return Status::OK();
}

Status Expr::create(const vector<TExpr>& texprs, const RowDescriptor& row_desc,
    RuntimeState* state, ObjectPool* pool, vector<Expr*>* exprs, MemTracker* tracker) {
  exprs->clear();
  for (const TExpr& texpr: texprs) {
    Expr* expr;
    RETURN_IF_ERROR(create(texpr, row_desc, state, pool, &expr, tracker));
    DCHECK(expr != nullptr);
    exprs->push_back(expr);
  }
  return Status::OK();
}

Status Expr::create(const TExpr& texpr, const RowDescriptor& row_desc,
    RuntimeState* state, Expr** scalar_expr, MemTracker* tracker) {
  return Expr::create(texpr, row_desc, state, state->obj_pool(), scalar_expr, tracker);
}

Status Expr::create(const vector<TExpr>& texprs, const RowDescriptor& row_desc,
    RuntimeState* state, vector<Expr*>* exprs, MemTracker* tracker) {
  return Expr::create(texprs, row_desc, state, state->obj_pool(), exprs, tracker);
}

Status Expr::create_tree(const TExpr& texpr, ObjectPool* pool, Expr* root) {
  DCHECK(!texpr.nodes.empty());
  DCHECK(root != nullptr);
  // The root of the tree at nodes[0] is already created and stored in 'root'.
  int child_node_idx = 0;
  int num_children = texpr.nodes[0].num_children;
  for (int i = 0; i < num_children; ++i) {
    ++child_node_idx;
    Status status = create_tree_internal(texpr.nodes, pool, root, &child_node_idx);
    if (UNLIKELY(!status.ok())) {
      LOG(ERROR) << "Could not construct expr tree.\n" << status.get_error_msg() << "\n"
                 << apache::thrift::ThriftDebugString(texpr);
      return status;
    }
  }
  if (UNLIKELY(child_node_idx + 1 != texpr.nodes.size())) {
    return Status::InternalError("Expression tree only partially reconstructed. Not all thrift " \
                  "nodes were used.");
  }
  return Status::OK();
}

Status Expr::create_tree_internal(const vector<TExprNode>& nodes, ObjectPool* pool,
    Expr* root, int* child_node_idx) {
  // propagate error case
  if (*child_node_idx >= nodes.size()) {
    return Status::InternalError("Failed to reconstruct expression tree from thrift.");
  }

  const TExprNode& texpr_node = nodes[*child_node_idx];
  DCHECK_NE(texpr_node.node_type, TExprNodeType::AGG_EXPR);
  Expr* child_expr;
  RETURN_IF_ERROR(create_expr(pool, texpr_node, &child_expr));
  root->_children.push_back(child_expr);

  int num_children = nodes[*child_node_idx].num_children;
  for (int i = 0; i < num_children; ++i) {
    *child_node_idx += 1;
    RETURN_IF_ERROR(create_tree_internal(nodes, pool, child_expr, child_node_idx));
    DCHECK(child_expr->get_child(i) != nullptr);
  }
  return Status::OK();
}

// TODO chenhao
void Expr::close() {
    for (Expr* child : _children) child->close();
    /*if (_cache_entry != nullptr) {
      LibCache::instance()->decrement_use_count(_cache_entry);
      _cache_entry = nullptr;
      }*/
    if (_cache_entry != nullptr) {
        UserFunctionCache::instance()->release_entry(_cache_entry);
        _cache_entry = nullptr;
    }
}

void Expr::close(const vector<Expr*>& exprs) {
  for (Expr* expr : exprs) expr->close();
}

}
