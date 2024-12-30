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

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/large_int_value.h"
#include "runtime/types.h"
#include "udf/udf.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/core/wide_integer.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/functions/function.h"

namespace doris {
class BitmapFilterFuncBase;
class BloomFilterFuncBase;
class HybridSetBase;
class ObjectPool;
class RowDescriptor;
class RuntimeState;

namespace vectorized {

#define RETURN_IF_ERROR_OR_PREPARED(stmt) \
    if (_prepared) {                      \
        return Status::OK();              \
    }                                     \
    _prepared = true;                     \
    RETURN_IF_ERROR(stmt);

// VExpr should be used as shared pointer because it will be passed between classes
// like runtime filter to scan node, or from scannode to scanner. We could not make sure
// the relatioinship between threads and classes.
class VExpr {
public:
    // resize inserted param column to make sure column size equal to block.rows()
    // and return param column index
    static size_t insert_param(Block* block, ColumnWithTypeAndName&& elem, size_t size) {
        // usually elem.column always is const column, so we just clone it.
        elem.column = elem.column->clone_resized(size);
        block->insert(std::move(elem));
        return block->columns() - 1;
    }

    static bool is_acting_on_a_slot(const VExpr& expr);

    VExpr(const TExprNode& node);
    VExpr(const VExpr& vexpr);
    VExpr(TypeDescriptor type, bool is_slotref, bool is_nullable);
    // only used for test
    VExpr() = default;
    virtual ~VExpr() = default;

    virtual const std::string& expr_name() const = 0;
    virtual std::string expr_label() { return ""; }

    /// Initializes this expr instance for execution. This does not include initializing
    /// state in the VExprContext; 'context' should only be used to register a
    /// FunctionContext via RegisterFunctionContext().
    ///
    /// Subclasses overriding this function should call VExpr::Prepare() to recursively call
    /// Prepare() on the expr tree
    /// row_desc used in vslot_ref and some subclass to specify column
    virtual Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                           VExprContext* context);

    /// Initializes 'context' for execution. If scope if FRAGMENT_LOCAL, both fragment- and
    /// thread-local state should be initialized. Otherwise, if scope is THREAD_LOCAL, only
    /// thread-local state should be initialized.
    //
    /// Subclasses overriding this function should call VExpr::Open() to recursively call
    /// Open() on the expr tree
    virtual Status open(RuntimeState* state, VExprContext* context,
                        FunctionContext::FunctionStateScope scope);

    // before execute, check if expr has been parepared+opened.
    [[maybe_unused]] Status ready_status() const {
        if (_prepare_finished && _open_finished) {
            return Status::OK();
        }
        return Status::InternalError(expr_name() + " is not ready when execute");
    }

    virtual Status execute(VExprContext* context, Block* block, int* result_column_id) = 0;

    // execute current expr with inverted index to filter block. Given a roaring bitmap of match rows
    virtual Status evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) {
        return Status::OK();
    }

    Status _evaluate_inverted_index(VExprContext* context, const FunctionBasePtr& function,
                                    uint32_t segment_num_rows);

    // Only the 4th parameter is used in the runtime filter. In and MinMax need overwrite the
    // interface
    virtual Status execute_runtime_fitler(VExprContext* context, Block* block,
                                          int* result_column_id, std::vector<size_t>& args) {
        return execute(context, block, result_column_id);
    };

    /// Subclasses overriding this function should call VExpr::Close().
    //
    /// If scope if FRAGMENT_LOCAL, both fragment- and thread-local state should be torn
    /// down. Otherwise, if scope is THREAD_LOCAL, only thread-local state should be torn
    /// down.
    virtual void close(VExprContext* context, FunctionContext::FunctionStateScope scope);

    DataTypePtr& data_type() { return _data_type; }

    TypeDescriptor type() { return _type; }

    bool is_slot_ref() const { return _node_type == TExprNodeType::SLOT_REF; }

    bool is_column_ref() const { return _node_type == TExprNodeType::COLUMN_REF; }

    virtual bool is_literal() const { return false; }

    TExprNodeType::type node_type() const { return _node_type; }

    TExprOpcode::type op() const { return _opcode; }

    void add_child(const VExprSPtr& expr) { _children.push_back(expr); }
    VExprSPtr get_child(int i) const { return _children[i]; }
    int get_num_children() const { return _children.size(); }

    static Status create_expr_tree(const TExpr& texpr, VExprContextSPtr& ctx);

    static Status create_expr_trees(const std::vector<TExpr>& texprs, VExprContextSPtrs& ctxs);

    static Status prepare(const VExprContextSPtrs& ctxs, RuntimeState* state,
                          const RowDescriptor& row_desc);

    static Status open(const VExprContextSPtrs& ctxs, RuntimeState* state);

    static Status clone_if_not_exists(const VExprContextSPtrs& ctxs, RuntimeState* state,
                                      VExprContextSPtrs& new_ctxs);

    bool is_nullable() const { return _data_type->is_nullable(); }

    PrimitiveType result_type() const { return _type.type; }

    static Status create_expr(const TExprNode& expr_node, VExprSPtr& expr);

    static Status create_tree_from_thrift(const std::vector<TExprNode>& nodes, int* node_idx,
                                          VExprSPtr& root_expr, VExprContextSPtr& ctx);

    static Status check_expr_output_type(const VExprContextSPtrs& ctxs,
                                         const RowDescriptor& output_row_desc);
    virtual const VExprSPtrs& children() const { return _children; }
    void set_children(const VExprSPtrs& children) { _children = children; }
    void set_children(VExprSPtrs&& children) { _children = std::move(children); }
    virtual std::string debug_string() const;
    static std::string debug_string(const VExprSPtrs& exprs);
    static std::string debug_string(const VExprContextSPtrs& ctxs);

    void set_getting_const_col(bool val = true) { _getting_const_col = val; }

    bool is_and_expr() const { return _fn.name.function_name == "and"; }

    virtual bool is_compound_predicate() const { return false; }

    const TFunction& fn() const { return _fn; }

    /// Returns true if expr doesn't contain slotrefs, i.e., can be evaluated
    /// with get_value(NULL). The default implementation returns true if all of
    /// the children are constant.
    virtual bool is_constant() const;

    /// If this expr is constant, evaluates the expr with no input row argument and returns
    /// the output. Returns nullptr if the argument is not constant. The returned ColumnPtr is
    /// owned by this expr. This should only be called after Open() has been called on this
    /// expr.
    Status get_const_col(VExprContext* context, std::shared_ptr<ColumnPtrWrapper>* column_wrapper);

    int fn_context_index() const { return _fn_context_index; }

    static const VExprSPtr expr_without_cast(const VExprSPtr& expr) {
        if (expr->node_type() == TExprNodeType::CAST_EXPR) {
            return expr_without_cast(expr->_children[0]);
        }
        return expr;
    }

    // If this expr is a RuntimeFilterWrapper, this method will return an underlying rf expression
    virtual const VExprSPtr get_impl() const { return {}; }

    // If this expr is a BloomPredicate, this method will return a BloomFilterFunc
    virtual std::shared_ptr<BloomFilterFuncBase> get_bloom_filter_func() const {
        LOG(FATAL) << "Method 'get_bloom_filter_func()' is not supported in expression: "
                   << this->debug_string();
        return nullptr;
    }

    virtual std::shared_ptr<HybridSetBase> get_set_func() const { return nullptr; }

    // If this expr is a BitmapPredicate, this method will return a BitmapFilterFunc
    virtual std::shared_ptr<BitmapFilterFuncBase> get_bitmap_filter_func() const {
        LOG(FATAL) << "Method 'get_bitmap_filter_func()' is not supported in expression: "
                   << this->debug_string();
        return nullptr;
    }

    // fast_execute can direct copy expr filter result which build by apply index in segment_iterator
    bool fast_execute(doris::vectorized::VExprContext* context, doris::vectorized::Block* block,
                      int* result_column_id);

    virtual bool can_push_down_to_index() const { return false; }
    virtual bool equals(const VExpr& other);
    void set_index_unique_id(uint32_t index_unique_id) { _index_unique_id = index_unique_id; }
    uint32_t index_unique_id() const { return _index_unique_id; }

protected:
    /// Simple debug string that provides no expr subclass-specific information
    std::string debug_string(const std::string& expr_name) const {
        std::stringstream out;
        out << expr_name << "(" << VExpr::debug_string() << ")";
        return out.str();
    }

    std::string get_child_names() {
        std::string res;
        for (auto child : _children) {
            if (!res.empty()) {
                res += ", ";
            }
            res += child->expr_name();
        }
        return res;
    }

    bool is_const_and_have_executed() { return (is_constant() && (_constant_col != nullptr)); }

    Status get_result_from_const(vectorized::Block* block, const std::string& expr_name,
                                 int* result_column_id);

    Status check_constant(const Block& block, ColumnNumbers arguments) const;

    /// Helper function that calls ctx->register(), sets fn_context_index_, and returns the
    /// registered FunctionContext
    void register_function_context(RuntimeState* state, VExprContext* context);

    /// Helper function to initialize function context, called in `open` phase of VExpr:
    /// 1. Set constant columns result of function arguments.
    /// 2. Call function's prepare() to initialize function state, fragment-local or
    /// thread-local according the input `FunctionStateScope` argument.
    Status init_function_context(VExprContext* context, FunctionContext::FunctionStateScope scope,
                                 const FunctionBasePtr& function) const;

    /// Helper function to close function context, fragment-local or thread-local according
    /// the input `FunctionStateScope` argument. Called in `close` phase of VExpr.
    void close_function_context(VExprContext* context, FunctionContext::FunctionStateScope scope,
                                const FunctionBasePtr& function) const;

    TExprNodeType::type _node_type;
    // Used to check what opcode
    TExprOpcode::type _opcode;
    TypeDescriptor _type;
    DataTypePtr _data_type;
    VExprSPtrs _children;
    TFunction _fn;

    /// Index to pass to ExprContext::fn_context() to retrieve this expr's FunctionContext.
    /// Set in RegisterFunctionContext(). -1 if this expr does not need a FunctionContext and
    /// doesn't call RegisterFunctionContext().
    int _fn_context_index = -1;

    // If this expr is constant, this will store and cache the value generated by
    // get_const_col()
    std::shared_ptr<ColumnPtrWrapper> _constant_col;
    bool _prepared = false; // for base class VExpr
    bool _getting_const_col =
            false; // if true, current execute() is in prepare() (that is, can't check _prepared)
    // for concrete classes
    bool _prepare_finished = false;
    bool _open_finished = false;

    // ensuring uniqueness during index traversal
    uint32_t _index_unique_id = 0;
    bool _enable_inverted_index_query = true;
    uint32_t _in_list_value_count_threshold = 10;
};

} // namespace vectorized

// NOLINTBEGIN(readability-function-size)
template <PrimitiveType T>
Status create_texpr_literal_node(const void* data, TExprNode* node, int precision = 0,
                                 int scale = 0) {
    if constexpr (T == TYPE_BOOLEAN) {
        const auto* origin_value = reinterpret_cast<const bool*>(data);
        TBoolLiteral boolLiteral;
        (*node).__set_node_type(TExprNodeType::BOOL_LITERAL);
        boolLiteral.__set_value(*origin_value);
        (*node).__set_bool_literal(boolLiteral);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    } else if constexpr (T == TYPE_TINYINT) {
        const auto* origin_value = reinterpret_cast<const int8_t*>(data);
        (*node).__set_node_type(TExprNodeType::INT_LITERAL);
        TIntLiteral intLiteral;
        intLiteral.__set_value(*origin_value);
        (*node).__set_int_literal(intLiteral);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_TINYINT));
    } else if constexpr (T == TYPE_SMALLINT) {
        const auto* origin_value = reinterpret_cast<const int16_t*>(data);
        (*node).__set_node_type(TExprNodeType::INT_LITERAL);
        TIntLiteral intLiteral;
        intLiteral.__set_value(*origin_value);
        (*node).__set_int_literal(intLiteral);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_SMALLINT));
    } else if constexpr (T == TYPE_INT) {
        const auto* origin_value = reinterpret_cast<const int32_t*>(data);
        (*node).__set_node_type(TExprNodeType::INT_LITERAL);
        TIntLiteral intLiteral;
        intLiteral.__set_value(*origin_value);
        (*node).__set_int_literal(intLiteral);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_INT));
    } else if constexpr (T == TYPE_BIGINT) {
        const auto* origin_value = reinterpret_cast<const int64_t*>(data);
        (*node).__set_node_type(TExprNodeType::INT_LITERAL);
        TIntLiteral intLiteral;
        intLiteral.__set_value(*origin_value);
        (*node).__set_int_literal(intLiteral);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_BIGINT));
    } else if constexpr (T == TYPE_LARGEINT) {
        const auto* origin_value = reinterpret_cast<const int128_t*>(data);
        (*node).__set_node_type(TExprNodeType::LARGE_INT_LITERAL);
        TLargeIntLiteral large_int_literal;
        large_int_literal.__set_value(LargeIntValue::to_string(*origin_value));
        (*node).__set_large_int_literal(large_int_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_LARGEINT));
    } else if constexpr ((T == TYPE_DATE) || (T == TYPE_DATETIME) || (T == TYPE_TIME)) {
        const auto* origin_value = reinterpret_cast<const VecDateTimeValue*>(data);
        TDateLiteral date_literal;
        char convert_buffer[30];
        origin_value->to_string(convert_buffer);
        date_literal.__set_value(convert_buffer);
        (*node).__set_date_literal(date_literal);
        (*node).__set_node_type(TExprNodeType::DATE_LITERAL);
        if (origin_value->type() == TimeType::TIME_DATE) {
            (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DATE));
        } else if (origin_value->type() == TimeType::TIME_DATETIME) {
            (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DATETIME));
        } else if (origin_value->type() == TimeType::TIME_TIME) {
            (*node).__set_type(create_type_desc(PrimitiveType::TYPE_TIME));
        }
    } else if constexpr (T == TYPE_DATEV2) {
        const auto* origin_value = reinterpret_cast<const DateV2Value<DateV2ValueType>*>(data);
        TDateLiteral date_literal;
        char convert_buffer[30];
        origin_value->to_string(convert_buffer);
        date_literal.__set_value(convert_buffer);
        (*node).__set_date_literal(date_literal);
        (*node).__set_node_type(TExprNodeType::DATE_LITERAL);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DATEV2));
    } else if constexpr (T == TYPE_DATETIMEV2) {
        const auto* origin_value = reinterpret_cast<const DateV2Value<DateTimeV2ValueType>*>(data);
        TDateLiteral date_literal;
        char convert_buffer[30];
        origin_value->to_string(convert_buffer, scale);
        date_literal.__set_value(convert_buffer);
        (*node).__set_date_literal(date_literal);
        (*node).__set_node_type(TExprNodeType::DATE_LITERAL);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DATETIMEV2, precision, scale));
    } else if constexpr (T == TYPE_DECIMALV2) {
        const auto* origin_value = reinterpret_cast<const DecimalV2Value*>(data);
        (*node).__set_node_type(TExprNodeType::DECIMAL_LITERAL);
        TDecimalLiteral decimal_literal;
        decimal_literal.__set_value(origin_value->to_string());
        (*node).__set_decimal_literal(decimal_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DECIMALV2, precision, scale));
    } else if constexpr (T == TYPE_DECIMAL32) {
        const auto* origin_value = reinterpret_cast<const vectorized::Decimal<int32_t>*>(data);
        (*node).__set_node_type(TExprNodeType::DECIMAL_LITERAL);
        TDecimalLiteral decimal_literal;
        decimal_literal.__set_value(origin_value->to_string(precision, scale));
        (*node).__set_decimal_literal(decimal_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DECIMAL32, precision, scale));
    } else if constexpr (T == TYPE_DECIMAL64) {
        const auto* origin_value = reinterpret_cast<const vectorized::Decimal<int64_t>*>(data);
        (*node).__set_node_type(TExprNodeType::DECIMAL_LITERAL);
        TDecimalLiteral decimal_literal;
        decimal_literal.__set_value(origin_value->to_string(precision, scale));
        (*node).__set_decimal_literal(decimal_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DECIMAL64, precision, scale));
    } else if constexpr (T == TYPE_DECIMAL128I) {
        const auto* origin_value = reinterpret_cast<const vectorized::Decimal<int128_t>*>(data);
        (*node).__set_node_type(TExprNodeType::DECIMAL_LITERAL);
        TDecimalLiteral decimal_literal;
        // e.g. For a decimal(26,6) column, the initial value of the _min of the MinMax RF
        // on the RF producer side is an int128 value with 38 digits of 9, and this is the
        // final min value of the MinMax RF if the fragment instance has no data.
        // Need to truncate the value to the right precision and scale here, to avoid
        // error when casting string back to decimal later.
        // TODO: this is a temporary solution, the best solution is to produce the
        // right min max value at the producer side.
        decimal_literal.__set_value(origin_value->to_string(precision, scale));
        (*node).__set_decimal_literal(decimal_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DECIMAL128I, precision, scale));
    } else if constexpr (T == TYPE_DECIMAL256) {
        const auto* origin_value = reinterpret_cast<const vectorized::Decimal<wide::Int256>*>(data);
        (*node).__set_node_type(TExprNodeType::DECIMAL_LITERAL);
        TDecimalLiteral decimal_literal;
        decimal_literal.__set_value(origin_value->to_string(precision, scale));
        (*node).__set_decimal_literal(decimal_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DECIMAL256, precision, scale));
    } else if constexpr (T == TYPE_FLOAT) {
        const auto* origin_value = reinterpret_cast<const float*>(data);
        (*node).__set_node_type(TExprNodeType::FLOAT_LITERAL);
        TFloatLiteral float_literal;
        float_literal.__set_value(*origin_value);
        (*node).__set_float_literal(float_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_FLOAT));
    } else if constexpr (T == TYPE_DOUBLE) {
        const auto* origin_value = reinterpret_cast<const double*>(data);
        (*node).__set_node_type(TExprNodeType::FLOAT_LITERAL);
        TFloatLiteral float_literal;
        float_literal.__set_value(*origin_value);
        (*node).__set_float_literal(float_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DOUBLE));
    } else if constexpr ((T == TYPE_STRING) || (T == TYPE_CHAR) || (T == TYPE_VARCHAR)) {
        const auto* origin_value = reinterpret_cast<const StringRef*>(data);
        (*node).__set_node_type(TExprNodeType::STRING_LITERAL);
        TStringLiteral string_literal;
        string_literal.__set_value(origin_value->to_string());
        (*node).__set_string_literal(string_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_STRING));
    } else if constexpr (T == TYPE_IPV4) {
        const auto* origin_value = reinterpret_cast<const IPv4*>(data);
        (*node).__set_node_type(TExprNodeType::IPV4_LITERAL);
        TIPv4Literal literal;
        literal.__set_value(*origin_value);
        (*node).__set_ipv4_literal(literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_IPV4));
    } else if constexpr (T == TYPE_IPV6) {
        const auto* origin_value = reinterpret_cast<const IPv6*>(data);
        (*node).__set_node_type(TExprNodeType::IPV6_LITERAL);
        TIPv6Literal literal;
        literal.__set_value(vectorized::DataTypeIPv6::to_string(*origin_value));
        (*node).__set_ipv6_literal(literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_IPV6));
    } else {
        return Status::InvalidArgument("Invalid argument type!");
    }
    return Status::OK();
}
// NOLINTEND(readability-function-size)

TExprNode create_texpr_node_from(const void* data, const PrimitiveType& type, int precision = 0,
                                 int scale = 0);

} // namespace doris
