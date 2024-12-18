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

#include "vec/exprs/vmatch_predicate.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif

#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <fmt/format.h>
#include <fmt/ranges.h> // IWYU pragma: keep
#include <gen_cpp/Exprs_types.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <string_view>
#include <vector>

#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#include "common/status.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris::vectorized {
using namespace doris::segment_v2;

VMatchPredicate::VMatchPredicate(const TExprNode& node) : VExpr(node) {
    _inverted_index_ctx = std::make_shared<InvertedIndexCtx>();
    _inverted_index_ctx->parser_type =
            get_inverted_index_parser_type_from_string(node.match_predicate.parser_type);
    _inverted_index_ctx->parser_mode = node.match_predicate.parser_mode;
    _inverted_index_ctx->char_filter_map = node.match_predicate.char_filter_map;
    _analyzer = InvertedIndexReader::create_analyzer(_inverted_index_ctx.get());
    _analyzer->set_lowercase(node.match_predicate.parser_lowercase);
    if (node.match_predicate.parser_stopwords == "none") {
        _analyzer->set_stopwords(nullptr);
    } else {
        _analyzer->set_stopwords(&lucene::analysis::standard95::stop_words);
    }
    _inverted_index_ctx->analyzer = _analyzer.get();
}

VMatchPredicate::~VMatchPredicate() = default;

Status VMatchPredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                                VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(_children.size());
    std::vector<std::string_view> child_expr_name;
    for (auto child : _children) {
        argument_template.emplace_back(nullptr, child->data_type(), child->expr_name());
        child_expr_name.emplace_back(child->expr_name());
    }

    _function = SimpleFunctionFactory::instance().get_function(
            _fn.name.function_name, argument_template, _data_type,
            {.enable_decimal256 = state->enable_decimal256()});
    if (_function == nullptr) {
        std::string type_str;
        for (auto arg : argument_template) {
            type_str = type_str + " " + arg.type->get_name();
        }
        return Status::NotSupported(
                "Function {} is not implemented, input param type is {}, "
                "and return type is {}.",
                _fn.name.function_name, type_str, _data_type->get_name());
    }

    VExpr::register_function_context(state, context);
    _expr_name = fmt::format("{}({})", _fn.name.function_name, child_expr_name);
    _function_name = _fn.name.function_name;
    _prepare_finished = true;
    return Status::OK();
}

Status VMatchPredicate::open(RuntimeState* state, VExprContext* context,
                             FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->open(state, context, scope));
    }
    RETURN_IF_ERROR(VExpr::init_function_context(context, scope, _function));
    if (scope == FunctionContext::THREAD_LOCAL || scope == FunctionContext::FRAGMENT_LOCAL) {
        context->fn_context(_fn_context_index)->set_function_state(scope, _inverted_index_ctx);
    }
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }
    _open_finished = true;
    return Status::OK();
}

void VMatchPredicate::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    VExpr::close_function_context(context, scope, _function);
    VExpr::close(context, scope);
}

Status VMatchPredicate::evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) {
    DCHECK_EQ(get_num_children(), 2);
    return _evaluate_inverted_index(context, _function, segment_num_rows);
}

Status VMatchPredicate::execute(VExprContext* context, Block* block, int* result_column_id) {
    DCHECK(_open_finished || _getting_const_col);
    if (fast_execute(context, block, result_column_id)) {
        return Status::OK();
    }
    DBUG_EXECUTE_IF("VMatchPredicate.execute", {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "{} not support slow path, hit debug point.", _expr_name);
    });
    DBUG_EXECUTE_IF("VMatchPredicate.must_in_slow_path", {
        auto debug_col_name = DebugPoints::instance()->get_debug_param_or_default<std::string>(
                "VMatchPredicate.must_in_slow_path", "column_name", "");

        std::vector<std::string> column_names;
        boost::split(column_names, debug_col_name, boost::algorithm::is_any_of(","));

        auto* column_slot_ref = assert_cast<VSlotRef*>(get_child(0).get());
        std::string column_name = column_slot_ref->expr_name();
        auto it = std::find(column_names.begin(), column_names.end(), column_name);
        if (it == column_names.end()) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "column {} should in slow path while VMatchPredicate::execute.", column_name);
        }
    })
    doris::vectorized::ColumnNumbers arguments(_children.size());
    for (int i = 0; i < _children.size(); ++i) {
        int column_id = -1;
        RETURN_IF_ERROR(_children[i]->execute(context, block, &column_id));
        arguments[i] = column_id;
    }
    // call function
    size_t num_columns_without_result = block->columns();
    // prepare a column to save result
    block->insert({nullptr, _data_type, _expr_name});
    RETURN_IF_ERROR(_function->execute(context->fn_context(_fn_context_index), *block, arguments,
                                       num_columns_without_result, block->rows(), false));
    *result_column_id = num_columns_without_result;
    return Status::OK();
}

const std::string& VMatchPredicate::expr_name() const {
    return _expr_name;
}

const std::string& VMatchPredicate::function_name() const {
    return _function_name;
}

std::string VMatchPredicate::debug_string() const {
    std::stringstream out;
    out << "MatchPredicate(" << children()[0]->debug_string() << ",[";
    int num_children = children().size();

    for (int i = 1; i < num_children; ++i) {
        out << (i == 1 ? "" : " ") << children()[i]->debug_string();
    }

    out << "])";
    return out.str();
}

} // namespace doris::vectorized