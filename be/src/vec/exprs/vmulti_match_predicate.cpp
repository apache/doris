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

#include "vec/exprs/vmulti_match_predicate.h"

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
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
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

VMultiMatchPredicate::VMultiMatchPredicate(const TExprNode& node) : VExpr(node) {
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

VMultiMatchPredicate::~VMultiMatchPredicate() = default;

Status VMultiMatchPredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                                     VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(_children.size());
    std::vector<std::string_view> child_expr_name;
    for (auto child : _children) {
        argument_template.emplace_back(nullptr, child->data_type(), child->expr_name());
        child_expr_name.emplace_back(child->expr_name());
    }
    // result column always not null
    if (_data_type->is_nullable()) {
        _function = SimpleFunctionFactory::instance().get_function(
                _fn.name.function_name, argument_template, remove_nullable(_data_type));
    } else {
        _function = SimpleFunctionFactory::instance().get_function(_fn.name.function_name,
                                                                   argument_template, _data_type);
    }
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
    _function_name = "match_phrase_prefix";
    _prepare_finished = true;
    return Status::OK();
}

Status VMultiMatchPredicate::open(RuntimeState* state, VExprContext* context,
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

void VMultiMatchPredicate::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    VExpr::close_function_context(context, scope, _function);
    VExpr::close(context, scope);
}

Result<segment_v2::InvertedIndexResultBitmap>
VMultiMatchPredicate::_evaluate_inverted_index_by_field(
        VExprContext* context, vectorized::ColumnsWithTypeAndName& arguments,
        uint32_t segment_num_rows, std::string field) const {
    auto result_bitmap = segment_v2::InvertedIndexResultBitmap();
    auto* iter = context->get_inverted_index_iterators_by_column_name(field);
    //column does not have inverted index
    if (iter == nullptr) {
        return result_bitmap;
    }
    auto storage_name_type = context->get_storage_name_and_type_by_column_name(field);

    auto st = _function->evaluate_inverted_index(arguments, storage_name_type, iter,
                                                 segment_num_rows, result_bitmap);
    if (!st.ok()) {
        return ResultError(st);
    }
    result_bitmap.mask_out_null();
    context->set_inverted_index_result_for_expr(this, result_bitmap);
    LOG(ERROR) << "expr " << _expr_name << " " << this << " evaluate_inverted_index result:"
               << result_bitmap.get_data_bitmap()->cardinality();
    return result_bitmap;
}

Status VMultiMatchPredicate::evaluate_inverted_index(VExprContext* context,
                                                     uint32_t segment_num_rows) const {
    DCHECK_EQ(get_num_children(), 4);
    std::set<std::string> columns_names;
    if (get_child(0)->is_slot_ref()) {
        auto* column_slot_ref = assert_cast<VSlotRef*>(get_child(0).get());
        columns_names.insert(column_slot_ref->expr_name());
    } else {
        return Status::NotSupported(
                "child 0 in evaluate_inverted_index for VMultiMatchPredicate must be slot ref, but "
                "we "
                "got {}",
                get_child(0)->expr_name());
    }
    if (get_child(1)->is_literal()) {
        auto* column_literal = assert_cast<VLiteral*>(get_child(1).get());
        auto field_names_str = column_literal->value();
        field_names_str.erase(std::remove_if(field_names_str.begin(), field_names_str.end(),
                                             [](unsigned char c) { return std::isspace(c); }),
                              field_names_str.end());
        std::vector<std::string> field_names;
        boost::split(field_names, field_names_str, boost::algorithm::is_any_of(","));
        for (const auto& field_name : field_names) {
            if (!field_name.empty()) {
                columns_names.insert(field_name);
            }
        }
    } else {
        return Status::NotSupported(
                "child 1 in evaluate_inverted_index for VMultiMatchPredicate must be "
                "literal, but we got {}",
                get_child(1)->expr_name());
    }
    if (get_child(2)->is_literal()) {
        auto* column_literal = assert_cast<VLiteral*>(get_child(2).get());
        auto match_type = column_literal->value();
        if (match_type != "phrase_prefix") {
            return Status::NotSupported("query type is incorrect, only support phrase_prefix");
        }
    } else {
        return Status::NotSupported(
                "child 2 in evaluate_inverted_index for VMultiMatchPredicate must be "
                "literal, but we got {}",
                get_child(2)->expr_name());
    }
    vectorized::ColumnsWithTypeAndName arguments;
    segment_v2::InvertedIndexResultBitmap ret;
    if (get_child(3)->is_literal()) {
        auto* column_literal = assert_cast<VLiteral*>(get_child(3).get());
        arguments.emplace_back(column_literal->get_column_ptr(), column_literal->get_data_type(),
                               column_literal->expr_name());
    } else {
        return Status::NotSupported(
                "child 3 in evaluate_inverted_index for VMultiMatchPredicate must be "
                "literal, but we got {}",
                get_child(3)->expr_name());
    }
    for (auto& col_name : columns_names) {
        auto result = DORIS_TRY(
                _evaluate_inverted_index_by_field(context, arguments, segment_num_rows, col_name));
        if (ret.is_empty()) {
            ret = std::move(result);
        } else {
            ret |= result;
        }
    }
    if (!ret.is_empty()) {
        context->set_inverted_index_result_for_expr(this, ret);
    }
    return Status::OK();
}

Status VMultiMatchPredicate::execute(VExprContext* context, Block* block, int* result_column_id) {
    return Status::NotSupported("not support for VMultiMatchPredicate::execute");
}

const std::string& VMultiMatchPredicate::expr_name() const {
    return _expr_name;
}

const std::string& VMultiMatchPredicate::function_name() const {
    return _function_name;
}

std::string VMultiMatchPredicate::debug_string() const {
    std::stringstream out;
    out << "MultiMatchPredicate(" << children()[0]->debug_string() << ",[";
    int num_children = children().size();

    for (int i = 1; i < num_children; ++i) {
        out << (i == 1 ? "" : " ") << children()[i]->debug_string();
    }

    out << "])";
    return out.str();
}

} // namespace doris::vectorized