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

#include "exprs/vmatch_predicate.h"

#include <cstdint>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif

#include <fmt/format.h>
#include <fmt/ranges.h> // IWYU pragma: keep
#include <gen_cpp/Exprs_types.h>
#include <glog/logging.h>

#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "exprs/function/match.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/schema.h"
#include "storage/segment/segment.h"
#include "util/json/path_in_data.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris {
#include "common/compile_check_begin.h"

using namespace doris::segment_v2;

VMatchPredicate::VMatchPredicate(const TExprNode& node) : VExpr(node) {
    const auto resolved = AnalyzerConfigParser::parse(
            node.match_predicate.analyzer_name, node.match_predicate.parser_type,
            node.match_predicate.parser_mode, node.match_predicate.parser_lowercase,
            node.match_predicate.char_filter_map);

    InvertedIndexAnalyzerConfig config;
    config.analyzer_name = resolved.provider_name;
    config.parser_type = resolved.parser_type;
    config.parser_mode = node.match_predicate.parser_mode;
    config.char_filter_map = node.match_predicate.char_filter_map;
    if (node.match_predicate.parser_lowercase) {
        config.lower_case = INVERTED_INDEX_PARSER_TRUE;
    } else {
        config.lower_case = INVERTED_INDEX_PARSER_FALSE;
    }
    DBUG_EXECUTE_IF("inverted_index_parser.get_parser_lowercase_from_properties",
                    { config.lower_case = ""; })
    config.stop_words = node.match_predicate.parser_stopwords;

    _analyzer_ctx = std::make_shared<InvertedIndexAnalyzerCtx>();
    _analyzer_ctx->analyzer_key = resolved.analyzer_key;
    _analyzer_ctx->analyzer_name = resolved.provider_name;
    _analyzer_ctx->parser_type = resolved.parser_type;

    if (_analyzer_ctx->requires_analysis()) {
        std::string bound_name;
        std::string legacy_name;
        _analyzer_provider = inverted_index::InvertedIndexAnalyzer::create_analyzer_provider(
                &config, &bound_name, &legacy_name);
        _analyzer = _analyzer_provider->get_analyzer();
        if (!legacy_name.empty() && legacy_name != bound_name) {
            _analyzer_ctx->legacy_analyzer_key =
                    AnalyzerConfigParser::parse(legacy_name, node.match_predicate.parser_type,
                                                node.match_predicate.parser_mode,
                                                node.match_predicate.parser_lowercase,
                                                node.match_predicate.char_filter_map)
                            .analyzer_key;
        }
        if (bound_name != resolved.provider_name) {
            // Reader selection and query tokenization must use the same policy binding.
            _analyzer_ctx->analyzer_key =
                    AnalyzerConfigParser::parse(bound_name, node.match_predicate.parser_type,
                                                node.match_predicate.parser_mode,
                                                node.match_predicate.parser_lowercase,
                                                node.match_predicate.char_filter_map)
                            .analyzer_key;
            _analyzer_ctx->analyzer_name = std::move(bound_name);
        }
    }

    _analyzer_ctx->char_filter_map = std::move(config.char_filter_map);
    _analyzer_ctx->analyzer = _analyzer;
    _analyzer_ctx->analyzer_provider = _analyzer_provider;
}

VMatchPredicate::~VMatchPredicate() = default;

Status VMatchPredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                                VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(_children.size());
    std::vector<std::string_view> child_expr_name;
    for (const auto& child : _children) {
        argument_template.emplace_back(nullptr, child->data_type(), child->expr_name());
        child_expr_name.emplace_back(child->expr_name());
    }

    _function = SimpleFunctionFactory::instance().get_function(_fn.name.function_name,
                                                               argument_template, _data_type, {});
    if (_function == nullptr) {
        std::string type_str;
        for (const auto& arg : argument_template) {
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
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->open(state, context, scope));
    }
    RETURN_IF_ERROR(VExpr::init_function_context(state, context, scope, _function));
    if (scope == FunctionContext::THREAD_LOCAL || scope == FunctionContext::FRAGMENT_LOCAL) {
        context->fn_context(_fn_context_index)->set_function_state(scope, _analyzer_ctx);
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
    auto index_context = context != nullptr ? context->get_index_context() : nullptr;
    if (index_context != nullptr && _analyzer_ctx != nullptr) {
        index_context->set_analyzer_ctx_for_expr(this, _analyzer_ctx);
    }
    auto status = _evaluate_inverted_index(context, _function, segment_num_rows);
    if (status.ok() && index_context != nullptr &&
        !index_context->has_index_result_for_expr(this)) {
        RETURN_IF_ERROR(_evaluate_absent_variant_path(*index_context, segment_num_rows));
    }

    const auto& input = get_child(0);
    if (index_context != nullptr && input->is_slot_ref() && input->result_type() == TYPE_VARIANT) {
        const auto column_id = assert_cast<const VSlotRef*>(input.get())->column_id();
        const auto& query_context = index_context->get_index_query_context();
        const auto* segment = index_context->segment();
        if (query_context != nullptr && query_context->stats != nullptr && segment != nullptr &&
            (index_context->get_inverted_index_iterator_by_column_id(column_id) == nullptr ||
             status.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>())) {
            const auto format = segment->tablet_schema()->get_inverted_index_storage_format();
            auto diagnostic = fmt::format(
                    "[VariantValuesIndex] phase=query_index reason={} format={} rowset_id={} "
                    "segment_id={} column={}",
                    format == InvertedIndexStorageFormatPB::SNII ? "missing_index"
                                                                 : "legacy_storage_format",
                    InvertedIndexStorageFormatPB_Name(format), segment->rowset_id().to_string(),
                    segment->id(), input->expr_name());
            VLOG_DEBUG << diagnostic;
            query_context->stats->inverted_index_stats.add_binding_diagnostic(
                    std::move(diagnostic));
        }
    }
    return status;
}

// A MATCH on a VARIANT path that this segment does not store is NULL on every row, which the
// index can answer exactly. Without that answer the MATCH falls back to row evaluation, and
// score() then rejects an AND/OR whose other branches were scored. For example, in a segment
// written from {"title":"apple archive"}, `v['content'] MATCH 'search'` is answered here, so
// `v['title'] MATCH 'apple' OR v['content'] MATCH 'search'` can still score the title branch.
Status VMatchPredicate::_evaluate_absent_variant_path(IndexExecContext& index_context,
                                                      uint32_t segment_num_rows) {
    // The planner reads `v['content']` as a path slot, usually under a cast to the MATCH type.
    // Without a segment there is no VARIANT metadata to prove the path absent from.
    const auto input = expr_without_cast(get_child(0));
    if (!input->is_slot_ref() || index_context.segment() == nullptr) {
        return Status::OK();
    }
    // A path with an index iterator has already been answered or declined by its index, and
    // only an extracted VARIANT path can be absent from a segment.
    const int column_id = assert_cast<const VSlotRef*>(input.get())->column_id();
    const auto& read_schema = index_context.read_schema();
    const auto& column = *read_schema->column(read_schema->column_id(column_id));
    if (index_context.get_inverted_index_iterator_by_column_id(column_id) != nullptr ||
        !column.is_extracted_column()) {
        return Status::OK();
    }
    bool absent = false;
    RETURN_IF_ERROR(index_context.is_variant_path_absent(
            column_id, column.path_info_ptr()->copy_pop_front(), &absent));
    // A path that is stored but has no index, such as one in the sparse column, still needs
    // row evaluation.
    if (!absent) {
        return Status::OK();
    }
    auto null_rows = std::make_shared<roaring::Roaring>();
    null_rows->addRange(0, segment_num_rows);
    index_context.set_index_result_for_expr(
            this,
            InvertedIndexResultBitmap(std::make_shared<roaring::Roaring>(), std::move(null_rows)));
    index_context.set_true_for_index_status(this, column_id);
    return Status::OK();
}

const std::string& VMatchPredicate::get_analyzer_key() const {
    return _analyzer_ctx->analyzer_key;
}

Status VMatchPredicate::execute_column_impl(VExprContext* context, const Block* block,
                                            const Selector* selector, size_t count,
                                            ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr);
    if (fast_execute(context, selector, count, result_column)) {
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
        auto it = std::ranges::find(column_names, column_name);
        if (it == column_names.end()) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "column {} should in slow path while VMatchPredicate::execute.", column_name);
        }
    })
    ColumnNumbers arguments(_children.size());
    Block temp_block;
    for (size_t i = 0; i < _children.size(); ++i) {
        ColumnPtr arg_column;
        RETURN_IF_ERROR(_children[i]->execute_column(context, block, selector, count, arg_column));
        auto arg_type = _children[i]->execute_type(block);
        temp_block.insert({arg_column, arg_type, _children[i]->expr_name()});
        arguments[i] = static_cast<uint32_t>(i);
    }
    uint32_t num_columns_without_result = temp_block.columns();
    // prepare a column to save result
    temp_block.insert({nullptr, _data_type, _expr_name});

    RETURN_IF_ERROR(_function->execute(context->fn_context(_fn_context_index), temp_block,
                                       arguments, num_columns_without_result, temp_block.rows()));
    result_column = temp_block.get_by_position(num_columns_without_result).column;
    DCHECK_EQ(result_column->size(), count);
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
    uint16_t num_children = get_num_children();

    for (uint16_t i = 1; i < num_children; ++i) {
        out << (i == 1 ? "" : " ") << children()[i]->debug_string();
    }

    out << "])";
    return out.str();
}

#include "common/compile_check_end.h"
} // namespace doris
