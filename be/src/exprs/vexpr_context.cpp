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

#include "exprs/vexpr_context.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "exec/common/util.hpp"
#include "exprs/function_context.h"
#include "exprs/lambda_function/lambda_execution_context.h"
#include "exprs/vexpr.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "storage/olap_common.h"
#include "storage/segment/column_reader.h"
#include "util/simd/bits.h"
#include "util/time.h"

namespace doris {
class RowDescriptor;
} // namespace doris

namespace doris {

VExprContext::VExprContext(VExprSPtr expr) : _root(std::move(expr)) {}

VExprContext::~VExprContext() {
    // In runtime filter, only create expr context to get expr root, will not call
    // prepare or open, so that it is not need to call close. And call close may core
    // because the function context in expr is not set.
    if (!_prepared || !_opened) {
        return;
    }
    try {
        close();
    } catch (const Exception& e) {
        LOG(WARNING) << "Exception occurs when expr context deconstruct: " << e.to_string();
    }
}

LambdaExecutionContext& VExprContext::lambda_execution_context() {
    if (!_lambda_execution_context) {
        _lambda_execution_context = std::make_unique<LambdaExecutionContext>();
    }
    return *_lambda_execution_context;
}

Status VExprContext::execute(Block* block, int* result_column_id) {
    Status st;
    RETURN_IF_CATCH_EXCEPTION({
        st = _root->execute(this, block, result_column_id);
        _last_result_column_id = *result_column_id;
        // We should first check the status, as some expressions might incorrectly set result_column_id, even if the st is not ok.
        if (st.ok() && _last_result_column_id != -1) {
            block->get_by_position(*result_column_id).column->sanity_check();
            RETURN_IF_ERROR(
                    block->get_by_position(*result_column_id).check_type_and_column_match());
        }
    });
    return st;
}

Status VExprContext::execute(const Block* block, ColumnPtr& result_column) {
    Status st;
    RETURN_IF_CATCH_EXCEPTION(
            { st = _root->execute_column(this, block, nullptr, block->rows(), result_column); });
    return st;
}

Status VExprContext::execute(const Block* block, ColumnWithTypeAndName& result_data) {
    Status st;
    ColumnPtr result_column;
    RETURN_IF_CATCH_EXCEPTION(
            { st = _root->execute_column(this, block, nullptr, block->rows(), result_column); });
    RETURN_IF_ERROR(st);
    result_data.column = result_column;
    result_data.type = execute_type(block);
    result_data.name = _root->expr_name();
    return Status::OK();
}

DataTypePtr VExprContext::execute_type(const Block* block) {
    return _root->execute_type(block);
}

Status VExprContext::execute_const_expr(ColumnWithTypeAndName& result) {
    Status st;
    RETURN_IF_CATCH_EXCEPTION(
            { st = _root->execute_column(this, nullptr, nullptr, 1, result.column); });
    RETURN_IF_ERROR(st);
    result.type = _root->execute_type(nullptr);
    result.name = _root->expr_name();
    return Status::OK();
}

[[nodiscard]] const std::string& VExprContext::expr_name() const {
    return _root->expr_name();
}

bool VExprContext::is_blockable() const {
    return _root->is_blockable();
}

Status VExprContext::prepare(RuntimeState* state, const RowDescriptor& row_desc) {
    _prepared = true;
    Status st;
    RETURN_IF_CATCH_EXCEPTION({ st = _root->prepare(state, row_desc, this); });
    return st;
}

Status VExprContext::open(RuntimeState* state) {
    DCHECK(_prepared);
    if (_opened) {
        return Status::OK();
    }
    _opened = true;
    // Fragment-local state is only initialized for original contexts. Clones inherit the
    // original's fragment state and only need to have thread-local state initialized.
    FunctionContext::FunctionStateScope scope =
            _is_clone ? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
    Status st;
    RETURN_IF_CATCH_EXCEPTION({ st = _root->open(state, this, scope); });
    return st;
}

void VExprContext::close() {
    // Sometimes expr context may not have a root, then it need not call close
    if (_root == nullptr) {
        return;
    }
    FunctionContext::FunctionStateScope scope =
            _is_clone ? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
    _root->close(this, scope);
}

Status VExprContext::clone(RuntimeState* state, VExprContextSPtr& new_ctx) {
    DCHECK(_prepared) << "expr context not prepared";
    DCHECK(_opened);
    DCHECK(new_ctx.get() == nullptr);

    new_ctx = std::make_shared<VExprContext>(_root);
    for (auto& _fn_context : _fn_contexts) {
        new_ctx->_fn_contexts.push_back(_fn_context->clone());
    }

    new_ctx->_is_clone = true;
    new_ctx->_prepared = true;
    new_ctx->_opened = true;
    // segment_v2::AnnRangeSearchRuntime should be cloned as well.
    // The object of segment_v2::AnnRangeSearchRuntime is not shared by threads.
    new_ctx->_ann_range_search_runtime = this->_ann_range_search_runtime;

    return _root->open(state, new_ctx.get(), FunctionContext::THREAD_LOCAL);
}

void VExprContext::clone_fn_contexts(VExprContext* other) {
    for (auto& _fn_context : _fn_contexts) {
        other->_fn_contexts.push_back(_fn_context->clone());
    }
}

int VExprContext::register_function_context(RuntimeState* state, const DataTypePtr& return_type,
                                            const std::vector<DataTypePtr>& arg_types) {
    _fn_contexts.push_back(FunctionContext::create_context(state, return_type, arg_types));
    _fn_contexts.back()->set_check_overflow_for_decimal(state->check_overflow_for_decimal());
    _fn_contexts.back()->set_enable_strict_mode(state->enable_strict_mode());
    return static_cast<int>(_fn_contexts.size()) - 1;
}

Status VExprContext::evaluate_inverted_index(uint32_t segment_num_rows) {
    Status st;
    RETURN_IF_CATCH_EXCEPTION({ st = _root->evaluate_inverted_index(this, segment_num_rows); });
    return st;
}

ZoneMapFilterResult VExprContext::evaluate_zonemap_filter(const VExprContextSPtrs& conjuncts,
                                                          const ZoneMapEvalContext& ctx) {
    for (const auto& conjunct : conjuncts) {
        DORIS_CHECK(conjunct != nullptr);
        const auto& root = conjunct->root();
        DORIS_CHECK(root != nullptr);
        if (!root->can_evaluate_zonemap_filter()) {
            continue;
        }
        if (root->evaluate_zonemap_filter(ctx) == ZoneMapFilterResult::kNoMatch) {
            return ZoneMapFilterResult::kNoMatch;
        }
    }
    return ZoneMapFilterResult::kMayMatch;
}

ZoneMapFilterResult VExprContext::evaluate_dictionary_filter(const VExprContextSPtrs& conjuncts,
                                                             const DictionaryEvalContext& ctx) {
    for (const auto& conjunct : conjuncts) {
        DORIS_CHECK(conjunct != nullptr);
        const auto& root = conjunct->root();
        DORIS_CHECK(root != nullptr);
        if (!root->can_evaluate_dictionary_filter()) {
            continue;
        }
        if (root->evaluate_dictionary_filter(ctx) == ZoneMapFilterResult::kNoMatch) {
            return ZoneMapFilterResult::kNoMatch;
        }
    }
    return ZoneMapFilterResult::kMayMatch;
}

ZoneMapFilterResult VExprContext::evaluate_bloom_filter(const VExprContextSPtrs& conjuncts,
                                                        const BloomFilterEvalContext& ctx) {
    for (const auto& conjunct : conjuncts) {
        DORIS_CHECK(conjunct != nullptr);
        const auto& root = conjunct->root();
        DORIS_CHECK(root != nullptr);
        if (!root->can_evaluate_bloom_filter()) {
            continue;
        }
        if (root->evaluate_bloom_filter(ctx) == ZoneMapFilterResult::kNoMatch) {
            return ZoneMapFilterResult::kNoMatch;
        }
    }
    return ZoneMapFilterResult::kMayMatch;
}

bool VExprContext::all_expr_inverted_index_evaluated() {
    return _index_context->has_index_result_for_expr(_root.get());
}

Status VExprContext::filter_block(VExprContext* vexpr_ctx, Block* block) {
    if (vexpr_ctx == nullptr || block->rows() == 0) {
        return Status::OK();
    }
    ColumnPtr filter_column;
    RETURN_IF_ERROR(vexpr_ctx->execute(block, filter_column));
    size_t filter_column_id = block->columns();
    block->insert({filter_column, vexpr_ctx->execute_type(block), "filter_column"});
    vexpr_ctx->_memory_usage = filter_column->allocated_bytes();
    return Block::filter_block(block, filter_column_id, filter_column_id);
}

Status VExprContext::filter_block(const VExprContextSPtrs& expr_contexts, Block* block,
                                  size_t column_to_keep) {
    if (expr_contexts.empty() || block->rows() == 0) {
        return Status::OK();
    }

    ColumnNumbers columns_to_filter(column_to_keep);
    std::iota(columns_to_filter.begin(), columns_to_filter.end(), 0);

    return execute_conjuncts_and_filter_block(expr_contexts, block, columns_to_filter,
                                              static_cast<int>(column_to_keep));
}

Status VExprContext::execute_conjuncts(const VExprContextSPtrs& ctxs,
                                       const std::vector<IColumn::Filter*>* filters, Block* block,
                                       IColumn::Filter* result_filter, bool* can_filter_all) {
    return execute_conjuncts(ctxs, filters, false, block, result_filter, can_filter_all);
}

Status VExprContext::execute_filter(const Block* block, uint8_t* __restrict result_filter_data,
                                    size_t rows, bool accept_null, bool* can_filter_all) {
    return _root->execute_filter(this, block, result_filter_data, rows, accept_null,
                                 can_filter_all);
}

Status VExprContext::execute_conjuncts(const VExprContextSPtrs& ctxs,
                                       const std::vector<IColumn::Filter*>* filters,
                                       bool accept_null, const Block* block,
                                       IColumn::Filter* result_filter, bool* can_filter_all) {
    size_t rows = block->rows();
    DCHECK_EQ(result_filter->size(), rows);
    *can_filter_all = false;
    auto* __restrict result_filter_data = result_filter->data();
    for (const auto& ctx : ctxs) {
        RETURN_IF_ERROR(
                ctx->execute_filter(block, result_filter_data, rows, accept_null, can_filter_all));
        if (*can_filter_all) {
            return Status::OK();
        }
    }
    if (filters != nullptr) {
        for (auto* filter : *filters) {
            auto* __restrict filter_data = filter->data();
            const size_t size = filter->size();
            for (size_t i = 0; i < size; ++i) {
                result_filter_data[i] &= filter_data[i];
            }
            if (memchr(result_filter_data, 0x1, size) == nullptr) {
                *can_filter_all = true;
                return Status::OK();
            }
        }
    }
    return Status::OK();
}

Status VExprContext::execute_conjuncts_selective(const VExprContextSPtrs& ctxs,
                                                 const std::vector<IColumn::Filter*>* filters,
                                                 const Block* block, IColumn::Filter* result_filter,
                                                 bool* can_filter_all) {
    size_t rows = block->rows();
    DCHECK_EQ(result_filter->size(), rows);
    *can_filter_all = false;
    auto* __restrict result_filter_data = result_filter->data();

    // Fold any pre-existing masks (delete / position-delete filters) into result_filter first,
    // so the surviving set starts from rows those masks already keep.
    size_t surviving = rows;
    if (filters != nullptr) {
        for (auto* filter : *filters) {
            const auto* __restrict fdata = filter->data();
            for (size_t i = 0; i < rows; ++i) {
                result_filter_data[i] &= fdata[i];
            }
        }
        surviving = rows -
                    simd::count_zero_num(reinterpret_cast<const int8_t*>(result_filter_data), rows);
        if (surviving == 0) {
            *can_filter_all = true;
            return Status::OK();
        }
    }

    // Below this surviving fraction, gathering the surviving rows for a conjunct's inputs is
    // worth its memcpy cost; above it, evaluate the conjunct full-width (like the original
    // execute_conjuncts) so we do not gather nearly the whole block. Mirrors PrestoDB selecting
    // only once a batch has meaningfully shrunk.
    constexpr double kSelectiveGatherThreshold = 0.5;

    // `selection` holds the original row indices still alive. It is (re)built from
    // result_filter lazily, only when a conjunct is about to be evaluated selectively.
    IColumn::Selector selection;
    bool selection_valid = false;

    auto rebuild_selection = [&]() {
        selection.clear();
        selection.reserve(surviving);
        for (size_t i = 0; i < rows; ++i) {
            if (result_filter_data[i]) {
                selection.push_back(static_cast<IColumn::Selector::value_type>(i));
            }
        }
        selection_valid = true;
    };

    for (const auto& ctx : ctxs) {
        if (ctx == nullptr) {
            continue;
        }
        // Runtime-filter wrappers carry selectivity tracking and counter side effects in their
        // execute_filter override, so they must run full-width; they are also cheap (bloom /
        // min-max) and belong up front. A high surviving fraction also stays on the full-width
        // path, where gathering would copy almost the whole block for no benefit.
        const bool use_selective = !ctx->root()->is_rf_wrapper() &&
                                   surviving < static_cast<size_t>(static_cast<double>(rows) *
                                                                   kSelectiveGatherThreshold);
        if (!use_selective) {
            // Full-width path: input is every row still alive (result_filter carries them).
            const size_t input_rows = surviving;
            // Sample every kTimingSampleEvery-th batch. RF wrappers are skipped: their
            // execute_filter includes their own selectivity/counter side effects and
            // wrapping it in another clock read is noise. The static cost stays as the
            // cold-start estimate; timed rows accumulate on the non-RF conjuncts we care
            // about reordering.
            const bool sample =
                    !ctx->root()->is_rf_wrapper() && ctx->filter_runtime_stats().should_sample();
            const int64_t t0 = sample ? MonotonicNanos() : 0;
            RETURN_IF_ERROR(
                    ctx->execute_filter(block, result_filter_data, rows, false, can_filter_all));
            const int64_t elapsed_ns = sample ? MonotonicNanos() - t0 : 0;
            if (*can_filter_all) {
                ctx->filter_runtime_stats().update(static_cast<int64_t>(input_rows), 0, elapsed_ns);
                return Status::OK();
            }
            surviving = rows - simd::count_zero_num(
                                       reinterpret_cast<const int8_t*>(result_filter_data), rows);
            ctx->filter_runtime_stats().update(static_cast<int64_t>(input_rows),
                                               static_cast<int64_t>(surviving), elapsed_ns);
            if (surviving == 0) {
                *can_filter_all = true;
                return Status::OK();
            }
            selection_valid = false;
            continue;
        }

        if (!selection_valid) {
            rebuild_selection();
        }
        // Evaluate this conjunct only on the surviving rows. execute_column with a selector
        // gathers the inputs down to `selection.size()` rows, so a heavy function runs once
        // per surviving row rather than once per block row.
        const size_t count = selection.size();
        ColumnPtr result_column;
        const bool sample = ctx->filter_runtime_stats().should_sample();
        const int64_t t0 = sample ? MonotonicNanos() : 0;
        RETURN_IF_ERROR(
                ctx->root()->execute_column(ctx.get(), block, &selection, count, result_column));
        const int64_t elapsed_ns = sample ? MonotonicNanos() - t0 : 0;
        auto [filter_column, is_const] = unpack_if_const(result_column);

        if (is_const) {
            // Const predicate over the surviving rows: keep all or drop all of them.
            const bool keep = !filter_column->is_null_at(0) && filter_column->get_bool(0);
            ctx->filter_runtime_stats().update(static_cast<int64_t>(count),
                                               keep ? static_cast<int64_t>(count) : 0, elapsed_ns);
            if (!keep) {
                memset(result_filter_data, 0, rows);
                *can_filter_all = true;
                return Status::OK();
            }
            // keep == true: result_filter and selection unchanged.
            continue;
        }

        // Narrow the surviving set: keep selection[j] only where the predicate is true and not
        // NULL at position j (accept_null == false, matching execute_filter's scan semantics).
        IColumn::Selector next;
        next.reserve(count);
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*filter_column)) {
            const auto* __restrict fdata =
                    assert_cast<const ColumnUInt8&>(nullable->get_nested_column())
                            .get_data()
                            .data();
            const auto* __restrict nmap = nullable->get_null_map_data().data();
            for (size_t j = 0; j < count; ++j) {
                if (!nmap[j] && fdata[j]) {
                    next.push_back(selection[j]);
                } else {
                    result_filter_data[selection[j]] = 0;
                }
            }
        } else {
            const auto* __restrict fdata =
                    assert_cast<const ColumnUInt8&>(*filter_column).get_data().data();
            for (size_t j = 0; j < count; ++j) {
                if (fdata[j]) {
                    next.push_back(selection[j]);
                } else {
                    result_filter_data[selection[j]] = 0;
                }
            }
        }
        selection.swap(next);
        ctx->filter_runtime_stats().update(static_cast<int64_t>(count),
                                           static_cast<int64_t>(selection.size()), elapsed_ns);
        surviving = selection.size();
        selection_valid = true;
        if (surviving == 0) {
            *can_filter_all = true;
            return Status::OK();
        }
    }
    return Status::OK();
}

bool VExprContext::adaptive_reorder_conjuncts(VExprContextSPtrs& conjuncts, int64_t min_rows,
                                              double min_cost) {
    if (conjuncts.size() < 2) {
        return false;
    }
    // Adaptive reorder only pays off when some conjunct is expensive: shuffling cheap-only
    // predicates saves less than the reorder itself costs. "Expensive" is either
    //   (a) the static structural estimate scored high (cost >= min_cost), or
    //   (b) the measured per-row cost scored high (per_row_ns >= min_per_row_ns).
    // Case (b) catches two failure modes of the static score: a FUNCTION_CALL underestimate
    // (length(col) is nowhere near 100ns/row) and inability to distinguish two calls that
    // score the same but run 10-100x apart. Also gate on every measurable conjunct having
    // seen enough rows so an early noisy selectivity does not churn the order. RF wrappers
    // run full-width and stay in front, so they are exempt from both checks.
    constexpr double kMinPerRowNs = kAdaptiveReorderMinPerRowNs;
    bool has_expensive = false;
    for (const auto& ctx : conjuncts) {
        if (ctx == nullptr || ctx->root()->is_rf_wrapper()) {
            continue;
        }
        const auto& stats = ctx->filter_runtime_stats();
        if (stats.input_rows < min_rows) {
            return false;
        }
        if (VExpr::compute_conjunct_cost(ctx->root()) >= min_cost ||
            stats.per_row_ns() >= kMinPerRowNs) {
            has_expensive = true;
        }
    }
    if (!has_expensive) {
        return false;
    }
    // Sort key: per-row cost divided by measured drop fraction -- cheap predicates that
    // eliminate many rows sort first, an expensive predicate that turns out unselective sinks.
    // Cost prefers the measured per_row_ns once enough sampled rows have accrued, and falls
    // back on the static structural estimate otherwise. Selectivity stays the (free)
    // measurement. eps floors the divisor so a conjunct that drops nothing sorts last instead
    // of dividing by zero. RF wrappers score 0 to stay in front. The two cost sources are on
    // different units (ns vs structural score) but this only affects the absolute magnitude
    // of the key -- ordering is preserved because either every conjunct we care about has a
    // measurement (comparable among themselves) or none does (all fall back to structural),
    // and RF wrappers pin at 0 either way.
    constexpr double kEps = 1e-6;
    auto sort_key = [kEps](const VExprContextSPtr& c) -> double {
        if (c == nullptr || c->root()->is_rf_wrapper()) {
            return 0.0;
        }
        const auto& stats = c->filter_runtime_stats();
        const double per_row = stats.per_row_ns();
        const double cost = per_row > 0.0 ? per_row : VExpr::compute_conjunct_cost(c->root());
        return cost / std::max(stats.dropped_fraction(), kEps);
    };
    std::stable_sort(conjuncts.begin(), conjuncts.end(),
                     [&sort_key](const VExprContextSPtr& a, const VExprContextSPtr& b) {
                         return sort_key(a) < sort_key(b);
                     });
    return true;
}

Status VExprContext::execute_conjuncts(const VExprContextSPtrs& conjuncts, const Block* block,
                                       ColumnUInt8& null_map, IColumn::Filter& filter) {
    const auto& rows = block->rows();
    if (rows == 0) {
        return Status::OK();
    }
    if (null_map.size() != rows) {
        return Status::InternalError("null_map.size()!=rows, null_map.size()={}, rows={}",
                                     null_map.size(), rows);
    }

    auto* final_null_map = null_map.get_data().data();
    auto* final_filter_ptr = filter.data();

    for (const auto& conjunct : conjuncts) {
        ColumnPtr result_column;
        RETURN_IF_ERROR(conjunct->execute(block, result_column));
        auto [filter_column, is_const] = unpack_if_const(result_column);
        const auto* nullable_column = assert_cast<const ColumnNullable*>(filter_column.get());
        if (!is_const) {
            const ColumnPtr& nested_column = nullable_column->get_nested_column_ptr();
            const IColumn::Filter& result =
                    assert_cast<const ColumnUInt8&>(*nested_column).get_data();
            const auto* __restrict filter_data = result.data();
            const auto* __restrict null_map_data = nullable_column->get_null_map_data().data();
            DCHECK_EQ(rows, nullable_column->size());

            for (size_t i = 0; i != rows; ++i) {
                // null and null    => null
                // null and true    => null
                // null and false   => false
                final_null_map[i] = (final_null_map[i] & (null_map_data[i] | filter_data[i])) |
                                    (null_map_data[i] & (final_null_map[i] | final_filter_ptr[i]));
                final_filter_ptr[i] = final_filter_ptr[i] & filter_data[i];
            }
        } else {
            bool filter_data = nullable_column->get_bool(0);
            bool null_map_data = nullable_column->is_null_at(0);
            for (size_t i = 0; i != rows; ++i) {
                // null and null    => null
                // null and true    => null
                // null and false   => false
                final_null_map[i] = (final_null_map[i] & (null_map_data | filter_data)) |
                                    (null_map_data & (final_null_map[i] | final_filter_ptr[i]));
                final_filter_ptr[i] = final_filter_ptr[i] & filter_data;
            }
        }
    }
    return Status::OK();
}

// TODO Performance Optimization
// need exception safety
Status VExprContext::execute_conjuncts_and_filter_block(const VExprContextSPtrs& ctxs, Block* block,
                                                        std::vector<uint32_t>& columns_to_filter,
                                                        int column_to_keep) {
    IColumn::Filter result_filter(block->rows(), 1);
    bool can_filter_all;

    _reset_memory_usage(ctxs);

    RETURN_IF_ERROR(
            execute_conjuncts(ctxs, nullptr, false, block, &result_filter, &can_filter_all));

    // Accumulate the usage of `result_filter` into the first context.
    if (!ctxs.empty()) {
        ctxs[0]->_memory_usage += result_filter.allocated_bytes();
    }
    if (can_filter_all) {
        for (auto& col : columns_to_filter) {
            auto& column = block->get_by_position(col).column;
            if (column->is_exclusive()) {
                column->assert_mutable()->clear();
            } else {
                column = column->clone_empty();
            }
        }
    } else {
        try {
            Block::filter_block_internal(block, columns_to_filter, result_filter);
        } catch (const Exception& e) {
            std::string str;
            for (auto ctx : ctxs) {
                if (str.length()) {
                    str += ",";
                }
                str += ctx->root()->debug_string();
            }

            return Status::InternalError(
                    "filter_block_internal meet exception, exprs=[{}], exception={}", str,
                    e.what());
        }
    }
    Block::erase_useless_column(block, column_to_keep);
    return Status::OK();
}

Status VExprContext::execute_conjuncts_and_filter_block(const VExprContextSPtrs& ctxs, Block* block,
                                                        std::vector<uint32_t>& columns_to_filter,
                                                        int column_to_keep,
                                                        IColumn::Filter& filter) {
    _reset_memory_usage(ctxs);
    filter.resize_fill(block->rows(), 1);
    bool can_filter_all;
    RETURN_IF_ERROR(execute_conjuncts(ctxs, nullptr, false, block, &filter, &can_filter_all));

    // Accumulate the usage of `result_filter` into the first context.
    if (!ctxs.empty()) {
        ctxs[0]->_memory_usage += filter.allocated_bytes();
    }
    if (can_filter_all) {
        for (auto& col : columns_to_filter) {
            auto& column = block->get_by_position(col).column;
            if (column->is_exclusive()) {
                column->assert_mutable()->clear();
            } else {
                column = column->clone_empty();
            }
        }
    } else {
        RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(block, columns_to_filter, filter));
    }

    Block::erase_useless_column(block, column_to_keep);
    return Status::OK();
}

// do_projection: for some query(e.g. in MultiCastDataStreamerSourceOperator::get_block()),
// output_vexpr_ctxs will output the same column more than once, and if the output_block
// is mem-reused later, it will trigger DCHECK_EQ(d.column->use_count(), 1) failure when
// doing Block::clear_column_data, set do_projection to true to copy the column data to
// avoid this problem.
Status VExprContext::get_output_block_after_execute_exprs(
        const VExprContextSPtrs& output_vexpr_ctxs, const Block& input_block, Block* output_block,
        bool do_projection) {
    auto rows = input_block.rows();
    ColumnsWithTypeAndName result_columns;
    _reset_memory_usage(output_vexpr_ctxs);

    for (const auto& vexpr_ctx : output_vexpr_ctxs) {
        ColumnPtr result_column;
        RETURN_IF_ERROR(vexpr_ctx->execute(&input_block, result_column));

        auto type = vexpr_ctx->execute_type(&input_block);
        const auto& name = vexpr_ctx->expr_name();

        vexpr_ctx->_memory_usage += result_column->allocated_bytes();
        if (do_projection) {
            result_columns.emplace_back(result_column->clone_resized(rows), type, name);

        } else {
            result_columns.emplace_back(result_column, type, name);
        }
    }
    *output_block = {result_columns};
    return Status::OK();
}

void VExprContext::_reset_memory_usage(const VExprContextSPtrs& contexts) {
    std::for_each(contexts.begin(), contexts.end(),
                  [](auto&& context) { context->_memory_usage = 0; });
}

void VExprContext::prepare_ann_range_search(const doris::VectorSearchUserParams& params) {
    if (_root == nullptr) {
        return;
    }

    _root->prepare_ann_range_search(params, _ann_range_search_runtime, _suitable_for_ann_index);
    VLOG_DEBUG << fmt::format("Prepare ann range search result {}, _suitable_for_ann_index {}",
                              this->_ann_range_search_runtime.to_string(),
                              this->_suitable_for_ann_index);
    return;
}

Status VExprContext::evaluate_ann_range_search(
        const std::vector<std::unique_ptr<segment_v2::IndexIterator>>& cid_to_index_iterators,
        const std::vector<ColumnId>& idx_to_cid,
        const std::vector<std::unique_ptr<segment_v2::ColumnIterator>>& column_iterators,
        const std::unordered_map<VExprContext*, std::unordered_map<ColumnId, VExpr*>>&
                common_expr_to_slotref_map,
        size_t rows_of_segment, roaring::Roaring& row_bitmap,
        segment_v2::AnnIndexStats& ann_index_stats, bool enable_result_cache,
        bool* ann_range_search_executed) {
    if (ann_range_search_executed != nullptr) {
        *ann_range_search_executed = false;
    }
    if (_root == nullptr) {
        return Status::OK();
    }

    AnnRangeSearchEvaluationResult evaluation_result;
    RETURN_IF_ERROR(_root->evaluate_ann_range_search(
            _ann_range_search_runtime, cid_to_index_iterators, idx_to_cid, column_iterators,
            rows_of_segment, row_bitmap, ann_index_stats, enable_result_cache, evaluation_result));

    if (!evaluation_result.executed) {
        return Status::OK();
    }
    if (ann_range_search_executed != nullptr) {
        *ann_range_search_executed = true;
    }

    DCHECK(_index_context != nullptr);
    _index_context->set_index_result_for_expr(
            _root.get(),
            segment_v2::InvertedIndexResultBitmap(std::make_shared<roaring::Roaring>(row_bitmap),
                                                  std::make_shared<roaring::Roaring>()));

    if (!evaluation_result.dist_fulfilled) {
        // Do not perform index scan in this case.
        return Status::OK();
    }

    DCHECK_LT(_ann_range_search_runtime.src_col_idx, idx_to_cid.size());
    const auto src_col_idx = cast_set<int>(_ann_range_search_runtime.src_col_idx);
    const auto src_col_key = cast_set<ColumnId>(_ann_range_search_runtime.src_col_idx);
    auto slot_ref_map_it = common_expr_to_slotref_map.find(this);
    if (slot_ref_map_it == common_expr_to_slotref_map.end()) {
        return Status::OK();
    }
    auto& slot_ref_map = slot_ref_map_it->second;
    auto slot_ref_it = slot_ref_map.find(src_col_key);
    if (slot_ref_it == slot_ref_map.end()) {
        return Status::OK();
    }
    const VExpr* slot_ref_expr_addr = slot_ref_it->second;
    _index_context->set_true_for_index_status(slot_ref_expr_addr, src_col_idx);

    VLOG_DEBUG << fmt::format(
            "Evaluate ann range search for expr {}, src_col_idx {}, cid {}, row_bitmap "
            "cardinality {}",
            _root->debug_string(), src_col_idx, idx_to_cid[_ann_range_search_runtime.src_col_idx],
            row_bitmap.cardinality());
    return Status::OK();
}

uint64_t VExprContext::get_digest(uint64_t seed) const {
    return _root->get_digest(seed);
}

double VExprContext::execute_cost() const {
    if (_root == nullptr) {
        // When there is no expression root, treat the cost as a base value.
        // This avoids null dereferences while keeping a deterministic cost.
        return 0.0;
    }
    return _root->execute_cost();
}

} // namespace doris
