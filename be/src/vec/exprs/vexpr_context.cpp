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

#include "vec/exprs/vexpr_context.h"

#include <algorithm>
#include <ostream>
#include <string>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/object_pool.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "udf/udf.h"
#include "util/stack_util.h"
#include "vec/columns/column_const.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/exprs/vexpr.h"

namespace doris {
class RowDescriptor;
} // namespace doris

namespace doris::vectorized {
VExprContext::VExprContext(const VExprSPtr& expr)
        : _root(expr),
          _is_clone(false),
          _prepared(false),
          _opened(false),
          _last_result_column_id(-1) {}

VExprContext::~VExprContext() {
    // In runtime filter, only create expr context to get expr root, will not call
    // prepare or open, so that it is not need to call close. And call close may core
    // because the function context in expr is not set.
    if (!_prepared || !_opened) {
        return;
    }
    close();
}

doris::Status VExprContext::execute(doris::vectorized::Block* block, int* result_column_id) {
    Status st;
    RETURN_IF_CATCH_EXCEPTION({
        st = _root->execute(this, block, result_column_id);
        _last_result_column_id = *result_column_id;
    });
    return st;
}

doris::Status VExprContext::prepare(doris::RuntimeState* state,
                                    const doris::RowDescriptor& row_desc) {
    _prepared = true;
    Status st;
    RETURN_IF_CATCH_EXCEPTION({ st = _root->prepare(state, row_desc, this); });
    return st;
}

doris::Status VExprContext::open(doris::RuntimeState* state) {
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

doris::Status VExprContext::clone(RuntimeState* state, VExprContextSPtr& new_ctx) {
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

    return _root->open(state, new_ctx.get(), FunctionContext::THREAD_LOCAL);
}

void VExprContext::clone_fn_contexts(VExprContext* other) {
    for (auto& _fn_context : _fn_contexts) {
        other->_fn_contexts.push_back(_fn_context->clone());
    }
}

int VExprContext::register_function_context(RuntimeState* state,
                                            const doris::TypeDescriptor& return_type,
                                            const std::vector<doris::TypeDescriptor>& arg_types) {
    _fn_contexts.push_back(FunctionContext::create_context(state, return_type, arg_types));
    _fn_contexts.back()->set_check_overflow_for_decimal(state->check_overflow_for_decimal());
    return _fn_contexts.size() - 1;
}

Status VExprContext::filter_block(VExprContext* vexpr_ctx, Block* block, int column_to_keep) {
    if (vexpr_ctx == nullptr || block->rows() == 0) {
        return Status::OK();
    }
    int result_column_id = -1;
    RETURN_IF_ERROR(vexpr_ctx->execute(block, &result_column_id));
    return Block::filter_block(block, result_column_id, column_to_keep);
}

Status VExprContext::filter_block(const VExprContextSPtrs& expr_contexts, Block* block,
                                  int column_to_keep) {
    if (expr_contexts.empty() || block->rows() == 0) {
        return Status::OK();
    }

    std::vector<uint32_t> columns_to_filter(column_to_keep);
    std::iota(columns_to_filter.begin(), columns_to_filter.end(), 0);

    return execute_conjuncts_and_filter_block(expr_contexts, nullptr, block, columns_to_filter,
                                              column_to_keep);
}

Status VExprContext::execute_conjuncts(const VExprContextSPtrs& ctxs,
                                       const std::vector<IColumn::Filter*>* filters, Block* block,
                                       IColumn::Filter* result_filter, bool* can_filter_all) {
    return execute_conjuncts(ctxs, filters, false, block, result_filter, can_filter_all);
}

// TODO Performance Optimization
Status VExprContext::execute_conjuncts(const VExprContextSPtrs& ctxs,
                                       const std::vector<IColumn::Filter*>* filters,
                                       const bool accept_null, Block* block,
                                       IColumn::Filter* result_filter, bool* can_filter_all) {
    DCHECK(result_filter->size() == block->rows());
    *can_filter_all = false;
    auto* __restrict result_filter_data = result_filter->data();
    for (auto& ctx : ctxs) {
        int result_column_id = -1;
        RETURN_IF_ERROR(ctx->execute(block, &result_column_id));
        ColumnPtr& filter_column = block->get_by_position(result_column_id).column;
        if (auto* nullable_column = check_and_get_column<ColumnNullable>(*filter_column)) {
            size_t column_size = nullable_column->size();
            if (column_size == 0) {
                *can_filter_all = true;
                return Status::OK();
            } else {
                const ColumnPtr& nested_column = nullable_column->get_nested_column_ptr();
                const IColumn::Filter& filter =
                        assert_cast<const ColumnUInt8&>(*nested_column).get_data();
                auto* __restrict filter_data = filter.data();
                const size_t size = filter.size();
                auto* __restrict null_map_data = nullable_column->get_null_map_data().data();

                if (accept_null) {
                    for (size_t i = 0; i < size; ++i) {
                        result_filter_data[i] &= (null_map_data[i]) || filter_data[i];
                    }
                } else {
                    for (size_t i = 0; i < size; ++i) {
                        result_filter_data[i] &= (!null_map_data[i]) & filter_data[i];
                    }
                }

                if (memchr(result_filter_data, 0x1, size) == nullptr) {
                    *can_filter_all = true;
                    return Status::OK();
                }
            }
        } else if (auto* const_column = check_and_get_column<ColumnConst>(*filter_column)) {
            // filter all
            if (!const_column->get_bool(0)) {
                *can_filter_all = true;
                memset(result_filter_data, 0, result_filter->size());
                return Status::OK();
            }
        } else {
            const IColumn::Filter& filter =
                    assert_cast<const ColumnUInt8&>(*filter_column).get_data();
            auto* __restrict filter_data = filter.data();

            const size_t size = filter.size();
            for (size_t i = 0; i < size; ++i) {
                result_filter_data[i] &= filter_data[i];
            }

            if (memchr(result_filter_data, 0x1, size) == nullptr) {
                *can_filter_all = true;
                return Status::OK();
            }
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

// TODO Performance Optimization
// need exception safety
Status VExprContext::execute_conjuncts_and_filter_block(
        const VExprContextSPtrs& ctxs, const std::vector<IColumn::Filter*>* filters, Block* block,
        std::vector<uint32_t>& columns_to_filter, int column_to_keep) {
    IColumn::Filter result_filter(block->rows(), 1);
    bool can_filter_all;
    RETURN_IF_ERROR(
            execute_conjuncts(ctxs, filters, false, block, &result_filter, &can_filter_all));
    if (can_filter_all) {
        for (auto& col : columns_to_filter) {
            std::move(*block->get_by_position(col).column).assume_mutable()->clear();
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
    filter.resize_fill(block->rows(), 1);
    bool can_filter_all;
    RETURN_IF_ERROR(execute_conjuncts(ctxs, nullptr, false, block, &filter, &can_filter_all));
    if (can_filter_all) {
        for (auto& col : columns_to_filter) {
            std::move(*block->get_by_position(col).column).assume_mutable()->clear();
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
    vectorized::Block tmp_block(input_block.get_columns_with_type_and_name());
    vectorized::ColumnsWithTypeAndName result_columns;
    for (auto& vexpr_ctx : output_vexpr_ctxs) {
        int result_column_id = -1;
        RETURN_IF_ERROR(vexpr_ctx->execute(&tmp_block, &result_column_id));
        DCHECK(result_column_id != -1);
        const auto& col = tmp_block.get_by_position(result_column_id);
        if (do_projection) {
            result_columns.emplace_back(col.column->clone_resized(rows), col.type, col.name);
        } else {
            result_columns.emplace_back(tmp_block.get_by_position(result_column_id));
        }
    }
    *output_block = {result_columns};
    return Status::OK();
}

} // namespace doris::vectorized
