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

#include "exec/schema_scanner/materialized_schema_table_reader.h"

#include <cctz/time_zone.h>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {
#include "common/compile_check_begin.h"

Status MaterializedSchemaTableReader::prepare(vectorized::Block* active_block) {
    // 1. copy active_block
    materialized_block_ = vectorized::Block::create_unique(active_block->clone_empty());
    vectorized::MutableBlock mblock =
            vectorized::MutableBlock::build_mutable_block(materialized_block_.get());
    RETURN_IF_ERROR(mblock.merge(*active_block));

    // 2. filter time slice
    RETURN_IF_ERROR(filter_time_slice_());
    return Status::OK();
}

Status MaterializedSchemaTableReader::filter_time_slice_() {
    // 1. find time slice expr context
    std::vector<vectorized::VExprContextSPtr> time_slice_expr_ctx;
    for (const auto& expr_ctx : expr_ctxs_) {
        const auto& root_expr = expr_ctx->root();
        if (root_expr == nullptr) {
            continue;
        }

        std::stack<vectorized::VExprSPtr> stack;
        stack.emplace(root_expr);

        while (!stack.empty()) {
            const auto& expr = stack.top();
            stack.pop();

            for (const auto& child : expr->children()) {
                if (child->is_slot_ref() && !root_expr->expr_label().empty()) {
                    auto* column_slot_ref = assert_cast<vectorized::VSlotRef*>(child.get());
                    if (column_slot_ref->expr_label() == "time_slice") {
                        time_slice_expr_ctx.push_back(expr_ctx);
                        break;
                    }
                }
            }

            if (!time_slice_expr_ctx.empty()) {
                break;
            }

            const auto& children = expr->children();
            for (size_t i = children.size(); i-- > 0;) {
                if (!children[i]->children().empty()) {
                    stack.emplace(children[i]);
                }
            }
        }

        if (!time_slice_expr_ctx.empty()) {
            break;
        }
    }
    if (time_slice_expr_ctx.empty()) {
        return Status::OK();
    }

    // 2. insert block, only one column of type DATETIMEV2, insert all time slice end_timestamp.
    vectorized::Block block;
    auto data_type =
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false);
    block.insert(
            vectorized::ColumnWithTypeAndName(data_type->create_column(), data_type, "time_slice"));
    block.reserve(time_slices_.size());
    vectorized::MutableColumnPtr mutable_col_ptr;
    mutable_col_ptr = std::move(block.get_by_position(0).column)->assume_mutable();
    vectorized::IColumn* col_ptr = mutable_col_ptr.get();

    for (auto& time_slice : time_slices_) {
        std::vector<void*> datas(1);
        VecDateTimeValue src[1];
        src[0].from_unixtime(time_slice.end_timestamp_, cctz::time_zone());
        datas[0] = src;
        auto* data = datas[0];
        assert_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_data(
                reinterpret_cast<char*>(data), 0);
    }

    // 3. filter block by time_slice_expr_ctx
    size_t prev_columns = block.columns();
    RETURN_IF_ERROR(
            vectorized::VExprContext::filter_block(time_slice_expr_ctx, &block, prev_columns));

    // 4. update time_slices_ from block
    // block.get_by_position(0).column->
    return Status::OK();
}

Status MaterializedSchemaTableReader::read_next_time_slice_(vectorized::Block* block, bool* eos) {
    // 1. open time slice file reader
    bool exists = false;
    io::FileReaderSPtr file_reader;
    while (read_time_slice_offset_ < time_slices_.size()) {
        const auto& file_path = time_slices_[read_time_slice_offset_++].file_path_;
        auto status = io::global_local_filesystem()->exists(file_path, &exists);
        if (status.ok() && exists) {
            status = io::global_local_filesystem()->open_file(file_path, &file_reader);
            if (status.ok() && file_reader) {
                break;
            }
        }
    }
    if (read_time_slice_offset_ >= time_slices_.size()) {
        *eos = true;
        return Status::OK();
    }

    size_t file_size = file_reader->size();
    DCHECK(file_size != 0);

    // 2. read time slice file reader
    std::unique_ptr<char[]> read_buff_;
    Slice result(read_buff_.get(), file_size);
    size_t bytes_read = 0;
    RETURN_IF_ERROR(file_reader->read_at(0, result, &bytes_read));
    DCHECK(bytes_read == file_size);

    PBlock pb_block_;
    if (!pb_block_.ParseFromArray(result.data, cast_set<int>(result.size))) {
        return Status::InternalError("Failed to read block");
    }

    // 3. deserialize block from pb_block_
    vectorized::Block tmp_block = block->clone_empty();
    RETURN_IF_ERROR(tmp_block.deserialize(pb_block_));
    if (tmp_block.rows() != 0) {
        vectorized::MutableBlock mblock = vectorized::MutableBlock::build_mutable_block(block);
        RETURN_IF_ERROR(mblock.merge(tmp_block));
    }

    (void)file_reader->close();
    file_reader.reset();
    return Status::OK();
}

Status MaterializedSchemaTableReader::get_batch(vectorized::Block* output, bool* eos) {
    output->clear_column_data();

    // 1. get batch from existing materialized_block_
    size_t first_get_rows = std::min(materialized_block_->rows() - row_idx_, batch_size_);
    vectorized::MutableBlock mblock = vectorized::MutableBlock::build_mutable_block(output);
    RETURN_IF_ERROR(mblock.add_rows(materialized_block_.get(), row_idx_, first_get_rows));
    row_idx_ += first_get_rows;
    if (first_get_rows == batch_size_) {
        return Status::OK();
    }

    // 2. read new materialized_block_ until batch_size_ is reached.
    materialized_block_->clear_column_data();
    row_idx_ = 0;
    while (!read_time_slice_eos_ && materialized_block_->rows() < batch_size_) {
        RETURN_IF_ERROR(read_next_time_slice_(materialized_block_.get(), &read_time_slice_eos_));
        if (!expr_ctxs_.empty()) {
            RETURN_IF_ERROR(vectorized::VExprContext::filter_block(
                    expr_ctxs_, materialized_block_.get(), materialized_block_->columns()));
        }
    }

    // 3. get batch from new materialized_block_.
    size_t second_get_rows = std::min(materialized_block_->rows(), batch_size_ - first_get_rows);
    RETURN_IF_ERROR(mblock.add_rows(materialized_block_.get(), row_idx_, second_get_rows));
    row_idx_ += second_get_rows;

    if (first_get_rows + second_get_rows == batch_size_ && !read_time_slice_eos_) {
        return Status::OK();
    } else {
        *eos = true;
        return Status::OK();
    }
}

} // namespace doris