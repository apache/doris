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

#include "format_v2/materialized_reader_util.h"

#include <utility>

#include "core/block/block.h"
#include "core/data_type/data_type_nullable.h"
#include "exprs/vexpr_context.h"
#include "format_v2/file_reader.h"
#include "io/io_common.h"

namespace doris::format {
namespace {

void update_counter(RuntimeProfile::Counter* counter, int64_t value) {
    if (counter != nullptr) {
        COUNTER_UPDATE(counter, value);
    }
}

} // namespace

ColumnPtr make_column_nullable_if_needed(ColumnPtr column, const DataTypePtr& target_type) {
    if (target_type != nullptr && target_type->is_nullable() && column.get() != nullptr &&
        !column->is_nullable()) {
        return make_nullable(std::move(column));
    }
    return column;
}

Status apply_materialized_reader_filters(const FileScanRequest* request, io::IOContext* io_ctx,
                                         Block* file_block, size_t* rows,
                                         const MaterializedReaderFilterProfile* profile) {
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    const size_t rows_before_filter = *rows;
    size_t rows_after_delete_filter = rows_before_filter;
    if (request != nullptr && rows_before_filter > 0 && !request->delete_conjuncts.empty()) {
        {
            SCOPED_TIMER(profile == nullptr ? nullptr : profile->delete_conjunct_filter_time);
            RETURN_IF_ERROR(VExprContext::filter_block(request->delete_conjuncts, file_block,
                                                       file_block->columns()));
        }
        rows_after_delete_filter =
                file_block->columns() == 0 ? rows_before_filter : file_block->rows();
        if (profile != nullptr) {
            update_counter(profile->rows_filtered_by_delete_conjunct,
                           rows_before_filter - rows_after_delete_filter);
        }
    }

    size_t rows_after_filter = rows_after_delete_filter;
    if (request != nullptr && rows_after_delete_filter > 0 && !request->conjuncts.empty()) {
        {
            SCOPED_TIMER(profile == nullptr ? nullptr : profile->conjunct_filter_time);
            RETURN_IF_ERROR(VExprContext::filter_block(request->conjuncts, file_block,
                                                       file_block->columns()));
        }
        rows_after_filter =
                file_block->columns() == 0 ? rows_after_delete_filter : file_block->rows();
        const auto rows_filtered_by_conjunct = rows_after_delete_filter - rows_after_filter;
        if (profile != nullptr) {
            update_counter(profile->rows_filtered_by_conjunct, rows_filtered_by_conjunct);
        }
        if (io_ctx != nullptr) {
            io_ctx->predicate_filtered_rows += rows_filtered_by_conjunct;
        }
    }
    *rows = rows_after_filter;
    return Status::OK();
}

} // namespace doris::format
