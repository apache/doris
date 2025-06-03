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

#include <string>
#include <unordered_map>
#include <utility>

#include "runtime/define_primitive_type.h"
#include "util/date_func.h"
#include "util/time.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/spill/spill_writer.h"

namespace doris {
#include "common/compile_check_begin.h"

class MaterializedSchemaTable {
public:
    struct MaterializedSchemaTableTimeSlice {
        MaterializedSchemaTableTimeSlice(uint64_t begin_timestamp, uint64_t end_timestamp, std::vector<uint64_t> materialized_timestamps, uint64_t disk_space_bytes)
                : begin_timestamp_(begin_timestamp), end_timestamp_(end_timestamp), materialized_timestamps_(materialized_timestamps), disk_space_bytes_(disk_space_bytes) {
                filename_ = std::to_string(begin_timestamp) + "_" + std::to_string(end_timestamp);
            }

            uint64_t begin_timestamp_;
            uint64_t end_timestamp_;
            std::vector<uint64_t> materialized_timestamps_;
            uint64_t disk_space_bytes_;
            std::string filename_;
    };

    MaterializedSchemaTable(TSchemaTableType::type type, vectorized::SpillDataDir* materialized_dir, size_t ttl_s) : type_(type), materialized_dir_(materialized_dir), ttl_s_(ttl_s) {}

    ~SchemaTableSpillManager() {}

    void init() {
        std::vector<std::string> filenames;
        std::string absolute_path = "/" + schema_table_spill_path(type);
        std::filesystem::list_dir(absolute_path, filenames);
    }

    void put_block(const std::shared_ptr<vectorized::Block>& block) {
        block_.merge(*block);
        curl_slice_timestamps_.push_back(UnixMillis());
    }

    Status get_reader(TSchemaTableType::type type,
                           const vectorized::VExprContextSPtrs& expr_ctxs,
                           std::vector<std::string>& time_slice_names) {
        DCHECK(schema_table_time_slice_map_.find(type) != schema_table_time_slice_map_.end());

        // 1. get datetime expr.
        vectorized::VExprContextSPtrs effective_expr_ctxs;
        for (auto expr_ctx : expr_ctxs) {
            if (expr_ctx->root()->get_child_names() == "datetime") {
                effective_expr_ctxs.push_back(expr_ctx);
                break;
            }
        }
        if (effective_expr_ctxs.empty()) {
            return Status::OK();
        }

        vectorized::Block time_slices_block;
        // 2. list time slices.
        list_schema_time_slices_(type, &time_slices_block);

        // 3. filter time slices
        RETURN_IF_ERROR(filter_schema_time_slices_(expr_ctxs, &time_slices_block));

        // 4. extract time slices name
        // time_slices_block -> time_slice_names
    }

    bool get_time_slice_in_stream(TSchemaTableType::type type, std::string time_slice_name,
                                  vectorized::Block* output) {
        for (auto& time_slice : schema_table_time_slice_map_[type]) {
            if (time_slice.date_ == time_slice_name) {
                auto block = time_slice.block_;
                if (block->rows() != 0) {
                    output->merge(block);
                    return true;
                }
                return false;
            }
        }
    }

    Status flush() {
        // lock
        if (block->rows() == 0) {
            return Status::OK();
        }
        size_t written_bytes = 0;
        vectorized::SpillWriterUPtr writer = std::make_unique<vectorized::SpillWriter>(
                state_->get_query_ctx()->resource_ctx(), profile_, stream_id_, batch_rows_,
                data_dir_, spill_dir_);
        RETURN_IF_ERROR(writer->open());
        RETURN_IF_ERROR(writer->write(state_, *block, written_bytes));
        time_slices_.push_back(
                MaterializedSchemaTableTimeSlice(curl_slice_timestamps_[0], curl_slice_timestamps_[-1], curl_slice_timestamps_, written_bytes));
        total_written_bytes_ = writer->get_written_bytes();
        return Status::OK();
    }

    void clear_expired() {
        for (auto& slice : time_slices_) {
            auto now = UnixMillis();
            std::vector<std::string> filenames;
            std::string absolute_path = "/" + schema_table_spill_path(type);
            std::filesystem::list_dir(absolute_path, filenames);
            for (auto& filename : filenames) {
                if (now - ttl_s_ * 1000 >
                    timestamp_from_datetime(filename).to_olap_datetime()) {
                    std::string file_path = absolute_path + "/" + filename;
                    if (std::filesystem::exists(file_path)) {
                        std::filesystem::remove(file_path);
                    }
                }
            }
        }
    }

    void clear_disk_space(uint64_t bytes) {
        for (auto& slice : time_slices_) {
            
        }
    }

private:
    void list_schema_time_slices_(TSchemaTableType::type type, vectorized::Block* block) {
        // 1. list spilled files.
        std::vector<std::string> filenames;
        std::string absolute_path = "/" + schema_table_spill_path(type);
        std::filesystem::list_dir(absolute_path, filenames);

        // 2. add in stream time slice.
        for (auto& time_slice : schema_table_time_slice_map_[type]) {
            filenames.push_back(time_slice.date_);
        }

        // 3. insert block, only one column of type DATETIMEV2, insert all filenames.
        auto data_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false);
        block->insert(vectorized::ColumnWithTypeAndName(data_type->create_column(), data_type,
                                                        "datetime"));
        block->reserve(filenames.size());
        vectorized::MutableColumnPtr mutable_col_ptr;
        mutable_col_ptr = std::move(block->get_by_position(0).column).assume_mutable();
        auto* nullable_column =
                reinterpret_cast<vectorized::ColumnNullable*>(mutable_col_ptr.get());
        vectorized::IColumn* col_ptr = &nullable_column->get_nested_column();
        for (auto filename : filenames) {
            std::vector<void*> datas(1);
            VecDateTimeValue src[1];
            src[0].from_date_str(filename.data(), filename.size());
            datas[0] = src;
            auto* data = datas[0];
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_data(
                    reinterpret_cast<char*>(data), 0);
        }
    }

    Status filter_schema_time_slices_(const vectorized::VExprContextSPtrs& expr_ctxs,
                                      vectorized::Block* block) {
        DCHECK(!expr_ctxs.empty());
        if (block->rows() == 0) {
            return Status::OK();
        }

        int prev_columns = block->columns();
        vectorized::IColumn::Filter filter;
        std::vector<ColumnId> columns_to_filter;
        columns_to_filter.push_back(0);
        RETURN_IF_ERROR(vectorized::VExprContext::execute_conjuncts_and_filter_block(
                expr_ctxs, block, columns_to_filter, prev_columns, filter));

        return Status::OK();
    }

    Status prepare();

    TSchemaTableType::type type_;
    vectorized::SpillDataDir* materialized_dir_ = nullptr;

    vectorized::Block block_;
    std::vector<uint64_t> curl_slice_timestamps_;
    std::vector<MaterializedSchemaTableTimeSlice> time_slices_;
    size_t ttl_s_;

    RuntimeState* state_ = nullptr;
    int64_t stream_id_;
    std::string spill_dir_;
    size_t batch_rows_;
    int64_t total_written_bytes_ = 0;
};

#include "common/compile_check_end.h"
} // namespace doris
