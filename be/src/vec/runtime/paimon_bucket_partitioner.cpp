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

#include "vec/runtime/paimon_bucket_partitioner.h"

#ifdef WITH_PAIMON_CPP

#include <arrow/c/bridge.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "paimon/utils/bucket_id_calculator.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/arrow/block_convertor.h"
#include "util/arrow/row_batch.h"
#include "vec/common/allocator.h"
#include "vec/exec/format/parquet/arrow_memory_pool.h"
#include "vec/sink/writer/paimon/paimon_doris_memory_pool.h"

namespace doris::vectorized {

PaimonBucketPartitioner::PaimonBucketPartitioner(size_t partition_count,
                                                 PaimonBucketShuffleParams params)
        : PartitionerBase(partition_count), _params(std::move(params)) {}

Status PaimonBucketPartitioner::init(const std::vector<TExpr>& texprs) {
    if (!texprs.empty()) {
        return Status::InvalidArgument("paimon bucket partitioner does not accept partition exprs");
    }
    if (_params.bucket_num <= 0) {
        return Status::InvalidArgument("paimon bucket partitioner requires bucket_num > 0");
    }
    if (_params.bucket_keys.empty()) {
        return Status::InvalidArgument("paimon bucket partitioner requires bucket_keys");
    }
    return Status::OK();
}

Status PaimonBucketPartitioner::prepare(RuntimeState* /*state*/,
                                        const RowDescriptor& /*row_desc*/) {
    return Status::OK();
}

Status PaimonBucketPartitioner::open(RuntimeState* state) {
    if (_calculator != nullptr) {
        return Status::OK();
    }
    _pool = std::make_shared<PaimonDorisMemoryPool>(
            ::doris::QueryThreadContext {state->query_id(), state->query_mem_tracker(),
                                         state->get_query_ctx()->workload_group()});
    auto calc_res = ::paimon::BucketIdCalculator::Create(false, _params.bucket_num, _pool);
    if (!calc_res.ok()) {
        return Status::InternalError("failed to create paimon bucket calculator: {}",
                                     calc_res.status().ToString());
    }
    _calculator = std::move(calc_res).value();
    return Status::OK();
}

Status PaimonBucketPartitioner::_compute_bucket_ids(RuntimeState* state, const Block& block,
                                                    int32_t* bucket_ids,
                                                    MemTracker* mem_tracker) const {
    std::unordered_map<std::string, int> name_to_idx;
    name_to_idx.reserve(block.columns());
    for (int i = 0; i < block.columns(); ++i) {
        std::string col_name = block.get_by_position(i).name;
        if (col_name.empty()) {
            if (i < _params.column_names.size()) {
                col_name = _params.column_names[i];
            }
        }
        name_to_idx.emplace(col_name, i);
    }

    Block bucket_block;
    bucket_block.reserve(_params.bucket_keys.size());
    for (const auto& key_name : _params.bucket_keys) {
        auto it = name_to_idx.find(key_name);
        if (it == name_to_idx.end()) {
            return Status::InvalidArgument("paimon bucket key {} not found in input block",
                                           key_name);
        }
        bucket_block.insert(block.get_by_position(it->second));
    }

    ArrowMemoryPool<> arrow_pool;
    std::shared_ptr<arrow::Schema> arrow_schema;
    RETURN_IF_ERROR(get_arrow_schema_from_block(bucket_block, &arrow_schema, state->timezone()));
    std::shared_ptr<arrow::RecordBatch> record_batch;
    {
        SCOPED_CONSUME_MEM_TRACKER(mem_tracker);
        RETURN_IF_ERROR(convert_to_arrow_batch(bucket_block, arrow_schema, &arrow_pool,
                                               &record_batch, state->timezone_obj()));
    }

    std::vector<std::shared_ptr<arrow::Field>> bucket_fields;
    std::vector<std::shared_ptr<arrow::Array>> bucket_columns;
    bucket_fields.reserve(record_batch->num_columns());
    bucket_columns.reserve(record_batch->num_columns());
    for (int i = 0; i < record_batch->num_columns(); ++i) {
        bucket_fields.push_back(arrow_schema->field(i));
        bucket_columns.push_back(record_batch->column(i));
    }

    auto bucket_struct_res = arrow::StructArray::Make(bucket_columns, bucket_fields);
    if (!bucket_struct_res.ok()) {
        return Status::InternalError("failed to build bucket struct array: {}",
                                     bucket_struct_res.status().ToString());
    }
    std::shared_ptr<arrow::Array> bucket_struct = bucket_struct_res.ValueOrDie();
    std::shared_ptr<arrow::Schema> bucket_schema = arrow::schema(bucket_fields);

    ArrowArray c_bucket_array;
    auto arrow_status = arrow::ExportArray(*bucket_struct, &c_bucket_array);
    if (!arrow_status.ok()) {
        return Status::InternalError("failed to export bucket arrow array: {}",
                                     arrow_status.ToString());
    }
    ArrowSchema c_bucket_schema;
    arrow_status = arrow::ExportSchema(*bucket_schema, &c_bucket_schema);
    if (!arrow_status.ok()) {
        if (c_bucket_array.release) {
            c_bucket_array.release(&c_bucket_array);
        }
        return Status::InternalError("failed to export bucket arrow schema: {}",
                                     arrow_status.ToString());
    }

    auto paimon_st = _calculator->CalculateBucketIds(&c_bucket_array, &c_bucket_schema, bucket_ids);
    if (c_bucket_array.release) {
        c_bucket_array.release(&c_bucket_array);
    }
    if (c_bucket_schema.release) {
        c_bucket_schema.release(&c_bucket_schema);
    }
    if (!paimon_st.ok()) {
        return Status::InternalError("failed to calculate paimon bucket ids: {}",
                                     paimon_st.ToString());
    }
    return Status::OK();
}

Status PaimonBucketPartitioner::do_partitioning(RuntimeState* state, Block* block,
                                                MemTracker* mem_tracker) const {
    const int rows = block->rows();
    if (rows <= 0) {
        return Status::OK();
    }
    if (_calculator == nullptr) {
        return Status::InternalError("paimon bucket partitioner is not opened");
    }

    std::vector<int32_t> bucket_ids(rows, -1);
    RETURN_IF_ERROR(_compute_bucket_ids(state, *block, bucket_ids.data(), mem_tracker));

    _channel_ids.resize(rows);
    for (int i = 0; i < rows; ++i) {
        int32_t b = bucket_ids[i];
        uint32_t u = b < 0 ? 0 : static_cast<uint32_t>(b);
        _channel_ids[i] = u % static_cast<uint32_t>(_partition_count);
    }
    return Status::OK();
}

Status PaimonBucketPartitioner::clone(RuntimeState* state,
                                      std::unique_ptr<PartitionerBase>& partitioner) {
    auto* new_partitioner = new PaimonBucketPartitioner(_partition_count, _params);
    partitioner.reset(new_partitioner);
    RETURN_IF_ERROR(new_partitioner->init({}));
    RETURN_IF_ERROR(new_partitioner->open(state));
    return Status::OK();
}

} // namespace doris::vectorized

#endif
