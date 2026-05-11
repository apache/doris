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

#include "exprs/function/simple_function_factory.h"

#ifdef WITH_PAIMON_CPP

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/column/column_const.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/function.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "format/parquet/arrow_memory_pool.h"
#include "paimon/utils/bucket_id_calculator.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "exec/sink/writer/paimon/paimon_doris_memory_pool.h"

namespace doris {

struct PaimonBucketIdState {
    int32_t bucket_num = 0;
    std::shared_ptr<::paimon::MemoryPool> pool;
    std::unique_ptr<::paimon::BucketIdCalculator> calculator;
};

class FunctionPaimonBucketId final : public IFunction {
public:
    static constexpr auto name = "paimon_bucket_id";
    static FunctionPtr create() { return std::make_shared<FunctionPaimonBucketId>(); }
    String get_name() const override { return name; }
    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }
    bool use_default_implementation_for_constants() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt32>();
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope != FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        const int num_args = context->get_num_args();
        if (num_args < 2) {
            return Status::InvalidArgument("paimon_bucket_id requires at least 2 arguments");
        }
        const int bucket_num_arg_idx = num_args - 1;
        if (!context->is_col_constant(bucket_num_arg_idx)) {
            return Status::InvalidArgument("paimon_bucket_id requires constant bucket_num");
        }
        int64_t bucket_num64 = 0;
        if (!context->get_constant_col(bucket_num_arg_idx)->column_ptr->is_null_at(0)) {
            bucket_num64 = context->get_constant_col(bucket_num_arg_idx)->column_ptr->get_int(0);
        }
        if (bucket_num64 <= 0 || bucket_num64 > std::numeric_limits<int32_t>::max()) {
            return Status::InvalidArgument("invalid paimon bucket_num {}", bucket_num64);
        }
        auto st = std::make_shared<PaimonBucketIdState>();
        st->bucket_num = static_cast<int32_t>(bucket_num64);
        st->pool = std::make_shared<PaimonDorisMemoryPool>(context->state()->query_mem_tracker());
        auto calc_res = ::paimon::BucketIdCalculator::Create(false, st->bucket_num, st->pool);
        if (!calc_res.ok()) {
            return Status::InternalError("failed to create paimon bucket calculator: {}",
                                         calc_res.status().ToString());
        }
        st->calculator = std::move(calc_res).value();
        context->set_function_state(scope, st);
        return Status::OK();
    }

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            context->set_function_state(scope, nullptr);
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto* st = reinterpret_cast<PaimonBucketIdState*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        if (st == nullptr || st->calculator == nullptr) {
            return Status::InternalError("paimon_bucket_id state is not initialized");
        }
        if (arguments.size() < 2) {
            return Status::InvalidArgument("paimon_bucket_id requires at least 2 arguments");
        }

        Block key_block;
        key_block.reserve(arguments.size() - 1);
        for (size_t i = 0; i + 1 < arguments.size(); ++i) {
            const auto& col = block.get_by_position(arguments[i]);
            key_block.insert(col);
        }

        ArrowMemoryPool<> arrow_pool;
        std::shared_ptr<arrow::Schema> arrow_schema;
        RETURN_IF_ERROR(get_arrow_schema_from_block(key_block, &arrow_schema,
                                                    context->state()->timezone()));
        std::shared_ptr<arrow::RecordBatch> record_batch;
        RETURN_IF_ERROR(convert_to_arrow_batch(key_block, arrow_schema, &arrow_pool, &record_batch,
                                               context->state()->timezone_obj()));

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

        std::vector<int32_t> bucket_ids(input_rows_count, -1);
        auto paimon_st = st->calculator->CalculateBucketIds(&c_bucket_array, &c_bucket_schema,
                                                            bucket_ids.data());
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

        auto col_res = ColumnInt32::create(input_rows_count);
        auto& res_data = col_res->get_data();
        for (size_t i = 0; i < input_rows_count; ++i) {
            res_data[i] = bucket_ids[i];
        }
        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }
};

void register_function_paimon(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionPaimonBucketId>();
}

} // namespace doris

#else

namespace doris {
void register_function_paimon(SimpleFunctionFactory&) {}
} // namespace doris

#endif
