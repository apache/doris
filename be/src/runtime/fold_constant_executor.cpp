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
#include "runtime/fold_constant_executor.h"

#include <map>
#include <string>

#include "runtime/tuple_row.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/mem_tracker.h"
#include "exprs/expr_context.h"
#include "exprs/expr.h"
#include "common/object_pool.h"
#include "common/status.h"

#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

#include "gen_cpp/internal_service.pb.h"
#include "gen_cpp/PaloInternalService_types.h"

using std::string;
using std::map;

namespace doris {

TUniqueId FoldConstantExecutor::_dummy_id;

Status FoldConstantExecutor::fold_constant_expr(
        const TFoldConstantParams& params, PConstantExprResult* response) {
    const auto& expr_map = params.expr_map;
    auto expr_result_map = response->mutable_expr_result_map();

    TQueryGlobals query_globals = params.query_globals;
    // init
    Status status = _init(query_globals);
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to init mem trackers, msg: " << status.get_error_msg();
        return status;
    }

    for (const auto& m : expr_map) {
        PExprResultMap pexpr_result_map;
        for (const auto& n : m.second) {
            ExprContext* ctx = nullptr;
            const TExpr& texpr = n.second;
            // create expr tree from TExpr
            RETURN_IF_ERROR(Expr::create_expr_tree(&_pool, texpr, &ctx));
            // prepare and open context
            status = _prepare_and_open(ctx);
            if (UNLIKELY(!status.ok())) {
                LOG(WARNING) << "Failed to init mem trackers, msg: " << status.get_error_msg();
                return status;
            }

            TupleRow* row = nullptr;
            // calc expr
            void* src = ctx->get_value(row);
            PrimitiveType root_type = ctx->root()->type().type;
            // covert to thrift type
            TPrimitiveType::type t_type = doris::to_thrift(root_type);

            // collect result
            PExprResult expr_result;
            string result;
            if (src == nullptr) {
                expr_result.set_success(false);
            } else {
                expr_result.set_success(true);
                result = _get_result(src, 0, ctx->root()->type().type);
            }

            expr_result.set_content(std::move(result));
            expr_result.mutable_type()->set_type(t_type);
            pexpr_result_map.mutable_map()->insert({n.first, expr_result});

            // close context expr
            ctx->close(_runtime_state.get());
        }

        expr_result_map->insert({m.first, pexpr_result_map});
    }

    return Status::OK();
}

Status FoldConstantExecutor::fold_constant_vexpr(
        const TFoldConstantParams& params, PConstantExprResult* response) {
    const auto& expr_map = params.expr_map;
    auto expr_result_map = response->mutable_expr_result_map();

    TQueryGlobals query_globals = params.query_globals;
    // init
    Status status = _init(query_globals);
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to init mem trackers, msg: " << status.get_error_msg();
        return status;
    }

    for (const auto& m : expr_map) {
        PExprResultMap pexpr_result_map;
        for (const auto& n : m.second) {
            vectorized::VExprContext* ctx = nullptr;
            const TExpr& texpr = n.second;
            // create expr tree from TExpr
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(&_pool, texpr, &ctx));
            // prepare and open context
            status = _prepare_and_open(ctx);
            if (UNLIKELY(!status.ok())) {
                LOG(WARNING) << "Failed to init mem trackers, msg: " << status.get_error_msg();
                return status;
            }

            vectorized::Block tmp_block;
            tmp_block.insert({vectorized::ColumnUInt8::create(1),
                    std::make_shared<vectorized::DataTypeUInt8>(), ""});
            int result_column = -1;
            // calc vexpr
            RETURN_IF_ERROR(ctx->execute(&tmp_block, &result_column));
            DCHECK(result_column != -1);
            PrimitiveType root_type = ctx->root()->type().type;
            // covert to thrift type
            TPrimitiveType::type t_type = doris::to_thrift(root_type);

            // collect result
            PExprResult expr_result;
            string result;
            const auto& column_ptr = tmp_block.get_by_position(result_column).column;
            if (column_ptr->is_null_at(0)) {
                expr_result.set_success(false);
            } else {
                expr_result.set_success(true);
                auto string_ref = column_ptr->get_data_at(0);
                result = _get_result<true>((void*)string_ref.data, string_ref.size, ctx->root()->type().type);
            }

            expr_result.set_content(std::move(result));
            expr_result.mutable_type()->set_type(t_type);
            pexpr_result_map.mutable_map()->insert({n.first, expr_result});

            // close context expr
            ctx->close(_runtime_state.get());
        }
        expr_result_map->insert({m.first, pexpr_result_map});
    }

    return Status::OK();
}

Status FoldConstantExecutor::_init(const TQueryGlobals& query_globals) {
    // init runtime state, runtime profile
    TPlanFragmentExecParams params;
    params.fragment_instance_id = FoldConstantExecutor::_dummy_id;
    params.query_id = FoldConstantExecutor::_dummy_id;
    TExecPlanFragmentParams fragment_params;
    fragment_params.params = params;
    fragment_params.protocol_version = PaloInternalServiceVersion::V1;
    TQueryOptions query_options;
    _runtime_state.reset(new RuntimeState(fragment_params.params, query_options, query_globals,
                                          ExecEnv::GetInstance()));
    DescriptorTbl* desc_tbl = nullptr;
    Status status = DescriptorTbl::create(_runtime_state->obj_pool(), TDescriptorTable(), &desc_tbl);
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to create descriptor table, msg: " << status.get_error_msg();
        return Status::Uninitialized(status.get_error_msg());
    }
    _runtime_state->set_desc_tbl(desc_tbl);
    status = _runtime_state->init_mem_trackers(FoldConstantExecutor::_dummy_id);
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to init mem trackers, msg: " << status.get_error_msg();
        return Status::Uninitialized(status.get_error_msg());
    }

    _runtime_profile = _runtime_state->runtime_profile();
    _runtime_profile->set_name("FoldConstantExpr");
    _mem_tracker = MemTracker::CreateTracker(-1, "FoldConstantExpr", _runtime_state->instance_mem_tracker());
    _mem_pool.reset(new MemPool(_mem_tracker.get()));

    return Status::OK();
}

template <typename Context>
Status FoldConstantExecutor::_prepare_and_open(Context* ctx) {
    RETURN_IF_ERROR(ctx->prepare(_runtime_state.get(), RowDescriptor(), _mem_tracker));
    return ctx->open(_runtime_state.get());
}

template <bool is_vec>
string FoldConstantExecutor::_get_result(void* src, size_t size, PrimitiveType slot_type){
    switch (slot_type) {
    case TYPE_BOOLEAN: {
        bool val = *reinterpret_cast<const bool*>(src);
        return val ? "true" : "false";
    }
    case TYPE_TINYINT: {
        int8_t val = *reinterpret_cast<const int8_t*>(src);
        return fmt::format_int(val).str();
    }
    case TYPE_SMALLINT: {
        int16_t val = *reinterpret_cast<const int16_t*>(src);
        return fmt::format_int(val).str();
    }
    case TYPE_INT: {
        int32_t val = *reinterpret_cast<const int32_t*>(src);
        return fmt::format_int(val).str();
    }
    case TYPE_BIGINT: {
        int64_t val = *reinterpret_cast<const int64_t*>(src);
        return fmt::format_int(val).str();
    }
    case TYPE_LARGEINT: {
        return LargeIntValue::to_string(*reinterpret_cast<__int128*>(src));
    }
    case TYPE_FLOAT: {
        float val = *reinterpret_cast<const float*>(src);
        return fmt::format("{:.9g}", val);
    }
    case TYPE_TIME:
    case TYPE_DOUBLE: {
        double val = *reinterpret_cast<double*>(src);
        return fmt::format("{:.17g}", val);
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
    case TYPE_HLL:
    case TYPE_OBJECT: {
        if constexpr (is_vec) {
            return std::string((char*)src, size);
        }
        return (reinterpret_cast<StringValue*>(src))->to_string();
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        if constexpr (is_vec) {
            auto date_value = reinterpret_cast<vectorized::VecDateTimeValue*>(src);
            char str[MAX_DTVALUE_STR_LEN];
            date_value->to_string(str);
            return str;
        } else {
            const DateTimeValue date_value = *reinterpret_cast<DateTimeValue *>(src);
            char str[MAX_DTVALUE_STR_LEN];
            date_value.to_string(str);
            return str;
        }
    }
    case TYPE_DECIMALV2: {
        return reinterpret_cast<DecimalV2Value*>(src)->to_string();
    }
    default:
        DCHECK(false) << "Type not implemented: " << slot_type;
        return "";
    }
}


}

