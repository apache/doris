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

#include <fmt/format.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <map>
#include <ostream>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/signal_handler.h"
#include "common/status.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/large_int_value.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/binary_cast.hpp"
#include "util/defer_op.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_vector.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

static std::unordered_set<PrimitiveType> PRIMITIVE_TYPE_SET {
        TYPE_BOOLEAN,  TYPE_TINYINT, TYPE_SMALLINT,   TYPE_INT,      TYPE_BIGINT,
        TYPE_LARGEINT, TYPE_FLOAT,   TYPE_DOUBLE,     TYPE_TIMEV2,   TYPE_CHAR,
        TYPE_VARCHAR,  TYPE_STRING,  TYPE_HLL,        TYPE_BITMAP,   TYPE_DATE,
        TYPE_DATETIME, TYPE_DATEV2,  TYPE_DATETIMEV2, TYPE_DECIMALV2};

Status FoldConstantExecutor::fold_constant_vexpr(const TFoldConstantParams& params,
                                                 PConstantExprResult* response) {
    const auto& expr_map = params.expr_map;
    auto* expr_result_map = response->mutable_expr_result_map();

    TQueryGlobals query_globals = params.query_globals;
    _query_id = params.query_id;
    LOG(INFO) << "fold_query_id: " << print_id(_query_id);
    // init
    RETURN_IF_ERROR(_init(query_globals, params.query_options));
    // only after init operation, _mem_tracker is ready
    SCOPED_ATTACH_TASK(_mem_tracker);
    signal::SignalTaskIdKeeper keeper(_query_id);

    for (const auto& m : expr_map) {
        PExprResultMap pexpr_result_map;
        for (const auto& n : m.second) {
            vectorized::VExprContextSPtr ctx;
            const TExpr& texpr = n.second;
            // create expr tree from TExpr
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(texpr, ctx));
            // prepare and open context
            RETURN_IF_ERROR(_prepare_and_open(ctx.get()));

            vectorized::Block tmp_block;
            tmp_block.insert({vectorized::ColumnUInt8::create(1),
                              std::make_shared<vectorized::DataTypeUInt8>(), ""});
            int result_column = -1;
            // calc vexpr
            RETURN_IF_ERROR(ctx->execute(&tmp_block, &result_column));
            DCHECK(result_column != -1);
            // covert to thrift type
            const auto& res_type = ctx->root()->data_type();
            TPrimitiveType::type t_type = doris::to_thrift(res_type->get_primitive_type());
            // collect result
            PExprResult expr_result;
            std::string result;
            const auto& column_ptr = tmp_block.get_by_position(result_column).column;
            const auto& column_type = tmp_block.get_by_position(result_column).type;
            // 4 from fe: Config.be_exec_version maybe need remove after next version, now in 2.1
            DCHECK(_runtime_state->be_exec_version() >= 4)
                    << "be_exec_version: " << _runtime_state->be_exec_version();
            auto* p_type_desc = expr_result.mutable_type_desc();
            auto* p_values = expr_result.mutable_result_content();
            res_type->to_protobuf(p_type_desc);
            auto datatype_serde = column_type->get_serde();
            RETURN_IF_ERROR(datatype_serde->write_column_to_pb(
                    *column_ptr->convert_to_full_column_if_const(), *p_values, 0, 1));
            expr_result.set_success(true);
            // after refactor, this field is useless, but it's required
            expr_result.set_content("ERROR");
            expr_result.mutable_type()->set_type(t_type);
            pexpr_result_map.mutable_map()->insert({n.first, expr_result});
        }
        expr_result_map->insert({m.first, pexpr_result_map});
    }
    return Status::OK();
}

Status FoldConstantExecutor::_init(const TQueryGlobals& query_globals,
                                   const TQueryOptions& query_options) {
    // init runtime state, runtime profile
    TPlanFragmentExecParams params;
    params.fragment_instance_id = _query_id;
    params.query_id = _query_id;
    TExecPlanFragmentParams fragment_params;
    fragment_params.params = params;
    fragment_params.protocol_version = PaloInternalServiceVersion::V1;
    _mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER,
            fmt::format("FoldConstant:query_id={}", print_id(_query_id)));
    _runtime_state =
            RuntimeState::create_unique(fragment_params.params, query_options, query_globals,
                                        ExecEnv::GetInstance(), nullptr, _mem_tracker);
    DescriptorTbl* desc_tbl = nullptr;
    Status status =
            DescriptorTbl::create(_runtime_state->obj_pool(), TDescriptorTable(), &desc_tbl);
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to create descriptor table, msg: " << status;
        return status;
    }
    _runtime_state->set_desc_tbl(desc_tbl);

    _runtime_profile = _runtime_state->runtime_profile();
    _runtime_profile->set_name("FoldConstantExpr");

    return Status::OK();
}

template <typename Context>
Status FoldConstantExecutor::_prepare_and_open(Context* ctx) {
    RETURN_IF_ERROR(ctx->prepare(_runtime_state.get(), RowDescriptor()));
    return ctx->open(_runtime_state.get());
}

} // namespace doris