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

#include <map>
#include <string>

#include "runtime/fold_constant_mgr.h"
#include "runtime/tuple_row.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/mem_tracker.h"
#include "exprs/expr_context.h"
#include "exprs/expr.h"
#include "common/object_pool.h"
#include "common/status.h"

#include "gen_cpp/internal_service.pb.h"
#include "gen_cpp/PaloInternalService_types.h"

using std::string;
using std::map;

namespace doris {

TUniqueId FoldConstantMgr::_dummy_id;

FoldConstantMgr::FoldConstantMgr(ExecEnv* exec_env)
    : _exec_env(exec_env), _pool(){
    
}

Status FoldConstantMgr::fold_constant_expr(
        const TFoldConstantParams& params, PConstantExprResult* response) {
    auto expr_map = params.expr_map; 
    auto expr_result_map = response->mutable_expr_result_map();

    TQueryGlobals query_globals = params.query_globals;

    // init
    Status status = init(query_globals);
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to init mem trackers, msg: " << status.get_error_msg();
        return status;
    }

    for (auto m : expr_map) {
        PExprResultMap pexpr_result_map;
        for (auto n : m.second) {
            ExprContext* ctx = nullptr;
            TExpr& texpr = n.second;
            // create expr tree from TExpr
            RETURN_IF_ERROR(Expr::create_expr_tree(&_pool, texpr, &ctx));
            // prepare and open context
            status = prepare_and_open(ctx);
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
                result = get_result(src, ctx->root()->type().type);
            }

            expr_result.set_content(result);
            expr_result.mutable_type()->set_type(t_type);
            
            pexpr_result_map.mutable_map()->insert({n.first, expr_result});

            // close context expr
            ctx->close(_runtime_state.get());
        }

        expr_result_map->insert({m.first, pexpr_result_map});
    }

    return Status::OK();
         
}

Status FoldConstantMgr::init(TQueryGlobals query_globals) {
    // init runtime state, runtime profile
    TPlanFragmentExecParams params;
    params.fragment_instance_id = FoldConstantMgr::_dummy_id;
    params.query_id = FoldConstantMgr::_dummy_id;
    TExecPlanFragmentParams fragment_params;
    fragment_params.params = params;
    fragment_params.protocol_version = PaloInternalServiceVersion::V1;
    TQueryOptions query_options;
    _runtime_state.reset(new RuntimeState(fragment_params.params, query_options, query_globals,
                                          ExecEnv::GetInstance()));
    DescriptorTbl* desc_tbl = NULL;
    TDescriptorTable* t_desc_tbl = new TDescriptorTable();
    Status status = DescriptorTbl::create(_runtime_state->obj_pool(), *t_desc_tbl, &desc_tbl);
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to create descriptor table, msg: " << status.get_error_msg();
        return Status::Uninitialized(status.get_error_msg());
    }
    _runtime_state->set_desc_tbl(desc_tbl);
    status = _runtime_state->init_mem_trackers(FoldConstantMgr::_dummy_id);
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

Status FoldConstantMgr::prepare_and_open(ExprContext* ctx) {
    RowDescriptor* desc = new RowDescriptor();
    ctx -> prepare(_runtime_state.get(), *desc, _mem_tracker);
    return ctx -> open(_runtime_state.get());
}

string FoldConstantMgr::get_result(void* src, PrimitiveType slot_type){
    switch (slot_type) {
    case TYPE_NULL: {
        return NULL;
    }
    case TYPE_BOOLEAN: {
        bool val = *reinterpret_cast<const bool*>(src);
        return val ? "true" : "false";
    }
    case TYPE_TINYINT: {
        int8_t val = *reinterpret_cast<const int8_t*>(src);
        string s;
        s.push_back(val);
        return s;
    }
    case TYPE_SMALLINT: {
        int16_t val = *reinterpret_cast<const int16_t*>(src);
        return std::to_string(val);
    }
    case TYPE_INT: {
        int32_t val = *reinterpret_cast<const int32_t*>(src);
        return std::to_string(val);
    }
    case TYPE_BIGINT: {
        int64_t val = *reinterpret_cast<const int64_t*>(src);
        return std::to_string(val);
    }
    case TYPE_LARGEINT: {
        char buf[48];
        int len = 48;
        char* v = LargeIntValue::to_string(*reinterpret_cast<__int128*>(src), buf, &len);
        return std::string(v, len);
    }
    case TYPE_FLOAT: {
        float val = *reinterpret_cast<const float*>(src);
        return std::to_string(val);
    }
    case TYPE_TIME:
    case TYPE_DOUBLE: {
        double val = *reinterpret_cast<double*>(src);
        return std::to_string(val);
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT: {
        return (reinterpret_cast<StringValue*>(src))->debug_string();
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        const DateTimeValue date_value = *reinterpret_cast<DateTimeValue*>(src);
        char str[MAX_DTVALUE_STR_LEN];
        date_value.to_string(str);
        return str;
    }
    case TYPE_DECIMALV2: {
        return reinterpret_cast<DecimalV2Value*>(src)->to_string();
    }
    default:
        DCHECK(false) << "Type not implemented: " << slot_type;
        return NULL;
    }
    return NULL;
}


}

