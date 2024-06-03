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
#include "vec/columns/columns_number.h"
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
        TYPE_BOOLEAN,  TYPE_TINYINT,  TYPE_SMALLINT, TYPE_INT,        TYPE_BIGINT,
        TYPE_LARGEINT, TYPE_FLOAT,    TYPE_TIME,     TYPE_DOUBLE,     TYPE_TIMEV2,
        TYPE_CHAR,     TYPE_VARCHAR,  TYPE_STRING,   TYPE_HLL,        TYPE_OBJECT,
        TYPE_DATE,     TYPE_DATETIME, TYPE_DATEV2,   TYPE_DATETIMEV2, TYPE_DECIMALV2};

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
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);

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
            const TypeDescriptor& res_type = ctx->root()->type();
            TPrimitiveType::type t_type = doris::to_thrift(res_type.type);
            // collect result
            PExprResult expr_result;
            string result;
            const auto& column_ptr = tmp_block.get_by_position(result_column).column;
            const auto& column_type = tmp_block.get_by_position(result_column).type;
            // 4 from fe: Config.be_exec_version maybe need remove after next version, now in 2.1
            if (_runtime_state->be_exec_version() >= 4 && params.__isset.is_nereids &&
                params.is_nereids) {
                auto* p_type_desc = expr_result.mutable_type_desc();
                auto* p_values = expr_result.mutable_result_content();
                res_type.to_protobuf(p_type_desc);
                auto datatype_serde = column_type->get_serde();
                RETURN_IF_ERROR(datatype_serde->write_column_to_pb(
                        *column_ptr->convert_to_full_column_if_const(), *p_values, 0, 1));
                expr_result.set_success(true);
                // after refactor, this field is useless, but it's required
                expr_result.set_content("ERROR");
                expr_result.mutable_type()->set_type(t_type);
                pexpr_result_map.mutable_map()->insert({n.first, expr_result});
            } else {
                if (column_ptr->is_null_at(0)) {
                    expr_result.set_success(false);
                } else {
                    expr_result.set_success(true);
                    StringRef string_ref;
                    auto type = ctx->root()->type().type;
                    //eg: strcut, array, map VARIANT... will not impl get_data_at, so could use column->to_string()
                    if (PRIMITIVE_TYPE_SET.contains(type)) {
                        string_ref = column_ptr->get_data_at(0);
                    }
                    RETURN_IF_ERROR(_get_result((void*)string_ref.data, string_ref.size,
                                                ctx->root()->type(), column_ptr, column_type,
                                                result));
                }
                expr_result.set_content(std::move(result));
                expr_result.mutable_type()->set_type(t_type);
                expr_result.mutable_type()->set_scale(res_type.scale);
                expr_result.mutable_type()->set_precision(res_type.precision);
                expr_result.mutable_type()->set_len(res_type.len);
                pexpr_result_map.mutable_map()->insert({n.first, expr_result});
            }
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

Status FoldConstantExecutor::_get_result(void* src, size_t size, const TypeDescriptor& type,
                                         const vectorized::ColumnPtr column_ptr,
                                         const vectorized::DataTypePtr column_type,
                                         std::string& result) {
    switch (type.type) {
    case TYPE_BOOLEAN: {
        bool val = *reinterpret_cast<const bool*>(src);
        result = val ? "true" : "false";
        break;
    }
    case TYPE_TINYINT: {
        int8_t val = *reinterpret_cast<const int8_t*>(src);
        result = fmt::format(FMT_COMPILE("{}"), val);
        break;
    }
    case TYPE_SMALLINT: {
        int16_t val = *reinterpret_cast<const int16_t*>(src);
        result = fmt::format(FMT_COMPILE("{}"), val);
        break;
    }
    case TYPE_INT: {
        int32_t val = *reinterpret_cast<const int32_t*>(src);
        result = fmt::format(FMT_COMPILE("{}"), val);
        break;
    }
    case TYPE_BIGINT: {
        int64_t val = *reinterpret_cast<const int64_t*>(src);
        result = fmt::format(FMT_COMPILE("{}"), val);
        break;
    }
    case TYPE_LARGEINT: {
        result = LargeIntValue::to_string(*reinterpret_cast<__int128*>(src));
        break;
    }
    case TYPE_FLOAT: {
        float val = *reinterpret_cast<const float*>(src);
        result = fmt::format(FMT_COMPILE("{}"), val);
        break;
    }
    case TYPE_TIME:
    case TYPE_DOUBLE: {
        double val = *reinterpret_cast<double*>(src);
        result = fmt::format(FMT_COMPILE("{}"), val);
        break;
    }
    case TYPE_TIMEV2: {
        constexpr static auto ratio_to_time = (1000 * 1000);
        double val = *reinterpret_cast<double*>(src);
        result = fmt::format(FMT_COMPILE("{}"), val / ratio_to_time);
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
    case TYPE_HLL:
    case TYPE_OBJECT: {
        result = std::string((char*)src, size);
        break;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        auto* date_value = reinterpret_cast<VecDateTimeValue*>(src);
        char str[MAX_DTVALUE_STR_LEN];
        date_value->to_string(str);
        result = std::string(str);
        break;
    }
    case TYPE_DATEV2: {
        DateV2Value<DateV2ValueType> value =
                binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(*(int32_t*)src);

        char buf[64];
        char* pos = value.to_string(buf);
        result = std::string(buf, pos - buf - 1);
        break;
    }
    case TYPE_DATETIMEV2: {
        DateV2Value<DateTimeV2ValueType> value =
                binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(*(int64_t*)src);

        char buf[64];
        char* pos = value.to_string(buf, type.scale);
        result = std::string(buf, pos - buf - 1);
        break;
    }
    case TYPE_DECIMALV2: {
        result = reinterpret_cast<DecimalV2Value*>(src)->to_string(type.scale);
        break;
    }
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256: {
        result = column_type->to_string(*column_ptr, 0);
        break;
    }
    case TYPE_ARRAY:
    case TYPE_JSONB:
    case TYPE_MAP:
    case TYPE_STRUCT: {
        result = column_type->to_string(*column_ptr, 0);
        break;
    }
    case TYPE_VARIANT:
    case TYPE_QUANTILE_STATE: {
        result = column_type->to_string(*column_ptr, 0);
        break;
    }
    case TYPE_IPV4:
    case TYPE_IPV6: {
        result = column_type->to_string(*column_ptr, 0);
        break;
    }
    default:
        auto error_msg =
                fmt::format("Type not implemented:{} need check it, and exec_query_id is: {}.",
                            type.debug_string(), query_id_string());
        return Status::InternalError(error_msg);
    }
    return Status::OK();
}

} // namespace doris