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

#include "runtime_filter/utils.h"

#include "vec/exprs/vexpr.h"
#include "vec/exprs/vliteral.h"

namespace doris {
#include "common/compile_check_begin.h"
std::string filter_type_to_string(RuntimeFilterType type) {
    switch (type) {
    case RuntimeFilterType::IN_FILTER: {
        return "IN_FILTER";
    }
    case RuntimeFilterType::BLOOM_FILTER: {
        return "BLOOM_FILTER";
    }
    case RuntimeFilterType::MIN_FILTER: {
        return "MIN_FILTER";
    }
    case RuntimeFilterType::MAX_FILTER: {
        return "MAX_FILTER";
    }
    case RuntimeFilterType::MINMAX_FILTER: {
        return "MINMAX_FILTER";
    }
    case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
        return "IN_OR_BLOOM_FILTER";
    }
    case RuntimeFilterType::BITMAP_FILTER: {
        return "BITMAP_FILTER";
    }
    default:
        return "UNKNOWN_FILTER";
    }
}

RuntimeFilterType get_runtime_filter_type(const TRuntimeFilterDesc* desc) {
    switch (desc->type) {
    case TRuntimeFilterType::BLOOM: {
        return RuntimeFilterType::BLOOM_FILTER;
    }
    case TRuntimeFilterType::MIN_MAX: {
        if (desc->__isset.min_max_type) {
            if (desc->min_max_type == TMinMaxRuntimeFilterType::MIN) {
                return RuntimeFilterType::MIN_FILTER;
            } else if (desc->min_max_type == TMinMaxRuntimeFilterType::MAX) {
                return RuntimeFilterType::MAX_FILTER;
            }
        }
        return RuntimeFilterType::MINMAX_FILTER;
    }
    case TRuntimeFilterType::IN: {
        return RuntimeFilterType::IN_FILTER;
    }
    case TRuntimeFilterType::IN_OR_BLOOM: {
        return RuntimeFilterType::IN_OR_BLOOM_FILTER;
    }
    case TRuntimeFilterType::BITMAP: {
        return RuntimeFilterType::BITMAP_FILTER;
    }
    default: {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Invalid runtime filter type {}",
                               int(desc->type));
    }
    }
}

// PFilterType -> RuntimeFilterType
RuntimeFilterType get_type(int filter_type) {
    switch (filter_type) {
    case PFilterType::IN_FILTER: {
        return RuntimeFilterType::IN_FILTER;
    }
    case PFilterType::BLOOM_FILTER: {
        return RuntimeFilterType::BLOOM_FILTER;
    }
    case PFilterType::MINMAX_FILTER:
        return RuntimeFilterType::MINMAX_FILTER;
    case PFilterType::MIN_FILTER:
        return RuntimeFilterType::MIN_FILTER;
    case PFilterType::MAX_FILTER:
        return RuntimeFilterType::MAX_FILTER;
    case PFilterType::IN_OR_BLOOM_FILTER:
        return RuntimeFilterType::IN_OR_BLOOM_FILTER;
    default:
        return RuntimeFilterType::UNKNOWN_FILTER;
    }
}

// RuntimeFilterType -> PFilterType
PFilterType get_type(RuntimeFilterType type) {
    switch (type) {
    case RuntimeFilterType::IN_FILTER:
        return PFilterType::IN_FILTER;
    case RuntimeFilterType::BLOOM_FILTER:
        return PFilterType::BLOOM_FILTER;
    case RuntimeFilterType::MIN_FILTER:
        return PFilterType::MIN_FILTER;
    case RuntimeFilterType::MAX_FILTER:
        return PFilterType::MAX_FILTER;
    case RuntimeFilterType::MINMAX_FILTER:
        return PFilterType::MINMAX_FILTER;
    case RuntimeFilterType::IN_OR_BLOOM_FILTER:
        return PFilterType::IN_OR_BLOOM_FILTER;
    default:
        return PFilterType::UNKNOWN_FILTER;
    }
}

Status create_literal(const vectorized::DataTypePtr& type, const void* data,
                      vectorized::VExprSPtr& expr) {
    try {
        TExprNode node = create_texpr_node_from(data, type->get_primitive_type(),
                                                type->get_precision(), type->get_scale());
        expr = vectorized::VLiteral::create_shared(node);
    } catch (const Exception& e) {
        return e.to_status();
    }

    return Status::OK();
}

Status create_vbin_predicate(const vectorized::DataTypePtr& type, TExprOpcode::type opcode,
                             vectorized::VExprSPtr& expr, TExprNode* tnode, bool contain_null) {
    TExprNode node;
    TScalarType tscalar_type;
    tscalar_type.__set_type(TPrimitiveType::BOOLEAN);
    TTypeNode ttype_node;
    ttype_node.__set_type(TTypeNodeType::SCALAR);
    ttype_node.__set_scalar_type(tscalar_type);
    TTypeDesc t_type_desc;
    t_type_desc.types.push_back(ttype_node);
    node.__set_type(t_type_desc);
    node.__set_opcode(opcode);
    node.__set_child_type(to_thrift(type->get_primitive_type()));
    node.__set_num_children(2);
    node.__set_output_scale(type->get_scale());
    node.__set_node_type(contain_null ? TExprNodeType::NULL_AWARE_BINARY_PRED
                                      : TExprNodeType::BINARY_PRED);
    TFunction fn;
    TFunctionName fn_name;
    fn_name.__set_db_name("");
    switch (opcode) {
    case TExprOpcode::LE:
        fn_name.__set_function_name("le");
        break;
    case TExprOpcode::GE:
        fn_name.__set_function_name("ge");
        break;
    default:
        return Status::InternalError("Invalid opcode for max_min_runtimefilter: '{}'", opcode);
    }
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);

    TTypeNode type_node;
    type_node.__set_type(TTypeNodeType::SCALAR);
    TScalarType scalar_type;
    scalar_type.__set_type(to_thrift(type->get_primitive_type()));
    scalar_type.__set_precision(type->get_precision());
    scalar_type.__set_scale(type->get_scale());
    type_node.__set_scalar_type(scalar_type);

    std::vector<TTypeNode> type_nodes;
    type_nodes.push_back(type_node);

    TTypeDesc type_desc;
    type_desc.__set_types(type_nodes);

    std::vector<TTypeDesc> arg_types;
    arg_types.push_back(type_desc);
    arg_types.push_back(type_desc);
    fn.__set_arg_types(arg_types);

    fn.__set_ret_type(t_type_desc);
    fn.__set_has_var_args(false);
    node.__set_fn(fn);
    *tnode = node;
    return vectorized::VExpr::create_expr(node, expr);
}

} // namespace doris
