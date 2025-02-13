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

std::string filter_type_to_string(RuntimeFilterType type) {
    switch (type) {
    case RuntimeFilterType::IN_FILTER: {
        return "in";
    }
    case RuntimeFilterType::BLOOM_FILTER: {
        return "bloomfilter";
    }
    case RuntimeFilterType::MIN_FILTER: {
        return "only_min";
    }
    case RuntimeFilterType::MAX_FILTER: {
        return "only_max";
    }
    case RuntimeFilterType::MINMAX_FILTER: {
        return "minmax";
    }
    case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
        return "in_or_bloomfilter";
    }
    case RuntimeFilterType::BITMAP_FILTER: {
        return "bitmapfilter";
    }
    default:
        return "unknown";
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

// PrimitiveType-> PColumnType
PColumnType to_proto(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return PColumnType::COLUMN_TYPE_BOOL;
    case TYPE_TINYINT:
        return PColumnType::COLUMN_TYPE_TINY_INT;
    case TYPE_SMALLINT:
        return PColumnType::COLUMN_TYPE_SMALL_INT;
    case TYPE_INT:
        return PColumnType::COLUMN_TYPE_INT;
    case TYPE_BIGINT:
        return PColumnType::COLUMN_TYPE_BIGINT;
    case TYPE_LARGEINT:
        return PColumnType::COLUMN_TYPE_LARGEINT;
    case TYPE_FLOAT:
        return PColumnType::COLUMN_TYPE_FLOAT;
    case TYPE_DOUBLE:
        return PColumnType::COLUMN_TYPE_DOUBLE;
    case TYPE_DATE:
        return PColumnType::COLUMN_TYPE_DATE;
    case TYPE_DATEV2:
        return PColumnType::COLUMN_TYPE_DATEV2;
    case TYPE_DATETIMEV2:
        return PColumnType::COLUMN_TYPE_DATETIMEV2;
    case TYPE_DATETIME:
        return PColumnType::COLUMN_TYPE_DATETIME;
    case TYPE_DECIMALV2:
        return PColumnType::COLUMN_TYPE_DECIMALV2;
    case TYPE_DECIMAL32:
        return PColumnType::COLUMN_TYPE_DECIMAL32;
    case TYPE_DECIMAL64:
        return PColumnType::COLUMN_TYPE_DECIMAL64;
    case TYPE_DECIMAL128I:
        return PColumnType::COLUMN_TYPE_DECIMAL128I;
    case TYPE_DECIMAL256:
        return PColumnType::COLUMN_TYPE_DECIMAL256;
    case TYPE_CHAR:
        return PColumnType::COLUMN_TYPE_CHAR;
    case TYPE_VARCHAR:
        return PColumnType::COLUMN_TYPE_VARCHAR;
    case TYPE_STRING:
        return PColumnType::COLUMN_TYPE_STRING;
    case TYPE_IPV4:
        return PColumnType::COLUMN_TYPE_IPV4;
    case TYPE_IPV6:
        return PColumnType::COLUMN_TYPE_IPV6;
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "runtime filter meet invalid PrimitiveType type {}", int(type));
    }
}

// PColumnType->PrimitiveType
PrimitiveType to_primitive_type(PColumnType type) {
    switch (type) {
    case PColumnType::COLUMN_TYPE_BOOL:
        return TYPE_BOOLEAN;
    case PColumnType::COLUMN_TYPE_TINY_INT:
        return TYPE_TINYINT;
    case PColumnType::COLUMN_TYPE_SMALL_INT:
        return TYPE_SMALLINT;
    case PColumnType::COLUMN_TYPE_INT:
        return TYPE_INT;
    case PColumnType::COLUMN_TYPE_BIGINT:
        return TYPE_BIGINT;
    case PColumnType::COLUMN_TYPE_LARGEINT:
        return TYPE_LARGEINT;
    case PColumnType::COLUMN_TYPE_FLOAT:
        return TYPE_FLOAT;
    case PColumnType::COLUMN_TYPE_DOUBLE:
        return TYPE_DOUBLE;
    case PColumnType::COLUMN_TYPE_DATE:
        return TYPE_DATE;
    case PColumnType::COLUMN_TYPE_DATEV2:
        return TYPE_DATEV2;
    case PColumnType::COLUMN_TYPE_DATETIMEV2:
        return TYPE_DATETIMEV2;
    case PColumnType::COLUMN_TYPE_DATETIME:
        return TYPE_DATETIME;
    case PColumnType::COLUMN_TYPE_DECIMALV2:
        return TYPE_DECIMALV2;
    case PColumnType::COLUMN_TYPE_DECIMAL32:
        return TYPE_DECIMAL32;
    case PColumnType::COLUMN_TYPE_DECIMAL64:
        return TYPE_DECIMAL64;
    case PColumnType::COLUMN_TYPE_DECIMAL128I:
        return TYPE_DECIMAL128I;
    case PColumnType::COLUMN_TYPE_DECIMAL256:
        return TYPE_DECIMAL256;
    case PColumnType::COLUMN_TYPE_VARCHAR:
        return TYPE_VARCHAR;
    case PColumnType::COLUMN_TYPE_CHAR:
        return TYPE_CHAR;
    case PColumnType::COLUMN_TYPE_STRING:
        return TYPE_STRING;
    case PColumnType::COLUMN_TYPE_IPV4:
        return TYPE_IPV4;
    case PColumnType::COLUMN_TYPE_IPV6:
        return TYPE_IPV6;
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "runtime filter meet invalid PColumnType type {}", int(type));
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
        return PFilterType::UNKNOW_FILTER;
    }
}

Status create_literal(const TypeDescriptor& type, const void* data, vectorized::VExprSPtr& expr) {
    try {
        TExprNode node = create_texpr_node_from(data, type.type, type.precision, type.scale);
        expr = vectorized::VLiteral::create_shared(node);
    } catch (const Exception& e) {
        return e.to_status();
    }

    return Status::OK();
}

Status create_vbin_predicate(const TypeDescriptor& type, TExprOpcode::type opcode,
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
    node.__set_child_type(to_thrift(type.type));
    node.__set_num_children(2);
    node.__set_output_scale(type.scale);
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
    scalar_type.__set_type(to_thrift(type.type));
    scalar_type.__set_precision(type.precision);
    scalar_type.__set_scale(type.scale);
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
