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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/types.cpp
// and modified by Doris

#include "runtime/types.h"

#include <ostream>

namespace doris {

TypeDescriptor::TypeDescriptor(const std::vector<TTypeNode>& types, int* idx)
        : len(-1), precision(-1), scale(-1) {
    DCHECK_GE(*idx, 0);
    DCHECK_LT(*idx, types.size());
    const TTypeNode& node = types[*idx];
    switch (node.type) {
    case TTypeNodeType::SCALAR: {
        DCHECK(node.__isset.scalar_type);
        const TScalarType scalar_type = node.scalar_type;
        type = thrift_to_type(scalar_type.type);
        if (type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_HLL) {
            DCHECK(scalar_type.__isset.len);
            len = scalar_type.len;
        } else if (type == TYPE_DECIMALV2 || type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 ||
                   type == TYPE_DECIMAL128 || type == TYPE_DATETIMEV2) {
            DCHECK(scalar_type.__isset.precision);
            DCHECK(scalar_type.__isset.scale);
            precision = scalar_type.precision;
            scale = scalar_type.scale;
        }
        break;
    }
    case TTypeNodeType::ARRAY: {
        DCHECK(!node.__isset.scalar_type);
        DCHECK_LT(*idx, types.size() - 1);
        type = TYPE_ARRAY;
        if (node.__isset.contains_null) {
            contains_null = node.contains_null;
        }
        ++(*idx);
        children.push_back(TypeDescriptor(types, idx));
        break;
    }
    // case TTypeNodeType::STRUCT:
    //     type = TYPE_STRUCT;
    //     for (int i = 0; i < node.struct_fields.size(); ++i) {
    //         ++(*idx);
    //         children.push_back(TypeDescriptor(types, idx));
    //         field_names.push_back(node.struct_fields[i].name);
    //     }
    //     break;
    // case TTypeNodeType::ARRAY:
    //     DCHECK(!node.__isset.scalar_type);
    //     DCHECK_LT(*idx, types.size() - 1);
    //     type = TYPE_ARRAY;
    //     ++(*idx);
    //     children.push_back(TypeDescriptor(types, idx));
    //     break;
    // case TTypeNodeType::MAP:
    //     DCHECK(!node.__isset.scalar_type);
    //     DCHECK_LT(*idx, types.size() - 2);
    //     type = TYPE_MAP;
    //     ++(*idx);
    //     children.push_back(TypeDescriptor(types, idx));
    //     ++(*idx);
    //     children.push_back(TypeDescriptor(types, idx));
    //     break;
    default:
        DCHECK(false) << node.type;
    }
}

void TypeDescriptor::to_thrift(TTypeDesc* thrift_type) const {
    thrift_type->types.push_back(TTypeNode());
    TTypeNode& node = thrift_type->types.back();
    if (is_complex_type()) {
        if (type == TYPE_ARRAY) {
            node.type = TTypeNodeType::ARRAY;
        } else if (type == TYPE_MAP) {
            node.type = TTypeNodeType::MAP;
        } else {
            DCHECK_EQ(type, TYPE_STRUCT);
            node.type = TTypeNodeType::STRUCT;
            node.__set_struct_fields(std::vector<TStructField>());
            for (auto& field_name : field_names) {
                node.struct_fields.push_back(TStructField());
                node.struct_fields.back().name = field_name;
            }
        }
        for (const TypeDescriptor& child : children) {
            child.to_thrift(thrift_type);
        }
    } else {
        node.type = TTypeNodeType::SCALAR;
        node.__set_scalar_type(TScalarType());
        TScalarType& scalar_type = node.scalar_type;
        scalar_type.__set_type(doris::to_thrift(type));
        if (type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_HLL) {
            // DCHECK_NE(len, -1);
            scalar_type.__set_len(len);
        } else if (type == TYPE_DECIMALV2 || type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 ||
                   type == TYPE_DECIMAL128 || type == TYPE_DATETIMEV2) {
            DCHECK_NE(precision, -1);
            DCHECK_NE(scale, -1);
            scalar_type.__set_precision(precision);
            scalar_type.__set_scale(scale);
        }
    }
}

void TypeDescriptor::to_protobuf(PTypeDesc* ptype) const {
    DCHECK(!is_complex_type() || type == TYPE_ARRAY)
            << "Don't support complex type now, type=" << type;
    auto node = ptype->add_types();
    node->set_type(TTypeNodeType::SCALAR);
    auto scalar_type = node->mutable_scalar_type();
    scalar_type->set_type(doris::to_thrift(type));
    if (type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_HLL) {
        scalar_type->set_len(len);
    } else if (type == TYPE_DECIMALV2 || type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 ||
               type == TYPE_DECIMAL128 || type == TYPE_DATETIMEV2) {
        DCHECK_NE(precision, -1);
        DCHECK_NE(scale, -1);
        scalar_type->set_precision(precision);
        scalar_type->set_scale(scale);
    } else if (type == TYPE_ARRAY) {
        node->set_type(TTypeNodeType::ARRAY);
        for (const TypeDescriptor& child : children) {
            child.to_protobuf(ptype);
        }
    }
}

TypeDescriptor::TypeDescriptor(const google::protobuf::RepeatedPtrField<PTypeNode>& types, int* idx)
        : len(-1), precision(-1), scale(-1) {
    DCHECK_GE(*idx, 0);
    DCHECK_LT(*idx, types.size());

    const PTypeNode& node = types.Get(*idx);
    switch (node.type()) {
    case TTypeNodeType::SCALAR: {
        DCHECK(node.has_scalar_type());
        const PScalarType& scalar_type = node.scalar_type();
        type = thrift_to_type((TPrimitiveType::type)scalar_type.type());
        if (type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_HLL) {
            DCHECK(scalar_type.has_len());
            len = scalar_type.len();
        } else if (type == TYPE_DECIMALV2 || type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 ||
                   type == TYPE_DECIMAL128 || type == TYPE_DATETIMEV2) {
            DCHECK(scalar_type.has_precision());
            DCHECK(scalar_type.has_scale());
            precision = scalar_type.precision();
            scale = scalar_type.scale();
        }
        break;
    }
    case TTypeNodeType::ARRAY: {
        type = TYPE_ARRAY;
        if (node.has_contains_null()) {
            contains_null = node.contains_null();
        }
        ++(*idx);
        children.push_back(TypeDescriptor(types, idx));
        break;
    }
    default:
        DCHECK(false) << node.type();
    }
}

std::string TypeDescriptor::debug_string() const {
    std::stringstream ss;
    switch (type) {
    case TYPE_CHAR:
        ss << "CHAR(" << len << ")";
        return ss.str();
    case TYPE_DECIMALV2:
        ss << "DECIMALV2(" << precision << ", " << scale << ")";
        return ss.str();
    case TYPE_DECIMAL32:
        ss << "DECIMAL32(" << precision << ", " << scale << ")";
        return ss.str();
    case TYPE_DECIMAL64:
        ss << "DECIMAL64(" << precision << ", " << scale << ")";
        return ss.str();
    case TYPE_DECIMAL128:
        ss << "DECIMAL128(" << precision << ", " << scale << ")";
        return ss.str();
    case TYPE_ARRAY:
        ss << "ARRAY(" << type_to_string(children[0].type) << ")";
        return ss.str();
    default:
        return type_to_string(type);
    }
}

std::ostream& operator<<(std::ostream& os, const TypeDescriptor& type) {
    os << type.debug_string();
    return os;
}

TTypeDesc create_type_desc(PrimitiveType type, int precision, int scale) {
    TTypeDesc type_desc;
    std::vector<TTypeNode> node_type;
    node_type.emplace_back();
    TScalarType scalarType;
    scalarType.__set_type(to_thrift(type));
    scalarType.__set_len(-1);
    scalarType.__set_precision(precision);
    scalarType.__set_scale(scale);
    node_type.back().__set_scalar_type(scalarType);
    type_desc.__set_types(node_type);
    return type_desc;
}
} // namespace doris
