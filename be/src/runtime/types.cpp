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

#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>
#include <stddef.h>

#include <ostream>
#include <utility>

#include "olap/olap_define.h"
#include "runtime/primitive_type.h"

namespace doris {

TypeDescriptor::TypeDescriptor(const std::vector<TTypeNode>& types, int* idx)
        : len(-1), precision(-1), scale(-1) {
    DCHECK_GE(*idx, 0);
    DCHECK_LT(*idx, types.size());
    const TTypeNode& node = types[*idx];
    switch (node.type) {
    case TTypeNodeType::SCALAR: {
        DCHECK(node.__isset.scalar_type);
        const TScalarType& scalar_type = node.scalar_type;
        type = thrift_to_type(scalar_type.type);
        if (type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_HLL) {
            DCHECK(scalar_type.__isset.len);
            len = scalar_type.len;
        } else if (type == TYPE_DECIMALV2 || type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 ||
                   type == TYPE_DECIMAL128I || type == TYPE_DATETIMEV2 || type == TYPE_TIMEV2) {
            DCHECK(scalar_type.__isset.precision);
            DCHECK(scalar_type.__isset.scale);
            precision = scalar_type.precision;
            scale = scalar_type.scale;
        } else if (type == TYPE_STRING) {
            if (scalar_type.__isset.len) {
                len = scalar_type.len;
            } else {
                len = OLAP_STRING_MAX_LENGTH;
            }
        }
        break;
    }
    case TTypeNodeType::ARRAY: {
        DCHECK(!node.__isset.scalar_type);
        DCHECK_LT(*idx, types.size() - 1);
        type = TYPE_ARRAY;
        contains_nulls.reserve(1);
        // here should compatible with fe 1.2, because use contains_null in contains_nulls
        if (node.__isset.contains_nulls) {
            DCHECK_EQ(node.contains_nulls.size(), 1);
            contains_nulls.push_back(node.contains_nulls[0]);
        } else {
            contains_nulls.push_back(node.contains_null);
        }
        ++(*idx);
        children.push_back(TypeDescriptor(types, idx));
        break;
    }
    case TTypeNodeType::STRUCT: {
        DCHECK(!node.__isset.scalar_type);
        DCHECK_LT(*idx, types.size() - 1);
        DCHECK(!node.__isset.contains_nulls);
        DCHECK(node.__isset.struct_fields);
        DCHECK_GE(node.struct_fields.size(), 1);
        type = TYPE_STRUCT;
        contains_nulls.reserve(node.struct_fields.size());
        for (size_t i = 0; i < node.struct_fields.size(); i++) {
            ++(*idx);
            children.push_back(TypeDescriptor(types, idx));
            field_names.push_back(node.struct_fields[i].name);
            contains_nulls.push_back(node.struct_fields[i].contains_null);
        }
        break;
    }
    case TTypeNodeType::MAP: {
        //TODO(xy): handle contains_null[0] for key and [1] for value
        DCHECK(!node.__isset.scalar_type);
        DCHECK_LT(*idx, types.size() - 2);
        DCHECK_EQ(node.contains_nulls.size(), 2);
        contains_nulls.reserve(2);
        type = TYPE_MAP;
        ++(*idx);
        children.push_back(TypeDescriptor(types, idx));
        contains_nulls.push_back(node.contains_nulls[0]);
        ++(*idx);
        children.push_back(TypeDescriptor(types, idx));
        contains_nulls.push_back(node.contains_nulls[1]);
        break;
    }
    default:
        DCHECK(false) << node.type;
    }
}

void TypeDescriptor::to_thrift(TTypeDesc* thrift_type) const {
    thrift_type->types.push_back(TTypeNode());
    TTypeNode& node = thrift_type->types.back();
    if (is_complex_type()) {
        if (type == TYPE_ARRAY) {
            DCHECK_EQ(contains_nulls.size(), 1);
            node.type = TTypeNodeType::ARRAY;
            node.contains_nulls.reserve(1);
            node.contains_nulls.push_back(contains_nulls[0]);
        } else if (type == TYPE_MAP) {
            DCHECK_EQ(contains_nulls.size(), 2);
            node.type = TTypeNodeType::MAP;
            node.contains_nulls.reserve(2);
            node.contains_nulls.push_back(contains_nulls[0]);
            node.contains_nulls.push_back(contains_nulls[1]);
        } else if (type == TYPE_VARIANT) {
            node.type = TTypeNodeType::VARIANT;
        } else {
            DCHECK_EQ(type, TYPE_STRUCT);
            node.type = TTypeNodeType::STRUCT;
            node.__set_struct_fields(std::vector<TStructField>());
            for (size_t i = 0; i < field_names.size(); i++) {
                node.struct_fields.push_back(TStructField());
                node.struct_fields.back().name = field_names[i];
                node.struct_fields.back().contains_null = contains_nulls[i];
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
        if (type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_HLL || type == TYPE_STRING) {
            // DCHECK_NE(len, -1);
            scalar_type.__set_len(len);
        } else if (type == TYPE_DECIMALV2 || type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 ||
                   type == TYPE_DECIMAL128I || type == TYPE_DATETIMEV2) {
            DCHECK_NE(precision, -1);
            DCHECK_NE(scale, -1);
            scalar_type.__set_precision(precision);
            scalar_type.__set_scale(scale);
        }
    }
}

void TypeDescriptor::to_protobuf(PTypeDesc* ptype) const {
    auto node = ptype->add_types();
    node->set_type(TTypeNodeType::SCALAR);
    auto scalar_type = node->mutable_scalar_type();
    scalar_type->set_type(doris::to_thrift(type));
    if (type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_HLL || type == TYPE_STRING) {
        scalar_type->set_len(len);
    } else if (type == TYPE_DECIMALV2 || type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 ||
               type == TYPE_DECIMAL128I || type == TYPE_DATETIMEV2) {
        DCHECK_NE(precision, -1);
        DCHECK_NE(scale, -1);
        scalar_type->set_precision(precision);
        scalar_type->set_scale(scale);
    } else if (type == TYPE_ARRAY) {
        node->set_type(TTypeNodeType::ARRAY);
        node->set_contains_null(contains_nulls[0]);
        for (const TypeDescriptor& child : children) {
            child.to_protobuf(ptype);
        }
    } else if (type == TYPE_STRUCT) {
        node->set_type(TTypeNodeType::STRUCT);
        DCHECK_EQ(field_names.size(), contains_nulls.size());
        for (size_t i = 0; i < field_names.size(); ++i) {
            auto field = node->add_struct_fields();
            field->set_name(field_names[i]);
            field->set_contains_null(contains_nulls[i]);
        }
        for (const TypeDescriptor& child : children) {
            child.to_protobuf(ptype);
        }
    } else if (type == TYPE_MAP) {
        node->set_type(TTypeNodeType::MAP);
        DCHECK_EQ(2, contains_nulls.size());
        node->add_contains_nulls(contains_nulls[0]);
        node->add_contains_nulls(contains_nulls[1]);
        for (const TypeDescriptor& child : children) {
            child.to_protobuf(ptype);
        }
    } else if (type == TYPE_VARIANT) {
        node->set_type(TTypeNodeType::VARIANT);
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
                   type == TYPE_DECIMAL128I || type == TYPE_DATETIMEV2) {
            DCHECK(scalar_type.has_precision());
            DCHECK(scalar_type.has_scale());
            precision = scalar_type.precision();
            scale = scalar_type.scale();
        } else if (type == TYPE_STRING) {
            if (scalar_type.has_len()) {
                len = scalar_type.len();
            } else {
                len = OLAP_STRING_MAX_LENGTH;
            }
        }
        break;
    }
    case TTypeNodeType::ARRAY: {
        type = TYPE_ARRAY;
        contains_nulls.push_back(true);
        if (node.has_contains_null()) {
            contains_nulls[0] = node.contains_null();
        }
        ++(*idx);
        children.push_back(TypeDescriptor(types, idx));
        break;
    }
    case TTypeNodeType::MAP: {
        type = TYPE_MAP;
        ++(*idx);
        children.push_back(TypeDescriptor(types, idx));
        ++(*idx);
        children.push_back(TypeDescriptor(types, idx));
        if (node.contains_nulls_size() > 1) {
            contains_nulls.push_back(node.contains_nulls(0));
            contains_nulls.push_back(node.contains_nulls(1));
        }
        break;
    }
    case TTypeNodeType::STRUCT: {
        type = TYPE_STRUCT;
        size_t children_size = node.struct_fields_size();
        for (size_t i = 0; i < children_size; ++i) {
            const auto& field = node.struct_fields(i);
            field_names.push_back(field.name());
            contains_nulls.push_back(field.contains_null());
        }
        for (size_t i = 0; i < children_size; ++i) {
            ++(*idx);
            children.push_back(TypeDescriptor(types, idx));
        }
        break;
    }
    case TTypeNodeType::VARIANT: {
        type = TYPE_VARIANT;
        break;
    }
    default:
        DCHECK(false) << node.type();
    }
}

void TypeDescriptor::add_sub_type(TypeDescriptor sub_type, bool is_nullable) {
    children.push_back(std::move(sub_type));
    contains_nulls.push_back(is_nullable);
}

void TypeDescriptor::add_sub_type(TypeDescriptor sub_type, std::string field_name,
                                  bool is_nullable) {
    children.push_back(std::move(sub_type));
    field_names.push_back(std::move(field_name));
    contains_nulls.push_back(is_nullable);
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
    case TYPE_DECIMAL128I:
        ss << "DECIMAL128(" << precision << ", " << scale << ")";
        return ss.str();
    case TYPE_ARRAY: {
        ss << "ARRAY<" << children[0].debug_string() << ">";
        return ss.str();
    }
    case TYPE_MAP:
        ss << "MAP<" << children[0].debug_string() << ", " << children[1].debug_string() << ">";
        return ss.str();
    case TYPE_STRUCT: {
        ss << "STRUCT<";
        for (size_t i = 0; i < children.size(); i++) {
            ss << field_names[i];
            ss << ":";
            ss << children[i].debug_string();
            if (i != children.size() - 1) {
                ss << ",";
            }
        }
        ss << ">";
        return ss.str();
    }
    case TYPE_VARIANT:
        ss << "VARIANT";
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
