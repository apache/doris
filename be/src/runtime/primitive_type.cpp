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

#include "runtime/primitive_type.h"

#include <gen_cpp/Types_types.h>

#include <vector>

#include "runtime/define_primitive_type.h"

namespace doris {

bool is_type_compatible(PrimitiveType lhs, PrimitiveType rhs) {
    if (lhs == TYPE_VARCHAR) {
        return rhs == TYPE_CHAR || rhs == TYPE_VARCHAR || rhs == TYPE_HLL || rhs == TYPE_OBJECT ||
               rhs == TYPE_QUANTILE_STATE || rhs == TYPE_STRING;
    }

    if (lhs == TYPE_OBJECT) {
        return rhs == TYPE_VARCHAR || rhs == TYPE_OBJECT || rhs == TYPE_STRING;
    }

    if (lhs == TYPE_CHAR || lhs == TYPE_HLL) {
        return rhs == TYPE_CHAR || rhs == TYPE_VARCHAR || rhs == TYPE_HLL || rhs == TYPE_STRING;
    }

    if (lhs == TYPE_STRING) {
        return rhs == TYPE_CHAR || rhs == TYPE_VARCHAR || rhs == TYPE_HLL || rhs == TYPE_OBJECT ||
               rhs == TYPE_STRING;
    }

    if (lhs == TYPE_QUANTILE_STATE) {
        return rhs == TYPE_VARCHAR || rhs == TYPE_QUANTILE_STATE || rhs == TYPE_STRING;
    }

    return lhs == rhs;
}

PrimitiveType thrift_to_type(TPrimitiveType::type ttype) {
    switch (ttype) {
    case TPrimitiveType::INVALID_TYPE:
        return INVALID_TYPE;

    case TPrimitiveType::NULL_TYPE:
        return TYPE_NULL;

    case TPrimitiveType::BOOLEAN:
        return TYPE_BOOLEAN;

    case TPrimitiveType::TINYINT:
        return TYPE_TINYINT;

    case TPrimitiveType::SMALLINT:
        return TYPE_SMALLINT;

    case TPrimitiveType::INT:
        return TYPE_INT;

    case TPrimitiveType::BIGINT:
        return TYPE_BIGINT;

    case TPrimitiveType::LARGEINT:
        return TYPE_LARGEINT;

    case TPrimitiveType::FLOAT:
        return TYPE_FLOAT;

    case TPrimitiveType::DOUBLE:
        return TYPE_DOUBLE;

    case TPrimitiveType::DATE:
        return TYPE_DATE;

    case TPrimitiveType::DATETIME:
        return TYPE_DATETIME;

    case TPrimitiveType::DATEV2:
        return TYPE_DATEV2;

    case TPrimitiveType::DATETIMEV2:
        return TYPE_DATETIMEV2;

    case TPrimitiveType::TIMEV2:
        return TYPE_TIMEV2;

    case TPrimitiveType::TIME:
        return TYPE_TIME;

    case TPrimitiveType::VARCHAR:
        return TYPE_VARCHAR;

    case TPrimitiveType::STRING:
        return TYPE_STRING;

    case TPrimitiveType::JSONB:
        return TYPE_JSONB;

    case TPrimitiveType::BINARY:
        return TYPE_BINARY;

    case TPrimitiveType::DECIMALV2:
        return TYPE_DECIMALV2;

    case TPrimitiveType::DECIMAL32:
        return TYPE_DECIMAL32;

    case TPrimitiveType::DECIMAL64:
        return TYPE_DECIMAL64;

    case TPrimitiveType::DECIMAL128I:
        return TYPE_DECIMAL128I;

    case TPrimitiveType::CHAR:
        return TYPE_CHAR;

    case TPrimitiveType::HLL:
        return TYPE_HLL;

    case TPrimitiveType::OBJECT:
        return TYPE_OBJECT;

    case TPrimitiveType::QUANTILE_STATE:
        return TYPE_QUANTILE_STATE;

    case TPrimitiveType::ARRAY:
        return TYPE_ARRAY;

    case TPrimitiveType::MAP:
        return TYPE_MAP;

    case TPrimitiveType::STRUCT:
        return TYPE_STRUCT;

    case TPrimitiveType::LAMBDA_FUNCTION:
        return TYPE_LAMBDA_FUNCTION;

    case TPrimitiveType::AGG_STATE:
        return TYPE_AGG_STATE;

    case TPrimitiveType::VARIANT:
        return TYPE_VARIANT;
    default:
        CHECK(false) << ", meet unknown type " << ttype;
        return INVALID_TYPE;
    }
}

TPrimitiveType::type to_thrift(PrimitiveType ptype) {
    switch (ptype) {
    case INVALID_TYPE:
        return TPrimitiveType::INVALID_TYPE;

    case TYPE_NULL:
        return TPrimitiveType::NULL_TYPE;

    case TYPE_BOOLEAN:
        return TPrimitiveType::BOOLEAN;

    case TYPE_TINYINT:
        return TPrimitiveType::TINYINT;

    case TYPE_SMALLINT:
        return TPrimitiveType::SMALLINT;

    case TYPE_INT:
        return TPrimitiveType::INT;

    case TYPE_BIGINT:
        return TPrimitiveType::BIGINT;

    case TYPE_LARGEINT:
        return TPrimitiveType::LARGEINT;

    case TYPE_FLOAT:
        return TPrimitiveType::FLOAT;

    case TYPE_DOUBLE:
        return TPrimitiveType::DOUBLE;

    case TYPE_DATE:
        return TPrimitiveType::DATE;

    case TYPE_DATETIME:
        return TPrimitiveType::DATETIME;

    case TYPE_TIME:
        return TPrimitiveType::TIME;

    case TYPE_DATEV2:
        return TPrimitiveType::DATEV2;

    case TYPE_DATETIMEV2:
        return TPrimitiveType::DATETIMEV2;

    case TYPE_TIMEV2:
        return TPrimitiveType::TIMEV2;

    case TYPE_VARCHAR:
        return TPrimitiveType::VARCHAR;

    case TYPE_STRING:
        return TPrimitiveType::STRING;

    case TYPE_JSONB:
        return TPrimitiveType::JSONB;

    case TYPE_VARIANT:
        return TPrimitiveType::VARIANT;

    case TYPE_BINARY:
        return TPrimitiveType::BINARY;

    case TYPE_DECIMALV2:
        return TPrimitiveType::DECIMALV2;

    case TYPE_DECIMAL32:
        return TPrimitiveType::DECIMAL32;

    case TYPE_DECIMAL64:
        return TPrimitiveType::DECIMAL64;

    case TYPE_DECIMAL128I:
        return TPrimitiveType::DECIMAL128I;

    case TYPE_CHAR:
        return TPrimitiveType::CHAR;

    case TYPE_HLL:
        return TPrimitiveType::HLL;

    case TYPE_OBJECT:
        return TPrimitiveType::OBJECT;

    case TYPE_QUANTILE_STATE:
        return TPrimitiveType::QUANTILE_STATE;

    case TYPE_ARRAY:
        return TPrimitiveType::ARRAY;

    case TYPE_MAP:
        return TPrimitiveType::MAP;

    case TYPE_STRUCT:
        return TPrimitiveType::STRUCT;
    case TYPE_LAMBDA_FUNCTION:
        return TPrimitiveType::LAMBDA_FUNCTION;

    default:
        return TPrimitiveType::INVALID_TYPE;
    }
}

std::string type_to_string(PrimitiveType t) {
    switch (t) {
    case INVALID_TYPE:
        return "INVALID";

    case TYPE_NULL:
        return "NULL";

    case TYPE_BOOLEAN:
        return "BOOL";

    case TYPE_TINYINT:
        return "TINYINT";

    case TYPE_SMALLINT:
        return "SMALLINT";

    case TYPE_INT:
        return "INT";

    case TYPE_BIGINT:
        return "BIGINT";

    case TYPE_LARGEINT:
        return "LARGEINT";

    case TYPE_FLOAT:
        return "FLOAT";

    case TYPE_DOUBLE:
        return "DOUBLE";

    case TYPE_DATE:
        return "DATE";

    case TYPE_DATETIME:
        return "DATETIME";

    case TYPE_TIME:
        return "TIME";

    case TYPE_DATEV2:
        return "DATEV2";

    case TYPE_DATETIMEV2:
        return "DATETIMEV2";

    case TYPE_TIMEV2:
        return "TIMEV2";

    case TYPE_VARCHAR:
        return "VARCHAR";

    case TYPE_STRING:
        return "STRING";

    case TYPE_JSONB:
        return "JSONB";

    case TYPE_BINARY:
        return "BINARY";

    case TYPE_DECIMALV2:
        return "DECIMALV2";

    case TYPE_DECIMAL32:
        return "DECIMAL32";

    case TYPE_DECIMAL64:
        return "DECIMAL64";

    case TYPE_DECIMAL128I:
        return "DECIMAL128I";

    case TYPE_CHAR:
        return "CHAR";

    case TYPE_HLL:
        return "HLL";

    case TYPE_OBJECT:
        return "OBJECT";

    case TYPE_QUANTILE_STATE:
        return "QUANTILE_STATE";

    case TYPE_AGG_STATE:
        return "AGG_STATE";

    case TYPE_ARRAY:
        return "ARRAY";

    case TYPE_MAP:
        return "MAP";

    case TYPE_STRUCT:
        return "STRUCT";
    case TYPE_LAMBDA_FUNCTION:
        return "LAMBDA_FUNCTION TYPE";

    default:
        return "";
    };

    return "";
}

std::string type_to_odbc_string(PrimitiveType t) {
    // ODBC driver requires types in lower case
    switch (t) {
    default:
    case INVALID_TYPE:
        return "invalid";

    case TYPE_NULL:
        return "null";

    case TYPE_BOOLEAN:
        return "boolean";

    case TYPE_TINYINT:
        return "tinyint";

    case TYPE_SMALLINT:
        return "smallint";

    case TYPE_INT:
        return "int";

    case TYPE_BIGINT:
        return "bigint";

    case TYPE_LARGEINT:
        return "largeint";

    case TYPE_FLOAT:
        return "float";

    case TYPE_DOUBLE:
        return "double";

    case TYPE_DATE:
        return "date";

    case TYPE_DATETIME:
        return "datetime";

    case TYPE_DATEV2:
        return "datev2";

    case TYPE_DATETIMEV2:
        return "datetimev2";

    case TYPE_TIMEV2:
        return "timev2";

    case TYPE_VARCHAR:
        return "string";

    case TYPE_STRING:
        return "string";

    case TYPE_JSONB:
        return "jsonb";

    case TYPE_BINARY:
        return "binary";

    case TYPE_DECIMALV2:
        return "decimalv2";

    case TYPE_DECIMAL32:
        return "decimal32";

    case TYPE_DECIMAL64:
        return "decimal64";

    case TYPE_DECIMAL128I:
        return "decimal128";

    case TYPE_CHAR:
        return "char";

    case TYPE_HLL:
        return "hll";

    case TYPE_OBJECT:
        return "object";

    case TYPE_QUANTILE_STATE:
        return "quantile_state";

    case TYPE_AGG_STATE:
        return "agg_state";
    };

    return "unknown";
}

// for test only
TTypeDesc gen_type_desc(const TPrimitiveType::type val) {
    std::vector<TTypeNode> types_list;
    TTypeNode type_node;
    TTypeDesc type_desc;
    TScalarType scalar_type;
    scalar_type.__set_type(val);
    type_node.__set_scalar_type(scalar_type);
    types_list.push_back(type_node);
    type_desc.__set_types(types_list);
    return type_desc;
}

// for test only
TTypeDesc gen_type_desc(const TPrimitiveType::type val, const std::string& name) {
    std::vector<TTypeNode> types_list;
    TTypeNode type_node;
    TTypeDesc type_desc;
    TScalarType scalar_type;
    scalar_type.__set_type(val);
    std::vector<TStructField> fields;
    TStructField field;
    field.__set_name(name);
    fields.push_back(field);
    type_node.__set_struct_fields(fields);
    type_node.__set_scalar_type(scalar_type);
    types_list.push_back(type_node);
    type_desc.__set_types(types_list);
    return type_desc;
}

} // namespace doris
