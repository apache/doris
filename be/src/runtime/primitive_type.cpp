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

#include "gen_cpp/Types_types.h"
#include "runtime/collection_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/jsonb_value.h"
#include "runtime/string_value.h"

namespace doris {

PrimitiveType convert_type_to_primitive(FunctionContext::Type type) {
    switch (type) {
    case FunctionContext::Type::INVALID_TYPE:
        return PrimitiveType::INVALID_TYPE;
    case FunctionContext::Type::TYPE_DOUBLE:
        return PrimitiveType::TYPE_DOUBLE;
    case FunctionContext::Type::TYPE_NULL:
        return PrimitiveType::TYPE_NULL;
    case FunctionContext::Type::TYPE_CHAR:
        return PrimitiveType::TYPE_CHAR;
    case FunctionContext::Type::TYPE_VARCHAR:
        return PrimitiveType::TYPE_VARCHAR;
    case FunctionContext::Type::TYPE_STRING:
        return PrimitiveType::TYPE_STRING;
    case FunctionContext::Type::TYPE_DATETIME:
        return PrimitiveType::TYPE_DATETIME;
    case FunctionContext::Type::TYPE_DECIMALV2:
        return PrimitiveType::TYPE_DECIMALV2;
    case FunctionContext::Type::TYPE_DECIMAL32:
        return PrimitiveType::TYPE_DECIMAL32;
    case FunctionContext::Type::TYPE_DECIMAL64:
        return PrimitiveType::TYPE_DECIMAL64;
    case FunctionContext::Type::TYPE_DECIMAL128I:
        return PrimitiveType::TYPE_DECIMAL128I;
    case FunctionContext::Type::TYPE_BOOLEAN:
        return PrimitiveType::TYPE_BOOLEAN;
    case FunctionContext::Type::TYPE_ARRAY:
        return PrimitiveType::TYPE_ARRAY;
    case FunctionContext::Type::TYPE_OBJECT:
        return PrimitiveType::TYPE_OBJECT;
    case FunctionContext::Type::TYPE_HLL:
        return PrimitiveType::TYPE_HLL;
    case FunctionContext::Type::TYPE_QUANTILE_STATE:
        return PrimitiveType::TYPE_QUANTILE_STATE;
    case FunctionContext::Type::TYPE_TINYINT:
        return PrimitiveType::TYPE_TINYINT;
    case FunctionContext::Type::TYPE_SMALLINT:
        return PrimitiveType::TYPE_SMALLINT;
    case FunctionContext::Type::TYPE_INT:
        return PrimitiveType::TYPE_INT;
    case FunctionContext::Type::TYPE_BIGINT:
        return PrimitiveType::TYPE_BIGINT;
    case FunctionContext::Type::TYPE_LARGEINT:
        return PrimitiveType::TYPE_LARGEINT;
    case FunctionContext::Type::TYPE_DATE:
        return PrimitiveType::TYPE_DATE;
    case FunctionContext::Type::TYPE_DATEV2:
        return PrimitiveType::TYPE_DATEV2;
    case FunctionContext::Type::TYPE_DATETIMEV2:
        return PrimitiveType::TYPE_DATETIMEV2;
    case FunctionContext::Type::TYPE_TIMEV2:
        return PrimitiveType::TYPE_TIMEV2;
    case FunctionContext::Type::TYPE_JSONB:
        return PrimitiveType::TYPE_JSONB;
    default:
        DCHECK(false);
    }

    return PrimitiveType::INVALID_TYPE;
}

// Returns the byte size of 'type'  Returns 0 for variable length types.
int get_byte_size(PrimitiveType type) {
    switch (type) {
    case TYPE_VARCHAR:
    case TYPE_STRING:
    case TYPE_OBJECT:
    case TYPE_HLL:
    case TYPE_QUANTILE_STATE:
    case TYPE_ARRAY:
    case TYPE_MAP:
        return 0;

    case TYPE_NULL:
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
        return 1;

    case TYPE_SMALLINT:
        return 2;

    case TYPE_INT:
    case TYPE_FLOAT:
    case TYPE_DECIMAL32:
        return 4;

    case TYPE_BIGINT:
    case TYPE_DOUBLE:
    case TYPE_TIME:
    case TYPE_DECIMAL64:
        return 8;

    case TYPE_DATETIME:
    case TYPE_DATE:
    case TYPE_LARGEINT:
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL128I:
        return 16;

    case INVALID_TYPE:
    // datev2/datetimev2/timev2 is not supported on row-based engine
    case TYPE_DATEV2:
    case TYPE_DATETIMEV2:
    case TYPE_TIMEV2:
    default:
        DCHECK(false);
    }

    return 0;
}

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

//to_tcolumn_type_thrift only test
TColumnType to_tcolumn_type_thrift(TPrimitiveType::type ttype) {
    TColumnType t;
    t.__set_type(ttype);
    return t;
}

TExprOpcode::type to_in_opcode(PrimitiveType t) {
    return TExprOpcode::FILTER_IN;
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

    default:
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

    case TYPE_ARRAY:
        return "ARRAY";

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

int get_slot_size(PrimitiveType type) {
    switch (type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
    case TYPE_OBJECT:
    case TYPE_HLL:
    case TYPE_QUANTILE_STATE:
        return sizeof(StringValue);
    case TYPE_JSONB:
        return sizeof(JsonBinaryValue);
    case TYPE_ARRAY:
        return sizeof(CollectionValue);

    case TYPE_NULL:
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
        return 1;

    case TYPE_SMALLINT:
        return 2;

    case TYPE_INT:
    case TYPE_DATEV2:
    case TYPE_FLOAT:
    case TYPE_DECIMAL32:
        return 4;

    case TYPE_BIGINT:
    case TYPE_DOUBLE:
    case TYPE_TIME:
    case TYPE_DECIMAL64:
    case TYPE_DATETIMEV2:
    case TYPE_TIMEV2:
        return 8;

    case TYPE_LARGEINT:
        return sizeof(__int128);

    case TYPE_DATE:
    case TYPE_DATETIME:
        // This is the size of the slot, the actual size of the data is 12.
        return sizeof(DateTimeValue);

    case TYPE_DECIMALV2:
    case TYPE_DECIMAL128I:
        return 16;

    case INVALID_TYPE:
    default:
        DCHECK(false);
    }

    return 0;
}

} // namespace doris
