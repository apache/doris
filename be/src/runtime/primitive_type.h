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

#ifndef DORIS_BE_RUNTIME_PRIMITIVE_TYPE_H
#define DORIS_BE_RUNTIME_PRIMITIVE_TYPE_H

#include <string>

#include "common/logging.h"
#include "gen_cpp/Opcodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/datetime_value.h"
#include "runtime/decimal_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/large_int_value.h"
#include "runtime/string_value.h"

namespace doris {

enum PrimitiveType {
    INVALID_TYPE = 0,
    TYPE_NULL,     /* 1 */
    TYPE_BOOLEAN,  /* 2 */
    TYPE_TINYINT,  /* 3 */
    TYPE_SMALLINT, /* 4 */
    TYPE_INT,      /* 5 */
    TYPE_BIGINT,   /* 6 */
    TYPE_LARGEINT, /* 7 */
    TYPE_FLOAT,    /* 8 */
    TYPE_DOUBLE,   /* 9 */
    TYPE_VARCHAR,  /* 10 */
    TYPE_DATE,     /* 11 */
    TYPE_DATETIME, /* 12 */
    TYPE_BINARY,
    /* 13 */      // Not implemented
    TYPE_DECIMAL, /* 14 */
    TYPE_CHAR,    /* 15 */

    TYPE_STRUCT,    /* 16 */
    TYPE_ARRAY,     /* 17 */
    TYPE_MAP,       /* 18 */
    TYPE_HLL,       /* 19 */
    TYPE_DECIMALV2, /* 20 */

    TYPE_TIME, /* 21 */
    TYPE_OBJECT,
};

inline bool is_enumeration_type(PrimitiveType type) {
    switch (type) {
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_NULL:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_DATETIME:
    case TYPE_DECIMAL:
    case TYPE_DECIMALV2:
    case TYPE_BOOLEAN:
    case TYPE_HLL:
        return false;

    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_DATE:
        return true;

    case INVALID_TYPE:
    default:
        DCHECK(false);
    }

    return false;
}

// inline bool is_date_type(PrimitiveType type) {
//     return type == TYPE_DATETIME || type == TYPE_DATE;
// }
//
// inline bool is_string_type(PrimitiveType type) {
//     return type == TYPE_CHAR || type == TYPE_VARCHAR;
// }

// Returns the byte size of 'type'  Returns 0 for variable length types.
inline int get_byte_size(PrimitiveType type) {
    switch (type) {
    case TYPE_OBJECT:
    case TYPE_HLL:
    case TYPE_VARCHAR:
        return 0;

    case TYPE_NULL:
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
        return 1;

    case TYPE_SMALLINT:
        return 2;

    case TYPE_INT:
    case TYPE_FLOAT:
        return 4;

    case TYPE_BIGINT:
    case TYPE_TIME:
    case TYPE_DOUBLE:
        return 8;

    case TYPE_LARGEINT:
    case TYPE_DATETIME:
    case TYPE_DATE:
    case TYPE_DECIMALV2:
        return 16;

    case TYPE_DECIMAL:
        return 40;

    case INVALID_TYPE:
    default:
        DCHECK(false);
    }

    return 0;
}

inline int get_real_byte_size(PrimitiveType type) {
    switch (type) {
    case TYPE_OBJECT:
    case TYPE_HLL:
    case TYPE_VARCHAR:
        return 0;

    case TYPE_NULL:
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
        return 1;

    case TYPE_SMALLINT:
        return 2;

    case TYPE_INT:
    case TYPE_FLOAT:
        return 4;

    case TYPE_BIGINT:
    case TYPE_TIME:
    case TYPE_DOUBLE:
        return 8;

    case TYPE_DATETIME:
    case TYPE_DATE:
    case TYPE_DECIMALV2:
        return 16;

    case TYPE_DECIMAL:
        return 40;

    case TYPE_LARGEINT:
        return 16;

    case INVALID_TYPE:
    default:
        DCHECK(false);
    }

    return 0;
}
// Returns the byte size of type when in a tuple
inline int get_slot_size(PrimitiveType type) {
    switch (type) {
    case TYPE_OBJECT:
    case TYPE_HLL:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return sizeof(StringValue);

    case TYPE_NULL:
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
        return 1;

    case TYPE_SMALLINT:
        return 2;

    case TYPE_INT:
    case TYPE_FLOAT:
        return 4;

    case TYPE_BIGINT:
    case TYPE_DOUBLE:
        return 8;

    case TYPE_LARGEINT:
        return sizeof(__int128);

    case TYPE_DATE:
    case TYPE_DATETIME:
        // This is the size of the slot, the actual size of the data is 12.
        return 16;

    case TYPE_DECIMAL:
        return sizeof(DecimalValue);

    case TYPE_DECIMALV2:
        return 16;

    case INVALID_TYPE:
    default:
        DCHECK(false);
    }

    return 0;
}

inline bool is_type_compatible(PrimitiveType lhs, PrimitiveType rhs) {
    if (lhs == TYPE_VARCHAR) {
        return rhs == TYPE_CHAR || rhs == TYPE_VARCHAR || rhs == TYPE_HLL || rhs == TYPE_OBJECT;
    }

    if (lhs == TYPE_OBJECT) {
        return rhs == TYPE_VARCHAR || rhs == TYPE_OBJECT;
    }

    if (lhs == TYPE_CHAR || lhs == TYPE_HLL) {
        return rhs == TYPE_CHAR || rhs == TYPE_VARCHAR || rhs == TYPE_HLL;
    }

    return lhs == rhs;
}

TExprOpcode::type to_in_opcode(PrimitiveType t);
PrimitiveType thrift_to_type(TPrimitiveType::type ttype);
TPrimitiveType::type to_thrift(PrimitiveType ptype);
TColumnType to_tcolumn_type_thrift(TPrimitiveType::type ttype);
std::string type_to_string(PrimitiveType t);
std::string type_to_odbc_string(PrimitiveType t);
TTypeDesc gen_type_desc(const TPrimitiveType::type val);
TTypeDesc gen_type_desc(const TPrimitiveType::type val, const std::string& name);

} // namespace doris

#endif
