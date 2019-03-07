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

#include <string>
#include "olap/field_info.h"

using std::nothrow;
using std::string;

namespace doris {

FieldType FieldInfo::get_field_type_by_string(const string& type_str) {
    string upper_type_str = type_str;
    std::transform(type_str.begin(), type_str.end(), upper_type_str.begin(), toupper);
    FieldType type;

    if (0 == upper_type_str.compare("TINYINT")) {
        type = OLAP_FIELD_TYPE_TINYINT;
    } else if (0 == upper_type_str.compare("SMALLINT")) {
        type = OLAP_FIELD_TYPE_SMALLINT;
    } else if (0 == upper_type_str.compare("INT")) {
        type = OLAP_FIELD_TYPE_INT;
    } else if (0 == upper_type_str.compare("BIGINT")) {
        type = OLAP_FIELD_TYPE_BIGINT;
    } else if (0 == upper_type_str.compare("LARGEINT")) {
        type = OLAP_FIELD_TYPE_LARGEINT;
    } else if (0 == upper_type_str.compare("UNSIGNED_TINYINT")) {
        type = OLAP_FIELD_TYPE_UNSIGNED_TINYINT;
    } else if (0 == upper_type_str.compare("UNSIGNED_SMALLINT")) {
        type = OLAP_FIELD_TYPE_UNSIGNED_SMALLINT;
    } else if (0 == upper_type_str.compare("UNSIGNED_INT")) {
        type = OLAP_FIELD_TYPE_UNSIGNED_INT;
    } else if (0 == upper_type_str.compare("UNSIGNED_BIGINT")) {
        type = OLAP_FIELD_TYPE_UNSIGNED_BIGINT;
    } else if (0 == upper_type_str.compare("FLOAT")) {
        type = OLAP_FIELD_TYPE_FLOAT;
    } else if (0 == upper_type_str.compare("DISCRETE_DOUBLE")) {
        type = OLAP_FIELD_TYPE_DISCRETE_DOUBLE;
    } else if (0 == upper_type_str.compare("DOUBLE")) {
        type = OLAP_FIELD_TYPE_DOUBLE;
    } else if (0 == upper_type_str.compare("CHAR")) {
        type = OLAP_FIELD_TYPE_CHAR;
    } else if (0 == upper_type_str.compare("DATE")) {
        type = OLAP_FIELD_TYPE_DATE;
    } else if (0 == upper_type_str.compare("DATETIME")) {
        type = OLAP_FIELD_TYPE_DATETIME;
    } else if (0 == upper_type_str.compare(0, 7, "DECIMAL")) {
        type = OLAP_FIELD_TYPE_DECIMAL;
    } else if (0 == upper_type_str.compare(0, 7, "VARCHAR")) {
        type = OLAP_FIELD_TYPE_VARCHAR;
    } else if (0 == upper_type_str.compare(0, 3, "HLL")) {
        type = OLAP_FIELD_TYPE_HLL;
    } else if (0 == upper_type_str.compare("STRUCT")) {
        type = OLAP_FIELD_TYPE_STRUCT;
    } else if (0 == upper_type_str.compare("LIST")) {
        type = OLAP_FIELD_TYPE_LIST;
    } else if (0 == upper_type_str.compare("MAP")) {
        type = OLAP_FIELD_TYPE_MAP;
    } else {
        LOG(WARNING) << "invalid type string. [type='" << type_str << "']";
        type = OLAP_FIELD_TYPE_UNKNOWN;
    }

    return type;
}

FieldAggregationMethod FieldInfo::get_aggregation_type_by_string(const string& str) {
    string upper_str = str;
    std::transform(str.begin(), str.end(), upper_str.begin(), toupper);
    FieldAggregationMethod aggregation_type;

    if (0 == upper_str.compare("NONE")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_NONE;
    } else if (0 == upper_str.compare("SUM")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_SUM;
    } else if (0 == upper_str.compare("MIN")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_MIN;
    } else if (0 == upper_str.compare("MAX")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_MAX;
    } else if (0 == upper_str.compare("REPLACE")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_REPLACE;
    } else if (0 == upper_str.compare("HLL_UNION")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_HLL_UNION;
    } else {
        LOG(WARNING) << "invalid aggregation type string. [aggregation='" << str << "']";
        aggregation_type = OLAP_FIELD_AGGREGATION_UNKNOWN;
    }

    return aggregation_type;
}

string FieldInfo::get_string_by_field_type(FieldType type) {
    switch (type) {
        case OLAP_FIELD_TYPE_TINYINT:
            return "TINYINT";

        case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
            return "UNSIGNED_TINYINT";

        case OLAP_FIELD_TYPE_SMALLINT:
            return "SMALLINT";

        case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
            return "UNSIGNED_SMALLINT";

        case OLAP_FIELD_TYPE_INT:
            return "INT";

        case OLAP_FIELD_TYPE_UNSIGNED_INT:
            return "UNSIGNED_INT";

        case OLAP_FIELD_TYPE_BIGINT:
            return "BIGINT";

        case OLAP_FIELD_TYPE_LARGEINT:
            return "LARGEINT";

        case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
            return "UNSIGNED_BIGINT";

        case OLAP_FIELD_TYPE_FLOAT:
            return "FLOAT";

        case OLAP_FIELD_TYPE_DOUBLE:
            return "DOUBLE";

        case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
            return "DISCRETE_DOUBLE";

        case OLAP_FIELD_TYPE_CHAR:
            return "CHAR";

        case OLAP_FIELD_TYPE_DATE:
            return "DATE";

        case OLAP_FIELD_TYPE_DATETIME:
            return "DATETIME";

        case OLAP_FIELD_TYPE_DECIMAL:
            return "DECIMAL";

        case OLAP_FIELD_TYPE_VARCHAR:
            return "VARCHAR";

        case OLAP_FIELD_TYPE_HLL:
            return "HLL";

        case OLAP_FIELD_TYPE_STRUCT:
            return "STRUCT";

        case OLAP_FIELD_TYPE_LIST:
            return "LIST";

        case OLAP_FIELD_TYPE_MAP:
            return "MAP";

        default:
            return "UNKNOWN";
    }
}

string FieldInfo::get_string_by_aggregation_type(FieldAggregationMethod type) {
    switch (type) {
        case OLAP_FIELD_AGGREGATION_NONE:
            return "NONE";

        case OLAP_FIELD_AGGREGATION_SUM:
            return "SUM";

        case OLAP_FIELD_AGGREGATION_MIN:
            return "MIN";

        case OLAP_FIELD_AGGREGATION_MAX:
            return "MAX";

        case OLAP_FIELD_AGGREGATION_REPLACE:
            return "REPLACE";

        case OLAP_FIELD_AGGREGATION_HLL_UNION:
            return "HLL_UNION";

        default:
            return "UNKNOWN";
    }
}

uint32_t FieldInfo::get_field_length_by_type(TPrimitiveType::type type, uint32_t string_length) {
    switch (type) {
        case TPrimitiveType::TINYINT:
            return 1;
        case TPrimitiveType::SMALLINT:
            return 2;
        case TPrimitiveType::INT:
            return 4;
        case TPrimitiveType::BIGINT:
            return 8;
        case TPrimitiveType::LARGEINT:
            return 16;
        case TPrimitiveType::DATE:
            return 3;
        case TPrimitiveType::DATETIME:
            return 8;
        case TPrimitiveType::FLOAT:
            return 4;
        case TPrimitiveType::DOUBLE:
            return 8;
        case TPrimitiveType::CHAR:
            return string_length;
        case TPrimitiveType::VARCHAR:
        case TPrimitiveType::HLL:
            return string_length + sizeof(OLAP_STRING_MAX_LENGTH);
        case TPrimitiveType::DECIMAL:    
        case TPrimitiveType::DECIMALV2:    
            return 12; // use 12 bytes in olap engine.
        default:
            OLAP_LOG_WARNING("unknown field type. [type=%d]", type);
            return 0;
    }
}

}  // namespace doris
