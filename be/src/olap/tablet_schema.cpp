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

#include "olap/tablet_schema.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <algorithm>
#include <cctype>
// IWYU pragma: no_include <bits/std_abs.h>
#include <cmath> // IWYU pragma: keep
#include <memory>
#include <ostream>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/consts.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "olap/inverted_index_parser.h"
#include "olap/olap_define.h"
#include "olap/types.h"
#include "olap/utils.h"
#include "runtime/thread_context.h"
#include "tablet_meta.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/aggregate_function_state_union.h"
#include "vec/common/hex.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/json/path_in_data.h"

namespace doris {

static bvar::Adder<size_t> g_total_tablet_schema_num("doris_total_tablet_schema_num");

FieldType TabletColumn::get_field_type_by_type(PrimitiveType primitiveType) {
    switch (primitiveType) {
    case PrimitiveType::INVALID_TYPE:
        return FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    case PrimitiveType::TYPE_NULL:
        return FieldType::OLAP_FIELD_TYPE_NONE;
    case PrimitiveType::TYPE_BOOLEAN:
        return FieldType::OLAP_FIELD_TYPE_BOOL;
    case PrimitiveType::TYPE_TINYINT:
        return FieldType::OLAP_FIELD_TYPE_TINYINT;
    case PrimitiveType::TYPE_SMALLINT:
        return FieldType::OLAP_FIELD_TYPE_SMALLINT;
    case PrimitiveType::TYPE_INT:
        return FieldType::OLAP_FIELD_TYPE_INT;
    case PrimitiveType::TYPE_BIGINT:
        return FieldType::OLAP_FIELD_TYPE_BIGINT;
    case PrimitiveType::TYPE_LARGEINT:
        return FieldType::OLAP_FIELD_TYPE_LARGEINT;
    case PrimitiveType::TYPE_FLOAT:
        return FieldType::OLAP_FIELD_TYPE_FLOAT;
    case PrimitiveType::TYPE_DOUBLE:
        return FieldType::OLAP_FIELD_TYPE_DOUBLE;
    case PrimitiveType::TYPE_VARCHAR:
        return FieldType::OLAP_FIELD_TYPE_VARCHAR;
    case PrimitiveType::TYPE_DATE:
        return FieldType::OLAP_FIELD_TYPE_DATE;
    case PrimitiveType::TYPE_DATETIME:
        return FieldType::OLAP_FIELD_TYPE_DATETIME;
    case PrimitiveType::TYPE_BINARY:
        return FieldType::OLAP_FIELD_TYPE_UNKNOWN; // Not implemented
    case PrimitiveType::TYPE_CHAR:
        return FieldType::OLAP_FIELD_TYPE_CHAR;
    case PrimitiveType::TYPE_STRUCT:
        return FieldType::OLAP_FIELD_TYPE_STRUCT;
    case PrimitiveType::TYPE_ARRAY:
        return FieldType::OLAP_FIELD_TYPE_ARRAY;
    case PrimitiveType::TYPE_MAP:
        return FieldType::OLAP_FIELD_TYPE_MAP;
    case PrimitiveType::TYPE_HLL:
        return FieldType::OLAP_FIELD_TYPE_HLL;
    case PrimitiveType::TYPE_DECIMALV2:
        return FieldType::OLAP_FIELD_TYPE_UNKNOWN; // Not implemented
    case PrimitiveType::TYPE_TIME:
        return FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    case PrimitiveType::TYPE_OBJECT:
        return FieldType::OLAP_FIELD_TYPE_OBJECT;
    case PrimitiveType::TYPE_STRING:
        return FieldType::OLAP_FIELD_TYPE_STRING;
    case PrimitiveType::TYPE_QUANTILE_STATE:
        return FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE;
    case PrimitiveType::TYPE_DATEV2:
        return FieldType::OLAP_FIELD_TYPE_DATEV2;
    case PrimitiveType::TYPE_DATETIMEV2:
        return FieldType::OLAP_FIELD_TYPE_DATETIMEV2;
    case PrimitiveType::TYPE_TIMEV2:
        return FieldType::OLAP_FIELD_TYPE_TIMEV2;
    case PrimitiveType::TYPE_DECIMAL32:
        return FieldType::OLAP_FIELD_TYPE_DECIMAL32;
    case PrimitiveType::TYPE_DECIMAL64:
        return FieldType::OLAP_FIELD_TYPE_DECIMAL64;
    case PrimitiveType::TYPE_DECIMAL128I:
        return FieldType::OLAP_FIELD_TYPE_DECIMAL128I;
    case PrimitiveType::TYPE_JSONB:
        return FieldType::OLAP_FIELD_TYPE_JSONB;
    case PrimitiveType::TYPE_VARIANT:
        return FieldType::OLAP_FIELD_TYPE_VARIANT;
    case PrimitiveType::TYPE_LAMBDA_FUNCTION:
        return FieldType::OLAP_FIELD_TYPE_UNKNOWN; // Not implemented
    case PrimitiveType::TYPE_AGG_STATE:
        return FieldType::OLAP_FIELD_TYPE_AGG_STATE;
    default:
        return FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    }
}

FieldType TabletColumn::get_field_type_by_string(const std::string& type_str) {
    std::string upper_type_str = type_str;
    std::transform(type_str.begin(), type_str.end(), upper_type_str.begin(),
                   [](auto c) { return std::toupper(c); });
    FieldType type;

    if (0 == upper_type_str.compare("TINYINT")) {
        type = FieldType::OLAP_FIELD_TYPE_TINYINT;
    } else if (0 == upper_type_str.compare("SMALLINT")) {
        type = FieldType::OLAP_FIELD_TYPE_SMALLINT;
    } else if (0 == upper_type_str.compare("INT")) {
        type = FieldType::OLAP_FIELD_TYPE_INT;
    } else if (0 == upper_type_str.compare("BIGINT")) {
        type = FieldType::OLAP_FIELD_TYPE_BIGINT;
    } else if (0 == upper_type_str.compare("LARGEINT")) {
        type = FieldType::OLAP_FIELD_TYPE_LARGEINT;
    } else if (0 == upper_type_str.compare("UNSIGNED_TINYINT")) {
        type = FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT;
    } else if (0 == upper_type_str.compare("UNSIGNED_SMALLINT")) {
        type = FieldType::OLAP_FIELD_TYPE_UNSIGNED_SMALLINT;
    } else if (0 == upper_type_str.compare("UNSIGNED_INT")) {
        type = FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT;
    } else if (0 == upper_type_str.compare("UNSIGNED_BIGINT")) {
        type = FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT;
    } else if (0 == upper_type_str.compare("IPV4")) {
        type = FieldType::OLAP_FIELD_TYPE_IPV4;
    } else if (0 == upper_type_str.compare("IPV6")) {
        type = FieldType::OLAP_FIELD_TYPE_IPV6;
    } else if (0 == upper_type_str.compare("FLOAT")) {
        type = FieldType::OLAP_FIELD_TYPE_FLOAT;
    } else if (0 == upper_type_str.compare("DISCRETE_DOUBLE")) {
        type = FieldType::OLAP_FIELD_TYPE_DISCRETE_DOUBLE;
    } else if (0 == upper_type_str.compare("DOUBLE")) {
        type = FieldType::OLAP_FIELD_TYPE_DOUBLE;
    } else if (0 == upper_type_str.compare("CHAR")) {
        type = FieldType::OLAP_FIELD_TYPE_CHAR;
    } else if (0 == upper_type_str.compare("DATE")) {
        type = FieldType::OLAP_FIELD_TYPE_DATE;
    } else if (0 == upper_type_str.compare("DATEV2")) {
        type = FieldType::OLAP_FIELD_TYPE_DATEV2;
    } else if (0 == upper_type_str.compare("DATETIMEV2")) {
        type = FieldType::OLAP_FIELD_TYPE_DATETIMEV2;
    } else if (0 == upper_type_str.compare("DATETIME")) {
        type = FieldType::OLAP_FIELD_TYPE_DATETIME;
    } else if (0 == upper_type_str.compare("DECIMAL32")) {
        type = FieldType::OLAP_FIELD_TYPE_DECIMAL32;
    } else if (0 == upper_type_str.compare("DECIMAL64")) {
        type = FieldType::OLAP_FIELD_TYPE_DECIMAL64;
    } else if (0 == upper_type_str.compare("DECIMAL128I")) {
        type = FieldType::OLAP_FIELD_TYPE_DECIMAL128I;
    } else if (0 == upper_type_str.compare("DECIMAL256")) {
        type = FieldType::OLAP_FIELD_TYPE_DECIMAL256;
    } else if (0 == upper_type_str.compare(0, 7, "DECIMAL")) {
        type = FieldType::OLAP_FIELD_TYPE_DECIMAL;
    } else if (0 == upper_type_str.compare(0, 7, "VARCHAR")) {
        type = FieldType::OLAP_FIELD_TYPE_VARCHAR;
    } else if (0 == upper_type_str.compare("STRING")) {
        type = FieldType::OLAP_FIELD_TYPE_STRING;
    } else if (0 == upper_type_str.compare("JSONB")) {
        type = FieldType::OLAP_FIELD_TYPE_JSONB;
    } else if (0 == upper_type_str.compare("VARIANT")) {
        type = FieldType::OLAP_FIELD_TYPE_VARIANT;
    } else if (0 == upper_type_str.compare("BOOLEAN")) {
        type = FieldType::OLAP_FIELD_TYPE_BOOL;
    } else if (0 == upper_type_str.compare(0, 3, "HLL")) {
        type = FieldType::OLAP_FIELD_TYPE_HLL;
    } else if (0 == upper_type_str.compare("STRUCT")) {
        type = FieldType::OLAP_FIELD_TYPE_STRUCT;
    } else if (0 == upper_type_str.compare("LIST")) {
        type = FieldType::OLAP_FIELD_TYPE_ARRAY;
    } else if (0 == upper_type_str.compare("MAP")) {
        type = FieldType::OLAP_FIELD_TYPE_MAP;
    } else if (0 == upper_type_str.compare("OBJECT")) {
        type = FieldType::OLAP_FIELD_TYPE_OBJECT;
    } else if (0 == upper_type_str.compare("ARRAY")) {
        type = FieldType::OLAP_FIELD_TYPE_ARRAY;
    } else if (0 == upper_type_str.compare("QUANTILE_STATE")) {
        type = FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE;
    } else if (0 == upper_type_str.compare("AGG_STATE")) {
        type = FieldType::OLAP_FIELD_TYPE_AGG_STATE;
    } else {
        LOG(WARNING) << "invalid type string. [type='" << type_str << "']";
        type = FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    }

    return type;
}

FieldAggregationMethod TabletColumn::get_aggregation_type_by_string(const std::string& str) {
    std::string upper_str = str;
    std::transform(str.begin(), str.end(), upper_str.begin(),
                   [](auto c) { return std::toupper(c); });
    FieldAggregationMethod aggregation_type;

    if (0 == upper_str.compare("NONE")) {
        aggregation_type = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE;
    } else if (0 == upper_str.compare("SUM")) {
        aggregation_type = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM;
    } else if (0 == upper_str.compare("MIN")) {
        aggregation_type = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MIN;
    } else if (0 == upper_str.compare("MAX")) {
        aggregation_type = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MAX;
    } else if (0 == upper_str.compare("REPLACE")) {
        aggregation_type = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE;
    } else if (0 == upper_str.compare("REPLACE_IF_NOT_NULL")) {
        aggregation_type = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL;
    } else if (0 == upper_str.compare("HLL_UNION")) {
        aggregation_type = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_HLL_UNION;
    } else if (0 == upper_str.compare("BITMAP_UNION")) {
        aggregation_type = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_BITMAP_UNION;
    } else if (0 == upper_str.compare("QUANTILE_UNION")) {
        aggregation_type = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_QUANTILE_UNION;
    } else if (!upper_str.empty()) {
        aggregation_type = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_GENERIC;
    } else {
        aggregation_type = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_UNKNOWN;
    }

    return aggregation_type;
}

std::string TabletColumn::get_string_by_field_type(FieldType type) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return "TINYINT";

    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        return "UNSIGNED_TINYINT";

    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return "SMALLINT";

    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        return "UNSIGNED_SMALLINT";

    case FieldType::OLAP_FIELD_TYPE_INT:
        return "INT";

    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT:
        return "UNSIGNED_INT";

    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        return "BIGINT";

    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        return "LARGEINT";

    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        return "UNSIGNED_BIGINT";

    case FieldType::OLAP_FIELD_TYPE_IPV4:
        return "IPV4";

    case FieldType::OLAP_FIELD_TYPE_IPV6:
        return "IPV6";

    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        return "FLOAT";

    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        return "DOUBLE";

    case FieldType::OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
        return "DISCRETE_DOUBLE";

    case FieldType::OLAP_FIELD_TYPE_CHAR:
        return "CHAR";

    case FieldType::OLAP_FIELD_TYPE_DATE:
        return "DATE";

    case FieldType::OLAP_FIELD_TYPE_DATEV2:
        return "DATEV2";

    case FieldType::OLAP_FIELD_TYPE_DATETIME:
        return "DATETIME";

    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        return "DATETIMEV2";

    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        return "DECIMAL";

    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        return "DECIMAL32";

    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        return "DECIMAL64";

    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
        return "DECIMAL128I";

    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        return "DECIMAL256";

    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
        return "VARCHAR";

    case FieldType::OLAP_FIELD_TYPE_JSONB:
        return "JSONB";

    case FieldType::OLAP_FIELD_TYPE_VARIANT:
        return "VARIANT";

    case FieldType::OLAP_FIELD_TYPE_STRING:
        return "STRING";

    case FieldType::OLAP_FIELD_TYPE_BOOL:
        return "BOOLEAN";

    case FieldType::OLAP_FIELD_TYPE_HLL:
        return "HLL";

    case FieldType::OLAP_FIELD_TYPE_STRUCT:
        return "STRUCT";

    case FieldType::OLAP_FIELD_TYPE_ARRAY:
        return "ARRAY";

    case FieldType::OLAP_FIELD_TYPE_MAP:
        return "MAP";

    case FieldType::OLAP_FIELD_TYPE_OBJECT:
        return "OBJECT";
    case FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE:
        return "QUANTILE_STATE";
    case FieldType::OLAP_FIELD_TYPE_AGG_STATE:
        return "AGG_STATE";
    default:
        return "UNKNOWN";
    }
}

std::string TabletColumn::get_string_by_aggregation_type(FieldAggregationMethod type) {
    switch (type) {
    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE:
        return "NONE";

    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM:
        return "SUM";

    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MIN:
        return "MIN";

    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MAX:
        return "MAX";

    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE:
        return "REPLACE";

    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL:
        return "REPLACE_IF_NOT_NULL";

    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_HLL_UNION:
        return "HLL_UNION";

    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_BITMAP_UNION:
        return "BITMAP_UNION";

    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_QUANTILE_UNION:
        return "QUANTILE_UNION";

    default:
        return "UNKNOWN";
    }
}

uint32_t TabletColumn::get_field_length_by_type(TPrimitiveType::type type, uint32_t string_length) {
    switch (type) {
    case TPrimitiveType::TINYINT:
    case TPrimitiveType::BOOLEAN:
        return 1;
    case TPrimitiveType::SMALLINT:
        return 2;
    case TPrimitiveType::INT:
        return 4;
    case TPrimitiveType::BIGINT:
        return 8;
    case TPrimitiveType::LARGEINT:
        return 16;
    case TPrimitiveType::IPV4:
        return 4;
    case TPrimitiveType::IPV6:
        return 16;
    case TPrimitiveType::DATE:
        return 3;
    case TPrimitiveType::DATEV2:
        return 4;
    case TPrimitiveType::DATETIME:
        return 8;
    case TPrimitiveType::DATETIMEV2:
        return 8;
    case TPrimitiveType::FLOAT:
        return 4;
    case TPrimitiveType::DOUBLE:
        return 8;
    case TPrimitiveType::QUANTILE_STATE:
    case TPrimitiveType::OBJECT:
        return 16;
    case TPrimitiveType::CHAR:
        return string_length;
    case TPrimitiveType::VARCHAR:
    case TPrimitiveType::HLL:
    case TPrimitiveType::AGG_STATE:
        return string_length + sizeof(OLAP_VARCHAR_MAX_LENGTH);
    case TPrimitiveType::STRING:
    case TPrimitiveType::VARIANT:
        return string_length + sizeof(OLAP_STRING_MAX_LENGTH);
    case TPrimitiveType::JSONB:
        return string_length + sizeof(OLAP_JSONB_MAX_LENGTH);
    case TPrimitiveType::STRUCT:
        // Note that(xy): this is the length of struct type itself,
        // the length of its subtypes are not included.
        return OLAP_STRUCT_MAX_LENGTH;
    case TPrimitiveType::ARRAY:
        return OLAP_ARRAY_MAX_LENGTH;
    case TPrimitiveType::MAP:
        return OLAP_MAP_MAX_LENGTH;
    case TPrimitiveType::DECIMAL32:
        return 4;
    case TPrimitiveType::DECIMAL64:
        return 8;
    case TPrimitiveType::DECIMAL128I:
        return 16;
    case TPrimitiveType::DECIMAL256:
        return 32;
    case TPrimitiveType::DECIMALV2:
        return 12; // use 12 bytes in olap engine.
    default:
        LOG(WARNING) << "unknown field type. [type=" << type << "]";
        return 0;
    }
}

TabletColumn::TabletColumn() : _aggregation(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE) {}

TabletColumn::TabletColumn(FieldAggregationMethod agg, FieldType type) {
    _aggregation = agg;
    _type = type;
}

TabletColumn::TabletColumn(FieldAggregationMethod agg, FieldType filed_type, bool is_nullable) {
    _aggregation = agg;
    _type = filed_type;
    _length = get_scalar_type_info(filed_type)->size();
    _is_nullable = is_nullable;
}

TabletColumn::TabletColumn(FieldAggregationMethod agg, FieldType filed_type, bool is_nullable,
                           int32_t unique_id, size_t length) {
    _aggregation = agg;
    _type = filed_type;
    _is_nullable = is_nullable;
    _unique_id = unique_id;
    _length = length;
}

TabletColumn::TabletColumn(const ColumnPB& column) {
    init_from_pb(column);
}

TabletColumn::TabletColumn(const TColumn& column) {
    init_from_thrift(column);
}

void TabletColumn::init_from_thrift(const TColumn& tcolumn) {
    ColumnPB column_pb;
    TabletMeta::init_column_from_tcolumn(tcolumn.col_unique_id, tcolumn, &column_pb);
    init_from_pb(column_pb);
}

void TabletColumn::init_from_pb(const ColumnPB& column) {
    _unique_id = column.unique_id();
    _col_name = column.name();
    _col_name_lower_case = to_lower(_col_name);
    _type = TabletColumn::get_field_type_by_string(column.type());
    _is_key = column.is_key();
    _is_nullable = column.is_nullable();
    _is_auto_increment = column.is_auto_increment();

    _has_default_value = column.has_default_value();
    if (_has_default_value) {
        _default_value = column.default_value();
    }

    if (column.has_precision()) {
        _is_decimal = true;
        _precision = column.precision();
    } else {
        _is_decimal = false;
    }
    if (column.has_frac()) {
        _frac = column.frac();
    }
    _length = column.length();
    _index_length = column.index_length();
    if (column.has_is_bf_column()) {
        _is_bf_column = column.is_bf_column();
    } else {
        _is_bf_column = false;
    }
    if (column.has_has_bitmap_index()) {
        _has_bitmap_index = column.has_bitmap_index();
    } else {
        _has_bitmap_index = false;
    }
    if (column.has_aggregation()) {
        _aggregation = get_aggregation_type_by_string(column.aggregation());
        _aggregation_name = column.aggregation();
    }
    if (column.has_result_is_nullable()) {
        _result_is_nullable = column.result_is_nullable();
    }
    if (column.has_visible()) {
        _visible = column.visible();
    }
    if (_type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        CHECK(column.children_columns_size() == 1) << "ARRAY type has more than 1 children types.";
    }
    if (_type == FieldType::OLAP_FIELD_TYPE_MAP) {
        CHECK(column.children_columns_size() == 2) << "MAP type has more than 2 children types.";
    }
    for (size_t i = 0; i < column.children_columns_size(); i++) {
        TabletColumn child_column;
        child_column.init_from_pb(column.children_columns(i));
        add_sub_column(child_column);
    }
    if (column.has_column_path_info()) {
        _column_path = std::make_shared<vectorized::PathInData>();
        _column_path->from_protobuf(column.column_path_info());
        _parent_col_unique_id = column.column_path_info().parrent_column_unique_id();
    }
    for (auto& column_pb : column.sparse_columns()) {
        TabletColumn column;
        column.init_from_pb(column_pb);
        _sparse_cols.emplace_back(std::make_shared<TabletColumn>(std::move(column)));
        _num_sparse_columns++;
    }
}

TabletColumn TabletColumn::create_materialized_variant_column(const std::string& root,
                                                              const std::vector<std::string>& paths,
                                                              int32_t parent_unique_id) {
    TabletColumn subcol;
    subcol.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    subcol.set_is_nullable(true);
    subcol.set_unique_id(-1);
    subcol.set_parent_unique_id(parent_unique_id);
    vectorized::PathInData path(root, paths);
    subcol.set_path_info(path);
    subcol.set_name(path.get_path());
    return subcol;
}

void TabletColumn::to_schema_pb(ColumnPB* column) const {
    column->set_unique_id(_unique_id);
    column->set_name(_col_name);
    column->set_type(get_string_by_field_type(_type));
    column->set_is_key(_is_key);
    column->set_is_nullable(_is_nullable);
    if (_has_default_value) {
        column->set_default_value(_default_value);
    }
    if (_is_decimal) {
        column->set_precision(_precision);
        column->set_frac(_frac);
    }
    column->set_length(_length);
    column->set_index_length(_index_length);
    if (_is_bf_column) {
        column->set_is_bf_column(_is_bf_column);
    }
    if (!_aggregation_name.empty()) {
        column->set_aggregation(_aggregation_name);
    }
    column->set_result_is_nullable(_result_is_nullable);
    if (_has_bitmap_index) {
        column->set_has_bitmap_index(_has_bitmap_index);
    }
    column->set_visible(_visible);

    if (_type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        CHECK(_sub_columns.size() == 1) << "ARRAY type has more than 1 children types.";
    }
    if (_type == FieldType::OLAP_FIELD_TYPE_MAP) {
        CHECK(_sub_columns.size() == 2) << "MAP type has more than 2 children types.";
    }

    for (size_t i = 0; i < _sub_columns.size(); i++) {
        ColumnPB* child = column->add_children_columns();
        _sub_columns[i]->to_schema_pb(child);
    }

    // set parts info
    if (has_path_info()) {
        // CHECK_GT(_parent_col_unique_id, 0);
        _column_path->to_protobuf(column->mutable_column_path_info(), _parent_col_unique_id);
        // Update unstable information for variant columns. Some of the fields in the tablet schema
        // are irrelevant for variant sub-columns, but retaining them may lead to an excessive growth
        // in the number of tablet schema cache entries.
        if (_type == FieldType::OLAP_FIELD_TYPE_STRING) {
            column->set_length(INT_MAX);
        }
        column->set_index_length(0);
    }
    for (auto& col : _sparse_cols) {
        ColumnPB* sparse_column = column->add_sparse_columns();
        col->to_schema_pb(sparse_column);
    }
}

void TabletColumn::add_sub_column(TabletColumn& sub_column) {
    _sub_columns.push_back(std::make_shared<TabletColumn>(sub_column));
    sub_column._parent_col_unique_id = this->_unique_id;
    _sub_column_count += 1;
}

bool TabletColumn::is_row_store_column() const {
    return _col_name == BeConsts::ROW_STORE_COL;
}

vectorized::AggregateFunctionPtr TabletColumn::get_aggregate_function_union(
        vectorized::DataTypePtr type) const {
    auto state_type = assert_cast<const vectorized::DataTypeAggState*>(type.get());
    return vectorized::AggregateStateUnion::create(state_type->get_nested_function(), {type}, type);
}

vectorized::AggregateFunctionPtr TabletColumn::get_aggregate_function(std::string suffix) const {
    auto type = vectorized::DataTypeFactory::instance().create_data_type(*this);
    if (type && type->get_type_as_type_descriptor().type == PrimitiveType::TYPE_AGG_STATE) {
        return get_aggregate_function_union(type);
    }

    std::string origin_name = TabletColumn::get_string_by_aggregation_type(_aggregation);
    std::string agg_name = origin_name + suffix;
    std::transform(agg_name.begin(), agg_name.end(), agg_name.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    auto function = vectorized::AggregateFunctionSimpleFactory::instance().get(agg_name, {type},
                                                                               type->is_nullable());
    if (function) {
        return function;
    }
    LOG(WARNING) << "get column aggregate function failed, aggregation_name=" << origin_name
                 << ", column_type=" << type->get_name();
    return nullptr;
}

void TabletColumn::set_path_info(const vectorized::PathInData& path) {
    _column_path = std::make_shared<vectorized::PathInData>(path);
}

vectorized::DataTypePtr TabletColumn::get_vec_type() const {
    return vectorized::DataTypeFactory::instance().create_data_type(*this);
}

// escape '.' and '_'
std::string escape_for_path_name(const std::string& s) {
    std::string res;
    const char* pos = s.data();
    const char* end = pos + s.size();
    while (pos != end) {
        unsigned char c = *pos;
        if (c == '.' || c == '_') {
            res += '%';
            res += vectorized::hex_digit_uppercase(c / 16);
            res += vectorized::hex_digit_uppercase(c % 16);
        } else {
            res += c;
        }
        ++pos;
    }
    return res;
}

void TabletIndex::set_escaped_escaped_index_suffix_path(const std::string& path_name) {
    std::string escaped_path = escape_for_path_name(path_name);
    _escaped_index_suffix_path = escaped_path;
}

void TabletIndex::init_from_thrift(const TOlapTableIndex& index,
                                   const TabletSchema& tablet_schema) {
    _index_id = index.index_id;
    _index_name = index.index_name;
    // init col_unique_id in index at be side, since col_unique_id may be -1 at fe side
    // get column unique id by name
    std::vector<int32_t> col_unique_ids(index.columns.size());
    for (size_t i = 0; i < index.columns.size(); i++) {
        auto column_idx = tablet_schema.field_index(index.columns[i]);
        if (column_idx >= 0) {
            col_unique_ids[i] = tablet_schema.column(column_idx).unique_id();
        } else {
            col_unique_ids[i] = -1;
        }
    }
    _col_unique_ids = std::move(col_unique_ids);

    switch (index.index_type) {
    case TIndexType::BITMAP:
        _index_type = IndexType::BITMAP;
        break;
    case TIndexType::INVERTED:
        _index_type = IndexType::INVERTED;
        break;
    case TIndexType::BLOOMFILTER:
        _index_type = IndexType::BLOOMFILTER;
        break;
    case TIndexType::NGRAM_BF:
        _index_type = IndexType::NGRAM_BF;
        break;
    }
    if (index.__isset.properties) {
        for (auto kv : index.properties) {
            _properties[kv.first] = kv.second;
        }
    }
}

void TabletIndex::init_from_thrift(const TOlapTableIndex& index,
                                   const std::vector<int32_t>& column_uids) {
    _index_id = index.index_id;
    _index_name = index.index_name;
    _col_unique_ids = column_uids;

    switch (index.index_type) {
    case TIndexType::BITMAP:
        _index_type = IndexType::BITMAP;
        break;
    case TIndexType::INVERTED:
        _index_type = IndexType::INVERTED;
        break;
    case TIndexType::BLOOMFILTER:
        _index_type = IndexType::BLOOMFILTER;
        break;
    case TIndexType::NGRAM_BF:
        _index_type = IndexType::NGRAM_BF;
        break;
    }
    if (index.__isset.properties) {
        for (auto kv : index.properties) {
            _properties[kv.first] = kv.second;
        }
    }
}

void TabletIndex::init_from_pb(const TabletIndexPB& index) {
    _index_id = index.index_id();
    _index_name = index.index_name();
    _col_unique_ids.clear();
    for (auto col_unique_id : index.col_unique_id()) {
        _col_unique_ids.push_back(col_unique_id);
    }
    _index_type = index.index_type();
    for (auto& kv : index.properties()) {
        _properties[kv.first] = kv.second;
    }
    _escaped_index_suffix_path = index.index_suffix_name();
}

void TabletIndex::to_schema_pb(TabletIndexPB* index) const {
    index->set_index_id(_index_id);
    index->set_index_name(_index_name);
    index->clear_col_unique_id();
    for (auto col_unique_id : _col_unique_ids) {
        index->add_col_unique_id(col_unique_id);
    }
    index->set_index_type(_index_type);
    for (const auto& kv : _properties) {
        DBUG_EXECUTE_IF("tablet_schema.to_schema_pb", {
            if (kv.first == INVERTED_INDEX_PARSER_LOWERCASE_KEY) {
                continue;
            }
        })
        (*index->mutable_properties())[kv.first] = kv.second;
    }
    index->set_index_suffix_name(_escaped_index_suffix_path);

    DBUG_EXECUTE_IF("tablet_schema.to_schema_pb", { return; })

    // lowercase by default
    if (!_properties.empty()) {
        if (!_properties.contains(INVERTED_INDEX_PARSER_LOWERCASE_KEY)) {
            (*index->mutable_properties())[INVERTED_INDEX_PARSER_LOWERCASE_KEY] =
                    INVERTED_INDEX_PARSER_TRUE;
        }
    }
}

TabletSchema::TabletSchema() {
    g_total_tablet_schema_num << 1;
}

TabletSchema::~TabletSchema() {
    g_total_tablet_schema_num << -1;
}

void TabletSchema::append_column(TabletColumn column, ColumnType col_type) {
    if (column.is_key()) {
        _num_key_columns++;
    }
    if (column.is_nullable()) {
        _num_null_columns++;
    }
    if (column.is_variant_type()) {
        ++_num_variant_columns;
        if (!column.has_path_info()) {
            const std::string& col_name = column.name_lower_case();
            vectorized::PathInData path(col_name);
            column.set_path_info(path);
        }
    }
    if (UNLIKELY(column.name() == DELETE_SIGN)) {
        _delete_sign_idx = _num_columns;
    } else if (UNLIKELY(column.name() == SEQUENCE_COL)) {
        _sequence_col_idx = _num_columns;
    } else if (UNLIKELY(column.name() == VERSION_COL)) {
        _version_col_idx = _num_columns;
    }
    _field_id_to_index[column.unique_id()] = _num_columns;
    _cols.push_back(std::make_shared<TabletColumn>(std::move(column)));
    // The dropped column may have same name with exsiting column, so that
    // not add to name to index map, only for uid to index map
    if (col_type == ColumnType::VARIANT || _cols.back()->is_variant_type()) {
        _field_name_to_index.emplace(StringRef(_cols.back()->name()), _num_columns);
        _field_path_to_index[_cols.back()->path_info_ptr().get()] = _num_columns;
    } else if (col_type == ColumnType::NORMAL) {
        _field_name_to_index.emplace(StringRef(_cols.back()->name()), _num_columns);
    }
    _num_columns++;
}

void TabletColumn::append_sparse_column(TabletColumn column) {
    _sparse_cols.push_back(std::make_shared<TabletColumn>(column));
    _num_sparse_columns++;
}

void TabletSchema::append_index(TabletIndex index) {
    _indexes.push_back(std::move(index));
}

void TabletSchema::update_index(const TabletColumn& col, TabletIndex index) {
    int32_t col_unique_id = col.unique_id();
    const std::string& suffix_path =
            col.has_path_info() ? escape_for_path_name(col.path_info_ptr()->get_path()) : "";
    for (size_t i = 0; i < _indexes.size(); i++) {
        for (int32_t id : _indexes[i].col_unique_ids()) {
            if (id == col_unique_id && _indexes[i].get_index_suffix() == suffix_path) {
                _indexes[i] = index;
            }
        }
    }
}

void TabletSchema::replace_column(size_t pos, TabletColumn new_col) {
    CHECK_LT(pos, num_columns()) << " outof range";
    _cols[pos] = std::make_shared<TabletColumn>(std::move(new_col));
}

void TabletSchema::clear_index() {
    _indexes.clear();
}

void TabletSchema::remove_index(int64_t index_id) {
    std::vector<TabletIndex> indexes;
    for (auto index : _indexes) {
        if (index.index_id() == index_id) {
            continue;
        }
        indexes.emplace_back(std::move(index));
    }
    _indexes = std::move(indexes);
}

void TabletSchema::clear_columns() {
    _field_path_to_index.clear();
    _field_name_to_index.clear();
    _field_id_to_index.clear();
    _num_columns = 0;
    _num_variant_columns = 0;
    _num_null_columns = 0;
    _num_key_columns = 0;
    _cols.clear();
}

void TabletSchema::init_from_pb(const TabletSchemaPB& schema, bool ignore_extracted_columns) {
    _keys_type = schema.keys_type();
    _num_columns = 0;
    _num_variant_columns = 0;
    _num_key_columns = 0;
    _num_null_columns = 0;
    _cols.clear();
    _indexes.clear();
    _field_name_to_index.clear();
    _field_id_to_index.clear();
    _cluster_key_idxes.clear();
    for (const auto& i : schema.cluster_key_idxes()) {
        _cluster_key_idxes.push_back(i);
    }
    for (auto& column_pb : schema.column()) {
        TabletColumn column;
        column.init_from_pb(column_pb);
        if (ignore_extracted_columns && column.is_extracted_column()) {
            continue;
        }
        if (column.is_key()) {
            _num_key_columns++;
        }
        if (column.is_nullable()) {
            _num_null_columns++;
        }
        if (column.is_variant_type()) {
            ++_num_variant_columns;
        }
        _cols.emplace_back(std::make_shared<TabletColumn>(std::move(column)));
        _field_name_to_index.emplace(StringRef(_cols.back()->name()), _num_columns);
        _field_id_to_index[_cols.back()->unique_id()] = _num_columns;
        _num_columns++;
    }
    for (auto& index_pb : schema.index()) {
        TabletIndex index;
        index.init_from_pb(index_pb);
        _indexes.emplace_back(std::move(index));
    }
    _num_short_key_columns = schema.num_short_key_columns();
    _num_rows_per_row_block = schema.num_rows_per_row_block();
    _compress_kind = schema.compress_kind();
    _next_column_unique_id = schema.next_column_unique_id();
    if (schema.has_bf_fpp()) {
        _has_bf_fpp = true;
        _bf_fpp = schema.bf_fpp();
    } else {
        _has_bf_fpp = false;
        _bf_fpp = BLOOM_FILTER_DEFAULT_FPP;
    }
    _is_in_memory = schema.is_in_memory();
    _disable_auto_compaction = schema.disable_auto_compaction();
    _enable_single_replica_compaction = schema.enable_single_replica_compaction();
    _store_row_column = schema.store_row_column();
    _skip_write_index_on_load = schema.skip_write_index_on_load();
    _delete_sign_idx = schema.delete_sign_idx();
    _sequence_col_idx = schema.sequence_col_idx();
    _version_col_idx = schema.version_col_idx();
    _sort_type = schema.sort_type();
    _sort_col_num = schema.sort_col_num();
    _compression_type = schema.compression_type();
    _schema_version = schema.schema_version();
    // Default to V1 inverted index storage format for backward compatibility if not specified in schema.
    if (!schema.has_inverted_index_storage_format()) {
        _inverted_index_storage_format = InvertedIndexStorageFormatPB::V1;
    } else {
        _inverted_index_storage_format = schema.inverted_index_storage_format();
    }
}

void TabletSchema::copy_from(const TabletSchema& tablet_schema) {
    TabletSchemaPB tablet_schema_pb;
    tablet_schema.to_schema_pb(&tablet_schema_pb);
    init_from_pb(tablet_schema_pb);
    _table_id = tablet_schema.table_id();
}

void TabletSchema::update_index_info_from(const TabletSchema& tablet_schema) {
    for (auto& col : _cols) {
        if (col->unique_id() < 0) {
            continue;
        }
        const auto iter = tablet_schema._field_id_to_index.find(col->unique_id());
        if (iter == tablet_schema._field_id_to_index.end()) {
            continue;
        }
        int32_t col_idx = iter->second;
        if (col_idx < 0 || col_idx >= tablet_schema._cols.size()) {
            continue;
        }
        col->set_is_bf_column(tablet_schema._cols[col_idx]->is_bf_column());
        col->set_has_bitmap_index(tablet_schema._cols[col_idx]->has_bitmap_index());
    }
}

std::string TabletSchema::to_key() const {
    TabletSchemaPB pb;
    to_schema_pb(&pb);
    return TabletSchema::deterministic_string_serialize(pb);
}

void TabletSchema::build_current_tablet_schema(int64_t index_id, int32_t version,
                                               const OlapTableIndexSchema* index,
                                               const TabletSchema& ori_tablet_schema) {
    // copy from ori_tablet_schema
    _keys_type = ori_tablet_schema.keys_type();
    _num_short_key_columns = ori_tablet_schema.num_short_key_columns();
    _num_rows_per_row_block = ori_tablet_schema.num_rows_per_row_block();
    _compress_kind = ori_tablet_schema.compress_kind();

    // todo(yixiu): unique_id
    _next_column_unique_id = ori_tablet_schema.next_column_unique_id();
    _is_in_memory = ori_tablet_schema.is_in_memory();
    _disable_auto_compaction = ori_tablet_schema.disable_auto_compaction();
    _enable_single_replica_compaction = ori_tablet_schema.enable_single_replica_compaction();
    _store_row_column = ori_tablet_schema.store_row_column();
    _skip_write_index_on_load = ori_tablet_schema.skip_write_index_on_load();
    _sort_type = ori_tablet_schema.sort_type();
    _sort_col_num = ori_tablet_schema.sort_col_num();

    // copy from table_schema_param
    _schema_version = version;
    _num_columns = 0;
    _num_variant_columns = 0;
    _num_key_columns = 0;
    _num_null_columns = 0;
    bool has_bf_columns = false;
    _cols.clear();
    _indexes.clear();
    _field_name_to_index.clear();
    _field_id_to_index.clear();
    _delete_sign_idx = -1;
    _sequence_col_idx = -1;
    _version_col_idx = -1;
    _cluster_key_idxes.clear();
    for (const auto& i : ori_tablet_schema._cluster_key_idxes) {
        _cluster_key_idxes.push_back(i);
    }
    for (auto& column : index->columns) {
        if (column->is_key()) {
            _num_key_columns++;
        }
        if (column->is_nullable()) {
            _num_null_columns++;
        }
        if (column->is_bf_column()) {
            has_bf_columns = true;
        }
        if (column->is_variant_type()) {
            ++_num_variant_columns;
        }
        if (UNLIKELY(column->name() == DELETE_SIGN)) {
            _delete_sign_idx = _num_columns;
        } else if (UNLIKELY(column->name() == SEQUENCE_COL)) {
            _sequence_col_idx = _num_columns;
        } else if (UNLIKELY(column->name() == VERSION_COL)) {
            _version_col_idx = _num_columns;
        }
        _cols.emplace_back(std::make_shared<TabletColumn>(*column));
        _field_name_to_index.emplace(StringRef(_cols.back()->name()), _num_columns);
        _field_id_to_index[_cols.back()->unique_id()] = _num_columns;
        _num_columns++;
    }

    for (auto& i : index->indexes) {
        _indexes.emplace_back(*i);
    }

    if (has_bf_columns) {
        _has_bf_fpp = true;
        _bf_fpp = ori_tablet_schema.bloom_filter_fpp();
    } else {
        _has_bf_fpp = false;
        _bf_fpp = BLOOM_FILTER_DEFAULT_FPP;
    }
}

void TabletSchema::merge_dropped_columns(const TabletSchema& src_schema) {
    // If they are the same tablet schema object, then just return
    if (this == &src_schema) {
        return;
    }
    for (const auto& src_col : src_schema.columns()) {
        if (_field_id_to_index.find(src_col->unique_id()) == _field_id_to_index.end()) {
            CHECK(!src_col->is_key())
                    << src_col->name() << " is key column, should not be dropped.";
            ColumnPB src_col_pb;
            // There are some pointer in tablet column, not sure the reference relation, so
            // that deep copy it.
            src_col->to_schema_pb(&src_col_pb);
            TabletColumn new_col(src_col_pb);
            append_column(new_col, TabletSchema::ColumnType::DROPPED);
        }
    }
}

TabletSchemaSPtr TabletSchema::copy_without_extracted_columns() {
    TabletSchemaSPtr copy = std::make_shared<TabletSchema>();
    TabletSchemaPB tablet_schema_pb;
    this->to_schema_pb(&tablet_schema_pb);
    copy->init_from_pb(tablet_schema_pb, true /*ignore extracted_columns*/);
    return copy;
}

// Dropped column is in _field_id_to_index but not in _field_name_to_index
// Could refer to append_column method
bool TabletSchema::is_dropped_column(const TabletColumn& col) const {
    CHECK(_field_id_to_index.find(col.unique_id()) != _field_id_to_index.end())
            << "could not find col with unique id = " << col.unique_id()
            << " and name = " << col.name();
    return _field_name_to_index.find(StringRef(col.name())) == _field_name_to_index.end() ||
           column(col.name()).unique_id() != col.unique_id();
}

void TabletSchema::copy_extracted_columns(const TabletSchema& src_schema) {
    std::unordered_set<int32_t> variant_columns;
    for (const auto& col : columns()) {
        if (col->is_variant_type()) {
            variant_columns.insert(col->unique_id());
        }
    }
    for (const TabletColumnPtr& col : src_schema.columns()) {
        if (col->is_extracted_column() && variant_columns.contains(col->parent_unique_id())) {
            ColumnPB col_pb;
            col->to_schema_pb(&col_pb);
            TabletColumn new_col(col_pb);
            append_column(new_col, ColumnType::VARIANT);
        }
    }
}

void TabletSchema::reserve_extracted_columns() {
    for (auto it = _cols.begin(); it != _cols.end();) {
        if (!(*it)->is_extracted_column()) {
            it = _cols.erase(it);
        } else {
            ++it;
        }
    }
}

void TabletSchema::to_schema_pb(TabletSchemaPB* tablet_schema_pb) const {
    for (const auto& i : _cluster_key_idxes) {
        tablet_schema_pb->add_cluster_key_idxes(i);
    }
    tablet_schema_pb->set_keys_type(_keys_type);
    for (const auto& col : _cols) {
        ColumnPB* column = tablet_schema_pb->add_column();
        col->to_schema_pb(column);
    }
    for (const auto& index : _indexes) {
        auto* index_pb = tablet_schema_pb->add_index();
        index.to_schema_pb(index_pb);
    }
    tablet_schema_pb->set_num_short_key_columns(_num_short_key_columns);
    tablet_schema_pb->set_num_rows_per_row_block(_num_rows_per_row_block);
    tablet_schema_pb->set_compress_kind(_compress_kind);
    if (_has_bf_fpp) {
        tablet_schema_pb->set_bf_fpp(_bf_fpp);
    }
    tablet_schema_pb->set_next_column_unique_id(_next_column_unique_id);
    tablet_schema_pb->set_is_in_memory(_is_in_memory);
    tablet_schema_pb->set_disable_auto_compaction(_disable_auto_compaction);
    tablet_schema_pb->set_enable_single_replica_compaction(_enable_single_replica_compaction);
    tablet_schema_pb->set_store_row_column(_store_row_column);
    tablet_schema_pb->set_skip_write_index_on_load(_skip_write_index_on_load);
    tablet_schema_pb->set_delete_sign_idx(_delete_sign_idx);
    tablet_schema_pb->set_sequence_col_idx(_sequence_col_idx);
    tablet_schema_pb->set_sort_type(_sort_type);
    tablet_schema_pb->set_sort_col_num(_sort_col_num);
    tablet_schema_pb->set_schema_version(_schema_version);
    tablet_schema_pb->set_compression_type(_compression_type);
    tablet_schema_pb->set_version_col_idx(_version_col_idx);
    tablet_schema_pb->set_inverted_index_storage_format(_inverted_index_storage_format);
}

size_t TabletSchema::row_size() const {
    size_t size = 0;
    for (const auto& column : _cols) {
        size += column->length();
    }
    size += (_num_columns + 7) / 8;

    return size;
}

int32_t TabletSchema::field_index(const std::string& field_name) const {
    const auto& found = _field_name_to_index.find(StringRef(field_name));
    return (found == _field_name_to_index.end()) ? -1 : found->second;
}

int32_t TabletSchema::field_index(const vectorized::PathInData& path) const {
    const auto& found = _field_path_to_index.find(vectorized::PathInDataRef(&path));
    return (found == _field_path_to_index.end()) ? -1 : found->second;
}

int32_t TabletSchema::field_index(int32_t col_unique_id) const {
    const auto& found = _field_id_to_index.find(col_unique_id);
    return (found == _field_id_to_index.end()) ? -1 : found->second;
}

const std::vector<TabletColumnPtr>& TabletSchema::columns() const {
    return _cols;
}

const std::vector<TabletColumnPtr>& TabletColumn::sparse_columns() const {
    return _sparse_cols;
}

const TabletColumn& TabletSchema::column(size_t ordinal) const {
    DCHECK(ordinal < _num_columns) << "ordinal:" << ordinal << ", _num_columns:" << _num_columns;
    return *_cols[ordinal];
}

const TabletColumn& TabletColumn::sparse_column_at(size_t ordinal) const {
    DCHECK(ordinal < _sparse_cols.size())
            << "ordinal:" << ordinal << ", _num_columns:" << _sparse_cols.size();
    return *_sparse_cols[ordinal];
}

const TabletColumn& TabletSchema::column_by_uid(int32_t col_unique_id) const {
    return *_cols.at(_field_id_to_index.at(col_unique_id));
}

TabletColumn& TabletSchema::mutable_column_by_uid(int32_t col_unique_id) {
    return *_cols.at(_field_id_to_index.at(col_unique_id));
}

TabletColumn& TabletSchema::mutable_column(size_t ordinal) {
    return *_cols.at(ordinal);
}

void TabletSchema::update_indexes_from_thrift(const std::vector<doris::TOlapTableIndex>& tindexes) {
    std::vector<TabletIndex> indexes;
    for (auto& tindex : tindexes) {
        TabletIndex index;
        index.init_from_thrift(tindex, *this);
        indexes.emplace_back(std::move(index));
    }
    _indexes = std::move(indexes);
}

Status TabletSchema::have_column(const std::string& field_name) const {
    if (!_field_name_to_index.contains(StringRef(field_name))) {
        return Status::Error<ErrorCode::INTERNAL_ERROR>(
                "Not found field_name, field_name:{}, schema:{}", field_name,
                get_all_field_names());
    }
    return Status::OK();
}

const TabletColumn& TabletSchema::column(const std::string& field_name) const {
    DCHECK(_field_name_to_index.contains(StringRef(field_name)) != 0)
            << ", field_name=" << field_name << ", field_name_to_index=" << get_all_field_names();
    const auto& found = _field_name_to_index.find(StringRef(field_name));
    return *_cols[found->second];
}

std::vector<const TabletIndex*> TabletSchema::get_indexes_for_column(
        const TabletColumn& col) const {
    std::vector<const TabletIndex*> indexes_for_column;
    // Some columns (Float, Double, JSONB ...) from the variant do not support index, but they are listed in TabltetIndex.
    if (!segment_v2::InvertedIndexColumnWriter::check_support_inverted_index(col)) {
        return indexes_for_column;
    }
    int32_t col_unique_id = col.is_extracted_column() ? col.parent_unique_id() : col.unique_id();
    const std::string& suffix_path =
            col.has_path_info() ? escape_for_path_name(col.path_info_ptr()->get_path()) : "";
    // TODO use more efficient impl
    for (size_t i = 0; i < _indexes.size(); i++) {
        for (int32_t id : _indexes[i].col_unique_ids()) {
            if (id == col_unique_id && _indexes[i].get_index_suffix() == suffix_path) {
                indexes_for_column.push_back(&(_indexes[i]));
            }
        }
    }

    return indexes_for_column;
}

bool TabletSchema::has_inverted_index(const TabletColumn& col) const {
    // TODO use more efficient impl
    int32_t col_unique_id = col.is_extracted_column() ? col.parent_unique_id() : col.unique_id();
    const std::string& suffix_path =
            col.has_path_info() ? escape_for_path_name(col.path_info_ptr()->get_path()) : "";
    for (size_t i = 0; i < _indexes.size(); i++) {
        if (_indexes[i].index_type() == IndexType::INVERTED) {
            for (int32_t id : _indexes[i].col_unique_ids()) {
                if (id == col_unique_id && _indexes[i].get_index_suffix() == suffix_path) {
                    return true;
                }
            }
        }
    }

    return false;
}

bool TabletSchema::has_inverted_index_with_index_id(int64_t index_id,
                                                    const std::string& suffix_name) const {
    for (size_t i = 0; i < _indexes.size(); i++) {
        if (_indexes[i].index_type() == IndexType::INVERTED &&
            _indexes[i].get_index_suffix() == suffix_name && _indexes[i].index_id() == index_id) {
            return true;
        }
    }
    return false;
}

const TabletIndex* TabletSchema::get_inverted_index_with_index_id(
        int64_t index_id, const std::string& suffix_name) const {
    for (size_t i = 0; i < _indexes.size(); i++) {
        if (_indexes[i].index_type() == IndexType::INVERTED &&
            _indexes[i].get_index_suffix() == suffix_name && _indexes[i].index_id() == index_id) {
            return &(_indexes[i]);
        }
    }

    return nullptr;
}

const TabletIndex* TabletSchema::get_inverted_index(int32_t col_unique_id,
                                                    const std::string& suffix_path) const {
    for (size_t i = 0; i < _indexes.size(); i++) {
        if (_indexes[i].index_type() == IndexType::INVERTED) {
            for (int32_t id : _indexes[i].col_unique_ids()) {
                if (id == col_unique_id &&
                    _indexes[i].get_index_suffix() == escape_for_path_name(suffix_path)) {
                    return &(_indexes[i]);
                }
            }
        }
    }
    return nullptr;
}

const TabletIndex* TabletSchema::get_inverted_index(const TabletColumn& col,
                                                    bool check_valid) const {
    // With check_valid set to true by default
    // Some columns(Float, Double, JSONB ...) from the variant do not support inverted index
    if (check_valid && !segment_v2::InvertedIndexColumnWriter::check_support_inverted_index(col)) {
        return nullptr;
    }
    // TODO use more efficient impl
    // Use parent id if unique not assigned, this could happend when accessing subcolumns of variants
    int32_t col_unique_id = col.is_extracted_column() ? col.parent_unique_id() : col.unique_id();
    const std::string& suffix_path =
            col.has_path_info() ? escape_for_path_name(col.path_info_ptr()->get_path()) : "";
    return get_inverted_index(col_unique_id, suffix_path);
}

bool TabletSchema::has_ngram_bf_index(int32_t col_unique_id) const {
    // TODO use more efficient impl
    for (size_t i = 0; i < _indexes.size(); i++) {
        if (_indexes[i].index_type() == IndexType::NGRAM_BF) {
            for (int32_t id : _indexes[i].col_unique_ids()) {
                if (id == col_unique_id) {
                    return true;
                }
            }
        }
    }

    return false;
}

const TabletIndex* TabletSchema::get_ngram_bf_index(int32_t col_unique_id) const {
    // TODO use more efficient impl
    for (size_t i = 0; i < _indexes.size(); i++) {
        if (_indexes[i].index_type() == IndexType::NGRAM_BF) {
            for (int32_t id : _indexes[i].col_unique_ids()) {
                if (id == col_unique_id) {
                    return &(_indexes[i]);
                }
            }
        }
    }

    return nullptr;
}

vectorized::Block TabletSchema::create_block(
        const std::vector<uint32_t>& return_columns,
        const std::unordered_set<uint32_t>* tablet_columns_need_convert_null) const {
    vectorized::Block block;
    for (int i = 0; i < return_columns.size(); ++i) {
        const auto& col = *_cols[return_columns[i]];
        bool is_nullable = (tablet_columns_need_convert_null != nullptr &&
                            tablet_columns_need_convert_null->find(return_columns[i]) !=
                                    tablet_columns_need_convert_null->end());
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(col, is_nullable);
        auto column = data_type->create_column();
        block.insert({std::move(column), data_type, col.name()});
    }
    return block;
}

vectorized::Block TabletSchema::create_block(bool ignore_dropped_col) const {
    vectorized::Block block;
    for (const auto& col : _cols) {
        if (ignore_dropped_col && is_dropped_column(*col)) {
            continue;
        }
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(*col);
        block.insert({data_type->create_column(), data_type, col->name()});
    }
    return block;
}

vectorized::Block TabletSchema::create_block_by_cids(const std::vector<uint32_t>& cids) {
    vectorized::Block block;
    for (const auto& cid : cids) {
        const auto& col = *_cols[cid];
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(col);
        block.insert({data_type->create_column(), data_type, col.name()});
    }
    return block;
}

bool operator==(const TabletColumn& a, const TabletColumn& b) {
    if (a._unique_id != b._unique_id) return false;
    if (a._col_name != b._col_name) return false;
    if (a._type != b._type) return false;
    if (a._is_key != b._is_key) return false;
    if (a._aggregation != b._aggregation) return false;
    if (a._is_nullable != b._is_nullable) return false;
    if (a._has_default_value != b._has_default_value) return false;
    if (a._has_default_value) {
        if (a._default_value != b._default_value) return false;
    }
    if (a._is_decimal != b._is_decimal) return false;
    if (a._is_decimal) {
        if (a._precision != b._precision) return false;
        if (a._frac != b._frac) return false;
    }
    if (a._length != b._length) return false;
    if (a._index_length != b._index_length) return false;
    if (a._is_bf_column != b._is_bf_column) return false;
    if (a._has_bitmap_index != b._has_bitmap_index) return false;
    if (a._column_path == nullptr && a._column_path != nullptr) return false;
    if (b._column_path == nullptr && a._column_path != nullptr) return false;
    if (b._column_path != nullptr && a._column_path != nullptr &&
        *a._column_path != *b._column_path)
        return false;
    return true;
}

bool operator!=(const TabletColumn& a, const TabletColumn& b) {
    return !(a == b);
}

bool operator==(const TabletSchema& a, const TabletSchema& b) {
    if (a._keys_type != b._keys_type) return false;
    if (a._cols.size() != b._cols.size()) return false;
    for (int i = 0; i < a._cols.size(); ++i) {
        if (*a._cols[i] != *b._cols[i]) return false;
    }
    if (a._num_columns != b._num_columns) return false;
    if (a._num_key_columns != b._num_key_columns) return false;
    if (a._num_null_columns != b._num_null_columns) return false;
    if (a._num_short_key_columns != b._num_short_key_columns) return false;
    if (a._num_rows_per_row_block != b._num_rows_per_row_block) return false;
    if (a._compress_kind != b._compress_kind) return false;
    if (a._next_column_unique_id != b._next_column_unique_id) return false;
    if (a._has_bf_fpp != b._has_bf_fpp) return false;
    if (a._has_bf_fpp) {
        if (std::abs(a._bf_fpp - b._bf_fpp) > 1e-6) return false;
    }
    if (a._is_in_memory != b._is_in_memory) return false;
    if (a._delete_sign_idx != b._delete_sign_idx) return false;
    if (a._disable_auto_compaction != b._disable_auto_compaction) return false;
    if (a._enable_single_replica_compaction != b._enable_single_replica_compaction) return false;
    if (a._store_row_column != b._store_row_column) return false;
    if (a._skip_write_index_on_load != b._skip_write_index_on_load) return false;
    return true;
}

bool operator!=(const TabletSchema& a, const TabletSchema& b) {
    return !(a == b);
}

std::string TabletSchema::deterministic_string_serialize(const TabletSchemaPB& schema_pb) {
    std::string output;
    google::protobuf::io::StringOutputStream string_output_stream(&output);
    google::protobuf::io::CodedOutputStream output_stream(&string_output_stream);
    output_stream.SetSerializationDeterministic(true);
    schema_pb.SerializeToCodedStream(&output_stream);
    return output;
}

} // namespace doris
