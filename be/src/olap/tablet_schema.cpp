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

#include "tablet_meta.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

FieldType TabletColumn::get_field_type_by_string(const std::string& type_str) {
    std::string upper_type_str = type_str;
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
    } else if (0 == upper_type_str.compare("STRING")) {
        type = OLAP_FIELD_TYPE_STRING;
    } else if (0 == upper_type_str.compare("BOOLEAN")) {
        type = OLAP_FIELD_TYPE_BOOL;
    } else if (0 == upper_type_str.compare(0, 3, "HLL")) {
        type = OLAP_FIELD_TYPE_HLL;
    } else if (0 == upper_type_str.compare("STRUCT")) {
        type = OLAP_FIELD_TYPE_STRUCT;
    } else if (0 == upper_type_str.compare("LIST")) {
        type = OLAP_FIELD_TYPE_ARRAY;
    } else if (0 == upper_type_str.compare("MAP")) {
        type = OLAP_FIELD_TYPE_MAP;
    } else if (0 == upper_type_str.compare("OBJECT")) {
        type = OLAP_FIELD_TYPE_OBJECT;
    } else if (0 == upper_type_str.compare("ARRAY")) {
        type = OLAP_FIELD_TYPE_ARRAY;
    } else if (0 == upper_type_str.compare("QUANTILE_STATE")) {
        type = OLAP_FIELD_TYPE_QUANTILE_STATE;
    } else {
        LOG(WARNING) << "invalid type string. [type='" << type_str << "']";
        type = OLAP_FIELD_TYPE_UNKNOWN;
    }

    return type;
}

FieldAggregationMethod TabletColumn::get_aggregation_type_by_string(const std::string& str) {
    std::string upper_str = str;
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
    } else if (0 == upper_str.compare("REPLACE_IF_NOT_NULL")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL;
    } else if (0 == upper_str.compare("HLL_UNION")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_HLL_UNION;
    } else if (0 == upper_str.compare("BITMAP_UNION")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_BITMAP_UNION;
    } else if (0 == upper_str.compare("QUANTILE_UNION")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_QUANTILE_UNION;
    } else {
        LOG(WARNING) << "invalid aggregation type string. [aggregation='" << str << "']";
        aggregation_type = OLAP_FIELD_AGGREGATION_UNKNOWN;
    }

    return aggregation_type;
}

std::string TabletColumn::get_string_by_field_type(FieldType type) {
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

    case OLAP_FIELD_TYPE_STRING:
        return "STRING";

    case OLAP_FIELD_TYPE_BOOL:
        return "BOOLEAN";

    case OLAP_FIELD_TYPE_HLL:
        return "HLL";

    case OLAP_FIELD_TYPE_STRUCT:
        return "STRUCT";

    case OLAP_FIELD_TYPE_ARRAY:
        return "ARRAY";

    case OLAP_FIELD_TYPE_MAP:
        return "MAP";

    case OLAP_FIELD_TYPE_OBJECT:
        return "OBJECT";
    case OLAP_FIELD_TYPE_QUANTILE_STATE:
        return "QUANTILE_STATE";

    default:
        return "UNKNOWN";
    }
}

std::string TabletColumn::get_string_by_aggregation_type(FieldAggregationMethod type) {
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

    case OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL:
        return "REPLACE_IF_NOT_NULL";

    case OLAP_FIELD_AGGREGATION_HLL_UNION:
        return "HLL_UNION";

    case OLAP_FIELD_AGGREGATION_BITMAP_UNION:
        return "BITMAP_UNION";

    case OLAP_FIELD_AGGREGATION_QUANTILE_UNION:
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
    case TPrimitiveType::DATE:
        return 3;
    case TPrimitiveType::DATETIME:
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
        return string_length + sizeof(OLAP_VARCHAR_MAX_LENGTH);
    case TPrimitiveType::STRING:
        return string_length + sizeof(OLAP_STRING_MAX_LENGTH);
    case TPrimitiveType::ARRAY:
        return OLAP_ARRAY_MAX_LENGTH;
    case TPrimitiveType::DECIMALV2:
        return 12; // use 12 bytes in olap engine.
    default:
        OLAP_LOG_WARNING("unknown field type. [type=%d]", type);
        return 0;
    }
}

TabletColumn::TabletColumn() : _aggregation(OLAP_FIELD_AGGREGATION_NONE) {}

TabletColumn::TabletColumn(FieldAggregationMethod agg, FieldType type) {
    _aggregation = agg;
    _type = type;
}

TabletColumn::TabletColumn(FieldAggregationMethod agg, FieldType filed_type, bool is_nullable) {
    _aggregation = agg;
    _type = filed_type;
    _length = get_type_info(filed_type)->size();
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

void TabletColumn::init_from_pb(const ColumnPB& column) {
    _unique_id = column.unique_id();
    _col_name = column.name();
    _type = TabletColumn::get_field_type_by_string(column.type());
    _is_key = column.is_key();
    _is_nullable = column.is_nullable();

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
    _has_referenced_column = column.has_referenced_column_id();
    if (_has_referenced_column) {
        _referenced_column_id = column.referenced_column_id();
    }
    if (column.has_aggregation()) {
        _aggregation = get_aggregation_type_by_string(column.aggregation());
    }
    if (column.has_visible()) {
        _visible = column.visible();
    }
    if (_type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        DCHECK(column.children_columns_size() == 1) << "ARRAY type has more than 1 children types.";
        TabletColumn child_column;
        child_column.init_from_pb(column.children_columns(0));
        add_sub_column(child_column);
    }
}

void TabletColumn::to_schema_pb(ColumnPB* column) {
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
    column->set_aggregation(get_string_by_aggregation_type(_aggregation));
    if (_has_referenced_column) {
        column->set_referenced_column_id(_referenced_column_id);
    }
    if (_has_bitmap_index) {
        column->set_has_bitmap_index(_has_bitmap_index);
    }
    column->set_visible(_visible);

    if (_type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        DCHECK(_sub_columns.size() == 1) << "ARRAY type has more than 1 children types.";
        ColumnPB* child = column->add_children_columns();
        _sub_columns[0].to_schema_pb(child);
    }
}

uint32_t TabletColumn::mem_size() const {
    auto size = sizeof(TabletColumn);
    if (_has_default_value) {
        size += _default_value.size();
    }
    for (auto& sub_column : _sub_columns) {
        size += sub_column.mem_size();
    }
    return size;
}

void TabletColumn::add_sub_column(TabletColumn& sub_column) {
    _sub_columns.push_back(sub_column);
    sub_column._parent = this;
    _sub_column_count += 1;
}

void TabletSchema::init_from_pb(const TabletSchemaPB& schema) {
    _keys_type = schema.keys_type();
    _num_columns = 0;
    _num_key_columns = 0;
    _num_null_columns = 0;
    _cols.clear();
    _field_name_to_index.clear();
    for (auto& column_pb : schema.column()) {
        TabletColumn column;
        column.init_from_pb(column_pb);
        if (column.is_key()) {
            _num_key_columns++;
        }
        if (column.is_nullable()) {
            _num_null_columns++;
        }
        _field_name_to_index[column.name()] = _num_columns;
        _cols.emplace_back(std::move(column));
        _num_columns++;
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
    _delete_sign_idx = schema.delete_sign_idx();
    _sequence_col_idx = schema.sequence_col_idx();
    _sort_type = schema.sort_type();
    _sort_col_num = schema.sort_col_num();
}

void TabletSchema::to_schema_pb(TabletSchemaPB* tablet_meta_pb) {
    tablet_meta_pb->set_keys_type(_keys_type);
    for (auto& col : _cols) {
        ColumnPB* column = tablet_meta_pb->add_column();
        col.to_schema_pb(column);
    }
    tablet_meta_pb->set_num_short_key_columns(_num_short_key_columns);
    tablet_meta_pb->set_num_rows_per_row_block(_num_rows_per_row_block);
    tablet_meta_pb->set_compress_kind(_compress_kind);
    if (_has_bf_fpp) {
        tablet_meta_pb->set_bf_fpp(_bf_fpp);
    }
    tablet_meta_pb->set_next_column_unique_id(_next_column_unique_id);
    tablet_meta_pb->set_is_in_memory(_is_in_memory);
    tablet_meta_pb->set_delete_sign_idx(_delete_sign_idx);
    tablet_meta_pb->set_sequence_col_idx(_sequence_col_idx);
    tablet_meta_pb->set_sort_type(_sort_type);
    tablet_meta_pb->set_sort_col_num(_sort_col_num);
}

uint32_t TabletSchema::mem_size() const {
    auto size = sizeof(TabletSchema);
    for (auto& col : _cols) {
        size += col.mem_size();
    }

    for (auto& pair : _field_name_to_index) {
        size += pair.first.size();
        size += sizeof(pair.second);
    }
    return size;
}

size_t TabletSchema::row_size() const {
    size_t size = 0;
    for (auto& column : _cols) {
        size += column.length();
    }
    size += (_num_columns + 7) / 8;

    return size;
}

int32_t TabletSchema::field_index(const std::string& field_name) const {
    const auto& found = _field_name_to_index.find(field_name);
    return (found == _field_name_to_index.end()) ? -1 : found->second;
}

const std::vector<TabletColumn>& TabletSchema::columns() const {
    return _cols;
}

const TabletColumn& TabletSchema::column(size_t ordinal) const {
    DCHECK(ordinal < _num_columns) << "ordinal:" << ordinal << ", _num_columns:" << _num_columns;
    return _cols[ordinal];
}

void TabletSchema::init_field_index_for_test() {
    _field_name_to_index.clear();
    for (int i = 0; i < _cols.size(); ++i) {
        _field_name_to_index[_cols[i].name()] = i;
    }
}

vectorized::Block TabletSchema::create_block(
        const std::vector<uint32_t>& return_columns,
        const std::unordered_set<uint32_t>* tablet_columns_need_convert_null) const {
    vectorized::Block block;
    for (int i = 0; i < return_columns.size(); ++i) {
        const auto& col = _cols[return_columns[i]];
        bool is_nullable = (tablet_columns_need_convert_null != nullptr &&
                tablet_columns_need_convert_null->find(return_columns[i]) != tablet_columns_need_convert_null->end());
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(col, is_nullable);
        auto column = data_type->create_column();
        block.insert({std::move(column), data_type, col.name()});
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
    if (a._has_referenced_column != b._has_referenced_column) return false;
    if (a._has_referenced_column) {
        if (a._referenced_column_id != b._referenced_column_id) return false;
        if (a._referenced_column != b._referenced_column) return false;
    }
    if (a._has_bitmap_index != b._has_bitmap_index) return false;
    return true;
}

bool operator!=(const TabletColumn& a, const TabletColumn& b) {
    return !(a == b);
}

bool operator==(const TabletSchema& a, const TabletSchema& b) {
    if (a._keys_type != b._keys_type) return false;
    if (a._cols.size() != b._cols.size()) return false;
    for (int i = 0; i < a._cols.size(); ++i) {
        if (a._cols[i] != b._cols[i]) return false;
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
    return true;
}

bool operator!=(const TabletSchema& a, const TabletSchema& b) {
    return !(a == b);
}

} // namespace doris
