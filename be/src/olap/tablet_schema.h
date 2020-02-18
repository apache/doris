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

#ifndef DORIS_BE_SRC_OLAP_TABLET_SCHEMA_H
#define DORIS_BE_SRC_OLAP_TABLET_SCHEMA_H

#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "olap/olap_define.h"
#include "olap/types.h"

namespace doris {

class TabletColumn {
public:
    TabletColumn();
    TabletColumn(FieldAggregationMethod agg, FieldType type);
    TabletColumn(FieldAggregationMethod agg, FieldType filed_type, bool is_nullable);
    OLAPStatus init_from_pb(const ColumnPB& column);
    OLAPStatus to_schema_pb(ColumnPB* column);

    inline int32_t unique_id() const { return _unique_id; }
    inline std::string name() const { return _col_name; }
    inline FieldType type() const { return _type; }
    inline bool is_key() const { return _is_key; }
    inline bool is_nullable() const { return _is_nullable; }
    inline bool is_bf_column() const { return _is_bf_column; }
    inline bool has_bitmap_index() const {return _has_bitmap_index; }
    bool has_default_value() const { return _has_default_value; }
    std::string default_value() const { return _default_value; }
    bool has_reference_column() const { return _has_referenced_column; }
    int32_t referenced_column_id() const { return _referenced_column_id; }
    std::string referenced_column() const { return _referenced_column; }
    size_t length() const { return _length; }
    size_t index_length() const { return _index_length; }
    FieldAggregationMethod aggregation() const { return _aggregation; }
    int precision() const { return _precision; }
    int frac() const { return _frac; }

    static std::string get_string_by_field_type(FieldType type);
    static std::string get_string_by_aggregation_type(FieldAggregationMethod aggregation_type);
    static FieldType get_field_type_by_string(const std::string& str);
    static FieldAggregationMethod get_aggregation_type_by_string(const std::string& str);
    static uint32_t get_field_length_by_type(TPrimitiveType::type type, uint32_t string_length);

private:
    int32_t _unique_id;
    std::string _col_name;
    FieldType _type;
    bool _is_key;
    FieldAggregationMethod _aggregation;
    bool _is_nullable;

    bool _has_default_value;
    std::string _default_value;

    bool _is_decimal;
    int32_t _precision;
    int32_t _frac;

    int32_t _length;
    int32_t _index_length;

    bool _is_bf_column;

    bool _has_referenced_column;
    int32_t _referenced_column_id;
    std::string _referenced_column;

    bool _has_bitmap_index = false;
};

class TabletSchema {
public:
    TabletSchema();
    OLAPStatus init_from_pb(const TabletSchemaPB& schema);
    OLAPStatus to_schema_pb(TabletSchemaPB* tablet_meta_pb);
    size_t row_size() const;
    size_t field_index(const std::string& field_name) const;
    const TabletColumn& column(size_t ordinal) const;
    const std::vector<TabletColumn>& columns() const;
    inline size_t num_columns() const { return _num_columns; }
    inline size_t num_key_columns() const { return _num_key_columns; }
    inline size_t num_null_columns() const { return _num_null_columns; }
    inline size_t num_short_key_columns() const { return _num_short_key_columns; }
    inline size_t num_rows_per_row_block() const { return _num_rows_per_row_block; }
    inline KeysType keys_type() const { return _keys_type; }
    inline CompressKind compress_kind() const { return _compress_kind; }
    inline size_t next_column_unique_id() const { return _next_column_unique_id; }
    inline double bloom_filter_fpp() const { return _bf_fpp; }
    inline bool is_in_memory() const {return _is_in_memory; }
    inline void set_is_in_memory (bool is_in_memory) {
        _is_in_memory = is_in_memory;
    }
private:
    KeysType _keys_type;
    std::vector<TabletColumn> _cols;
    size_t _num_columns;
    size_t _num_key_columns;
    size_t _num_null_columns;
    size_t _num_short_key_columns;
    size_t _num_rows_per_row_block;
    CompressKind _compress_kind;
    size_t _next_column_unique_id;

    bool _has_bf_fpp;
    double _bf_fpp;
    bool _is_in_memory = false;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_TABLET_SCHEMA_H
