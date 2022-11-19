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

#pragma once

#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment_v2.pb.h"
#include "olap/olap_define.h"
#include "olap/types.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/data_types/data_type.h"

namespace doris {
namespace vectorized {
class Block;
}

class POlapTableIndexSchema;

class TabletColumn {
public:
    TabletColumn();
    TabletColumn(const ColumnPB& column);
    TabletColumn(const TColumn& column);
    TabletColumn(FieldAggregationMethod agg, FieldType type);
    TabletColumn(FieldAggregationMethod agg, FieldType filed_type, bool is_nullable);
    TabletColumn(FieldAggregationMethod agg, FieldType filed_type, bool is_nullable,
                 int32_t unique_id, size_t length);
    void init_from_pb(const ColumnPB& column);
    void init_from_thrift(const TColumn& column);
    void to_schema_pb(ColumnPB* column) const;
    uint32_t mem_size() const;

    int32_t unique_id() const { return _unique_id; }
    std::string name() const { return _col_name; }
    void set_name(std::string col_name) { _col_name = col_name; }
    FieldType type() const { return _type; }
    bool is_key() const { return _is_key; }
    bool is_nullable() const { return _is_nullable; }
    bool is_bf_column() const { return _is_bf_column; }
    bool has_bitmap_index() const { return _has_bitmap_index; }
    bool is_length_variable_type() const {
        return _type == OLAP_FIELD_TYPE_CHAR || _type == OLAP_FIELD_TYPE_VARCHAR ||
               _type == OLAP_FIELD_TYPE_STRING || _type == OLAP_FIELD_TYPE_HLL ||
               _type == OLAP_FIELD_TYPE_OBJECT || _type == OLAP_FIELD_TYPE_QUANTILE_STATE;
    }
    bool has_default_value() const { return _has_default_value; }
    std::string default_value() const { return _default_value; }
    size_t length() const { return _length; }
    size_t index_length() const { return _index_length; }
    void set_index_length(size_t index_length) { _index_length = index_length; }
    FieldAggregationMethod aggregation() const { return _aggregation; }
    vectorized::AggregateFunctionPtr get_aggregate_function(vectorized::DataTypes argument_types,
                                                            std::string suffix) const;
    int precision() const { return _precision; }
    int frac() const { return _frac; }
    bool visible() const { return _visible; }
    // Add a sub column.
    void add_sub_column(TabletColumn& sub_column);

    uint32_t get_subtype_count() const { return _sub_column_count; }
    const TabletColumn& get_sub_column(uint32_t i) const { return _sub_columns[i]; }

    friend bool operator==(const TabletColumn& a, const TabletColumn& b);
    friend bool operator!=(const TabletColumn& a, const TabletColumn& b);

    static std::string get_string_by_field_type(FieldType type);
    static std::string get_string_by_aggregation_type(FieldAggregationMethod aggregation_type);
    static FieldType get_field_type_by_string(const std::string& str);
    static FieldAggregationMethod get_aggregation_type_by_string(const std::string& str);
    static uint32_t get_field_length_by_type(TPrimitiveType::type type, uint32_t string_length);

private:
    int32_t _unique_id;
    std::string _col_name;
    FieldType _type;
    bool _is_key = false;
    FieldAggregationMethod _aggregation;
    bool _is_nullable = false;

    bool _has_default_value = false;
    std::string _default_value;

    bool _is_decimal = false;
    int32_t _precision;
    int32_t _frac;

    int32_t _length;
    int32_t _index_length;

    bool _is_bf_column = false;

    bool _has_bitmap_index = false;
    bool _visible = true;

    TabletColumn* _parent = nullptr;
    std::vector<TabletColumn> _sub_columns;
    uint32_t _sub_column_count = 0;
};

bool operator==(const TabletColumn& a, const TabletColumn& b);
bool operator!=(const TabletColumn& a, const TabletColumn& b);

class TabletSchema;

class TabletIndex {
public:
    void init_from_thrift(const TOlapTableIndex& index, const TabletSchema& tablet_schema);
    void init_from_pb(const TabletIndexPB& index);
    void to_schema_pb(TabletIndexPB* index) const;

    const int64_t index_id() const { return _index_id; }
    const std::string& index_name() const { return _index_name; }
    const IndexType index_type() const { return _index_type; }
    const vector<int32_t>& col_unique_ids() const { return _col_unique_ids; }
    const std::map<string, string>& properties() const { return _properties; }

private:
    int64_t _index_id;
    std::string _index_name;
    IndexType _index_type;
    std::vector<int32_t> _col_unique_ids;
    std::map<string, string> _properties;
};

class TabletSchema {
public:
    // TODO(yingchun): better to make constructor as private to avoid
    // manually init members incorrectly, and define a new function like
    // void create_from_pb(const TabletSchemaPB& schema, TabletSchema* tablet_schema).
    TabletSchema() = default;
    void init_from_pb(const TabletSchemaPB& schema);
    void to_schema_pb(TabletSchemaPB* tablet_meta_pb) const;
    void append_column(TabletColumn column, bool is_dropped_column = false);
    void copy_from(const TabletSchema& tablet_schema);
    std::string to_key() const;
    uint32_t mem_size() const;

    size_t row_size() const;
    int32_t field_index(const std::string& field_name) const;
    int32_t field_index(int32_t col_unique_id) const;
    const TabletColumn& column(size_t ordinal) const;
    const TabletColumn& column(const std::string& field_name) const;
    const TabletColumn& column_by_uid(int32_t col_unique_id) const;
    const std::vector<TabletColumn>& columns() const;
    size_t num_columns() const { return _num_columns; }
    size_t num_key_columns() const { return _num_key_columns; }
    size_t num_null_columns() const { return _num_null_columns; }
    size_t num_short_key_columns() const { return _num_short_key_columns; }
    size_t num_rows_per_row_block() const { return _num_rows_per_row_block; }
    KeysType keys_type() const { return _keys_type; }
    SortType sort_type() const { return _sort_type; }
    size_t sort_col_num() const { return _sort_col_num; }
    CompressKind compress_kind() const { return _compress_kind; }
    size_t next_column_unique_id() const { return _next_column_unique_id; }
    bool has_bf_fpp() const { return _has_bf_fpp; }
    double bloom_filter_fpp() const { return _bf_fpp; }
    bool is_in_memory() const { return _is_in_memory; }
    void set_is_in_memory(bool is_in_memory) { _is_in_memory = is_in_memory; }
    void set_disable_auto_compaction(bool disable_auto_compaction) {
        _disable_auto_compaction = disable_auto_compaction;
    }
    bool disable_auto_compaction() const { return _disable_auto_compaction; }
    int32_t delete_sign_idx() const { return _delete_sign_idx; }
    void set_delete_sign_idx(int32_t delete_sign_idx) { _delete_sign_idx = delete_sign_idx; }
    bool has_sequence_col() const { return _sequence_col_idx != -1; }
    int32_t sequence_col_idx() const { return _sequence_col_idx; }
    segment_v2::CompressionTypePB compression_type() const { return _compression_type; }

    const std::vector<TabletIndex>& indexes() const { return _indexes; }
    std::vector<const TabletIndex*> get_indexes_for_column(int32_t col_unique_id) const;
    bool has_inverted_index(int32_t col_unique_id) const;
    const TabletIndex* get_inverted_index(int32_t col_unique_id) const;
    void update_indexes_from_thrift(const std::vector<doris::TOlapTableIndex>& indexes);

    int32_t schema_version() const { return _schema_version; }
    void clear_columns();
    vectorized::Block create_block(
            const std::vector<uint32_t>& return_columns,
            const std::unordered_set<uint32_t>* tablet_columns_need_convert_null = nullptr) const;
    vectorized::Block create_block(bool ignore_dropped_col = true) const;

    void build_current_tablet_schema(int64_t index_id, int32_t version,
                                     const POlapTableIndexSchema& index,
                                     const TabletSchema& out_tablet_schema);

    // Merge columns that not exit in current schema, these column is dropped in current schema
    // but they are useful in some cases. For example,
    // 1. origin schema is  ColA, ColB
    // 2. insert values     1, 2
    // 3. delete where ColB = 2
    // 4. drop ColB
    // 5. insert values  3
    // 6. add column ColB, although it is name ColB, but it is different with previous ColB, the new ColB we name could call ColB'
    // 7. insert value  4, 5
    // Then the read schema should be ColA, ColB, ColB' because the delete predicate need ColB to remove related data.
    // Because they have same name, so that the dropped column should not be added to the map, only with unique id.
    void merge_dropped_columns(std::shared_ptr<TabletSchema> src_schema);

    bool is_dropped_column(const TabletColumn& col) const;

private:
    friend bool operator==(const TabletSchema& a, const TabletSchema& b);
    friend bool operator!=(const TabletSchema& a, const TabletSchema& b);

private:
    KeysType _keys_type = DUP_KEYS;
    SortType _sort_type = SortType::LEXICAL;
    size_t _sort_col_num = 0;
    std::vector<TabletColumn> _cols;
    std::vector<TabletIndex> _indexes;
    std::unordered_map<std::string, int32_t> _field_name_to_index;
    std::unordered_map<int32_t, int32_t> _field_id_to_index;
    size_t _num_columns = 0;
    size_t _num_key_columns = 0;
    size_t _num_null_columns = 0;
    size_t _num_short_key_columns = 0;
    size_t _num_rows_per_row_block = 0;
    CompressKind _compress_kind = COMPRESS_NONE;
    segment_v2::CompressionTypePB _compression_type = segment_v2::CompressionTypePB::LZ4F;
    size_t _next_column_unique_id = 0;

    bool _has_bf_fpp = false;
    double _bf_fpp = 0;
    bool _is_in_memory = false;
    int32_t _delete_sign_idx = -1;
    int32_t _sequence_col_idx = -1;
    int32_t _schema_version = -1;
    bool _disable_auto_compaction = false;
};

bool operator==(const TabletSchema& a, const TabletSchema& b);
bool operator!=(const TabletSchema& a, const TabletSchema& b);

using TabletSchemaSPtr = std::shared_ptr<TabletSchema>;

} // namespace doris