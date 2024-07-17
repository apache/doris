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

#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/consts.h"
#include "common/status.h"
#include "gutil/stringprintf.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/options.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "util/string_util.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/common/string_ref.h"
#include "vec/common/string_utils/string_utils.h"
#include "vec/core/types.h"
#include "vec/json/path_in_data.h"

namespace doris {
namespace vectorized {
class Block;
class PathInData;
class IDataType;
} // namespace vectorized

struct OlapTableIndexSchema;
class TColumn;
class TOlapTableIndex;
class TabletColumn;

using TabletColumnPtr = std::shared_ptr<TabletColumn>;

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

    int32_t unique_id() const { return _unique_id; }
    void set_unique_id(int32_t id) { _unique_id = id; }
    const std::string& name() const { return _col_name; }
    const std::string& name_lower_case() const { return _col_name_lower_case; }
    void set_name(std::string col_name) {
        _col_name = col_name;
        _col_name_lower_case = to_lower(_col_name);
    }
    FieldType type() const { return _type; }
    void set_type(FieldType type) { _type = type; }
    bool is_key() const { return _is_key; }
    bool is_nullable() const { return _is_nullable; }
    bool is_auto_increment() const { return _is_auto_increment; }
    bool is_variant_type() const { return _type == FieldType::OLAP_FIELD_TYPE_VARIANT; }
    bool is_bf_column() const { return _is_bf_column; }
    bool has_bitmap_index() const { return _has_bitmap_index; }
    bool is_array_type() const { return _type == FieldType::OLAP_FIELD_TYPE_ARRAY; }
    bool is_jsonb_type() const { return _type == FieldType::OLAP_FIELD_TYPE_JSONB; }
    bool is_length_variable_type() const {
        return _type == FieldType::OLAP_FIELD_TYPE_CHAR ||
               _type == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
               _type == FieldType::OLAP_FIELD_TYPE_STRING ||
               _type == FieldType::OLAP_FIELD_TYPE_HLL ||
               _type == FieldType::OLAP_FIELD_TYPE_OBJECT ||
               _type == FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE ||
               _type == FieldType::OLAP_FIELD_TYPE_AGG_STATE;
    }
    // Such columns are not exist in frontend schema info, so we need to
    // add them into tablet_schema for later column indexing.
    static TabletColumn create_materialized_variant_column(const std::string& root,
                                                           const std::vector<std::string>& paths,
                                                           int32_t parent_unique_id);
    bool has_default_value() const { return _has_default_value; }
    std::string default_value() const { return _default_value; }
    size_t length() const { return _length; }
    void set_length(size_t length) { _length = length; }
    void set_default_value(const std::string& default_value) {
        _default_value = default_value;
        _has_default_value = true;
    }
    size_t index_length() const { return _index_length; }
    void set_index_length(size_t index_length) { _index_length = index_length; }
    void set_is_key(bool is_key) { _is_key = is_key; }
    void set_is_nullable(bool is_nullable) { _is_nullable = is_nullable; }
    void set_is_auto_increment(bool is_auto_increment) { _is_auto_increment = is_auto_increment; }
    void set_has_default_value(bool has) { _has_default_value = has; }
    void set_path_info(const vectorized::PathInData& path);
    FieldAggregationMethod aggregation() const { return _aggregation; }
    vectorized::AggregateFunctionPtr get_aggregate_function_union(
            vectorized::DataTypePtr type) const;
    vectorized::AggregateFunctionPtr get_aggregate_function(std::string suffix) const;
    int precision() const { return _precision; }
    int frac() const { return _frac; }
    inline bool visible() const { return _visible; }

    void set_aggregation_method(FieldAggregationMethod agg) {
        _aggregation = agg;
        _aggregation_name = get_string_by_aggregation_type(agg);
    }

    /**
     * Add a sub column.
     */
    void add_sub_column(TabletColumn& sub_column);

    uint32_t get_subtype_count() const { return _sub_column_count; }
    const TabletColumn& get_sub_column(uint32_t i) const { return *_sub_columns[i]; }
    const std::vector<TabletColumnPtr>& get_sub_columns() const { return _sub_columns; }

    friend bool operator==(const TabletColumn& a, const TabletColumn& b);
    friend bool operator!=(const TabletColumn& a, const TabletColumn& b);

    static std::string get_string_by_field_type(FieldType type);
    static std::string get_string_by_aggregation_type(FieldAggregationMethod aggregation_type);
    static FieldType get_field_type_by_string(const std::string& str);
    static FieldType get_field_type_by_type(PrimitiveType type);
    static FieldAggregationMethod get_aggregation_type_by_string(const std::string& str);
    static uint32_t get_field_length_by_type(TPrimitiveType::type type, uint32_t string_length);
    bool is_row_store_column() const;
    std::string get_aggregation_name() const { return _aggregation_name; }
    bool get_result_is_nullable() const { return _result_is_nullable; }
    int get_be_exec_version() const { return _be_exec_version; }
    bool has_path_info() const { return _column_path != nullptr && !_column_path->empty(); }
    const vectorized::PathInDataPtr& path_info_ptr() const { return _column_path; }
    // If it is an extracted column from variant column
    bool is_extracted_column() const {
        return _column_path != nullptr && !_column_path->empty() && _parent_col_unique_id > 0;
    };
    int32_t parent_unique_id() const { return _parent_col_unique_id; }
    void set_parent_unique_id(int32_t col_unique_id) { _parent_col_unique_id = col_unique_id; }
    void set_is_bf_column(bool is_bf_column) { _is_bf_column = is_bf_column; }
    void set_has_bitmap_index(bool has_bitmap_index) { _has_bitmap_index = has_bitmap_index; }
    std::shared_ptr<const vectorized::IDataType> get_vec_type() const;

    void append_sparse_column(TabletColumn column);
    const TabletColumn& sparse_column_at(size_t oridinal) const;
    const std::vector<TabletColumnPtr>& sparse_columns() const;
    size_t num_sparse_columns() const { return _num_sparse_columns; }

    Status check_valid() const {
        if (type() != FieldType::OLAP_FIELD_TYPE_ARRAY &&
            type() != FieldType::OLAP_FIELD_TYPE_STRUCT &&
            type() != FieldType::OLAP_FIELD_TYPE_MAP) {
            return Status::OK();
        }
        if (is_bf_column()) {
            return Status::NotSupported("Do not support bloom filter index, type={}",
                                        get_string_by_field_type(type()));
        }
        if (has_bitmap_index()) {
            return Status::NotSupported("Do not support bitmap index, type={}",
                                        get_string_by_field_type(type()));
        }
        return Status::OK();
    }

private:
    int32_t _unique_id = -1;
    std::string _col_name;
    std::string _col_name_lower_case;
    // the field _type will change from TPrimitiveType
    // to string by 'EnumToString(TPrimitiveType, tcolumn.column_type.type, data_type);' (reference: TabletMeta::init_column_from_tcolumn)
    // to FieldType by 'TabletColumn::get_field_type_by_string' (reference: TabletColumn::init_from_pb).
    // And the _type in columnPB is string and it changed from FieldType by 'get_string_by_field_type' (reference: TabletColumn::to_schema_pb).
    FieldType _type;
    bool _is_key = false;
    FieldAggregationMethod _aggregation;
    std::string _aggregation_name;
    bool _is_nullable = false;
    bool _is_auto_increment = false;

    bool _has_default_value = false;
    std::string _default_value;

    bool _is_decimal = false;
    int32_t _precision = -1;
    int32_t _frac = -1;

    int32_t _length = -1;
    int32_t _index_length = -1;

    bool _is_bf_column = false;

    bool _has_bitmap_index = false;
    bool _visible = true;
    int32_t _parent_col_unique_id = -1;
    std::vector<TabletColumnPtr> _sub_columns;
    uint32_t _sub_column_count = 0;

    bool _result_is_nullable = false;
    int _be_exec_version = -1;
    vectorized::PathInDataPtr _column_path;

    // Record information about columns merged into a sparse column within a variant
    // `{"id": 100, "name" : "jack", "point" : 3.9}`
    // If the information mentioned above is inserted into the variant column,
    // 'id' and 'name' are correctly extracted, while 'point' is merged into the sparse column due to its sparsity.
    // The path_info and type of 'point' will be recorded using the TabletColumn.
    // Use shared_ptr for reuse and reducing column memory usage
    std::vector<TabletColumnPtr> _sparse_cols;
    size_t _num_sparse_columns = 0;
};

bool operator==(const TabletColumn& a, const TabletColumn& b);
bool operator!=(const TabletColumn& a, const TabletColumn& b);

class TabletSchema;

class TabletIndex {
public:
    TabletIndex() = default;
    void init_from_thrift(const TOlapTableIndex& index, const TabletSchema& tablet_schema);
    void init_from_thrift(const TOlapTableIndex& index, const std::vector<int32_t>& column_uids);
    void init_from_pb(const TabletIndexPB& index);
    void to_schema_pb(TabletIndexPB* index) const;

    int64_t index_id() const { return _index_id; }
    const std::string& index_name() const { return _index_name; }
    IndexType index_type() const { return _index_type; }
    const vector<int32_t>& col_unique_ids() const { return _col_unique_ids; }
    const std::map<string, string>& properties() const { return _properties; }
    int32_t get_gram_size() const {
        if (_properties.contains("gram_size")) {
            return std::stoi(_properties.at("gram_size"));
        }

        return 0;
    }
    int32_t get_gram_bf_size() const {
        if (_properties.contains("bf_size")) {
            return std::stoi(_properties.at("bf_size"));
        }

        return 0;
    }

    const std::string& get_index_suffix() const { return _escaped_index_suffix_path; }

    void set_escaped_escaped_index_suffix_path(const std::string& name);

private:
    int64_t _index_id = -1;
    // Identify the different index with the same _index_id
    std::string _escaped_index_suffix_path;
    std::string _index_name;
    IndexType _index_type;
    std::vector<int32_t> _col_unique_ids;
    std::map<string, string> _properties;
};

class TabletSchema {
public:
    enum ColumnType { NORMAL = 0, DROPPED = 1, VARIANT = 2 };
    // TODO(yingchun): better to make constructor as private to avoid
    // manually init members incorrectly, and define a new function like
    // void create_from_pb(const TabletSchemaPB& schema, TabletSchema* tablet_schema).
    TabletSchema();
    virtual ~TabletSchema();

    void init_from_pb(const TabletSchemaPB& schema, bool ignore_extracted_columns = false);
    // Notice: Use deterministic way to serialize protobuf,
    // since serialize Map in protobuf may could lead to un-deterministic by default
    static std::string deterministic_string_serialize(const TabletSchemaPB& schema_pb);
    void to_schema_pb(TabletSchemaPB* tablet_meta_pb) const;
    void append_column(TabletColumn column, ColumnType col_type = ColumnType::NORMAL);
    void append_index(TabletIndex index);
    void update_index(const TabletColumn& column, TabletIndex index);
    void remove_index(int64_t index_id);
    void clear_index();
    // Must make sure the row column is always the last column
    void add_row_column();
    void copy_from(const TabletSchema& tablet_schema);
    void update_index_info_from(const TabletSchema& tablet_schema);
    std::string to_key() const;
    // Don't use.
    // TODO: memory size of TabletSchema cannot be accurately tracked.
    // In some places, temporarily use num_columns() as TabletSchema size.
    int64_t mem_size() const { return _mem_size; }
    size_t row_size() const;
    int32_t field_index(const std::string& field_name) const;
    int32_t field_index(const vectorized::PathInData& path) const;
    int32_t field_index(int32_t col_unique_id) const;
    const TabletColumn& column(size_t ordinal) const;
    Result<const TabletColumn*> column(const std::string& field_name) const;
    Status have_column(const std::string& field_name) const;
    const TabletColumn& column_by_uid(int32_t col_unique_id) const;
    TabletColumn& mutable_column_by_uid(int32_t col_unique_id);
    TabletColumn& mutable_column(size_t ordinal);
    void replace_column(size_t pos, TabletColumn new_col);
    const std::vector<TabletColumnPtr>& columns() const;
    size_t num_columns() const { return _num_columns; }
    size_t num_key_columns() const { return _num_key_columns; }
    const std::vector<uint32_t>& cluster_key_idxes() const { return _cluster_key_idxes; }
    size_t num_null_columns() const { return _num_null_columns; }
    size_t num_short_key_columns() const { return _num_short_key_columns; }
    size_t num_rows_per_row_block() const { return _num_rows_per_row_block; }
    size_t num_variant_columns() const { return _num_variant_columns; };
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
    void set_enable_single_replica_compaction(bool enable_single_replica_compaction) {
        _enable_single_replica_compaction = enable_single_replica_compaction;
    }
    bool enable_single_replica_compaction() const { return _enable_single_replica_compaction; }
    // indicate if full row store column(all the columns encodes as row) exists
    bool has_row_store_for_all_columns() const {
        return _store_row_column && row_columns_uids().empty();
    }
    void set_skip_write_index_on_load(bool skip) { _skip_write_index_on_load = skip; }
    bool skip_write_index_on_load() const { return _skip_write_index_on_load; }
    int32_t delete_sign_idx() const { return _delete_sign_idx; }
    void set_delete_sign_idx(int32_t delete_sign_idx) { _delete_sign_idx = delete_sign_idx; }
    bool has_sequence_col() const { return _sequence_col_idx != -1; }
    int32_t sequence_col_idx() const { return _sequence_col_idx; }
    void set_version_col_idx(int32_t version_col_idx) { _version_col_idx = version_col_idx; }
    int32_t version_col_idx() const { return _version_col_idx; }
    segment_v2::CompressionTypePB compression_type() const { return _compression_type; }
    void set_row_store_page_size(long page_size) { _row_store_page_size = page_size; }
    long row_store_page_size() const { return _row_store_page_size; }

    const std::vector<TabletIndex>& indexes() const { return _indexes; }
    bool has_inverted_index() const {
        for (const auto& index : _indexes) {
            if (index.index_type() == IndexType::INVERTED) {
                return true;
            }
        }
        return false;
    }
    std::vector<const TabletIndex*> get_indexes_for_column(const TabletColumn& col) const;
    bool has_inverted_index(const TabletColumn& col) const;
    bool has_inverted_index_with_index_id(int64_t index_id, const std::string& suffix_path) const;
    const TabletIndex* get_inverted_index_with_index_id(int64_t index_id,
                                                        const std::string& suffix_name) const;
    // check_valid: check if this column supports inverted index
    // Some columns (Float, Double, JSONB ...) from the variant do not support index, but they are listed in TabletIndex.
    // If returned, the index file will not be found.
    const TabletIndex* get_inverted_index(const TabletColumn& col, bool check_valid = true) const;
    const TabletIndex* get_inverted_index(int32_t col_unique_id,
                                          const std::string& suffix_path) const;
    bool has_ngram_bf_index(int32_t col_unique_id) const;
    const TabletIndex* get_ngram_bf_index(int32_t col_unique_id) const;
    void update_indexes_from_thrift(const std::vector<doris::TOlapTableIndex>& indexes);
    // If schema version is not set, it should be -1
    int32_t schema_version() const { return _schema_version; }
    void clear_columns();
    vectorized::Block create_block(
            const std::vector<uint32_t>& return_columns,
            const std::unordered_set<uint32_t>* tablet_columns_need_convert_null = nullptr) const;
    vectorized::Block create_block(bool ignore_dropped_col = true) const;
    void set_schema_version(int32_t version) { _schema_version = version; }
    void set_auto_increment_column(const std::string& auto_increment_column) {
        _auto_increment_column = auto_increment_column;
    }
    std::string auto_increment_column() const { return _auto_increment_column; }

    void set_table_id(int64_t table_id) { _table_id = table_id; }
    int64_t table_id() const { return _table_id; }
    void set_db_id(int64_t db_id) { _db_id = db_id; }
    int64_t db_id() const { return _db_id; }
    void build_current_tablet_schema(int64_t index_id, int32_t version,
                                     const OlapTableIndexSchema* index,
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
    void merge_dropped_columns(const TabletSchema& src_schema);

    bool is_dropped_column(const TabletColumn& col) const;

    // copy extracted columns from src_schema
    void copy_extracted_columns(const TabletSchema& src_schema);

    // only reserve extracted columns
    void reserve_extracted_columns();

    string get_all_field_names() const {
        string str = "[";
        for (auto p : _field_name_to_index) {
            if (str.size() > 1) {
                str += ", ";
            }
            str += p.first.to_string() + "(" + std::to_string(_cols[p.second]->unique_id()) + ")";
        }
        str += "]";
        return str;
    }

    // Dump [(name, type, is_nullable), ...]
    string dump_structure() const {
        string str = "[";
        for (auto p : _cols) {
            if (str.size() > 1) {
                str += ", ";
            }
            str += "(";
            str += p->name();
            str += ", ";
            str += TabletColumn::get_string_by_field_type(p->type());
            str += ", ";
            str += "is_nullable:";
            str += (p->is_nullable() ? "true" : "false");
            str += ")";
        }
        str += "]";
        return str;
    }

    string dump_full_schema() const {
        string str = "[";
        for (auto p : _cols) {
            if (str.size() > 1) {
                str += ", ";
            }
            ColumnPB col_pb;
            p->to_schema_pb(&col_pb);
            str += "(";
            str += col_pb.ShortDebugString();
            str += ")";
        }
        str += "]";
        return str;
    }

    vectorized::Block create_block_by_cids(const std::vector<uint32_t>& cids);

    std::shared_ptr<TabletSchema> copy_without_variant_extracted_columns();
    InvertedIndexStorageFormatPB get_inverted_index_storage_format() const {
        return _inverted_index_storage_format;
    }

    void update_tablet_columns(const TabletSchema& tablet_schema,
                               const std::vector<TColumn>& t_columns);

    const std::vector<int32_t>& row_columns_uids() const { return _row_store_column_unique_ids; }

private:
    friend bool operator==(const TabletSchema& a, const TabletSchema& b);
    friend bool operator!=(const TabletSchema& a, const TabletSchema& b);

    KeysType _keys_type = DUP_KEYS;
    SortType _sort_type = SortType::LEXICAL;
    size_t _sort_col_num = 0;
    std::vector<TabletColumnPtr> _cols;

    std::vector<TabletIndex> _indexes;
    std::unordered_map<StringRef, int32_t, StringRefHash> _field_name_to_index;
    std::unordered_map<int32_t, int32_t> _field_id_to_index;
    std::unordered_map<vectorized::PathInDataRef, int32_t, vectorized::PathInDataRef::Hash>
            _field_path_to_index;
    size_t _num_columns = 0;
    size_t _num_variant_columns = 0;
    size_t _num_key_columns = 0;
    std::vector<uint32_t> _cluster_key_idxes;
    size_t _num_null_columns = 0;
    size_t _num_short_key_columns = 0;
    size_t _num_rows_per_row_block = 0;
    CompressKind _compress_kind = COMPRESS_NONE;
    segment_v2::CompressionTypePB _compression_type = segment_v2::CompressionTypePB::LZ4F;
    long _row_store_page_size = segment_v2::ROW_STORE_PAGE_SIZE_DEFAULT_VALUE;
    size_t _next_column_unique_id = 0;
    std::string _auto_increment_column;

    bool _has_bf_fpp = false;
    double _bf_fpp = 0;
    bool _is_in_memory = false;
    int32_t _delete_sign_idx = -1;
    int32_t _sequence_col_idx = -1;
    int32_t _version_col_idx = -1;
    int32_t _schema_version = -1;
    int64_t _table_id = -1;
    int64_t _db_id = -1;
    bool _disable_auto_compaction = false;
    bool _enable_single_replica_compaction = false;
    int64_t _mem_size = 0;
    bool _store_row_column = false;
    bool _skip_write_index_on_load = false;
    InvertedIndexStorageFormatPB _inverted_index_storage_format = InvertedIndexStorageFormatPB::V1;

    // Contains column ids of which columns should be encoded into row store.
    // ATTN: For compability reason empty cids means all columns of tablet schema are encoded to row column
    std::vector<int32_t> _row_store_column_unique_ids;
};

bool operator==(const TabletSchema& a, const TabletSchema& b);
bool operator!=(const TabletSchema& a, const TabletSchema& b);

using TabletSchemaSPtr = std::shared_ptr<TabletSchema>;

} // namespace doris
