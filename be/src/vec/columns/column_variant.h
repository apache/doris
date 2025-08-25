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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnVariant.h
// and modified by Doris

#pragma once
#include <butil/compiler_specific.h>
#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <sys/types.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/subcolumn_tree.h"
#include "vec/common/cow.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_variant.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/json/path_in_data.h"

class SipHash;

namespace doris {
namespace vectorized {
class Arena;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

#ifdef NDEBUG
#define ENABLE_CHECK_CONSISTENCY (void)/* Nothing */
#else
#define ENABLE_CHECK_CONSISTENCY(this) (this)->check_consistency()
#endif

/// Info that represents a scalar or array field in a decomposed view.
/// It allows to recreate field with different number
/// of dimensions or nullability.
struct FieldInfo {
    /// The common type id of of all scalars in field.
    PrimitiveType scalar_type_id = PrimitiveType::INVALID_TYPE;
    /// Do we have NULL scalar in field.
    bool have_nulls = false;
    /// If true then we have scalars with different types in array and
    /// we need to convert scalars to the common type.
    bool need_convert = false;
    /// Number of dimension in array. 0 if field is scalar.
    size_t num_dimensions = 0;

    // decimal info
    int scale = 0;
    int precision = 0;
};

/** A column that represents object with dynamic set of subcolumns.
 *  Subcolumns are identified by paths in document and are stored in
 *  a trie-like structure. ColumnVariant is not suitable for writing into tables
 *  and it should be converted to Tuple with fixed set of subcolumns before that.
 */
class ColumnVariant final : public COWHelper<IColumn, ColumnVariant> {
public:
    /** Class that represents one subcolumn.
     * It stores values in several parts of column
     * and keeps current common type of all parts.
     * We add a new column part with a new type, when we insert a field,
     * which can't be converted to the current common type.
     * After insertion of all values subcolumn should be finalized
     * for writing and other operations.
     */

    // Using jsonb type as most common type, since it's adopted all types of json
    using MostCommonType = DataTypeJsonb;
    constexpr static PrimitiveType MOST_COMMON_TYPE_ID = PrimitiveType::TYPE_JSONB;
    // Nullable(Array(Nullable(Object)))
    const static DataTypePtr NESTED_TYPE;
    // Array(Nullable(Jsonb))
    const static DataTypePtr NESTED_TYPE_AS_ARRAY_OF_JSONB;

    // Finlize mode for subcolumns, write mode will estimate which subcolumns are sparse columns(too many null values inside column),
    // merge and encode them into a shared column in root column. Only affects in flush block to segments.
    // Otherwise read mode should be as default mode.
    enum class FinalizeMode { WRITE_MODE, READ_MODE };
    class Subcolumn {
    public:
        Subcolumn() = default;

        Subcolumn(size_t size_, bool is_nullable_, bool is_root = false);

        Subcolumn(MutableColumnPtr&& data_, DataTypePtr type, bool is_nullable_,
                  bool is_root = false);

        size_t size() const;

        size_t byteSize() const;

        size_t allocatedBytes() const;

        bool is_finalized() const;

        const DataTypePtr& get_least_common_type() const { return least_common_type.get(); }

        const PrimitiveType& get_least_common_base_type_id() const {
            return least_common_type.get_base_type_id();
        }

        const DataTypePtr& get_least_common_typeBase() const {
            return least_common_type.get_base();
        }

        size_t get_non_null_value_size() const;

        size_t serialize_text_json(size_t n, BufferWritable& output,
                                   DataTypeSerDe::FormatOptions opt = {}) const;

        const DataTypeSerDeSPtr& get_least_common_type_serde() const {
            return least_common_type.get_serde();
        }

        size_t get_dimensions() const { return least_common_type.get_dimensions(); }

        void get(size_t n, FieldWithDataType& res) const;

        bool is_null_at(size_t n) const;

        /// Inserts a field, which scalars can be arbitrary, but number of
        /// dimensions should be consistent with current common type.
        /// throws InvalidArgument when meet conflict types
        void insert(Field field);

        void insert(Field field, FieldInfo info);

        void insert(FieldWithDataType field);

        void insert_default();

        void increment_default_counter() { ++current_num_of_defaults; }

        void reset_current_num_of_defaults() { current_num_of_defaults = 0; }

        size_t cur_num_of_defaults() const { return current_num_of_defaults; }

        void insert_many_defaults(size_t length);

        void insert_range_from(const Subcolumn& src, size_t start, size_t length);

        /// Recreates subcolumn with default scalar values and keeps sizes of arrays.
        /// Used to create columns of type Nested with consistent array sizes.
        Subcolumn clone_with_default_values(const FieldInfo& field_info) const;

        void pop_back(size_t n);

        // Cut a new subcolumns from current one, element from start to start + length
        Subcolumn cut(size_t start, size_t length) const;

        /// Converts all column's parts to the common type and
        /// creates a single column that stores all values.
        void finalize(FinalizeMode mode = FinalizeMode::READ_MODE);

        /// Returns last inserted field.
        Field get_last_field() const;

        /// Returns single column if subcolumn in finalizes.
        /// Otherwise -- undefined behaviour.
        IColumn& get_finalized_column();

        const IColumn& get_finalized_column() const;

        const ColumnPtr& get_finalized_column_ptr() const;

        ColumnPtr& get_finalized_column_ptr();

        void remove_nullable();

        void add_new_column_part(DataTypePtr type);

        // Serialize the i-th row of the column into the sparse column.
        void serialize_to_sparse_column(ColumnString* key, std::string_view path,
                                        ColumnString* value, size_t row);

        static DataTypeSerDeSPtr generate_data_serdes(DataTypePtr type, bool is_root = false);

        friend class ColumnVariant;

        bool is_empty_nested(size_t row) const;

        void resize(size_t n);

    private:
        class LeastCommonType {
        public:
            LeastCommonType() = default;

            explicit LeastCommonType(DataTypePtr type_, bool is_root = false);

            const DataTypePtr& get() const { return type; }

            const DataTypePtr& get_base() const { return base_type; }

            // The least command type id
            const PrimitiveType& get_type_id() const { return type_id; }

            // The inner least common type if of array,
            // example: Array(Nullable(Object))
            // then the base type id is Object
            const PrimitiveType& get_base_type_id() const { return base_type_id; }

            size_t get_dimensions() const { return num_dimensions; }

            void remove_nullable() { type = doris::vectorized::remove_nullable(type); }

            const DataTypeSerDeSPtr& get_serde() const { return least_common_type_serder; }

        private:
            DataTypePtr type;
            DataTypePtr base_type;
            PrimitiveType type_id;
            PrimitiveType base_type_id;
            size_t num_dimensions = 0;
            DataTypeSerDeSPtr least_common_type_serder;
        };

        void wrapp_array_nullable();

        /// Current least common type of all values inserted to this subcolumn.
        LeastCommonType least_common_type;
        /// If true then common type type of subcolumn is Nullable
        /// and default values are NULLs.
        bool is_nullable = false;
        /// Parts of column. Parts should be in increasing order in terms of subtypes/supertypes.
        /// That means that the least common type for i-th prefix is the type of i-th part
        /// and it's the supertype for all type of column from 0 to i-1.
        std::vector<WrappedPtr> data;
        std::vector<DataTypePtr> data_types;
        std::vector<DataTypeSerDeSPtr> data_serdes;
        /// Until we insert any non-default field we don't know further
        /// least common type and we count number of defaults in prefix,
        /// which will be converted to the default type of final common type.
        size_t num_of_defaults_in_prefix = 0;
        // If it is the root subcolumn of SubcolumnsTree,
        // the root Node should be JSONB type when finalize
        bool is_root = false;
        size_t num_rows = 0;
        // distinguish from num_of_defaults_in_prefix when data is not empty
        size_t current_num_of_defaults = 0;
    };
    using Subcolumns = SubcolumnsTree<Subcolumn, false>;

private:
    /// If true then all subcolumns are nullable.
    const bool is_nullable;
    Subcolumns subcolumns;
    size_t num_rows;

    using SubColumnWithName = std::pair<PathInData, const Subcolumn*>;
    // Cached search results for previous row (keyed as index in JSON object) - used as a hint.
    mutable std::vector<SubColumnWithName> _prev_positions;

    // It's filled when the number of subcolumns reaches the limit.
    // It has type Map(String, String) and stores a map (path, binary serialized subcolumn value) for each row.
    WrappedPtr serialized_sparse_column = ColumnMap::create(
            ColumnString::create(), ColumnString::create(), ColumnArray::ColumnOffsets::create());

    int32_t _max_subcolumns_count = 0;

    size_t typed_path_count = 0;

    size_t nested_path_count = 0;

public:
    static constexpr auto COLUMN_NAME_DUMMY = "_dummy";

    // always create root: data type nothing
    explicit ColumnVariant(int32_t max_subcolumns_count);

    // always create root: data type nothing
    explicit ColumnVariant(int32_t max_subcolumns_count, size_t size);

    explicit ColumnVariant(int32_t max_subcolumns_count, DataTypePtr root_type,
                           MutableColumnPtr&& root_column);

    explicit ColumnVariant(int32_t max_subcolumns_count, Subcolumns&& subcolumns_);

    ~ColumnVariant() override = default;

    /// Checks that all subcolumns have consistent sizes.
    void check_consistency() const;

    MutableColumnPtr get_root() {
        if (subcolumns.empty()) {
            return nullptr;
        }
        return subcolumns.get_mutable_root()->data.get_finalized_column_ptr()->assume_mutable();
    }

    void serialize_one_row_to_string(int64_t row, std::string* output) const;

    void serialize_one_row_to_string(int64_t row, BufferWritable& output) const;

    // serialize one row to json format
    void serialize_one_row_to_json_format(int64_t row, BufferWritable& output, bool* is_null) const;

    // Fill the `serialized_sparse_column`
    Status serialize_sparse_columns(std::map<std::string_view, Subcolumn>&& remaing_subcolumns);

    // ensure root node is a certain type
    void ensure_root_node_type(const DataTypePtr& type);

    // create root with type and column if missing
    void create_root(const DataTypePtr& type, MutableColumnPtr&& column);

    static const DataTypePtr& get_most_common_type();

    void clear_sparse_column();

    // root is null or type nothing
    bool is_null_root() const;

    // Only single scalar root column
    bool is_scalar_variant() const;

    ColumnPtr get_root() const { return subcolumns.get_root()->data.get_finalized_column_ptr(); }

    bool has_subcolumn(const PathInData& key) const;

    DataTypePtr get_root_type() const;

    // return null if not found
    const Subcolumn* get_subcolumn(const PathInData& key) const;

    // return null if not found
    const Subcolumn* get_subcolumn(const PathInData& key, size_t index_hint) const;

    // return null if not found
    Subcolumn* get_subcolumn(const PathInData& key);

    // return null if not found
    Subcolumn* get_subcolumn(const PathInData& key, size_t index_hint);

    void incr_num_rows() { ++num_rows; }

    void incr_num_rows(size_t n) { num_rows += n; }

    void set_num_rows(size_t n);

    size_t rows() const { return num_rows; }

    int32_t max_subcolumns_count() const { return _max_subcolumns_count; }

    /// Adds a subcolumn from existing IColumn.
    bool add_sub_column(const PathInData& key, MutableColumnPtr&& subcolumn, DataTypePtr type);

    /// Adds a subcolumn of specific size with default values.
    bool add_sub_column(const PathInData& key, size_t new_size);
    /// Adds a subcolumn of type Nested of specific size with default values.
    /// It cares about consistency of sizes of Nested arrays.
    void add_nested_subcolumn(const PathInData& key, const FieldInfo& field_info, size_t new_size);
    /// Finds a subcolumn from the same Nested type as @entry and inserts
    /// an array with default values with consistent sizes as in Nested type.
    bool try_insert_default_from_nested(const Subcolumns::NodePtr& entry) const;
    bool try_insert_many_defaults_from_nested(const Subcolumns::NodePtr& entry) const;

    const Subcolumns& get_subcolumns() const { return subcolumns; }

    Subcolumns& get_subcolumns() { return subcolumns; }

    ColumnPtr get_sparse_column() const { return serialized_sparse_column; }

    // use sparse_subcolumns_schema to record sparse column's path info and type
    static MutableColumnPtr create_sparse_column_fn() {
        return vectorized::ColumnMap::create(vectorized::ColumnString::create(),
                                             vectorized::ColumnString::create(),
                                             vectorized::ColumnArray::ColumnOffsets::create());
    }

    static const DataTypePtr& get_sparse_column_type() {
        static DataTypePtr type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(),
                                                                std::make_shared<DataTypeString>());
        return type;
    }

    void set_sparse_column(ColumnPtr column) { serialized_sparse_column = column; }

    void finalize(FinalizeMode mode);

    /// Finalizes all subcolumns.
    void finalize() override;

    bool is_finalized() const;

    MutableColumnPtr clone_finalized() const {
        auto finalized = IColumn::mutate(get_ptr());
        static_cast<ColumnVariant*>(finalized.get())->finalize(FinalizeMode::READ_MODE);
        return finalized;
    }

    MutableColumnPtr clone() const override;

    void clear() override;

    void resize(size_t n) override;

    std::string get_name() const override {
        if (is_scalar_variant()) {
            return "var_scalar(" + get_root()->get_name() + ")";
        }
        return "variant";
    }

    size_t size() const override;

    MutableColumnPtr clone_resized(size_t new_size) const override;

    size_t byte_size() const override;

    size_t allocated_bytes() const override;

    bool has_enough_capacity(const IColumn& src) const override { return false; }

    void for_each_subcolumn(ColumnCallback callback) override;

    // Do nothing, call try_insert instead
    void insert(const Field& field) override { try_insert(field); }

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override;

    void insert_from(const IColumn& src, size_t n) override;

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_default() override;

    void insert_many_defaults(size_t length) override;

    void pop_back(size_t length) override;

    Field operator[](size_t n) const override;

    void get(size_t n, Field& res) const override;

    void update_hash_with_value(size_t n, SipHash& hash) const override;

    ColumnPtr filter(const Filter&, ssize_t) const override;

    size_t filter(const Filter&) override;

    MutableColumnPtr permute(const Permutation&, size_t) const override;

    bool is_variable_length() const override { return true; }

    template <typename Func>
    MutableColumnPtr apply_for_columns(Func&& func) const;

    bool empty() const;

    // Check if all columns and types are aligned, only in debug mode
    Status sanitize() const;

    std::string debug_string() const;

    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data = nullptr) const override;

    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override;

    void update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type, uint32_t rows,
                                uint32_t offset = 0,
                                const uint8_t* __restrict null_data = nullptr) const override;

    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override;

    // Not implemented
    StringRef get_data_at(size_t) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "get_data_at " + std::string(get_name()));
    }

    StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                         char const*& begin) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,

                               "serialize_value_into_arena " + get_name());
    }

    const char* deserialize_and_insert_from_arena(const char* pos) override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "deserialize_and_insert_from_arena " + get_name());
    }

    void insert_data(const char* pos, size_t length) override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "insert_data " + get_name());
    }

    void replace_column_data(const IColumn&, size_t row, size_t self_row) override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "replace_column_data " + get_name());
    }

    std::pair<ColumnString*, ColumnString*> get_sparse_data_paths_and_values() {
        auto& column_map = assert_cast<ColumnMap&>(*serialized_sparse_column);
        auto& key = assert_cast<ColumnString&>(column_map.get_keys());
        auto& value = assert_cast<ColumnString&>(column_map.get_values());
        return {&key, &value};
    }

    std::pair<const ColumnString*, const ColumnString*> get_sparse_data_paths_and_values() const {
        const auto& column_map = assert_cast<const ColumnMap&>(*serialized_sparse_column);
        const auto& key = assert_cast<const ColumnString&>(column_map.get_keys());
        const auto& value = assert_cast<const ColumnString&>(column_map.get_values());
        return {&key, &value};
    }

    ColumnArray::Offsets64& ALWAYS_INLINE serialized_sparse_column_offsets() {
        auto& column_map = assert_cast<ColumnMap&>(*serialized_sparse_column);
        return column_map.get_offsets();
    }

    const ColumnArray::Offsets64& ALWAYS_INLINE serialized_sparse_column_offsets() const {
        const auto& column_map = assert_cast<const ColumnMap&>(*serialized_sparse_column);
        return column_map.get_offsets();
    }
    // Insert all the data from sparse data with specified path to sub column.
    static void fill_path_column_from_sparse_data(Subcolumn& subcolumn, NullMap* null_map,
                                                  StringRef path,
                                                  const ColumnPtr& sparse_data_column, size_t start,
                                                  size_t end);

    static size_t find_path_lower_bound_in_sparse_data(StringRef path,
                                                       const ColumnString& sparse_data_paths,
                                                       size_t start, size_t end);

    // Deserialize the i-th row of the column from the sparse column.
    static std::pair<Field, FieldInfo> deserialize_from_sparse_column(const ColumnString* value,
                                                                      size_t row);

    Status pick_subcolumns_to_sparse_column(
            const std::unordered_map<std::string, TabletSchema::SubColumnInfo>& typed_paths,
            bool variant_enable_typed_paths_to_sparse);

    Status convert_typed_path_to_storage_type(
            const std::unordered_map<std::string, TabletSchema::SubColumnInfo>& typed_paths);

    void set_max_subcolumns_count(int32_t max_subcolumns_count) {
        _max_subcolumns_count = max_subcolumns_count;
    }

    void clear_subcolumns_data();

private:
    // May throw execption
    void try_insert(const Field& field);

    /// It's used to get shared sized of Nested to insert correct default values.
    const Subcolumns::Node* get_leaf_of_the_same_nested(const Subcolumns::NodePtr& entry) const;

    template <typename Func>
    void for_each_imutable_column(Func&& callback) const;

    // return null if not found
    const Subcolumn* get_subcolumn_with_cache(const PathInData& key, size_t index_hint) const;

    // unnest nested type columns, and flat them into finlized array subcolumns
    void unnest(Subcolumns::NodePtr& entry, Subcolumns& subcolumns) const;

    void insert_from_sparse_column_and_fill_remaing_dense_column(
            const ColumnVariant& src,
            std::vector<std::pair<std::string_view, Subcolumn>>&&
                    sorted_src_subcolumn_for_sparse_column,
            size_t start, size_t length);

    bool try_add_new_subcolumn(const PathInData& path);

    bool is_visible_root_value(size_t nrow) const;
};

} // namespace doris::vectorized
