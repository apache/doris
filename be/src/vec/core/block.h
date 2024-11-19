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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Block.h
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <parallel_hashmap/phmap.h>

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <list>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/factory_creator.h"
#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

class SipHash;

namespace doris {

class TupleDescriptor;
class PBlock;
class SlotDescriptor;

namespace segment_v2 {
enum CompressionTypePB : int;
} // namespace segment_v2

namespace vectorized {

/** Container for set of columns for bunch of rows in memory.
  * This is unit of data processing.
  * Also contains metadata - data types of columns and their names
  *  (either original names from a table, or generated names during temporary calculations).
  * Allows to insert, remove columns in arbitrary position, to change order of columns.
  */
class MutableBlock;

class Block {
    ENABLE_FACTORY_CREATOR(Block);

private:
    using Container = ColumnsWithTypeAndName;
    using IndexByName = phmap::flat_hash_map<String, size_t>;
    Container data;
    IndexByName index_by_name;
    std::vector<bool> row_same_bit;

    int64_t _decompress_time_ns = 0;
    int64_t _decompressed_bytes = 0;

    mutable int64_t _compress_time_ns = 0;

public:
    Block() = default;
    Block(std::initializer_list<ColumnWithTypeAndName> il);
    Block(ColumnsWithTypeAndName data_);
    Block(const std::vector<SlotDescriptor*>& slots, size_t block_size,
          bool ignore_trivial_slot = false);

    ~Block() = default;
    Block(const Block& block) = default;
    Block& operator=(const Block& p) = default;
    Block(Block&& block) = default;
    Block& operator=(Block&& other) = default;

    /**
     * Reserves memory for the Block's internal data structures.
     * This method pre-allocates memory for both the index_by_name map and data vector
     * to avoid reallocations when inserting elements up to the specified count.
     *
     * @param count The number of elements to reserve space for
     */
    void reserve(size_t count);
    /**
     * Clears all column names from the Block.
     * This method removes all name mappings from the index and clears the name field 
     * of each column entry, while preserving the actual column data.
     */
    void clear_names();

    /// insert the column at the specified position
    void insert(size_t position, const ColumnWithTypeAndName& elem);
    void insert(size_t position, ColumnWithTypeAndName&& elem);
    /// insert the column to the end
    void insert(const ColumnWithTypeAndName& elem);
    void insert(ColumnWithTypeAndName&& elem);
    /// remove the column at the specified position
    void erase(size_t position);
    /// remove the column at the [start, end)
    void erase_tail(size_t start);
    /// remove the columns at the specified positions
    void erase(const std::set<size_t>& positions);
    /// remove the column with the specified name
    void erase(const String& name);
    // T was std::set<int>, std::vector<int>, std::list<int>
    template <class T>
    void erase_not_in(const T& container) {
        Container new_data;
        for (auto pos : container) {
            new_data.emplace_back(std::move(data[pos]));
        }
        std::swap(data, new_data);
    }

    /**
     * Initializes or rebuilds the name-to-position index mapping for all columns in the Block.
     * Creates a mapping from column names to their positions in the data array.
     * This method is typically called after bulk modifications to the Block's structure.
     */
    void initialize_index_by_name();

    /// References are invalidated after calling functions above.
    ColumnWithTypeAndName& get_by_position(size_t position) {
        DCHECK(data.size() > position)
                << ", data.size()=" << data.size() << ", position=" << position;
        return data[position];
    }
    const ColumnWithTypeAndName& get_by_position(size_t position) const { return data[position]; }

    /**
     * Replace column at specified position with a new column.
     * These are overloaded methods that handle both move and copy semantics.
     * The column type and name remain unchanged, only the column data is replaced.
     *
     * @param position The position of the column to replace
     * @param res The new column to replace with (either by rvalue reference or const reference)
     */
    void replace_by_position(size_t position, ColumnPtr&& res) {
        this->get_by_position(position).column = std::move(res);
    }
    void replace_by_position(size_t position, const ColumnPtr& res) {
        this->get_by_position(position).column = res;
    }

    /**
     * Convert const column at position to full column if it is const.
     * This method checks if the column at the specified position is a const column,
     * and if so, converts it to a full column with actual data.
     *
     * @param position The position of the column to check and potentially convert
     */
    void replace_by_position_if_const(size_t position) {
        auto& element = this->get_by_position(position);
        element.column = element.column->convert_to_full_column_if_const();
    }

    /**
     * Convert all columns to new columns if they overflow.
     * This method checks each column in the block for potential overflow conditions
     * and converts them to appropriate new columns if necessary.
     * The conversion is done in-place using move semantics for efficiency.
     */
    void replace_if_overflow() {
        for (auto& ele : data) {
            ele.column = std::move(*ele.column).mutate()->convert_column_if_overflow();
        }
    }

    // get column by position, throw exception when position is invalid
    ColumnWithTypeAndName& safe_get_by_position(size_t position);
    const ColumnWithTypeAndName& safe_get_by_position(size_t position) const;

    // get column by name, throw exception when no such column name
    ColumnWithTypeAndName& get_by_name(const std::string& name);
    const ColumnWithTypeAndName& get_by_name(const std::string& name) const;

    // get column by name, return nullptr when no such column name
    ColumnWithTypeAndName* try_get_by_name(const std::string& name);
    const ColumnWithTypeAndName* try_get_by_name(const std::string& name) const;

    Container::iterator begin() { return data.begin(); }
    Container::iterator end() { return data.end(); }
    Container::const_iterator begin() const { return data.begin(); }
    Container::const_iterator end() const { return data.end(); }
    Container::const_iterator cbegin() const { return data.cbegin(); }
    Container::const_iterator cend() const { return data.cend(); }

    /**
     * Checks if a column with the specified name exists in the Block.
     * Uses the index_by_name map for efficient lookup.
     *
     * @param name The name of the column to check for
     * @return true if the column exists, false otherwise
     */
    bool has(const std::string& name) const;

    /**
     * Gets the position of a column by its name.
     * Performs a lookup in the index_by_name map and throws an exception if the column is not found.
     *
     * @param name The name of the column to look up
     * @return The position of the column in the Block
     * @throws Exception if the column name is not found
     */
    size_t get_position_by_name(const std::string& name) const;

    /**
     * Returns a const reference to the internal data structure containing all columns with their types and names.
     * Provides direct access to the Block's underlying column data structure.
     *
     * @return Const reference to ColumnsWithTypeAndName container holding all column information
     */
    const ColumnsWithTypeAndName& get_columns_with_type_and_name() const;

    /**
     * Returns a vector containing all column names in the Block.
     * Creates a new vector with the names of all columns in their current order.
     * Pre-reserves space for efficiency.
     *
     * @return Vector of strings containing all column names
     */
    std::vector<std::string> get_names() const;
    /**
     * Returns a vector containing all column data types in the Block.
     * Creates a new vector with the data types of all columns in their current order.
     * Pre-reserves space for efficiency.
     *
     * @return Vector of DataTypePtr containing all column data types
     */
    DataTypes get_data_types() const;

    /**
     * Returns the data type of the column at the specified index.
     * Performs bounds checking using CHECK macro.
     *
     * @param index The index of the column whose data type is requested
     * @return DataTypePtr pointing to the column's data type
     * @throws CHECK failure if index is out of bounds
     */
    DataTypePtr get_data_type(size_t index) const {
        CHECK(index < data.size());
        return data[index].type;
    }

    /**
     * Returns the number of rows in the Block.
     * Finds the first non-null column and returns its size.
     * All columns in a valid Block should have the same number of rows.
     *
     * @return The number of rows in the Block, or 0 if no valid columns exist
     */
    size_t rows() const;

    /**
     * Returns a string representation of each column's size in the Block.
     * Format: "size1 | size2 | size3 | ..."
     * Uses -1 to indicate null columns.
     * Useful for debugging and logging purposes.
     *
     * @return A string showing the size of each column separated by " | "
     */
    std::string each_col_size() const;

    /**
     * Reduces the number of rows in the Block to the specified length.
     * Only performs the operation if the current number of rows is greater than the target length.
     * Shrinks all columns and adjusts row_same_bit vector accordingly.
     *
     * @param length The target number of rows
     * Note: Does nothing if current rows <= length
     */
    void set_num_rows(size_t length);

    /**
     * Skips a specified number of rows from the beginning of the Block.
     * If length >= current rows, clears the entire Block and updates remaining length.
     * Otherwise, removes first 'length' rows and keeps the rest.
     *
     * @param length Input/Output parameter - number of rows to skip, updated with remaining rows to skip
     */
    void skip_num_rows(int64_t& length);

    /**
     * Returns the number of columns in the Block.
     * As the assumption we used around, the number of columns won't exceed int16 range.
     * So no need to worry when we assign it to int32.
     *
     * @return The number of columns in the Block as a uint32_t
     */
    uint32_t columns() const { return static_cast<uint32_t>(data.size()); }

    /**
     * Validates that all non-null columns in the Block have the same number of rows.
     * Checks for consistency in the Block's structure.
     *
     * @param allow_null_columns If true, skips null columns during validation
     * @throws Exception if any column is null (when not allowed) or if column sizes don't match
     */
    void check_number_of_rows(bool allow_null_columns = false) const;

    /**
     * Calculates the total memory size in bytes used by all columns in the Block.
     * Throws an exception if any column is null.
     *
     * @return Total size in bytes of all columns
     * @throws Exception if any column is null, with detailed error message listing all column names
     */
    size_t bytes() const;

    /**
     * Returns a string representation of the memory size of each column in the Block.
     * Format: "column bytes: [,size1, size2, size3, ...]"
     * Throws an exception if any column is null.
     *
     * @return Formatted string showing byte size of each column
     * @throws Exception if any column is null, with detailed error message listing all column names
     */
    std::string columns_bytes() const;

    /**
     * Calculates the total allocated memory in bytes for all non-null columns in the Block.
     * Unlike bytes(), this method:
     * 1. Skips null columns instead of throwing exception
     * 2. Uses allocated_bytes() which may include memory reserved but not used
     *
     * @return Total allocated memory size in bytes for all non-null columns
     */
    size_t allocated_bytes() const;

    /**
     * Returns a comma-separated string of all column names in the Block.
     * Format: "name1, name2, name3, ..."
     * Used for debugging and error reporting.
     *
     * @return String containing all column names separated by commas
     */
    std::string dump_names() const;

    /**
     * Returns a comma-separated string of all column types in the Block.
     * Format: "type1, type2, type3, ..."
     * Used for debugging and error reporting.
     *
     * @return String containing all column data types separated by commas
     */
    std::string dump_types() const;

    /**
     * Returns a detailed string representation of the Block's structure.
     * Each column's structure is dumped on a new line.
     * Format:
     * col1_structure,
     * col2_structure,
     * col3_structure, ...
     *
     * Uses each column's dump_structure() method for detailed information.
     *
     * @return Multi-line string containing detailed structure of all columns
     */
    std::string dump_structure() const;

    /**
     * Creates a new Block with the same structure but no data.
     * Clones all columns as empty columns while preserving types and names.
     * Useful for creating a template Block with the same schema.
     *
     * @return A new Block with same structure but zero rows
     */
    Block clone_empty() const;

    /**
     * Returns all columns in the Block as a vector of Column objects.
     * Converts any const columns to their full (non-const) representation.
     *
     * @return Vector of Column objects, where const columns are converted to full columns
     * Note: The returned columns are shared pointers to the actual column data
     */
    Columns get_columns() const;
    /**
     * Returns all columns and converts const columns to full columns in-place.
     * Unlike get_columns(), this method modifies the original Block's columns.
     *
     * @return Vector of Column objects after converting const columns
     * Note: This method mutates the Block by converting const columns to full columns
     */
    Columns get_columns_and_convert();

    /**
     * Sets the Block's columns to the provided columns.
     * Replaces existing column data while maintaining types and names.
     *
     * @param columns Vector of Column objects to set
     * @throws DCHECK failure if columns.size() < data.size()
     * Note: Only updates column data, keeps original column types and names
     */
    void set_columns(const Columns& columns);
    /**
     * Creates a new Block with given columns while keeping original types and names.
     * These are overloaded methods that handle both move and copy semantics.
     *
     * @param columns Columns to use in new Block
     * @return New Block with provided columns and original metadata
     * @throws FATAL error if column count mismatch (only in const version)
     */
    Block clone_with_columns(const Columns& columns) const;
    Block clone_with_columns(MutableColumns&& columns) const;

    /**
     * Creates a new Block with null columns but preserving types and names.
     * Optionally allows selecting specific columns using column_offset.
     *
     * @param column_offset Optional vector of column indices to include
     *                     If null, all columns are included
     * @return New Block with null columns but original metadata
     * Note: Resulting Block has same structure but no actual column data
     */
    Block clone_without_columns(const std::vector<int>* column_offset = nullptr) const;

    /**
     * Creates empty mutable columns with same structure as current Block.
     * For each column:
     * - If source column exists: creates empty clone
     * - If source column is null: creates new empty column of same type
     *
     * @return Vector of empty MutableColumns matching Block's structure
     * Note: Returns independent columns that can be modified without affecting original Block
     */
    MutableColumns clone_empty_columns() const;

    /**
     * Creates mutable columns from current Block's columns.
     * For each column:
     * - If source column exists: moves and mutates it
     * - If source column is null: creates new empty column
     *
     * @return Vector of MutableColumns
     * Note: This method modifies the original Block by moving out its columns
     * Note: Returns columns that can be modified independently
     */
    MutableColumns mutate_columns();

    /**
     * Sets Block's columns using provided MutableColumns.
     * Moves ownership of columns into Block.
     *
     * @param columns MutableColumns to move into Block
     * @throws DCHECK failure if columns.size() < data.size()
     * Note: Uses move semantics to avoid copying
     * Note: Original columns vector will be modified (moved from)
     */
    void set_columns(MutableColumns&& columns);

    /**
     * Creates a new Block with columns sorted by their names.
     * Uses index_by_name map to determine column order.
     *
     * @return New Block with same columns but sorted by column names
     * Note: Original Block remains unchanged
     * Note: Column data is shared with original Block
     */
    Block sort_columns() const;

    void clear();
    void swap(Block& other) noexcept;
    void swap(Block&& other) noexcept;

    /**
     * Shuffle columns in place based on the result_column_ids.
     * Creates a new temporary Block with reordered columns and swaps with current Block.
     *
     * @param result_column_ids Vector of column indices specifying new order
     * Note: Modifies current Block's column order
     * Note: Only keeps columns specified in result_column_ids
     */
    void shuffle_columns(const std::vector<int>& result_column_ids);

    /**
     * Clears all column data in the Block and optionally removes excess columns.
     *
     * @param column_size If >= 0, removes all columns beyond this index
     *                   If -1, keeps all columns but clears their data
     *
     * Note: Skips memory checking during operation
     * Note: Verifies each column has single reference before clearing
     * Note: Clears row_same_bit vector
     * Note: Uses move semantics for efficiency
     */
    void clear_column_data(int64_t column_size = -1) noexcept;

    /**
     * Checks if the block is not empty.
     *
     * @return True if the block is not empty, false otherwise
     */
    bool mem_reuse() { return !data.empty(); }

    /**
     * Checks if the block has no columns.
     *
     * @return True if the block has no columns, false otherwise
     */
    bool is_empty_column() { return data.empty(); }

    /**
     * Checks if the block has no rows (i.e. all columns have 0 rows).
     * This is different from is_empty_column() which checks for absence of columns.
     *
     * @return True if the block has no rows, false otherwise
     */
    bool empty() const { return rows() == 0; }

    /**
     * Updates SipHash of the Block, using update method of columns.
     * Returns hash for block, that could be used to differentiate blocks
     * with same structure, but different data.
     */
    void update_hash(SipHash& hash) const;

    /**
     * Get block data in string.
     * If code is in default_implementation_for_nulls or something likely, type and column's nullity could
     * temporarily be not same. set allow_null_mismatch to true to dump it correctly.
     */
    std::string dump_data(size_t begin = 0, size_t row_limit = 100,
                          bool allow_null_mismatch = false) const;

    static std::string dump_column(ColumnPtr col, DataTypePtr type) {
        ColumnWithTypeAndName type_name {col, type, ""};
        Block b {type_name};
        return b.dump_data(0, b.rows());
    }

    // get one line data from block, only use in load data
    std::string dump_one_line(size_t row, int column_end) const;

    // copy a new block by the offset column
    Block copy_block(const std::vector<int>& column_offset) const;

    /**
     * Appends selected rows from this Block to destination MutableBlock based on selector.
     *
     * @param dst Destination MutableBlock to append data to
     * @param selector Vector specifying which rows to append
     * @return Status::OK() if successful, error status otherwise
     *
     * Note: Skips const columns during append
     * Note: Requires dst to have same number of columns as source
     * Note: Uses RETURN_IF_CATCH_EXCEPTION for error handling
     */
    Status append_to_block_by_selector(MutableBlock* dst, const IColumn::Selector& selector) const;

    // need exception safety
    static void filter_block_internal(Block* block, const std::vector<uint32_t>& columns_to_filter,
                                      const IColumn::Filter& filter);
    // need exception safety
    static void filter_block_internal(Block* block, const IColumn::Filter& filter,
                                      uint32_t column_to_keep);
    // need exception safety
    static void filter_block_internal(Block* block, const IColumn::Filter& filter);

    /**
     * Filters block columns based on a filter column and specified columns to filter.
     * Handles three types of filter columns:
     * 1. Nullable columns - combines null map with filter
     * 2. Const columns - clears all data if false
     * 3. Regular UInt8 columns - uses directly as filter
     *
     * @param block Block to filter
     * @param columns_to_filter Vector of column indices to apply filter to
     * @param filter_column_id Index of column containing filter
     * @param column_to_keep Number of columns to keep
     * @return Status::OK() if successful, error status otherwise
     *
     * Note: Modifies block in place
     * Note: Removes unnecessary columns after filtering
     */
    static Status filter_block(Block* block, const std::vector<uint32_t>& columns_to_filter,
                               size_t filter_column_id, size_t column_to_keep);

    /**
     * Simplified version of filter_block that filters first N columns.
     * Creates a vector of column indices [0, column_to_keep) and calls main filter_block.
     *
     * @param block Block to filter
     * @param filter_column_id Index of column containing filter
     * @param column_to_keep Number of columns to keep
     * @return Status::OK() if successful, error status otherwise
     *
     * Note: Convenience wrapper around main filter_block method
     * Note: Filters columns in order from 0 to column_to_keep-1
     */
    static Status filter_block(Block* block, size_t filter_column_id, size_t column_to_keep);

    /**
     * Remove columns after column_to_keep from the Block.
     * Static helper method that wraps Block::erase_tail.
     *
     * @param block Pointer to Block to modify
     * @param column_to_keep Number of columns to keep (removes all columns after this index)
     *
     * Note: Modifies block in place
     * Note: Removes all columns with index >= column_to_keep
     */
    static void erase_useless_column(Block* block, size_t column_to_keep) {
        block->erase_tail(column_to_keep);
    }

    /**
     * Serializes Block data into a PBlock (Protocol Buffer) format with optional compression.
     *
     * @param be_exec_version Backend execution version
     * @param pblock Output Protocol Buffer block
     * @param uncompressed_bytes Output parameter for uncompressed size
     * @param compressed_bytes Output parameter for compressed size
     * @param compression_type Type of compression to use
     * @param allow_transfer_large_data Whether to allow blocks larger than 2GB
     *
     * @return Status::OK() if successful, error status otherwise
     *
     * Process:
     * 1. Validates version and calculates uncompressed size
     * 2. Serializes column metadata and data
     * 3. Optionally compresses the data
     * 4. Sets appropriate PBlock fields
     *
     * Note: Handles memory allocation failures
     * Note: Supports various compression types
     * Note: Has 2GB size limit by default
     */
    Status serialize(int be_exec_version, PBlock* pblock, size_t* uncompressed_bytes,
                     size_t* compressed_bytes, segment_v2::CompressionTypePB compression_type,
                     bool allow_transfer_large_data = false) const;

    /**
     * Deserializes a PBlock (Protocol Buffer) into this Block.
     * Handles both compressed and uncompressed data.
     *
     * @param pblock Protocol Buffer block to deserialize
     * @return Status::OK() if successful, error status otherwise
     *
     * Process:
     * 1. Clears current Block data
     * 2. Handles version compatibility
     * 3. Decompresses data if needed
     * 4. Deserializes column metadata and data
     * 5. Rebuilds column index
     *
     * Supports:
     * - Multiple compression types
     * - Legacy snappy compression
     * - Version compatibility
     * - Memory allocation failure handling
     */
    Status deserialize(const PBlock& pblock);

    /**
     * Creates a new Block with same structure but different size.
     *
     * @param size Number of rows for the new Block
     * @param is_reserve If true, reserves space; if false, fills with default values
     * @return Unique pointer to new Block with same structure
     *
     * Creates a Block that:
     * - Has same column types and names
     * - Either reserves space or contains default values
     * - Is independent from original Block
     *
     * Note: Returns unique_ptr for automatic memory management
     * Note: Column data is not copied, only structure is preserved
     */
    std::unique_ptr<Block> create_same_struct_block(size_t size, bool is_reserve = false) const;

    /** Compares (*this) n-th row and rhs m-th row.
      * Returns negative number, 0, or positive number  (*this) n-th row is less, equal, greater than rhs m-th row respectively.
      * Is used in sortings.
      *
      * If one of element's value is NaN or NULLs, then:
      * - if nan_direction_hint == -1, NaN and NULLs are considered as least than everything other;
      * - if nan_direction_hint ==  1, NaN and NULLs are considered as greatest than everything other.
      * For example, if nan_direction_hint == -1 is used by descending sorting, NaNs will be at the end.
      *
      * For non Nullable and non floating point types, nan_direction_hint is ignored.
      */
    int compare_at(size_t n, size_t m, const Block& rhs, int nan_direction_hint) const {
        DCHECK_EQ(columns(), rhs.columns());
        return compare_at(n, m, columns(), rhs, nan_direction_hint);
    }

    /**
     * Compares rows by first num_columns columns in sequential order.
     *
     * @param n Row index in this Block
     * @param m Row index in rhs Block
     * @param num_columns Number of columns to compare
     * @param rhs Block to compare against
     * @param nan_direction_hint Direction for NaN comparison (-1: NaN is smallest, 1: NaN is largest)
     * @return -1 if this < rhs, 0 if equal, 1 if this > rhs
     *
     * Checks:
     * - Both blocks have enough columns
     * - Row indices are valid
     * - Column types match
     *
     * Note: Compares columns sequentially until difference found
     * Note: Returns 0 if all specified columns are equal
     */
    int compare_at(size_t n, size_t m, size_t num_columns, const Block& rhs,
                   int nan_direction_hint) const {
        DCHECK_GE(columns(), num_columns);
        DCHECK_GE(rhs.columns(), num_columns);

        DCHECK_LE(n, rows());
        DCHECK_LE(m, rhs.rows());
        for (size_t i = 0; i < num_columns; ++i) {
            DCHECK(get_by_position(i).type->equals(*rhs.get_by_position(i).type));
            auto res = get_by_position(i).column->compare_at(n, m, *(rhs.get_by_position(i).column),
                                                             nan_direction_hint);
            if (res) {
                return res;
            }
        }
        return 0;
    }

    /**
     * Compare rows by specified columns in compare_columns vector.
     *
     * @param n Row index in this Block
     * @param m Row index in rhs Block
     * @param compare_columns Vector of column indices to compare
     * @param rhs Block to compare against
     * @param nan_direction_hint Direction for NaN comparison (-1: NaN is smallest, 1: NaN is largest)
     * @return -1 if this < rhs, 0 if equal, 1 if this > rhs
     *
     * Checks:
     * - Both blocks have enough columns
     * - Row indices are valid
     * - Column types match for specified columns
     *
     * Note: Compares only columns specified in compare_columns
     * Note: Returns 0 if all specified columns are equal
     */
    int compare_at(size_t n, size_t m, const std::vector<uint32_t>* compare_columns,
                   const Block& rhs, int nan_direction_hint) const {
        DCHECK_GE(columns(), compare_columns->size());
        DCHECK_GE(rhs.columns(), compare_columns->size());

        DCHECK_LE(n, rows());
        DCHECK_LE(m, rhs.rows());
        for (auto i : *compare_columns) {
            DCHECK(get_by_position(i).type->equals(*rhs.get_by_position(i).type));
            auto res = get_by_position(i).column->compare_at(n, m, *(rhs.get_by_position(i).column),
                                                             nan_direction_hint);
            if (res) {
                return res;
            }
        }
        return 0;
    }

    /**
     * Compare single column values between two blocks at specified row positions.
     *
     * @param n Row index in this Block
     * @param m Row index in rhs Block
     * @param col_idx Index of column to compare
     * @param rhs Block to compare against
     * @param nan_direction_hint Direction for NaN comparison (-1: NaN is smallest, 1: NaN is largest)
     * @return -1 if this < rhs, 0 if equal, 1 if this > rhs
     *
     * Note: No DCHECK here as this method is typically called after compare_at
     * Note: Assumes column types match and indices are valid
     * Note: Direct wrapper around column's compare_at method
     */
    int compare_column_at(size_t n, size_t m, size_t col_idx, const Block& rhs,
                          int nan_direction_hint) const {
        auto res = get_by_position(col_idx).column->compare_at(
                n, m, *(rhs.get_by_position(col_idx).column), nan_direction_hint);
        return res;
    }

    // for String type or Array<String> type
    void shrink_char_type_column_suffix_zero(const std::vector<size_t>& char_type_idx);

    int64_t get_decompress_time() const { return _decompress_time_ns; }
    int64_t get_decompressed_bytes() const { return _decompressed_bytes; }
    int64_t get_compress_time() const { return _compress_time_ns; }

    /**
     * Set same bit flags for rows in block.
     *
     * @param begin Iterator to start of source same bits
     * @param end Iterator to end of source same bits
     *
     * Note: Appends same bits to row_same_bit vector
     * Note: Verifies final size matches number of rows
     * Note: Used to track which rows are identical
     * Note: Important for optimization in data processing
     */
    void set_same_bit(std::vector<bool>::const_iterator begin,
                      std::vector<bool>::const_iterator end) {
        row_same_bit.insert(row_same_bit.end(), begin, end);

        DCHECK_EQ(row_same_bit.size(), rows());
    }

    /**
     * Get same bit flag for specified row position.
     *
     * @param position Row index to check
     * @return true if row is marked as same, false if different or position invalid
     *
     * Note: Returns false for out of range positions
     * Note: Used to check if row is identical to previous
     * Note: Part of row deduplication optimization
     * Note: Safe access with bounds checking
     */
    bool get_same_bit(size_t position) {
        if (position >= row_same_bit.size()) {
            return false;
        }
        return row_same_bit[position];
    }

    /**
     * Clear all same bit flags.
     *
     * Note: Resets all flags to false
     * Note: Used to reset row deduplication tracking
     */
    void clear_same_bit() { row_same_bit.clear(); }

    // return string contains use_count() of each columns
    // for debug purpose.
    std::string print_use_count();

    // remove tmp columns in block
    // in inverted index apply logic, in order to optimize query performance,
    // we built some temporary columns into block
    void erase_tmp_columns() noexcept;

    // Clear columns not marked for keeping
    void clear_column_mem_not_keep(const std::vector<bool>& column_keep_flags,
                                   bool need_keep_first);

private:
    void erase_impl(size_t position);
};

using Blocks = std::vector<Block>;
using BlocksList = std::list<Block>;
using BlocksPtr = std::shared_ptr<Blocks>;
using BlocksPtrs = std::shared_ptr<std::vector<BlocksPtr>>;

class MutableBlock {
    ENABLE_FACTORY_CREATOR(MutableBlock);

private:
    MutableColumns _columns;
    DataTypes _data_types;
    std::vector<std::string> _names;

    using IndexByName = phmap::flat_hash_map<String, size_t>;
    IndexByName index_by_name;

public:
    /**
     * Static factory method to create a MutableBlock from a Block pointer.
     *
     * @param block Pointer to source Block, can be nullptr
     * @return MutableBlock instance
     * - Empty MutableBlock if input is nullptr
     * - MutableBlock containing source Block's data if input is valid
     *
     * Note: Handles nullptr gracefully
     * Note: Uses MutableBlock constructor for valid input
     * Note: Convenient way to create MutableBlock
     * Note: Safe conversion from Block to MutableBlock
     */
    static MutableBlock build_mutable_block(Block* block) {
        return block == nullptr ? MutableBlock() : MutableBlock(block);
    }
    MutableBlock() = default;
    ~MutableBlock() = default;

    MutableBlock(const std::vector<TupleDescriptor*>& tuple_descs, int reserve_size = 0,
                 bool igore_trivial_slot = false);

    MutableBlock(Block* block)
            : _columns(block->mutate_columns()),
              _data_types(block->get_data_types()),
              _names(block->get_names()) {
        initialize_index_by_name();
    }
    MutableBlock(Block&& block)
            : _columns(block.mutate_columns()),
              _data_types(block.get_data_types()),
              _names(block.get_names()) {
        initialize_index_by_name();
    }

    void operator=(MutableBlock&& m_block) {
        _columns = std::move(m_block._columns);
        _data_types = std::move(m_block._data_types);
        _names = std::move(m_block._names);
        initialize_index_by_name();
    }
    /**
     * Get number of rows in MutableBlock.
     * Returns size of first non-null column, or 0 if all columns are null.
     *
     * @return Number of rows in block
     *
     * Note: All non-null columns should have same number of rows
     * Note: Returns 0 for empty block or all null columns
     */
    size_t rows() const;
    /**
     * Get number of columns in MutableBlock.
     *
     * @return Number of columns in block
     */
    size_t columns() const { return _columns.size(); }

    /**
     * Check if MutableBlock is empty.
     *
     * @return true if no rows, false otherwise
     *
     * Note: Simple check for zero rows
     */
    bool empty() const { return rows() == 0; }

    /**
     * Get mutable columns of MutableBlock.
     *
     * @return Reference to mutable columns vector
     */
    MutableColumns& mutable_columns() { return _columns; }

    /**
     * Set mutable columns of MutableBlock.
     *
     * @param columns MutableColumns to set
     */
    void set_mutable_columns(MutableColumns&& columns) { _columns = std::move(columns); }

    /**
     * Get data types of MutableBlock.
     *
     * @return Reference to data types vector
     */
    DataTypes& data_types() { return _data_types; }

    /**
     * Get column by position.
     *
     * @param position Column index to get
     * @return Reference to mutable column pointer
     */
    MutableColumnPtr& get_column_by_position(size_t position) { return _columns[position]; }
    const MutableColumnPtr& get_column_by_position(size_t position) const {
        return _columns[position];
    }

    /**
     * Get data type by position.
     *
     * @param position Column index to get
     * @return Reference to data type pointer
     */
    DataTypePtr& get_datatype_by_position(size_t position) { return _data_types[position]; }
    const DataTypePtr& get_datatype_by_position(size_t position) const {
        return _data_types[position];
    }

    /**
     * Compare values in a single column between two rows in the same block.
     *
     * @param n First row index to compare
     * @param m Second row index to compare
     * @param column_id Column index to compare
     * @param nan_direction_hint Direction for NaN comparison (-1: NaN is smallest, 1: NaN is largest)
     * @return -1 if row n < row m, 0 if equal, 1 if row n > row m
     *
     * Note: Checks indices validity with DCHECK
     * Note: Compares values within the same column
     * Note: Handles NaN values according to direction hint
     * Note: const method guarantees no modification
     */
    int compare_one_column(size_t n, size_t m, size_t column_id, int nan_direction_hint) const {
        DCHECK_LE(column_id, columns());
        DCHECK_LE(n, rows());
        DCHECK_LE(m, rows());
        auto& column = get_column_by_position(column_id);
        return column->compare_at(n, m, *column, nan_direction_hint);
    }

    /**
     * Compare rows by first num_columns columns in sequential order.
     *
     * @param n Row index in this MutableBlock
     * @param m Row index in rhs MutableBlock
     * @param num_columns Number of columns to compare
     * @param rhs MutableBlock to compare against
     * @param nan_direction_hint Direction for NaN comparison (-1: NaN is smallest, 1: NaN is largest)
     * @return -1 if this < rhs, 0 if equal, 1 if this > rhs
     *
     * Checks:
     * - Both blocks have enough columns
     * - Row indices are valid
     * - Column types match
     *
     * Note: Compares columns sequentially until difference found
     * Note: Returns 0 if all specified columns are equal
     */
    int compare_at(size_t n, size_t m, size_t num_columns, const MutableBlock& rhs,
                   int nan_direction_hint) const {
        DCHECK_GE(columns(), num_columns);
        DCHECK_GE(rhs.columns(), num_columns);

        DCHECK_LE(n, rows());
        DCHECK_LE(m, rhs.rows());
        for (size_t i = 0; i < num_columns; ++i) {
            DCHECK(get_datatype_by_position(i)->equals(*rhs.get_datatype_by_position(i)));
            auto res = get_column_by_position(i)->compare_at(n, m, *(rhs.get_column_by_position(i)),
                                                             nan_direction_hint);
            if (res) {
                return res;
            }
        }
        return 0;
    }

    /**
     * Compare rows by specified columns in compare_columns vector.
     *
     * @param n Row index in this MutableBlock
     * @param m Row index in rhs MutableBlock
     * @param compare_columns Vector of column indices to compare
     * @param rhs MutableBlock to compare against
     * @param nan_direction_hint Direction for NaN comparison (-1: NaN is smallest, 1: NaN is largest)
     * @return -1 if this < rhs, 0 if equal, 1 if this > rhs
     *
     * Checks:
     * - Both blocks have enough columns
     * - Row indices are valid
     * - Column types match for specified columns
     *
     * Note: Compares only columns specified in compare_columns
     * Note: Returns 0 if all specified columns are equal
     */
    int compare_at(size_t n, size_t m, const std::vector<uint32_t>* compare_columns,
                   const MutableBlock& rhs, int nan_direction_hint) const {
        DCHECK_GE(columns(), compare_columns->size());
        DCHECK_GE(rhs.columns(), compare_columns->size());

        DCHECK_LE(n, rows());
        DCHECK_LE(m, rhs.rows());
        for (auto i : *compare_columns) {
            DCHECK(get_datatype_by_position(i)->equals(*rhs.get_datatype_by_position(i)));
            auto res = get_column_by_position(i)->compare_at(n, m, *(rhs.get_column_by_position(i)),
                                                             nan_direction_hint);
            if (res) {
                return res;
            }
        }
        return 0;
    }

    /**
     * Get a string representation of the block's data types.
     *
     * @return Comma-separated string of column data type names
     *
     * Note: Creates human-readable format
     * Note: Adds commas between type names
     * Note: No trailing comma
     * Note: Empty string for empty block
     */
    std::string dump_types() const {
        std::string res;
        for (auto type : _data_types) {
            if (res.size()) {
                res += ", ";
            }
            res += type->get_name();
        }
        return res;
    }

    template <typename T>
    [[nodiscard]] Status merge(T&& block) {
        RETURN_IF_CATCH_EXCEPTION(return merge_impl(block););
    }

    template <typename T>
    [[nodiscard]] Status merge_ignore_overflow(T&& block) {
        RETURN_IF_CATCH_EXCEPTION(return merge_impl_ignore_overflow(block););
    }

    // only use for join. call ignore_overflow to prevent from throw exception in join
    template <typename T>
    [[nodiscard]] Status merge_impl_ignore_overflow(T&& block) {
        if (_columns.size() != block.columns()) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "Merge block not match, self:[columns: {}, types: {}], input:[columns: {}, "
                    "types: {}], ",
                    dump_names(), dump_types(), block.dump_names(), block.dump_types());
        }
        for (int i = 0; i < _columns.size(); ++i) {
            DCHECK(_data_types[i]->equals(*block.get_by_position(i).type))
                    << " target type: " << _data_types[i]->get_name()
                    << " src type: " << block.get_by_position(i).type->get_name();
            _columns[i]->insert_range_from_ignore_overflow(
                    *block.get_by_position(i).column->convert_to_full_column_if_const().get(), 0,
                    block.rows());
        }
        return Status::OK();
    }

    // Merge another block into current block with strict type check and overflow handling.
    template <typename T>
    [[nodiscard]] Status merge_impl(T&& block) {
        // merge is not supported in dynamic block
        if (_columns.size() == 0 && _data_types.size() == 0) {
            _data_types = block.get_data_types();
            _names = block.get_names();
            _columns.resize(block.columns());
            for (size_t i = 0; i < block.columns(); ++i) {
                if (block.get_by_position(i).column) {
                    _columns[i] = (*std::move(block.get_by_position(i)
                                                      .column->convert_to_full_column_if_const()))
                                          .mutate();
                } else {
                    _columns[i] = _data_types[i]->create_column();
                }
            }
            initialize_index_by_name();
        } else {
            if (_columns.size() != block.columns()) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "Merge block not match, self:[columns: {}, types: {}], input:[columns: {}, "
                        "types: {}], ",
                        dump_names(), dump_types(), block.dump_names(), block.dump_types());
            }
            for (int i = 0; i < _columns.size(); ++i) {
                if (!_data_types[i]->equals(*block.get_by_position(i).type)) {
                    DCHECK(_data_types[i]->is_nullable())
                            << " target type: " << _data_types[i]->get_name()
                            << " src type: " << block.get_by_position(i).type->get_name();
                    DCHECK(((DataTypeNullable*)_data_types[i].get())
                                   ->get_nested_type()
                                   ->equals(*block.get_by_position(i).type));
                    DCHECK(!block.get_by_position(i).type->is_nullable());
                    _columns[i]->insert_range_from(*make_nullable(block.get_by_position(i).column)
                                                            ->convert_to_full_column_if_const(),
                                                   0, block.rows());
                } else {
                    _columns[i]->insert_range_from(
                            *block.get_by_position(i)
                                     .column->convert_to_full_column_if_const()
                                     .get(),
                            0, block.rows());
                }
            }
        }
        return Status::OK();
    }

    /**
     * Convert MutableBlock to Block, optionally selecting a range of columns.
     *
     * First overload:
     * @param start_column Starting column index
     * @param end_column Ending column index (exclusive)
     * @return Block containing specified range of columns
     *
     * Note: Moves column data to new Block
     * Note: Original MutableBlock columns become invalid in range
     * Note: Preserves column types and names
     */
    Block to_block(int start_column = 0);
    Block to_block(int start_column, int end_column);

    /**
     * Swap contents with another MutableBlock.
     *
     * @param other MutableBlock to swap with
     * Swaps all members between blocks using std::swap
     *
     * Note: Both are noexcept operations
     * Note: SCOPED_SKIP_MEMORY_CHECK disables memory tracking
     * Note: Efficiently transfers ownership of resources
     * Note: Complete swap/move of all internal structures
     */
    void swap(MutableBlock& other) noexcept;
    void swap(MutableBlock&& other) noexcept;

    /**
     * Add a single row from source Block to this MutableBlock.
     *
     * @param block Pointer to source Block
     * @param row Index of row to copy from source Block
     *
     * Note: Assumes compatible column structure between blocks
     * Note: Copies data from specified row for all columns
     * Note: Performs column-wise insertion
     * Note: No size checks or type validation
     *
     * Important:
     * - Caller must ensure block is not null
     * - Caller must ensure row index is valid
     * - Caller must ensure column types match
     * - Caller must ensure column counts match
     */
    void add_row(const Block* block, int row);
    // Batch add row should return error status if allocate memory failed.
    Status add_rows(const Block* block, const uint32_t* row_begin, const uint32_t* row_end,
                    const std::vector<int>* column_offset = nullptr);
    Status add_rows(const Block* block, size_t row_begin, size_t length);
    Status add_rows(const Block* block, const std::vector<int64_t>& rows);

    /**
     * Remove a column by name from the MutableBlock.
     *
     * @param name Name of column to remove
     * @throws Exception if column name not found
     */
    void erase(const String& name);

    /**
     * Generate a formatted string representation of the MutableBlock data.
     *
     * @param row_limit Maximum number of rows to display
     * @return Formatted string with ASCII table representation
     *
     * Format:
     * +------+------+------+
     * | Col1 | Col2 | Col3 |
     * +------+------+------+
     * | val1 | val2 | val3 |
     * +------+------+------+
     *
     * Features:
     * - Column headers with data types
     * - Fixed width columns (min 15 chars)
     * - Truncates long values with ...
     * - Shows row count if limited
     * - Handles empty columns
     *
     * Note: Used for debugging and data inspection
     * Note: Formats data in a readable table structure
     */
    std::string dump_data(size_t row_limit = 100) const;

    // Clear the block's data
    void clear() {
        _columns.clear();
        _data_types.clear();
        _names.clear();
    }

    // columns resist. columns' inner data removed.
    void clear_column_data() noexcept;
    // reset columns by types and names.
    void reset_column_data() noexcept;

    /**
     * Calculate total memory allocated by all columns in MutableBlock.
     *
     * @return Total number of bytes allocated
     *
     * Features:
     * - Sums allocated memory across all columns
     * - Skips null columns
     * - Includes both data and metadata memory
     * - const method for safe memory inspection
     *
     * Note: Used for memory tracking and optimization
     * Note: Only counts valid columns
     * Note: Delegates to column's allocated_bytes()
     * Note: Important for memory management
     */
    size_t allocated_bytes() const;

    /**
     * Calculate approximate memory usage of all columns in MutableBlock.
     *
     * @return Approximate total bytes used by all columns
     *
     * Features:
     * - Returns actual data size without allocations
     * - Sums byte_size() of all columns
     * - Quick memory usage estimation
     * - const method for safe inspection
     *
     * Note: Different from allocated_bytes() which includes allocations
     * Note: More lightweight than allocated_bytes()
     * Note: Used for size estimation
     * Note: Does not skip null columns
     */
    size_t bytes() const {
        size_t res = 0;
        for (const auto& elem : _columns) {
            res += elem->byte_size();
        }

        return res;
    }

    /**
     * Get the names of the columns in the block.
     *
     * @return Reference to the vector of column names
     */
    std::vector<std::string>& get_names() { return _names; }

    /**
     * Checks if a column with the specified name exists in the Block.
     * Uses the index_by_name map for efficient lookup.
     *
     * @param name The name of the column to check for
     * @return true if the column exists, false otherwise
     */
    bool has(const std::string& name) const;

    /**
     * Get column position by column name.
     *
     * @param name Column name to look up
     * @return Zero-based position index of the column
     * @throws Exception if column name not found
     */
    size_t get_position_by_name(const std::string& name) const;

    /**
     * Get a list of column names separated by commas.
     *
     * @return Comma-separated string of column names
     *
     * Note: Joins all column names with commas
     * Note: Useful for debugging and logging
     */
    std::string dump_names() const;

private:
    // Initialize the index by name map
    void initialize_index_by_name();
};

struct IteratorRowRef {
    std::shared_ptr<Block> block;
    int row_pos;
    bool is_same;

    // Compare rows by specified arguments
    template <typename T>
    int compare(const IteratorRowRef& rhs, const T& compare_arguments) const {
        return block->compare_at(row_pos, rhs.row_pos, compare_arguments, *rhs.block, -1);
    }

    // Reset the IteratorRowRef to default values
    void reset() {
        block = nullptr;
        row_pos = -1;
        is_same = false;
    }
};

using BlockView = std::vector<IteratorRowRef>;
using BlockUPtr = std::unique_ptr<Block>;

} // namespace vectorized
} // namespace doris
