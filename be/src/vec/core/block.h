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
#include <stddef.h>
#include <stdint.h>

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
    Block(const ColumnsWithTypeAndName& data_);
    Block(const std::vector<SlotDescriptor*>& slots, size_t block_size,
          bool ignore_trivial_slot = false);

    virtual ~Block() = default;
    Block(const Block& block) = default;
    Block& operator=(const Block& p) = default;
    Block(Block&& block) = default;
    Block& operator=(Block&& other) = default;

    void reserve(size_t count);
    // Make sure the nammes is useless when use block
    void clear_names();

    /// insert the column at the specified position
    void insert(size_t position, const ColumnWithTypeAndName& elem);
    void insert(size_t position, ColumnWithTypeAndName&& elem);
    /// insert the column to the end
    void insert(const ColumnWithTypeAndName& elem);
    void insert(ColumnWithTypeAndName&& elem);
    /// insert the column to the end, if there is no column with that name yet
    void insert_unique(const ColumnWithTypeAndName& elem);
    void insert_unique(ColumnWithTypeAndName&& elem);
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

    void initialize_index_by_name();

    /// References are invalidated after calling functions above.
    ColumnWithTypeAndName& get_by_position(size_t position) {
        DCHECK(data.size() > position)
                << ", data.size()=" << data.size() << ", position=" << position;
        return data[position];
    }
    const ColumnWithTypeAndName& get_by_position(size_t position) const { return data[position]; }

    // need exception safety
    Status copy_column_data_to_block(doris::vectorized::IColumn* input_col_ptr,
                                     uint16_t* sel_rowid_idx, uint16_t select_size, int block_cid,
                                     size_t batch_size) {
        // Only the additional deleted filter condition need to materialize column be at the end of the block
        // We should not to materialize the column of query engine do not need. So here just return OK.
        // Eg:
        //      `delete from table where a = 10;`
        //      `select b from table;`
        // a column only effective in segment iterator, the block from query engine only contain the b column.
        // so the `block_cid >= data.size()` is true
        if (block_cid >= data.size()) {
            return Status::OK();
        }

        MutableColumnPtr raw_res_ptr = this->get_by_position(block_cid).column->assume_mutable();
        raw_res_ptr->reserve(batch_size);

        // adapt for outer join change column to nullable
        if (raw_res_ptr->is_nullable() && !input_col_ptr->is_nullable()) {
            auto col_ptr_nullable =
                    reinterpret_cast<vectorized::ColumnNullable*>(raw_res_ptr.get());
            col_ptr_nullable->get_null_map_column().insert_many_defaults(select_size);
            raw_res_ptr = col_ptr_nullable->get_nested_column_ptr();
        }

        return input_col_ptr->filter_by_selector(sel_rowid_idx, select_size, raw_res_ptr);
    }

    void replace_by_position(size_t position, ColumnPtr&& res) {
        this->get_by_position(position).column = std::move(res);
    }

    void replace_by_position(size_t position, const ColumnPtr& res) {
        this->get_by_position(position).column = res;
    }

    void replace_by_position_if_const(size_t position) {
        auto& element = this->get_by_position(position);
        element.column = element.column->convert_to_full_column_if_const();
    }

    ColumnWithTypeAndName& safe_get_by_position(size_t position);
    const ColumnWithTypeAndName& safe_get_by_position(size_t position) const;

    ColumnWithTypeAndName& get_by_name(const std::string& name);
    const ColumnWithTypeAndName& get_by_name(const std::string& name) const;

    // return nullptr when no such column name
    ColumnWithTypeAndName* try_get_by_name(const std::string& name);
    const ColumnWithTypeAndName* try_get_by_name(const std::string& name) const;

    Container::iterator begin() { return data.begin(); }
    Container::iterator end() { return data.end(); }
    Container::const_iterator begin() const { return data.begin(); }
    Container::const_iterator end() const { return data.end(); }
    Container::const_iterator cbegin() const { return data.cbegin(); }
    Container::const_iterator cend() const { return data.cend(); }

    bool has(const std::string& name) const;

    size_t get_position_by_name(const std::string& name) const;

    const ColumnsWithTypeAndName& get_columns_with_type_and_name() const;

    std::vector<std::string> get_names() const;
    DataTypes get_data_types() const;

    DataTypePtr get_data_type(size_t index) const {
        CHECK(index < data.size());
        return data[index].type;
    }

    /// Returns number of rows from first column in block, not equal to nullptr. If no columns, returns 0.
    size_t rows() const;

    std::string each_col_size() const;

    // Cut the rows in block, use in LIMIT operation
    void set_num_rows(size_t length);

    // Skip the rows in block, use in OFFSET, LIMIT operation
    void skip_num_rows(int64_t& offset);

    size_t columns() const { return data.size(); }

    /// Checks that every column in block is not nullptr and has same number of elements.
    void check_number_of_rows(bool allow_null_columns = false) const;

    /// Approximate number of bytes in memory - for profiling and limits.
    size_t bytes() const;

    /// Approximate number of allocated bytes in memory - for profiling and limits.
    size_t allocated_bytes() const;

    /** Get a list of column names separated by commas. */
    std::string dump_names() const;

    std::string dump_types() const;

    /** List of names, types and lengths of columns. Designed for debugging. */
    std::string dump_structure() const;

    /** Get the same block, but empty. */
    Block clone_empty() const;

    Columns get_columns() const;
    void set_columns(const Columns& columns);
    Block clone_with_columns(const Columns& columns) const;
    Block clone_without_columns() const;

    /** Get empty columns with the same types as in block. */
    MutableColumns clone_empty_columns() const;

    /** Get columns from block for mutation. Columns in block will be nullptr. */
    MutableColumns mutate_columns();

    /** Replace columns in a block */
    void set_columns(MutableColumns&& columns);
    Block clone_with_columns(MutableColumns&& columns) const;

    /** Get a block with columns that have been rearranged in the order of their names. */
    Block sort_columns() const;

    void clear();
    void swap(Block& other) noexcept;
    void swap(Block&& other) noexcept;

    // Default column size = -1 means clear all column in block
    // Else clear column [0, column_size) delete column [column_size, data.size)
    void clear_column_data(int column_size = -1) noexcept;

    bool mem_reuse() { return !data.empty(); }

    bool is_empty_column() { return data.empty(); }

    bool empty() const { return rows() == 0; }

    /** Updates SipHash of the Block, using update method of columns.
      * Returns hash for block, that could be used to differentiate blocks
      *  with same structure, but different data.
      */
    void update_hash(SipHash& hash) const;

    /** Get block data in string. */
    std::string dump_data(size_t begin = 0, size_t row_limit = 100) const;

    /** Get one line data from block, only use in load data */
    std::string dump_one_line(size_t row, int column_end) const;

    // copy a new block by the offset column
    Block copy_block(const std::vector<int>& column_offset) const;

    void append_to_block_by_selector(MutableBlock* dst, const IColumn::Selector& selector) const;

    // need exception safety
    static void filter_block_internal(Block* block, const std::vector<uint32_t>& columns_to_filter,
                                      const IColumn::Filter& filter);

    // need exception safety
    static void filter_block_internal(Block* block, const IColumn::Filter& filter,
                                      uint32_t column_to_keep);

    static Status filter_block(Block* block, const std::vector<uint32_t>& columns_to_filter,
                               int filter_column_id, int column_to_keep);

    static Status filter_block(Block* block, int filter_column_id, int column_to_keep);

    static void erase_useless_column(Block* block, int column_to_keep) {
        block->erase_tail(column_to_keep);
    }

    // serialize block to PBlock
    Status serialize(int be_exec_version, PBlock* pblock, size_t* uncompressed_bytes,
                     size_t* compressed_bytes, segment_v2::CompressionTypePB compression_type,
                     bool allow_transfer_large_data = false) const;

    Status deserialize(const PBlock& pblock);

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

    //note(wb) no DCHECK here, because this method is only used after compare_at now, so no need to repeat check here.
    // If this method is used in more places, you can add DCHECK case by case.
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

    void set_same_bit(std::vector<bool>::const_iterator begin,
                      std::vector<bool>::const_iterator end) {
        row_same_bit.insert(row_same_bit.end(), begin, end);

        DCHECK_EQ(row_same_bit.size(), rows());
    }

    bool get_same_bit(size_t position) {
        if (position >= row_same_bit.size()) {
            return false;
        }
        return row_same_bit[position];
    }

    void clear_same_bit() { row_same_bit.clear(); }

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

    size_t rows() const;
    size_t columns() const { return _columns.size(); }

    bool empty() const { return rows() == 0; }

    MutableColumns& mutable_columns() { return _columns; }

    void set_muatable_columns(MutableColumns&& columns) { _columns = std::move(columns); }

    DataTypes& data_types() { return _data_types; }

    MutableColumnPtr& get_column_by_position(size_t position) { return _columns[position]; }
    const MutableColumnPtr& get_column_by_position(size_t position) const {
        return _columns[position];
    }

    DataTypePtr& get_datatype_by_position(size_t position) { return _data_types[position]; }
    const DataTypePtr& get_datatype_by_position(size_t position) const {
        return _data_types[position];
    }

    int compare_one_column(size_t n, size_t m, size_t column_id, int nan_direction_hint) const {
        DCHECK_LE(column_id, columns());
        DCHECK_LE(n, rows());
        DCHECK_LE(m, rows());
        auto& column = get_column_by_position(column_id);
        return column->compare_at(n, m, *column, nan_direction_hint);
    }

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
                        "Merge block not match, self:[{}], input:[{}], ", dump_types(),
                        block.dump_types());
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

    Block to_block(int start_column = 0);

    Block to_block(int start_column, int end_column);

    void swap(MutableBlock& other) noexcept;

    void swap(MutableBlock&& other) noexcept;

    void add_row(const Block* block, int row);
    void add_rows(const Block* block, const int* row_begin, const int* row_end);
    void add_rows(const Block* block, size_t row_begin, size_t length);

    /// remove the column with the specified name
    void erase(const String& name);

    std::string dump_data(size_t row_limit = 100) const;

    void clear() {
        _columns.clear();
        _data_types.clear();
        _names.clear();
    }

    void clear_column_data() noexcept;

    size_t allocated_bytes() const;

    size_t bytes() const {
        size_t res = 0;
        for (const auto& elem : _columns) {
            res += elem->byte_size();
        }

        return res;
    }

    std::vector<std::string>& get_names() { return _names; }

    bool has(const std::string& name) const;

    size_t get_position_by_name(const std::string& name) const;

    /** Get a list of column names separated by commas. */
    std::string dump_names() const;

private:
    void initialize_index_by_name();
};

struct IteratorRowRef {
    std::shared_ptr<Block> block;
    int row_pos;
    bool is_same;

    template <typename T>
    int compare(const IteratorRowRef& rhs, const T& compare_arguments) const {
        return block->compare_at(row_pos, rhs.row_pos, compare_arguments, *rhs.block, -1);
    }

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
