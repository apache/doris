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

#include <sstream>
#include <string>

#include "olap/row_cursor_cell.h"
#include "olap/schema.h"

namespace doris {

class MemPool;
class Arena;

// The row has all columns layed out in memory based on the schema.column_offset()
struct ContiguousRow {
    ContiguousRow(const Schema* schema, const void* row) : _schema(schema), _row((void*)row) {}
    ContiguousRow(const Schema* schema, void* row) : _schema(schema), _row(row) {}
    RowCursorCell cell(uint32_t cid) const {
        return RowCursorCell((char*)_row + _schema->column_offset(cid));
    }
    void set_is_null(uint32_t cid, bool is_null) const { _schema->set_is_null(_row, cid, is_null); }
    const Schema* schema() const { return _schema; }
    void* row_ptr() const { return _row; }

private:
    const Schema* _schema;
    void* _row;
};

template <typename LhsRowType, typename RhsRowType>
bool equal_row(const std::vector<uint32_t>& ids, const LhsRowType& lhs, const RhsRowType& rhs) {
    for (auto id : ids) {
        if (!lhs.schema()->column(id)->equal(lhs.cell(id), rhs.cell(id))) {
            return false;
        }
    }
    return true;
}

template <typename LhsRowType, typename RhsRowType>
int compare_row(const LhsRowType& lhs, const RhsRowType& rhs) {
    for (uint32_t cid = 0; cid < lhs.schema()->num_key_columns(); ++cid) {
        auto res = lhs.schema()->column(cid)->compare_cell(lhs.cell(cid), rhs.cell(cid));
        if (res != 0) {
            return res;
        }
    }
    return 0;
}

// Used to compare row with input scan key. Scan key only contains key columns,
// row contains all key columns, which is superset of key columns.
// So we should compare the common prefix columns of lhs and rhs.
//
// NOTE: if you are not sure if you can use it, please don't use this function.
template <typename LhsRowType, typename RhsRowType>
int compare_row_key(const LhsRowType& lhs, const RhsRowType& rhs) {
    auto cmp_cids = std::min(lhs.schema()->num_column_ids(), rhs.schema()->num_column_ids());
    for (uint32_t cid = 0; cid < cmp_cids; ++cid) {
        auto res = lhs.schema()->column(cid)->compare_cell(lhs.cell(cid), rhs.cell(cid));
        if (res != 0) {
            return res;
        }
    }
    return 0;
}

// This function is only used to do index compare, currently it is only used in
// OlapIndex to compare input key with short key item, and both of two only
// contain key column ids in row's Schema.
//
// NOTE: Client should assure that lhs and rhs only contain KEY column ids in its
// Schema. Otherwise it may lead to an error.
template <typename LhsRowType, typename RhsRowType>
int index_compare_row(const LhsRowType& lhs, const RhsRowType& rhs) {
    auto cmp_cids = std::min(lhs.schema()->num_column_ids(), rhs.schema()->num_column_ids());
    for (uint32_t cid = 0; cid < cmp_cids; ++cid) {
        auto res = lhs.schema()->column(cid)->index_cmp(lhs.cell(cid), rhs.cell(cid));
        if (res != 0) {
            return res;
        }
    }
    return 0;
}

// This function will initialize dst row with src row. For key columns, this function
// will direct_copy source column to destination column, and for value columns, this
// function will first initialize destination column and then update with source column
// value.
template <typename DstRowType, typename SrcRowType>
void init_row_with_others(DstRowType* dst, const SrcRowType& src, MemPool* mem_pool,
                          ObjectPool* agg_pool) {
    for (auto cid : dst->schema()->column_ids()) {
        auto dst_cell = dst->cell(cid);
        dst->schema()->column(cid)->agg_init(&dst_cell, src.cell(cid), mem_pool, agg_pool);
    }
}

// Copy other row to destination directly. This function assume
// that destination has enough space for source content.
template <typename DstRowType, typename SrcRowType>
void direct_copy_row(DstRowType* dst, const SrcRowType& src) {
    for (auto cid : dst->schema()->column_ids()) {
        auto dst_cell = dst->cell(cid);
        dst->schema()->column(cid)->direct_copy(&dst_cell, src.cell(cid));
    }
}

// Deep copy other row's content into itself.
template <typename DstRowType, typename SrcRowType>
void copy_row(DstRowType* dst, const SrcRowType& src, MemPool* pool) {
    for (auto cid : dst->schema()->column_ids()) {
        auto dst_cell = dst->cell(cid);
        auto src_cell = src.cell(cid);
        dst->schema()->column(cid)->deep_copy(&dst_cell, src_cell, pool);
    }
}

// NOTE: only use in MemTable for now, it is same with copy_row(), but for HLL and Object type,
// its content is in Slice->data, other than a normal Slice, so we just assign the Slice->data
// pointer, instead of memcpy the data.
// TODO(lingbin): remove this method
template <typename DstRowType, typename SrcRowType>
void copy_row_in_memtable(DstRowType* dst, const SrcRowType& src, MemPool* pool) {
    for (auto cid : dst->schema()->column_ids()) {
        auto dst_cell = dst->cell(cid);
        auto src_cell = src.cell(cid);
        dst->schema()->column(cid)->copy_object(&dst_cell, src_cell, pool);
    }
}

template <typename DstRowType, typename SrcRowType>
void agg_update_row(DstRowType* dst, const SrcRowType& src, MemPool* mem_pool) {
    for (uint32_t cid = dst->schema()->num_key_columns(); cid < dst->schema()->num_columns();
         ++cid) {
        auto dst_cell = dst->cell(cid);
        auto src_cell = src.cell(cid);
        dst->schema()->column(cid)->agg_update(&dst_cell, src_cell, mem_pool);
    }
}

template <typename DstRowType, typename SrcRowType>
void agg_update_row_with_sequence(DstRowType* dst, const SrcRowType& src, uint32_t sequence_idx,
                                  MemPool* mem_pool) {
    auto seq_dst_cell = dst->cell(sequence_idx);
    auto seq_src_cell = src.cell(sequence_idx);
    auto res = src.schema()->column(sequence_idx)->compare_cell(seq_dst_cell, seq_src_cell);
    // dst sequence column larger than src, don't need to update
    if (res > 0) {
        return;
    }
    for (uint32_t cid = dst->schema()->num_key_columns(); cid < dst->schema()->num_columns();
         ++cid) {
        auto dst_cell = dst->cell(cid);
        auto src_cell = src.cell(cid);
        dst->schema()->column(cid)->agg_update(&dst_cell, src_cell, mem_pool);
    }
}

// Do aggregate update source row to destination row.
// This function will operate on given cids.
// TODO(zc): unify two versions of agg_update_row
template <typename DstRowType, typename SrcRowType>
void agg_update_row(const std::vector<uint32_t>& cids, DstRowType* dst, const SrcRowType& src) {
    for (auto cid : cids) {
        auto dst_cell = dst->cell(cid);
        auto src_cell = src.cell(cid);
        dst->schema()->column(cid)->agg_update(&dst_cell, src_cell);
    }
}

template <typename RowType>
void agg_finalize_row(RowType* row, MemPool* mem_pool) {
    for (uint32_t cid = row->schema()->num_key_columns(); cid < row->schema()->num_columns();
         ++cid) {
        auto cell = row->cell(cid);
        row->schema()->column(cid)->agg_finalize(&cell, mem_pool);
    }
}

template <typename RowType>
void agg_finalize_row(const std::vector<uint32_t>& ids, RowType* row, MemPool* mem_pool) {
    for (uint32_t id : ids) {
        auto cell = row->cell(id);
        row->schema()->column(id)->agg_finalize(&cell, mem_pool);
    }
}

template <typename RowType>
uint32_t hash_row(const RowType& row, uint32_t seed) {
    for (uint32_t cid : row.schema()->column_ids()) {
        FieldType type = row.schema()->column(cid)->type();
        // The approximation of float/double in a certain precision range, the binary of byte is not
        // a fixed value, so these two types are ignored in calculating hash code.
        if (type == OLAP_FIELD_TYPE_FLOAT || type == OLAP_FIELD_TYPE_DOUBLE) {
            continue;
        }
        seed = row.schema()->column(cid)->hash_code(row.cell(cid), seed);
    }
    return seed;
}

template <typename RowType>
std::string print_row(const RowType& row) {
    std::stringstream ss;

    size_t i = 0;
    for (auto cid : row.schema()->column_ids()) {
        if (i++ > 0) {
            ss << "|";
        }
        ss << row.schema()->column(cid)->debug_string(row.cell(cid));
    }

    return ss.str();
}

} // namespace doris
