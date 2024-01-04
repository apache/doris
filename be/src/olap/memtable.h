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

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "gutil/integral_types.h"
#include "olap/olap_common.h"
#include "olap/partial_update_info.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "runtime/memory/mem_tracker.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/common/arena.h"
#include "vec/core/block.h"

namespace doris {

class RowsetWriter;
class Schema;
class SlotDescriptor;
class TabletSchema;
class TupleDescriptor;
enum KeysType : int;
struct FlushContext;

// row pos in _input_mutable_block
struct RowInBlock {
    size_t _row_pos;
    char* _agg_mem;
    size_t* _agg_state_offset;
    bool _has_init_agg;

    RowInBlock(size_t row) : _row_pos(row), _has_init_agg(false) {}

    void init_agg_places(char* agg_mem, size_t* agg_state_offset) {
        _has_init_agg = true;
        _agg_mem = agg_mem;
        _agg_state_offset = agg_state_offset;
    }

    char* agg_places(size_t offset) const { return _agg_mem + _agg_state_offset[offset]; }

    inline bool has_init_agg() const { return _has_init_agg; }

    inline void remove_init_agg() { _has_init_agg = false; }
};

class Tie {
public:
    class Iter {
    public:
        Iter(Tie& tie) : _tie(tie), _next(tie._begin + 1) {}
        size_t left() { return _left; }
        size_t right() { return _right; }

        // return false means no more ranges
        bool next() {
            if (_next >= _tie._end) {
                return false;
            }
            _next = _find(1, _next);
            if (_next >= _tie._end) {
                return false;
            }
            _left = _next - 1;
            _next = _find(0, _next);
            _right = _next;
            return true;
        }

    private:
        size_t _find(uint8_t value, size_t start) {
            if (start >= _tie._end) {
                return start;
            }
            size_t offset = start - _tie._begin;
            size_t size = _tie._end - start;
            void* p = std::memchr(_tie._bits.data() + offset, value, size);
            if (p == nullptr) {
                return _tie._end;
            }
            return static_cast<uint8_t*>(p) - _tie._bits.data() + _tie._begin;
        }

    private:
        Tie& _tie;
        size_t _left;
        size_t _right;
        size_t _next;
    };

public:
    Tie(size_t begin, size_t end) : _begin(begin), _end(end) {
        _bits = std::vector<uint8_t>(_end - _begin, 1);
    }
    uint8_t operator[](int i) const { return _bits[i - _begin]; }
    uint8_t& operator[](int i) { return _bits[i - _begin]; }
    Iter iter() { return Iter(*this); }

private:
    const size_t _begin;
    const size_t _end;
    std::vector<uint8_t> _bits;
};

class RowInBlockComparator {
public:
    RowInBlockComparator(const Schema* schema) : _schema(schema) {}
    // call set_block before operator().
    // only first time insert block to create _input_mutable_block,
    // so can not Comparator of construct to set pblock
    void set_block(vectorized::MutableBlock* pblock) { _pblock = pblock; }
    int operator()(const RowInBlock* left, const RowInBlock* right) const;

private:
    const Schema* _schema;
    vectorized::MutableBlock* _pblock; // 对应Memtable::_input_mutable_block
};

class MemTableStat {
public:
    MemTableStat& operator+=(MemTableStat& stat) {
        raw_rows += stat.raw_rows;
        merged_rows += stat.merged_rows;
        sort_ns += stat.sort_ns;
        agg_ns += stat.agg_ns;
        put_into_output_ns += stat.put_into_output_ns;
        delete_bitmap_ns += stat.delete_bitmap_ns;
        segment_writer_ns += stat.segment_writer_ns;
        duration_ns += stat.duration_ns;
        sort_times += stat.sort_times;
        agg_times += stat.agg_times;

        return *this;
    }

    std::atomic<int64_t> raw_rows = 0;
    std::atomic<int64_t> merged_rows = 0;
    int64_t sort_ns = 0;
    int64_t agg_ns = 0;
    int64_t put_into_output_ns = 0;
    int64_t delete_bitmap_ns = 0;
    int64_t segment_writer_ns = 0;
    int64_t duration_ns = 0;
    std::atomic<int64_t> sort_times = 0;
    std::atomic<int64_t> agg_times = 0;
};

class MemTable {
public:
    MemTable(TabletSharedPtr tablet, Schema* schema, const TabletSchema* tablet_schema,
             const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
             RowsetWriter* rowset_writer, std::shared_ptr<MowContext> mow_context,
             PartialUpdateInfo* partial_update_info,
             const std::shared_ptr<MemTracker>& insert_mem_tracker,
             const std::shared_ptr<MemTracker>& flush_mem_tracker);
    ~MemTable();

    int64_t tablet_id() const { return _tablet->tablet_id(); }
    size_t memory_usage() const {
        return _insert_mem_tracker->consumption() + _arena->used_size() +
               _flush_mem_tracker->consumption();
    }
    // insert tuple from (row_pos) to (row_pos+num_rows)
    void insert(const vectorized::Block* block, const std::vector<int>& row_idxs,
                bool is_append = false);

    void shrink_memtable_by_agg();

    bool need_flush() const;

    bool need_agg() const;

    /// Flush
    Status flush();
    Status close();

    int64_t flush_size() const { return _flush_size; }

    void set_callback(std::function<void(MemTableStat&)> callback) {
        _delta_writer_callback = callback;
    }

    bool empty() const { return _input_mutable_block.rows() == 0; }
    void assign_segment_id();

private:
    Status _do_flush();

private:
    // for vectorized
    void _aggregate_two_row_in_block(vectorized::MutableBlock& mutable_block, RowInBlock* new_row,
                                     RowInBlock* row_in_skiplist);

    Status _generate_delete_bitmap(int32_t segment_id);

    // serialize block to row store format and append serialized data into row store column
    // in block
    void serialize_block_to_row_column(vectorized::Block& block);

    // Unfold variant column to Block
    // Eg. [A | B | C | (D, E, F)]
    // After unfold block structure changed to -> [A | B | C | D | E | F]
    // The expanded D, E, F is dynamic part of the block
    // The flushed Block columns should match exactly from the same type of frontend meta
    Status unfold_variant_column(vectorized::Block& block, FlushContext* ctx);

private:
    TabletSharedPtr _tablet;
    const KeysType _keys_type;
    bool _is_partial_update = false;
    Schema* _schema;
    const TabletSchema* _tablet_schema;

    std::shared_ptr<RowInBlockComparator> _vec_row_comparator;

    // `_insert_manual_mem_tracker` manually records the memory value of memtable insert()
    // `_flush_hook_mem_tracker` automatically records the memory value of memtable flush() through mem hook.
    // Is used to flush when _insert_manual_mem_tracker larger than write_buffer_size and run flush memtable
    // when the sum of all memtable (_insert_manual_mem_tracker + _flush_hook_mem_tracker) exceeds the limit.
    std::shared_ptr<MemTracker> _insert_mem_tracker;
    std::shared_ptr<MemTracker> _flush_mem_tracker;
    // It is only used for verification when the value of `_insert_manual_mem_tracker` is suspected to be wrong.
    // The memory value automatically tracked by the mem hook is 20% less than the manually recorded
    // value in the memtable, because some freed memory is not allocated in the DeltaWriter.
    std::unique_ptr<MemTracker> _insert_mem_tracker_use_hook;
    // Only the rows will be inserted into SkipList can allocate memory from _arena.
    // In this way, we can make MemTable::memory_usage() to be more accurate, and eventually
    // reduce the number of segment files that are generated by current load
    std::unique_ptr<vectorized::Arena> _arena;
    // The object buffer pool for convert tuple to row
    ObjectPool _agg_buffer_pool;
    // Only the rows will be inserted into SkipList can acquire the owner ship from
    // `_agg_buffer_pool`
    ObjectPool _agg_object_pool;

    size_t _schema_size;

    void _init_columns_offset_by_slot_descs(const std::vector<SlotDescriptor*>* slot_descs,
                                            const TupleDescriptor* tuple_desc);
    std::vector<int> _column_offset;

    RowsetWriter* _rowset_writer;

    // the data size flushed on disk of this memtable
    int64_t _flush_size = 0;
    // Number of rows inserted to this memtable.
    // This is not the rows in this memtable, because rows may be merged
    // in unique or aggregate key model.
    MemTableStat _stat;

    //for vectorized
    vectorized::MutableBlock _input_mutable_block;
    vectorized::MutableBlock _output_mutable_block;
    size_t _last_sorted_pos = 0;

    //return number of same keys
    size_t _sort();
    void _sort_one_column(std::vector<RowInBlock*>& row_in_blocks, Tie& tie,
                          std::function<int(const RowInBlock*, const RowInBlock*)> cmp);
    template <bool is_final>
    void _finalize_one_row(RowInBlock* row, const vectorized::ColumnsWithTypeAndName& block_data,
                           int row_pos);
    template <bool is_final>
    void _aggregate();
    void _put_into_output(vectorized::Block& in_block);
    bool _is_first_insertion;
    std::function<void(MemTableStat&)> _delta_writer_callback;
    std::optional<int32_t> _segment_id = std::nullopt;

    void _init_agg_functions(const vectorized::Block* block);
    std::vector<vectorized::AggregateFunctionPtr> _agg_functions;
    std::vector<size_t> _offsets_of_aggregate_states;
    size_t _total_size_of_aggregate_states;
    std::vector<RowInBlock*> _row_in_blocks;
    // Memory usage without _arena.
    size_t _mem_usage;

    std::shared_ptr<MowContext> _mow_context;
    size_t _num_columns;
    int32_t _seq_col_idx_in_block = -1;
}; // class MemTable

inline std::ostream& operator<<(std::ostream& os, const MemTable& table) {
    os << "MemTable(addr=" << &table << ", tablet=" << table.tablet_id()
       << ", mem=" << table.memory_usage();
    return os;
}

} // namespace doris
