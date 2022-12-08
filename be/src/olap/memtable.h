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

#include <ostream>

#include "common/object_pool.h"
#include "olap/olap_define.h"
#include "olap/skiplist.h"
#include "olap/tablet.h"
#include "runtime/memory/mem_tracker.h"
#include "util/tuple_row_zorder_compare.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"

namespace doris {

struct ContiguousRow;
class RowsetWriter;
class Schema;
class SlotDescriptor;
class TabletSchema;
class Tuple;
class TupleDescriptor;

class MemTable {
public:
    MemTable(TabletSharedPtr tablet, Schema* schema, const TabletSchema* tablet_schema,
             const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
             RowsetWriter* rowset_writer, DeleteBitmapPtr delete_bitmap,
             const RowsetIdUnorderedSet& rowset_ids, int64_t cur_max_version,
             const std::shared_ptr<MemTracker>& insert_mem_tracker,
             const std::shared_ptr<MemTracker>& flush_mem_tracker, bool support_vec = false);
    ~MemTable();

    int64_t tablet_id() const { return _tablet->tablet_id(); }
    KeysType keys_type() const { return _tablet->keys_type(); }
    size_t memory_usage() const {
        return _insert_mem_tracker->consumption() + _flush_mem_tracker->consumption();
    }

    inline void insert(const Tuple* tuple) { (this->*_insert_fn)(tuple); }
    // insert tuple from (row_pos) to (row_pos+num_rows)
    void insert(const vectorized::Block* block, const std::vector<int>& row_idxs);

    void shrink_memtable_by_agg();

    bool is_flush() const;

    bool need_to_agg();

    /// Flush
    Status flush();
    Status close();

    int64_t flush_size() const { return _flush_size; }

private:
    Status _do_flush(int64_t& duration_ns);

    class RowCursorComparator : public RowComparator {
    public:
        RowCursorComparator(const Schema* schema);
        int operator()(const char* left, const char* right) const override;

    private:
        const Schema* _schema;
    };

    // row pos in _input_mutable_block
    struct RowInBlock {
        size_t _row_pos;
        char* _agg_mem;
        size_t* _agg_state_offset;

        RowInBlock(size_t row) : _row_pos(row) {};

        void init_agg_places(char* agg_mem, size_t* agg_state_offset) {
            _agg_mem = agg_mem;
            _agg_state_offset = agg_state_offset;
        }

        char* agg_places(size_t offset) const { return _agg_mem + _agg_state_offset[offset]; }
    };

    class RowInBlockComparator {
    public:
        RowInBlockComparator(const Schema* schema) : _schema(schema) {};
        // call set_block before operator().
        // only first time insert block to create _input_mutable_block,
        // so can not Comparator of construct to set pblock
        void set_block(vectorized::MutableBlock* pblock) { _pblock = pblock; }
        int operator()(const RowInBlock* left, const RowInBlock* right) const;

    private:
        const Schema* _schema;
        vectorized::MutableBlock* _pblock; // 对应Memtable::_input_mutable_block
    };

private:
    using Table = SkipList<char*, RowComparator>;
    using TableKey = Table::key_type;
    using VecTable = SkipList<RowInBlock*, RowInBlockComparator>;

public:
    /// The iterator of memtable, so that the data in this memtable
    /// can be visited outside.
    class Iterator {
    public:
        Iterator(MemTable* mem_table);
        ~Iterator() = default;

        void seek_to_first();
        bool valid();
        void next();
        ContiguousRow get_current_row();

    private:
        MemTable* _mem_table;
        Table::Iterator _it;
    };

private:
    void _tuple_to_row(const Tuple* tuple, ContiguousRow* row, MemPool* mem_pool);
    void _aggregate_two_row(const ContiguousRow& new_row, TableKey row_in_skiplist);
    void _replace_row(const ContiguousRow& src_row, TableKey row_in_skiplist);
    void _insert_dup(const Tuple* tuple);
    void _insert_agg(const Tuple* tuple);
    // for vectorized
    void _insert_one_row_from_block(RowInBlock* row_in_block);
    void _aggregate_two_row_in_block(RowInBlock* new_row, RowInBlock* row_in_skiplist);

    Status _generate_delete_bitmap();

private:
    TabletSharedPtr _tablet;
    Schema* _schema;
    const TabletSchema* _tablet_schema;
    // the slot in _slot_descs are in order of tablet's schema
    const std::vector<SlotDescriptor*>* _slot_descs;

    // TODO: change to unique_ptr of comparator
    std::shared_ptr<RowComparator> _row_comparator;

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
    // This is a buffer, to hold the memory referenced by the rows that have not
    // been inserted into the SkipList
    std::unique_ptr<MemPool> _buffer_mem_pool;
    // Only the rows will be inserted into SkipList can allocate memory from _table_mem_pool.
    // In this way, we can make MemTable::memory_usage() to be more accurate, and eventually
    // reduce the number of segment files that are generated by current load
    std::unique_ptr<MemPool> _table_mem_pool;
    // The object buffer pool for convert tuple to row
    ObjectPool _agg_buffer_pool;
    // Only the rows will be inserted into SkipList can acquire the owner ship from
    // `_agg_buffer_pool`
    ObjectPool _agg_object_pool;

    size_t _schema_size;
    std::unique_ptr<Table> _skip_list;
    Table::Hint _hint;

    std::unique_ptr<VecTable> _vec_skip_list;
    VecTable::Hint _vec_hint;
    void _init_columns_offset_by_slot_descs(const std::vector<SlotDescriptor*>* slot_descs,
                                            const TupleDescriptor* tuple_desc);
    std::vector<int> _column_offset;

    RowsetWriter* _rowset_writer;

    // the data size flushed on disk of this memtable
    int64_t _flush_size = 0;
    // Number of rows inserted to this memtable.
    // This is not the rows in this memtable, because rows may be merged
    // in unique or aggregate key model.
    int64_t _rows = 0;
    void (MemTable::*_insert_fn)(const Tuple* tuple) = nullptr;
    void (MemTable::*_aggregate_two_row_fn)(const ContiguousRow& new_row,
                                            TableKey row_in_skiplist) = nullptr;

    //for vectorized
    vectorized::MutableBlock _input_mutable_block;
    vectorized::MutableBlock _output_mutable_block;

    template <bool is_final>
    void _collect_vskiplist_results();
    bool _is_first_insertion;

    void _init_agg_functions(const vectorized::Block* block);
    std::vector<vectorized::AggregateFunctionPtr> _agg_functions;
    std::vector<size_t> _offsets_of_aggregate_states;
    size_t _total_size_of_aggregate_states;
    std::vector<RowInBlock*> _row_in_blocks;
    // Memory usage without mempool.
    size_t _mem_usage;

    DeleteBitmapPtr _delete_bitmap;
    RowsetIdUnorderedSet _rowset_ids;
    int64_t _cur_max_version;
}; // class MemTable

inline std::ostream& operator<<(std::ostream& os, const MemTable& table) {
    os << "MemTable(addr=" << &table << ", tablet=" << table.tablet_id()
       << ", mem=" << table.memory_usage();
    return os;
}

} // namespace doris
