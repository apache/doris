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
#include "runtime/mem_tracker.h"
#include "util/tuple_row_zorder_compare.h"
#include "vec/core/block.h"
#include "vec/common/string_ref.h"
#include "vec/aggregate_functions/aggregate_function.h"

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
    MemTable(int64_t tablet_id, Schema* schema, const TabletSchema* tablet_schema,
             const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
             KeysType keys_type, RowsetWriter* rowset_writer,
             const std::shared_ptr<MemTracker>& parent_tracker, bool support_vec = false);
    ~MemTable();

    int64_t tablet_id() const { return _tablet_id; }
    size_t memory_usage() const { return _mem_tracker->consumption(); }
    std::shared_ptr<MemTracker>& mem_tracker() { return _mem_tracker; }

    void insert(const Tuple* tuple);
    // insert tuple from (row_pos) to (row_pos+num_rows)
    void insert(const vectorized::Block* block, size_t row_pos, size_t num_rows);

    /// Flush
    Status flush();
    Status close();

    int64_t flush_size() const { return _flush_size; }

private:
    Status _do_flush(int64_t& duration_ns);

    class RowCursorComparator : public RowComparator {
    public:
        RowCursorComparator(const Schema* schema);
        int operator()(const char* left, const char* right) const;

    private:
        const Schema* _schema;
    };

    // row pos in _input_mutable_block
    struct RowInBlock {
        size_t _row_pos;
        std::vector<vectorized::AggregateDataPtr> _agg_places;
        explicit RowInBlock(size_t i) : _row_pos(i) {}

        void init_agg_places(std::vector<vectorized::AggregateFunctionPtr>& agg_functions,
                             int key_column_count) {
            _agg_places.resize(agg_functions.size());
            for (int cid = 0; cid < agg_functions.size(); cid++) {
                if (cid < key_column_count) {
                    _agg_places[cid] = nullptr;
                } else {
                    auto function = agg_functions[cid];
                    size_t place_size = function->size_of_data();
                    _agg_places[cid] = new char[place_size];
                    function->create(_agg_places[cid]);
                }
            }
        }

        ~RowInBlock() {
            for (auto agg_place : _agg_places) {
                delete[] agg_place;
            }
        }
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
    typedef SkipList<char*, RowComparator> Table;
    typedef Table::key_type TableKey;
    typedef SkipList<RowInBlock*, RowInBlockComparator> VecTable;

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
    // for vectorized
    void _insert_one_row_from_block(RowInBlock* row_in_block);
    void _aggregate_two_row_in_block(RowInBlock* new_row, RowInBlock* row_in_skiplist);

    int64_t _tablet_id;
    Schema* _schema;
    const TabletSchema* _tablet_schema;
    // the slot in _slot_descs are in order of tablet's schema
    const std::vector<SlotDescriptor*>* _slot_descs;
    KeysType _keys_type;

    // TODO: change to unique_ptr of comparator
    std::shared_ptr<RowComparator> _row_comparator;

    std::shared_ptr<RowInBlockComparator> _vec_row_comparator;

    std::shared_ptr<MemTracker> _mem_tracker;
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
    Table* _skip_list;
    Table::Hint _hint;

    VecTable* _vec_skip_list;
    VecTable::Hint _vec_hint;

    RowsetWriter* _rowset_writer;

    // the data size flushed on disk of this memtable
    int64_t _flush_size = 0;
    // Number of rows inserted to this memtable.
    // This is not the rows in this memtable, because rows may be merged
    // in unique or aggragate key model.
    int64_t _rows = 0;

    //for vectorized
    vectorized::MutableBlock _input_mutable_block;
    vectorized::MutableBlock _output_mutable_block;
    vectorized::Block _collect_vskiplist_results();
    bool _is_first_insertion;

    void _init_agg_functions(const vectorized::Block* block);
    std::vector<vectorized::AggregateFunctionPtr> _agg_functions;
    std::vector<RowInBlock*> _row_in_blocks;
    size_t _mem_usage;
}; // class MemTable

inline std::ostream& operator<<(std::ostream& os, const MemTable& table) {
    os << "MemTable(addr=" << &table << ", tablet=" << table.tablet_id()
       << ", mem=" << table.memory_usage();
    return os;
}

} // namespace doris
