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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/custom_allocator.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
#include "storage/partial_update_info.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

class SlotDescriptor;
class TabletSchema;
struct TabletAddRowsPayload;
class TupleDescriptor;
enum KeysType : int;

// Active: the memtable is currently used by writer to insert into blocks
// Write_finished: the memtable finished write blocks and in the queue waiting for flush
// FLUSH: the memtable is under flushing, write segment to disk.
enum MemType { ACTIVE = 0, WRITE_FINISHED = 1, FLUSH = 2 };

// row pos in _input_mutable_block
struct RowInBlock {
    size_t _row_pos;
    int64_t _allocated_lsn = 0;
    char* _agg_mem = nullptr;
    size_t* _agg_state_offset = nullptr;
    bool _has_init_agg;

    RowInBlock(size_t row = 0) : _row_pos(row), _has_init_agg(false) {}
    RowInBlock(size_t row, int64_t allocated_lsn)
            : _row_pos(row), _allocated_lsn(allocated_lsn), _has_init_agg(false) {}

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
        size_t left() const { return _left; }
        size_t right() const { return _right; }

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
    Tie(size_t begin, size_t end) : _begin(begin), _end(end), _bits(end - begin, 1) {}
    uint8_t operator[](size_t i) const { return _bits[i - _begin]; }
    uint8_t& operator[](size_t i) { return _bits[i - _begin]; }
    Iter iter() { return Iter(*this); }

private:
    const size_t _begin;
    const size_t _end;
    DorisVector<uint8_t> _bits;
};

// Compares two rows of one mutable block by key. The block is an argument of every call instead of
// being stored, so the comparator can never keep a pointer to a block that already went away.
class RowInBlockComparator {
public:
    RowInBlockComparator(std::shared_ptr<TabletSchema> tablet_schema)
            : _tablet_schema(std::move(tablet_schema)) {}
    int operator()(const MutableBlock& block, const RowInBlock* left,
                   const RowInBlock* right) const;

private:
    std::shared_ptr<TabletSchema> _tablet_schema;
};

class MemTableStat {
public:
    MemTableStat& operator+=(const MemTableStat& stat) {
        raw_rows += stat.raw_rows;
        merged_rows += stat.merged_rows;
        sort_ns += stat.sort_ns;
        agg_ns += stat.agg_ns;
        put_into_output_ns += stat.put_into_output_ns;
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
    int64_t duration_ns = 0;
    std::atomic<int64_t> sort_times = 0;
    std::atomic<int64_t> agg_times = 0;
};

// Common interface and block lifetime of a load memtable.
//
// The table model decides how inserted rows are tracked and merged, so each model is a separate
// implementation: DupKeysMemTable keeps a compact row permutation, while AGG and UNIQUE tables
// share the aggregation implementation through AggregatingMemTable. The model is selected once
// per memtable by create(); the hooks below are called once per inserted batch or per to_block()
// call, never per row, so the sorting and aggregation inner loops stay free of virtual dispatch.
class MemTable {
public:
    static std::shared_ptr<MemTable> create(int64_t tablet_id,
                                            std::shared_ptr<TabletSchema> tablet_schema,
                                            const std::vector<SlotDescriptor*>* slot_descs,
                                            TupleDescriptor* tuple_desc, bool enable_unique_key_mow,
                                            PartialUpdateInfo* partial_update_info,
                                            const std::shared_ptr<ResourceContext>& resource_ctx,
                                            bool need_lsn = false);

    virtual ~MemTable();

    int64_t tablet_id() const { return _tablet_id; }
    size_t memory_usage() const { return _mem_tracker->consumption(); }
    // Insert the rows named by `rows` from `block`.
    //
    // The caller must keep the memtable within MAX_MEMTABLE_ROWS rows: this batch has to fit, and
    // insert() asserts that instead of reporting an error. MemTableWriter::write is the production
    // caller and flushes the memtable before the limit is reached.
    //
    // Expected failures are reported through the returned Status. Allocation failures raised by a
    // model hook are not all converted here, so this may still throw a doris::Exception: after such
    // a failure the block can be ahead of its row tracking, and the only safe reaction is to catch
    // it and discard the whole memtable. MemTableWriter::write is the production caller and does
    // exactly that.
    Status insert(const Block* block, const TabletAddRowsPayload& rows);

    virtual void shrink_memtable_by_agg() {}

    bool need_flush() const;

    virtual bool need_agg() const { return false; }

    Status to_block(std::unique_ptr<Block>* res);

    ConstAllocatedLsnVectorSharedPtr allocated_lsns() const { return _output_allocated_lsns; }

    bool empty() const { return _input_mutable_block.rows() == 0; }

    const MemTableStat& stat() { return _stat; }

    std::shared_ptr<ResourceContext> resource_ctx() { return _resource_ctx; }

    std::shared_ptr<MemTracker> mem_tracker() { return _mem_tracker; }

    void set_flush_success() { _is_flush_success = true; }

    MemType get_mem_type() { return _mem_type; }

    void update_mem_type(MemType memtype) { _mem_type = memtype; }

    int64_t raw_rows() { return _stat.raw_rows.load(); }

protected:
    MemTable(int64_t tablet_id, std::shared_ptr<TabletSchema> tablet_schema,
             const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
             PartialUpdateInfo* partial_update_info,
             const std::shared_ptr<ResourceContext>& resource_ctx, bool need_lsn);

    // Called once when the first batch creates the input/output blocks, before any row is
    // appended. Derived models resolve their per-column state from _input_mutable_block here.
    virtual Status _on_first_insertion() { return Status::OK(); }
    // Reserve the row metadata of one batch before its columns are appended.
    virtual void _reserve_rows(size_t num_rows, size_t cursor) = 0;
    // Record the row metadata of a batch whose columns were appended successfully.
    virtual void _append_row_metadata(size_t num_rows, size_t cursor,
                                      const DorisVector<int64_t>& allocated_lsns) = 0;
    // Sort the tracked rows and return how many adjacent rows share a key.
    virtual size_t _sort() = 0;
    virtual Status _put_into_output(Block& in_block) = 0;
    virtual Status _to_block(std::unique_ptr<Block>* res) = 0;
    // Flush threshold of this model, before the memory-pressure check.
    virtual int64_t _flush_buffer_size() const;

    // Rows are addressed by int32 positions inside a block, so a memtable never tracks more. The
    // limit is a precondition of insert() rather than a runtime check: MemTableWriter::write
    // flushes the memtable before it is reached.
    static constexpr size_t MAX_MEMTABLE_ROWS =
            static_cast<size_t>(std::numeric_limits<int32_t>::max());

    // Grow amortized while never exceeding the row limit. Called before the batch columns are
    // appended so a failed reservation leaves the block and its row tracking untouched.
    template <typename T>
    static void reserve_for_append(DorisVector<T>& rows, size_t append_size, size_t max_size) {
        DORIS_CHECK(append_size <= max_size && rows.size() <= max_size - append_size);
        const auto required_size = rows.size() + append_size;
        if (required_size > rows.capacity()) {
            rows.reserve(
                    std::min(max_size, rows.capacity() + std::max(rows.capacity(), append_size)));
        }
    }

    int64_t _adaptive_write_buffer_size() const;

    void _init_columns_offset_by_slot_descs(const std::vector<SlotDescriptor*>* slot_descs,
                                            const TupleDescriptor* tuple_desc,
                                            PartialUpdateInfo* partial_update_info);

    std::atomic<MemType> _mem_type;
    int64_t _tablet_id;
    bool _is_flush_success = false;
    std::shared_ptr<TabletSchema> _tablet_schema;

    std::shared_ptr<ResourceContext> _resource_ctx;

    std::shared_ptr<MemTracker> _mem_tracker;
    int64_t _load_mem_limit = -1;

    std::vector<int> _column_offset;
    int32_t _row_lsn_col_pos = -1;

    // Number of rows inserted to this memtable.
    // This is not the rows in this memtable, because rows may be merged
    // in unique or aggregate key model.
    MemTableStat _stat;

    //for vectorized
    MutableBlock _input_mutable_block;
    MutableBlock _output_mutable_block;
    AllocatedLsnVectorSharedPtr _output_allocated_lsns = std::make_shared<std::vector<int64_t>>();
    bool _need_lsn = false;
    size_t _last_sorted_pos = 0;

    size_t _num_columns;
    bool _is_first_insertion;
}; // class MemTable

} // namespace doris
