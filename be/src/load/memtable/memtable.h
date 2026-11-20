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

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <vector>

#include "common/status.h"
#include "core/arena.h"
#include "core/block/block.h"
#include "core/custom_allocator.h"
#include "exprs/aggregate/aggregate_function.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
#include "storage/partial_update_info.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/sparse_flush_output.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

inline constexpr size_t kSparseMemtableSampleRows = 4096;
inline constexpr double kSparseMemtableMaxNonNullRatio = 0.10;
inline constexpr double kSparseMemtableMinSavingRatio = 0.30;

class Schema;
class SlotDescriptor;
class TabletSchema;
struct TabletAddRowsPayload;
class TupleDescriptor;
enum KeysType : int;

enum class SparseFlushCapabilityReason : uint8_t {
    ELIGIBLE,
    CONFIG_DISABLED,
    UNSUPPORTED_KEYS,
    PARTIAL_UPDATE,
    NO_KEY_COLUMNS,
    CLUSTER_KEY,
    ROW_STORE,
    VARIANT,
    NO_SPARSE_COLUMNS,
    UNIQUE_DUPLICATES,
};

struct SparseFlushCapability {
    SparseFlushCapabilityReason reason = SparseFlushCapabilityReason::ELIGIBLE;
    bool eligible() const { return reason == SparseFlushCapabilityReason::ELIGIBLE; }
};

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

class RowInBlockComparator {
public:
    RowInBlockComparator(std::shared_ptr<TabletSchema> tablet_schema)
            : _tablet_schema(tablet_schema) {}
    // call set_block before operator().
    // only first time insert block to create _input_mutable_block,
    // so can not Comparator of construct to set pblock
    void set_block(MutableBlock* pblock) { _pblock = pblock; }
    int operator()(const RowInBlock* left, const RowInBlock* right) const;

private:
    std::shared_ptr<TabletSchema> _tablet_schema;
    MutableBlock* _pblock = nullptr; //  corresponds to Memtable::_input_mutable_block
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
        sparse_candidate_columns += stat.sparse_candidate_columns;
        sparse_encoded_columns += stat.sparse_encoded_columns;
        sparse_all_null_columns += stat.sparse_all_null_columns;
        sparse_fallback_columns += stat.sparse_fallback_columns;
        sparse_dense_equivalent_bytes += stat.sparse_dense_equivalent_bytes;
        sparse_actual_bytes_at_flush += stat.sparse_actual_bytes_at_flush;
        sparse_sidecar_bytes_at_flush += stat.sparse_sidecar_bytes_at_flush;
        sparse_materialize_bytes += stat.sparse_materialize_bytes;
        sparse_materialize_ns += stat.sparse_materialize_ns;
        const int64_t peak = stat.memtable_peak_memory_bytes.load();
        int64_t current_peak = memtable_peak_memory_bytes.load();
        while (current_peak < peak &&
               !memtable_peak_memory_bytes.compare_exchange_weak(current_peak, peak)) {
        }

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
    std::atomic<int64_t> sparse_candidate_columns = 0;
    std::atomic<int64_t> sparse_encoded_columns = 0;
    std::atomic<int64_t> sparse_all_null_columns = 0;
    std::atomic<int64_t> sparse_fallback_columns = 0;
    std::atomic<int64_t> sparse_dense_equivalent_bytes = 0;
    std::atomic<int64_t> sparse_actual_bytes_at_flush = 0;
    std::atomic<int64_t> sparse_sidecar_bytes_at_flush = 0;
    std::atomic<int64_t> sparse_materialize_bytes = 0;
    std::atomic<int64_t> sparse_materialize_ns = 0;
    // Maximum tracker consumption of one MemTable in this writer, not a sum across flushes.
    std::atomic<int64_t> memtable_peak_memory_bytes = 0;
};

class MemTable {
public:
    MemTable(int64_t tablet_id, std::shared_ptr<TabletSchema> tablet_schema,
             const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
             bool enable_unique_key_mow, PartialUpdateInfo* partial_update_info,
             const std::shared_ptr<ResourceContext>& resource_ctx, bool need_lsn = false);
    ~MemTable();

    int64_t tablet_id() const { return _tablet_id; }
    size_t memory_usage() const { return _mem_tracker->consumption(); }
    size_t get_flush_reserve_memory_size() const;
    // insert tuple from (row_pos) to (row_pos+num_rows)
    Status insert(const Block* block, const TabletAddRowsPayload& rows);

    void shrink_memtable_by_agg();

    bool need_flush() const;

    bool need_agg() const;

    Status to_block(std::unique_ptr<Block>* res);

    ConstAllocatedLsnVectorSharedPtr allocated_lsns() const { return _output_allocated_lsns; }
    Status to_flush_output(MemTableFlushOutput* output);

    SparseFlushCapability sparse_flush_capability_for_test(size_t duplicate_keys = 0) const;

    bool empty() const { return _input_mutable_block.rows() == 0; }

    const MemTableStat& stat() { return _stat; }
    void set_peak_memory_bytes(int64_t bytes) { _stat.memtable_peak_memory_bytes = bytes; }

    std::shared_ptr<ResourceContext> resource_ctx() { return _resource_ctx; }

    std::shared_ptr<MemTracker> mem_tracker() { return _mem_tracker; }

    void set_flush_success() { _is_flush_success = true; }

    MemType get_mem_type() { return _mem_type; }

    void update_mem_type(MemType memtype) { _mem_type = memtype; }

    int64_t raw_rows() { return _stat.raw_rows.load(); }

private:
    enum class SparseState : uint8_t {
        DISABLED,
        COLLECTING_SAMPLE,
        ALL_NULL,
        SPARSE,
        PERMANENT_DENSE
    };

    struct SparseNullableSidecar {
        SparseState state = SparseState::DISABLED;
        uint64_t non_null_count = 0;
        size_t dense_bytes_per_row = 0;
        SparseColumnData data;

        bool encoded() const {
            return state == SparseState::ALL_NULL || state == SparseState::SPARSE;
        }
    };

    struct SparseOptions {
        size_t sample_rows = 0;
        double max_non_null_ratio = 0;
        double min_saving_ratio = 0;
        size_t materialize_limit_bytes = 0;
        uint64_t max_rows = 0;
    };

    // for vectorized
    template <bool has_skip_bitmap_col>
    void _aggregate_two_row_in_block(MutableBlock& mutable_block, RowInBlock* new_row,
                                     RowInBlock* row_in_skiplist);

    // Merge allocated LSN sidecar only when MemTable merges two RowInBlock objects.
    // Table models that require complex merge semantics, such as AGG tables and unique key
    // merge-on-read tables, do not support allocated LSN now and are rejected in insert().
    void _merge_allocated_lsn(RowInBlock* src_row, RowInBlock* dst_row);

    void _append_output_allocated_lsn(RowInBlock* row);

    void _aggregate_two_row_with_sequence_map(MutableBlock& mutable_block, RowInBlock* new_row,
                                              RowInBlock* row_in_skiplist);

    int64_t _adaptive_write_buffer_size() const;
    Status _to_flush_output(MemTableFlushOutput* output);

private:
    std::atomic<MemType> _mem_type;
    int64_t _tablet_id;
    bool _enable_unique_key_mow = false;
    bool _is_flush_success = false;
    UniqueKeyUpdateModePB _partial_update_mode {UniqueKeyUpdateModePB::UPSERT};
    const KeysType _keys_type;
    std::shared_ptr<TabletSchema> _tablet_schema;

    std::shared_ptr<RowInBlockComparator> _vec_row_comparator;

    std::shared_ptr<ResourceContext> _resource_ctx;

    std::shared_ptr<MemTracker> _mem_tracker;
    // Only the rows will be inserted into block can allocate memory from _arena.
    // In this way, we can make MemTable::memory_usage() to be more accurate, and eventually
    // reduce the number of segment files that are generated by current load
    Arena _arena;
    int64_t _load_mem_limit = -1;

    void _init_columns_offset_by_slot_descs(const std::vector<SlotDescriptor*>* slot_descs,
                                            const TupleDescriptor* tuple_desc);
    std::vector<int> _column_offset;
    int32_t _row_lsn_col_pos = -1;
    std::vector<int> _schema_column_index;

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
    size_t _last_agg_pos = 0;

    //return number of same keys
    size_t _sort();
    Status _sort_by_cluster_keys();
    void _sort_one_column(DorisVector<std::shared_ptr<RowInBlock>>& row_in_blocks, Tie& tie,
                          std::function<int(RowInBlock*, RowInBlock*)> cmp);
    template <bool is_final>
    void _finalize_one_row(RowInBlock* row, MutableBlock& mutable_block, int row_pos);
    void _init_row_for_agg(RowInBlock* row, MutableBlock& mutable_block);
    void _clear_row_agg(RowInBlock* row);

    template <bool is_final, bool has_skip_bitmap_col = false>
    void _aggregate();

    template <bool is_final>
    void _aggregate_for_flexible_partial_update_without_seq_col(
            MutableBlock& mutable_block,
            DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks);

    template <bool is_final>
    void _aggregate_for_flexible_partial_update_with_seq_col(
            MutableBlock& mutable_block,
            DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks);

    Status _put_into_flush_output(size_t same_keys_num, bool use_sparse_output,
                                  MemTableFlushOutput* output);
    Status _put_into_output(Block& in_block);
    Status _put_into_sparse_output(Block& in_block,
                                   std::shared_ptr<const SparseFlushOutput>* sparse_output);
    SparseFlushCapability _sparse_flush_capability(size_t duplicate_keys) const;
    SparseFlushCapability _sparse_flush_static_capability() const;
    void _record_sparse_flush_fallback(SparseFlushCapabilityReason reason) const;
    void _init_sparse_columns(const Block& clone_block);
    static void _append_sparse_column_data(SparseNullableSidecar& sidecar, const IColumn& src,
                                           const DorisVector<uint32_t>& row_idxs);
    Status _append_rows_by_sparse_state(const Block* input_block,
                                        const DorisVector<uint32_t>& row_idxs);
    void _make_sparse_column_decisions();
    Status _materialize_sparse_column(size_t column_idx, bool fallback);
    Status _materialize_sparse_columns();
    bool _is_sparse_supported_type(FieldType type) const;
    size_t _sparse_sidecar_bytes() const;
    size_t _dense_equivalent_bytes() const;
    size_t _effective_write_buffer_size() const;
#ifndef NDEBUG
    void _check_sparse_invariants() const;
#endif
    bool _is_first_insertion;

    void _init_agg_functions(const Block* block);
    std::vector<AggregateFunctionPtr> _agg_functions;
    std::vector<size_t> _offsets_of_aggregate_states;
    size_t _total_size_of_aggregate_states;
    std::unique_ptr<DorisVector<std::shared_ptr<RowInBlock>>> _row_in_blocks;

    size_t _num_columns;
    int32_t _seq_col_idx_in_block {-1};
    int32_t _skip_bitmap_col_idx {-1};
    int32_t _delete_sign_col_idx {-1};
    int32_t _delete_sign_col_unique_id {-1};
    int32_t _seq_col_unique_id {-1};

    bool _is_partial_update_and_auto_inc = false;
    bool _enable_sparse = false;
    const bool _enable_sparse_snapshot;
    const SparseOptions _sparse_options;
    std::vector<SparseNullableSidecar> _sparse_columns;
}; // class MemTable

} // namespace doris
