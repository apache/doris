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

#ifndef DORIS_BE_SRC_QUERY_EXEC_OLAP_SCAN_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_OLAP_SCAN_NODE_H

#include <boost/thread.hpp>
#include <boost/variant/static_visitor.hpp>
#include <condition_variable>
#include <queue>

#include "exec/olap_common.h"
#include "exec/olap_scanner.h"
#include "exec/scan_node.h"
#include "exprs/in_predicate.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch_interface.hpp"
#include "runtime/vectorized_row_batch.h"
#include "util/progress_updater.h"
#include "util/spinlock.h"

namespace doris {

enum TransferStatus {
    READ_ROWBATCH = 1,
    INIT_HEAP = 2,
    BUILD_ROWBATCH = 3,
    MERGE = 4,
    FINISH = 5,
    ADD_ROWBATCH = 6,
    ERROR = 7
};

class OlapScanNode : public ScanNode {
public:
    OlapScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~OlapScanNode();
    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    Status collect_query_statistics(QueryStatistics* statistics) override;
    virtual Status close(RuntimeState* state);
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges);
    inline void set_no_agg_finalize() { _need_agg_finalize = false; }

protected:
    typedef struct {
        Tuple* tuple;
        int id;
    } HeapType;
    class IsFixedValueRangeVisitor : public boost::static_visitor<bool> {
    public:
        template <class T>
        bool operator()(T& v) const {
            return v.is_fixed_value_range();
        }
    };

    class GetFixedValueSizeVisitor : public boost::static_visitor<size_t> {
    public:
        template <class T>
        size_t operator()(T& v) const {
            return v.get_fixed_value_size();
        }
    };

    class ExtendScanKeyVisitor : public boost::static_visitor<Status> {
    public:
        ExtendScanKeyVisitor(OlapScanKeys& scan_keys, int32_t max_scan_key_num)
                : _scan_keys(scan_keys), _max_scan_key_num(max_scan_key_num) {}
        template <class T>
        Status operator()(T& v) {
            return _scan_keys.extend_scan_key(v, _max_scan_key_num);
        }

    private:
        OlapScanKeys& _scan_keys;
        int32_t _max_scan_key_num;
    };

    typedef boost::variant<std::list<std::string>> string_list;

    class ToOlapFilterVisitor : public boost::static_visitor<void> {
    public:
        template <class T, class P>
        void operator()(T& v, P& v2) const {
            v.to_olap_filter(v2);
        }
    };

    class MergeComparison {
    public:
        MergeComparison(CompareLargeFunc compute_fn, int offset) {
            _compute_fn = compute_fn;
            _offset = offset;
        }
        bool operator()(const HeapType& lhs, const HeapType& rhs) const {
            return (*_compute_fn)(lhs.tuple->get_slot(_offset), rhs.tuple->get_slot(_offset));
        }

    private:
        CompareLargeFunc _compute_fn;
        int _offset;
    };

    typedef std::priority_queue<HeapType, std::vector<HeapType>, MergeComparison> Heap;

    void display_heap(Heap& heap) {
        Heap h = heap;
        std::stringstream s;
        s << "Heap: [";

        while (!h.empty()) {
            HeapType v = h.top();
            s << "\nID: " << v.id << " Value:" << Tuple::to_string(v.tuple, *_tuple_desc);
            h.pop();
        }

        VLOG_CRITICAL << s.str() << "\n]";
    }

    // In order to ensure the accuracy of the query result
    // only key column conjuncts will be remove as idle conjunct
    bool is_key_column(const std::string& key_name);
    void remove_pushed_conjuncts(RuntimeState *state);

    Status start_scan(RuntimeState* state);

    void eval_const_conjuncts();
    Status normalize_conjuncts();
    Status build_olap_filters();
    Status build_scan_key();
    Status start_scan_thread(RuntimeState* state);

    template <class T>
    Status normalize_predicate(ColumnValueRange<T>& range, SlotDescriptor* slot);

    template <class T>
    Status normalize_in_and_eq_predicate(SlotDescriptor* slot, ColumnValueRange<T>* range);

    template <class T>
    Status normalize_not_in_and_not_eq_predicate(SlotDescriptor* slot, ColumnValueRange<T>* range);

    template <class T>
    Status normalize_noneq_binary_predicate(SlotDescriptor* slot, ColumnValueRange<T>* range);

    template <typename T>
    static bool normalize_is_null_predicate(Expr* expr, SlotDescriptor* slot,
            const std::string& is_null_str, ColumnValueRange<T>* range);

    void transfer_thread(RuntimeState* state);
    void scanner_thread(OlapScanner* scanner);

    Status add_one_batch(RowBatchInterface* row_batch);

    // Write debug string of this into out.
    virtual void debug_string(int indentation_level, std::stringstream* out) const;

private:
    void _init_counter(RuntimeState* state);
    // OLAP_SCAN_NODE profile layering: OLAP_SCAN_NODE, OlapScanner, and SegmentIterator
    // according to the calling relationship
    void init_scan_profile();

    bool should_push_down_in_predicate(SlotDescriptor* slot, InPredicate* in_pred);

    std::pair<bool, void*> should_push_down_eq_predicate(SlotDescriptor* slot, Expr* pred, int conj_idx, int child_idx);

    template <typename T, typename ChangeFixedValueRangeFunc>
    static Status change_fixed_value_range(ColumnValueRange <T> &range, PrimitiveType type, void *value,
                                               const ChangeFixedValueRangeFunc& func);

    friend class OlapScanner;

    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;
    // doris scan node used to scan doris
    TOlapScanNode _olap_scan_node;
    // tuple descriptors
    const TupleDescriptor* _tuple_desc;
    // tuple index
    int _tuple_idx;
    // string slots
    std::vector<SlotDescriptor*> _string_slots;
    // conjunct's index which already be push down storage engine
    // should be remove in olap_scan_node, no need check this conjunct again
    std::set<uint32_t> _pushed_conjuncts_index;

    bool _eos;

    // column -> ColumnValueRange map
    std::map<std::string, ColumnValueRangeType> _column_value_ranges;

    OlapScanKeys _scan_keys;

    std::vector<std::unique_ptr<TPaloScanRange>> _scan_ranges;

    std::vector<TCondition> _olap_filter;

    // Pool for storing allocated scanner objects.  We don't want to use the
    // runtime pool to ensure that the scanner objects are deleted before this
    // object is.
    std::unique_ptr<ObjectPool> _scanner_pool;

    boost::thread_group _transfer_thread;

    // Keeps track of total splits and the number finished.
    ProgressUpdater _progress;

    // Lock and condition variables protecting _materialized_row_batches.  Row batches are
    // produced asynchronously by the scanner threads and consumed by the main thread in
    // GetNext.  Row batches must be processed by the main thread in the order they are
    // queued to avoid freeing attached resources prematurely (row batches will never depend
    // on resources attached to earlier batches in the queue).
    // This lock cannot be taken together with any other locks except _lock.
    std::mutex _row_batches_lock;
    std::condition_variable _row_batch_added_cv;
    std::condition_variable _row_batch_consumed_cv;

    std::list<RowBatchInterface*> _materialized_row_batches;

    std::mutex _scan_batches_lock;
    std::condition_variable _scan_batch_added_cv;
    int64_t _running_thread = 0;
    std::condition_variable _scan_thread_exit_cv;

    std::list<RowBatchInterface*> _scan_row_batches;

    std::list<OlapScanner*> _olap_scanners;

    int _max_materialized_row_batches;
    bool _start;
    bool _scanner_done;
    bool _transfer_done;
    size_t _direct_conjunct_size;

    int _total_assign_num;
    int _nice;

    // protect _status, for many thread may change _status
    SpinLock _status_mutex;
    Status _status;
    RuntimeState* _runtime_state;
    RuntimeProfile::Counter* _scan_timer;
    RuntimeProfile::Counter* _scan_cpu_timer = nullptr;
    RuntimeProfile::Counter* _tablet_counter;
    RuntimeProfile::Counter* _rows_pushed_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _reader_init_timer = nullptr;

    TResourceInfo* _resource_info;

    int64_t _buffered_bytes;
    EvalConjunctsFn _eval_conjuncts_fn;

    bool _need_agg_finalize = true;

    // the max num of scan keys of this scan request.
    // it will set as BE's config `doris_max_scan_key_num`,
    // or be overwritten by value in TQueryOptions
    int32_t _max_scan_key_num = 1024;
    // The max number of conditions in InPredicate  that can be pushed down
    // into OlapEngine.
    // If conditions in InPredicate is larger than this, all conditions in
    // InPredicate will not be pushed to the OlapEngine.
    // it will set as BE's config `max_pushdown_conditions_per_column`,
    // or be overwritten by value in TQueryOptions
    int32_t _max_pushdown_conditions_per_column = 1024;

    std::unique_ptr<RuntimeProfile> _scanner_profile;
    std::unique_ptr<RuntimeProfile> _segment_profile;

    // Counters
    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _read_compressed_counter = nullptr;
    RuntimeProfile::Counter* _decompressor_timer = nullptr;
    RuntimeProfile::Counter* _read_uncompressed_counter = nullptr;
    RuntimeProfile::Counter* _raw_rows_counter = nullptr;

    RuntimeProfile::Counter* _rows_vec_cond_counter = nullptr;
    RuntimeProfile::Counter* _vec_cond_timer = nullptr;

    RuntimeProfile::Counter* _stats_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bf_filtered_counter = nullptr;
    RuntimeProfile::Counter* _del_filtered_counter = nullptr;
    RuntimeProfile::Counter* _conditions_filtered_counter = nullptr;
    RuntimeProfile::Counter* _key_range_filtered_counter = nullptr;

    RuntimeProfile::Counter* _block_seek_timer = nullptr;
    RuntimeProfile::Counter* _block_seek_counter = nullptr;
    RuntimeProfile::Counter* _block_convert_timer = nullptr;
    RuntimeProfile::Counter* _block_load_timer = nullptr;
    RuntimeProfile::Counter* _block_load_counter = nullptr;
    RuntimeProfile::Counter* _block_fetch_timer = nullptr;

    RuntimeProfile::Counter* _index_load_timer = nullptr;

    // total pages read
    // used by segment v2
    RuntimeProfile::Counter* _total_pages_num_counter = nullptr;
    // page read from cache
    // used by segment v2
    RuntimeProfile::Counter* _cached_pages_num_counter = nullptr;

    // row count filtered by bitmap inverted index
    RuntimeProfile::Counter* _bitmap_index_filter_counter = nullptr;
    // time fro bitmap inverted index read and filter
    RuntimeProfile::Counter* _bitmap_index_filter_timer = nullptr;
    // number of created olap scanners
    RuntimeProfile::Counter* _num_scanners = nullptr;

    // number of segment filtered by column stat when creating seg iterator
    RuntimeProfile::Counter* _filtered_segment_counter = nullptr;
    // total number of segment related to this scan node
    RuntimeProfile::Counter* _total_segment_counter = nullptr;

    RuntimeProfile::Counter* _scanner_wait_batch_timer = nullptr;
    RuntimeProfile::Counter* _scanner_wait_worker_timer = nullptr;
};

} // namespace doris

#endif
