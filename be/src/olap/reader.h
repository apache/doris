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

#ifndef DORIS_BE_SRC_OLAP_READER_H
#define DORIS_BE_SRC_OLAP_READER_H

#include <gen_cpp/PaloInternalService_types.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <list>
#include <memory>
#include <queue>
#include <sstream>
#include <stack>
#include <string>
#include <utility>
#include <vector>

#include "exprs/bloomfilter_predicate.h"
#include "olap/column_predicate.h"
#include "olap/delete_handler.h"
#include "olap/olap_cond.h"
#include "olap/olap_define.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/scan_range.h"
#include "olap/tablet.h"
#include "util/runtime_profile.h"

namespace doris {

class Tablet;
class RowCursor;
class RowBlock;
class CollectIterator;
class RuntimeState;

// Params for Reader,
// mainly include tablet, data version and fetch range.
struct ReaderParams {
    TabletSharedPtr tablet;
    ReaderType reader_type = READER_QUERY;
    bool aggregation = false;
    bool need_agg_finalize = true;
    // 1. when read column data page:
    //     for compaction, schema_change, check_sum: we don't use page cache
    //     for query and config::disable_storage_page_cache is false, we use page cache
    // 2. when read column index page
    //     if config::disable_storage_page_cache is false, we use page cache
    bool use_page_cache = false;
    Version version = Version(-1, 0);
    // possible values are "gt", "ge", "eq"
    std::string range;
    // possible values are "lt", "le"
    std::string end_range;
    std::vector<OlapTuple> start_key;
    std::vector<OlapTuple> end_key;

    std::vector<TCondition> conditions;
    std::vector<std::pair<string, std::shared_ptr<IBloomFilterFuncBase>>> bloom_filters;

    // The ColumnData will be set when using Merger, eg Cumulative, BE.
    std::vector<RowsetReaderSharedPtr> rs_readers;
    std::vector<uint32_t> return_columns;
    RuntimeProfile* profile = nullptr;
    RuntimeState* runtime_state = nullptr;

    ScanRangeType range_type;
    std::map<RowsetId, std::set<uint64_t>> segments;

    void check_validation() const;

    std::string to_string() const;
};

class Reader {
public:
    Reader();
    ~Reader();

    // Initialize Reader with tablet, data version and fetch range.
    OLAPStatus init(const ReaderParams& read_params);

    void close();

    // Reader next row with aggregation.
    // Return OLAP_SUCCESS and set `*eof` to false when next row is read into `row_cursor`.
    // Return OLAP_SUCCESS and set `*eof` to true when no more rows can be read.
    // Return others when unexpected error happens.
    OLAPStatus next_row_with_aggregation(RowCursor* row_cursor, MemPool* mem_pool,
                                         ObjectPool* agg_pool, bool* eof) {
        return (this->*_next_row_func)(row_cursor, mem_pool, agg_pool, eof);
    }

    uint64_t merged_rows() const { return _merged_rows; }

    uint64_t filtered_rows() const {
        return _stats.rows_del_filtered + _stats.rows_conditions_filtered +
               _stats.rows_vec_del_cond_filtered;
    }

    const OlapReaderStatistics& stats() const { return _stats; }
    OlapReaderStatistics* mutable_stats() { return &_stats; }

private:
    struct KeysParam {
        ~KeysParam();

        std::string to_string() const;

        std::string range;
        std::string end_range;
        std::vector<RowCursor*> start_keys;
        std::vector<RowCursor*> end_keys;
    };

    friend class CollectIterator;
    friend class DeleteHandler;

    OLAPStatus _init_params(const ReaderParams& read_params);

    OLAPStatus _capture_rs_readers(const ReaderParams& read_params,
                                   std::vector<RowsetReaderSharedPtr>* valid_rs_readers);

    bool _optimize_for_single_rowset(const std::vector<RowsetReaderSharedPtr>& rs_readers);

    OLAPStatus _init_keys_param(const ReaderParams& read_params);

    void _init_conditions_param(const ReaderParams& read_params);

    ColumnPredicate* _new_eq_pred(const TabletColumn& column, int index, const std::string& cond,
                                  bool opposite) const;
    ColumnPredicate* _new_ne_pred(const TabletColumn& column, int index, const std::string& cond,
                                  bool opposite) const;
    ColumnPredicate* _new_lt_pred(const TabletColumn& column, int index, const std::string& cond,
                                  bool opposite) const;
    ColumnPredicate* _new_le_pred(const TabletColumn& column, int index, const std::string& cond,
                                  bool opposite) const;
    ColumnPredicate* _new_gt_pred(const TabletColumn& column, int index, const std::string& cond,
                                  bool opposite) const;
    ColumnPredicate* _new_ge_pred(const TabletColumn& column, int index, const std::string& cond,
                                  bool opposite) const;

    ColumnPredicate* _parse_to_predicate(const TCondition& condition, bool opposite = false) const;

    ColumnPredicate* _parse_to_predicate(
            const std::pair<std::string, std::shared_ptr<IBloomFilterFuncBase>>& bloom_filter);

    OLAPStatus _init_delete_condition(const ReaderParams& read_params);

    OLAPStatus _init_return_columns(const ReaderParams& read_params);
    void _init_seek_columns();

    void _init_load_bf_columns(const ReaderParams& read_params);

    // Direcly read row from rowset and pass to upper caller. No need to do aggregation.
    // This is usually used for DUPLICATE KEY tables
    OLAPStatus _direct_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool,
                                bool* eof);
    // Just same as _direct_next_row, but this is only for AGGREGATE KEY tables.
    // And this is an optimization for AGGR tables.
    // When there is only one rowset and is not overlapping, we can read it directly without aggregation.
    OLAPStatus _direct_agg_key_next_row(RowCursor* row_cursor, MemPool* mem_pool,
                                        ObjectPool* agg_pool, bool* eof);
    // For normal AGGREGATE KEY tables, read data by a merge heap.
    OLAPStatus _agg_key_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool,
                                 bool* eof);
    // For UNIQUE KEY tables, read data by a merge heap.
    // The difference from _agg_key_next_row is that it will read the data from high version to low version,
    // to minimize the comparison time in merge heap.
    OLAPStatus _unique_key_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool,
                                    bool* eof);

    TabletSharedPtr tablet() { return _tablet; }

private:
    std::shared_ptr<MemTracker> _tracker;
    std::unique_ptr<MemPool> _predicate_mem_pool;
    std::set<uint32_t> _load_bf_columns;
    std::vector<uint32_t> _return_columns;
    std::vector<uint32_t> _seek_columns;

    TabletSharedPtr _tablet;
    RowsetReaderContext _reader_context;
    KeysParam _keys_param;
    std::vector<bool> _is_lower_keys_included;
    std::vector<bool> _is_upper_keys_included;
    Conditions _conditions;
    std::vector<ColumnPredicate*> _col_predicates;
    std::vector<ColumnPredicate*> _value_col_predicates;
    DeleteHandler _delete_handler;

    OLAPStatus (Reader::*_next_row_func)(RowCursor* row_cursor, MemPool* mem_pool,
                                         ObjectPool* agg_pool, bool* eof) = nullptr;

    bool _aggregation = false;
    // for agg query, we don't need to finalize when scan agg object data
    bool _need_agg_finalize = true;
    ReaderType _reader_type = READER_QUERY;
    bool _next_delete_flag = false;
    bool _filter_delete = false;
    bool _has_sequence_col = false;
    int32_t _sequence_col_idx = -1;
    const RowCursor* _next_key = nullptr;
    std::unique_ptr<CollectIterator> _collect_iter;
    std::vector<uint32_t> _key_cids;
    std::vector<uint32_t> _value_cids;

    uint64_t _merged_rows = 0;
    OlapReaderStatistics _stats;

    DISALLOW_COPY_AND_ASSIGN(Reader);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_READER_H
