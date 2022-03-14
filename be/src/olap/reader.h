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
#include "olap/collect_iterator.h"
#include "olap/delete_handler.h"
#include "olap/olap_cond.h"
#include "olap/olap_define.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/tablet.h"
#include "util/runtime_profile.h"

namespace doris {

class Tablet;
class RowCursor;
class RowBlock;
class CollectIterator;
class RuntimeState;

namespace vectorized {
class VCollectIterator;
class Block;
} // namespace vectorized

class TabletReader {
    struct KeysParam {
        std::string to_string() const;

        std::vector<RowCursor> start_keys;
        std::vector<RowCursor> end_keys;
        bool start_key_include = false;
        bool end_key_include = false;
    };
public:
    // Params for Reader,
    // mainly include tablet, data version and fetch range.
    struct ReaderParams {
        TabletSharedPtr tablet;
        ReaderType reader_type = READER_QUERY;
        bool direct_mode = false;
        bool aggregation = false;
        bool need_agg_finalize = true;
        // 1. when read column data page:
        //     for compaction, schema_change, check_sum: we don't use page cache
        //     for query and config::disable_storage_page_cache is false, we use page cache
        // 2. when read column index page
        //     if config::disable_storage_page_cache is false, we use page cache
        bool use_page_cache = false;
        Version version = Version(-1, 0);

        std::vector<OlapTuple> start_key;
        std::vector<OlapTuple> end_key;
        bool start_key_include = false;
        bool end_key_include = false;

        std::vector<TCondition> conditions;
        std::vector<std::pair<string, std::shared_ptr<IBloomFilterFuncBase>>> bloom_filters;

        // The ColumnData will be set when using Merger, eg Cumulative, BE.
        std::vector<RowsetReaderSharedPtr> rs_readers;
        std::vector<uint32_t> return_columns;
        RuntimeProfile* profile = nullptr;
        RuntimeState* runtime_state = nullptr;

        // use only in vec exec engine
        std::vector<uint32_t>* origin_return_columns = nullptr;
        std::unordered_set<uint32_t>* tablet_columns_convert_to_null_set = nullptr;

        void check_validation() const;

        std::string to_string() const;
    };

    TabletReader() = default;

    virtual ~TabletReader();

    // Initialize TabletReader with tablet, data version and fetch range.
    virtual OLAPStatus init(const ReaderParams& read_params);

    // Read next row with aggregation.
    // Return OLAP_SUCCESS and set `*eof` to false when next row is read into `row_cursor`.
    // Return OLAP_SUCCESS and set `*eof` to true when no more rows can be read.
    // Return others when unexpected error happens.
    virtual OLAPStatus next_row_with_aggregation(RowCursor* row_cursor, MemPool* mem_pool,
                                                 ObjectPool* agg_pool, bool* eof) = 0;

    // Read next block with aggregation.
    // Return OLAP_SUCCESS and set `*eof` to false when next block is read
    // Return OLAP_SUCCESS and set `*eof` to true when no more rows can be read.
    // Return others when unexpected error happens.
    // TODO: Rethink here we still need mem_pool and agg_pool?
    virtual OLAPStatus next_block_with_aggregation(vectorized::Block* block, MemPool* mem_pool,
                                                   ObjectPool* agg_pool, bool* eof) {
        return OLAP_ERR_READER_INITIALIZE_ERROR;
    }

    uint64_t merged_rows() const { return _merged_rows; }

    uint64_t filtered_rows() const {
        return _stats.rows_del_filtered + _stats.rows_conditions_filtered +
               _stats.rows_vec_del_cond_filtered;
    }

    void set_batch_size(int batch_size) {
        _batch_size = batch_size;
    }

    const OlapReaderStatistics& stats() const { return _stats; }
    OlapReaderStatistics* mutable_stats() { return &_stats; }

protected:
    friend class CollectIterator;
    friend class vectorized::VCollectIterator;
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
    void _init_load_bf_columns(const ReaderParams& read_params, Conditions* conditions,
                               std::set<uint32_t>* load_bf_columns);

    TabletSharedPtr tablet() { return _tablet; }

    std::unique_ptr<MemPool> _predicate_mem_pool;
    std::set<uint32_t> _load_bf_columns;
    std::set<uint32_t> _load_bf_all_columns;
    std::vector<uint32_t> _return_columns;
    // only use in outer join which change the column nullable which must keep same in
    // vec query engine
    std::unordered_set<uint32_t>* _tablet_columns_convert_to_null_set = nullptr;
    std::vector<uint32_t> _seek_columns;

    TabletSharedPtr _tablet;
    RowsetReaderContext _reader_context;
    KeysParam _keys_param;
    std::vector<bool> _is_lower_keys_included;
    std::vector<bool> _is_upper_keys_included;
    // contains condition on key columns in agg or unique table or all column in dup tables
    Conditions _conditions;
    // contains _conditions and condition on value columns, used for push down
    // conditions to base rowset of unique table
    Conditions _all_conditions;
    std::vector<ColumnPredicate*> _col_predicates;
    std::vector<ColumnPredicate*> _value_col_predicates;
    DeleteHandler _delete_handler;

    bool _aggregation = false;
    // for agg query, we don't need to finalize when scan agg object data
    bool _need_agg_finalize = true;
    ReaderType _reader_type = READER_QUERY;
    bool _next_delete_flag = false;
    bool _filter_delete = false;
    int32_t _sequence_col_idx = -1;
    bool _direct_mode = false;
    int _batch_size = 1024;

    CollectIterator _collect_iter;
    std::vector<uint32_t> _key_cids;
    std::vector<uint32_t> _value_cids;

    uint64_t _merged_rows = 0;
    OlapReaderStatistics _stats;

    DISALLOW_COPY_AND_ASSIGN(TabletReader);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_READER_H
