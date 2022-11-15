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

#include <thrift/protocol/TDebugProtocol.h>

#include "exprs/bloomfilter_predicate.h"
#include "exprs/function_filter.h"
#include "exprs/hybrid_set.h"
#include "olap/delete_handler.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"
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
        TabletSchemaSPtr tablet_schema;
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
        std::vector<std::pair<string, std::shared_ptr<BloomFilterFuncBase>>> bloom_filters;
        std::vector<std::pair<string, std::shared_ptr<HybridSetBase>>> in_filters;
        std::vector<FunctionFilter> function_filters;
        std::vector<RowsetMetaSharedPtr> delete_predicates;

        // For unique key table with merge-on-write
        DeleteBitmap* delete_bitmap {nullptr};

        std::vector<RowsetReaderSharedPtr> rs_readers;
        std::vector<uint32_t> return_columns;
        RuntimeProfile* profile = nullptr;
        RuntimeState* runtime_state = nullptr;

        // use only in vec exec engine
        std::vector<uint32_t>* origin_return_columns = nullptr;
        std::unordered_set<uint32_t>* tablet_columns_convert_to_null_set = nullptr;
        TPushAggOp::type push_down_agg_type_opt = TPushAggOp::NONE;

        // used for comapction to record row ids
        bool record_rowids = false;
        // used for special optimization for query : ORDER BY key LIMIT n
        bool read_orderby_key = false;
        // used for special optimization for query : ORDER BY key DESC LIMIT n
        bool read_orderby_key_reverse = false;
        // num of columns for orderby key
        size_t read_orderby_key_num_prefix_columns = 0;

        void check_validation() const;

        std::string to_string() const;
    };

    TabletReader() = default;

    virtual ~TabletReader();

    TabletReader(const TabletReader&) = delete;
    void operator=(const TabletReader&) = delete;

    // Initialize TabletReader with tablet, data version and fetch range.
    virtual Status init(const ReaderParams& read_params);

    // Read next row with aggregation.
    // Return OLAP_SUCCESS and set `*eof` to false when next row is read into `row_cursor`.
    // Return OLAP_SUCCESS and set `*eof` to true when no more rows can be read.
    // Return others when unexpected error happens.
    virtual Status next_row_with_aggregation(RowCursor* row_cursor, MemPool* mem_pool,
                                             ObjectPool* agg_pool, bool* eof) = 0;

    // Read next block with aggregation.
    // Return OLAP_SUCCESS and set `*eof` to false when next block is read
    // Return OLAP_SUCCESS and set `*eof` to true when no more rows can be read.
    // Return others when unexpected error happens.
    // TODO: Rethink here we still need mem_pool and agg_pool?
    virtual Status next_block_with_aggregation(vectorized::Block* block, MemPool* mem_pool,
                                               ObjectPool* agg_pool, bool* eof) {
        return Status::OLAPInternalError(OLAP_ERR_READER_INITIALIZE_ERROR);
    }

    uint64_t merged_rows() const { return _merged_rows; }

    uint64_t filtered_rows() const {
        return _stats.rows_del_filtered + _stats.rows_del_by_bitmap +
               _stats.rows_conditions_filtered + _stats.rows_vec_del_cond_filtered +
               _stats.rows_vec_cond_filtered;
    }

    void set_batch_size(int batch_size) { _batch_size = batch_size; }

    const OlapReaderStatistics& stats() const { return _stats; }
    OlapReaderStatistics* mutable_stats() { return &_stats; }

    virtual bool update_profile(RuntimeProfile* profile) { return false; }

protected:
    friend class CollectIterator;
    friend class vectorized::VCollectIterator;
    friend class DeleteHandler;

    Status _init_params(const ReaderParams& read_params);

    Status _capture_rs_readers(const ReaderParams& read_params,
                               std::vector<RowsetReaderSharedPtr>* valid_rs_readers);

    bool _optimize_for_single_rowset(const std::vector<RowsetReaderSharedPtr>& rs_readers);

    Status _init_keys_param(const ReaderParams& read_params);

    Status _init_orderby_keys_param(const ReaderParams& read_params);

    void _init_conditions_param(const ReaderParams& read_params);

    ColumnPredicate* _parse_to_predicate(
            const std::pair<std::string, std::shared_ptr<BloomFilterFuncBase>>& bloom_filter);

    ColumnPredicate* _parse_to_predicate(
            const std::pair<std::string, std::shared_ptr<HybridSetBase>>& in_filter);

    virtual ColumnPredicate* _parse_to_predicate(const FunctionFilter& function_filter);

    Status _init_delete_condition(const ReaderParams& read_params);

    Status _init_return_columns(const ReaderParams& read_params);

    TabletSharedPtr tablet() { return _tablet; }
    const TabletSchema& tablet_schema() { return *_tablet_schema; }

    std::unique_ptr<MemPool> _predicate_mem_pool;
    std::vector<uint32_t> _return_columns;
    // used for special optimization for query : ORDER BY key [ASC|DESC] LIMIT n
    // columns for orderby keys
    std::vector<uint32_t> _orderby_key_columns;
    // only use in outer join which change the column nullable which must keep same in
    // vec query engine
    std::unordered_set<uint32_t>* _tablet_columns_convert_to_null_set = nullptr;

    TabletSharedPtr _tablet;
    RowsetReaderContext _reader_context;
    TabletSchemaSPtr _tablet_schema;
    KeysParam _keys_param;
    std::vector<bool> _is_lower_keys_included;
    std::vector<bool> _is_upper_keys_included;
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

    std::vector<uint32_t> _key_cids;
    std::vector<uint32_t> _value_cids;

    uint64_t _merged_rows = 0;
    OlapReaderStatistics _stats;
};

} // namespace doris
