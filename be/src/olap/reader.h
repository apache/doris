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

#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exprs/function_filter.h"
#include "gutil/strings/substitute.h"
#include "io/io_common.h"
#include "olap/delete_handler.h"
#include "olap/iterators.h"
#include "olap/olap_common.h"
#include "olap/olap_tuple.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/tablet_fwd.h"

namespace doris {

class RuntimeState;
class BitmapFilterFuncBase;
class BloomFilterFuncBase;
class ColumnPredicate;
class DeleteBitmap;
class HybridSetBase;
class RuntimeProfile;

namespace vectorized {
class VCollectIterator;
class Block;
class VExpr;
class Arena;
class VExprContext;
} // namespace vectorized

// Used to compare row with input scan key. Scan key only contains key columns,
// row contains all key columns, which is superset of key columns.
// So we should compare the common prefix columns of lhs and rhs.
//
// NOTE: if you are not sure if you can use it, please don't use this function.
inline int compare_row_key(const RowCursor& lhs, const RowCursor& rhs) {
    auto cmp_cids = std::min(lhs.schema()->num_column_ids(), rhs.schema()->num_column_ids());
    for (uint32_t cid = 0; cid < cmp_cids; ++cid) {
        auto res = lhs.schema()->column(cid)->compare_cell(lhs.cell(cid), rhs.cell(cid));
        if (res != 0) {
            return res;
        }
    }
    return 0;
}

class TabletReader {
    struct KeysParam {
        std::string to_string() const;

        std::vector<RowCursor> start_keys;
        std::vector<RowCursor> end_keys;
        bool start_key_include = false;
        bool end_key_include = false;
    };

public:
    struct ReadSource {
        std::vector<RowSetSplits> rs_splits;
        std::vector<RowsetMetaSharedPtr> delete_predicates;
        // Fill delete predicates with `rs_splits`
        void fill_delete_predicates();
    };
    // Params for Reader,
    // mainly include tablet, data version and fetch range.
    struct ReaderParams {
        bool has_single_version() const {
            return (rs_splits.size() == 1 &&
                    rs_splits[0].rs_reader->rowset()->start_version() == 0 &&
                    !rs_splits[0].rs_reader->rowset()->rowset_meta()->is_segments_overlapping()) ||
                   (rs_splits.size() == 2 &&
                    rs_splits[0].rs_reader->rowset()->rowset_meta()->num_rows() == 0 &&
                    rs_splits[1].rs_reader->rowset()->start_version() == 2 &&
                    !rs_splits[1].rs_reader->rowset()->rowset_meta()->is_segments_overlapping());
        }

        void set_read_source(ReadSource read_source) {
            rs_splits = std::move(read_source.rs_splits);
            delete_predicates = std::move(read_source.delete_predicates);
        }

        BaseTabletSPtr tablet;
        TabletSchemaSPtr tablet_schema;
        ReaderType reader_type = ReaderType::READER_QUERY;
        bool direct_mode = false;
        bool aggregation = false;
        // for compaction, schema_change, check_sum: we don't use page cache
        // for query and config::disable_storage_page_cache is false, we use page cache
        bool use_page_cache = false;
        Version version = Version(-1, 0);

        std::vector<OlapTuple> start_key;
        std::vector<OlapTuple> end_key;
        bool start_key_include = false;
        bool end_key_include = false;

        std::vector<TCondition> conditions;
        std::vector<std::pair<string, std::shared_ptr<BloomFilterFuncBase>>> bloom_filters;
        std::vector<std::pair<string, std::shared_ptr<BitmapFilterFuncBase>>> bitmap_filters;
        std::vector<std::pair<string, std::shared_ptr<HybridSetBase>>> in_filters;
        std::vector<TCondition> conditions_except_leafnode_of_andnode;
        std::vector<FunctionFilter> function_filters;
        std::vector<RowsetMetaSharedPtr> delete_predicates;

        std::vector<RowSetSplits> rs_splits;
        // For unique key table with merge-on-write
        DeleteBitmap* delete_bitmap {nullptr};

        // return_columns is init from query schema
        std::vector<uint32_t> return_columns;
        // output_columns only contain columns in OrderByExprs and outputExprs
        std::set<int32_t> output_columns;
        RuntimeProfile* profile = nullptr;
        RuntimeState* runtime_state = nullptr;

        // use only in vec exec engine
        std::vector<uint32_t>* origin_return_columns = nullptr;
        std::unordered_set<uint32_t>* tablet_columns_convert_to_null_set = nullptr;
        TPushAggOp::type push_down_agg_type_opt = TPushAggOp::NONE;
        vectorized::VExpr* remaining_vconjunct_root = nullptr;
        std::vector<vectorized::VExprSPtr> remaining_conjunct_roots;
        vectorized::VExprContextSPtrs common_expr_ctxs_push_down;

        // used for compaction to record row ids
        bool record_rowids = false;
        // flag for enable topn opt
        bool use_topn_opt = false;
        // used for special optimization for query : ORDER BY key LIMIT n
        bool read_orderby_key = false;
        // used for special optimization for query : ORDER BY key DESC LIMIT n
        bool read_orderby_key_reverse = false;
        // num of columns for orderby key
        size_t read_orderby_key_num_prefix_columns = 0;
        // limit of rows for read_orderby_key
        size_t read_orderby_key_limit = 0;
        // filter_block arguments
        vectorized::VExprContextSPtrs filter_block_conjuncts;

        // for vertical compaction
        bool is_key_column_group = false;

        bool is_segcompaction = false;

        std::vector<RowwiseIteratorUPtr>* segment_iters_ptr = nullptr;

        void check_validation() const;

        std::string to_string() const;
    };

    TabletReader() = default;

    virtual ~TabletReader();

    TabletReader(const TabletReader&) = delete;
    void operator=(const TabletReader&) = delete;

    // Initialize TabletReader with tablet, data version and fetch range.
    virtual Status init(const ReaderParams& read_params);

    // Read next block with aggregation.
    // Return OK and set `*eof` to false when next block is read
    // Return OK and set `*eof` to true when no more rows can be read.
    // Return others when unexpected error happens.
    virtual Status next_block_with_aggregation(vectorized::Block* block, bool* eof) {
        return Status::Error<ErrorCode::READER_INITIALIZE_ERROR>(
                "TabletReader not support next_block_with_aggregation");
    }

    virtual uint64_t merged_rows() const { return _merged_rows; }

    uint64_t filtered_rows() const {
        return _stats.rows_del_filtered + _stats.rows_del_by_bitmap +
               _stats.rows_conditions_filtered + _stats.rows_vec_del_cond_filtered +
               _stats.rows_vec_cond_filtered + _stats.rows_short_circuit_cond_filtered;
    }

    void set_batch_size(int batch_size) { _reader_context.batch_size = batch_size; }

    int batch_size() const { return _reader_context.batch_size; }

    const OlapReaderStatistics& stats() const { return _stats; }
    OlapReaderStatistics* mutable_stats() { return &_stats; }

    virtual bool update_profile(RuntimeProfile* profile) { return false; }
    static Status init_reader_params_and_create_block(
            TabletSharedPtr tablet, ReaderType reader_type,
            const std::vector<RowsetSharedPtr>& input_rowsets,
            TabletReader::ReaderParams* reader_params, vectorized::Block* block);

protected:
    friend class vectorized::VCollectIterator;
    friend class DeleteHandler;

    Status _init_params(const ReaderParams& read_params);

    Status _capture_rs_readers(const ReaderParams& read_params);

    bool _optimize_for_single_rowset(const std::vector<RowsetReaderSharedPtr>& rs_readers);

    Status _init_keys_param(const ReaderParams& read_params);

    Status _init_orderby_keys_param(const ReaderParams& read_params);

    Status _init_conditions_param(const ReaderParams& read_params);

    void _init_conditions_param_except_leafnode_of_andnode(const ReaderParams& read_params);

    ColumnPredicate* _parse_to_predicate(
            const std::pair<std::string, std::shared_ptr<BloomFilterFuncBase>>& bloom_filter);

    ColumnPredicate* _parse_to_predicate(
            const std::pair<std::string, std::shared_ptr<BitmapFilterFuncBase>>& bitmap_filter);

    ColumnPredicate* _parse_to_predicate(
            const std::pair<std::string, std::shared_ptr<HybridSetBase>>& in_filter);

    virtual ColumnPredicate* _parse_to_predicate(const FunctionFilter& function_filter);

    Status _init_delete_condition(const ReaderParams& read_params);

    Status _init_return_columns(const ReaderParams& read_params);

    const BaseTabletSPtr& tablet() { return _tablet; }
    const TabletSchema& tablet_schema() { return *_tablet_schema; }

    std::unique_ptr<vectorized::Arena> _predicate_arena;
    std::vector<uint32_t> _return_columns;
    // used for special optimization for query : ORDER BY key [ASC|DESC] LIMIT n
    // columns for orderby keys
    std::vector<uint32_t> _orderby_key_columns;
    // only use in outer join which change the column nullable which must keep same in
    // vec query engine
    std::unordered_set<uint32_t>* _tablet_columns_convert_to_null_set = nullptr;

    BaseTabletSPtr _tablet;
    RowsetReaderContext _reader_context;
    TabletSchemaSPtr _tablet_schema;
    KeysParam _keys_param;
    std::vector<bool> _is_lower_keys_included;
    std::vector<bool> _is_upper_keys_included;
    std::vector<ColumnPredicate*> _col_predicates;
    std::vector<ColumnPredicate*> _col_preds_except_leafnode_of_andnode;
    std::vector<ColumnPredicate*> _value_col_predicates;
    DeleteHandler _delete_handler;

    bool _aggregation = false;
    // for agg query, we don't need to finalize when scan agg object data
    ReaderType _reader_type = ReaderType::READER_QUERY;
    bool _next_delete_flag = false;
    bool _filter_delete = false;
    int32_t _sequence_col_idx = -1;
    bool _direct_mode = false;

    std::vector<uint32_t> _key_cids;
    std::vector<uint32_t> _value_cids;

    uint64_t _merged_rows = 0;
    OlapReaderStatistics _stats;
};

} // namespace doris
