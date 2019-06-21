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
#include <list>
#include <memory>
#include <queue>
#include <sstream>
#include <stack>
#include <string>
#include <thrift/protocol/TDebugProtocol.h>
#include <utility>
#include <vector>

#include "olap/delete_handler.h"
#include "olap/olap_cond.h"
#include "olap/olap_define.h"
#include "olap/row_cursor.h"
#include "util/runtime_profile.h"

#include "olap/column_predicate.h"
#include "olap/tablet.h"
#include "olap/rowset/rowset_reader.h"

namespace doris {

class Tablet;
class RowCursor;
class RowBlock;
class CollectIterator;
class RuntimeState;
class ColumnData;

// Params for Reader,
// mainly include tablet, data version and fetch range.
struct ReaderParams {
    TabletSharedPtr tablet;
    ReaderType reader_type;
    bool aggregation;
    Version version;
    std::string range;
    std::string end_range;
    std::vector<OlapTuple> start_key;
    std::vector<OlapTuple> end_key;
    std::vector<TCondition> conditions;
    // The ColumnData will be set when using Merger, eg Cumulative, BE.
    std::vector<RowsetReaderSharedPtr> rs_readers;
    std::vector<uint32_t> return_columns;
    RuntimeProfile* profile;
    RuntimeState* runtime_state;

    ReaderParams() :
            reader_type(READER_QUERY),
            aggregation(false),
            version(-1, 0),
            profile(NULL),
            runtime_state(NULL) {
        start_key.clear();
        end_key.clear();
        conditions.clear();
        rs_readers.clear();
    }

    void check_validation() const {
        if (version.first == -1) {
            LOG(FATAL) << "verison is not set. tablet=" << tablet->full_name();
        }
    }

    std::string to_string() {
        std::stringstream ss;

        ss << "tablet=" << tablet->full_name()
           << " reader_type=" << reader_type
           << " aggregation=" << aggregation
           << " version=" << version.first << "-" << version.second
           << " range=" << range
           << " end_range=" << end_range;

        for (auto& key : start_key) {
            ss << " keys=" << key;
        }

        for (auto& key : end_key){
            ss << " end_keys=" << key;
        }

        for (int i = 0, size = conditions.size(); i < size; ++i) {
            ss << " conditions=" << apache::thrift::ThriftDebugString(conditions[i]);
        }
        
        return ss.str();
    }
};

class Reader {
public:
    Reader();
    ~Reader();

    // Initialize Reader with tablet, data version and fetch range.
    OLAPStatus init(const ReaderParams& read_params);

    void close();

    // Reader next row with aggregation.
    OLAPStatus next_row_with_aggregation(RowCursor *row_cursor, bool *eof) {
        return (this->*_next_row_func)(row_cursor, eof);
    }

    uint64_t merged_rows() const {
        return _merged_rows;
    }

    uint64_t filtered_rows() const {
        return _stats.rows_del_filtered;
    }

    const OlapReaderStatistics& stats() const { return _stats; }
    OlapReaderStatistics* mutable_stats() { return &_stats; }

private:
    struct KeysParam {
        ~KeysParam() {
            for (int32_t i = 0; i < start_keys.size(); i++) {
                SAFE_DELETE(start_keys[i]);
            }

            for (int32_t i = 0; i < end_keys.size(); i++) {
                SAFE_DELETE(end_keys[i]);
            }
        }

        std::string to_string() const {
            std::stringstream ss;
            ss << "range=" << range
               << " end_range=" << end_range;

            for (int i = 0, size = this->start_keys.size(); i < size; ++i) {
                ss << " keys=" << start_keys[i]->to_string();
            }

            for (int i = 0, size = this->end_keys.size(); i < size; ++i) {
                ss << " end_keys=" << end_keys[i]->to_string();
            }

            return ss.str();
        }

        std::string range;
        std::string end_range;
        std::vector<RowCursor*> start_keys;
        std::vector<RowCursor*> end_keys;
    };

    friend class CollectIterator;

    OLAPStatus _init_params(const ReaderParams& read_params);

    OLAPStatus _capture_rs_readers(const ReaderParams& read_params);

    OLAPStatus _init_keys_param(const ReaderParams& read_params);

    OLAPStatus _init_conditions_param(const ReaderParams& read_params);

    ColumnPredicate* _new_eq_pred(const TabletColumn& column, int index, const std::string& cond);
    ColumnPredicate* _new_ne_pred(const TabletColumn& column, int index, const std::string& cond);
    ColumnPredicate* _new_lt_pred(const TabletColumn& column, int index, const std::string& cond);
    ColumnPredicate* _new_le_pred(const TabletColumn& column, int index, const std::string& cond);
    ColumnPredicate* _new_gt_pred(const TabletColumn& column, int index, const std::string& cond);
    ColumnPredicate* _new_ge_pred(const TabletColumn& column, int index, const std::string& cond);

    ColumnPredicate* _parse_to_predicate(const TCondition& condition);

    OLAPStatus _init_delete_condition(const ReaderParams& read_params);

    OLAPStatus _init_return_columns(const ReaderParams& read_params);
    OLAPStatus _init_seek_columns();

    OLAPStatus _init_load_bf_columns(const ReaderParams& read_params);

    OLAPStatus _dup_key_next_row(RowCursor* row_cursor, bool* eof);
    OLAPStatus _agg_key_next_row(RowCursor* row_cursor, bool* eof);
    OLAPStatus _unique_key_next_row(RowCursor* row_cursor, bool* eof);

    TabletSharedPtr tablet() { return _tablet; }

private:
    std::unique_ptr<MemTracker> _tracker;
    std::unique_ptr<MemPool> _predicate_mem_pool;
    std::set<uint32_t> _load_bf_columns;
    std::vector<uint32_t> _return_columns;
    std::vector<uint32_t> _seek_columns;

    Version _version;

    TabletSharedPtr _tablet;

    // _own_rs_readers is data source that reader aquire from tablet, so we need to
    // release these when reader closing
    std::vector<RowsetReaderSharedPtr> _own_rs_readers;
    std::vector<RowsetReaderSharedPtr> _rs_readers;
    RowsetReaderContext _reader_context;

    KeysParam _keys_param;
    std::vector<bool> _is_lower_keys_included;
    std::vector<bool> _is_upper_keys_included;
    int32_t _next_key_index;

    Conditions _conditions;
    std::vector<ColumnPredicate*> _col_predicates;

    DeleteHandler _delete_handler;

    OLAPStatus (Reader::*_next_row_func)(RowCursor* row_cursor, bool* eof) = nullptr;

    bool _aggregation;
    bool _version_locked;
    ReaderType _reader_type;
    bool _next_delete_flag;
    const RowCursor* _next_key;
    CollectIterator* _collect_iter = nullptr;
    std::vector<uint32_t> _key_cids;
    std::vector<uint32_t> _value_cids;

    uint64_t _merged_rows;

    OlapReaderStatistics _stats;
    DISALLOW_COPY_AND_ASSIGN(Reader);

};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_READER_H
