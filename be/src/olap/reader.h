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

namespace doris {

class OLAPTable;
class RowCursor;
class RowBlock;
class CollectIterator;
class RuntimeState;

// Params for Reader,
// mainly include tablet, data version and fetch range.
struct ReaderParams {
    OLAPTablePtr olap_table;
    ReaderType reader_type;
    bool aggregation;
    Version version;
    std::string range;
    std::string end_range;
    std::vector<OlapTuple> start_key;
    std::vector<OlapTuple> end_key;
    std::vector<TCondition> conditions;
    // The ColumnData will be set when using Merger, eg Cumulative, BE.
    std::vector<ColumnData*> olap_data_arr;
    std::vector<uint32_t> return_columns;
    RuntimeProfile* profile;
    RuntimeState* runtime_state;

    ReaderParams() :
            reader_type(READER_QUERY),
            aggregation(true),
            profile(NULL),
            runtime_state(NULL) {
        start_key.clear();
        end_key.clear();
        conditions.clear();
        olap_data_arr.clear();
    }

    std::string to_string() {
        std::stringstream ss;

        ss << "table=" << olap_table->full_name()
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

    uint64_t filted_rows() const {
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

    OLAPStatus _acquire_data_sources(const ReaderParams& read_params);

    OLAPStatus _init_keys_param(const ReaderParams& read_params);

    OLAPStatus _init_conditions_param(const ReaderParams& read_params);

    ColumnPredicate* _new_eq_pred(FieldInfo& type, int index, const std::string& cond);
    ColumnPredicate* _new_ne_pred(FieldInfo& type, int index, const std::string& cond);
    ColumnPredicate* _new_lt_pred(FieldInfo& type, int index, const std::string& cond);
    ColumnPredicate* _new_le_pred(FieldInfo& type, int index, const std::string& cond);
    ColumnPredicate* _new_gt_pred(FieldInfo& type, int index, const std::string& cond);
    ColumnPredicate* _new_ge_pred(FieldInfo& type, int index, const std::string& cond);

    ColumnPredicate* _parse_to_predicate(const TCondition& condition);

    OLAPStatus _init_delete_condition(const ReaderParams& read_params);

    OLAPStatus _init_return_columns(const ReaderParams& read_params);

    OLAPStatus _init_load_bf_columns(const ReaderParams& read_params);

    OLAPStatus _attach_data_to_merge_set(bool first, bool *eof);
    
    OLAPStatus _dup_key_next_row(RowCursor* row_cursor, bool* eof);
    OLAPStatus _agg_key_next_row(RowCursor* row_cursor, bool* eof);
    OLAPStatus _unique_key_next_row(RowCursor* row_cursor, bool* eof);

private:
    std::unique_ptr<MemTracker> _tracker;
    std::unique_ptr<MemPool> _predicate_mem_pool;
    std::set<uint32_t> _load_bf_columns;
    std::vector<uint32_t> _return_columns;

    Version _version;

    OLAPTablePtr _olap_table;

    // _own_data_sources is data source that reader aquire from olap_table, so we need to
    // release these when reader closing
    std::vector<ColumnData*> _own_data_sources;
    std::vector<ColumnData*> _data_sources;

    KeysParam _keys_param;
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
