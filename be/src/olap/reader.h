// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_OLAP_READER_H
#define BDG_PALO_BE_SRC_OLAP_READER_H

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

#include "exprs/expr.h"
#include "olap/delete_handler.h"
#include "olap/olap_cond.h"
#include "olap/olap_define.h"
#include "olap/row_cursor.h"
#include "util/runtime_profile.h"

namespace palo {

class OLAPTable;
class RowCursor;
class RowBlock;

// Params for Reader,
// mainly include tablet, data version and fetch range.
struct ReaderParams {
    SmartOLAPTable olap_table;
    ReaderType reader_type;
    bool aggregation;
    Version version;
    std::string range;
    std::string end_range;
    std::vector<TFetchStartKey> start_key;
    std::vector<TFetchEndKey> end_key;
    std::vector<TCondition> conditions;
    std::vector<ExprContext*>* conjunct_ctxs;
    // The IData will be set when using Merger, eg Cumulative, BE.
    std::vector<IData*> olap_data_arr;
    std::vector<uint32_t> return_columns;
    RuntimeProfile* profile;
    RuntimeState* runtime_state;

    ReaderParams() :
            reader_type(READER_FETCH),
            aggregation(true),
            conjunct_ctxs(NULL),
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

        for (int i = 0, size = start_key.size(); i < size; ++i) {
            ss << " keys=" << apache::thrift::ThriftDebugString(start_key[i]);
        }

        for (int i = 0, size = end_key.size(); i < size; ++i) {
            ss << " end_keys=" << apache::thrift::ThriftDebugString(end_key[i]);
        }

        for (int i = 0, size = conditions.size(); i < size; ++i) {
            ss << " conditions=" << apache::thrift::ThriftDebugString(conditions[i]);
        }
        
        return ss.str();
    }
};

class Reader {
public:
    Reader() :
            _is_inited(false),
            _aggregation(false),
            _version_locked(false),
            _reader_type(READER_FETCH),
            _is_set_data_sources(false),
            _current_key_index(0),
            _next_key(NULL),
            _next_delete_flag(false),
            _scan_rows(0),
            _filted_rows(0),
            _merged_rows(0) {}

    ~Reader() {
        close();
    }

    // Initialize Reader with tablet, data version and fetch range.
    OLAPStatus init(const ReaderParams& read_params);

    void close();

    // Reader next row with aggregation.
    OLAPStatus next_row_with_aggregation(RowCursor *row_cursor, int64_t* raw_rows_read, bool *eof);

    uint64_t merged_rows() const {
        return _merged_rows;
    }

    uint64_t filted_rows() const {
        return _filted_rows;
    }

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

    typedef IData* MergeElement;

    // Use priority_queue as heap to merge multiple data versions.
    class MergeSet {
    public:
        MergeSet() : _heap(NULL) {}
        ~MergeSet();

        // Hold reader point to get reader params, 
        // set reverse to true if need read in reverse order.
        OLAPStatus init(Reader* reader, bool reverse);

        // Add merge element into heap.
        bool attach(const MergeElement& merge_element, const RowCursor* row);

        // Get top row of the heap, NULL if reach end.
        const RowCursor* curr(bool* delete_flag);

        // Pop the top element and rebuild the heap to 
        // get the next row cursor.
        bool next(const RowCursor** element, bool* delete_flag);

        // Clear the MergeSet element and reset state.
        bool clear();

    private:
        // Compare row cursors between multiple merge elements,
        // if row cursors equal, compare data version.
        class RowCursorComparator {
        public:
            explicit RowCursorComparator(bool reverse) : _reverse(reverse) {}
            bool operator()(const MergeElement& a, const MergeElement& b);

        private:
            bool _reverse;
        };

        typedef std::priority_queue<MergeElement, std::vector<MergeElement>, RowCursorComparator>
            heap_t;
        
        bool _pop_from_heap();

        heap_t* _heap;

        uint64_t _merge_count;

        // Hold reader point to access read params, such as fetch conditions.
        Reader* _reader;
    }; 

    friend class MergeSet;

    OLAPStatus _init_params(const ReaderParams& read_params);

    OLAPStatus _acquire_data_sources(const ReaderParams& read_params);

    OLAPStatus _init_keys_param(const ReaderParams& read_params);

    OLAPStatus _init_conditions_param(const ReaderParams& read_params);

    OLAPStatus _init_delete_condition(const ReaderParams& read_params);

    OLAPStatus _init_return_columns(const ReaderParams& read_params);

    OLAPStatus _init_load_bf_columns(const ReaderParams& read_params);

    OLAPStatus _attach_data_to_merge_set(bool first, bool *eof);

    bool _is_inited;
    bool _aggregation;
    bool _version_locked;
    ReaderType _reader_type;

    Version _version;

    SmartOLAPTable _olap_table;

    std::vector<IData*> _data_sources;

    // If ReaderParams.olap_data_arr is set out of reader,
    // will not acquire data sources according to version.
    bool _is_set_data_sources;

    KeysParam _keys_param;

    int32_t _current_key_index;

    Conditions _conditions;

    std::vector<ExprContext*>* _query_conjunct_ctxs;

    DeleteHandler _delete_handler;

    MergeSet _merge_set;

    const RowCursor* _next_key;
    bool _next_delete_flag;

    std::set<uint32_t> _load_bf_columns;
    std::vector<uint32_t> _return_columns;

    uint64_t _scan_rows;

    uint64_t _filted_rows;
    uint64_t _merged_rows;

    DISALLOW_COPY_AND_ASSIGN(Reader);

};

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_READER_H
