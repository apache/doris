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

#include <string>

#include "rapidjson/document.h"
#include "runtime/descriptors.h"
#include "runtime/tuple.h"

namespace doris {

class Status;

class ScrollParser {
public:
    enum EsAggregationOp {
        COUNT,
        MIN,
        MAX,
        SUM,
        AVG,
        COUNT_ONE_OR_STAR,
        OTHER,
    };

    ScrollParser(bool doc_value_mode);
    ~ScrollParser();

    Status parse(const std::string& scroll_result, bool exactly_once = false);
    Status fill_tuple(const TupleDescriptor* _tuple_desc, Tuple* tuple, MemPool* mem_pool,
                      bool* line_eof, const std::map<std::string, std::string>& docvalue_context);
    Status fill_agg_tuple(const TupleDescriptor* intermediate_tuple_desc, Tuple* tuple, MemPool* mem_pool,
                          bool* line_eof, const std::map<std::string, std::string>& docvalue_context,
                          const int group_by_size, const std::vector<EsAggregationOp>& aggregate_functions);
    Status set_any_val_from_json(PrimitiveType type, MemPool* tuple_pool, const rapidjson::Value& val, void* slot, bool pure_doc_value);
    const std::string& get_scroll_id();
    const std::string& get_after_key();
    int get_size();

private:
    // helper method for processing date/datetime cols with rapidjson::Value
    // type is used for distinguish date and datetime
    // fill date slot with string format date
    Status fill_date_slot_with_strval(void* slot, const rapidjson::Value& col, PrimitiveType type);
    // fill date slot with timestamp
    Status fill_date_slot_with_timestamp(void* slot, const rapidjson::Value& col,
                                         PrimitiveType type);

private:
    std::string _scroll_id;
    int _size;
    // _size is used to check read is completed.
    // when sql is "select count(1)", aggregation dsl is empty, _size will be 0,
    // but we still need to construct the tuple from <_count_with_null> (hits.total.value).
    // _first_read_tuple is used to make <es_scroll_parser> construct the
    // first tuple in any case and the check only happen when no group by.
    int _first_read_tuple = true;
    rapidjson::SizeType _line_index;

    rapidjson::Document _document_node;
    // we do es aggregation query via composite query, it will return _after_key
    // we can use _after_key to read the next page, just add parameter 'after':'after_key'
    std::string _after_key;
    rapidjson::Value _inner_hits_node;
    rapidjson::Value _inner_agg_values;
    // hits.total.value. the length of the table (include NULL value).
    rapidjson::Value _count_with_null;

    struct AvgState {
        double sum = 0;
        int64_t count = 0;
    };

    // todo(milimin): ScrollParser should be divided into two classes: SourceParser and DocValueParser,
    // including remove some variables in the current implementation, e.g. pure_doc_value.
    // All above will be done in the DOE refactoring projects.
    // Current bug fixes minimize the scope of changes to avoid introducing other new bugs.
    bool _doc_value_mode;
};
} // namespace doris
