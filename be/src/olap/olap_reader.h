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

#ifndef BDG_PALO_BE_SRC_OLAP_OLAP_READER_H
#define BDG_PALO_BE_SRC_OLAP_OLAP_READER_H

#include <gen_cpp/PaloInternalService_types.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/object_pool.h"
#include "olap/delete_handler.h"
#include "olap/i_data.h"
#include "olap/olap_cond.h"
#include "olap/olap_engine.h"
#include "util/palo_metrics.h"
#include "olap/reader.h"

namespace palo {

class OLAPShowHints {
public:
    static Status show_hints(
            TShowHintsRequest& fetch_request,
            std::vector<std::vector<std::vector<std::string>>>* ranges,
            RuntimeProfile* profile);
};

class OLAPReader {
public:
    static void init_profile(RuntimeProfile* profile);
    static OLAPReader* create(const TupleDescriptor &tuple_desc, RuntimeState* runtime_state);

    explicit OLAPReader(const TupleDescriptor &tuple_desc) :
            _tuple_desc(tuple_desc),
            _conjunct_ctxs(nullptr),
            _is_inited(false),
            _request_version(-1),
            _aggregation(false),
            _get_tablet_timer(nullptr),
            _init_reader_timer(nullptr),
            _read_data_timer(nullptr),
            _runtime_state(nullptr) {}

    OLAPReader(const TupleDescriptor &tuple_desc, RuntimeState* runtime_state) :
            _tuple_desc(tuple_desc),
            _conjunct_ctxs(nullptr),
            _is_inited(false),
            _request_version(-1),
            _aggregation(false),
            _get_tablet_timer(nullptr),
            _init_reader_timer(nullptr),
            _read_data_timer(nullptr),
            _runtime_state(runtime_state) {}

    ~OLAPReader() {
        close();
    }

    Status init(TFetchRequest& fetch_request,
                std::vector<ExprContext*> *conjunct_ctxs,
                RuntimeProfile* profile);

    Status close();

    Status next_tuple(Tuple *tuple, int64_t* raw_rows_read, bool* eof);
    
private: 
    OLAPStatus _init_params(TFetchRequest& fetch_request, RuntimeProfile* profile);

    OLAPStatus _init_return_columns(TFetchRequest& fetch_request);

    OLAPStatus _convert_row_to_tuple(Tuple* tuple);

    Reader _reader;

    const TupleDescriptor &_tuple_desc;

    std::vector<ExprContext*> *_conjunct_ctxs;

    bool _is_inited;

    int32_t _request_version;

    bool _aggregation;

    SmartOLAPTable _olap_table;

    std::vector<uint32_t> _return_columns;

    RowCursor _read_row_cursor;

    RowCursor _return_row_cursor;

    std::vector<uint32_t> _request_columns_size;

    std::vector<SlotDescriptor*> _query_slots;

    // time costed and row returned statistics
    RuntimeProfile::Counter* _get_tablet_timer;
    RuntimeProfile::Counter* _init_reader_timer;
    OlapStopWatch _read_data_watch;
    RuntimeProfile::Counter* _read_data_timer;
    RuntimeState* _runtime_state;
};

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_OLAP_READER_H
