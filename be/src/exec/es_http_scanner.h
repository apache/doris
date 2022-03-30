
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

#ifndef BE_EXEC_ES_HTTP_SCANNER_H
#define BE_EXEC_ES_HTTP_SCANNER_H

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"
#include "exec/es/es_scan_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/mem_pool.h"
#include "util/runtime_profile.h"

namespace doris {

class Tuple;
class SlotDescriptor;
class RuntimeState;
class ExprContext;
class TextConverter;
class TupleDescriptor;
class TupleRow;
class RowDescriptor;
class RuntimeProfile;

struct EsScanCounter {
    EsScanCounter() : num_rows_returned(0), num_rows_filtered(0) {}

    int64_t num_rows_returned;
    int64_t num_rows_filtered;
};

class EsHttpScanner {
public:
    EsHttpScanner(RuntimeState* state, RuntimeProfile* profile, TupleId tuple_id,
                  const std::map<std::string, std::string>& properties,
                  const std::vector<ExprContext*>& conjunct_ctxs, EsScanCounter* counter,
                  bool doc_value_mode);
    ~EsHttpScanner();

    Status open();

    Status get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof,
                    const std::map<std::string, std::string>& docvalue_context);

    void close();

protected:
    RuntimeState* _state;
    RuntimeProfile* _profile;
    TupleId _tuple_id;
    const std::map<std::string, std::string>& _properties;
    const std::vector<ExprContext*>& _conjunct_ctxs;

    int _next_range;
    bool _line_eof;
    bool _batch_eof;

    std::vector<SlotDescriptor*> _slot_descs;
    std::unique_ptr<RowDescriptor> _row_desc;

    std::unique_ptr<MemPool> _mem_pool;

    const TupleDescriptor* _tuple_desc;
    EsScanCounter* _counter;
    std::unique_ptr<ESScanReader> _es_reader;
    std::unique_ptr<ScrollParser> _es_scroll_parser;

    bool _doc_value_mode;

    // Profile
    RuntimeProfile::Counter* _rows_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _materialize_timer;
};

} // namespace doris

#endif
