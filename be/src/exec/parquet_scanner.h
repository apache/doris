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

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/base_scanner.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/mem_pool.h"
#include "util/runtime_profile.h"
#include "util/slice.h"

namespace doris {

class Tuple;
class SlotDescriptor;
struct Slice;
class ParquetReaderWrap;
class RuntimeState;
class ExprContext;
class TupleDescriptor;
class TupleRow;
class RowDescriptor;
class RuntimeProfile;
class StreamLoadPipe;

// Broker scanner convert the data read from broker to doris's tuple.
class ParquetScanner : public BaseScanner {
public:
    ParquetScanner(RuntimeState* state, RuntimeProfile* profile,
                   const TBrokerScanRangeParams& params,
                   const std::vector<TBrokerRangeDesc>& ranges,
                   const std::vector<TNetworkAddress>& broker_addresses,
                   const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);

    ~ParquetScanner();

    // Open this scanner, will initialize information need to
    virtual Status open();

    // Get next tuple
    virtual Status get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof, bool* fill_tuple);

    // Close this scanner
    virtual void close();

private:
    // Read next buffer from reader
    Status open_next_reader();

private:
    //const TBrokerScanRangeParams& _params;
    const std::vector<TBrokerRangeDesc>& _ranges;
    const std::vector<TNetworkAddress>& _broker_addresses;

    // Reader
    ParquetReaderWrap* _cur_file_reader;
    int _next_range;
    bool _cur_file_eof; // is read over?

    // used to hold current StreamLoadPipe
    std::shared_ptr<StreamLoadPipe> _stream_load_pipe;
};

} // namespace doris
