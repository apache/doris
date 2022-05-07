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
#include <unordered_map>

#include <arrow/array.h>
#include "common/status.h"
#include <exec/parquet_scanner.h>
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/mem_pool.h"
#include "util/runtime_profile.h"

namespace doris::vectorized {

// VParquet scanner convert the data read from Parquet to doris's columns.
class VParquetScanner : public ParquetScanner {
public:
    VParquetScanner(RuntimeState* state, RuntimeProfile* profile,
                   const TBrokerScanRangeParams& params,
                   const std::vector<TBrokerRangeDesc>& ranges,
                   const std::vector<TNetworkAddress>& broker_addresses,
                   const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);

    virtual ~VParquetScanner();

    // Open this scanner, will initialize information need to
    Status open();

    Status get_next(std::vector<MutableColumnPtr>& columns, bool* eof);

private:
    Status next_arrow_batch();
    Status init_arrow_batch_if_necessary();
    Status init_src_block(Block* block);
    Status append_batch_to_src_block(Block* block);
    Status cast_src_block(Block* block);
    Status eval_conjunts(Block* block);
    Status materialize_block(Block* block, std::vector<MutableColumnPtr>& columns);
    void fill_columns_from_path(Block* block);

private:
    std::shared_ptr<arrow::RecordBatch> _batch;
    size_t _arrow_batch_cur_idx;
    int _num_of_columns_from_file;
};

} // namespace doris
