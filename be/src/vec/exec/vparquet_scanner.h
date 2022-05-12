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
#include <unordered_map>
#include <vector>

#include <arrow/array.h>
#include <exec/parquet_scanner.h>
#include "common/status.h"
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

    Status get_next(Block* block, bool* eof);

private:
    Status _next_arrow_batch();
    Status _init_arrow_batch_if_necessary();
    Status _init_src_block(Block* block);
    Status _append_batch_to_src_block(Block* block);
    Status _cast_src_block(Block* block);
    Status _eval_conjunts(Block* block);
    Status _materialize_block(Block* block, Block* dest_block);
    void _fill_columns_from_path(Block* block);

private:
    std::shared_ptr<arrow::RecordBatch> _batch;
    size_t _arrow_batch_cur_idx;
    int _num_of_columns_from_file;
};

} // namespace doris::vectorized
