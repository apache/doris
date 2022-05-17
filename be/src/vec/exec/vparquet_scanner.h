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

#include <arrow/array.h>
#include <exec/parquet_scanner.h>

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

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
                    const TExpr& vpre_filter_texpr, ScannerCounter* counter);

    ~VParquetScanner() override;

    // Open this scanner, will initialize information need to
    Status open() override;

    Status get_next(Block* block, bool* eof) override;

private:
    Status _next_arrow_batch();
    Status _init_arrow_batch_if_necessary();
    Status _init_src_block() override;
    Status _append_batch_to_src_block(Block* block);
    Status _cast_src_block(Block* block);

private:
    std::shared_ptr<arrow::RecordBatch> _batch;
    size_t _arrow_batch_cur_idx;
};

} // namespace doris::vectorized
