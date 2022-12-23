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
#include <exec/arrow/arrow_reader.h>
#include <exec/arrow/orc_reader.h>

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "exec/arrow/parquet_reader.h"
#include "exec/base_scanner.h"
#include "gen_cpp/Types_types.h"
#include "runtime/mem_pool.h"
#include "util/runtime_profile.h"

namespace doris::vectorized {

// VArrow scanner convert the data read from orc|parquet to doris's columns.
class VArrowScanner : public BaseScanner {
public:
    VArrowScanner(RuntimeState* state, RuntimeProfile* profile,
                  const TBrokerScanRangeParams& params, const std::vector<TBrokerRangeDesc>& ranges,
                  const std::vector<TNetworkAddress>& broker_addresses,
                  const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);

    virtual ~VArrowScanner();

    // Open this scanner, will initialize information need to
    virtual Status open() override;

    virtual Status get_next(Block* block, bool* eof) override;

    // Update file predicate filter profile
    void update_profile(std::shared_ptr<Statistics>& statistics);

    virtual void close() override;

protected:
    virtual ArrowReaderWrap* _new_arrow_reader(const std::vector<SlotDescriptor*>& file_slot_descs,
                                               FileReader* file_reader,
                                               int32_t num_of_columns_from_file,
                                               int64_t range_start_offset, int64_t range_size) = 0;

private:
    // Read next buffer from reader
    Status _open_next_reader();
    Status _next_arrow_batch();
    Status _init_arrow_batch_if_necessary();
    Status _init_src_block() override;
    Status _append_batch_to_src_block(Block* block);
    Status _cast_src_block(Block* block);

private:
    // Reader
    ArrowReaderWrap* _cur_file_reader;
    bool _cur_file_eof; // is read over?
    std::shared_ptr<arrow::RecordBatch> _batch;
    size_t _arrow_batch_cur_idx;

    RuntimeProfile::Counter* _filtered_row_groups_counter;
    RuntimeProfile::Counter* _filtered_rows_counter;
    RuntimeProfile::Counter* _filtered_bytes_counter;
    RuntimeProfile::Counter* _total_rows_counter;
    RuntimeProfile::Counter* _total_groups_counter;
};

} // namespace doris::vectorized
