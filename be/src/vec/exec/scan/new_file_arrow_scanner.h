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

#include <exec/arrow/arrow_reader.h>

#include "exprs/bloomfilter_predicate.h"
#include "exprs/function_filter.h"
#include "vec/exec/scan/new_file_scanner.h"
#include "vec/exec/scan/vscanner.h"

namespace doris::vectorized {
class NewFileArrowScanner : public NewFileScanner {
public:
    NewFileArrowScanner(RuntimeState* state, NewFileScanNode* parent, int64_t limit,
                        const TFileScanRange& scan_range, MemTracker* tracker,
                        RuntimeProfile* profile, const std::vector<TExpr>& pre_filter_texprs);
    Status open(RuntimeState* state) override;

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eos) override;
    virtual ArrowReaderWrap* _new_arrow_reader(const std::vector<SlotDescriptor*>& file_slot_descs,
                                               FileReader* file_reader,
                                               int32_t num_of_columns_from_file,
                                               int64_t range_start_offset, int64_t range_size) = 0;
    // Convert input block to output block, if needed.
    Status _convert_to_output_block(Block* output_block);

private:
    Status _open_next_reader();
    Status _next_arrow_batch();
    Status _init_arrow_batch_if_necessary();
    Status _append_batch_to_block(Block* block);
    Status _cast_src_block(Block* block);

private:
    // Reader
    ArrowReaderWrap* _cur_file_reader;
    bool _cur_file_eof; // is read over?
    std::shared_ptr<arrow::RecordBatch> _batch;
    size_t _arrow_batch_cur_idx;
};

class NewFileParquetScanner final : public NewFileArrowScanner {
public:
    NewFileParquetScanner(RuntimeState* state, NewFileScanNode* parent, int64_t limit,
                          const TFileScanRange& scan_range, MemTracker* tracker,
                          RuntimeProfile* profile, const std::vector<TExpr>& pre_filter_texprs);

    ~NewFileParquetScanner() override = default;

protected:
    ArrowReaderWrap* _new_arrow_reader(const std::vector<SlotDescriptor*>& file_slot_descs,
                                       FileReader* file_reader, int32_t num_of_columns_from_file,
                                       int64_t range_start_offset, int64_t range_size) override;

    void _init_profiles(RuntimeProfile* profile) override {};
};

class NewFileORCScanner final : public NewFileArrowScanner {
public:
    NewFileORCScanner(RuntimeState* state, NewFileScanNode* parent, int64_t limit,
                      const TFileScanRange& scan_range, MemTracker* tracker,
                      RuntimeProfile* profile, const std::vector<TExpr>& pre_filter_texprs);

    ~NewFileORCScanner() override = default;

protected:
    ArrowReaderWrap* _new_arrow_reader(const std::vector<SlotDescriptor*>& file_slot_descs,
                                       FileReader* file_reader, int32_t num_of_columns_from_file,
                                       int64_t range_start_offset, int64_t range_size) override;
    void _init_profiles(RuntimeProfile* profile) override {};
};
} // namespace doris::vectorized
