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

#include "exec/text_converter.h"
#include "exprs/bloomfilter_predicate.h"
#include "exprs/function_filter.h"
#include "vec/exec/scan/vscanner.h"

namespace doris::vectorized {

class NewFileScanNode;

class NewFileScanner : public VScanner {
public:
    NewFileScanner(RuntimeState* state, NewFileScanNode* parent, int64_t limit,
                   const TFileScanRange& scan_range, MemTracker* tracker, RuntimeProfile* profile);

    Status open(RuntimeState* state) override;

    Status prepare(VExprContext** vconjunct_ctx_ptr);

protected:
    virtual void _init_profiles(RuntimeProfile* profile) = 0;

    Status _fill_columns_from_path(vectorized::Block* output_block, size_t rows);
    Status init_block(vectorized::Block* block);

    std::unique_ptr<TextConverter> _text_converter;

    const TFileScanRangeParams& _params;

    const std::vector<TFileRangeDesc>& _ranges;
    int _next_range;

    // Used for constructing tuple
    std::vector<SlotDescriptor*> _required_slot_descs;
    // File source slot descriptors
    std::vector<SlotDescriptor*> _file_slot_descs;
    // File slot id to index map.
    std::map<SlotId, int> _file_slot_index_map;
    // Partition source slot descriptors
    std::vector<SlotDescriptor*> _partition_slot_descs;
    // Partition slot id to index map
    std::map<SlotId, int> _partition_slot_index_map;
    std::unique_ptr<RowDescriptor> _row_desc;

    // Profile
    RuntimeProfile* _profile;
    RuntimeProfile::Counter* _rows_read_counter;
    RuntimeProfile::Counter* _read_timer;

    bool _scanner_eof = false;
    int _rows = 0;
    int _num_of_columns_from_file;

private:
    Status _init_expr_ctxes();
};
} // namespace doris::vectorized
