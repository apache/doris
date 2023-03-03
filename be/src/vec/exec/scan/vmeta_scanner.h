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

#include "runtime/runtime_state.h"
#include "vec/exec/scan/vscanner.h"
#include "vmeta_scan_node.h"

namespace doris::vectorized {

class VMetaScanner : public VScanner {
public:
    VMetaScanner(RuntimeState* state, VMetaScanNode* parent, int64_t tuple_id,
                 const TScanRangeParams& scan_range, int64_t limit, RuntimeProfile* profile);

    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    Status prepare(RuntimeState* state, VExprContext** vconjunct_ctx_ptr);

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eos) override;
    Status _fill_block_with_remote_data(const std::vector<MutableColumnPtr>& columns);
    Status _fetch_iceberg_metadata_batch();

private:
    VMetaScanNode* _parent;
    bool _meta_eos;
    TupleId _tuple_id;
    const TupleDescriptor* _tuple_desc;
    std::vector<TRow> _batch_data;
    const TScanRange& _scan_range;
};
} // namespace doris::vectorized
