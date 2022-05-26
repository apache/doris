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

#include "exec/olap_scanner.h"

#include "vec/olap/block_reader.h"

namespace doris {
class OlapScanNode;
class RuntimeProfile;
class Field;
class RowBatch;

namespace vectorized {
class VOlapScanNode;

class VOlapScanner : public OlapScanner {
public:
    VOlapScanner(RuntimeState* runtime_state, VOlapScanNode* parent, bool aggregation,
                 bool need_agg_finalize, const TPaloScanRange& scan_range);

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eof);

    Status close(RuntimeState* state) override;

    Status get_batch(RuntimeState* state, RowBatch* row_batch, bool* eos) override {
        return Status::NotSupported("Not Implemented VOlapScanNode Node::get_next scalar");
    }

    VExprContext** vconjunct_ctx_ptr() { return &_vconjunct_ctx; }

    void mark_to_need_to_close() { _need_to_close = true; }

    bool need_to_close() { return _need_to_close; }

protected:
    virtual void set_tablet_reader() override;

private:
    VExprContext* _vconjunct_ctx = nullptr;
    bool _need_to_close = false;
};

} // namespace vectorized
} // namespace doris
