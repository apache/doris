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

#include "common/status.h"
#include "runtime/dpp_sink_internal.h"
#include "vec/exprs/vexpr.h"

namespace doris {
namespace vectorized {

class VPartitionInfo {
public:
    VPartitionInfo() : _id(-1), _distributed_bucket(0) {}

    static Status from_thrift(ObjectPool* pool, const TRangePartition& t_partition,
                              VPartitionInfo* partition);

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                   const std::shared_ptr<MemTracker>& mem_tracker);

    Status open(RuntimeState* state);

    void close(RuntimeState* state);

    int64_t id() const { return _id; }

    const std::vector<VExprContext*>& distributed_expr_ctxs() const {
        return _distributed_expr_ctxs;
    }

    int distributed_bucket() const { return _distributed_bucket; }

    const PartRange& range() const { return _range; }

private:
    int64_t _id;
    PartRange _range;
    // Information used to distribute data
    // distribute express.
    std::vector<VExprContext*> _distributed_expr_ctxs;
    int32_t _distributed_bucket;
};
} // namespace vectorized
} // namespace doris
