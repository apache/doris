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

#include "vec/runtime/vpartition_info.h"

namespace doris::vectorized {
Status VPartitionInfo::from_thrift(ObjectPool* pool, const TRangePartition& t_partition,
                                   VPartitionInfo* partition) {
    partition->_id = t_partition.partition_id;
    RETURN_IF_ERROR(PartRange::from_thrift(pool, t_partition.range, &partition->_range));
    if (t_partition.__isset.distributed_exprs) {
        partition->_distributed_bucket = t_partition.distribute_bucket;
        if (partition->_distributed_bucket == 0) {
            return Status::InternalError("Distributed bucket is 0.");
        }
        RETURN_IF_ERROR(VExpr::create_expr_trees(pool, t_partition.distributed_exprs,
                                                 &partition->_distributed_expr_ctxs));
    }
    return Status::OK();
}

Status VPartitionInfo::prepare(RuntimeState* state, const RowDescriptor& row_desc,
                               const std::shared_ptr<MemTracker>& mem_tracker) {
    if (_distributed_expr_ctxs.size() > 0) {
        RETURN_IF_ERROR(VExpr::prepare(_distributed_expr_ctxs, state, row_desc, mem_tracker));
    }
    return Status::OK();
}

Status VPartitionInfo::open(RuntimeState* state) {
    if (_distributed_expr_ctxs.size() > 0) {
        return VExpr::open(_distributed_expr_ctxs, state);
    }
    return Status::OK();
}

void VPartitionInfo::close(RuntimeState* state) {
    if (_distributed_expr_ctxs.size() > 0) {
        VExpr::close(_distributed_expr_ctxs, state);
    }
}
} // namespace doris::vectorized