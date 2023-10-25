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

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "exec/exec_node.h"
#include "runtime/descriptors.h"
#include "vec/common/sort/vsort_exec_exprs.h"

namespace doris {
class DorisNodesInfo;
class ObjectPool;
class QueryStatistics;
class QueryStatisticsRecvr;
class RuntimeState;
class TPlanNode;

namespace pipeline {
class ExchangeSourceOperator;
}
namespace vectorized {
class VDataStreamRecvr;
class Block;

class VExchangeNode : public ExecNode {
public:
    friend class doris::pipeline::ExchangeSourceOperator;
    VExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VExchangeNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status alloc_resource(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, Block* row_batch, bool* eos) override;
    void release_resource(RuntimeState* state) override;
    Status collect_query_statistics(QueryStatistics* statistics) override;
    Status collect_query_statistics(QueryStatistics* statistics, int sender_id) override;
    Status close(RuntimeState* state) override;

    void set_num_senders(int num_senders) { _num_senders = num_senders; }

private:
    int _num_senders;
    bool _is_merging;
    bool _is_ready;
    std::shared_ptr<VDataStreamRecvr> _stream_recvr;
    RowDescriptor _input_row_desc;
    std::shared_ptr<QueryStatisticsRecvr> _sub_plan_query_statistics_recvr;

    // use in merge sort
    size_t _offset;
    int64_t _num_rows_skipped;
    VSortExecExprs _vsort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
};
} // namespace vectorized
} // namespace doris
