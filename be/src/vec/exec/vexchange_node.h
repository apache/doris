#pragma once

#include <memory>

#include "exec/exec_node.h"

#include "vec/exec/vsort_exec_exprs.h"

namespace doris {
namespace vectorized {
class VDataStreamRecvr;

class VExchangeNode : public ExecNode {
public:
    VExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    virtual ~VExchangeNode() {}

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    virtual Status prepare(RuntimeState* state) override;
    virtual Status open(RuntimeState* state) override;
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    virtual Status get_next(RuntimeState* state, Block* row_batch, bool* eos) override;
    virtual Status close(RuntimeState* state) override;

    // Status collect_query_statistics(QueryStatistics* statistics) override;
    void set_num_senders(int num_senders) { _num_senders = num_senders; }

private:
    int _num_senders;
    bool _is_merging;
    std::shared_ptr<VDataStreamRecvr> _stream_recvr;
    RowDescriptor _input_row_desc;
    std::shared_ptr<QueryStatisticsRecvr> _sub_plan_query_statistics_recvr;

    // use in merge sort
    size_t _offset;
    VSortExecExprs _vsort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
};
} // namespace vectorized
} // namespace doris
