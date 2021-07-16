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
    // distribute exprs
    std::vector<VExprContext*> _distributed_expr_ctxs;
    int32_t _distributed_bucket;
};
} // namespace vectorized
} // namespace doris
