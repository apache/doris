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