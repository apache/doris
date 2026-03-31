#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "vec/runtime/partitioner.h"

#ifdef WITH_PAIMON_CPP
namespace paimon {
class BucketIdCalculator;
class MemoryPool;
} // namespace paimon
#endif

namespace doris::vectorized {

struct PaimonBucketShuffleParams {
    int32_t bucket_num = 0;
    std::vector<std::string> bucket_keys;
    std::vector<std::string> column_names;
};

class PaimonBucketPartitioner final : public PartitionerBase {
public:
    explicit PaimonBucketPartitioner(size_t partition_count, PaimonBucketShuffleParams params);
    ~PaimonBucketPartitioner() override = default;

    Status init(const std::vector<TExpr>& texprs) override;
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override;
    Status open(RuntimeState* state) override;
    Status do_partitioning(RuntimeState* state, Block* block, MemTracker* mem_tracker) const override;

    ChannelField get_channel_ids() const override { return {_channel_ids.data(), sizeof(uint32_t)}; }

    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override;

private:
    Status _compute_bucket_ids(RuntimeState* state, const Block& block, int32_t* bucket_ids,
                              MemTracker* mem_tracker) const;

    PaimonBucketShuffleParams _params;
    mutable std::vector<uint32_t> _channel_ids;

#ifdef WITH_PAIMON_CPP
    std::shared_ptr<::paimon::MemoryPool> _pool;
    std::unique_ptr<::paimon::BucketIdCalculator> _calculator;
#endif
};

} // namespace doris::vectorized
