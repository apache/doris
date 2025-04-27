#pragma once

#include <map>
#include <string>

namespace doris {
namespace segment_v2 {

struct VariantStatistics {
    // If reached the size of this, we should stop writing statistics for sparse data
    std::map<std::string, int64_t> subcolumns_non_null_size;
    std::map<std::string, int64_t> sparse_column_non_null_size;

    void to_pb(VariantStatisticsPB* stats) const {
        for (const auto& [path, value] : sparse_column_non_null_size) {
            stats->mutable_sparse_column_non_null_size()->emplace(path, value);
        }
        LOG(INFO) << "num subcolumns " << subcolumns_non_null_size.size() << ", num sparse columns "
                  << sparse_column_non_null_size.size();
    }

    void from_pb(const VariantStatisticsPB& stats) {
        for (const auto& [path, value] : stats.sparse_column_non_null_size()) {
            sparse_column_non_null_size[path] = value;
        }
    }
};
} // namespace segment_v2
} // namespace doris