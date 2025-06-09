#include "olap/rowset/segment_v2/ann_index/range_search_runtime_info.h"

#include <fmt/format.h>

#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"

namespace doris::vectorized {
RangeSearchParams RangeSearchRuntimeInfo::to_range_search_params() const {
    RangeSearchParams params;
    params.query_value = query_value.get();
    params.radius = static_cast<float>(radius);
    params.roaring = nullptr;
    params.is_le_or_lt = is_le_or_lt;
    return params;
}

std::string RangeSearchRuntimeInfo::to_string() const {
    return fmt::format(
            "is_ann_range_search: {}, is_le_or_lt: {}, src_col_idx: {}, "
            "dst_col_idx: {}, metric_type {}, radius: {}, user params: {}, query_vector is null: "
            "{}",
            is_ann_range_search, is_le_or_lt, src_col_idx, dst_col_idx,
            segment_v2::metric_to_string(metric_type), radius, user_params.to_string(),
            query_value == nullptr);
}
} // namespace doris::vectorized