#pragma once

#include <cstdint>
#include <vector>

namespace snii::query::internal {

std::vector<uint32_t> intersect_sorted(const std::vector<uint32_t>& a,
                                       const std::vector<uint32_t>& b);

void union_sorted_into(std::vector<uint32_t>* acc, const std::vector<uint32_t>& next);

std::vector<uint32_t> union_sorted_many(const std::vector<std::vector<uint32_t>>& lists);

} // namespace snii::query::internal
