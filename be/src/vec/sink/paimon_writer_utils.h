#pragma once

#include <algorithm>
#include <cstdint>

namespace doris::vectorized {

inline int64_t get_paimon_write_buffer_size(int64_t configured_buffer_size, bool enable_adaptive,
                                            int32_t bucket_num) {
    if (!enable_adaptive || bucket_num <= 0) {
        return configured_buffer_size;
    }
    if (bucket_num >= 500) {
        return std::min(configured_buffer_size, 32L * 1024L * 1024L);
    }
    if (bucket_num >= 200) {
        return std::min(configured_buffer_size, 64L * 1024L * 1024L);
    }
    if (bucket_num >= 50) {
        return std::min(configured_buffer_size, 128L * 1024L * 1024L);
    }
    return configured_buffer_size;
}

} // namespace doris::vectorized
