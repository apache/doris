#pragma once

#include <cstdint>

namespace snii::io {

// Object-storage access metrics collected at FileReader boundaries.
struct IoMetrics {
    uint64_t read_at_calls = 0;       // BE-internal logical read requests issued
    uint64_t serial_rounds = 0;       // dependent serial I/O rounds
    uint64_t range_gets = 0;          // remote range GETs after cache coalescing
    uint64_t remote_bytes = 0;        // bytes fetched from remote
    uint64_t total_request_bytes = 0; // sum of requested lengths before cache
};

inline IoMetrics delta(const IoMetrics& after, const IoMetrics& before) {
    IoMetrics out;
    out.read_at_calls = after.read_at_calls - before.read_at_calls;
    out.serial_rounds = after.serial_rounds - before.serial_rounds;
    out.range_gets = after.range_gets - before.range_gets;
    out.remote_bytes = after.remote_bytes - before.remote_bytes;
    out.total_request_bytes = after.total_request_bytes - before.total_request_bytes;
    return out;
}

} // namespace snii::io
