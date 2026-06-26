#pragma once

#include <chrono>
#include <cstdint>

#include "snii/io/io_metrics.h"

namespace snii::io {
class FileReader;
}

namespace snii::query {

struct QueryProfile {
    uint64_t elapsed_ns = 0;
    bool has_io_metrics = false;
    snii::io::IoMetrics io_before;
    snii::io::IoMetrics io_after;
    snii::io::IoMetrics io_delta;
};

class QueryProfileScope {
public:
    QueryProfileScope(snii::io::FileReader* reader, QueryProfile* profile);
    ~QueryProfileScope();
    QueryProfileScope(const QueryProfileScope&) = delete;
    QueryProfileScope& operator=(const QueryProfileScope&) = delete;

    void finish();

private:
    snii::io::FileReader* reader_ = nullptr;
    QueryProfile* profile_ = nullptr;
    std::chrono::steady_clock::time_point start_;
    bool finished_ = false;
};

} // namespace snii::query
