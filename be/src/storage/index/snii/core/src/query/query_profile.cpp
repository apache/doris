#include "snii/query/query_profile.h"

#include <algorithm>
#include <chrono>

#include "snii/io/file_reader.h"

namespace snii::query {

QueryProfileScope::QueryProfileScope(snii::io::FileReader* reader, QueryProfile* profile)
        : reader_(reader), profile_(profile), start_(std::chrono::steady_clock::now()) {
    if (profile_ == nullptr) return;

    *profile_ = QueryProfile {};
    if (reader_ == nullptr) return;

    const snii::io::IoMetrics* metrics = reader_->io_metrics();
    if (metrics == nullptr) return;

    profile_->has_io_metrics = true;
    profile_->io_before = *metrics;
}

QueryProfileScope::~QueryProfileScope() {
    finish();
}

void QueryProfileScope::finish() {
    if (profile_ == nullptr || finished_) return;
    finished_ = true;

    const auto end = std::chrono::steady_clock::now();
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_).count();
    profile_->elapsed_ns = std::max<uint64_t>(1, static_cast<uint64_t>(elapsed));

    if (!profile_->has_io_metrics || reader_ == nullptr) return;
    const snii::io::IoMetrics* metrics = reader_->io_metrics();
    if (metrics == nullptr) {
        profile_->has_io_metrics = false;
        return;
    }
    profile_->io_after = *metrics;
    profile_->io_delta = snii::io::delta(profile_->io_after, profile_->io_before);
}

} // namespace snii::query
