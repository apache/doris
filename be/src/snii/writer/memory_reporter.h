#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <utility>

namespace snii::writer {

// Per-WRITER accurate byte counter for build-time RAM (one per SniiCompoundWriter =
// one per segment's inverted index). Modules report their own resident-byte deltas;
// current_bytes() is that writer's accurate live usage. OBSERVE-ONLY -- SNII never
// makes a flush decision from it (gate 1 belongs to Doris; gate 2 is the internal
// threshold). consume_release mirrors the delta into Doris's LOAD MemTracker so the
// inverted-index RAM is counted by MemTableMemoryLimiter's pressure decision; it is
// null off-Doris (bench / unit tests), where only the local atomic is updated.
class MemoryReporter {
public:
    using ConsumeReleaseFn = std::function<void(int64_t delta)>; // null off-Doris
    // cap_bytes is the UNIFIED gate-2 buffer cap for the WHOLE writer (e.g. Doris's
    // 512 MiB inverted-index buffer config); 0 = unlimited. Every build buffer of this
    // writer (SPIMI arena + dict) self-spills when over_cap() is true -- one threshold on
    // the unified total, not a separate per-buffer threshold.
    explicit MemoryReporter(ConsumeReleaseFn consume_release = nullptr, uint64_t cap_bytes = 0)
            : consume_release_(std::move(consume_release)), cap_bytes_(cap_bytes) {}

    MemoryReporter(const MemoryReporter&) = delete;
    MemoryReporter& operator=(const MemoryReporter&) = delete;

    // delta > 0 grows, delta < 0 shrinks/frees. Exactly one report per change site.
    void report(int64_t delta) {
        current_.fetch_add(delta, std::memory_order_relaxed);
        if (consume_release_) consume_release_(delta); // mirror into Doris load tracker
    }

    int64_t current_bytes() const { return current_.load(std::memory_order_relaxed); }

    // True once the writer's UNIFIED total build RAM (arena + slot index + dict + ...)
    // reaches the cap. The single gate-2 trigger shared by every buffer of the writer.
    bool over_cap() const {
        return cap_bytes_ != 0 && current_bytes() >= static_cast<int64_t>(cap_bytes_);
    }
    uint64_t cap_bytes() const { return cap_bytes_; }

private:
    std::atomic<int64_t> current_ {0};
    ConsumeReleaseFn consume_release_;
    uint64_t cap_bytes_ = 0;
};

} // namespace snii::writer
