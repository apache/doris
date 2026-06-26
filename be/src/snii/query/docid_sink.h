#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include "snii/common/status.h"

namespace snii::query {

// Bulk docid handoff for query operators. Each span is sorted ascending; callers
// that need a single vector can use VectorDocIdSink.
class DocIdSink {
public:
    virtual ~DocIdSink() = default;
    virtual Status append_sorted(std::span<const uint32_t> docids) = 0;
};

class VectorDocIdSink final : public DocIdSink {
public:
    explicit VectorDocIdSink(std::vector<uint32_t>& docids) : docids_(docids) {}

    Status append_sorted(std::span<const uint32_t> docids) override {
        docids_.insert(docids_.end(), docids.begin(), docids.end());
        return Status::OK();
    }

private:
    std::vector<uint32_t>& docids_;
};

} // namespace snii::query
