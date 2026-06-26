#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "snii/common/slice.h"
#include "snii/common/status.h"
#include "snii/encoding/byte_sink.h"

namespace snii::format {

// norms POD: per logical index / field stores 1-byte encoded doc length per doc,
// used by BM25 length normalization (SniiStatsProvider::encoded_norm) for per-docid lookup.
//
// On-disk layout (the whole section is framed by SectionFramer, which adds a type+len+crc32c envelope):
//   framer payload = [varint64 doc_count][bytes encoded_norm[doc_count]]
//   framer envelope = [u8 type][varint64 payload_len][payload][fixed32 crc32c]
// The encoding of encoded_norm (length -> 1B) is out of scope for this module; here we only handle raw byte storage and retrieval.
class NormsPodWriter {
public:
    // Appends the encoded_norm for the next docid (docid is implicit, assigned in append order starting from 0).
    void add(uint8_t encoded_norm) { norms_.push_back(encoded_norm); }

    // Number of docs accumulated so far (i.e., the next docid to be assigned).
    size_t count() const { return norms_.size(); }

    // Writes [doc_count][bytes] framed by SectionFramer into sink (appends; does not clear sink).
    void finish(ByteSink* sink) const;

private:
    std::vector<uint8_t> norms_;
};

// Read-only view: on open, verifies the framer CRC and checks that doc_count/payload length are consistent,
// afterwards encoded_norm(docid) is O(1) direct indexing (zero-copy, borrows the underlying buffer).
class NormsPodReader {
public:
    NormsPodReader() = default;

    // Parses the entire section (including the framer envelope). Returns Corruption on CRC mismatch, truncation, or length inconsistency.
    // On success, *out borrows the memory pointed to by framer_payload; the caller must ensure its lifetime.
    static Status open(Slice framed, NormsPodReader* out);

    uint32_t doc_count() const { return doc_count_; }

    // Precondition (hard contract): docid < doc_count(). Semantics match std::vector::operator[]:
    // the caller is responsible for guaranteeing this (docid comes from trusted postings decoded internally by SNII). Asserts in debug builds;
    // no check in Release (NDEBUG). Use try_encoded_norm when the docid is untrusted and needs validation.
    uint8_t encoded_norm(uint32_t docid) const {
        assert(docid < doc_count_);
        return norms_[docid];
    }

    // Checked access: returns InvalidArgument if docid is out of range; never reads out-of-range memory.
    Status try_encoded_norm(uint32_t docid, uint8_t* out) const {
        if (docid >= doc_count_) return Status::InvalidArgument("norms: docid out of range");
        *out = norms_[docid];
        return Status::OK();
    }

private:
    const uint8_t* norms_ = nullptr;
    uint32_t doc_count_ = 0;
};

} // namespace snii::format
