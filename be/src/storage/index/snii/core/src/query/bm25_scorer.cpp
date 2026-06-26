#include "snii/query/bm25_scorer.h"

#include <algorithm>
#include <cmath>

namespace snii::query {

double decode_norm(uint8_t encoded) {
    return encoded == 0 ? 1.0 : static_cast<double>(encoded);
}

uint8_t encode_norm(uint64_t doc_length) {
    const uint64_t clamped = std::clamp<uint64_t>(doc_length, 1, 255);
    return static_cast<uint8_t>(clamped);
}

ScorerContext ScorerContext::make(uint64_t n, uint64_t df) {
    ScorerContext ctx;
    ctx.df_ = df;
    const double nn = static_cast<double>(n);
    const double dff = static_cast<double>(df);
    // idf = log(1 + (N - df + 0.5) / (df + 0.5)); always positive for df <= N.
    ctx.idf_ = std::log(1.0 + (nn - dff + 0.5) / (dff + 0.5));
    return ctx;
}

double ScorerContext::score(uint32_t tf, uint8_t encoded_norm, double avgdl,
                            const Bm25Params& params) const {
    const double dl = decode_norm(encoded_norm);
    const double tff = static_cast<double>(tf);
    const double denom = tff + params.k1 * (1.0 - params.b + params.b * dl / avgdl);
    return idf_ * (tff * (params.k1 + 1.0)) / denom;
}

double ScorerContext::max_score(uint32_t max_freq, uint8_t min_norm, double avgdl,
                                const Bm25Params& params) const {
    // The score grows monotonically with tf and shrinks with dl, so the per-window
    // upper bound uses the window's largest tf and smallest dl (min encoded norm).
    return score(max_freq, min_norm, avgdl, params);
}

} // namespace snii::query
