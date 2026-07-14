// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>

// Bm25Scorer -- classic Okapi BM25 relevance scoring over SNII native stats.
//
// Per query term, idf is precomputed once from the collection statistics:
//   idf = log(1 + (N - df + 0.5) / (df + 0.5))
// where N = indexed doc count and df = the term's document frequency. The
// per-document contribution of a term then is:
//   score = idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avgdl))
// where tf is the in-doc term frequency, dl the document length decoded from the
// 1-byte encoded norm, and avgdl the average document length.
//
// Norm encode/decode (DOCUMENTED CONTRACT): the writer stores doc length as a
// byte-quantized value floor-clamped to [1, 255]; decode is the identity map
// back to a double length. encode_norm(len) = clamp(len, 1, 255);
// decode_norm(b) = (b == 0 ? 1.0 : (double)b). This keeps short docs (len <= 255)
// exact and saturates longer docs at 255, matching the reference oracle.
namespace doris::snii::query {

// BM25 free parameters. Defaults are the classic Lucene/Elasticsearch values.
struct Bm25Params {
    double k1 = 1.2;
    double b = 0.75;
};

// Decodes a 1-byte encoded norm into a document length. byte 0 maps to 1.0 to
// avoid a zero-length divisor; otherwise it is the byte value itself.
double decode_norm(uint8_t encoded);

// Encodes a document length into a 1-byte norm (clamped to [1, 255]). Provided
// so writers and test oracles share one quantization.
uint8_t encode_norm(uint64_t doc_length);

// Per-term scoring context: the precomputed idf and the term's df. Built once per
// query term, then reused for every candidate document of that term.
class ScorerContext {
public:
    // Builds the context from collection size n (indexed doc count) and the term's
    // document frequency df. avgdl and params are supplied per score call.
    static ScorerContext make(uint64_t n, uint64_t df);

    double idf() const { return idf_; }
    uint64_t df() const { return df_; }

    // Scores one document occurrence: tf is the in-doc term frequency, encoded_norm
    // the doc's 1-byte length norm, avgdl the collection average length.
    double score(uint32_t tf, uint8_t encoded_norm, double avgdl, const Bm25Params& params) const;

    // Upper bound on score() over any document, given a window's maximum tf and the
    // shortest doc length in the window (smallest dl maximizes the score). Used by
    // the WAND-style block-max pruner. max_freq is the window's max tf; min_norm is
    // the smallest encoded norm (=> smallest dl => largest score).
    double max_score(uint32_t max_freq, uint8_t min_norm, double avgdl,
                     const Bm25Params& params) const;

private:
    double idf_ = 0.0;
    uint64_t df_ = 0;
};

} // namespace doris::snii::query
