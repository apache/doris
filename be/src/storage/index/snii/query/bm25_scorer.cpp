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

#include "storage/index/snii/query/bm25_scorer.h"

#include <algorithm>

namespace doris::snii::query {

double decode_norm(uint8_t encoded) {
    return encoded == 0 ? 1.0 : static_cast<double>(encoded);
}

uint8_t encode_norm(uint64_t doc_length) {
    const uint64_t clamped = std::clamp<uint64_t>(doc_length, 1, 255);
    return static_cast<uint8_t>(clamped);
}

ScorerContext ScorerContext::from_idf(double idf) {
    ScorerContext ctx;
    ctx.idf_ = idf;
    return ctx;
}

double ScorerContext::score(double tf, uint8_t encoded_norm, double avgdl,
                            const Bm25Params& params) const {
    const double dl = decode_norm(encoded_norm);
    const double denom = tf + params.k1 * (1.0 - params.b + params.b * dl / avgdl);
    return idf_ * (tf * (params.k1 + 1.0)) / denom;
}

} // namespace doris::snii::query
