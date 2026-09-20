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

#include "storage/index/query/spi/scoring_context.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>

#include "storage/index/inverted/similarity/bm25_similarity.h"

namespace doris::index_query {

TEST(ScoringContextTest, LuceneUpperBoundCoversQuantizedNorms) {
    for (float average_length : {1.0F, 32.0F, 1000.0F}) {
        SCOPED_TRACE(average_length);
        segment_v2::BM25Similarity similarity(2.0F, average_length);
        ScoringContext<float>& scoring = similarity;
        for (int32_t frequency : {1, 24, 25, 31, 63, 127, 255, 511, 1023, 4095, 65535,
                                  std::numeric_limits<int32_t>::max()}) {
            SCOPED_TRACE(frequency);
            const auto encoded_length = segment_v2::BM25Similarity::int_to_byte4(frequency);
            EXPECT_GE(scoring.max_score(), scoring.score(frequency, encoded_length));
        }
    }
}

} // namespace doris::index_query
