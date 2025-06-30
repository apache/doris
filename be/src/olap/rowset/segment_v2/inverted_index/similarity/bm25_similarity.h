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

#include "similarity.h"

namespace doris::segment_v2 {

class BM25Similarity : public Similarity {
public:
    BM25Similarity();
    ~BM25Similarity() override = default;

    void for_one_term(const IndexQueryContextPtr& context, const std::wstring& field_name,
                      const std::wstring& term) override;

    float score(float freq, int64_t encoded_norm) override;

    static uint8_t int_to_byte4(int32_t i);
    static int32_t byte4_to_int(uint8_t b);

private:
    static std::vector<float> initialize_length_table();

    static int32_t number_of_leading_zeros(uint64_t value);
    static uint32_t long_to_int4(uint64_t i);
    static uint64_t int4_to_long(uint32_t i);

    static const int32_t MAX_INT32;
    static const uint32_t MAX_INT4;
    static const int32_t NUM_FREE_VALUES;

    static std::vector<float> LENGTH_TABLE;

    float _boost = 1.0;
    float _k1 = 1.2;
    float _b = 0.75;
    float _idf = 0.0;
    float _avgdl = 0.0;
    float _weight = 1.0;

    std::vector<float> _cache;
};

} // namespace doris::segment_v2