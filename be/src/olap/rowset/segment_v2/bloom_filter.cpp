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

#include "olap/rowset/segment_v2/bloom_filter.h"

#include <exception>

#include "gutil/strings/substitute.h"

namespace doris {
namespace segment_v2 {

void BloomFilterBuilder::add_key(const BloomKeyProbe& key) {
    uint32_t h = key.initial_hash();
    for (uint32_t i = 0; i < _n_hashes; ++i) {
        uint32_t pos = h % _n_bits;
        _bitmap.add(pos);
        h = key.mix_hash(h);
    }
    ++_num_inserted;
}

Status BloomFilter::load() {
    try {
        _bitmap = Roaring::read(_data.data);
    } catch (std::exception& e) {
        return Status::Corruption(strings::Substitute("parse bloom filter failed, exception:$0", e.what()));
    }
    return Status::OK();
}

bool BloomFilter::check_key(const BloomKeyProbe& key) const {
    uint32_t h = key.initial_hash();
    for (uint32_t i = 0; i < _n_hashes; ++i) {
        uint32_t pos = h % _bit_size;
        if (!_bitmap.contains(pos)) {
            return false;
        }
        h = key.mix_hash(h);
    }
    return true;
}

} // namespace segment_v2
} // namespace doris
