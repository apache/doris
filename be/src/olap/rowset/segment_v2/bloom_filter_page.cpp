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

#include "olap/rowset/segment_v2/bloom_filter_page.h"

#include "util/debug_util.h"
#include "gen_cpp/segment_v2.pb.h"

namespace doris {
namespace segment_v2 {

Status BloomFilterPageBuilder::add(const uint8_t* vals, size_t count) {
    for (int i = 0; i < count; ++i) {
        if (vals == nullptr) {
            _bf_builder.add_bytes(nullptr, 1);
        } else {
            uint64_t hash = _type_info->hash_code64(vals, DEFAULT_SEED);
            _bf_builder.add_hash(hash);
        }
        
        if (vals != nullptr) {
            vals += _type_info->size();
        }
        ++_num_inserted;
        if (_num_inserted >= _block_size) {
            flush();
        }
    }
    return Status::OK();
}

Status BloomFilterPageBuilder::flush() {
    Slice data((char*)_bf_builder.bit_set_data(), _bf_builder.bit_set_data_len() * 8);
    size_t num = 1;
    RETURN_IF_ERROR(_page_builder->add((const uint8_t*)&data, &num));
    // reset the bloom filter builder
    _bf_builder.clear();
    _num_inserted = 0;
    return Status::OK();
}

Slice BloomFilterPageBuilder::finish() {
    // first flush last bloom filter block data
    if (_num_inserted > 0) {
        auto st = flush();
        if (!st.ok()) {
            return Slice();
        }
    }
    
    // write BloomFilterPageFooterPB to page
    BloomFilterPageFooterPB footer;
    footer.set_hash_function_num(_bf_builder.hash_function_num());
    footer.set_expected_num(_block_size);
    std::string value;
    bool ret = footer.SerializeToString(&value);
    if (!ret) {
        return Slice();
    }
    // add BloomFilterPageFooterPB as the last entry
    size_t num = 1;
    auto st = _page_builder->add((const uint8_t*)&value, &num);
    if (!st.ok()) {
        return Slice();
    }
    return _page_builder->finish();
}

Status BloomFilterPage::load() {
    BinaryPlainPageDecoder page_decoder(_data);
    RETURN_IF_ERROR(page_decoder.init());
    size_t count = page_decoder.count();
    if (count == 0) {
        return Status::Corruption("invalid bloom filter page");
    }
    // the number of bloom filter blocks
    _block_num = count - 1;
    // last entry is BloomFilterPageFooterPB slice
    Slice footer_slice = page_decoder.string_at_index(count - 1);
    BloomFilterPageFooterPB footer;
    bool ret = footer.ParseFromString(std::string(footer_slice.data, footer_slice.size));
    if (!ret) {
        return Status::Corruption("parse BloomFilterPageFooterPB failed");
    }
    uint32_t hash_function_num = footer.hash_function_num();
    _expected_num = footer.expected_num();
    for (int i = 0; i < count - 1; ++i) {
        Slice data = page_decoder.string_at_index(i);
        std::shared_ptr<BloomFilter> bloom_filter(new BloomFilter());
        bool ret = bloom_filter->init_with_deep_copy((uint64_t*)data.data, data.size / 8, hash_function_num);
        if (!ret) {
            return Status::Corruption("load bloom filter failed");
        }
        _bloom_filters.emplace_back(bloom_filter);
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
