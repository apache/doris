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

#include <vector>
#include <memory>

#include "common/status.h"
#include "util/slice.h"
#include "olap/bloom_filter.hpp"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"

namespace doris {
namespace segment_v2 {

// This class encodes bloom filter data of column.
// We will encode every num_rows_per_block rows'
// bloom filter info into one BloomFilter.
// the format is as following:
//      BloomFilter of blocks
//      BloomFilterPageFooterPB
class BloomFilterPageBuilder {
public:
    BloomFilterPageBuilder(const TypeInfo* type_info, size_t bloom_filter_block_size, double fp_rate)
            : _type_info(type_info), _block_size(bloom_filter_block_size), _num_inserted(0) {
        PageBuilderOptions opts;
        // do not limit the page size
        opts.data_page_size = 0;
        _page_builder.reset(new BinaryPlainPageBuilder(opts));
        _bf_builder.init(_block_size, fp_rate);
    }

    Status add(const uint8_t* vals, size_t count);

    Status flush();

    Slice finish();

private:
    const TypeInfo* _type_info;
    uint32_t _block_size;
    std::unique_ptr<BinaryPlainPageBuilder> _page_builder;
    BloomFilter _bf_builder;
    size_t _num_inserted;
};

// BloomFilterPage manage column's bloom filters.
// Bloom Filter info of one column will be totally loaded into memory at a time for now.
class BloomFilterPage {
public:
    BloomFilterPage(Slice data)
        : _data(data), _block_num(0), _expected_num(0) {
    }
    
    // parse bloom filter of each block.
    Status load();
    
    // get column's bloom filter infos
    std::shared_ptr<BloomFilter> get_bloom_filter(int index) {
        return _bloom_filters[index];
    }

    // the number of bloom filter blocks
    size_t block_num() { return _block_num; }

    uint32_t expected_num() { return _expected_num; }

private:
    Slice _data;
    size_t _block_num;
    uint32_t _expected_num;
    std::vector<std::shared_ptr<BloomFilter>> _bloom_filters;
};

} // namespace segment_v2
} // namespace doris
