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

#include "olap/rowset/segment_v2/bloom_filter_index_reader.h"

#include "olap/types.h"
#include "olap/rowset/segment_v2/bloom_filter.h"

namespace doris {
namespace segment_v2 {

Status BloomFilterIndexReader::load(bool cache_in_memory) {
    const IndexedColumnMetaPB& bf_index_meta = _bloom_filter_index_meta.bloom_filter();

    _bloom_filter_reader.reset(new IndexedColumnReader(_file_name, bf_index_meta, cache_in_memory));
    RETURN_IF_ERROR(_bloom_filter_reader->load());
    return Status::OK();
}

Status BloomFilterIndexReader::new_iterator(std::unique_ptr<BloomFilterIndexIterator>* iterator) {
    iterator->reset(new BloomFilterIndexIterator(this));
    return Status::OK();
}

Status BloomFilterIndexIterator::read_bloom_filter(rowid_t ordinal, std::unique_ptr<BloomFilter>* bf) {
    Slice value;
    uint8_t nullmap = 0;
    size_t num_to_read = 1;
    ColumnBlock block(_reader->type_info(), (uint8_t*)&value, &nullmap, num_to_read, _pool.get());
    ColumnBlockView column_block_view(&block);

    RETURN_IF_ERROR(_bloom_filter_iter.seek_to_ordinal(ordinal));
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(_bloom_filter_iter.next_batch(&num_read, &column_block_view));
    DCHECK(num_to_read == num_read);
    // construct bloom filter
    BloomFilter::create(_reader->_bloom_filter_index_meta.algorithm(), bf);
    RETURN_IF_ERROR((*bf)->init(value.data, value.size, _reader->_bloom_filter_index_meta.hash_strategy()));
    _pool->clear();
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris