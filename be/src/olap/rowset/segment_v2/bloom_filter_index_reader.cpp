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

Status BloomFilterIndexReader::load() {
    const IndexedColumnMetaPB& bf_index_meta = _bloom_filter_index_meta.bloom_filter();

    _bloom_filter_reader.reset(new IndexedColumnReader(_file, bf_index_meta));
    RETURN_IF_ERROR(_bloom_filter_reader->load());
    return Status::OK();
}

Status BloomFilterIndexReader::new_iterator(std::unique_ptr<BloomFilterIndexIterator>* iterator) {
    iterator->reset(new BloomFilterIndexIterator(this));
    return Status::OK();
}

Status BloomFilterIndexIterator::read_bloom_filter(rowid_t ordinal, std::unique_ptr<BloomFilter>* bf) {
    // value[0] is bloom filter data
    // value[1] is has_null flag
    Slice value;
    uint8_t nullmap;
    size_t num_to_read = 1;
    ColumnBlock block(_reader->type_info(), (uint8_t*)&value, &nullmap, num_to_read, _pool.get());
    ColumnBlockView column_block_view(&block);

    RETURN_IF_ERROR(_bloom_filter_iter.seek_to_ordinal(ordinal));
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(_bloom_filter_iter.next_batch(&num_read, &column_block_view));
    DCHECK(num_to_read == num_read);
    // construct bloom filter
    BloomFilter::create(_reader->_bloom_filter_index_meta.algorithm(), bf);
    auto ret = (*bf)->init(value.data, value.size, _reader->_bloom_filter_index_meta.hash_strategy());
    if (!ret) {
        return Status::InternalError("init bloom filter failed");
    }
    _pool->clear();
    return Status::OK();
}

Status BloomFilterIndexIterator::read_range_bloom_filters(rowid_t from, rowid_t to, std::map<RowRange, std::unique_ptr<BloomFilter>,
        RowRange::Comparator>* bfs) {
    DCHECK(from < to);
    //DCHECK(0 <= from && from <= to && to <= _reader->bitmap_nums());
    // get the bloom filters between [from, to)
    rowid_t ordinal = from;
    while (ordinal < to) {
        std::unique_ptr<BloomFilter> bf;
        RETURN_IF_ERROR(read_bloom_filter(ordinal, &bf));
        rowid_t page_last_rowid = _bloom_filter_iter.current_page_last_rowid();
        RowRange row_range(ordinal, page_last_rowid + 1);
        (*bfs)[row_range] = std::move(bf);
        ordinal = page_last_rowid + 1;
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris