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

#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>

#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/types.h"
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
namespace segment_v2 {

Status BloomFilterIndexReader::load(bool use_page_cache, bool kept_in_memory) {
    // TODO yyq: implement a new once flag to avoid status construct.
    return _load_once.call([this, use_page_cache, kept_in_memory] {
        return _load(use_page_cache, kept_in_memory);
    });
}

Status BloomFilterIndexReader::_load(bool use_page_cache, bool kept_in_memory) {
    const IndexedColumnMetaPB& bf_index_meta = _bloom_filter_index_meta->bloom_filter();

    _bloom_filter_reader.reset(new IndexedColumnReader(_file_reader, bf_index_meta));
    RETURN_IF_ERROR(_bloom_filter_reader->load(use_page_cache, kept_in_memory));
    return Status::OK();
}

Status BloomFilterIndexReader::new_iterator(std::unique_ptr<BloomFilterIndexIterator>* iterator) {
    iterator->reset(new BloomFilterIndexIterator(this));
    return Status::OK();
}

Status BloomFilterIndexIterator::read_bloom_filter(rowid_t ordinal,
                                                   std::unique_ptr<BloomFilter>* bf) {
    size_t num_to_read = 1;
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
            _reader->type_info()->type(), 1, 0);
    auto column = data_type->create_column();

    RETURN_IF_ERROR(_bloom_filter_iter.seek_to_ordinal(ordinal));
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(_bloom_filter_iter.next_batch(&num_read, column));
    DCHECK(num_to_read == num_read);
    // construct bloom filter
    StringRef value = column->get_data_at(0);
    static_cast<void>(
            BloomFilter::create(_reader->_bloom_filter_index_meta->algorithm(), bf, value.size));
    RETURN_IF_ERROR((*bf)->init(value.data, value.size,
                                _reader->_bloom_filter_index_meta->hash_strategy()));
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
