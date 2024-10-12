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

#include <stddef.h>

#include <memory>
#include <utility>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "olap/types.h"
#include "util/once.h"

namespace doris {

namespace segment_v2 {

class BloomFilterIndexIterator;
class BloomFilter;
class BloomFilterIndexPB;

class BloomFilterIndexReader : public MetadataAdder<BloomFilterIndexReader> {
public:
    explicit BloomFilterIndexReader(io::FileReaderSPtr file_reader,
                                    const BloomFilterIndexPB& bloom_filter_index_meta)
            : _file_reader(std::move(file_reader)),
              _type_info(get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_VARCHAR>()) {
        _bloom_filter_index_meta.reset(new BloomFilterIndexPB(bloom_filter_index_meta));
    }

    Status load(bool use_page_cache, bool kept_in_memory);

    BloomFilterAlgorithmPB algorithm() { return _bloom_filter_index_meta->algorithm(); }

    // create a new column iterator.
    Status new_iterator(std::unique_ptr<BloomFilterIndexIterator>* iterator);

    const TypeInfo* type_info() const { return _type_info; }

private:
    Status _load(bool use_page_cache, bool kept_in_memory);

    int64_t get_metadata_size() const override;

private:
    friend class BloomFilterIndexIterator;

    io::FileReaderSPtr _file_reader;
    DorisCallOnce<Status> _load_once;
    const TypeInfo* _type_info = nullptr;
    std::unique_ptr<BloomFilterIndexPB> _bloom_filter_index_meta = nullptr;
    std::unique_ptr<IndexedColumnReader> _bloom_filter_reader;
};

class BloomFilterIndexIterator {
public:
    explicit BloomFilterIndexIterator(BloomFilterIndexReader* reader)
            : _reader(reader), _bloom_filter_iter(reader->_bloom_filter_reader.get()) {}

    // Read bloom filter at the given ordinal into `bf`.
    Status read_bloom_filter(rowid_t ordinal, std::unique_ptr<BloomFilter>* bf);

    size_t current_bloom_filter_index() const { return _bloom_filter_iter.get_current_ordinal(); }

private:
    BloomFilterIndexReader* _reader = nullptr;
    IndexedColumnIterator _bloom_filter_iter;
};

} // namespace segment_v2
} // namespace doris
