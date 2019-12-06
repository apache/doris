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

#include <set>

#include "common/logging.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/types.h"
#include "runtime/mem_pool.h"

namespace doris {
namespace segment_v2 {

template<typename CppType>
struct BloomFilterTraits {
    using ValueDict = std::set<CppType>;
};

template<>
struct BloomFilterTraits<Slice> {
    using ValueDict = std::set<Slice, Slice::Comparator>;
};

// BloomFilterPage is for building bloom filter of data
//
template<FieldType field_type>
class BloomFilterPageBuilder : public PageBuilder {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using ValueDict = typename BloomFilterTraits<CppType>::ValueDict;

    BloomFilterPageBuilder(const PageBuilderOptions& options) :
            _finished(false), _options(options), _count(0) {
        BloomFilter::create(options.bf_algorithm, &_bf);
        uint32_t num_bytes = BloomFilter::optimal_bit_num(_options.expected_num, _options.bf_fpp) / 8;
        _bf->init(num_bytes, _options.hash_strategy);
        reset();
    }

    ~BloomFilterPageBuilder() = default;

    bool is_page_full() override {
        // data_page_size is 0, do not limit the page size
        return (uint32_t)_distinct_values.size() + (uint32_t)_bf->has_null() >= _options.expected_num;
    }

    Status add(const uint8_t* vals, size_t* count) override {
        DCHECK(!_finished);
        DCHECK_GT(*count, 0);
        CppType* p = (CppType*)vals;
        if (vals == nullptr) {
            _bf->set_has_null(true);
            _count += *count;
            return Status::OK();
        }
        int i = 0;
        for (; i < *count; ++i) {
            if (is_page_full()) {
                break;
            }
            ++_count;
            if (_distinct_values.find(*p) != _distinct_values.end()) {
                // value exists, just skip
                continue;
            }
            if (_is_slice_type()) {
                Slice* value = (Slice*)p;
                _bf->add_bytes(value->data, value->size);
            } else {
                _bf->add_bytes((char*)p, (uint32_t)sizeof(CppType));
            }
            _distinct_values.insert(*p);
            ++p;
        }
        *count = i;
        return Status::OK();
    }

    OwnedSlice finish() override {
        DCHECK(!_finished);
        _finished = true;
        PageBuilderOptions options;
        // page size is not limited
        options.data_page_size = 0;
        BinaryPlainPageBuilder page_builder(options);
        // write bloom filter to page
        Slice bf_slice(_bf->data(), _bf->size());
        size_t to_add = 1;
        page_builder.add((const uint8_t*)&bf_slice, &to_add);
        return page_builder.finish();
    }

    void reset() override {
        _bf->reset();
        _finished = false;
        _count = 0;
        _distinct_values.clear();
    }

    size_t count() const override {
        return _count;
    }

    uint64_t size() const override {
        return _bf->size();
    }

    Status get_first_value(void* value) const override {
        return Status::NotSupported("bloom filter page do not support get first value");
    }
    Status get_last_value(void* value) const override {
        return Status::NotSupported("bloom filter page do not support get last value");
    }

private:
    // supported slice types are: OLAP_FIELD_TYPE_CHAR|OLAP_FIELD_TYPE_VARCHAR
    bool _is_slice_type() const {
        return field_type == OLAP_FIELD_TYPE_VARCHAR || field_type == OLAP_FIELD_TYPE_CHAR;
    }

private:
    bool _finished;
    PageBuilderOptions _options;
    std::unique_ptr<BloomFilter> _bf;
    size_t _count;
    ValueDict _distinct_values;
};

class BloomFilterPageDecoder : public PageDecoder {
public:
    BloomFilterPageDecoder(Slice data) : BloomFilterPageDecoder(data, PageDecoderOptions()) { }

    BloomFilterPageDecoder(Slice data, const PageDecoderOptions& options) : _data(data),
            _options(options),
            _parsed(false),
            _num_elems(0),
            _cur_idx(0),
            _page_decoder(nullptr) { }

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < BloomFilter::MINIMUM_BYTES + 1) {
            std::stringstream ss;
            ss << "file corrupton: not enough bytes for trailer in BloomFilterPageDecoder ."
                  "invalid data size:" << _data.size;
            return Status::Corruption(ss.str());
        }

        _page_decoder = new BinaryPlainPageDecoder(_data);
        RETURN_IF_ERROR(_page_decoder->init());
        _num_elems = 1;
        _parsed = true;

        return Status::OK();
    }

    Status seek_to_position_in_page(size_t pos) override {
        // seek to rowid 0
        _page_decoder->seek_to_position_in_page(0);
        _cur_idx = pos;
        return Status::OK();
    }

    Status next_batch(size_t* n, ColumnBlockView* dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(*n == 0 || _page_decoder->current_index() >= _num_elems)) {
            *n = 0;
            return Status::OK();
        }

        size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elems - _page_decoder->current_index()));
        _page_decoder->next_batch(&max_fetch, dst);

        *n = max_fetch;
        return Status::OK();
    }

    size_t count() const override {
        DCHECK(_parsed);
        return _num_elems;
    }

    size_t current_index() const override {
        DCHECK(_parsed);
        return _cur_idx;
    }

private:
    Slice _data;
    PageDecoderOptions _options;
    bool _parsed;
    uint32_t _num_elems;
    uint32_t _cur_idx;
    BinaryPlainPageDecoder* _page_decoder;
};

} // namespace segment_v2
} // namespace doris
