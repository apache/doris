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

#include <cstdint>
#include <iterator>
#include <string>
#include <vector>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {

// Used to encode a segment short key indices to binary format. This version
// only accepts binary key, client should assure that input key is sorted,
// otherwise error could happens. This builder would arrange data in following
// format.
//      index = encoded_keys + encoded_offsets + footer + footer_size + checksum
//      encoded_keys = binary_key + [, ...]
//      encoded_offsets = encoded_offset + [, ...]
//      encoded_offset = variant32
//      footer = ShortKeyFooterPB
//      footer_size = fixed32
//      checksum = fixed32
// Usage:
//      ShortKeyIndexBuilder builder(segment_id, num_rows_per_block);
//      builder.add_item(key1);
//      ...
//      builder.add_item(keyN);
//      builder.finalize(segment_size, num_rows, &slices);
// TODO(zc):
// 1. If this can leverage binary page to save key and offset data
// 2. Extending this to save in a BTree like struct, which can index full key
//    more than short key
class ShortKeyIndexBuilder {
public:
    ShortKeyIndexBuilder(uint32_t segment_id,
                         uint32_t num_rows_per_block) {
        _footer.set_segment_id(segment_id);
        _footer.set_num_rows_per_block(num_rows_per_block);
    }
    
    Status add_item(const Slice& key);

    Status finalize(uint32_t segment_size, uint32_t num_rows, std::vector<Slice>* slices);

private:
    segment_v2::ShortKeyFooterPB _footer;

    faststring _key_buf;
    faststring _offset_buf;
    std::string _footer_buf;
    std::vector<uint32_t> _offsets;
};

class ShortKeyIndexDecoder;

// An Iterator to iterate one short key index.
// Client can use this class to iterator all items
// item in this index.
class ShortKeyIndexIterator {
public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = Slice;
    using pointer = Slice*;
    using reference = Slice&;
    using difference_type = ssize_t;

    ShortKeyIndexIterator(const ShortKeyIndexDecoder* decoder, uint32_t ordinal = 0)
        : _decoder(decoder), _ordinal(ordinal) { }

    ShortKeyIndexIterator& operator-=(ssize_t step) {
        _ordinal -= step;
        return *this;
    }

    ShortKeyIndexIterator& operator+=(ssize_t step) {
        _ordinal += step;
        return *this;
    }

    ShortKeyIndexIterator& operator++() {
        _ordinal++;
        return *this;
    }
    
    bool operator!=(const ShortKeyIndexIterator& other) {
        return _ordinal != other._ordinal || _decoder != other._decoder;
    }

    bool operator==(const ShortKeyIndexIterator& other) {
        return _ordinal == other._ordinal && _decoder == other._decoder;
    }

    ssize_t operator-(const ShortKeyIndexIterator& other) const {
        return _ordinal - other._ordinal;
    }

    inline bool valid() const;

    Slice operator*() const;

    ssize_t ordinal() const { return _ordinal; }

private:
    const ShortKeyIndexDecoder* _decoder;
    ssize_t _ordinal;
};

// Used to decode short key to header and encoded index data.
// Usage:
//      MemIndex index;
//      ShortKeyIndexDecoder decoder(slice)
//      decoder.parse();
//      auto iter = decoder.lower_bound(key);
class ShortKeyIndexDecoder {
public:
    // Client should assure that data is available when this class
    // is used.
    ShortKeyIndexDecoder(const Slice& data) : _data(data) { }

    Status parse();

    ShortKeyIndexIterator begin() const { return {this, 0}; }
    ShortKeyIndexIterator end() const { return {this, num_items()}; }

    // lower_bound will return a iterator which locates the first item
    // equal with or larger than given key.
    // NOTE: This function holds that without common prefix key, the one
    // who has more length it the bigger one. Two key is the same only
    // when their length are equal
    ShortKeyIndexIterator lower_bound(const Slice& key) const {
        return seek<true>(key);
    }

    // Return the iterator which locates the first item larger than the
    // input key.
    ShortKeyIndexIterator upper_bound(const Slice& key) const {
        return seek<false>(key);
    }

    uint32_t num_items() const { return _footer.num_items(); }

    Slice key(ssize_t ordinal) const {
        DCHECK(ordinal >= 0 && ordinal < num_items());
        return {_key_data.data + _offsets[ordinal], _offsets[ordinal + 1] - _offsets[ordinal]};
    }

private:
    template<bool lower_bound>
    ShortKeyIndexIterator seek(const Slice& key) const {
        auto comparator = [this] (const Slice& lhs, const Slice& rhs) {
            return lhs.compare(rhs) < 0;
        };
        if (lower_bound) {
            return std::lower_bound(begin(), end(), key, comparator);
        } else {
            return std::upper_bound(begin(), end(), key, comparator);
        }
    }

private:
    Slice _data;

    // All following fields are only valid after pares has been executed successfully
    segment_v2::ShortKeyFooterPB _footer;
    std::vector<uint32_t> _offsets;
    Slice _key_data;
};

inline Slice ShortKeyIndexIterator::operator*() const {
    return _decoder->key(_ordinal);
}

inline bool ShortKeyIndexIterator::valid() const {
    return _ordinal >= 0  && _ordinal < _decoder->num_items();
}

}
