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

#include <parallel_hashmap/phmap.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "vec/common/arena.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type.h"

namespace doris {
struct StringRef;

namespace segment_v2 {
enum EncodingTypePB : int;
template <FieldType Type>
class BitShufflePageDecoder;

enum { BINARY_DICT_PAGE_HEADER_SIZE = 4 };

// This type of page use dictionary encoding for strings.
// There is only one dictionary page for all the data pages within a column.
//
// Layout for dictionary encoded data page:
// The data page starts with a 4-byte header (EncodingTypePB) followed by the encoded data.
// There are three possible encoding formats:
//
// 1. header(4 bytes) + bitshuffle encoded codeword page, when mode_ = DICT_ENCODING.
//    The codeword page contains integer codes referencing the dictionary, compressed with bitshuffle+lz4.
//
// 2. header(4 bytes) + BinaryPlainPageV2, when mode_ = PLAIN_ENCODING_V2.
//    Used as fallback when dictionary is full. Stores raw strings with varint-encoded lengths.
//
// 3. header(4 bytes) + BinaryPlainPage, when mode_ = PLAIN_ENCODING.
//    Used as fallback when dictionary is full. Stores raw strings with offset array.
//
// Data pages start with mode_ = DICT_ENCODING. When the size of dictionary page
// goes beyond options.dict_page_size, subsequent data pages will switch to plain
// encoding (either PLAIN_ENCODING_V2 or PLAIN_ENCODING based on config) automatically.
//
// The dictionary page itself is encoded as either BinaryPlainPage (PLAIN_ENCODING) or
// BinaryPlainPageV2 (PLAIN_ENCODING_V2), determined by config::binary_plain_encoding_default_impl.
class BinaryDictPageBuilder : public PageBuilderHelper<BinaryDictPageBuilder> {
public:
    using Self = BinaryDictPageBuilder;
    friend class PageBuilderHelper<Self>;

    Status init() override;

    bool is_page_full() override;

    Status add(const uint8_t* vals, size_t* count) override;

    Status finish(OwnedSlice* slice) override;

    Status reset() override;

    size_t count() const override;

    uint64_t size() const override;

    Status get_dictionary_page(OwnedSlice* dictionary_page) override;

    Status get_dictionary_page_encoding(EncodingTypePB* encoding) const override;

    Status get_first_value(void* value) const override;

    Status get_last_value(void* value) const override;

    uint64_t get_raw_data_size() const override;

private:
    BinaryDictPageBuilder(const PageBuilderOptions& options);

    PageBuilderOptions _options;
    bool _finished;

    std::unique_ptr<PageBuilder> _data_page_builder;

    std::unique_ptr<PageBuilder> _dict_builder = nullptr;

    EncodingTypePB _encoding_type;

    EncodingTypePB
            _dict_word_page_encoding_type; // currently only support PLAIN_ENCODING and PLAIN_ENCODING_V2
    EncodingTypePB
            _fallback_binary_encoding_type; // currently only support PLAIN_ENCODING and PLAIN_ENCODING_V2

    struct HashOfSlice {
        size_t operator()(const Slice& slice) const { return crc32_hash(slice.data, slice.size); }
    };
    // query for dict item -> dict id
    phmap::flat_hash_map<Slice, uint32_t, HashOfSlice> _dictionary;
    // TODO(zc): rethink about this arena
    vectorized::Arena _arena;
    faststring _buffer;
    faststring _first_value;
    uint64_t _raw_data_size = 0;

    bool _has_empty = false;
    uint32_t _empty_code = 0;
};

class BinaryDictPageDecoder : public PageDecoder {
public:
    BinaryDictPageDecoder(Slice data, const PageDecoderOptions& options);

    Status init() override;

    Status seek_to_position_in_page(size_t pos) override;

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst) override;

    Status read_by_rowids(const rowid_t* rowids, ordinal_t page_first_ordinal, size_t* n,
                          vectorized::MutableColumnPtr& dst) override;

    size_t count() const override { return _data_page_decoder->count(); }

    size_t current_index() const override { return _data_page_decoder->current_index(); }

    bool is_dict_encoding() const;

    void set_dict_decoder(uint32_t num_dict_items, StringRef* dict_word_info);

    ~BinaryDictPageDecoder() override;

private:
    Slice _data;
    PageDecoderOptions _options;
    std::unique_ptr<PageDecoder> _data_page_decoder;
    BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_INT>* _bit_shuffle_ptr = nullptr;
    bool _parsed;
    EncodingTypePB _encoding_type;

    StringRef* _dict_word_info = nullptr;
    uint32_t _num_dict_items = 0;

    std::vector<int32_t> _buffer;
};

} // namespace segment_v2
} // namespace doris
