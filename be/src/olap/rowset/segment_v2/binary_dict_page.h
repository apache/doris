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

#include <memory>
#include <unordered_map>
#include <functional>
#include <string>

#include "olap/olap_common.h"
#include "olap/types.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/column_block.h"
#include "util/arena.h"
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/hash/string_hash.h"

namespace doris {
namespace segment_v2 {

enum {
    BINARY_DICT_PAGE_HEADER_SIZE = 4
};

// This type of page use dictionary encoding for strings.
// There is only one dictionary page for all the data pages within a column.
//
// Layout for dictionary encoded page:
// Either header + embedded codeword page, which can be encoded with any
//        int PageBuilder, when mode_ = DICT_ENCODING.
// Or     header + embedded BinaryPlainPage, when mode_ = PLAIN_ENCOING.
// Data pages start with mode_ = DICT_ENCODING, when the the size of dictionary
// page go beyond the option_->dict_page_size, the subsequent data pages will switch
// to string plain page automatically.
class BinaryDictPageBuilder : public PageBuilder {
public:
    BinaryDictPageBuilder(const PageBuilderOptions& options);

    bool is_page_full() override;

    Status add(const uint8_t* vals, size_t* count) override;

    Slice finish() override;

    void reset() override;

    size_t count() const override;

    Status get_dictionary_page(Slice* dictionary_page) override;

    // this api will release the memory ownership of encoded data
    // Note:
    //     release() should be called after finish
    //     reset() should be called after this function before reuse the builder
    void release() override {
        uint8_t* ret = _buffer.release();
        (void)ret;
    }

private:
    PageBuilderOptions _options;
    bool _finished;

    std::unique_ptr<PageBuilder> _data_page_builder;

    std::unique_ptr<BinaryPlainPageBuilder> _dict_builder;

    EncodingTypePB _encoding_type;
    struct HashOfSlice {                                                                                                                                              
        size_t operator()(const Slice& slice) const {
            return HashStringThoroughly(slice.data, slice.size);
        }   
    };
    // query for dict item -> dict id
    std::unordered_map<Slice, uint32_t, HashOfSlice> _dictionary;
    // used to remember the insertion order of dict keys
    std::vector<Slice> _dict_items;
    Arena _arena;
    faststring _buffer;
};

class BinaryDictPageDecoder : public PageDecoder {
public:
    BinaryDictPageDecoder(Slice data, const PageDecoderOptions& options);

    Status init() override;

    Status seek_to_position_in_page(size_t pos) override;

    Status next_batch(size_t* n, ColumnBlockView* dst) override;

    size_t count() const override {
        return _data_page_decoder->count();
    }

    size_t current_index() const override {
        return _data_page_decoder->current_index();
    }

private:
    Slice _data;
    PageDecoderOptions _options;
    std::unique_ptr<PageDecoder> _data_page_decoder;
    std::shared_ptr<BinaryPlainPageDecoder> _dict_decoder;
    bool _parsed;
    EncodingTypePB _encoding_type;
    faststring _code_buf;
};

} // namespace segment_v2
} // namespace doris
