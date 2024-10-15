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

#include <functional>
#include <memory>

#include "common/status.h"
#include "olap/page_cache.h"
#include "util/slice.h"

namespace doris {

class TypeInfo;
enum class FieldType;

namespace segment_v2 {

class PageBuilder;
class PageDecoder;
struct PageBuilderOptions;
struct PageDecoderOptions;
enum EncodingTypePB : int;

// For better performance, some encodings (like BitShuffle) need to be decoded before being added to the PageCache.
class DataPagePreDecoder {
public:
    virtual Status decode(std::unique_ptr<DataPage>* page, Slice* page_slice, size_t size_of_tail,
                          bool _use_cache, segment_v2::PageTypePB page_type) = 0;
    virtual ~DataPagePreDecoder() = default;
};

class EncodingInfo {
public:
    // Get EncodingInfo for TypeInfo and EncodingTypePB
    static Status get(const TypeInfo* type_info, EncodingTypePB encoding_type,
                      const EncodingInfo** encoding);

    // optimize_value_search: whether the encoding scheme should optimize for ordered data
    // and support fast value seek operation
    static EncodingTypePB get_default_encoding(const TypeInfo* type_info, bool optimize_value_seek);

    Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) const {
        return _create_builder_func(opts, builder);
    }
    Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                               PageDecoder** decoder) const {
        return _create_decoder_func(data, opts, decoder);
    }
    FieldType type() const { return _type; }
    EncodingTypePB encoding() const { return _encoding; }

    DataPagePreDecoder* get_data_page_pre_decoder() const { return _data_page_pre_decoder.get(); }

private:
    friend class EncodingInfoResolver;

    template <typename TypeEncodingTraits>
    explicit EncodingInfo(TypeEncodingTraits traits);

    using CreateBuilderFunc = std::function<Status(const PageBuilderOptions&, PageBuilder**)>;
    CreateBuilderFunc _create_builder_func;

    using CreateDecoderFunc =
            std::function<Status(const Slice&, const PageDecoderOptions& opts, PageDecoder**)>;
    CreateDecoderFunc _create_decoder_func;

    FieldType _type;
    EncodingTypePB _encoding;
    std::unique_ptr<DataPagePreDecoder> _data_page_pre_decoder;
};

} // namespace segment_v2
} // namespace doris
