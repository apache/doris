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

#include <functional>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "olap/types.h"

namespace doris {

class TypeInfo;

namespace segment_v2 {

class PageBuilder;
class PageDecoder;
struct PageBuilderOptions;
struct PageDecoderOptions;

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
};

} // namespace segment_v2
} // namespace doris
