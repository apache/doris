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

#include "olap/rowset/segment_v2/encoding_info.h"

#include <gen_cpp/segment_v2.pb.h>

#include <array>
#include <iterator>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/binary_dict_page.h"
#include "olap/rowset/segment_v2/binary_dict_page_pre_decoder.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/binary_plain_page_v2.h"
#include "olap/rowset/segment_v2/binary_plain_page_v2_pre_decoder.h"
#include "olap/rowset/segment_v2/binary_prefix_page.h"
#include "olap/rowset/segment_v2/bitshuffle_page.h"
#include "olap/rowset/segment_v2/bitshuffle_page_pre_decoder.h"
#include "olap/rowset/segment_v2/frame_of_reference_page.h"
#include "olap/rowset/segment_v2/plain_page.h"
#include "olap/rowset/segment_v2/rle_page.h"
#include "olap/types.h"
#include "runtime/exec_env.h"

namespace doris {
namespace segment_v2 {

template <FieldType type, typename CppType>
struct TypeEncodingTraits<type, PLAIN_ENCODING, CppType> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        return PlainPageBuilder<type>::create(builder, opts);
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new PlainPageDecoder<type>(data, opts);
        return Status::OK();
    }
};

template <FieldType type>
struct TypeEncodingTraits<type, PLAIN_ENCODING, Slice> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        return BinaryPlainPageBuilder<type>::create(builder, opts);
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new BinaryPlainPageDecoder<type>(data, opts);
        return Status::OK();
    }
};

template <FieldType type, typename CppType>
struct TypeEncodingTraits<type, PLAIN_ENCODING_V2, CppType> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        return PlainPageBuilder<type>::create(builder, opts);
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new PlainPageDecoder<type>(data, opts);
        return Status::OK();
    }
};

template <FieldType type>
struct TypeEncodingTraits<type, PLAIN_ENCODING_V2, Slice> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        return BinaryPlainPageV2Builder<type>::create(builder, opts);
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new BinaryPlainPageV2Decoder<type>(data, opts);
        return Status::OK();
    }
};

template <FieldType type, typename CppType>
struct TypeEncodingTraits<type, BIT_SHUFFLE, CppType,
                          typename std::enable_if<!std::is_same<CppType, Slice>::value>::type> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        return BitshufflePageBuilder<type>::create(builder, opts);
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new BitShufflePageDecoder<type>(data, opts);
        return Status::OK();
    }
};

template <>
struct TypeEncodingTraits<FieldType::OLAP_FIELD_TYPE_BOOL, RLE, bool> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        return RlePageBuilder<FieldType::OLAP_FIELD_TYPE_BOOL>::create(builder, opts);
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new RlePageDecoder<FieldType::OLAP_FIELD_TYPE_BOOL>(data, opts);
        return Status::OK();
    }
};

template <FieldType type>
struct TypeEncodingTraits<type, DICT_ENCODING, Slice> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        return BinaryDictPageBuilder::create(builder, opts);
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new BinaryDictPageDecoder(data, opts);
        return Status::OK();
    }
};

template <>
struct TypeEncodingTraits<FieldType::OLAP_FIELD_TYPE_DATE, FOR_ENCODING,
                          typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE>::CppType> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        return FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_DATE>::create(builder, opts);
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new FrameOfReferencePageDecoder<FieldType::OLAP_FIELD_TYPE_DATE>(data, opts);
        return Status::OK();
    }
};

template <>
struct TypeEncodingTraits<FieldType::OLAP_FIELD_TYPE_DATEV2, FOR_ENCODING,
                          typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATEV2>::CppType> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        return FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_DATEV2>::create(builder,
                                                                                      opts);
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new FrameOfReferencePageDecoder<FieldType::OLAP_FIELD_TYPE_DATEV2>(data, opts);
        return Status::OK();
    }
};

template <>
struct TypeEncodingTraits<FieldType::OLAP_FIELD_TYPE_DATETIMEV2, FOR_ENCODING,
                          typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>::CppType> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        return FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>::create(builder,
                                                                                          opts);
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder =
                new FrameOfReferencePageDecoder<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>(data, opts);
        return Status::OK();
    }
};

template <FieldType type, typename CppType>
struct TypeEncodingTraits<type, FOR_ENCODING, CppType,
                          typename std::enable_if<IsIntegral<CppType>::value>::type> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        return FrameOfReferencePageBuilder<type>::create(builder, opts);
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new FrameOfReferencePageDecoder<type>(data, opts);
        return Status::OK();
    }
};

template <FieldType type>
struct TypeEncodingTraits<type, PREFIX_ENCODING, Slice> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        return BinaryPrefixPageBuilder::create(builder, opts);
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new BinaryPrefixPageDecoder(data, opts);
        return Status::OK();
    }
};

EncodingInfoResolver::EncodingInfoResolver() {
    _add_map<FieldType::OLAP_FIELD_TYPE_TINYINT, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_TINYINT, FOR_ENCODING, true>();
    _add_map<FieldType::OLAP_FIELD_TYPE_TINYINT, PLAIN_ENCODING>();

    _add_map<FieldType::OLAP_FIELD_TYPE_SMALLINT, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_SMALLINT, FOR_ENCODING, true>();
    _add_map<FieldType::OLAP_FIELD_TYPE_SMALLINT, PLAIN_ENCODING>();

    _add_map<FieldType::OLAP_FIELD_TYPE_INT, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_INT, FOR_ENCODING, true>();
    _add_map<FieldType::OLAP_FIELD_TYPE_INT, PLAIN_ENCODING>();

    _add_map<FieldType::OLAP_FIELD_TYPE_BIGINT, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_BIGINT, FOR_ENCODING, true>();
    _add_map<FieldType::OLAP_FIELD_TYPE_BIGINT, PLAIN_ENCODING>();

    _add_map<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT, BIT_SHUFFLE>();

    _add_map<FieldType::OLAP_FIELD_TYPE_LARGEINT, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_LARGEINT, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_LARGEINT, FOR_ENCODING, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_FLOAT, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_FLOAT, PLAIN_ENCODING>();

    _add_map<FieldType::OLAP_FIELD_TYPE_DOUBLE, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DOUBLE, PLAIN_ENCODING>();

    _add_map<FieldType::OLAP_FIELD_TYPE_CHAR, DICT_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_CHAR, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_CHAR, PLAIN_ENCODING_V2>();
    _add_map<FieldType::OLAP_FIELD_TYPE_CHAR, PREFIX_ENCODING, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_VARCHAR, DICT_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_VARCHAR, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_VARCHAR, PLAIN_ENCODING_V2>();
    _add_map<FieldType::OLAP_FIELD_TYPE_VARCHAR, PREFIX_ENCODING, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_STRING, DICT_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_STRING, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_STRING, PLAIN_ENCODING_V2>();
    _add_map<FieldType::OLAP_FIELD_TYPE_STRING, PREFIX_ENCODING, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_JSONB, DICT_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_JSONB, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_JSONB, PLAIN_ENCODING_V2>();
    _add_map<FieldType::OLAP_FIELD_TYPE_JSONB, PREFIX_ENCODING, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_VARIANT, DICT_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_VARIANT, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_VARIANT, PLAIN_ENCODING_V2>();
    _add_map<FieldType::OLAP_FIELD_TYPE_VARIANT, PREFIX_ENCODING, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_BOOL, RLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_BOOL, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_BOOL, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_BOOL, PLAIN_ENCODING, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_DATE, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DATE, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DATE, FOR_ENCODING, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_DATEV2, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DATEV2, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DATEV2, FOR_ENCODING, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_DATETIMEV2, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DATETIMEV2, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DATETIMEV2, FOR_ENCODING, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_DATETIME, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DATETIME, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DATETIME, FOR_ENCODING, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL, BIT_SHUFFLE, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL32, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL32, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL32, BIT_SHUFFLE, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL64, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL64, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL64, BIT_SHUFFLE, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL128I, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL128I, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL128I, BIT_SHUFFLE, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL256, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL256, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_DECIMAL256, BIT_SHUFFLE, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_IPV4, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_IPV4, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_IPV4, BIT_SHUFFLE, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_IPV6, BIT_SHUFFLE>();
    _add_map<FieldType::OLAP_FIELD_TYPE_IPV6, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_IPV6, BIT_SHUFFLE, true>();

    _add_map<FieldType::OLAP_FIELD_TYPE_HLL, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_HLL, PLAIN_ENCODING_V2>();

    _add_map<FieldType::OLAP_FIELD_TYPE_BITMAP, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_BITMAP, PLAIN_ENCODING_V2>();

    _add_map<FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, PLAIN_ENCODING_V2>();

    _add_map<FieldType::OLAP_FIELD_TYPE_AGG_STATE, PLAIN_ENCODING>();
    _add_map<FieldType::OLAP_FIELD_TYPE_AGG_STATE, PLAIN_ENCODING_V2>();
}

EncodingInfoResolver::~EncodingInfoResolver() {
    for (auto& it : _encoding_map) {
        delete it.second;
    }
    _encoding_map.clear();
}

namespace {
bool is_integer_type(FieldType type) {
    return type == FieldType::OLAP_FIELD_TYPE_TINYINT ||
           type == FieldType::OLAP_FIELD_TYPE_SMALLINT || type == FieldType::OLAP_FIELD_TYPE_INT ||
           type == FieldType::OLAP_FIELD_TYPE_BIGINT || type == FieldType::OLAP_FIELD_TYPE_LARGEINT;
}

bool is_binary_type(FieldType type) {
    return type == FieldType::OLAP_FIELD_TYPE_CHAR || type == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
           type == FieldType::OLAP_FIELD_TYPE_STRING || type == FieldType::OLAP_FIELD_TYPE_JSONB ||
           type == FieldType::OLAP_FIELD_TYPE_VARIANT || type == FieldType::OLAP_FIELD_TYPE_HLL ||
           type == FieldType::OLAP_FIELD_TYPE_BITMAP ||
           type == FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE ||
           type == FieldType::OLAP_FIELD_TYPE_AGG_STATE;
}
} // namespace

EncodingTypePB EncodingInfoResolver::get_default_encoding(FieldType type,
                                                          bool optimize_value_seek) const {
    // Predicate for default encoding transformation
    // Parameters: (type, current_default_encoding, optimize_value_seek)
    // Returns: true if the transformation should be applied
    using Predicate = std::function<bool(FieldType, EncodingTypePB, bool)>;

    // Hook for transforming default encoding: predicate -> target encoding
    struct EncodingTransform {
        Predicate predicate;
        EncodingTypePB target_encoding;
    };

    // Static array of hooks for default encoding transformations
    static const std::vector<EncodingTransform> hooks = {
            // Hook 1: Binary types - PLAIN_ENCODING -> PLAIN_ENCODING_V2
            // Applies when: type is binary, encoding is PLAIN_ENCODING, and config enables v2
            EncodingTransform {
                    .predicate =
                            [](FieldType type, EncodingTypePB encoding, bool optimize_value_seek) {
                                return encoding == PLAIN_ENCODING && is_binary_type(type) &&
                                       config::binary_plain_encoding_default_impl == "v2";
                            },
                    .target_encoding = PLAIN_ENCODING_V2},

            // Hook 2: Integer types - any encoding -> PLAIN_ENCODING
            // Applies when: type is integer and config enables plain encoding for integers
            EncodingTransform {
                    .predicate =
                            [](FieldType type, EncodingTypePB encoding, bool optimize_value_seek) {
                                return is_integer_type(type) &&
                                       config::integer_type_default_use_plain_encoding;
                            },
                    .target_encoding = PLAIN_ENCODING}};

    auto& encoding_map =
            optimize_value_seek ? _value_seek_encoding_map : _default_encoding_type_map;
    auto it = encoding_map.find(type);
    if (it != encoding_map.end()) {
        EncodingTypePB encoding = it->second;

        // Apply hooks in order to transform the default encoding
        for (const auto& hook : hooks) {
            if (hook.predicate(type, encoding, optimize_value_seek)) {
                // Verify target encoding is available for this type
                if (_encoding_map.contains(std::make_pair(type, hook.target_encoding))) {
                    encoding = hook.target_encoding;
                    break; // Apply only the first matching hook
                }
            }
        }

        return encoding;
    }
    return UNKNOWN_ENCODING;
}

Status EncodingInfoResolver::get(FieldType data_type, EncodingTypePB encoding_type,
                                 const EncodingInfo** out) {
    if (encoding_type == DEFAULT_ENCODING) {
        encoding_type = get_default_encoding(data_type, false);
    }
    auto key = std::make_pair(data_type, encoding_type);
    auto it = _encoding_map.find(key);
    if (it == std::end(_encoding_map)) {
        return Status::InternalError("fail to find valid type encoding, type:{}, encoding:{}",
                                     data_type, encoding_type);
    }
    *out = it->second;
    return Status::OK();
}

template <typename TraitsClass>
EncodingInfo::EncodingInfo(TraitsClass traits)
        : _create_builder_func(TraitsClass::create_page_builder),
          _create_decoder_func(TraitsClass::create_page_decoder),
          _type(TraitsClass::type),
          _encoding(TraitsClass::encoding) {
    if (_encoding == BIT_SHUFFLE) {
        _data_page_pre_decoder = std::make_unique<BitShufflePagePreDecoder>();
    } else if (_encoding == DICT_ENCODING) {
        _data_page_pre_decoder = std::make_unique<BinaryDictPagePreDecoder>();
    } else if (_encoding == PLAIN_ENCODING_V2) {
        // Only binary types (Slice) need the predecoder for PLAIN_ENCODING_V2
        // to convert varint-encoded lengths to offset array format
        if constexpr (std::is_same_v<typename TraitsClass::CppType, Slice>) {
            _data_page_pre_decoder = std::make_unique<BinaryPlainPageV2PreDecoder>();
        }
    }
}

#ifdef BE_TEST
static EncodingInfoResolver s_encoding_info_resolver;
#endif

Status EncodingInfo::get(FieldType type, EncodingTypePB encoding_type, const EncodingInfo** out) {
#ifdef BE_TEST
    return s_encoding_info_resolver.get(type, encoding_type, out);
#else
    auto* resolver = ExecEnv::GetInstance()->get_encoding_info_resolver();
    if (resolver == nullptr) {
        return Status::InternalError("EncodingInfoResolver not initialized");
    }
    return resolver->get(type, encoding_type, out);
#endif
}

EncodingTypePB EncodingInfo::get_default_encoding(FieldType type, bool optimize_value_seek) {
#ifdef BE_TEST
    return s_encoding_info_resolver.get_default_encoding(type, optimize_value_seek);
#else
    auto* resolver = ExecEnv::GetInstance()->get_encoding_info_resolver();
    if (resolver == nullptr) {
        return UNKNOWN_ENCODING;
    }
    return resolver->get_default_encoding(type, optimize_value_seek);
#endif
}

Status EncodingInfo::create_page_builder(const PageBuilderOptions& opts,
                                         std::unique_ptr<PageBuilder>& builder) const {
    PageBuilder* raw_builder = nullptr;
    RETURN_IF_ERROR(_create_builder_func(opts, &raw_builder));
    builder.reset(raw_builder);
    return Status::OK();
}

Status EncodingInfo::create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                         std::unique_ptr<PageDecoder>& decoder) const {
    PageDecoder* raw_decoder = nullptr;
    RETURN_IF_ERROR(_create_decoder_func(data, opts, &raw_decoder));
    decoder.reset(raw_decoder);
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
