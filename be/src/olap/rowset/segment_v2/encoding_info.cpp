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

#include <type_traits>

#include "gutil/strings/substitute.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/binary_dict_page.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/binary_prefix_page.h"
#include "olap/rowset/segment_v2/bitshuffle_page.h"
#include "olap/rowset/segment_v2/frame_of_reference_page.h"
#include "olap/rowset/segment_v2/plain_page.h"
#include "olap/rowset/segment_v2/rle_page.h"

namespace doris {
namespace segment_v2 {

struct EncodingMapHash {
    size_t operator()(const std::pair<FieldType, EncodingTypePB>& pair) const {
        return (pair.first << 5) ^ pair.second;
    }
};

template <FieldType type, EncodingTypePB encoding, typename CppType, typename Enabled = void>
struct TypeEncodingTraits {};

template <FieldType type, typename CppType>
struct TypeEncodingTraits<type, PLAIN_ENCODING, CppType> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        *builder = new PlainPageBuilder<type>(opts);
        return Status::OK();
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
        *builder = new BinaryPlainPageBuilder(opts);
        return Status::OK();
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new BinaryPlainPageDecoder(data, opts);
        return Status::OK();
    }
};

template <FieldType type, typename CppType>
struct TypeEncodingTraits<type, BIT_SHUFFLE, CppType,
                          typename std::enable_if<!std::is_same<CppType, Slice>::value>::type> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        *builder = new BitshufflePageBuilder<type>(opts);
        return Status::OK();
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new BitShufflePageDecoder<type>(data, opts);
        return Status::OK();
    }
};

template <>
struct TypeEncodingTraits<OLAP_FIELD_TYPE_ARRAY, BIT_SHUFFLE, CollectionValue> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        *builder = new BitshufflePageBuilder<OLAP_FIELD_TYPE_UNSIGNED_BIGINT>(opts);
        return Status::OK();
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new BitShufflePageDecoder<OLAP_FIELD_TYPE_UNSIGNED_BIGINT>(data, opts);
        return Status::OK();
    }
};

template <>
struct TypeEncodingTraits<OLAP_FIELD_TYPE_BOOL, RLE, bool> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        *builder = new RlePageBuilder<OLAP_FIELD_TYPE_BOOL>(opts);
        return Status::OK();
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new RlePageDecoder<OLAP_FIELD_TYPE_BOOL>(data, opts);
        return Status::OK();
    }
};

template <FieldType type>
struct TypeEncodingTraits<type, DICT_ENCODING, Slice> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        *builder = new BinaryDictPageBuilder(opts);
        return Status::OK();
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new BinaryDictPageDecoder(data, opts);
        return Status::OK();
    }
};

template <>
struct TypeEncodingTraits<OLAP_FIELD_TYPE_DATE, FOR_ENCODING,
                          typename CppTypeTraits<OLAP_FIELD_TYPE_DATE>::CppType> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        *builder = new FrameOfReferencePageBuilder<OLAP_FIELD_TYPE_DATE>(opts);
        return Status::OK();
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new FrameOfReferencePageDecoder<OLAP_FIELD_TYPE_DATE>(data, opts);
        return Status::OK();
    }
};

template <FieldType type, typename CppType>
struct TypeEncodingTraits<type, FOR_ENCODING, CppType,
                          typename std::enable_if<std::is_integral<CppType>::value>::type> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        *builder = new FrameOfReferencePageBuilder<type>(opts);
        return Status::OK();
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
        *builder = new BinaryPrefixPageBuilder(opts);
        return Status::OK();
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                                      PageDecoder** decoder) {
        *decoder = new BinaryPrefixPageDecoder(data, opts);
        return Status::OK();
    }
};

template <FieldType field_type, EncodingTypePB encoding_type>
struct EncodingTraits : TypeEncodingTraits<field_type, encoding_type,
                                           typename CppTypeTraits<field_type>::CppType> {
    static const FieldType type = field_type;
    static const EncodingTypePB encoding = encoding_type;
};

class EncodingInfoResolver {
public:
    EncodingInfoResolver();
    ~EncodingInfoResolver();

    EncodingTypePB get_default_encoding(FieldType type, bool optimize_value_seek) const {
        auto& encoding_map =
                optimize_value_seek ? _value_seek_encoding_map : _default_encoding_type_map;
        auto it = encoding_map.find(type);
        if (it != encoding_map.end()) {
            return it->second;
        }
        return UNKNOWN_ENCODING;
    }

    Status get(FieldType data_type, EncodingTypePB encoding_type, const EncodingInfo** out);

private:
    // Not thread-safe
    template <FieldType type, EncodingTypePB encoding_type, bool optimize_value_seek = false>
    void _add_map() {
        EncodingTraits<type, encoding_type> traits;
        std::unique_ptr<EncodingInfo> encoding(new EncodingInfo(traits));
        if (_default_encoding_type_map.find(type) == std::end(_default_encoding_type_map)) {
            _default_encoding_type_map[type] = encoding_type;
        }
        if (optimize_value_seek &&
            _value_seek_encoding_map.find(type) == _value_seek_encoding_map.end()) {
            _value_seek_encoding_map[type] = encoding_type;
        }
        auto key = std::make_pair(type, encoding_type);
        auto it = _encoding_map.find(key);
        if (it != _encoding_map.end()) {
            return;
        }
        _encoding_map.emplace(key, encoding.release());
    }

    std::unordered_map<FieldType, EncodingTypePB, std::hash<int>> _default_encoding_type_map;

    // default encoding for each type which optimizes value seek
    std::unordered_map<FieldType, EncodingTypePB, std::hash<int>> _value_seek_encoding_map;

    std::unordered_map<std::pair<FieldType, EncodingTypePB>, EncodingInfo*, EncodingMapHash>
            _encoding_map;
};

EncodingInfoResolver::EncodingInfoResolver() {
    _add_map<OLAP_FIELD_TYPE_TINYINT, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_TINYINT, FOR_ENCODING, true>();
    _add_map<OLAP_FIELD_TYPE_TINYINT, PLAIN_ENCODING>();

    _add_map<OLAP_FIELD_TYPE_SMALLINT, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_SMALLINT, FOR_ENCODING, true>();
    _add_map<OLAP_FIELD_TYPE_SMALLINT, PLAIN_ENCODING>();

    _add_map<OLAP_FIELD_TYPE_INT, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_INT, FOR_ENCODING, true>();
    _add_map<OLAP_FIELD_TYPE_INT, PLAIN_ENCODING>();

    _add_map<OLAP_FIELD_TYPE_BIGINT, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_BIGINT, FOR_ENCODING, true>();
    _add_map<OLAP_FIELD_TYPE_BIGINT, PLAIN_ENCODING>();

    _add_map<OLAP_FIELD_TYPE_UNSIGNED_BIGINT, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_UNSIGNED_INT, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_ARRAY, BIT_SHUFFLE>();

    _add_map<OLAP_FIELD_TYPE_LARGEINT, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_LARGEINT, PLAIN_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_LARGEINT, FOR_ENCODING, true>();

    _add_map<OLAP_FIELD_TYPE_FLOAT, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_FLOAT, PLAIN_ENCODING>();

    _add_map<OLAP_FIELD_TYPE_DOUBLE, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_DOUBLE, PLAIN_ENCODING>();

    _add_map<OLAP_FIELD_TYPE_CHAR, DICT_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_CHAR, PLAIN_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_CHAR, PREFIX_ENCODING, true>();

    _add_map<OLAP_FIELD_TYPE_VARCHAR, DICT_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_VARCHAR, PLAIN_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_VARCHAR, PREFIX_ENCODING, true>();

    _add_map<OLAP_FIELD_TYPE_STRING, DICT_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_STRING, PLAIN_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_STRING, PREFIX_ENCODING, true>();

    _add_map<OLAP_FIELD_TYPE_BOOL, RLE>();
    _add_map<OLAP_FIELD_TYPE_BOOL, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_BOOL, PLAIN_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_BOOL, PLAIN_ENCODING, true>();

    _add_map<OLAP_FIELD_TYPE_DATE, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_DATE, PLAIN_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_DATE, FOR_ENCODING, true>();

    _add_map<OLAP_FIELD_TYPE_DATETIME, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_DATETIME, PLAIN_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_DATETIME, FOR_ENCODING, true>();

    _add_map<OLAP_FIELD_TYPE_DECIMAL, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_DECIMAL, PLAIN_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_DECIMAL, BIT_SHUFFLE, true>();

    _add_map<OLAP_FIELD_TYPE_HLL, PLAIN_ENCODING>();

    _add_map<OLAP_FIELD_TYPE_OBJECT, PLAIN_ENCODING>();
}

EncodingInfoResolver::~EncodingInfoResolver() {
    for (auto& it : _encoding_map) {
        delete it.second;
    }
    _encoding_map.clear();
}

Status EncodingInfoResolver::get(FieldType data_type, EncodingTypePB encoding_type,
                                 const EncodingInfo** out) {
    if (encoding_type == DEFAULT_ENCODING) {
        encoding_type = get_default_encoding(data_type, false);
    }
    auto key = std::make_pair(data_type, encoding_type);
    auto it = _encoding_map.find(key);
    if (it == std::end(_encoding_map)) {
        return Status::InternalError(
                strings::Substitute("fail to find valid type encoding, type:$0, encoding:$1",
                                    data_type, encoding_type));
    }
    *out = it->second;
    return Status::OK();
}

static EncodingInfoResolver s_encoding_info_resolver;

template <typename TraitsClass>
EncodingInfo::EncodingInfo(TraitsClass traits)
        : _create_builder_func(TraitsClass::create_page_builder),
          _create_decoder_func(TraitsClass::create_page_decoder),
          _type(TraitsClass::type),
          _encoding(TraitsClass::encoding) {}

Status EncodingInfo::get(const TypeInfo* type_info, EncodingTypePB encoding_type,
                         const EncodingInfo** out) {
    return s_encoding_info_resolver.get(type_info->type(), encoding_type, out);
}

EncodingTypePB EncodingInfo::get_default_encoding(const TypeInfo* type_info,
                                                  bool optimize_value_seek) {
    return s_encoding_info_resolver.get_default_encoding(type_info->type(), optimize_value_seek);
}

} // namespace segment_v2
} // namespace doris
