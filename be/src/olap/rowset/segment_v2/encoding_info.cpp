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

#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/bitshuffle_page.h"

namespace doris {
namespace segment_v2 {

struct EncodingMapHash {
    size_t operator()(const std::pair<FieldType, EncodingTypePB>& pair) const {
        return (pair.first << 5) ^ pair.second;
    }
};

template<FieldType type, EncodingTypePB encoding>
struct TypeEncodingTraits { };

template<FieldType type>
struct TypeEncodingTraits<type, PLAIN_ENCODING> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        return Status::OK();
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts, PageDecoder** decoder) {
        return Status::OK();
    }
};

template<FieldType type>
struct TypeEncodingTraits<type, BIT_SHUFFLE> {
    static Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) {
        *builder = new BitshufflePageBuilder<type>(opts);
        return Status::OK();
    }
    static Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts, PageDecoder** decoder) {
        *decoder = new BitShufflePageDecoder<type>(data, opts);
        return Status::OK();
    }
};

template<FieldType Type, EncodingTypePB Encoding>
struct EncodingTraits : TypeEncodingTraits<Type, Encoding> {
    static const FieldType type = Type;
    static const EncodingTypePB encoding = Encoding;
};


class EncodingInfoResolver {
public:
    EncodingInfoResolver();
    ~EncodingInfoResolver();

    EncodingTypePB get_default_encoding_type(FieldType type) const {
        auto it = _default_encoding_type_map.find(type);
        if (it != std::end(_default_encoding_type_map)) {
            return it->second;
        }
        return DEFAULT_ENCODING;
    }

    Status get(FieldType data_type, EncodingTypePB encoding_type, const EncodingInfo** out);

private:
    template<FieldType type, EncodingTypePB encoding_type>
    void _add_map() {
        EncodingTraits<type, encoding_type> traits;
        std::unique_ptr<EncodingInfo> encoding(new EncodingInfo(traits));
        if (_default_encoding_type_map.find(type) == std::end(_default_encoding_type_map)) {
            _default_encoding_type_map[type] = encoding_type;
        }
        auto key = std::make_pair(type, encoding_type);
        _encoding_map.emplace(key, encoding.release());
    }

    std::unordered_map<FieldType, EncodingTypePB, std::hash<int>> _default_encoding_type_map;

    std::unordered_map<std::pair<FieldType, EncodingTypePB>,
        EncodingInfo*, EncodingMapHash> _encoding_map;
};

EncodingInfoResolver::EncodingInfoResolver() {
    _add_map<OLAP_FIELD_TYPE_TINYINT, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_TINYINT, PLAIN_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_SMALLINT, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_SMALLINT, PLAIN_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_INT, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_INT, PLAIN_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_BIGINT, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_BIGINT, PLAIN_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_LARGEINT, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_LARGEINT, PLAIN_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_FLOAT, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_FLOAT, PLAIN_ENCODING>();
    _add_map<OLAP_FIELD_TYPE_DOUBLE, BIT_SHUFFLE>();
    _add_map<OLAP_FIELD_TYPE_DOUBLE, PLAIN_ENCODING>();
}

EncodingInfoResolver::~EncodingInfoResolver() {
    for (auto& it : _encoding_map) {
        delete it.second;
    }
    _encoding_map.clear();
}

Status EncodingInfoResolver::get(
        FieldType data_type,
        EncodingTypePB encoding_type,
        const EncodingInfo** out) {
    if (encoding_type == DEFAULT_ENCODING) {
        encoding_type = get_default_encoding_type(data_type);
    }
    auto key = std::make_pair(data_type, encoding_type);
    auto it = _encoding_map.find(key);
    if (it == std::end(_encoding_map)) {
        return Status::InternalError("fail to find valid type encoding");
    }
    *out = it->second;
    return Status::OK();
}

static EncodingInfoResolver s_encoding_info_resolver;

template<typename TraitsClass>
EncodingInfo::EncodingInfo(TraitsClass traits)
        : _create_builder_func(TraitsClass::create_page_builder),
        _create_decoder_func(TraitsClass::create_page_decoder),
        _type(TraitsClass::type),
        _encoding(TraitsClass::encoding) {
}

Status EncodingInfo::get(const TypeInfo* type_info,
                         EncodingTypePB encoding_type,
                         const EncodingInfo** out) {
    return s_encoding_info_resolver.get(type_info->type(), encoding_type, out);
}

EncodingTypePB EncodingInfo::get_default_encoding_type(const TypeInfo* type_info) {
    return s_encoding_info_resolver.get_default_encoding_type(type_info->type());
}

}
}
