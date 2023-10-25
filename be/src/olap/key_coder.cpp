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

#include "olap/key_coder.h"

#include <cstddef>
#include <unordered_map>
#include <utility>

namespace doris {

template <typename TraitsType>
KeyCoder::KeyCoder(TraitsType traits)
        : _full_encode_ascending(traits.full_encode_ascending),
          _encode_ascending(traits.encode_ascending),
          _decode_ascending(traits.decode_ascending) {}

struct EnumClassHash {
    template <typename T>
    std::size_t operator()(T t) const {
        return static_cast<std::size_t>(t);
    }
};

// Helper class used to get KeyCoder
class KeyCoderResolver {
public:
    ~KeyCoderResolver() {
        for (auto& iter : _coder_map) {
            delete iter.second;
        }
    }

    static KeyCoderResolver* instance() {
        static KeyCoderResolver s_instance;
        return &s_instance;
    }

    KeyCoder* get_coder(FieldType field_type) const {
        auto it = _coder_map.find(field_type);
        if (it != _coder_map.end()) {
            return it->second;
        }
        return nullptr;
    }

private:
    KeyCoderResolver() {
        add_mapping<FieldType::OLAP_FIELD_TYPE_TINYINT>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_SMALLINT>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_INT>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_BIGINT>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_LARGEINT>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_DATETIME>();

        add_mapping<FieldType::OLAP_FIELD_TYPE_DATE>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_DECIMAL>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_CHAR>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_STRING>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_BOOL>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_DATEV2>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_DECIMAL32>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_DECIMAL64>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>();
        add_mapping<FieldType::OLAP_FIELD_TYPE_DECIMAL256>();
    }

    template <FieldType field_type>
    void add_mapping() {
        _coder_map.emplace(field_type, new KeyCoder(KeyCoderTraits<field_type>()));
    }

    std::unordered_map<FieldType, KeyCoder*, EnumClassHash> _coder_map;
};

const KeyCoder* get_key_coder(FieldType type) {
    return KeyCoderResolver::instance()->get_coder(type);
}

} // namespace doris
