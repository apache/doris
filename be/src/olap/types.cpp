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

#include "olap/types.h"

namespace doris {

template<typename TypeTraitsClass>
TypeInfo::TypeInfo(TypeTraitsClass t)
      : _equal(TypeTraitsClass::equal),
        _cmp(TypeTraitsClass::cmp),
        _copy_with_pool(TypeTraitsClass::copy_with_pool),
        _copy_without_pool(TypeTraitsClass::copy_without_pool),
        _from_string(TypeTraitsClass::from_string),
        _to_string(TypeTraitsClass::to_string),
        _set_to_max(TypeTraitsClass::set_to_max),
        _set_to_min(TypeTraitsClass::set_to_min),
        _is_min(TypeTraitsClass::is_min),
        _hash_code(TypeTraitsClass::hash_code),
        _size(TypeTraitsClass::size),
        _field_type(TypeTraitsClass::type) {
}

class TypeInfoResolver {
    DECLARE_SINGLETON(TypeInfoResolver);
public:
    TypeInfo* get_type_info(const FieldType t) {
        auto pair = _mapping.find(t);
        DCHECK(pair != _mapping.end()) << "Bad field type: " << t;
        return pair->second.get();
    }

private:
    template<FieldType field_type> void add_mapping() {
        TypeTraits<field_type> traits;
        _mapping.emplace(field_type,
                 std::shared_ptr<TypeInfo>(new TypeInfo(traits)));
    }

    std::unordered_map<FieldType,
        std::shared_ptr<TypeInfo>,
        std::hash<size_t>> _mapping;

    DISALLOW_COPY_AND_ASSIGN(TypeInfoResolver);
};

TypeInfoResolver::TypeInfoResolver() {
    add_mapping<OLAP_FIELD_TYPE_TINYINT>();
    add_mapping<OLAP_FIELD_TYPE_SMALLINT>();
    add_mapping<OLAP_FIELD_TYPE_INT>();
    add_mapping<OLAP_FIELD_TYPE_UNSIGNED_INT>();
    add_mapping<OLAP_FIELD_TYPE_BOOL>();
    add_mapping<OLAP_FIELD_TYPE_BIGINT>();
    add_mapping<OLAP_FIELD_TYPE_LARGEINT>();
    add_mapping<OLAP_FIELD_TYPE_FLOAT>();
    add_mapping<OLAP_FIELD_TYPE_DOUBLE>();
    add_mapping<OLAP_FIELD_TYPE_DECIMAL>();
    add_mapping<OLAP_FIELD_TYPE_DATE>();
    add_mapping<OLAP_FIELD_TYPE_DATETIME>();
    add_mapping<OLAP_FIELD_TYPE_CHAR>();
    add_mapping<OLAP_FIELD_TYPE_VARCHAR>();
    add_mapping<OLAP_FIELD_TYPE_HLL>();
}

TypeInfoResolver::~TypeInfoResolver() {}

TypeInfo* get_type_info(FieldType field_type) {
    return TypeInfoResolver::instance()->get_type_info(field_type);
}

} // namespace doris
