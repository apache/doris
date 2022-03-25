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

#include <memory>

namespace doris {

void (*FieldTypeTraits<OLAP_FIELD_TYPE_CHAR>::set_to_max)(void*) = nullptr;

class ScalarTypeInfoResolver {
    DECLARE_SINGLETON(ScalarTypeInfoResolver);

public:
    std::shared_ptr<const TypeInfo> get_type_info(const FieldType t) {
        auto pair = _scalar_type_mapping.find(t);
        DCHECK(pair != _scalar_type_mapping.end()) << "Bad field type: " << t;
        return pair->second;
    }

private:
    template <FieldType field_type>
    void add_mapping() {
        TypeTraits<field_type> traits;
        _scalar_type_mapping.emplace(field_type,
                                     std::shared_ptr<const TypeInfo>(new ScalarTypeInfo(traits)));
    }

    std::unordered_map<FieldType, std::shared_ptr<const TypeInfo>, std::hash<size_t>>
            _scalar_type_mapping;

    DISALLOW_COPY_AND_ASSIGN(ScalarTypeInfoResolver);
};

ScalarTypeInfoResolver::ScalarTypeInfoResolver() {
    add_mapping<OLAP_FIELD_TYPE_TINYINT>();
    add_mapping<OLAP_FIELD_TYPE_SMALLINT>();
    add_mapping<OLAP_FIELD_TYPE_INT>();
    add_mapping<OLAP_FIELD_TYPE_UNSIGNED_INT>();
    add_mapping<OLAP_FIELD_TYPE_BOOL>();
    add_mapping<OLAP_FIELD_TYPE_BIGINT>();
    add_mapping<OLAP_FIELD_TYPE_UNSIGNED_BIGINT>();
    add_mapping<OLAP_FIELD_TYPE_LARGEINT>();
    add_mapping<OLAP_FIELD_TYPE_FLOAT>();
    add_mapping<OLAP_FIELD_TYPE_DOUBLE>();
    add_mapping<OLAP_FIELD_TYPE_DECIMAL>();
    add_mapping<OLAP_FIELD_TYPE_DATE>();
    add_mapping<OLAP_FIELD_TYPE_DATETIME>();
    add_mapping<OLAP_FIELD_TYPE_CHAR>();
    add_mapping<OLAP_FIELD_TYPE_VARCHAR>();
    add_mapping<OLAP_FIELD_TYPE_STRING>();
    add_mapping<OLAP_FIELD_TYPE_HLL>();
    add_mapping<OLAP_FIELD_TYPE_OBJECT>();
    add_mapping<OLAP_FIELD_TYPE_QUANTILE_STATE>();
}

ScalarTypeInfoResolver::~ScalarTypeInfoResolver() {}

bool is_scalar_type(FieldType field_type) {
    switch (field_type) {
    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_ARRAY:
    case OLAP_FIELD_TYPE_MAP:
        return false;
    default:
        return true;
    }
}

bool is_olap_string_type(FieldType field_type) {
    switch (field_type) {
    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_HLL:
    case OLAP_FIELD_TYPE_OBJECT:
    case OLAP_FIELD_TYPE_STRING:
        return true;
    default:
        return false;
    }
}

std::shared_ptr<const TypeInfo> get_scalar_type_info(FieldType field_type) {
    return ScalarTypeInfoResolver::instance()->get_type_info(field_type);
}

class ArrayTypeInfoResolver {
    DECLARE_SINGLETON(ArrayTypeInfoResolver);

public:
    std::shared_ptr<const TypeInfo> get_type_info(const FieldType t) {
        auto pair = _type_mapping.find(t);
        DCHECK(pair != _type_mapping.end()) << "Bad field type: list<" << t << ">";
        return pair->second;
    }

    std::shared_ptr<const TypeInfo> get_type_info(const TabletColumn& column) {
        DCHECK(column.get_subtype_count() == 1) << "more than 1 child type.";
        const auto& sub_column = column.get_sub_column(0);
        if (is_scalar_type(sub_column.type())) {
            return get_type_info(sub_column.type());
        } else {
            return std::make_shared<const ArrayTypeInfo>(get_type_info(sub_column));
        }
    }

    std::shared_ptr<const TypeInfo> get_type_info(const segment_v2::ColumnMetaPB& column_meta_pb) {
        DCHECK(column_meta_pb.children_columns_size() >= 1 &&
               column_meta_pb.children_columns_size() <= 3)
                << "more than 3 children or no children.";
        const auto& child_type = column_meta_pb.children_columns(0);
        if (is_scalar_type((FieldType)child_type.type())) {
            return get_type_info((FieldType)child_type.type());
        } else {
            return std::make_shared<const ArrayTypeInfo>(get_type_info(child_type));
        }
    }

private:
    template <FieldType field_type>
    void add_mapping() {
        _type_mapping.emplace(field_type, std::shared_ptr<const TypeInfo>(new ArrayTypeInfo(
                                                  get_scalar_type_info(field_type))));
    }

    // item_type_info -> list_type_info
    std::unordered_map<FieldType, std::shared_ptr<const TypeInfo>, std::hash<size_t>> _type_mapping;
};

ArrayTypeInfoResolver::~ArrayTypeInfoResolver() = default;

ArrayTypeInfoResolver::ArrayTypeInfoResolver() {
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
    add_mapping<OLAP_FIELD_TYPE_STRING>();
}

// equal to get_scalar_type_info
std::shared_ptr<const TypeInfo> get_type_info(FieldType field_type) {
    return get_scalar_type_info(field_type);
}

// get array array type info
std::shared_ptr<const TypeInfo> get_collection_type_info(FieldType sub_type) {
    return ArrayTypeInfoResolver::instance()->get_type_info(sub_type);
}

std::shared_ptr<const TypeInfo> get_type_info(segment_v2::ColumnMetaPB* column_meta_pb) {
    FieldType type = (FieldType)column_meta_pb->type();
    if (is_scalar_type(type)) {
        return get_scalar_type_info(type);
    } else {
        switch (type) {
        case OLAP_FIELD_TYPE_ARRAY: {
            return ArrayTypeInfoResolver::instance()->get_type_info(*column_meta_pb);
        }
        default:
            DCHECK(false) << "Bad field type: " << type;
            return nullptr;
        }
    }
}

std::shared_ptr<const TypeInfo> get_type_info(const TabletColumn* col) {
    if (is_scalar_type(col->type())) {
        return get_scalar_type_info(col->type());
    } else {
        switch (col->type()) {
        case OLAP_FIELD_TYPE_ARRAY:
            return ArrayTypeInfoResolver::instance()->get_type_info(*col);
        default:
            DCHECK(false) << "Bad field type: " << col->type();
            return nullptr;
        }
    }
}

} // namespace doris
