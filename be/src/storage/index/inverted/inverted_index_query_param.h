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
#include <string>
#include <type_traits>

#include "common/factory_creator.h"
#include "common/status.h"
#include "core/data_type/primitive_type.h"
#include "core/string_ref.h"
#include "core/type_limit.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/key_coder.h"

namespace doris {
class Field;

namespace segment_v2 {

// Typed query value passed from FE to InvertedIndexReader.
// Two disjoint shapes under a common root for unified ownership:
//   StringQueryParam  — get_string()                            (FullText / String readers)
//   NumericQueryParam — encode_ascending() + encode_min/max_*   (BkdIndexReader)
// TypedInvertedIndexQueryParam<PT> picks its base via PT (string vs numeric).

class InvertedIndexQueryParam {
public:
    virtual ~InvertedIndexQueryParam() = default;
};

class StringQueryParam : public InvertedIndexQueryParam {
public:
    virtual void get_string(std::string* out) const = 0;
};

class NumericQueryParam : public InvertedIndexQueryParam {
public:
    // Encode _value through KeyCoder ascending. encode_min/max encode the type's
    // sentinel for the open side of BKD half-bounded ranges.
    virtual void encode_ascending(const KeyCoder* coder, std::string* out) const = 0;
    virtual void encode_min_ascending(const KeyCoder* coder, std::string* out) const = 0;
    virtual void encode_max_ascending(const KeyCoder* coder, std::string* out) const = 0;
};

template <PrimitiveType PT>
class TypedInvertedIndexQueryParam : public NumericQueryParam {
    ENABLE_FACTORY_CREATOR(TypedInvertedIndexQueryParam);

public:
    // Storage type aligned with KeyCoder's view. Override per-PT when
    // PrimitiveTypeTraits disagrees with KeyCoder on signedness — otherwise
    // type_limit<storage_val>::min/max produces broken sentinels.
    //   TYPE_DATETIME: PrimitiveTypeTraits=uint64_t but KeyCoder=int64_t. With
    //   uint64_t, type_limit::max() = UINT64_MAX is read as -1 and encodes
    //   smaller than any real datetime — broken +inf.
    using storage_val = std::conditional_t<PT == TYPE_DATETIME, int64_t,
                                           typename PrimitiveTypeTraits<PT>::StorageFieldType>;

    void set_value(const storage_val* value) { _value = *value; }
    const storage_val& value() const { return _value; }

    void encode_ascending(const KeyCoder* coder, std::string* out) const override {
        coder->full_encode_ascending(&_value, out);
    }
    void encode_min_ascending(const KeyCoder* coder, std::string* out) const override {
        storage_val v = type_limit<storage_val>::min();
        coder->full_encode_ascending(&v, out);
    }
    void encode_max_ascending(const KeyCoder* coder, std::string* out) const override {
        storage_val v = type_limit<storage_val>::max();
        coder->full_encode_ascending(&v, out);
    }

private:
    storage_val _value;
};

template <PrimitiveType PT>
    requires(is_string_type(PT))
class TypedInvertedIndexQueryParam<PT> : public StringQueryParam {
    ENABLE_FACTORY_CREATOR(TypedInvertedIndexQueryParam);

public:
    void set_value(const std::string& value) { _value = value; }
    void set_value(const StringRef* value) { _value.assign(value->data, value->size); }

    const std::string& value() const { return _value; }

    void get_string(std::string* out) const override { *out = _value; }

private:
    std::string _value;
};

// Static-only: maps FE values (Field / scalars / StringRef) to the right
// TypedInvertedIndexQueryParam<PT>.
class InvertedIndexQueryParamFactory {
public:
    InvertedIndexQueryParamFactory() = delete;

    template <PrimitiveType PT, typename ValueType>
    static Status create_query_value(const ValueType* value,
                                     std::unique_ptr<InvertedIndexQueryParam>& result_param) {
        static_assert(!std::is_same_v<ValueType, void>,
                      "ValueType cannot be void, as it is unsupported and dangerous.");

        using CPP_TYPE = typename PrimitiveTypeTraits<PT>::CppType;
        std::unique_ptr<TypedInvertedIndexQueryParam<PT>> param =
                TypedInvertedIndexQueryParam<PT>::create_unique();

        if constexpr (is_string_type(PT)) {
            if constexpr (std::is_same_v<ValueType, doris::Field>) {
                const auto& str = value->template get<PT>();
                param->set_value(str);
            } else if constexpr (std::is_same_v<ValueType, StringRef>) {
                param->set_value(value);
            } else {
                static_assert(std::is_convertible_v<ValueType, std::string>,
                              "ValueType must be convertible to std::string for string types");
                param->set_value(std::string(*value));
            }
        } else {
            CPP_TYPE cpp_val;
            if constexpr (std::is_same_v<ValueType, doris::Field>) {
                auto field_val = value->template get<PT>();
                cpp_val = static_cast<CPP_TYPE>(field_val);
            } else {
                cpp_val = static_cast<CPP_TYPE>(*value);
            }

            typename TypedInvertedIndexQueryParam<PT>::storage_val storage_val_v =
                    PrimitiveTypeConvertor<PT>::to_storage_field_type(cpp_val);
            param->set_value(&storage_val_v);
        }
        result_param = std::move(param);
        return Status::OK();
    }

    static Status create_query_value(const PrimitiveType& primitiveType, const doris::Field* value,
                                     std::unique_ptr<InvertedIndexQueryParam>& result_param) {
        switch (primitiveType) {
#define M(TYPE)                                                             \
    case TYPE: {                                                            \
        return create_query_value<TYPE, doris::Field>(value, result_param); \
    }
            M(PrimitiveType::TYPE_BOOLEAN)
            M(PrimitiveType::TYPE_TINYINT)
            M(PrimitiveType::TYPE_SMALLINT)
            M(PrimitiveType::TYPE_INT)
            M(PrimitiveType::TYPE_BIGINT)
            M(PrimitiveType::TYPE_LARGEINT)
            M(PrimitiveType::TYPE_FLOAT)
            M(PrimitiveType::TYPE_DOUBLE)
            M(PrimitiveType::TYPE_DECIMALV2)
            M(PrimitiveType::TYPE_DECIMAL32)
            M(PrimitiveType::TYPE_DECIMAL64)
            M(PrimitiveType::TYPE_DECIMAL128I)
            M(PrimitiveType::TYPE_DECIMAL256)
            M(PrimitiveType::TYPE_DATE)
            M(PrimitiveType::TYPE_DATETIME)
            M(PrimitiveType::TYPE_CHAR)
            M(PrimitiveType::TYPE_VARCHAR)
            M(PrimitiveType::TYPE_STRING)
            M(PrimitiveType::TYPE_DATEV2)
            M(PrimitiveType::TYPE_DATETIMEV2)
            M(PrimitiveType::TYPE_IPV4)
            M(PrimitiveType::TYPE_IPV6)
#undef M
        default:
            return Status::NotSupported("Unsupported primitive type {} for inverted index reader",
                                        primitiveType);
        }
    }
};

} // namespace segment_v2
} // namespace doris
