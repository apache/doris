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

#include <glog/logging.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <ostream>
#include <string>
#include <type_traits>

#include "absl/strings/substitute.h"
#include "common/status.h"
#include "core/data_type/primitive_type.h"
#include "core/decimal12.h"
#include "core/extended_types.h"
#include "core/field.h"
#include "core/types.h"
#include "exec/common/endian.h"
#include "storage/olap_common.h"
#include "storage/types.h"
#include "util/slice.h"

namespace doris {

using FullEncodeAscendingFunc = void (*)(const void* value, std::string* buf);
using EncodeAscendingFunc = void (*)(const void* value, size_t index_size, std::string* buf);
using DecodeAscendingFunc = Status (*)(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr);

// Order-preserving binary encoding for values of a particular type so that
// those values can be compared by memcpy their encoded bytes.
//
// To obtain instance of this class, use the `get_key_coder(FieldType)` method.
class KeyCoder {
public:
    template <typename TraitsType>
    KeyCoder(TraitsType traits);

    // encode the provided `value` into `buf`.
    void full_encode_ascending(const void* value, std::string* buf) const {
        _full_encode_ascending(value, buf);
    }

    // similar to `full_encode_ascending`, but only encode part (the first `index_size` bytes) of the value.
    // only applicable to string type
    void encode_ascending(const void* value, size_t index_size, std::string* buf) const {
        _encode_ascending(value, index_size, buf);
    }

    // Only used for test, should delete it in the future
    Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) const {
        return _decode_ascending(encoded_key, index_size, cell_ptr);
    }

private:
    FullEncodeAscendingFunc _full_encode_ascending;
    EncodeAscendingFunc _encode_ascending;
    DecodeAscendingFunc _decode_ascending;
};

extern const KeyCoder* get_key_coder(FieldType type);

template <FieldType field_type, typename Enable = void>
class KeyCoderTraits {};

template <FieldType field_type>
class KeyCoderTraits<field_type,
                     typename std::enable_if<
                             IsIntegral<typename CppTypeTraits<field_type>::CppType>::value ||
                             IsDecimalNumber<typename CppTypeTraits<field_type>::CppType>>::type> {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using UnsignedCppType = typename CppTypeTraits<field_type>::UnsignedCppType;

    static void full_encode_ascending(const void* value, std::string* buf) {
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, value, sizeof(unsigned_val));
        // swap MSB to encode integer
        if (IsSigned<CppType>::value) {
            unsigned_val ^=
                    (static_cast<UnsignedCppType>(1) << (sizeof(UnsignedCppType) * CHAR_BIT - 1));
        }
        // make it bigendian
        unsigned_val = to_endian<std::endian::big>(unsigned_val);

        buf->append((char*)&unsigned_val, sizeof(unsigned_val));
    }

    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        full_encode_ascending(value, buf);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        // decode_ascending only used in orinal index page, maybe should remove it in the future.
        // currently, we reduce the usage of this method.
        if (encoded_key->size < sizeof(UnsignedCppType)) {
            return Status::InvalidArgument("Key too short, need={} vs real={}",
                                           sizeof(UnsignedCppType), encoded_key->size);
        }
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, encoded_key->data, sizeof(UnsignedCppType));
        unsigned_val = to_endian<std::endian::big>(unsigned_val);
        if (IsSigned<CppType>::value) {
            unsigned_val ^=
                    (static_cast<UnsignedCppType>(1) << (sizeof(UnsignedCppType) * CHAR_BIT - 1));
        }
        memcpy(cell_ptr, &unsigned_val, sizeof(UnsignedCppType));
        encoded_key->remove_prefix(sizeof(UnsignedCppType));
        return Status::OK();
    }
};

template <>
class KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_DATE> {
public:
    using CppType = typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE>::CppType;
    using UnsignedCppType =
            typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE>::UnsignedCppType;

public:
    static void full_encode_ascending(const void* value, std::string* buf) {
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, value, sizeof(unsigned_val));
        // make it bigendian
        unsigned_val = to_endian<std::endian::big>(unsigned_val);
        buf->append((char*)&unsigned_val, sizeof(unsigned_val));
    }

    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        full_encode_ascending(value, buf);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        if (encoded_key->size < sizeof(UnsignedCppType)) {
            return Status::InvalidArgument("Key too short, need={} vs real={}",
                                           sizeof(UnsignedCppType), encoded_key->size);
        }
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, encoded_key->data, sizeof(UnsignedCppType));
        unsigned_val = to_endian<std::endian::big>(unsigned_val);
        memcpy(cell_ptr, &unsigned_val, sizeof(UnsignedCppType));
        encoded_key->remove_prefix(sizeof(UnsignedCppType));
        return Status::OK();
    }
};

template <>
class KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_DATEV2> {
public:
    using CppType = typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATEV2>::CppType;
    using UnsignedCppType =
            typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATEV2>::UnsignedCppType;

public:
    static void full_encode_ascending(const void* value, std::string* buf) {
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, value, sizeof(unsigned_val));
        // make it bigendian
        unsigned_val = to_endian<std::endian::big>(unsigned_val);
        buf->append((char*)&unsigned_val, sizeof(unsigned_val));
    }

    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        full_encode_ascending(value, buf);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        if (encoded_key->size < sizeof(UnsignedCppType)) {
            return Status::InvalidArgument(absl::Substitute("Key too short, need=$0 vs real=$1",
                                                            sizeof(UnsignedCppType),
                                                            encoded_key->size));
        }
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, encoded_key->data, sizeof(UnsignedCppType));
        unsigned_val = to_endian<std::endian::big>(unsigned_val);
        memcpy(cell_ptr, &unsigned_val, sizeof(UnsignedCppType));
        encoded_key->remove_prefix(sizeof(UnsignedCppType));
        return Status::OK();
    }
};

template <>
class KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_DATETIMEV2> {
public:
    using CppType = typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>::CppType;
    using UnsignedCppType =
            typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>::UnsignedCppType;

public:
    static void full_encode_ascending(const void* value, std::string* buf) {
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, value, sizeof(unsigned_val));
        // make it bigendian
        unsigned_val = to_endian<std::endian::big>(unsigned_val);
        buf->append((char*)&unsigned_val, sizeof(unsigned_val));
    }

    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        full_encode_ascending(value, buf);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        if (encoded_key->size < sizeof(UnsignedCppType)) {
            return Status::InvalidArgument(absl::Substitute("Key too short, need=$0 vs real=$1",
                                                            sizeof(UnsignedCppType),
                                                            encoded_key->size));
        }
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, encoded_key->data, sizeof(UnsignedCppType));
        unsigned_val = to_endian<std::endian::big>(unsigned_val);
        memcpy(cell_ptr, &unsigned_val, sizeof(UnsignedCppType));
        encoded_key->remove_prefix(sizeof(UnsignedCppType));
        return Status::OK();
    }
};

template <>
class KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ> {
public:
    using CppType = typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ>::CppType;
    using UnsignedCppType =
            typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ>::UnsignedCppType;

public:
    static void full_encode_ascending(const void* value, std::string* buf) {
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, value, sizeof(unsigned_val));
        // make it bigendian
        unsigned_val = to_endian<std::endian::big>(unsigned_val);
        buf->append((char*)&unsigned_val, sizeof(unsigned_val));
    }

    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        full_encode_ascending(value, buf);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        if (encoded_key->size < sizeof(UnsignedCppType)) {
            return Status::InvalidArgument(absl::Substitute("Key too short, need=$0 vs real=$1",
                                                            sizeof(UnsignedCppType),
                                                            encoded_key->size));
        }
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, encoded_key->data, sizeof(UnsignedCppType));
        unsigned_val = to_endian<std::endian::big>(unsigned_val);
        memcpy(cell_ptr, &unsigned_val, sizeof(UnsignedCppType));
        encoded_key->remove_prefix(sizeof(UnsignedCppType));
        return Status::OK();
    }
};

template <>
class KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL> {
public:
    static void full_encode_ascending(const void* value, std::string* buf) {
        decimal12_t decimal_val;
        memcpy(&decimal_val, value, sizeof(decimal12_t));
        KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_BIGINT>::full_encode_ascending(
                &decimal_val.integer, buf);
        KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_INT>::full_encode_ascending(&decimal_val.fraction,
                                                                              buf);
    } // namespace doris

    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        full_encode_ascending(value, buf);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        decimal12_t decimal_val = {0, 0};
        RETURN_IF_ERROR(KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_BIGINT>::decode_ascending(
                encoded_key, sizeof(decimal_val.integer), (uint8_t*)&decimal_val.integer));
        RETURN_IF_ERROR(KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_INT>::decode_ascending(
                encoded_key, sizeof(decimal_val.fraction), (uint8_t*)&decimal_val.fraction));
        memcpy(cell_ptr, &decimal_val, sizeof(decimal12_t));
        return Status::OK();
    }
};

template <>
class KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_CHAR> {
public:
    NO_SANITIZE_UNDEFINED static void full_encode_ascending(const void* value, std::string* buf) {
        auto slice = reinterpret_cast<const Slice*>(value);
        buf->append(slice->get_data(), slice->get_size());
    }

    NO_SANITIZE_UNDEFINED static void encode_ascending(const void* value, size_t index_size,
                                                       std::string* buf) {
        const Slice* slice = (const Slice*)value;
        CHECK(index_size <= slice->size)
                << "index size is larger than char size, index=" << index_size
                << ", char=" << slice->size;
        buf->append(slice->data, index_size);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        throw Exception(Status::FatalError("decode_ascending is not implemented"));
    }
};

template <>
class KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR> {
public:
    NO_SANITIZE_UNDEFINED static void full_encode_ascending(const void* value, std::string* buf) {
        auto slice = reinterpret_cast<const Slice*>(value);
        buf->append(slice->get_data(), slice->get_size());
    }

    NO_SANITIZE_UNDEFINED static void encode_ascending(const void* value, size_t index_size,
                                                       std::string* buf) {
        const Slice* slice = (const Slice*)value;
        size_t copy_size = std::min(index_size, slice->size);
        buf->append(slice->data, copy_size);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        throw Exception(Status::FatalError("decode_ascending is not implemented"));
    }
};

template <>
class KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_STRING> {
public:
    NO_SANITIZE_UNDEFINED static void full_encode_ascending(const void* value, std::string* buf) {
        auto slice = reinterpret_cast<const Slice*>(value);
        buf->append(slice->get_data(), slice->get_size());
    }

    NO_SANITIZE_UNDEFINED static void encode_ascending(const void* value, size_t index_size,
                                                       std::string* buf) {
        const Slice* slice = (const Slice*)value;
        size_t copy_size = std::min(index_size, slice->size);
        buf->append(slice->data, copy_size);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        throw Exception(Status::FatalError("decode_ascending is not implemented"));
    }
};

template <FieldType field_type>
class KeyCoderTraitsForFloat {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using UnsignedCppType = typename CppTypeTraits<field_type>::UnsignedCppType;

    static UnsignedCppType encode_float(CppType value) {
        return sortable_float_bits(float_to_int_bits(value));
    }

    static CppType decode_float(UnsignedCppType sortable_bits) {
        return int_bits_to_float(unsortable_float_bits(sortable_bits));
    }

    // -infinity < -100.0 < -1.0 < -0.0 < 0.0 < 1.0 < 100.0 < infinity < NaN
    static void full_encode_ascending(const void* value, std::string* buf) {
        CppType val;
        std::memcpy(&val, value, sizeof(CppType));
        UnsignedCppType sortable_val = encode_float(val);
        constexpr UnsignedCppType sign_bit = UnsignedCppType(1)
                                             << (sizeof(UnsignedCppType) * 8 - 1);
        sortable_val ^= sign_bit;
        sortable_val = to_endian<std::endian::big>(sortable_val);
        buf->append(reinterpret_cast<const char*>(&sortable_val), sizeof(UnsignedCppType));
    }

    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        full_encode_ascending(value, buf);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        if (encoded_key->size < sizeof(UnsignedCppType)) {
            return Status::InvalidArgument(absl::Substitute("Key too short, need=$0 vs real=$1",
                                                            sizeof(UnsignedCppType),
                                                            encoded_key->size));
        }
        UnsignedCppType sortable_val;
        std::memcpy(&sortable_val, encoded_key->data, sizeof(UnsignedCppType));
        sortable_val = to_endian<std::endian::big>(sortable_val);
        constexpr UnsignedCppType sign_bit = UnsignedCppType(1)
                                             << (sizeof(UnsignedCppType) * 8 - 1);
        sortable_val ^= sign_bit;
        CppType val = decode_float(sortable_val);
        std::memcpy(cell_ptr, &val, sizeof(CppType));
        encoded_key->remove_prefix(sizeof(UnsignedCppType));
        return Status::OK();
    }

private:
    static UnsignedCppType float_to_int_bits(CppType value) {
        if (std::isnan(value)) {
            if constexpr (std::is_same_v<CppType, float>) {
                return 0x7FC00000U;
            } else {
                return 0x7FF8000000000000ULL;
            }
        }

        UnsignedCppType result;
        std::memcpy(&result, &value, sizeof(CppType));
        return result;
    }

    static UnsignedCppType sortable_float_bits(UnsignedCppType bits) {
        constexpr int32_t shift = sizeof(UnsignedCppType) * 8 - 1;
        constexpr UnsignedCppType sign_bit = static_cast<UnsignedCppType>(1) << shift;
        if ((bits & sign_bit) != 0) {
            return bits ^ (sign_bit - 1);
        } else {
            return bits;
        }
    }

    static CppType int_bits_to_float(UnsignedCppType bits) {
        CppType result;
        std::memcpy(&result, &bits, sizeof(CppType));
        return result;
    }

    static UnsignedCppType unsortable_float_bits(UnsignedCppType sortable_bits) {
        return sortable_float_bits(sortable_bits);
    }
};

template <>
class KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_FLOAT>
        : public KeyCoderTraitsForFloat<FieldType::OLAP_FIELD_TYPE_FLOAT> {};

template <>
class KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_DOUBLE>
        : public KeyCoderTraitsForFloat<FieldType::OLAP_FIELD_TYPE_DOUBLE> {};

// Convert a Field value to its storage representation (via PrimitiveTypeConvertor)
// and full-encode it as a byte-comparable ascending key via KeyCoder.
template <PrimitiveType PT>
inline void full_encode_field_as_key(const Field& f, const KeyCoder* coder, std::string* buf) {
    auto v = PrimitiveTypeConvertor<PT>::to_storage_field_type(f.get<PT>());
    coder->full_encode_ascending(&v, buf);
}

// Same as full_encode_field_as_key but truncates string keys to `index_size` bytes.
// For fixed-width types KeyCoder ignores `index_size`, so the output is identical
// to full_encode_field_as_key.
template <PrimitiveType PT>
inline void encode_field_as_key(const Field& f, const KeyCoder* coder, size_t index_size,
                                std::string* buf) {
    auto v = PrimitiveTypeConvertor<PT>::to_storage_field_type(f.get<PT>());
    coder->encode_ascending(&v, index_size, buf);
}

// X-macro listing every (FieldType, PrimitiveType) pair that goes through KeyCoder
// as a non-string scalar key. Strings are handled separately because they need
// length / padding logic outside KeyCoder. Each entry: M(FT_suffix, PT_suffix).
#define DORIS_APPLY_FOR_KEY_ENCODABLE_NON_STRING_TYPES(M) \
    M(OLAP_FIELD_TYPE_BOOL, TYPE_BOOLEAN)                 \
    M(OLAP_FIELD_TYPE_TINYINT, TYPE_TINYINT)              \
    M(OLAP_FIELD_TYPE_SMALLINT, TYPE_SMALLINT)            \
    M(OLAP_FIELD_TYPE_INT, TYPE_INT)                      \
    M(OLAP_FIELD_TYPE_BIGINT, TYPE_BIGINT)                \
    M(OLAP_FIELD_TYPE_LARGEINT, TYPE_LARGEINT)            \
    M(OLAP_FIELD_TYPE_FLOAT, TYPE_FLOAT)                  \
    M(OLAP_FIELD_TYPE_DOUBLE, TYPE_DOUBLE)                \
    M(OLAP_FIELD_TYPE_DECIMAL, TYPE_DECIMALV2)            \
    M(OLAP_FIELD_TYPE_DECIMAL32, TYPE_DECIMAL32)          \
    M(OLAP_FIELD_TYPE_DECIMAL64, TYPE_DECIMAL64)          \
    M(OLAP_FIELD_TYPE_DECIMAL128I, TYPE_DECIMAL128I)      \
    M(OLAP_FIELD_TYPE_DECIMAL256, TYPE_DECIMAL256)        \
    M(OLAP_FIELD_TYPE_DATE, TYPE_DATE)                    \
    M(OLAP_FIELD_TYPE_DATETIME, TYPE_DATETIME)            \
    M(OLAP_FIELD_TYPE_DATEV2, TYPE_DATEV2)                \
    M(OLAP_FIELD_TYPE_DATETIMEV2, TYPE_DATETIMEV2)        \
    M(OLAP_FIELD_TYPE_TIMESTAMPTZ, TYPE_TIMESTAMPTZ)      \
    M(OLAP_FIELD_TYPE_IPV4, TYPE_IPV4)                    \
    M(OLAP_FIELD_TYPE_IPV6, TYPE_IPV6)

} // namespace doris
