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
#include "gutil/endian.h"
#include "olap/decimal12.h"
#include "olap/olap_common.h"
#include "olap/types.h"
#include "util/slice.h"
#include "vec/core/extended_types.h"
#include "vec/core/types.h"

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
class KeyCoderTraits<
        field_type,
        typename std::enable_if<
                IsIntegral<typename CppTypeTraits<field_type>::CppType>::value ||
                vectorized::IsDecimalNumber<typename CppTypeTraits<field_type>::CppType>>::type> {
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
    static void full_encode_ascending(const void* value, std::string* buf) {
        auto slice = reinterpret_cast<const Slice*>(value);
        buf->append(slice->get_data(), slice->get_size());
    }

    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
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
    static void full_encode_ascending(const void* value, std::string* buf) {
        auto slice = reinterpret_cast<const Slice*>(value);
        buf->append(slice->get_data(), slice->get_size());
    }

    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
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
    static void full_encode_ascending(const void* value, std::string* buf) {
        auto slice = reinterpret_cast<const Slice*>(value);
        buf->append(slice->get_data(), slice->get_size());
    }

    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
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

    static UnsignedCppType encode_float(UnsignedCppType val) {
        constexpr UnsignedCppType sign_bit = (UnsignedCppType)1
                                             << (sizeof(UnsignedCppType) * 8 - 1);
        if (val & sign_bit) {
            return ~val;
        } else {
            return val ^ sign_bit;
        }
    }

    static UnsignedCppType decode_float(UnsignedCppType val) {
        constexpr UnsignedCppType sign_bit = (UnsignedCppType)1
                                             << (sizeof(UnsignedCppType) * 8 - 1);
        if (val & sign_bit) {
            return val ^ sign_bit;
        } else {
            return ~val;
        }
    }

    static void full_encode_ascending(const void* value, std::string* buf) {
        CppType val;
        memcpy(&val, value, sizeof(CppType));
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, &val, sizeof(UnsignedCppType));
        unsigned_val = encode_float(unsigned_val);
        unsigned_val = to_endian<std::endian::big>(unsigned_val);
        buf->append((char*)&unsigned_val, sizeof(UnsignedCppType));
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
        unsigned_val = decode_float(unsigned_val);
        CppType val;
        memcpy(&val, &unsigned_val, sizeof(CppType));
        memcpy(cell_ptr, &val, sizeof(CppType));
        encoded_key->remove_prefix(sizeof(UnsignedCppType));
        return Status::OK();
    }
};

template <>
class KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_FLOAT>
        : public KeyCoderTraitsForFloat<FieldType::OLAP_FIELD_TYPE_FLOAT> {};

template <>
class KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_DOUBLE>
        : public KeyCoderTraitsForFloat<FieldType::OLAP_FIELD_TYPE_DOUBLE> {};

} // namespace doris
