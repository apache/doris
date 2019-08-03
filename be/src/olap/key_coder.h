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

#include <type_traits>
#include <string>

#include "common/status.h"
#include "gutil/endian.h"
#include "gutil/strings/substitute.h"
#include "olap/types.h"
#include "util/arena.h"

namespace doris {

using strings::Substitute;

using EncodeAscendingFunc = void (*)(const void* value, size_t index_size, std::string* buf);
using DecodeAscendingFunc = Status (*)(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr, Arena* arena);

// Helper class that is used to encode types of value in memory format
// into a sorted binary. For example, this class will encode unsigned
// integer to bit endian format which can compare with memcmp.
class KeyCoder {
public:
    template<typename TraitsType>
    KeyCoder(TraitsType traits);

    void encode_ascending(const void* value, size_t index_size, std::string* buf) const {
        _encode_ascending(value, index_size, buf);
    }
    Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr, Arena* arena) const {
        return _decode_ascending(encoded_key, index_size, cell_ptr, arena);
    }

private:
    EncodeAscendingFunc _encode_ascending;
    DecodeAscendingFunc _decode_ascending;
};

extern const KeyCoder* get_key_coder(FieldType type);

template<FieldType field_type, typename Enable = void>
class KeyCoderTraits {
};

template<FieldType field_type>
class KeyCoderTraits<field_type,
      typename std::enable_if<
        std::is_integral<
            typename CppTypeTraits<field_type>::CppType>::value>::type> {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using UnsignedCppType = typename CppTypeTraits<field_type>::UnsignedCppType;

private:
    // Swap value's endian from/to big endian 
    static UnsignedCppType swap_big_endian(UnsignedCppType val) {
        switch (sizeof(UnsignedCppType)) {
        case 1: return val;
        case 2: return BigEndian::FromHost16(val);
        case 4: return BigEndian::FromHost32(val);
        case 8: return BigEndian::FromHost64(val);
        case 16: return BigEndian::FromHost128(val);
        default: LOG(FATAL) << "Invalid type to big endian, type=" << field_type
                 << ", size=" << sizeof(UnsignedCppType);
        }
    }

public:
    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, value, sizeof(unsigned_val));
        // swap MSB to encode integer
        if (std::is_signed<CppType>::value) {
            unsigned_val ^= (static_cast<UnsignedCppType>(1) << (sizeof(UnsignedCppType) * CHAR_BIT - 1));
        }
        // make it bigendian
        unsigned_val = swap_big_endian(unsigned_val);

        buf->append((char*)&unsigned_val, sizeof(unsigned_val));
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size,
                                   uint8_t* cell_ptr, Arena* arena) {
        if (encoded_key->size < sizeof(UnsignedCppType)) {
            return Status::InvalidArgument(
                Substitute("Key too short, need=$0 vs real=$1",
                           sizeof(UnsignedCppType), encoded_key->size));
        }
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, encoded_key->data, sizeof(UnsignedCppType));
        unsigned_val = swap_big_endian(unsigned_val);
        if (std::is_signed<CppType>::value) {
            unsigned_val ^= (static_cast<UnsignedCppType>(1) << (sizeof(UnsignedCppType) * CHAR_BIT - 1));
        }
        memcpy(cell_ptr, &unsigned_val, sizeof(UnsignedCppType));
        encoded_key->remove_prefix(sizeof(UnsignedCppType));
        return Status::OK();
    }
};

template<>
class KeyCoderTraits<OLAP_FIELD_TYPE_DATE> {
public:
    using CppType = typename CppTypeTraits<OLAP_FIELD_TYPE_DATE>::CppType;
    using UnsignedCppType = typename CppTypeTraits<OLAP_FIELD_TYPE_DATE>::UnsignedCppType;

public:
    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, value, sizeof(unsigned_val));
        // make it bigendian
        unsigned_val = BigEndian::FromHost24(unsigned_val);
        buf->append((char*)&unsigned_val, sizeof(unsigned_val));
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size,
                                   uint8_t* cell_ptr, Arena* arena) {
        if (encoded_key->size < sizeof(UnsignedCppType)) {
            return Status::InvalidArgument(
                Substitute("Key too short, need=$0 vs real=$1",
                           sizeof(UnsignedCppType), encoded_key->size));
        }
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, encoded_key->data, sizeof(UnsignedCppType));
        unsigned_val = BigEndian::FromHost24(unsigned_val);
        memcpy(cell_ptr, &unsigned_val, sizeof(UnsignedCppType));
        encoded_key->remove_prefix(sizeof(UnsignedCppType));
        return Status::OK();
    }
};

template<>
class KeyCoderTraits<OLAP_FIELD_TYPE_DECIMAL> {
public:
    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        decimal12_t decimal_val;
        memcpy(&decimal_val, value, sizeof(decimal12_t));
        // encode integer
        KeyCoderTraits<OLAP_FIELD_TYPE_BIGINT>::encode_ascending(
            &decimal_val.integer, sizeof(decimal_val.integer), buf);
        // encode integer
        KeyCoderTraits<OLAP_FIELD_TYPE_INT>::encode_ascending(
            &decimal_val.fraction, sizeof(decimal_val.fraction), buf);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size,
                                   uint8_t* cell_ptr, Arena* arena) {
        decimal12_t decimal_val;
        RETURN_IF_ERROR(KeyCoderTraits<OLAP_FIELD_TYPE_BIGINT>::decode_ascending(
                encoded_key, sizeof(decimal_val.integer), (uint8_t*)&decimal_val.integer, arena));
        RETURN_IF_ERROR(KeyCoderTraits<OLAP_FIELD_TYPE_INT>::decode_ascending(
                encoded_key, sizeof(decimal_val.fraction), (uint8_t*)&decimal_val.fraction, arena));
        memcpy(cell_ptr, &decimal_val, sizeof(decimal12_t));
        return Status::OK();
    }
};

template<>
class KeyCoderTraits<OLAP_FIELD_TYPE_CHAR> {
public:
    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        const Slice* slice = (const Slice*)value;
        CHECK(index_size <= slice->size) << "index size is larger than char size, index=" << index_size << ", char=" << slice->size;
        buf->append(slice->data, index_size);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size,
                                   uint8_t* cell_ptr, Arena* arena) {
        if (encoded_key->size < index_size) {
            return Status::InvalidArgument(
                Substitute("Key too short, need=$0 vs real=$1",
                           index_size, encoded_key->size));
        }
        Slice* slice = (Slice*)cell_ptr;
        slice->data = arena->Allocate(index_size);
        slice->size = index_size;
        memcpy(slice->data, encoded_key->data, index_size);
        encoded_key->remove_prefix(index_size);
        return Status::OK();
    }
};

template<>
class KeyCoderTraits<OLAP_FIELD_TYPE_VARCHAR> {
public:
    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        const Slice* slice = (const Slice*)value;
        size_t copy_size = std::min(index_size, slice->size);
        buf->append(slice->data, copy_size);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size,
                                   uint8_t* cell_ptr, Arena* arena) {
        CHECK(encoded_key->size <= index_size)
            << "encoded_key size is larger than index_size, key_size=" << encoded_key->size
            << ", index_size=" << index_size;
        auto copy_size = encoded_key->size;
        Slice* slice = (Slice*)cell_ptr;
        slice->data = arena->Allocate(copy_size);
        slice->size = copy_size;
        memcpy(slice->data, encoded_key->data, copy_size);
        encoded_key->remove_prefix(copy_size);
        return Status::OK();
    }
};

}
