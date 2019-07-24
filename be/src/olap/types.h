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

#ifndef DORIS_BE_SRC_OLAP_TYPES_H
#define DORIS_BE_SRC_OLAP_TYPES_H

#include <math.h>
#include <stdio.h>

#include <limits>
#include <sstream>
#include <string>

#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/uint24.h"
#include "olap/decimal12.h"
#include "runtime/mem_pool.h"
#include "util/hash_util.hpp"
#include "util/mem_util.hpp"
#include "util/slice.h"
#include "util/types.h"
#include "util/arena.h"

namespace doris {

class TypeInfo {
public:
    inline bool equal(const void* left, const void* right) const {
        return _equal(left, right);
    }

    inline int cmp(const void* left, const void* right) const {
        return _cmp(left, right);
    }

    inline void copy_with_pool(void* dest, const void* src, MemPool* mem_pool) const {
        _copy_with_pool(dest, src, mem_pool);
    }

    inline void copy_with_arena(void* dest, const void* src, Arena* arena) const {
        _copy_with_arena(dest, src, arena);
    }

    inline void copy_without_pool(void* dest, const void* src) const {
        _copy_without_pool(dest, src);
    }

    OLAPStatus from_string(void* buf, const std::string& scan_key) const {
        return _from_string(buf, scan_key);
    }

    std::string to_string(const void* src) const { return _to_string(src); }

    inline void set_to_max(void* buf) { _set_to_max(buf); }
    inline void set_to_min(void* buf) { _set_to_min(buf); }

    inline uint32_t hash_code(const void* data, uint32_t seed) const { return _hash_code(data, seed); }
    inline const size_t size() const { return _size; }

    inline FieldType type() const { return _field_type; }
private:
    bool (*_equal)(const void* left, const void* right);
    int (*_cmp)(const void* left, const void* right);

    void (*_copy_with_pool)(void* dest, const void* src, MemPool* mem_pool);
    void (*_copy_with_arena)(void* dest, const void* src, Arena* arena);
    void (*_copy_without_pool)(void* dest, const void* src);

    OLAPStatus (*_from_string)(void* buf, const std::string& scan_key);
    std::string (*_to_string)(const void* src);

    void (*_set_to_max)(void* buf);
    void (*_set_to_min)(void* buf);

    uint32_t (*_hash_code)(const void* data, uint32_t seed);

    const size_t _size;
    const FieldType _field_type;

    friend class TypeInfoResolver;
    template<typename TypeTraitsClass> TypeInfo(TypeTraitsClass t);
};

extern TypeInfo* get_type_info(FieldType field_type);

template<FieldType field_type>
struct CppTypeTraits {
};

template<> struct CppTypeTraits<OLAP_FIELD_TYPE_BOOL> {
    using CppType = bool;
};
template<> struct CppTypeTraits<OLAP_FIELD_TYPE_TINYINT> {
    using CppType = int8_t;
};
template<> struct CppTypeTraits<OLAP_FIELD_TYPE_SMALLINT> {
    using CppType = int16_t;
};
template<> struct CppTypeTraits<OLAP_FIELD_TYPE_INT> {
    using CppType = int32_t;
};
template<> struct CppTypeTraits<OLAP_FIELD_TYPE_UNSIGNED_INT> {
    using CppType = uint32_t;
};
template<> struct CppTypeTraits<OLAP_FIELD_TYPE_BIGINT> {
    using CppType = int64_t;
};
template<> struct CppTypeTraits<OLAP_FIELD_TYPE_LARGEINT> {
    using CppType = int128_t;
};
template<> struct CppTypeTraits<OLAP_FIELD_TYPE_FLOAT> {
    using CppType = float;
};
template<> struct CppTypeTraits<OLAP_FIELD_TYPE_DOUBLE> {
    using CppType = double;
};
template<> struct CppTypeTraits<OLAP_FIELD_TYPE_DECIMAL> {
    using CppType = decimal12_t;
};
template<> struct CppTypeTraits<OLAP_FIELD_TYPE_DATE> {
    using CppType = uint24_t;
};
template<> struct CppTypeTraits<OLAP_FIELD_TYPE_DATETIME> {
    using CppType = int64_t;
};
template<> struct CppTypeTraits<OLAP_FIELD_TYPE_CHAR> {
    using CppType = Slice;
};
template<> struct CppTypeTraits<OLAP_FIELD_TYPE_VARCHAR> {
    using CppType = Slice;
};
template<> struct CppTypeTraits<OLAP_FIELD_TYPE_HLL> {
    using CppType = Slice;
};

template<FieldType field_type>
struct BaseFieldtypeTraits : public CppTypeTraits<field_type> {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    static inline bool equal(const void* left, const void* right) {
        CppType l_value = *reinterpret_cast<const CppType*>(left);
        CppType r_value = *reinterpret_cast<const CppType*>(right);
        return l_value == r_value;
    }

    static inline int cmp(const void* left, const void* right) {
        CppType left_int = *reinterpret_cast<const CppType*>(left);
        CppType right_int = *reinterpret_cast<const CppType*>(right);
        if (left_int < right_int) {
            return -1;
        } else if (left_int > right_int) {
            return 1;
        } else {
            return 0;
        }
    }

    static inline void copy_with_pool(void* dest, const void* src, MemPool* mem_pool) {
        *reinterpret_cast<CppType*>(dest) = *reinterpret_cast<const CppType*>(src);
    }

    static inline void copy_with_arena(void* dest, const void* src, Arena* arena) {
        *reinterpret_cast<CppType*>(dest) = *reinterpret_cast<const CppType*>(src);
    }

    static inline void copy_without_pool(void* dest, const void* src) {
        *reinterpret_cast<CppType*>(dest) = *reinterpret_cast<const CppType*>(src);
    }

    static inline void set_to_max(void* buf) {
        *reinterpret_cast<CppType*>(buf) = std::numeric_limits<CppType>::max();
    }

    static inline void set_to_min(void* buf) {
        *reinterpret_cast<CppType*>(buf) = std::numeric_limits<CppType>::min();
    }

    static inline uint32_t hash_code(const void* data, uint32_t seed) {
        return HashUtil::hash(data, sizeof(CppType), seed);
    }

    static std::string to_string(const void* src) {
        std::stringstream stream;
        stream << *reinterpret_cast<const CppType*>(src);
        return stream.str();
    }

    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        CppType value = 0;
        if (scan_key.length() > 0) {
            value = static_cast<CppType>(strtol(scan_key.c_str(), NULL, 10));
        }
        *reinterpret_cast<CppType*>(buf) = value;
        return OLAP_SUCCESS;
    }
};

template<FieldType field_type>
struct FieldTypeTraits : public BaseFieldtypeTraits<field_type> { };

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_BOOL> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_BOOL> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", *reinterpret_cast<const bool*>(src));
        return std::string(buf);
    }
    static void set_to_max(void* buf) {
        (*(bool*)buf) = true;
    }
    static void set_to_min(void* buf) {
        (*(bool*)buf) = false;
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_TINYINT> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_TINYINT> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", *reinterpret_cast<const int8_t*>(src));
        return std::string(buf);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_LARGEINT> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        int128_t value = 0;

        const char* value_string = scan_key.c_str();
        char* end = NULL;
        value = strtol(value_string, &end, 10);
        if (*end != 0) {
            value = 0;
        } else if (value > LONG_MIN && value < LONG_MAX) {
            // use strtol result directly
        } else {
            bool is_negative = false;
            if (*value_string == '-' || *value_string == '+') {
                if (*(value_string++) == '-') {
                    is_negative = true;
                }
            }

            uint128_t current = 0;
            uint128_t max_int128 = ~((int128_t)(1) << 127);
            while (*value_string != 0) {
                if (current > max_int128 / 10) {
                    break;
                }

                current = current * 10 + (*(value_string++) - '0');
            }
            if (*value_string != 0
                || (!is_negative && current > max_int128)
                || ( is_negative&& current > max_int128 + 1)) {
                current = 0;
            }

            value = is_negative ? -current : current;
        }

        *reinterpret_cast<PackedInt128*>(buf) = value;

        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        char buf[1024];
        int128_t value = reinterpret_cast<const PackedInt128*>(src)->value;
        if (value >= std::numeric_limits<int64_t>::min()
            && value <= std::numeric_limits<int64_t>::max()) {
            snprintf(buf, sizeof(buf), "%ld", (int64_t)value);
        } else {
            char* current = buf;
            uint128_t abs_value = value;
            if (value < 0) {
                *(current++) = '-';
                abs_value = -value;
            }

            // the max value of uint64_t is 18446744073709551615UL,
            // so use Z19_UINT64 to divide uint128_t 
            const static uint64_t Z19_UINT64 = 10000000000000000000ULL;
            uint64_t suffix = abs_value % Z19_UINT64;
            uint64_t middle = abs_value / Z19_UINT64 % Z19_UINT64;
            uint64_t prefix = abs_value / Z19_UINT64 / Z19_UINT64;

            char* end = buf + sizeof(buf);
            if (prefix > 0) {
                current += snprintf(current, end - current, "%" PRIu64, prefix);
                current += snprintf(current, end - current, "%.19" PRIu64, middle);
                current += snprintf(current, end - current, "%.19" PRIu64, suffix);
            } else if (OLAP_LIKELY(middle > 0)) {
                current += snprintf(current, end - current, "%" PRIu64, middle);
                current += snprintf(current, end - current, "%.19" PRIu64, suffix);
            } else {
                current += snprintf(current, end - current, "%" PRIu64, suffix);
            }
        }

        return std::string(buf);
    }
    // GCC7.3 will generate movaps instruction, which will lead to SEGV when buf is
    // not aligned to 16 byte
    static void copy_with_pool(void* dest, const void* src, MemPool* mem_pool) {
        *reinterpret_cast<PackedInt128*>(dest) = *reinterpret_cast<const PackedInt128*>(src);
    }
    static void copy_with_arena(void* dest, const void* src, Arena* arena) {
        *reinterpret_cast<PackedInt128*>(dest) = *reinterpret_cast<const PackedInt128*>(src);
    }
    static void copy_without_pool(void* dest, const void* src) {
        *reinterpret_cast<PackedInt128*>(dest) = *reinterpret_cast<const PackedInt128*>(src);
    }
    static void set_to_max(void* buf) {
        *reinterpret_cast<PackedInt128*>(buf) = ~((int128_t)(1) << 127);
    }
    static void set_to_min(void* buf) {
        *reinterpret_cast<PackedInt128*>(buf) = (int128_t)(1) << 127;
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_FLOAT> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_FLOAT> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        CppType value = 0.0f;
        if (scan_key.length() > 0) {
            value = static_cast<CppType>(atof(scan_key.c_str()));
        }
        *reinterpret_cast<CppType*>(buf) = value;
        return OLAP_SUCCESS;
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DOUBLE> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_DOUBLE> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        CppType value = 0.0;
        if (scan_key.length() > 0) {
            value = atof(scan_key.c_str());
        }
        *reinterpret_cast<CppType*>(buf) = value;
        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%.10f", *reinterpret_cast<const CppType*>(src));
        return std::string(buf);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DECIMAL> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_DECIMAL> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        CppType* data_ptr = reinterpret_cast<CppType*>(buf);
        return data_ptr->from_string(scan_key);
    }
    static std::string to_string(const void* src) {
        const CppType* data_ptr = reinterpret_cast<const CppType*>(src);
        return data_ptr->to_string();
    }
    static void set_to_max(void* buf) {
        CppType* data = reinterpret_cast<CppType*>(buf);
        data->integer = 999999999999999999L;
        data->fraction = 999999999;
    }
    static void set_to_min(void* buf) {
        CppType* data = reinterpret_cast<CppType*>(buf);
        data->integer = -999999999999999999;
        data->fraction = -999999999;
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DATE> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_DATE> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        tm time_tm;
        char* res = strptime(scan_key.c_str(), "%Y-%m-%d", &time_tm);

        if (NULL != res) {
            int value = (time_tm.tm_year + 1900) * 16 * 32
                + (time_tm.tm_mon + 1) * 32
                + time_tm.tm_mday;
            *reinterpret_cast<CppType*>(buf) = value;
        } else {
            // 1400 - 01 - 01
            *reinterpret_cast<CppType*>(buf) = 716833;
        }

        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        tm time_tm;
        int value = *reinterpret_cast<const CppType*>(src);
        memset(&time_tm, 0, sizeof(time_tm));
        time_tm.tm_mday = static_cast<int>(value & 31);
        time_tm.tm_mon = static_cast<int>(value >> 5 & 15) - 1;
        time_tm.tm_year = static_cast<int>(value >> 9) - 1900;
        char buf[20] = {'\0'};
        strftime(buf, sizeof(buf), "%Y-%m-%d", &time_tm);
        return std::string(buf);
    }
    static void set_to_max(void* buf) {
        // max is 9999 * 16 * 32 + 12 * 32 + 31;
        *reinterpret_cast<CppType*>(buf) = 5119903;
    }
    static void set_to_min(void* buf) {
        // min is 0 * 16 * 32 + 1 * 32 + 1;
        *reinterpret_cast<CppType*>(buf) = 33;
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DATETIME> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_DATETIME> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        tm time_tm;
        char* res = strptime(scan_key.c_str(), "%Y-%m-%d %H:%M:%S", &time_tm);

        if (NULL != res) {
            CppType value = ((time_tm.tm_year + 1900) * 10000L
                            + (time_tm.tm_mon + 1) * 100L
                            + time_tm.tm_mday) * 1000000L
                            + time_tm.tm_hour * 10000L
                            + time_tm.tm_min * 100L
                            + time_tm.tm_sec;
            *reinterpret_cast<CppType*>(buf) = value;
        } else {
            // 1400 - 01 - 01
            *reinterpret_cast<CppType*>(buf) = 14000101000000L;
        }

        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        tm time_tm;
        CppType tmp = *reinterpret_cast<const CppType*>(src);
        CppType part1 = (tmp / 1000000L);
        CppType part2 = (tmp - part1 * 1000000L);

        time_tm.tm_year = static_cast<int>((part1 / 10000L) % 10000) - 1900;
        time_tm.tm_mon = static_cast<int>((part1 / 100) % 100) - 1;
        time_tm.tm_mday = static_cast<int>(part1 % 100);

        time_tm.tm_hour = static_cast<int>((part2 / 10000L) % 10000);
        time_tm.tm_min = static_cast<int>((part2 / 100) % 100);
        time_tm.tm_sec = static_cast<int>(part2 % 100);

        char buf[20] = {'\0'};
        strftime(buf, 20, "%Y-%m-%d %H:%M:%S", &time_tm);
        return std::string(buf);
    }
    static void set_to_max(void* buf) {
        // 设置为最大时间，其含义为：9999-12-31 23:59:59
        *reinterpret_cast<CppType*>(buf) = 99991231235959L;
    }
    static void set_to_min(void* buf) {
        *reinterpret_cast<CppType*>(buf) = 101000000;
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_CHAR> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_CHAR> {
    static bool equal(const void* left, const void* right) {
        const Slice* l_slice = reinterpret_cast<const Slice*>(left);
        const Slice* r_slice = reinterpret_cast<const Slice*>(right);
        return *l_slice == *r_slice;
    }
    static int cmp(const void* left, const void* right) {
        const Slice* l_slice = reinterpret_cast<const Slice*>(left);
        const Slice* r_slice = reinterpret_cast<const Slice*>(right);
        return l_slice->compare(*r_slice);
    }
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        size_t value_len = scan_key.length();
        if (value_len > OLAP_STRING_MAX_LENGTH) {
            LOG(WARNING) << "the len of value string is too long, len=" << value_len
                << ", max_len=" <<  OLAP_STRING_MAX_LENGTH;
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

        Slice* slice = reinterpret_cast<Slice*>(buf);
        memory_copy(slice->data, scan_key.c_str(), value_len);
        if (slice->size < value_len) {
            /*
             * CHAR type is of fixed length. Size in slice can be modified
             * only if value_len is greater than the fixed length. ScanKey
             * inputed by user may be greater than fixed length.
             */
            slice->size = value_len;
        }
        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        const Slice* slice = reinterpret_cast<const Slice*>(src);
        return slice->to_string();
    }
    static void copy_with_pool(void* dest, const void* src, MemPool* mem_pool) {
        Slice* l_slice = reinterpret_cast<Slice*>(dest);
        const Slice* r_slice = reinterpret_cast<const Slice*>(src);
        l_slice->data = reinterpret_cast<char*>(mem_pool->allocate(r_slice->size));
        memory_copy(l_slice->data, r_slice->data, r_slice->size);
        l_slice->size = r_slice->size;
    }
    static void copy_with_arena(void* dest, const void* src, Arena* arena) {
        Slice* l_slice = reinterpret_cast<Slice*>(dest);
        const Slice* r_slice = reinterpret_cast<const Slice*>(src);
        l_slice->data = reinterpret_cast<char*>(arena->Allocate(r_slice->size));
        memory_copy(l_slice->data, r_slice->data, r_slice->size);
        l_slice->size = r_slice->size;
    }
    static void copy_without_pool(void* dest, const void* src) {
        Slice* l_slice = reinterpret_cast<Slice*>(dest);
        const Slice* r_slice = reinterpret_cast<const Slice*>(src);
        memory_copy(l_slice->data, r_slice->data, r_slice->size);
        l_slice->size = r_slice->size;
    }
    static void set_to_max(void* buf) {
        // this function is used by scan key,
        // the size may be greater than length in schema.
        Slice* slice = reinterpret_cast<Slice*>(buf);
        memset(slice->data, 0xff, slice->size);
    }
    static void set_to_min(void* buf) {
        Slice* slice = reinterpret_cast<Slice*>(buf);
        memset(slice->data, 0, slice->size);
    }
    static uint32_t hash_code(const void* data, uint32_t seed) {
        const Slice* slice = reinterpret_cast<const Slice*>(data);
        return HashUtil::hash(slice->data, slice->size, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_VARCHAR> : public FieldTypeTraits<OLAP_FIELD_TYPE_CHAR> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        size_t value_len = scan_key.length();
        if (value_len > OLAP_STRING_MAX_LENGTH) {
            LOG(WARNING) << "the len of value string is too long, len=" << value_len
                << ", max_len=" <<  OLAP_STRING_MAX_LENGTH;
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

        Slice* slice = reinterpret_cast<Slice*>(buf);
        memory_copy(slice->data, scan_key.c_str(), value_len);
        slice->size = value_len;
        return OLAP_SUCCESS;
    }
    static void set_to_max(void* buf) {
        Slice* slice = reinterpret_cast<Slice*>(buf);
        slice->size = 1;
        memset(slice->data, 0xFF, 1);
    }
    static void set_to_min(void* buf) {
        Slice* slice = reinterpret_cast<Slice*>(buf);
        slice->size = 0;
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_HLL> : public FieldTypeTraits<OLAP_FIELD_TYPE_VARCHAR> {
    /*
     * Hyperloglog type only used as value, so
     * cmp/from_string/set_to_max/set_to_min function
     * in this struct has no significance
     */
};

// Instantiate this template to get static access to the type traits.
template<FieldType field_type>
struct TypeTraits : public FieldTypeTraits<field_type> {
    using CppType = typename FieldTypeTraits<field_type>::CppType;

    static const FieldType type = field_type;
    static const int32_t size = sizeof(CppType);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_TYPES_H
