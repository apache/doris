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
#include "olap/field_info.h"
#include "runtime/mem_pool.h"
#include "util/hash_util.hpp"
#include "util/mem_util.hpp"
#include "util/slice.h"
#include "util/types.h"

namespace doris {

class TypeInfo {
public:
    inline int equal(const char* left, const char* right) const {
        return _equal(left, right);
    }

    inline int cmp(const char* left, const char* right) const {
        return _cmp(left, right);
    }

    inline void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        _copy_with_pool(dest, src, mem_pool);
    }

    inline void copy_without_pool(char* dest, const char* src) {
        _copy_without_pool(dest, src);
    }

    OLAPStatus from_string(char* buf, const std::string& scan_key) {
        return _from_string(buf, scan_key);
    }

    std::string to_string(char* src) { return _to_string(src); }

    inline void set_to_max(char* buf) { _set_to_max(buf); }
    inline void set_to_min(char* buf) { _set_to_min(buf); }
    inline bool is_min(char* buf) { return _is_min(buf); }

    inline uint32_t hash_code(char* data, uint32_t seed) { return _hash_code(data, seed); }
    inline const size_t size() const { return _size; }

    inline FieldType type() const { return _field_type; }
private:
    int (*_equal)(const void* left, const void* right);
    int (*_cmp)(const void* left, const void* right);

    void (*_copy_with_pool)(char* dest, const char* src, MemPool* mem_pool);
    void (*_copy_without_pool)(char* dest, const char* src);

    OLAPStatus (*_from_string)(char* buf, const std::string& scan_key);
    std::string (*_to_string)(char* src);

    void (*_set_to_max)(char* buf);
    void (*_set_to_min)(char* buf);
    bool (*_is_min)(char* buf);

    uint32_t (*_hash_code)(char* data, uint32_t seed);

    const size_t _size;
    const FieldType _field_type;

    friend class TypeInfoResolver;
    template<typename TypeTraitsClass> TypeInfo(TypeTraitsClass t);
};

extern TypeInfo* get_type_info(FieldType field_type);

template<FieldType field_type> struct FieldTypeTraits {};

template<FieldType field_type>
static int generic_equal(const void* left, const void* right) {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;
    CppType l_value = *reinterpret_cast<const CppType*>(left);
    CppType r_value = *reinterpret_cast<const CppType*>(right);
    return l_value == r_value;
};

template<FieldType field_type>
static int generic_compare(const void* left, const void* right) {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;
    CppType left_int = *reinterpret_cast<const CppType*>(left);
    CppType right_int = *reinterpret_cast<const CppType*>(right);
    if (left_int < right_int) {
        return -1;
    } else if (left_int > right_int) {
        return 1;
    } else {
        return 0;
    }
};

template<FieldType field_type>
static void generic_copy(char* dest, const char* src) {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;
    *reinterpret_cast<CppType*>(dest) = *reinterpret_cast<const CppType*>(src);
}

template<>
void generic_copy<OLAP_FIELD_TYPE_LARGEINT>(char* dest, const char* src) {
    *reinterpret_cast<PackedInt128*>(dest) = *reinterpret_cast<const PackedInt128*>(src);
}

template<FieldType field_type>
static std::string generic_to_string(char* src) {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;
    std::stringstream stream;
    stream << *reinterpret_cast<CppType*>(src);
    return stream.str();
}

template<FieldType field_type>
static OLAPStatus generic_from_string(char* buf, const std::string& scan_key) {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;
    CppType value = 0;
    if (scan_key.length() > 0) {
        value = static_cast<CppType>(strtol(scan_key.c_str(), NULL, 10));
    }
    *reinterpret_cast<CppType*>(buf) = value;
    return OLAP_SUCCESS;
}

template<FieldType field_type>
static void generic_set_to_max(char* buf) {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;
    *reinterpret_cast<CppType*>(buf) = std::numeric_limits<CppType>::max();
}

template<FieldType field_type>
static void generic_set_to_min(char* buf) {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;
    *reinterpret_cast<CppType*>(buf) = std::numeric_limits<CppType>::min();
}

template<FieldType field_type>
static bool generic_is_min(char* buf) {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;
    CppType min_value = std::numeric_limits<CppType>::min();
    return (*reinterpret_cast<CppType*>(buf) == min_value);
}

template<FieldType field_type>
static uint32_t generic_hash_code(char* data, uint32_t seed) {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;
    return HashUtil::hash(data, sizeof(CppType), seed);
}

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_BOOL> {
    typedef bool CppType;
    static const char* name() {
        return "bool";
    }
    static int equal(const void* left, const void* right) {
        return generic_equal<OLAP_FIELD_TYPE_BOOL>(left, right);
    }
    static int cmp(const void* left, const void* right) {
        return generic_compare<OLAP_FIELD_TYPE_BOOL>(left, right);
    }
    static std::string to_string(char* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", *reinterpret_cast<CppType*>(src));
        return std::string(buf);
    }
    static OLAPStatus from_string(char* buf, const std::string& scan_key) {
        return generic_from_string<OLAP_FIELD_TYPE_BOOL>(buf, scan_key);
    }
    static void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        generic_copy<OLAP_FIELD_TYPE_BOOL>(dest, src);
    }
    static void copy_without_pool(char* dest, const char* src) {
        generic_copy<OLAP_FIELD_TYPE_BOOL>(dest, src);
    }
    static void set_to_max(char* buf) {
        static bool bool_max = true;
        (*(bool*)buf) = bool_max;
    }
    static void set_to_min(char* buf) {
        static bool bool_min = true;
        (*(bool*)buf) = bool_min;
    }
    static bool is_min(char* buf) {
        return (*(bool*)buf) == false;
    }
    static uint32_t hash_code(char* data, uint32_t seed) {
        return generic_hash_code<OLAP_FIELD_TYPE_BOOL>(data, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_TINYINT> {
    typedef int8_t CppType;
    static const char* name() {
        return "int8_t";
    }
    static int equal(const void* left, const void* right) {
        return generic_equal<OLAP_FIELD_TYPE_TINYINT>(left, right);
    }
    static int cmp(const void* left, const void* right) {
        return generic_compare<OLAP_FIELD_TYPE_TINYINT>(left, right);
    }
    static std::string to_string(char* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", *reinterpret_cast<CppType*>(src));
        return std::string(buf);
    }
    static OLAPStatus from_string(char* buf, const std::string& scan_key) {
        return generic_from_string<OLAP_FIELD_TYPE_TINYINT>(buf, scan_key);
    }
    static void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        generic_copy<OLAP_FIELD_TYPE_TINYINT>(dest, src);
    }
    static void copy_without_pool(char* dest, const char* src) {
        generic_copy<OLAP_FIELD_TYPE_TINYINT>(dest, src);
    }
    static void set_to_max(char* buf) {
        generic_set_to_max<OLAP_FIELD_TYPE_TINYINT>(buf);
    }
    static void set_to_min(char* buf) {
        generic_set_to_min<OLAP_FIELD_TYPE_TINYINT>(buf);
    }
    static bool is_min(char* buf) {
        return generic_is_min<OLAP_FIELD_TYPE_TINYINT>(buf);
    }
    static uint32_t hash_code(char* data, uint32_t seed) {
        return generic_hash_code<OLAP_FIELD_TYPE_TINYINT>(data, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_SMALLINT> {
    typedef int16_t CppType;
    static const char* name() {
        return "int16_t";
    }
    static int equal(const void* left, const void* right) {
        return generic_equal<OLAP_FIELD_TYPE_SMALLINT>(left, right);
    }
    static int cmp(const void* left, const void* right) {
        return generic_compare<OLAP_FIELD_TYPE_SMALLINT>(left, right);
    }
    static std::string to_string(char* src) {
        return generic_to_string<OLAP_FIELD_TYPE_SMALLINT>(src);
    }
    static OLAPStatus from_string(char* buf, const std::string& scan_key) {
        return generic_from_string<OLAP_FIELD_TYPE_SMALLINT>(buf, scan_key);
    }
    static void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        generic_copy<OLAP_FIELD_TYPE_SMALLINT>(dest, src);
    }
    static void copy_without_pool(char* dest, const char* src) {
        generic_copy<OLAP_FIELD_TYPE_SMALLINT>(dest, src);
    }
    static void set_to_max(char* buf) {
        generic_set_to_max<OLAP_FIELD_TYPE_SMALLINT>(buf);
    }
    static void set_to_min(char* buf) {
        generic_set_to_min<OLAP_FIELD_TYPE_SMALLINT>(buf);
    }
    static bool is_min(char* buf) {
        return generic_is_min<OLAP_FIELD_TYPE_SMALLINT>(buf);
    }
    static uint32_t hash_code(char* data, uint32_t seed) {
        return generic_hash_code<OLAP_FIELD_TYPE_SMALLINT>(data, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_INT> {
    typedef int32_t CppType;
    static const char* name() {
        return "int32_t";
    }
    static int equal(const void* left, const void* right) {
        return generic_equal<OLAP_FIELD_TYPE_INT>(left, right);
    }
    static int cmp(const void* left, const void* right) {
        return generic_compare<OLAP_FIELD_TYPE_INT>(left, right);
    }
    static std::string to_string(char* src) {
        return generic_to_string<OLAP_FIELD_TYPE_INT>(src);
    }
    static OLAPStatus from_string(char* buf, const std::string& scan_key) {
        return generic_from_string<OLAP_FIELD_TYPE_INT>(buf, scan_key);
    }
    static void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        generic_copy<OLAP_FIELD_TYPE_INT>(dest, src);
    }
    static void copy_without_pool(char* dest, const char* src) {
        generic_copy<OLAP_FIELD_TYPE_INT>(dest, src);
    }
    static void set_to_max(char* buf) {
        generic_set_to_max<OLAP_FIELD_TYPE_INT>(buf);
    }
    static void set_to_min(char* buf) {
        generic_set_to_min<OLAP_FIELD_TYPE_INT>(buf);
    }
    static bool is_min(char* buf) {
        return generic_is_min<OLAP_FIELD_TYPE_INT>(buf);
    }
    static uint32_t hash_code(char* data, uint32_t seed) {
        return generic_hash_code<OLAP_FIELD_TYPE_INT>(data, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_UNSIGNED_INT> {
    typedef uint32_t CppType;
    static const char* name() {
        return "uint32_t";
    }
    static int equal(const void* left, const void* right) {
        return generic_equal<OLAP_FIELD_TYPE_UNSIGNED_INT>(left, right);
    }
    static int cmp(const void* left, const void* right) {
        return generic_compare<OLAP_FIELD_TYPE_UNSIGNED_INT>(left, right);
    }
    static std::string to_string(char* src) {
        return generic_to_string<OLAP_FIELD_TYPE_UNSIGNED_INT>(src);
    }
    static OLAPStatus from_string(char* buf, const std::string& scan_key) {
        return generic_from_string<OLAP_FIELD_TYPE_UNSIGNED_INT>(buf, scan_key);
    }
    static void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        generic_copy<OLAP_FIELD_TYPE_UNSIGNED_INT>(dest, src);
    }
    static void copy_without_pool(char* dest, const char* src) {
        generic_copy<OLAP_FIELD_TYPE_UNSIGNED_INT>(dest, src);
    }
    static void set_to_max(char* buf) {
        generic_set_to_max<OLAP_FIELD_TYPE_UNSIGNED_INT>(buf);
    }
    static void set_to_min(char* buf) {
        generic_set_to_min<OLAP_FIELD_TYPE_UNSIGNED_INT>(buf);
    }
    static bool is_min(char* buf) {
        return generic_is_min<OLAP_FIELD_TYPE_UNSIGNED_INT>(buf);
    }
    static uint32_t hash_code(char* data, uint32_t seed) {
        return generic_hash_code<OLAP_FIELD_TYPE_UNSIGNED_INT>(data, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_BIGINT> {
    typedef int64_t CppType;
    static const char* name() {
        return "int64_t";
    }
    static int equal(const void* left, const void* right) {
        return generic_equal<OLAP_FIELD_TYPE_BIGINT>(left, right);
    }
    static int cmp(const void* left, const void* right) {
        return generic_compare<OLAP_FIELD_TYPE_BIGINT>(left, right);
    }
    static OLAPStatus from_string(char* buf, const std::string& scan_key) {
        return generic_from_string<OLAP_FIELD_TYPE_BIGINT>(buf, scan_key);
    }
    static std::string to_string(char* src) {
        return generic_to_string<OLAP_FIELD_TYPE_BIGINT>(src);
    }
    static void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        generic_copy<OLAP_FIELD_TYPE_BIGINT>(dest, src);
    }
    static void copy_without_pool(char* dest, const char* src) {
        generic_copy<OLAP_FIELD_TYPE_BIGINT>(dest, src);
    }
    static void set_to_max(char* buf) {
        generic_set_to_max<OLAP_FIELD_TYPE_BIGINT>(buf);
    }
    static void set_to_min(char* buf) {
        generic_set_to_min<OLAP_FIELD_TYPE_BIGINT>(buf);
    }
    static bool is_min(char* buf) {
        return generic_is_min<OLAP_FIELD_TYPE_BIGINT>(buf);
    }
    static uint32_t hash_code(char* data, uint32_t seed) {
        return generic_hash_code<OLAP_FIELD_TYPE_BIGINT>(data, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT> {
    typedef int128_t CppType;
    static const char* name() {
        return "int128_t";
    }
    static int equal(const void* left, const void* right) {
        return generic_equal<OLAP_FIELD_TYPE_LARGEINT>(left, right);
    }
    static int cmp(const void* left, const void* right) {
        return generic_compare<OLAP_FIELD_TYPE_LARGEINT>(left, right);
    }
    static OLAPStatus from_string(char* buf, const std::string& scan_key) {
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
    static std::string to_string(char* src) {
        char buf[1024];
        int128_t value = reinterpret_cast<PackedInt128*>(src)->value;
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
    static void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        generic_copy<OLAP_FIELD_TYPE_LARGEINT>(dest, src);
    }
    static void copy_without_pool(char* dest, const char* src) {
        generic_copy<OLAP_FIELD_TYPE_LARGEINT>(dest, src);
    }
    static void set_to_max(char* buf) {
        *reinterpret_cast<PackedInt128*>(buf) = ~((int128_t)(1) << 127);
    }
    static void set_to_min(char* buf) {
        *reinterpret_cast<PackedInt128*>(buf) = (int128_t)(1) << 127;
    }
    static bool is_min(char* buf) {
        int128_t min_value = (CppType)(1) << 127;
        return reinterpret_cast<PackedInt128*>(buf)->value == min_value;
    }
    static uint32_t hash_code(char* data, uint32_t seed) {
        return generic_hash_code<OLAP_FIELD_TYPE_LARGEINT>(data, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_FLOAT> {
    typedef float CppType;
    static const char* name() {
        return "float";
    }
    static int equal(const void* left, const void* right) {
        return generic_equal<OLAP_FIELD_TYPE_FLOAT>(left, right);
    }
    static int cmp(const void* left, const void* right) {
        return generic_compare<OLAP_FIELD_TYPE_FLOAT>(left, right);
    }
    static OLAPStatus from_string(char* buf, const std::string& scan_key) {
        CppType value = 0.0f;

        if (scan_key.length() > 0) {
            value = static_cast<CppType>(atof(scan_key.c_str()));
        }

        *reinterpret_cast<CppType*>(buf) = value;
        return OLAP_SUCCESS;
    }
    static std::string to_string(char* src) {
        return generic_to_string<OLAP_FIELD_TYPE_FLOAT>(src);
    }
    static void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        generic_copy<OLAP_FIELD_TYPE_FLOAT>(dest, src);
    }
    static void copy_without_pool(char* dest, const char* src) {
        generic_copy<OLAP_FIELD_TYPE_FLOAT>(dest, src);
    }
    static void set_to_max(char* buf) {
        generic_set_to_max<OLAP_FIELD_TYPE_FLOAT>(buf);
    }
    static void set_to_min(char* buf) {
        generic_set_to_min<OLAP_FIELD_TYPE_FLOAT>(buf);
    }
    static bool is_min(char* buf) {
        return generic_is_min<OLAP_FIELD_TYPE_FLOAT>(buf);
    }
    static uint32_t hash_code(char* data, uint32_t seed) {
        return generic_hash_code<OLAP_FIELD_TYPE_FLOAT>(data, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DOUBLE> {
    typedef double CppType;
    static const char* name() {
        return "double";
    }
    static int equal(const void* left, const void* right) {
        return generic_equal<OLAP_FIELD_TYPE_DOUBLE>(left, right);
    }
    static int cmp(const void* left, const void* right) {
        return generic_compare<OLAP_FIELD_TYPE_DOUBLE>(left, right);
    }
    static OLAPStatus from_string(char* buf, const std::string& scan_key) {
        double value = 0.0;

        if (scan_key.length() > 0) {
            value = atof(scan_key.c_str());
        }

        *reinterpret_cast<CppType*>(buf) = value;
        return OLAP_SUCCESS;
    }
    static std::string to_string(char* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%.10f", *reinterpret_cast<CppType*>(src));
        return std::string(buf);
    }
    static void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        generic_copy<OLAP_FIELD_TYPE_DOUBLE>(dest, src);
    }
    static void copy_without_pool(char* dest, const char* src) {
        generic_copy<OLAP_FIELD_TYPE_DOUBLE>(dest, src);
    }
    static void set_to_max(char* buf) {
        generic_set_to_max<OLAP_FIELD_TYPE_DOUBLE>(buf);
    }
    static void set_to_min(char* buf) {
        generic_set_to_min<OLAP_FIELD_TYPE_DOUBLE>(buf);
    }
    static bool is_min(char* buf) {
        return generic_is_min<OLAP_FIELD_TYPE_DOUBLE>(buf);
    }
    static uint32_t hash_code(char* data, uint32_t seed) {
        return generic_hash_code<OLAP_FIELD_TYPE_DOUBLE>(data, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DECIMAL> {
    typedef decimal12_t CppType;
    static const char* name() {
        return "decimal";
    }
    static int equal(const void* left, const void* right) {
        return generic_equal<OLAP_FIELD_TYPE_DECIMAL>(left, right);
    }
    static int cmp(const void* left, const void* right) {
        return generic_compare<OLAP_FIELD_TYPE_DECIMAL>(left, right);
    }
    static OLAPStatus from_string(char* buf, const std::string& scan_key) {
        decimal12_t* data_ptr = reinterpret_cast<decimal12_t*>(buf);
        return data_ptr->from_string(scan_key);
    }
    static std::string to_string(char* src) {
        decimal12_t* data_ptr = reinterpret_cast<CppType*>(src);
        return data_ptr->to_string();
    }
    static void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        generic_copy<OLAP_FIELD_TYPE_DECIMAL>(dest, src);
    }
    static void copy_without_pool(char* dest, const char* src) {
        generic_copy<OLAP_FIELD_TYPE_DECIMAL>(dest, src);
    }
    static void set_to_max(char* buf) {
        decimal12_t* data = reinterpret_cast<decimal12_t*>(buf);
        data->integer = 999999999999999999L;
        data->fraction = 999999999;
    }
    static void set_to_min(char* buf) {
        decimal12_t* data = reinterpret_cast<decimal12_t*>(buf);
        data->integer = -999999999999999999;
        data->fraction = -999999999;
    }
    static bool is_min(char* buf) {
        decimal12_t* data = reinterpret_cast<decimal12_t*>(buf);
        return (data->integer == -999999999999999999L
                && data->fraction == -999999999);
    }
    static uint32_t hash_code(char* data, uint32_t seed) {
        return generic_hash_code<OLAP_FIELD_TYPE_DECIMAL>(data, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DATE> {
    typedef uint24_t CppType;
    static const char* name() {
        return "date";
    }
    static int equal(const void* left, const void* right) {
        return generic_equal<OLAP_FIELD_TYPE_DATE>(left, right);
    }
    static int cmp(const void* left, const void* right) {
        return generic_compare<OLAP_FIELD_TYPE_DATE>(left, right);
    }
    static OLAPStatus from_string(char* buf, const std::string& scan_key) {
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
    static std::string to_string(char* src) {
        tm time_tm;
        int value = *reinterpret_cast<uint24_t*>(src);
        memset(&time_tm, 0, sizeof(time_tm));
        time_tm.tm_mday = static_cast<int>(value & 31);
        time_tm.tm_mon = static_cast<int>(value >> 5 & 15) - 1;
        time_tm.tm_year = static_cast<int>(value >> 9) - 1900;
        char buf[20] = {'\0'};
        strftime(buf, sizeof(buf), "%Y-%m-%d", &time_tm);
        return std::string(buf);
    }
    static void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        generic_copy<OLAP_FIELD_TYPE_DATE>(dest, src);
    }
    static void copy_without_pool(char* dest, const char* src) {
        generic_copy<OLAP_FIELD_TYPE_DATE>(dest, src);
    }
    static void set_to_max(char* buf) {
        // max is 9999 * 16 * 32 + 12 * 32 + 31;
        *reinterpret_cast<CppType*>(buf) = 5119903;
    }
    static void set_to_min(char* buf) {
        // min is 0 * 16 * 32 + 1 * 32 + 1;
        *reinterpret_cast<CppType*>(buf) = 33;
    }
    static bool is_min(char* buf) {
        CppType value = *reinterpret_cast<CppType*>(buf);
        return (33 == value);
    }
    static uint32_t hash_code(char* data, uint32_t seed) {
        return generic_hash_code<OLAP_FIELD_TYPE_DATE>(data, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DATETIME> {
    typedef int64_t CppType;
    static const char* name() {
        return "datetime";
    }
    static int equal(const void* left, const void* right) {
        return generic_equal<OLAP_FIELD_TYPE_DATETIME>(left, right);
    }
    static int cmp(const void* left, const void* right) {
        return generic_compare<OLAP_FIELD_TYPE_DATETIME>(left, right);
    }
    static OLAPStatus from_string(char* buf, const std::string& scan_key) {
        tm time_tm;
        char* res = strptime(scan_key.c_str(), "%Y-%m-%d %H:%M:%S", &time_tm);

        if (NULL != res) {
            int64_t value = ((time_tm.tm_year + 1900) * 10000L
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
    static std::string to_string(char* src) {
        tm time_tm;
        int64_t tmp = *reinterpret_cast<int64_t*>(src);
        int64_t part1 = (tmp / 1000000L);
        int64_t part2 = (tmp - part1 * 1000000L);

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
    static void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        generic_copy<OLAP_FIELD_TYPE_DATETIME>(dest, src);
    }
    static void copy_without_pool(char* dest, const char* src) {
        generic_copy<OLAP_FIELD_TYPE_DATETIME>(dest, src);
    }
    static void set_to_max(char* buf) {
        // 设置为最大时间，其含义为：9999-12-31 23:59:59
        *reinterpret_cast<CppType*>(buf) = 99991231235959L;
    }
    static void set_to_min(char* buf) {
        *reinterpret_cast<CppType*>(buf) = 101000000;
    }
    static bool is_min(char* buf) {
        CppType value = *reinterpret_cast<CppType*>(buf);
        return (value == 101000000);
    }
    static uint32_t hash_code(char* data, uint32_t seed) {
        return generic_hash_code<OLAP_FIELD_TYPE_DATETIME>(data, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_CHAR> {
    typedef Slice CppType;
    static const char* name() {
        return "char";
    }
    static int equal(const void* left, const void* right) {
        const Slice* l_slice = reinterpret_cast<const Slice*>(left);
        const Slice* r_slice = reinterpret_cast<const Slice*>(right);
        return *l_slice == *r_slice;
    }
    static int cmp(const void* left, const void* right) {
        const Slice* l_slice = reinterpret_cast<const Slice*>(left);
        const Slice* r_slice = reinterpret_cast<const Slice*>(right);
        return l_slice->compare(*r_slice);
    }
    static OLAPStatus from_string(char* buf, const std::string& scan_key) {
        size_t value_len = scan_key.length();
        if (value_len > OLAP_STRING_MAX_LENGTH) {
            OLAP_LOG_WARNING("the len of value string is too long[len=%lu, max_len=%lu].",
                             value_len, OLAP_STRING_MAX_LENGTH);
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
    static std::string to_string(char* src) {
        Slice* slice = reinterpret_cast<Slice*>(src);
        return slice->to_string();
    }
    static void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        Slice* l_slice = reinterpret_cast<Slice*>(dest);
        const Slice* r_slice = reinterpret_cast<const Slice*>(src);
        l_slice->data = reinterpret_cast<char*>(mem_pool->allocate(r_slice->size));
        memory_copy(l_slice->data, r_slice->data, r_slice->size);
        l_slice->size = r_slice->size;
    }
    static void copy_without_pool(char* dest, const char* src) {
        Slice* l_slice = reinterpret_cast<Slice*>(dest);
        const Slice* r_slice = reinterpret_cast<const Slice*>(src);
        memory_copy(l_slice->data, r_slice->data, r_slice->size);
        l_slice->size = r_slice->size;
    }
    static void set_to_max(char* buf) {
        // this function is used by scan key,
        // the size may be greater than length in schema.
        Slice* slice = reinterpret_cast<Slice*>(buf);
        memset(slice->data, 0xff, slice->size);
    }
    static void set_to_min(char* buf) {
        Slice* slice = reinterpret_cast<Slice*>(buf);
        memset(slice->data, 0, slice->size);
    }
    static bool is_min(char* buf) {
        Slice* slice = reinterpret_cast<Slice*>(buf);
        size_t i = 0;
        while (i < slice->size) {
            if (slice->data[i] != '\0') {
                return false;
            }
            i++;
        }
        return true;
    }
    static uint32_t hash_code(char* data, uint32_t seed) {
        Slice* slice = reinterpret_cast<Slice*>(data);
        return HashUtil::hash(slice->data, slice->size, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_VARCHAR> {
    typedef Slice CppType;
    static const char* name() {
        return "varchar";
    }
    static int equal(const void* left, const void* right) {
        const Slice* l_slice = reinterpret_cast<const Slice*>(left);
        const Slice* r_slice = reinterpret_cast<const Slice*>(right);
        return *l_slice == *r_slice;
    }
    static int cmp(const void* left, const void* right) {
        const Slice* l_slice = reinterpret_cast<const Slice*>(left);
        const Slice* r_slice = reinterpret_cast<const Slice*>(right);
        return l_slice->compare(*r_slice);
    }
    static OLAPStatus from_string(char* buf, const std::string& scan_key) {
        size_t value_len = scan_key.length();
        if (value_len > OLAP_STRING_MAX_LENGTH) {
            OLAP_LOG_WARNING("the len of value string is too long[len=%lu, max_len=%lu].",
                             value_len, OLAP_STRING_MAX_LENGTH);
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

        Slice* slice = reinterpret_cast<Slice*>(buf);
        memory_copy(slice->data, scan_key.c_str(), value_len);
        slice->size = value_len;
        return OLAP_SUCCESS;
    }
    static std::string to_string(char* src) {
        Slice* slice = reinterpret_cast<Slice*>(src);
        return slice->to_string();
    }
    static void copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
        Slice* l_slice = reinterpret_cast<Slice*>(dest);
        const Slice* r_slice = reinterpret_cast<const Slice*>(src);

        l_slice->data = reinterpret_cast<char*>(mem_pool->allocate(r_slice->size));
        memory_copy(l_slice->data, r_slice->data, r_slice->size);
        l_slice->size = r_slice->size;
    }
    static void copy_without_pool(char* dest, const char* src) {
        Slice* l_slice = reinterpret_cast<Slice*>(dest);
        const Slice* r_slice = reinterpret_cast<const Slice*>(src);
        memory_copy(l_slice->data, r_slice->data, r_slice->size);
        l_slice->size = r_slice->size;
    }
    static void set_to_max(char* buf) {
        Slice* slice = reinterpret_cast<Slice*>(buf);
        slice->size = 1;
        memset(slice->data, 0xFF, 1);
    }
    static void set_to_min(char* buf) {
        Slice* slice = reinterpret_cast<Slice*>(buf);
        slice->size = 0;
    }
    static bool is_min(char* buf) {
        Slice* slice = reinterpret_cast<Slice*>(buf);
        return (slice->size == 0);
    }
    static uint32_t hash_code(char* data, uint32_t seed) {
        Slice* slice = reinterpret_cast<Slice*>(data);
        return HashUtil::hash(slice->data, slice->size, seed);
    }
};

template<>
struct FieldTypeTraits<OLAP_FIELD_TYPE_HLL> : public FieldTypeTraits<OLAP_FIELD_TYPE_VARCHAR> {
    /*
     * Hyperloglog type only used as value, so
     * cmp/from_string/set_to_max/set_to_min/is_min function
     * in this struct has no significance
     */
    static const char* name() {
        return "hyperloglog";
    }
};

// Instantiate this template to get static access to the type traits.
template<FieldType field_type>
struct TypeTraits : public FieldTypeTraits<field_type> {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;

    static const FieldType type = field_type;
    static const int32_t size = sizeof(CppType);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_TYPES_H
