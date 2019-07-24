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

#ifndef DORIS_BE_SRC_OLAP_FIELD_H
#define DORIS_BE_SRC_OLAP_FIELD_H

#include <string>

#include "olap/aggregate_func.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "olap/utils.h"
#include "runtime/mem_pool.h"
#include "util/hash_util.hpp"
#include "util/mem_util.hpp"
#include "util/slice.h"

namespace doris {

// Field内部参数为Field*的方法都要求实例类型和当前类型一致，否则会产生无法预知的错误
// 出于效率的考虑，大部分函数实现均没有对参数进行检查
class Field {
public:
    // 使用FieldInfo创建一个Field对象的实例
    // 根据类型的不同，使用不同的类模板参数或者子类
    // 对于没有预料到的类型，会返回NULL
    static Field* create(const TabletColumn& column);
    static Field* create_by_type(const FieldType& type);

    Field(const TabletColumn& column);

    inline void set_offset(size_t offset) { _offset = offset; }
    inline size_t get_offset() const { return _offset; }

    //get ptr without NULL byte
    inline char* get_ptr(char* buf) const { return buf + _offset + 1; }

    //get ptr with NULL byte
    inline char* get_field_ptr(char* buf) const { return buf + _offset; }

    inline size_t size() const { return _size; }
    inline size_t field_size() const { return _size + 1; }
    inline size_t index_size() const { return _index_size; }

    inline void set_to_max(char* buf) { return _type_info->set_to_max(buf); }
    inline void set_to_min(char* buf) { return _type_info->set_to_min(buf); }

    inline bool is_null(char* buf) const {
        return *reinterpret_cast<bool*>(buf + _offset);
    }

    inline void set_null(char* buf) const {
        *reinterpret_cast<bool*>(buf + _offset) = true;
    }

    inline void set_not_null(char* buf) const {
        *reinterpret_cast<bool*>(buf + _offset) = false;
    }

    // 返回-1，0，1，分别代表当前field小于，等于，大于传入参数中的field
    inline int cmp(char* left, char* right) const;
    inline int cmp(char* left, bool r_null, char* right) const;
    inline int index_cmp(char* left, char* right) const;
    inline bool equal(char* left, char* right);

    inline void aggregate(char* dest, char* src);
    inline void finalize(char* data);

    inline void copy_with_pool(char* dest, const char* src, MemPool* mem_pool);
    inline void copy_without_pool(char* dest, const char* src);
    inline void copy_without_pool(char* dest, bool is_null, const char* src);
    inline void agg_init(char* dest, const char* src);

    // copy filed content from src to dest without nullbyte
    inline void copy_content(char* dest, const char* src, MemPool* mem_pool) {
        _type_info->copy_with_pool(dest, src, mem_pool);
    }
    inline void to_index(char* dest, const char* src);

    // used by init scan key stored in string format
    // value_string should end with '\0'
    inline OLAPStatus from_string(char* buf, const std::string& value_string) {
        return _type_info->from_string(buf, value_string);
    }

    // 将内部的value转成string输出
    // 没有考虑实现的性能，仅供DEBUG使用
    inline std::string to_string(char* src) const {
        return _type_info->to_string(src);
    }

    inline uint32_t hash_code(char* data, uint32_t seed) const;
private:
    FieldType _type;
    // Field的长度，单位为字节
    uint16_t _size;
    // Field的最大长度，单位为字节，通常等于length， 变长字符串不同
    uint16_t _index_size;
    size_t _offset; //offset in row buf
    TypeInfo* _type_info;

    const AggregateInfo* _agg_info;
};

// 返回-1，0，1，分别代表当前field小于，等于，大于传入参数中的field
inline int Field::cmp(char* left, char* right) const {
    bool l_null = *reinterpret_cast<bool*>(left);
    bool r_null = *reinterpret_cast<bool*>(right);
    if (l_null != r_null) {
        return l_null ? -1 : 1;
    } else {
        return l_null ? 0 : (_type_info->cmp(left + 1, right + 1));
    }
}

inline int Field::cmp(char* left, bool r_null, char* right) const {
    bool l_null = *reinterpret_cast<bool*>(left);
    if (l_null != r_null) {
        return l_null ? -1 : 1;
    } else {
        return l_null ? 0 : (_type_info->cmp(left + 1, right));
    }
}

inline int Field::index_cmp(char* left, char* right) const {
    bool l_null = *reinterpret_cast<bool*>(left);
    bool r_null = *reinterpret_cast<bool*>(right);
    if (l_null != r_null) {
        return l_null ? -1 : 1;
    } else if (l_null){
        return 0;
    }

    int32_t res = 0;
    if (_type == OLAP_FIELD_TYPE_VARCHAR) {
        Slice* l_slice = reinterpret_cast<Slice*>(left + 1);
        Slice* r_slice = reinterpret_cast<Slice*>(right + 1);

        if (r_slice->size + OLAP_STRING_MAX_BYTES > _index_size
                || l_slice->size + OLAP_STRING_MAX_BYTES > _index_size) {
            // 如果field的实际长度比short key长，则仅比较前缀，确保相同short key的所有block都被扫描，
            // 否则，可以直接比较short key和field
            int compare_size = _index_size - OLAP_STRING_MAX_BYTES;
            // l_slice size and r_slice size may be less than compare_size
            // so calculate the min of the three size as new compare_size
            compare_size = std::min(std::min(compare_size, (int)l_slice->size), (int)r_slice->size);

            // This functionn is used to compare prefix index.
            // Only the fixed length of prefix index should be compared.
            // If r_slice->size > l_slice->size, igonre the extra parts directly.
            res = strncmp(l_slice->data, r_slice->data, compare_size);
            if (res == 0 && compare_size != (_index_size - OLAP_STRING_MAX_BYTES)) {
                if (l_slice->size < r_slice->size) {
                    res = -1;
                } else if (l_slice->size > r_slice->size) {
                    res = 1;
                } else {
                    res = 0;
                }
            }
        } else {
            res = l_slice->compare(*r_slice);
        }
    } else {
        res = _type_info->cmp(left + 1, right + 1);
    }

    return res;
}

inline bool Field::equal(char* left, char* right) {
    bool l_null = *reinterpret_cast<bool*>(left);
    bool r_null = *reinterpret_cast<bool*>(right);

    if (l_null != r_null) {
        return false;
    } else if (l_null) {
        return true;
    } else {
        return _type_info->equal(left + 1, right + 1);
    }
}

inline void Field::aggregate(char* dest, char* src) {
    _agg_info->update(dest, src, nullptr);
}

inline void Field::finalize(char* data) {
    if (OLAP_UNLIKELY(_type == OLAP_FIELD_TYPE_HLL)) {
        // hyperloglog type use this function
        _agg_info->finalize(data, nullptr);
    }
}

inline void Field::copy_with_pool(char* dest, const char* src, MemPool* mem_pool) {
    bool is_null = *reinterpret_cast<const bool*>(src);
    *reinterpret_cast<bool*>(dest) = is_null;
    if (is_null) {
        return;
    }
    _type_info->copy_with_pool(dest + 1, src + 1, mem_pool);
}

inline void Field::copy_without_pool(char* dest, const char* src) {
    bool is_null = *reinterpret_cast<const bool*>(src);
    *reinterpret_cast<bool*>(dest) = is_null;
    if (is_null) {
        return;
    }
    return _type_info->copy_without_pool(dest + 1, src + 1);
}

inline void Field::copy_without_pool(char* dest, bool is_null, const char* src) {
    *reinterpret_cast<bool*>(dest) = is_null;
    if (is_null) {
        return;
    }
    return _type_info->copy_without_pool(dest + 1, src);
}

inline void Field::agg_init(char* dest, const char* src) {
    // TODO(zc): This function is also used to initialize key columns.
    // So, refactor this in later PR
    if (OLAP_LIKELY(_type != OLAP_FIELD_TYPE_HLL)) {
        copy_without_pool(dest, src);
    } else {
        bool is_null = *reinterpret_cast<const bool*>(src);
        *reinterpret_cast<bool*>(dest) = is_null;
        Slice* slice = reinterpret_cast<Slice*>(dest + 1);
        size_t hll_ptr = *(size_t*)(slice->data - sizeof(HllContext*));
        HllContext* context = (reinterpret_cast<HllContext*>(hll_ptr));
        HllSetHelper::init_context(context);
        HllSetHelper::fill_set(src + 1, context);
        context->has_value = true;
    }
}

inline void Field::to_index(char* dest, const char* src) {
    bool is_null = *reinterpret_cast<const bool*>(src);
    *reinterpret_cast<bool*>(dest) = is_null;
    if (is_null) {
        return;
    }

    if (_type == OLAP_FIELD_TYPE_VARCHAR) {
        // 先清零，再拷贝
        memset(dest + 1, 0, _index_size);
        const Slice* slice = reinterpret_cast<const Slice*>(src + 1);
        size_t copy_size = slice->size < _index_size - OLAP_STRING_MAX_BYTES ?
                           slice->size : _index_size - OLAP_STRING_MAX_BYTES;
        *reinterpret_cast<StringLengthType*>(dest + 1) = copy_size;
        memory_copy(dest + OLAP_STRING_MAX_BYTES + 1, slice->data, copy_size);
    } else if (_type == OLAP_FIELD_TYPE_CHAR) {
        // 先清零，再拷贝
        memset(dest + 1, 0, _index_size);
        const Slice* slice = reinterpret_cast<const Slice*>(src + 1);
        memory_copy(dest + 1, slice->data, _index_size);
    } else {
        memory_copy(dest + 1, src + 1, size());
    }
}

inline uint32_t Field::hash_code(char* data, uint32_t seed) const {
    bool is_null = (*reinterpret_cast<bool*>(data) != 0);
    if (is_null) {
        return HashUtil::hash(&is_null, sizeof(is_null), seed);
    }
    return _type_info->hash_code(data + 1, seed);
}

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_FIELD_H
