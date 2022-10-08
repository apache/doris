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
#include <parallel_hashmap/phmap.h>

#include "runtime/string_value.h"
#include "udf/udf.h"
#include "util/bitmap_value.h"

namespace doris {

namespace detail {
class Helper {
public:
    static const int DATETIME_PACKED_TIME_BYTE_SIZE = 8;
    static const int DATETIME_TYPE_BYTE_SIZE = 4;
    static const int DECIMAL_BYTE_SIZE = 16;

    // get_val start
    template <typename ValType, typename T>
    static T get_val(const ValType& x) {
        DCHECK(!x.is_null);
        return x.val;
    }

    // serialize_size start
    template <typename T>
    static int32_t serialize_size(const T& v) {
        return sizeof(T);
    }

    // write_to start
    template <typename T>
    static char* write_to(const T& v, char* dest) {
        size_t type_size = sizeof(T);
        memcpy(dest, &v, type_size);
        dest += type_size;
        return dest;
    }

    // read_from start
    template <typename T>
    static void read_from(const char** src, T* result) {
        size_t type_size = sizeof(T);
        memcpy(result, *src, type_size);
        *src += type_size;
    }
};

template <>
inline StringValue Helper::get_val<StringVal>(const StringVal& x) {
    DCHECK(!x.is_null);
    return StringValue::from_string_val(x);
}

template <>
inline DateTimeValue Helper::get_val<DateTimeVal>(const DateTimeVal& x) {
    return DateTimeValue::from_datetime_val(x);
}

template <>
inline DecimalV2Value Helper::get_val<DecimalV2Val>(const DecimalV2Val& x) {
    return DecimalV2Value::from_decimal_val(x);
}
// get_val end

template <>
inline char* Helper::write_to<DateTimeValue>(const DateTimeValue& v, char* dest) {
    DateTimeVal value;
    v.to_datetime_val(&value);
    *(int64_t*)dest = value.packed_time;
    dest += DATETIME_PACKED_TIME_BYTE_SIZE;
    *(int*)dest = value.type;
    dest += DATETIME_TYPE_BYTE_SIZE;
    return dest;
}

template <>
inline char* Helper::write_to<DecimalV2Value>(const DecimalV2Value& v, char* dest) {
    __int128 value = v.value();
    memcpy(dest, &value, DECIMAL_BYTE_SIZE);
    dest += DECIMAL_BYTE_SIZE;
    return dest;
}

template <>
inline char* Helper::write_to<StringValue>(const StringValue& v, char* dest) {
    *(int32_t*)dest = v.len;
    dest += 4;
    memcpy(dest, v.ptr, v.len);
    dest += v.len;
    return dest;
}

template <>
inline char* Helper::write_to<std::string>(const std::string& v, char* dest) {
    *(uint32_t*)dest = v.size();
    dest += 4;
    memcpy(dest, v.c_str(), v.size());
    dest += v.size();
    return dest;
}
// write_to end

template <>
inline int32_t Helper::serialize_size<DateTimeValue>(const DateTimeValue& v) {
    return Helper::DATETIME_PACKED_TIME_BYTE_SIZE + Helper::DATETIME_TYPE_BYTE_SIZE;
}

template <>
inline int32_t Helper::serialize_size<DecimalV2Value>(const DecimalV2Value& v) {
    return Helper::DECIMAL_BYTE_SIZE;
}

template <>
inline int32_t Helper::serialize_size<StringValue>(const StringValue& v) {
    return v.len + 4;
}

template <>
inline int32_t Helper::serialize_size<std::string>(const std::string& v) {
    return v.size() + 4;
}
// serialize_size end

template <>
inline void Helper::read_from<DateTimeValue>(const char** src, DateTimeValue* result) {
    DateTimeVal value;
    value.is_null = false;
    value.packed_time = *(int64_t*)(*src);
    *src += DATETIME_PACKED_TIME_BYTE_SIZE;
    value.type = *(int*)(*src);
    *src += DATETIME_TYPE_BYTE_SIZE;
    *result = DateTimeValue::from_datetime_val(value);
}

template <>
inline void Helper::read_from<DecimalV2Value>(const char** src, DecimalV2Value* result) {
    __int128 v = 0;
    memcpy(&v, *src, DECIMAL_BYTE_SIZE);
    *src += DECIMAL_BYTE_SIZE;
    *result = DecimalV2Value(v);
}

template <>
inline void Helper::read_from<StringValue>(const char** src, StringValue* result) {
    int32_t length = *(int32_t*)(*src);
    *src += 4;
    *result = StringValue((char*)*src, length);
    *src += length;
}

template <>
inline void Helper::read_from<std::string>(const char** src, std::string* result) {
    int32_t length = *(int32_t*)(*src);
    *src += 4;
    *result = std::string((char*)*src, length);
    *src += length;
}
// read_from end

} // namespace detail

// Calculate the intersection of two or more bitmaps
// Usage: intersect_count(bitmap_column_to_count, filter_column, filter_values ...)
// Example: intersect_count(user_id, event, 'A', 'B', 'C'), meaning find the intersect count of user_id in all A/B/C 3 bitmaps
// Todo(kks) Use Array type instead of variable arguments
template <typename T>
struct BitmapIntersect {
public:
    BitmapIntersect() = default;

    explicit BitmapIntersect(const char* src) { deserialize(src); }

    void add_key(const T key) {
        BitmapValue empty_bitmap;
        _bitmaps[key] = empty_bitmap;
    }

    void update(const T& key, const BitmapValue& bitmap) {
        if (_bitmaps.find(key) != _bitmaps.end()) {
            _bitmaps[key] |= bitmap;
        }
    }

    void merge(const BitmapIntersect& other) {
        for (auto& kv : other._bitmaps) {
            if (_bitmaps.find(kv.first) != _bitmaps.end()) {
                _bitmaps[kv.first] |= kv.second;
            } else {
                _bitmaps[kv.first] = kv.second;
            }
        }
    }

    // intersection
    BitmapValue intersect() const {
        BitmapValue result;
        auto it = _bitmaps.begin();
        result |= it->second;
        it++;
        for (; it != _bitmaps.end(); it++) {
            result &= it->second;
        }
        return result;
    }

    // calculate the intersection for _bitmaps's bitmap values
    int64_t intersect_count() const {
        if (_bitmaps.empty()) {
            return 0;
        }
        return intersect().cardinality();
    }

    // the serialize size
    size_t size() {
        size_t size = 4;
        for (auto& kv : _bitmaps) {
            size += detail::Helper::serialize_size(kv.first);
            size += kv.second.getSizeInBytes();
        }
        return size;
    }

    //must call size() first
    void serialize(char* dest) {
        char* writer = dest;
        *(int32_t*)writer = _bitmaps.size();
        writer += 4;
        for (auto& kv : _bitmaps) {
            writer = detail::Helper::write_to(kv.first, writer);
            kv.second.write(writer);
            writer += kv.second.getSizeInBytes();
        }
    }

    void deserialize(const char* src) {
        const char* reader = src;
        int32_t bitmaps_size = *(int32_t*)reader;
        reader += 4;
        for (int32_t i = 0; i < bitmaps_size; i++) {
            T key;
            detail::Helper::read_from(&reader, &key);
            BitmapValue bitmap(reader);
            reader += bitmap.getSizeInBytes();
            _bitmaps[key] = bitmap;
        }
    }

private:
    std::map<T, BitmapValue> _bitmaps;
};

template <>
struct BitmapIntersect<std::string_view> {
public:
    BitmapIntersect() = default;

    explicit BitmapIntersect(const char* src) { deserialize(src); }

    void add_key(const std::string_view key) {
        BitmapValue empty_bitmap;
        _bitmaps[key] = empty_bitmap;
    }

    void update(const std::string_view& key, const BitmapValue& bitmap) {
        if (_bitmaps.find(key) != _bitmaps.end()) {
            _bitmaps[key] |= bitmap;
        }
    }

    void merge(const BitmapIntersect& other) {
        for (auto& kv : other._bitmaps) {
            if (_bitmaps.find(kv.first) != _bitmaps.end()) {
                _bitmaps[kv.first] |= kv.second;
            } else {
                _bitmaps[kv.first] = kv.second;
            }
        }
    }

    // intersection
    BitmapValue intersect() const {
        BitmapValue result;
        auto it = _bitmaps.begin();
        result |= it->second;
        it++;
        for (; it != _bitmaps.end(); it++) {
            result &= it->second;
        }
        return result;
    }

    // calculate the intersection for _bitmaps's bitmap values
    int64_t intersect_count() const {
        if (_bitmaps.empty()) {
            return 0;
        }
        return intersect().cardinality();
    }

    // the serialize size
    size_t size() {
        size_t size = 4;
        for (auto& kv : _bitmaps) {
            size += detail::Helper::serialize_size(kv.first);
            size += kv.second.getSizeInBytes();
        }
        return size;
    }

    //must call size() first
    void serialize(char* dest) {
        char* writer = dest;
        *(int32_t*)writer = _bitmaps.size();
        writer += 4;
        for (auto& kv : _bitmaps) {
            writer = detail::Helper::write_to(kv.first, writer);
            kv.second.write(writer);
            writer += kv.second.getSizeInBytes();
        }
    }

    void deserialize(const char* src) {
        const char* reader = src;
        int32_t bitmaps_size = *(int32_t*)reader;
        reader += 4;
        for (int32_t i = 0; i < bitmaps_size; i++) {
            std::string key;
            detail::Helper::read_from(&reader, &key);
            BitmapValue bitmap(reader);
            reader += bitmap.getSizeInBytes();
            _bitmaps[key] = bitmap;
        }
    }

private:
    phmap::flat_hash_map<std::string, BitmapValue> _bitmaps;
};

} // namespace doris
