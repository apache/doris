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

#include "common/cast_set.h"
#include "core/string_ref.h"
#include "core/value/bitmap_value.h"

namespace doris {

namespace detail {
class Helper {
public:
    static const int DATETIME_PACKED_TIME_BYTE_SIZE = 8;
    static const int DATETIME_TYPE_BYTE_SIZE = 4;
    static const int DECIMAL_BYTE_SIZE = 16;

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
    static bool read_from(const char** src, size_t* remaining, T* result) {
        size_t type_size = sizeof(T);
        if (*remaining < type_size) {
            return false;
        }
        memcpy(result, *src, type_size);
        *src += type_size;
        *remaining -= type_size;
        return true;
    }
};

template <>
inline char* Helper::write_to<VecDateTimeValue>(const VecDateTimeValue& v, char* dest) {
    *(int64_t*)dest = v.to_int64_datetime_packed();
    dest += DATETIME_PACKED_TIME_BYTE_SIZE;
    *(int*)dest = v.type();
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
inline char* Helper::write_to<StringRef>(const StringRef& v, char* dest) {
    *(int32_t*)dest = cast_set<int32_t>(v.size);
    dest += 4;
    memcpy(dest, v.data, v.size);
    dest += v.size;
    return dest;
}

template <>
inline char* Helper::write_to<std::string>(const std::string& v, char* dest) {
    *(uint32_t*)dest = cast_set<uint32_t>(v.size());
    dest += 4;
    memcpy(dest, v.c_str(), v.size());
    dest += v.size();
    return dest;
}
// write_to end

template <>
inline int32_t Helper::serialize_size<VecDateTimeValue>(const VecDateTimeValue& v) {
    return Helper::DATETIME_PACKED_TIME_BYTE_SIZE + Helper::DATETIME_TYPE_BYTE_SIZE;
}

template <>
inline int32_t Helper::serialize_size<DecimalV2Value>(const DecimalV2Value& v) {
    return Helper::DECIMAL_BYTE_SIZE;
}

template <>
inline int32_t Helper::serialize_size<StringRef>(const StringRef& v) {
    return cast_set<int32_t>(v.size + 4);
}

template <>
inline int32_t Helper::serialize_size<std::string>(const std::string& v) {
    return cast_set<int32_t>(v.size() + 4);
}
// serialize_size end

template <>
inline bool Helper::read_from<VecDateTimeValue>(const char** src, size_t* remaining,
                                                VecDateTimeValue* result) {
    if (*remaining < (size_t)(DATETIME_PACKED_TIME_BYTE_SIZE + DATETIME_TYPE_BYTE_SIZE)) {
        return false;
    }
    result->from_packed_time(*(int64_t*)(*src));
    *src += DATETIME_PACKED_TIME_BYTE_SIZE;
    if (*(int*)(*src) == TIME_DATE) {
        result->cast_to_date();
    }
    *src += DATETIME_TYPE_BYTE_SIZE;
    *remaining -= (DATETIME_PACKED_TIME_BYTE_SIZE + DATETIME_TYPE_BYTE_SIZE);
    return true;
}

template <>
inline bool Helper::read_from<DecimalV2Value>(const char** src, size_t* remaining,
                                              DecimalV2Value* result) {
    if (*remaining < (size_t)DECIMAL_BYTE_SIZE) {
        return false;
    }
    __int128 v = 0;
    memcpy(&v, *src, DECIMAL_BYTE_SIZE);
    *src += DECIMAL_BYTE_SIZE;
    *remaining -= DECIMAL_BYTE_SIZE;
    *result = DecimalV2Value(v);
    return true;
}

template <>
inline bool Helper::read_from<StringRef>(const char** src, size_t* remaining, StringRef* result) {
    if (*remaining < 4) {
        return false;
    }
    int32_t length = *(int32_t*)(*src);
    *src += 4;
    *remaining -= 4;
    if (length < 0 || (size_t)length > *remaining) {
        return false;
    }
    *result = StringRef((char*)*src, length);
    *src += length;
    *remaining -= length;
    return true;
}

template <>
inline bool Helper::read_from<std::string>(const char** src, size_t* remaining,
                                           std::string* result) {
    if (*remaining < 4) {
        return false;
    }
    int32_t length = *(int32_t*)(*src);
    *src += 4;
    *remaining -= 4;
    if (length < 0 || (size_t)length > *remaining) {
        return false;
    }
    *result = std::string((char*)*src, length);
    *src += length;
    *remaining -= length;
    return true;
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

    explicit BitmapIntersect(const char* src, size_t maxbytes) { deserialize(src, maxbytes); }

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
        if (_bitmaps.empty()) {
            return result;
        }
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
        *(int32_t*)writer = cast_set<int32_t>(_bitmaps.size());
        writer += 4;
        for (auto& kv : _bitmaps) {
            writer = detail::Helper::write_to(kv.first, writer);
            kv.second.write_to(writer);
            writer += kv.second.getSizeInBytes();
        }
    }

    // Bounded deserialization. Returns true on success and false on any
    // truncated / malformed input. Never reads past `src + maxbytes`.
    bool deserialize(const char* src, size_t maxbytes) {
        const char* reader = src;
        size_t remaining = maxbytes;
        if (remaining < 4) {
            return false;
        }
        int32_t bitmaps_size = *(int32_t*)reader;
        reader += 4;
        remaining -= 4;
        if (bitmaps_size < 0) {
            return false;
        }
        for (int32_t i = 0; i < bitmaps_size; i++) {
            T key;
            if (!detail::Helper::read_from(&reader, &remaining, &key)) {
                return false;
            }
            BitmapValue bitmap;
            if (!bitmap.deserialize(reader, remaining)) {
                return false;
            }
            size_t consumed = bitmap.getSizeInBytes();
            if (consumed > remaining) {
                return false;
            }
            reader += consumed;
            remaining -= consumed;
            _bitmaps[key] = std::move(bitmap);
        }
        return true;
    }

protected:
    std::map<T, BitmapValue> _bitmaps;
};

template <>
struct BitmapIntersect<std::string_view> {
public:
    BitmapIntersect() = default;

    explicit BitmapIntersect(const char* src, size_t maxbytes) { deserialize(src, maxbytes); }

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
        *(int32_t*)writer = cast_set<int32_t>(_bitmaps.size());
        writer += 4;
        for (auto& kv : _bitmaps) {
            writer = detail::Helper::write_to(kv.first, writer);
            kv.second.write_to(writer);
            writer += kv.second.getSizeInBytes();
        }
    }

    bool deserialize(const char* src, size_t maxbytes) {
        const char* reader = src;
        size_t remaining = maxbytes;
        if (remaining < 4) {
            return false;
        }
        int32_t bitmaps_size = *(int32_t*)reader;
        reader += 4;
        remaining -= 4;
        if (bitmaps_size < 0) {
            return false;
        }
        for (int32_t i = 0; i < bitmaps_size; i++) {
            std::string key;
            if (!detail::Helper::read_from(&reader, &remaining, &key)) {
                return false;
            }
            BitmapValue bitmap;
            if (!bitmap.deserialize(reader, remaining)) {
                return false;
            }
            size_t consumed = bitmap.getSizeInBytes();
            if (consumed > remaining) {
                return false;
            }
            reader += consumed;
            remaining -= consumed;
            _bitmaps[key] = std::move(bitmap);
        }
        return true;
    }

protected:
    phmap::flat_hash_map<std::string, BitmapValue> _bitmaps;
};
} // namespace doris
