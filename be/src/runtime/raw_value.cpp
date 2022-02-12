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

#include "runtime/raw_value.h"

#include <sstream>

#include "olap/utils.h"
#include "runtime/collection_value.h"
#include "runtime/string_value.hpp"
#include "runtime/tuple.h"
#include "util/types.h"

namespace doris {

const int RawValue::ASCII_PRECISION = 16; // print 16 digits for double/float

void RawValue::print_value_as_bytes(const void* value, const TypeDescriptor& type,
                                    std::stringstream* stream) {
    if (value == nullptr) {
        return;
    }

    const char* chars = reinterpret_cast<const char*>(value);
    const StringValue* string_val = nullptr;

    switch (type.type) {
    case TYPE_NULL:
        break;
    case TYPE_BOOLEAN:
        stream->write(chars, sizeof(bool));
        return;

    case TYPE_TINYINT:
        stream->write(chars, sizeof(int8_t));
        break;

    case TYPE_SMALLINT:
        stream->write(chars, sizeof(int16_t));
        break;

    case TYPE_INT:
        stream->write(chars, sizeof(int32_t));
        break;

    case TYPE_BIGINT:
        stream->write(chars, sizeof(int64_t));
        break;

    case TYPE_FLOAT:
        stream->write(chars, sizeof(float));
        break;

    case TYPE_DOUBLE:
        stream->write(chars, sizeof(double));
        break;

    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_CHAR:
    case TYPE_STRING:
        string_val = reinterpret_cast<const StringValue*>(value);
        stream->write(static_cast<char*>(string_val->ptr), string_val->len);
        return;

    case TYPE_DATE:
    case TYPE_DATETIME:
        stream->write(chars, sizeof(DateTimeValue));
        break;

    case TYPE_DECIMALV2:
        stream->write(chars, sizeof(DecimalV2Value));
        break;

    case TYPE_LARGEINT:
        stream->write(chars, sizeof(__int128));
        break;

    default:
        DCHECK(false) << "bad RawValue::print_value() type: " << type;
    }
}

void RawValue::print_value(const void* value, const TypeDescriptor& type, int scale,
                           std::stringstream* stream) {
    if (value == nullptr) {
        *stream << "NULL";
        return;
    }

    int old_precision = stream->precision();
    std::ios_base::fmtflags old_flags = stream->flags();

    if (scale > -1) {
        stream->precision(scale);
        // Setting 'fixed' causes precision to set the number of digits printed after the
        // decimal (by default it sets the maximum number of digits total).
        *stream << std::fixed;
    }

    std::string tmp;
    const StringValue* string_val = nullptr;

    switch (type.type) {
    case TYPE_BOOLEAN: {
        bool val = *reinterpret_cast<const bool*>(value);
        *stream << (val ? "true" : "false");
        return;
    }

    case TYPE_TINYINT:
        // Extra casting for chars since they should not be interpreted as ASCII.
        *stream << static_cast<int>(*reinterpret_cast<const int8_t*>(value));
        break;

    case TYPE_SMALLINT:
        *stream << *reinterpret_cast<const int16_t*>(value);
        break;

    case TYPE_INT:
        *stream << *reinterpret_cast<const int32_t*>(value);
        break;

    case TYPE_BIGINT:
        *stream << *reinterpret_cast<const int64_t*>(value);
        break;

    case TYPE_FLOAT:
        *stream << *reinterpret_cast<const float*>(value);
        break;

    case TYPE_DOUBLE:
        *stream << *reinterpret_cast<const double*>(value);
        break;
    case TYPE_HLL:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        string_val = reinterpret_cast<const StringValue*>(value);
        tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
        *stream << tmp;
        return;

    case TYPE_DATE:
    case TYPE_DATETIME:
        *stream << *reinterpret_cast<const DateTimeValue*>(value);
        break;

    case TYPE_DECIMALV2:
        *stream << DecimalV2Value(reinterpret_cast<const PackedInt128*>(value)->value).to_string();
        break;

    case TYPE_LARGEINT:
        *stream << reinterpret_cast<const PackedInt128*>(value)->value;
        break;

    case TYPE_ARRAY: {
        const CollectionValue* src = reinterpret_cast<const CollectionValue*>(value);
        auto children_type = type.children.at(0);
        auto iter = src->iterator(children_type.type);
        *stream << "[";
        print_value(iter.value(), children_type, scale, stream);
        iter.next();
        for (; iter.has_next(); iter.next()) {
            *stream << ", ";
            print_value(iter.value(), children_type, scale, stream);
        }
        *stream << "]";
        break;
    }

    default:
        DCHECK(false) << "bad RawValue::print_value() type: " << type;
    }

    stream->precision(old_precision);
    // Undo setting stream to fixed
    stream->flags(old_flags);
}

void RawValue::print_value(const void* value, const TypeDescriptor& type, int scale,
                           std::string* str) {
    if (value == nullptr) {
        *str = "NULL";
        return;
    }

    std::stringstream out;
    out.precision(ASCII_PRECISION);
    const StringValue* string_val = nullptr;
    std::string tmp;
    bool val = false;

    // Special case types that we can print more efficiently without using a std::stringstream
    switch (type.type) {
    case TYPE_BOOLEAN:
        val = *reinterpret_cast<const bool*>(value);
        *str = (val ? "true" : "false");
        return;

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_OBJECT:
    case TYPE_HLL:
    case TYPE_STRING: {
        string_val = reinterpret_cast<const StringValue*>(value);
        std::stringstream ss;
        ss << "ptr:" << (void*)string_val->ptr << " len:" << string_val->len;
        tmp = ss.str();
        if (string_val->len <= 1000) {
            tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
        }
        str->swap(tmp);
        return;
    }
    case TYPE_NULL: {
        *str = "NULL";
        return;
    }
    default:
        print_value(value, type, scale, &out);
    }

    *str = out.str();
}

void RawValue::write(const void* value, void* dst, const TypeDescriptor& type, MemPool* pool) {
    DCHECK(value != nullptr);

    switch (type.type) {
    case TYPE_NULL:
        break;
    case TYPE_BOOLEAN: {
        *reinterpret_cast<bool*>(dst) = *reinterpret_cast<const bool*>(value);
        break;
    }

    case TYPE_TINYINT: {
        *reinterpret_cast<int8_t*>(dst) = *reinterpret_cast<const int8_t*>(value);
        break;
    }

    case TYPE_SMALLINT: {
        *reinterpret_cast<int16_t*>(dst) = *reinterpret_cast<const int16_t*>(value);
        break;
    }

    case TYPE_INT: {
        *reinterpret_cast<int32_t*>(dst) = *reinterpret_cast<const int32_t*>(value);
        break;
    }

    case TYPE_BIGINT: {
        *reinterpret_cast<int64_t*>(dst) = *reinterpret_cast<const int64_t*>(value);
        break;
    }

    case TYPE_LARGEINT: {
        *reinterpret_cast<PackedInt128*>(dst) = *reinterpret_cast<const PackedInt128*>(value);
        break;
    }

    case TYPE_FLOAT: {
        *reinterpret_cast<float*>(dst) = *reinterpret_cast<const float*>(value);
        break;
    }

    case TYPE_TIME:
    case TYPE_DOUBLE: {
        *reinterpret_cast<double*>(dst) = *reinterpret_cast<const double*>(value);
        break;
    }

    case TYPE_DATE:
    case TYPE_DATETIME:
        *reinterpret_cast<DateTimeValue*>(dst) = *reinterpret_cast<const DateTimeValue*>(value);
        break;

    case TYPE_DECIMALV2:
        *reinterpret_cast<PackedInt128*>(dst) = *reinterpret_cast<const PackedInt128*>(value);
        break;

    case TYPE_OBJECT:
    case TYPE_HLL:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_STRING: {
        const StringValue* src = reinterpret_cast<const StringValue*>(value);
        StringValue* dest = reinterpret_cast<StringValue*>(dst);
        dest->len = src->len;

        if (pool != nullptr) {
            dest->ptr = reinterpret_cast<char*>(pool->allocate(dest->len));
            memcpy(dest->ptr, src->ptr, dest->len);
        } else {
            dest->ptr = src->ptr;
        }

        break;
    }
    case TYPE_ARRAY: {
        DCHECK_EQ(type.children.size(), 1);

        const CollectionValue* src = reinterpret_cast<const CollectionValue*>(value);
        CollectionValue* val = reinterpret_cast<CollectionValue*>(dst);

        if (pool != nullptr) {
            auto children_type = type.children.at(0).type;
            CollectionValue::init_collection(pool, src->size(), children_type, val);
            ArrayIterator src_iter = src->iterator(children_type);
            ArrayIterator val_iter = val->iterator(children_type);

            val->copy_null_signs(src);

            while (src_iter.has_next() && val_iter.has_next()) {
                if (!src_iter.is_null()) {
                    // write children
                    write(src_iter.value(), val_iter.value(), children_type, pool);
                }
                src_iter.next();
                val_iter.next();
            }
        } else {
            val->shallow_copy(src);
        }
        break;
    }
    default:
        DCHECK(false) << "RawValue::write(): bad type: " << type;
    }
}

// TODO: can we remove some of this code duplication? Templated allocator?
void RawValue::write(const void* value, const TypeDescriptor& type, void* dst, uint8_t** buf) {
    DCHECK(value != nullptr);
    switch (type.type) {
    case TYPE_BOOLEAN:
        *reinterpret_cast<bool*>(dst) = *reinterpret_cast<const bool*>(value);
        break;
    case TYPE_TINYINT:
        *reinterpret_cast<int8_t*>(dst) = *reinterpret_cast<const int8_t*>(value);
        break;
    case TYPE_SMALLINT:
        *reinterpret_cast<int16_t*>(dst) = *reinterpret_cast<const int16_t*>(value);
        break;
    case TYPE_INT:
        *reinterpret_cast<int32_t*>(dst) = *reinterpret_cast<const int32_t*>(value);
        break;
    case TYPE_BIGINT:
        *reinterpret_cast<int64_t*>(dst) = *reinterpret_cast<const int64_t*>(value);
        break;
    case TYPE_LARGEINT:
        *reinterpret_cast<PackedInt128*>(dst) = *reinterpret_cast<const PackedInt128*>(value);
        break;
    case TYPE_FLOAT:
        *reinterpret_cast<float*>(dst) = *reinterpret_cast<const float*>(value);
        break;
    case TYPE_DOUBLE:
        *reinterpret_cast<double*>(dst) = *reinterpret_cast<const double*>(value);
        break;
    case TYPE_DATE:
    case TYPE_DATETIME:
        *reinterpret_cast<DateTimeValue*>(dst) = *reinterpret_cast<const DateTimeValue*>(value);
        break;
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_STRING: {
        DCHECK(buf != nullptr);
        const StringValue* src = reinterpret_cast<const StringValue*>(value);
        StringValue* dest = reinterpret_cast<StringValue*>(dst);
        dest->len = src->len;
        dest->ptr = reinterpret_cast<char*>(*buf);
        memcpy(dest->ptr, src->ptr, dest->len);
        *buf += dest->len;
        break;
    }

    case TYPE_DECIMALV2:
        *reinterpret_cast<PackedInt128*>(dst) = *reinterpret_cast<const PackedInt128*>(value);
        break;

    default:
        DCHECK(false) << "RawValue::write(): bad type: " << type.debug_string();
    }
}

void RawValue::write(const void* value, Tuple* tuple, const SlotDescriptor* slot_desc,
                     MemPool* pool) {
    if (value == nullptr) {
        tuple->set_null(slot_desc->null_indicator_offset());
    } else {
        void* slot = tuple->get_slot(slot_desc->tuple_offset());
        RawValue::write(value, slot, slot_desc->type(), pool);
    }
}

int RawValue::compare(const void* v1, const void* v2, const TypeDescriptor& type) {
    const StringValue* string_value1;
    const StringValue* string_value2;
    const DateTimeValue* ts_value1;
    const DateTimeValue* ts_value2;
    float f1 = 0;
    float f2 = 0;
    double d1 = 0;
    double d2 = 0;
    int32_t i1;
    int32_t i2;
    int64_t b1;
    int64_t b2;

    if (nullptr == v1 && nullptr == v2) {
        return 0;
    } else if (nullptr == v1 && nullptr != v2) {
        return -1;
    } else if (nullptr != v1 && nullptr == v2) {
        return 1;
    }

    switch (type.type) {
    case TYPE_NULL:
        return 0;

    case TYPE_BOOLEAN:
        return *reinterpret_cast<const bool*>(v1) - *reinterpret_cast<const bool*>(v2);

    case TYPE_TINYINT:
        return *reinterpret_cast<const int8_t*>(v1) - *reinterpret_cast<const int8_t*>(v2);

    case TYPE_SMALLINT:
        return *reinterpret_cast<const int16_t*>(v1) - *reinterpret_cast<const int16_t*>(v2);

    case TYPE_INT:
        i1 = *reinterpret_cast<const int32_t*>(v1);
        i2 = *reinterpret_cast<const int32_t*>(v2);
        return i1 > i2 ? 1 : (i1 < i2 ? -1 : 0);

    case TYPE_BIGINT:
        b1 = *reinterpret_cast<const int64_t*>(v1);
        b2 = *reinterpret_cast<const int64_t*>(v2);
        return b1 > b2 ? 1 : (b1 < b2 ? -1 : 0);

    case TYPE_FLOAT:
        // TODO: can this be faster? (just returning the difference has underflow problems)
        f1 = *reinterpret_cast<const float*>(v1);
        f2 = *reinterpret_cast<const float*>(v2);
        return f1 > f2 ? 1 : (f1 < f2 ? -1 : 0);

    case TYPE_DOUBLE:
        // TODO: can this be faster?
        d1 = *reinterpret_cast<const double*>(v1);
        d2 = *reinterpret_cast<const double*>(v2);
        return d1 > d2 ? 1 : (d1 < d2 ? -1 : 0);

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_STRING:
        string_value1 = reinterpret_cast<const StringValue*>(v1);
        string_value2 = reinterpret_cast<const StringValue*>(v2);
        return string_value1->compare(*string_value2);

    case TYPE_DATE:
    case TYPE_DATETIME:
        ts_value1 = reinterpret_cast<const DateTimeValue*>(v1);
        ts_value2 = reinterpret_cast<const DateTimeValue*>(v2);
        return *ts_value1 > *ts_value2 ? 1 : (*ts_value1 < *ts_value2 ? -1 : 0);

    case TYPE_DECIMALV2: {
        DecimalV2Value decimal_value1(reinterpret_cast<const PackedInt128*>(v1)->value);
        DecimalV2Value decimal_value2(reinterpret_cast<const PackedInt128*>(v2)->value);
        return (decimal_value1 > decimal_value2) ? 1 : (decimal_value1 < decimal_value2 ? -1 : 0);
    }

    case TYPE_LARGEINT: {
        __int128 large_int_value1 = reinterpret_cast<const PackedInt128*>(v1)->value;
        __int128 large_int_value2 = reinterpret_cast<const PackedInt128*>(v2)->value;
        return large_int_value1 > large_int_value2 ? 1
                                                   : (large_int_value1 < large_int_value2 ? -1 : 0);
    }

    default:
        DCHECK(false) << "invalid type: " << type.type;
        return 0;
    };
}

} // namespace doris
