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

#include <sstream>
#include <boost/functional/hash.hpp>

#include "runtime/raw_value.h"
#include "runtime/string_value.hpp"
#include "runtime/tuple.h"
#include "olap/utils.h"
#include "util/types.h"

namespace doris {

const int RawValue::ASCII_PRECISION = 16; // print 16 digits for double/float

void RawValue::print_value_as_bytes(const void* value, const TypeDescriptor& type,
                                 std::stringstream* stream) {
    if (value == NULL) {
        return;
    }

    const char* chars = reinterpret_cast<const char*>(value);
    const StringValue* string_val = NULL;

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
        string_val = reinterpret_cast<const StringValue*>(value);
        stream->write(static_cast<char*>(string_val->ptr), string_val->len);
        return;

    case TYPE_DATE:
    case TYPE_DATETIME:
        stream->write(chars, sizeof(DateTimeValue));
        break;

    case TYPE_DECIMAL:
        stream->write(chars, sizeof(DecimalValue));
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
    if (value == NULL) {
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
    const StringValue* string_val = NULL;

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
        string_val = reinterpret_cast<const StringValue*>(value);
        tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
        *stream << tmp;
        return;

    case TYPE_DATE:
    case TYPE_DATETIME:
        *stream << *reinterpret_cast<const DateTimeValue*>(value);
        break;

    case TYPE_DECIMAL:
        *stream << *reinterpret_cast<const DecimalValue*>(value);
        break;

    case TYPE_DECIMALV2:
        *stream << reinterpret_cast<const PackedInt128*>(value)->value;
        break;

    case TYPE_LARGEINT:
        *stream << reinterpret_cast<const PackedInt128*>(value)->value;
        break;

    default:
        DCHECK(false) << "bad RawValue::print_value() type: " << type;
    }

    stream->precision(old_precision);
    // Undo setting stream to fixed
    stream->flags(old_flags);
}

void RawValue::print_value(const void* value, const TypeDescriptor& type, int scale,
                          std::string* str) {
    if (value == NULL) {
        *str = "NULL";
        return;
    }

    std::stringstream out;
    out.precision(ASCII_PRECISION);
    const StringValue* string_val = NULL;
    std::string tmp;
    bool val = false;

    // Special case types that we can print more efficiently without using a stringstream
    switch (type.type) {
    case TYPE_BOOLEAN:
        val = *reinterpret_cast<const bool*>(value);
        *str = (val ? "true" : "false");
        return;

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_OBJECT:
    case TYPE_HLL: {
        string_val = reinterpret_cast<const StringValue*>(value);
        std::stringstream ss;
        ss << "ptr:" << (void*)string_val->ptr << " len" << string_val->len;
        tmp = ss.str();
        if (string_val->len <= 1000) {
            tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
        }
        str->swap(tmp);
        return;
    }

    default:
        print_value(value, type, scale, &out);
    }

    *str = out.str();
}

void RawValue::write(const void* value, void* dst, const TypeDescriptor& type, MemPool* pool) {
    DCHECK(value != NULL);

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
        *reinterpret_cast<DateTimeValue*>(dst) =
            *reinterpret_cast<const DateTimeValue*>(value);
        break;

    case TYPE_DECIMAL:
        *reinterpret_cast<DecimalValue*>(dst) =
                *reinterpret_cast<const DecimalValue*>(value);
        break;

    case TYPE_DECIMALV2:
        *reinterpret_cast<PackedInt128*>(dst) = *reinterpret_cast<const PackedInt128*>(value);
        break;

    case TYPE_OBJECT:
    case TYPE_HLL:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        const StringValue* src = reinterpret_cast<const StringValue*>(value);
        StringValue* dest = reinterpret_cast<StringValue*>(dst);
        dest->len = src->len;

        if (pool != NULL) {
            dest->ptr = reinterpret_cast<char*>(pool->allocate(dest->len));
            memcpy(dest->ptr, src->ptr, dest->len);
        } else {
            dest->ptr = src->ptr;
        }

        break;
    }

    default:
        DCHECK(false) << "RawValue::write(): bad type: " << type;
    }
}

// TODO: can we remove some of this code duplication? Templated allocator?
void RawValue::write(const void* value, const TypeDescriptor& type, void* dst, uint8_t** buf) {
    DCHECK(value != NULL);
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
            *reinterpret_cast<DateTimeValue*>(dst) =
                *reinterpret_cast<const DateTimeValue*>(value);
            break;
        case TYPE_VARCHAR:
        case TYPE_CHAR: {
            DCHECK(buf != NULL);
            const StringValue* src = reinterpret_cast<const StringValue*>(value);
            StringValue* dest = reinterpret_cast<StringValue*>(dst);
            dest->len = src->len;
            dest->ptr = reinterpret_cast<char*>(*buf);
            memcpy(dest->ptr, src->ptr, dest->len);
            *buf += dest->len;
            break;
        }
        case TYPE_DECIMAL:
            *reinterpret_cast<DecimalValue*>(dst) = *reinterpret_cast<const DecimalValue*>(value);
            break;

        case TYPE_DECIMALV2:
            *reinterpret_cast<PackedInt128*>(dst) = *reinterpret_cast<const PackedInt128*>(value);
            break;

        default:
            DCHECK(false) << "RawValue::write(): bad type: " << type.debug_string();
    }
}

void RawValue::write(const void* value, Tuple* tuple, const SlotDescriptor* slot_desc,
                     MemPool* pool) {
    if (value == NULL) {
        tuple->set_null(slot_desc->null_indicator_offset());
    } else {
        void* slot = tuple->get_slot(slot_desc->tuple_offset());
        RawValue::write(value, slot, slot_desc->type(), pool);
    }
}

}
