/*
 *  Copyright (c) 2014, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

/*
 * This header file defines miscellaneous utility classes.
 *
 * @author Tian Xia <tianx@fb.com>
 *
 *  this file is copied from 
 * https://github.com/facebook/mysql-5.6/blob/fb-mysql-5.6.35/fbson/FbsonUtil.h
 * and modified by Doris
 */

#ifndef JSONB_JSONBUTIL_H
#define JSONB_JSONBUTIL_H

#include "common/status.h"
#include "jsonb_document.h"
#include "jsonb_stream.h"

namespace doris {

#define OUT_BUF_SIZE 1024

/*
 * JsonbToJson converts an JsonbValue object to a JSON string.
 */
class JsonbToJson {
public:
    JsonbToJson() : os_(buffer_, OUT_BUF_SIZE) {}

    // get json string
    std::string to_json_string(const char* data, size_t size) {
        JsonbDocument* pdoc;
        THROW_IF_ERROR(doris::JsonbDocument::checkAndCreateDocument(data, size, &pdoc));
        return to_json_string(pdoc->getValue());
    }

    std::string to_json_string(const JsonbValue* val) {
        os_.clear();
        os_.seekp(0);

        if (val) {
            intern_json(val);
        }

        os_.put(0);

        std::string json_string {os_.getBuffer()};
        return json_string;
    }

    static std::string jsonb_to_json_string(const char* data, size_t size) {
        JsonbToJson jsonb_to_json;
        return jsonb_to_json.to_json_string(data, size);
    }

private:
    // recursively convert JsonbValue
    void intern_json(const JsonbValue* val) {
        switch (val->type) {
        case JsonbType::T_Null: {
            os_.write("null", 4);
            break;
        }
        case JsonbType::T_True: {
            os_.write("true", 4);
            break;
        }
        case JsonbType::T_False: {
            os_.write("false", 5);
            break;
        }
        case JsonbType::T_Int8: {
            os_.write(val->unpack<JsonbInt8Val>()->val());
            break;
        }
        case JsonbType::T_Int16: {
            os_.write(val->unpack<JsonbInt16Val>()->val());
            break;
        }
        case JsonbType::T_Int32: {
            os_.write(val->unpack<JsonbInt32Val>()->val());
            break;
        }
        case JsonbType::T_Int64: {
            os_.write(val->unpack<JsonbInt64Val>()->val());
            break;
        }
        case JsonbType::T_Double: {
            os_.write(val->unpack<JsonbDoubleVal>()->val());
            break;
        }
        case JsonbType::T_Float: {
            os_.write(val->unpack<JsonbFloatVal>()->val());
            break;
        }
        case JsonbType::T_Int128: {
            os_.write(val->unpack<JsonbInt128Val>()->val());
            break;
        }
        case JsonbType::T_String: {
            string_to_json(val->unpack<JsonbStringVal>()->getBlob(),
                           val->unpack<JsonbStringVal>()->length());
            break;
        }
        case JsonbType::T_Binary: {
            os_.write("\"<BINARY>", 9);
            os_.write(val->unpack<JsonbBinaryVal>()->getBlob(),
                      val->unpack<JsonbBinaryVal>()->getBlobLen());
            os_.write("<BINARY>\"", 9);
            break;
        }
        case JsonbType::T_Object: {
            object_to_json(val->unpack<ObjectVal>());
            break;
        }
        case JsonbType::T_Array: {
            array_to_json(val->unpack<ArrayVal>());
            break;
        }
        case JsonbType::T_Decimal32: {
            const auto* decimal_val = val->unpack<JsonbDecimal32>();
            decimal_to_json(vectorized::Decimal32 {decimal_val->val()}, decimal_val->precision,
                            decimal_val->scale);
            break;
        }
        case JsonbType::T_Decimal64: {
            const auto* decimal_val = val->unpack<JsonbDecimal64>();
            decimal_to_json(vectorized::Decimal64 {decimal_val->val()}, decimal_val->precision,
                            decimal_val->scale);
            break;
        }
        case JsonbType::T_Decimal128: {
            const auto* decimal_val = val->unpack<JsonbDecimal128>();
            decimal_to_json(vectorized::Decimal128V3 {decimal_val->val()}, decimal_val->precision,
                            decimal_val->scale);
            break;
        }
        case JsonbType::T_Decimal256: {
            const auto* decimal_val = val->unpack<JsonbDecimal256>();
            decimal_to_json(vectorized::Decimal256 {decimal_val->val()}, decimal_val->precision,
                            decimal_val->scale);
            break;
        }
        default:
            break;
        }
    }

    void string_to_json(const char* str, size_t len) {
        os_.put('"');
        if (nullptr == str) {
            os_.put('"');
            return;
        }
        char char_buffer[16];
        for (const char* ptr = str; ptr != str + len && *ptr; ++ptr) {
            if ((unsigned char)*ptr > 31 && *ptr != '\"' && *ptr != '\\') {
                os_.put(*ptr);
            } else {
                os_.put('\\');
                unsigned char token;
                switch (token = *ptr) {
                case '\\':
                    os_.put('\\');
                    break;
                case '\"':
                    os_.put('\"');
                    break;
                case '\b':
                    os_.put('b');
                    break;
                case '\f':
                    os_.put('f');
                    break;
                case '\n':
                    os_.put('n');
                    break;
                case '\r':
                    os_.put('r');
                    break;
                case '\t':
                    os_.put('t');
                    break;
                default: {
                    int char_num = snprintf(char_buffer, sizeof(char_buffer), "u%04x", token);
                    os_.write(char_buffer, char_num);
                    break;
                }
                }
            }
        }
        os_.put('"');
    }

    // convert object
    void object_to_json(const ObjectVal* val) {
        os_.put('{');

        auto iter = val->begin();
        auto iter_fence = val->end();

        while (iter < iter_fence) {
            // write key
            if (iter->klen()) {
                string_to_json(iter->getKeyStr(), iter->klen());
            } else {
                // NOTE: we use sMaxKeyId to represent an empty key. see jsonb_writer.h
                if (iter->getKeyId() == JsonbKeyValue::sMaxKeyId) {
                    string_to_json(nullptr, 0);
                } else {
                    os_.write(iter->getKeyId());
                }
            }
            os_.put(':');

            // convert value
            intern_json(iter->value());

            ++iter;
            if (iter != iter_fence) {
                os_.put(',');
            }
        }

        assert(iter == iter_fence);

        os_.put('}');
    }

    // convert array to json
    void array_to_json(const ArrayVal* val) {
        os_.put('[');

        auto iter = val->begin();
        auto iter_fence = val->end();

        while (iter != iter_fence) {
            // convert value
            intern_json((const JsonbValue*)iter);
            ++iter;
            if (iter != iter_fence) {
                os_.put(',');
            }
        }

        assert(iter == iter_fence);

        os_.put(']');
    }

    template <JsonbDecimalType T>
    void decimal_to_json(const T& value, const uint32_t precision, const uint32_t scale);

    JsonbOutStream os_;
    char buffer_[OUT_BUF_SIZE];
};

} // namespace doris

#endif // JSONB_JSONBUTIL_H
