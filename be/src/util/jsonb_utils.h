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

#include <sstream>

#include "jsonb_document.h"
#include "jsonb_stream.h"
#include "jsonb_writer.h"

namespace doris {

#define OUT_BUF_SIZE 1024

/*
 * JsonbToJson converts an JsonbValue object to a JSON string.
 */
class JsonbToJson {
public:
    JsonbToJson() : os_(buffer_, OUT_BUF_SIZE) {}

    // get json string
    const std::string to_json_string(const char* data, size_t size) {
        JsonbDocument* pdoc = doris::JsonbDocument::createDocument(data, size);
        if (!pdoc) {
            LOG(FATAL) << "invalid json binary value: " << std::string_view(data, size);
        }
        return to_json_string(pdoc->getValue());
    }

    const std::string to_json_string(const JsonbValue* val) {
        os_.clear();
        os_.seekp(0);

        if (val) {
            intern_json(val);
        }

        os_.put(0);

        std::string json_string {os_.getBuffer()};
        return json_string;
    }

    static const std::string jsonb_to_json_string(const char* data, size_t size) {
        JsonbToJson jsonb_to_json;
        return jsonb_to_json.to_json_string(data, size);
    }

private:
    // recursively convert JsonbValue
    void intern_json(const JsonbValue* val) {
        switch (val->type()) {
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
            os_.write(((JsonbInt8Val*)val)->val());
            break;
        }
        case JsonbType::T_Int16: {
            os_.write(((JsonbInt16Val*)val)->val());
            break;
        }
        case JsonbType::T_Int32: {
            os_.write(((JsonbInt32Val*)val)->val());
            break;
        }
        case JsonbType::T_Int64: {
            os_.write(((JsonbInt64Val*)val)->val());
            break;
        }
        case JsonbType::T_Double: {
            os_.write(((JsonbDoubleVal*)val)->val());
            break;
        }
        case JsonbType::T_Float: {
            os_.write(((JsonbFloatVal*)val)->val());
            break;
        }
        case JsonbType::T_Int128: {
            os_.write(((JsonbInt128Val*)val)->val());
            break;
        }
        case JsonbType::T_String: {
            string_to_json(((JsonbStringVal*)val)->getBlob(), ((JsonbStringVal*)val)->length());
            break;
        }
        case JsonbType::T_Binary: {
            os_.write("\"<BINARY>", 9);
            os_.write(((JsonbBinaryVal*)val)->getBlob(), ((JsonbBinaryVal*)val)->getBlobLen());
            os_.write("<BINARY>\"", 9);
            break;
        }
        case JsonbType::T_Object: {
            object_to_json((ObjectVal*)val);
            break;
        }
        case JsonbType::T_Array: {
            array_to_json((ArrayVal*)val);
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
            if ((unsigned char)*ptr > 31 && *ptr != '\"' && *ptr != '\\')
                os_.put(*ptr);
            else {
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

private:
    JsonbOutStream os_;
    char buffer_[OUT_BUF_SIZE];
};

// This class is a utility to create a JsonbValue.
template <class OS_TYPE>
class JsonbValueCreaterT {
public:
    JsonbValue* operator()(int32_t val) { return operator()((int64_t)val); }

    JsonbValue* operator()(int64_t val) {
        writer_.reset();
        writer_.writeStartArray();
        writer_.writeInt(val);
        writer_.writeEndArray();
        return extractValue();
    }
    JsonbValue* operator()(double val) {
        writer_.reset();
        writer_.writeStartArray();
        writer_.writeDouble(val);
        writer_.writeEndArray();
        return extractValue();
    }
    JsonbValue* operator()(const char* str) { return operator()(str, (unsigned int)strlen(str)); }
    JsonbValue* operator()(const char* str, unsigned int str_len) {
        writer_.reset();
        writer_.writeStartArray();
        writer_.writeStartString();
        writer_.writeString(str, str_len);
        writer_.writeEndString();
        writer_.writeEndArray();
        return extractValue();
    }
    JsonbValue* operator()(bool val) {
        writer_.reset();
        writer_.writeStartArray();
        writer_.writeBool(val);
        writer_.writeEndArray();
        return extractValue();
    }
    JsonbValue* operator()() {
        writer_.reset();
        writer_.writeStartArray();
        writer_.writeNull();
        writer_.writeEndArray();
        return extractValue();
    }

private:
    JsonbValue* extractValue() {
        return static_cast<ArrayVal*>(
                       JsonbDocument::createValue(writer_.getOutput()->getBuffer(),
                                                  (int)writer_.getOutput()->getSize()))
                ->get(0);
    }
    JsonbWriterT<OS_TYPE> writer_;
};
typedef JsonbValueCreaterT<JsonbOutStream> JsonbValueCreater;
} // namespace doris

#endif // JSONB_JSONBUTIL_H
