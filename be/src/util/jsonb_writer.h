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
 * This file defines JsonbWriterT (template) and JsonbWriter.
 *
 * JsonbWriterT is a template class which implements an JSONB serializer.
 * Users call various write functions of JsonbWriterT object to write values
 * directly to JSONB packed bytes. All write functions of value or key return
 * the number of bytes written to JSONB, or 0 if there is an error. To write an
 * object, an array, or a string, you must call writeStart[..] before writing
 * values or key, and call writeEnd[..] after finishing at the end.
 *
 * By default, an JsonbWriterT object creates an output stream buffer.
 * Alternatively, you can also pass any output stream object to a writer, as
 * long as the stream object implements some basic functions of std::ostream
 * (such as JsonbOutStream, see JsonbStream.h).
 *
 * JsonbWriter specializes JsonbWriterT with JsonbOutStream type (see
 * JsonbStream.h). So unless you want to provide own a different output stream
 * type, use JsonbParser object.
 *
 * @author Tian Xia <tianx@fb.com>
 * this file is copied from 
 * https://github.com/facebook/mysql-5.6/blob/fb-mysql-5.6.35/fbson/FbsonWriter.h
 * and modified by Doris
 */

#ifndef JSONB_JSONBWRITER_H
#define JSONB_JSONBWRITER_H

#include <limits>
#include <stack>
#include <string>

#include "jsonb_document.h"
#include "jsonb_stream.h"

namespace doris {

using int128_t = __int128;

template <class OS_TYPE>
class JsonbWriterT {
public:
    JsonbWriterT() : alloc_(true), hasHdr_(false), kvState_(WS_Value), str_pos_(0) {
        os_ = new OS_TYPE();
    }

    explicit JsonbWriterT(OS_TYPE& os)
            : os_(&os), alloc_(false), hasHdr_(false), kvState_(WS_Value), str_pos_(0) {}

    ~JsonbWriterT() {
        if (alloc_) {
            delete os_;
        }
    }

    void reset() {
        os_->clear();
        os_->seekp(0);
        hasHdr_ = false;
        kvState_ = WS_Value;
        first_ = true;
        for (; !stack_.empty(); stack_.pop())
            ;
    }

    uint32_t writeKey(const char* key, hDictInsert handler = nullptr) {
        return writeKey(key, strlen(key), handler);
    }

    // write a key string (or key id if an external dict is provided)
    uint32_t writeKey(const char* key, uint8_t len, hDictInsert handler = nullptr) {
        if (!stack_.empty() && verifyKeyState()) {
            int key_id = -1;
            if (handler) {
                key_id = handler(key, len);
            }

            uint32_t size = sizeof(uint8_t);
            if (key_id < 0) {
                os_->put(len);
                if (len == 0) {
                    // NOTE: we use sMaxKeyId to represent an empty key
                    JsonbKeyValue::keyid_type idx = JsonbKeyValue::sMaxKeyId;
                    os_->write((char*)&idx, sizeof(JsonbKeyValue::keyid_type));
                    size += sizeof(JsonbKeyValue::keyid_type);
                } else {
                    os_->write(key, len);
                    size += len;
                }
            } else if (key_id < JsonbKeyValue::sMaxKeyId) {
                JsonbKeyValue::keyid_type idx = key_id;
                os_->put(0);
                os_->write((char*)&idx, sizeof(JsonbKeyValue::keyid_type));
                size += sizeof(JsonbKeyValue::keyid_type);
            } else { // key id overflow
                assert(0);
                return 0;
            }

            kvState_ = WS_Key;
            return size;
        }

        return 0;
    }

    uint32_t writeValue(const JsonbValue* value) {
        if ((first_ && stack_.empty()) || (!stack_.empty() && verifyValueState())) {
            if (!writeFirstHeader()) return 0;
            os_->write((char*)value, value->numPackedBytes());
            kvState_ = WS_Value;
            return value->size();
        }
        return 0;
    }

    // write a key id
    uint32_t writeKey(JsonbKeyValue::keyid_type idx) {
        if (!stack_.empty() && verifyKeyState()) {
            os_->put(0);
            os_->write((char*)&idx, sizeof(JsonbKeyValue::keyid_type));
            kvState_ = WS_Key;
            return sizeof(uint8_t) + sizeof(JsonbKeyValue::keyid_type);
        }

        return 0;
    }

    bool writeFirstHeader() {
        if (first_ && stack_.empty()) {
            first_ = false;
            // if this is a new JSONB, write the header
            if (!hasHdr_) {
                writeHeader();
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    uint32_t writeNull() {
        if ((first_ && stack_.empty()) || (!stack_.empty() && verifyValueState())) {
            if (!writeFirstHeader()) return 0;
            os_->put((JsonbTypeUnder)JsonbType::T_Null);
            kvState_ = WS_Value;
            return sizeof(JsonbValue);
        }

        return 0;
    }

    uint32_t writeBool(bool b) {
        if ((first_ && stack_.empty()) || (!stack_.empty() && verifyValueState())) {
            if (!writeFirstHeader()) return 0;
            if (b) {
                os_->put((JsonbTypeUnder)JsonbType::T_True);
            } else {
                os_->put((JsonbTypeUnder)JsonbType::T_False);
            }

            kvState_ = WS_Value;
            return sizeof(JsonbValue);
        }

        return 0;
    }

    // This function is a helper. It will make use of smallest space to
    // write an int
    uint32_t writeInt(int64_t val) {
        if (val >= std::numeric_limits<int8_t>::min() &&
            val <= std::numeric_limits<int8_t>::max()) {
            return writeInt8((int8_t)val);
        } else if (val >= std::numeric_limits<int16_t>::min() &&
                   val <= std::numeric_limits<int16_t>::max()) {
            return writeInt16((int16_t)val);
        } else if (val >= std::numeric_limits<int32_t>::min() &&
                   val <= std::numeric_limits<int32_t>::max()) {
            return writeInt32((int32_t)val);
        } else {
            return writeInt64(val);
        }
    }

    uint32_t writeInt8(int8_t v) {
        if ((first_ && stack_.empty()) || (!stack_.empty() && verifyValueState())) {
            if (!writeFirstHeader()) return 0;
            os_->put((JsonbTypeUnder)JsonbType::T_Int8);
            os_->put(v);
            kvState_ = WS_Value;
            return sizeof(JsonbInt8Val);
        }

        return 0;
    }

    uint32_t writeInt16(int16_t v) {
        if ((first_ && stack_.empty()) || (!stack_.empty() && verifyValueState())) {
            if (!writeFirstHeader()) return 0;
            os_->put((JsonbTypeUnder)JsonbType::T_Int16);
            os_->write((char*)&v, sizeof(int16_t));
            kvState_ = WS_Value;
            return sizeof(JsonbInt16Val);
        }

        return 0;
    }

    uint32_t writeInt32(int32_t v) {
        if ((first_ && stack_.empty()) || (!stack_.empty() && verifyValueState())) {
            if (!writeFirstHeader()) return 0;
            os_->put((JsonbTypeUnder)JsonbType::T_Int32);
            os_->write((char*)&v, sizeof(int32_t));
            kvState_ = WS_Value;
            return sizeof(JsonbInt32Val);
        }

        return 0;
    }

    uint32_t writeInt64(int64_t v) {
        if ((first_ && stack_.empty()) || (!stack_.empty() && verifyValueState())) {
            if (!writeFirstHeader()) return 0;
            os_->put((JsonbTypeUnder)JsonbType::T_Int64);
            os_->write((char*)&v, sizeof(int64_t));
            kvState_ = WS_Value;
            return sizeof(JsonbInt64Val);
        }

        return 0;
    }

    uint32_t writeInt128(int128_t v) {
        if ((first_ && stack_.empty()) || (!stack_.empty() && verifyValueState())) {
            if (!writeFirstHeader()) return 0;
            os_->put((JsonbTypeUnder)JsonbType::T_Int128);
            os_->write((char*)&v, sizeof(int128_t));
            kvState_ = WS_Value;
            return sizeof(JsonbInt128Val);
        }

        return 0;
    }

    uint32_t writeDouble(double v) {
        if ((first_ && stack_.empty()) || (!stack_.empty() && verifyValueState())) {
            if (!writeFirstHeader()) return 0;
            os_->put((JsonbTypeUnder)JsonbType::T_Double);
            os_->write((char*)&v, sizeof(double));
            kvState_ = WS_Value;
            return sizeof(JsonbDoubleVal);
        }

        return 0;
    }

    uint32_t writeFloat(float v) {
        if ((first_ && stack_.empty()) || (!stack_.empty() && verifyValueState())) {
            if (!writeFirstHeader()) return 0;
            os_->put((JsonbTypeUnder)JsonbType::T_Float);
            os_->write((char*)&v, sizeof(float));
            kvState_ = WS_Value;
            return sizeof(JsonbFloatVal);
        }

        return 0;
    }

    // must call writeStartString before writing a string val
    bool writeStartString() {
        if ((first_ && stack_.empty()) || (!stack_.empty() && verifyValueState())) {
            if (!writeFirstHeader()) return 0;
            os_->put((JsonbTypeUnder)JsonbType::T_String);
            str_pos_ = os_->tellp();

            // fill the size bytes with 0 for now
            uint32_t size = 0;
            os_->write((char*)&size, sizeof(uint32_t));

            kvState_ = WS_String;
            return true;
        }

        return false;
    }

    // finish writing a string val
    bool writeEndString() {
        if (kvState_ == WS_String) {
            std::streampos cur_pos = os_->tellp();
            int32_t size = (int32_t)(cur_pos - str_pos_ - sizeof(uint32_t));
            assert(size >= 0);

            os_->seekp(str_pos_);
            os_->write((char*)&size, sizeof(uint32_t));
            os_->seekp(cur_pos);

            kvState_ = WS_Value;
            return true;
        }

        return false;
    }

    uint32_t writeString(const char* str, uint32_t len) {
        if (kvState_ == WS_String) {
            os_->write(str, len);
            return len;
        }

        return 0;
    }

    uint32_t writeString(const std::string& str) {
        return writeString(str.c_str(), (uint32_t)str.size());
    }
    uint32_t writeString(char ch) {
        if (kvState_ == WS_String) {
            os_->put(ch);
            return 1;
        }

        return 0;
    }

    // must call writeStartBinary before writing a binary val
    bool writeStartBinary() {
        if ((first_ && stack_.empty()) || (!stack_.empty() && verifyValueState())) {
            if (!writeFirstHeader()) return 0;
            os_->put((JsonbTypeUnder)JsonbType::T_Binary);
            str_pos_ = os_->tellp();

            // fill the size bytes with 0 for now
            uint32_t size = 0;
            os_->write((char*)&size, sizeof(uint32_t));

            kvState_ = WS_Binary;
            return true;
        }

        return false;
    }

    // finish writing a binary val
    bool writeEndBinary() {
        if (kvState_ == WS_Binary) {
            std::streampos cur_pos = os_->tellp();
            int32_t size = (int32_t)(cur_pos - str_pos_ - sizeof(uint32_t));
            assert(size >= 0);

            os_->seekp(str_pos_);
            os_->write((char*)&size, sizeof(uint32_t));
            os_->seekp(cur_pos);

            kvState_ = WS_Value;
            return true;
        }

        return false;
    }

    uint32_t writeBinary(const char* bin, uint32_t len) {
        if (kvState_ == WS_Binary) {
            os_->write(bin, len);
            return len;
        }

        return 0;
    }

    // must call writeStartObject before writing an object val
    bool writeStartObject() {
        if (stack_.empty() || verifyValueState()) {
            if (stack_.empty()) {
                // if this is a new JSONB, write the header
                if (!hasHdr_) {
                    writeHeader();
                } else
                    return false;
            }

            // check if the object exceeds the maximum nesting level
            if (stack_.size() >= MaxNestingLevel) return false;

            os_->put((JsonbTypeUnder)JsonbType::T_Object);
            // save the size position
            stack_.push(WriteInfo({WS_Object, os_->tellp()}));

            // fill the size bytes with 0 for now
            uint32_t size = 0;
            os_->write((char*)&size, sizeof(uint32_t));

            kvState_ = WS_Value;
            return true;
        }

        return false;
    }

    // finish writing an object val
    bool writeEndObject() {
        if (!stack_.empty() && stack_.top().state == WS_Object && kvState_ == WS_Value) {
            WriteInfo& ci = stack_.top();
            std::streampos cur_pos = os_->tellp();
            int32_t size = (int32_t)(cur_pos - ci.sz_pos - sizeof(uint32_t));
            assert(size >= 0);

            os_->seekp(ci.sz_pos);
            os_->write((char*)&size, sizeof(uint32_t));
            os_->seekp(cur_pos);
            stack_.pop();

            return true;
        }

        return false;
    }

    // must call writeStartArray before writing an array val
    bool writeStartArray() {
        if (stack_.empty() || verifyValueState()) {
            if (stack_.empty()) {
                // if this is a new JSONB, write the header
                if (!hasHdr_) {
                    writeHeader();
                } else
                    return false;
            }

            // check if the array exceeds the maximum nesting level
            if (stack_.size() >= MaxNestingLevel) return false;

            os_->put((JsonbTypeUnder)JsonbType::T_Array);
            // save the size position
            stack_.push(WriteInfo({WS_Array, os_->tellp()}));

            // fill the size bytes with 0 for now
            uint32_t size = 0;
            os_->write((char*)&size, sizeof(uint32_t));

            kvState_ = WS_Value;
            return true;
        }

        return false;
    }

    // finish writing an array val
    bool writeEndArray() {
        if (!stack_.empty() && stack_.top().state == WS_Array && kvState_ == WS_Value) {
            WriteInfo& ci = stack_.top();
            std::streampos cur_pos = os_->tellp();
            int32_t size = (int32_t)(cur_pos - ci.sz_pos - sizeof(uint32_t));
            assert(size >= 0);

            os_->seekp(ci.sz_pos);
            os_->write((char*)&size, sizeof(uint32_t));
            os_->seekp(cur_pos);
            stack_.pop();

            return true;
        }

        return false;
    }

    OS_TYPE* getOutput() { return os_; }
    JsonbDocument* getDocument() {
        return JsonbDocument::createDocument(getOutput()->getBuffer(), getOutput()->getSize());
    }

    JsonbValue* getValue() {
        return JsonbDocument::createValue(getOutput()->getBuffer(),
                                          (uint32_t)getOutput()->getSize());
    }

    bool writeEnd() {
        while (!stack_.empty()) {
            bool ok = false;
            switch (stack_.top().state) {
            case WS_Array:
                ok = writeEndArray();
                break;
            case WS_Object:
                ok = writeEndObject();
                break;
            case WS_String:
                ok = writeEndString();
                break;
            case WS_Binary:
                ok = writeEndBinary();
                break;
            default:
                ok = false;
                break;
            }
            if (ok == false) return false;
        }
        return true;
    }

private:
    // verify we are in the right state before writing a value
    bool verifyValueState() {
        assert(!stack_.empty());
        // The document can only be an Object or an Array which follows
        // the standard.
        return (stack_.top().state == WS_Object && kvState_ == WS_Key) ||
               (stack_.top().state == WS_Array && kvState_ == WS_Value);
    }

    // verify we are in the right state before writing a key
    bool verifyKeyState() {
        assert(!stack_.empty());
        return stack_.top().state == WS_Object && kvState_ == WS_Value;
    }

    void writeHeader() {
        os_->put(JSONB_VER);
        hasHdr_ = true;
    }

private:
    enum WriteState {
        WS_NONE,
        WS_Array,
        WS_Object,
        WS_Key,
        WS_Value,
        WS_String,
        WS_Binary,
    };

    struct WriteInfo {
        WriteState state;
        std::streampos sz_pos;
    };

private:
    OS_TYPE* os_ = nullptr;
    bool alloc_;
    bool hasHdr_;
    WriteState kvState_; // key or value state
    std::streampos str_pos_;
    std::stack<WriteInfo> stack_;
    bool first_ = true;
};

typedef JsonbWriterT<JsonbOutStream> JsonbWriter;

} // namespace doris

#endif // JSONB_JSONBWRITER_H
