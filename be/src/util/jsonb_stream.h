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
 * This header file defines JsonbInBuffer and JsonbOutStream classes.
 *
 * ** Input Buffer **
 * JsonbInBuffer is a customer input buffer to wrap raw character buffer. Its
 * object instances are used to create std::istream objects interally.
 *
 * ** Output Stream **
 * JsonbOutStream is a custom output stream classes, to contain the JSONB
 * serialized binary. The class is conveniently used to specialize templates of
 * JsonbParser and JsonbWriter.
 *
 * @author Tian Xia <tianx@fb.com>
 * this file is copied from 
 * https://github.com/facebook/mysql-5.6/blob/fb-mysql-5.6.35/fbson/FbsonStream.h
 * and modified by Doris
 */

#ifndef JSONB_JSONBSTREAM_H
#define JSONB_JSONBSTREAM_H

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <assert.h>
#include <fmt/format.h>
#include <string.h>

#include <algorithm>
#include <cinttypes>
#include <iostream>

namespace doris {

using int128_t = __int128;

// lengths includes sign
#define MAX_INT_DIGITS 11
#define MAX_INT64_DIGITS 20
#define MAX_INT128_DIGITS 40
#define MAX_DOUBLE_DIGITS 23 // 1(sign)+16(significant)+1(decimal)+5(exponent)

/*
 * JSONB's implementation of input buffer
 */
class JsonbInBuffer : public std::streambuf {
public:
    JsonbInBuffer(const char* str, uint32_t len) {
        // this is read buffer and the str will not be changed
        // so we use const_cast (ugly!) to remove constness
        char* pch(const_cast<char*>(str));
        setg(pch, pch, pch + len);
    }
};

/*
 * JSONB's implementation of output stream.
 *
 * This is a wrapper of a char buffer. By default, the buffer capacity is 1024
 * bytes. We will double the buffer if realloc is needed for writes.
 */
class JsonbOutStream : public std::ostream {
public:
    explicit JsonbOutStream(uint64_t capacity = 1024)
            : std::ostream(nullptr), head_(nullptr), size_(0), capacity_(capacity), alloc_(true) {
        if (capacity_ == 0) {
            capacity_ = 1024;
        }

        head_ = (char*)malloc(capacity_);
    }

    JsonbOutStream(char* buffer, uint64_t capacity)
            : std::ostream(nullptr), head_(buffer), size_(0), capacity_(capacity), alloc_(false) {
        assert(buffer && capacity_ > 0);
    }

    ~JsonbOutStream() {
        if (alloc_) {
            free(head_);
        }
    }

    void put(char c) { write(&c, 1); }

    void write(const char* c_str) { write(c_str, strlen(c_str)); }

    void write(const char* bytes, uint64_t len) {
        if (len == 0) {
            return;
        }

        if (size_ + len > capacity_) {
            realloc(len);
        }

        memcpy(head_ + size_, bytes, len);
        size_ += len;
    }

    // write the integer to string
    void write(int i) {
        // snprintf automatically adds a NULL, so we need one more char
        if (size_ + MAX_INT_DIGITS + 1 > capacity_) {
            realloc(MAX_INT_DIGITS + 1);
        }

        int len = snprintf(head_ + size_, MAX_INT_DIGITS + 1, "%d", i);
        assert(len > 0);
        size_ += len;
    }

    // write the 64bit integer to string
    void write(int64_t l) {
        // snprintf automatically adds a NULL, so we need one more char
        if (size_ + MAX_INT64_DIGITS + 1 > capacity_) {
            realloc(MAX_INT64_DIGITS + 1);
        }

        int len = snprintf(head_ + size_, MAX_INT64_DIGITS + 1, "%" PRIi64, l);
        assert(len > 0);
        size_ += len;
    }

    void write(int128_t l) {
        // snprintf automatically adds a NULL, so we need one more char
        if (size_ + MAX_INT128_DIGITS + 1 > capacity_) {
            realloc(MAX_INT128_DIGITS + 1);
        }

        const auto result = fmt::format_to_n(head_ + size_, MAX_INT128_DIGITS, "{}", l);
        assert(result.size > 0);
        size_ += result.size;
    }

    // write the double to string
    void write(double d) {
        // snprintf automatically adds a NULL, so we need one more char
        if (size_ + MAX_DOUBLE_DIGITS + 1 > capacity_) {
            realloc(MAX_DOUBLE_DIGITS + 1);
        }

        int len = snprintf(head_ + size_, MAX_DOUBLE_DIGITS + 1, "%.15g", d);
        assert(len > 0);
        size_ += len;
    }

    pos_type tellp() const { return size_; }

    void seekp(pos_type pos) { size_ = (uint64_t)pos; }

    const char* getBuffer() const { return head_; }

    pos_type getSize() const { return tellp(); }

private:
    void realloc(uint64_t len) {
        assert(capacity_ > 0);

        capacity_ *= 2;
        while (capacity_ < size_ + len) {
            capacity_ *= 2;
        }

        if (alloc_) {
            char* new_buf = (char*)::realloc(head_, capacity_);
            assert(new_buf);
            head_ = new_buf;
        } else {
            char* new_buf = (char*)::malloc(capacity_);
            assert(new_buf);
            memcpy(new_buf, head_, size_);
            head_ = new_buf;
            alloc_ = true;
        }
    }

private:
    char* head_ = nullptr;
    uint64_t size_;
    uint64_t capacity_;
    bool alloc_;
};

} // namespace doris

#endif // JSONB_JSONBSTREAM_H
