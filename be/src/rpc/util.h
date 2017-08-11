// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_RPC_UTIL_H
#define BDG_PALO_BE_SRC_RPC_UTIL_H

#include <string>
#include <sstream>
#include <limits>

namespace palo {

/**
 * this is a simple use for std::string
 *
 * In the future we might want to use something better later, as std::string
 * always causes a heap allocation, and is lacking in functionalities
 * cf. http://www.and.org/vstr/comparison
 */

/** Shortcut for printf formats */
typedef long unsigned int Lu;

/** Shortcut for printf formats */
typedef long long unsigned int Llu;

/** Shortcut for printf formats */
typedef long long int Lld;

/**
 * Returns a std::string using printf like format facilities
 * Vanilla snprintf is about 1.5x faster than this, which is about:
 *   10x faster than boost::format;
 *   1.5x faster than std::string append (operator+=);
 *   3.5x faster than std::string operator+;
 *
 * @param fmt A printf-like format string
 * @return A new std::string with the formatted text
 */
std::string format(const char *fmt, ...) __attribute__((format (printf, 1, 2)));

/**
 * Return decimal number string separated by a separator (default: comma)
 * for every 3 digits. Only 10-15% slower than sprintf("%lld", n);
 *
 * @param n The 64-bit number
 * @param sep The separator for every 3 digits
 * @return A new std::string with the formatted text
 */
std::string format_number(int64_t n, int sep = ',');

/**
 * Return first n bytes of buffer with an optional trailer if the
 * size of the buffer exceeds n.
 *
 * @param n The max. displayed size of the buffer
 * @param buf The memory buffer
 * @param len The size of the memory buffer
 * @param trailer Appended if %len exceeds %n
 * @return A new std::string with the formatted text
 */
std::string format_bytes(size_t n, const void *buf, size_t len,
        const char *trailer = "...");

/**
 * Return a string presentation of a sequence. Is quite slow but versatile,
 * as it uses ostringstream.
 *
 * @param seq A STL-compatible sequence with forward-directional iterators
 * @param sep A separator which is inserted after each list item
 * @return A new std::string with the formatted text
 */
template <class SequenceT>
    std::string format_list(const SequenceT &seq, const char *sep = ", ") {
        typedef typename SequenceT::const_iterator Iterator;
        Iterator it = seq.begin(), end = seq.end();
        std::ostringstream out;
        out << '[';
        if (it != end) {
            out << *it;
            ++it;
        }
        for (; it != end; ++it)
            out << sep << *it;
        out << ']';
        return out.str();
    }

/** Strips enclosing quotes.
 * Inspects the first and last characters of the string defined by
 * <code>input</code> and <code>input_len</code> and if they are either both
 * single quotes or double quotes, it sets <code>output</code> and
 * <code>output_len</code> to contained between them.  Otherwise,
 * <code>*output</code> is set to <code>input</code> and
 * <code>*output_len</code> is set to <code>input_len</code>.
 * @param input Input char string
 * @param input_len Input char string length
 * @param output Address of output char string pointer
 * @param output_len Address of output char string pointer
 * @return <i>true</i> if quotes were stripped, <i>false</i> otherwise.
 */
inline bool strip_enclosing_quotes(const char *input, size_t input_len,
        const char **output, size_t *output_len) {
    if (input_len < 2 ||
            !((*input == '\'' && input[input_len-1] == '\'') ||
                (*input == '"' && input[input_len-1] == '"'))) {
        *output = input;
        *output_len = input_len;
        return false;
    }
    *output = input + 1;
    *output_len = input_len - 2;
    return true;
}

/** The fast numeric formatters, very much inspired from http://cppformat.github.io/ */
class NumericFormatterDigits {
    protected:
        static const char DIGITS[];
};

template<class T>
class NumericFormatter : public NumericFormatterDigits {
public:
    
    ~NumericFormatter() {}
    
    /**
      Returns the number of characters written to the output buffer.
      */
    size_t size() const { return buf - s + BUFFER_SIZE - 1; }

    /**
      Returns a pointer to the output buffer content. No terminating null
      character is appended.
      */
    const char *data() const { return str; }

    /**
      Returns a pointer to the output buffer content with terminating null
      character appended.
      */
    const char *c_str() const {
        return s;
    }

    /**
      Returns the content of the output buffer as an `std::string`.
      */
    std::string str() const { return std::string(s, size()); }

    /**
      Appends the converted number to the buffer specified, returns the forwarded pointer.
      */
    char* append_to(char* p) const {
        memcpy(p, s, size());
        return p + size();
    }

protected:

    NumericFormatter() {
        buf[BUFFER_SIZE - 1] = '\0';
    }

    void format_unsigned(T value) {
        s = buf + BUFFER_SIZE - 1;
        while (value >= 100) {
            // Integer division is slow so do it for a group of two digits instead
            // of for every digit. The idea comes from the talk by Alexandrescu
            // "Three Optimization Tips for C++". See speed-test for a comparison.
            unsigned index = (value % 100) * 2;
            value /= 100;
            *--s = DIGITS[index + 1];
            *--s = DIGITS[index];
        }
        if (value < 10) {
            *--s = static_cast<char>('0' + value);
            return;
        }
        unsigned index = static_cast<unsigned>(value * 2);
        *--s = DIGITS[index + 1];
        *--s = DIGITS[index];
    }

    void format_signed(T value) {
        if (value >= 0) {
            format_unsigned(value);
        } else {
            format_unsigned(-value);
            *--s = '-';
        }
    }

private:
    enum { BUFFER_SIZE = std::numeric_limits<T>::digits10 + 3 };
    char buf[BUFFER_SIZE];
    char* s;
};

template<class T>
class NumericSignedFormatter : public NumericFormatter<T> {
    public:
        explicit NumericSignedFormatter(T value) {
            NumericFormatter<T>::format_signed(value);
        }
};

template<class T>
class NumericUnsignedFormatter : public NumericFormatter<T> {
    public:
        explicit NumericUnsignedFormatter(T value) {
            NumericFormatter<T>::format_unsigned(value);
        }
};

typedef NumericUnsignedFormatter<uint8_t> UInt8Formatter;
typedef NumericUnsignedFormatter<uint16_t> UInt16Formatter;
typedef NumericUnsignedFormatter<uint32_t> UInt32Formatter;
typedef NumericUnsignedFormatter<uint64_t> UInt64Formatter;
typedef NumericSignedFormatter<int8_t> Int8Formatter;
typedef NumericSignedFormatter<int16_t> Int16Formatter;
typedef NumericSignedFormatter<int32_t> Int32Formatter;
typedef NumericSignedFormatter<int64_t> Int64Formatter;

} // namespace palo
#endif //BDG_PALO_BE_SRC_RPC_UTIL_H
