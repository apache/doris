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
 * This file defines JsonbParserT (template) and JsonbParser.
 *
 * JsonbParserT is a template class which implements a JSON parser.
 * JsonbParserT parses JSON text, and serialize it to JSONB binary format
 * by using JsonbWriterT object. By default, JsonbParserT creates a new
 * JsonbWriterT object with an output stream object.  However, you can also
 * pass in your JsonbWriterT or any stream object that implements some basic
 * interface of std::ostream (see JsonbStream.h).
 *
 * JsonbParser specializes JsonbParserT with JsonbOutStream type (see
 * JsonbStream.h). So unless you want to provide own a different output stream
 * type, use JsonbParser object.
 *
 * ** Parsing JSON **
 * JsonbParserT parses JSON string, and directly serializes into JSONB
 * packed bytes. There are three ways to parse a JSON string: (1) using
 * c-string, (2) using string with len, (3) using std::istream object. You can
 * use custom streambuf to redirect output. JsonbOutBuffer is a streambuf used
 * internally if the input is raw character buffer.
 *
 * You can reuse an JsonbParserT object to parse/serialize multiple JSON
 * strings, and the previous JSONB will be overwritten.
 *
 * If parsing fails (returned false), the error code will be set to one of
 * JsonbErrType, and can be retrieved by calling getErrorCode().
 *
 * ** External dictionary **
 * During parsing a JSON string, you can pass a call-back function to map a key
 * string to an id, and store the dictionary id in JSONB to save space. The
 * purpose of using an external dictionary is more towards a collection of
 * documents (which has common keys) rather than a single document, so that
 * space saving will be significant.
 *
 * ** Endianness **
 * Note: JSONB serialization doesn't assume endianness of the server. However
 * you will need to ensure that the endianness at the reader side is the same
 * as that at the writer side (if they are on different machines). Otherwise,
 * proper conversion is needed when a number value is returned to the
 * caller/writer.
 *
 * @author Tian Xia <tianx@fb.com>
 * 
 * this file is copied from 
 * https://github.com/facebook/mysql-5.6/blob/fb-mysql-5.6.35/fbson/FbsonJsonParser.h
 * and modified by Doris
 */

#ifndef JSONB_JSONBJSONPARSER_H
#define JSONB_JSONBJSONPARSER_H

#include <cmath>
#include <limits>

#include "jsonb_document.h"
#include "jsonb_error.h"
#include "jsonb_writer.h"
#include "string_parser.hpp"

namespace doris {

const char* const kJsonDelim = " ,]}\t\r\n";
const char* const kWhiteSpace = " \t\n\r";

/*
 * Template JsonbParserT
 */
template <class OS_TYPE>
class JsonbParserT {
public:
    JsonbParserT() : stream_pos_(0), err_(JsonbErrType::E_NONE) {}

    explicit JsonbParserT(OS_TYPE& os) : writer_(os), stream_pos_(0), err_(JsonbErrType::E_NONE) {}

    // parse a UTF-8 JSON string
    bool parse(const std::string& str, hDictInsert handler = nullptr) {
        return parse(str.c_str(), (unsigned int)str.size(), handler);
    }

    // parse a UTF-8 JSON c-style string (NULL terminated)
    bool parse(const char* c_str, hDictInsert handler = nullptr) {
        return parse(c_str, (unsigned int)strlen(c_str), handler);
    }

    // parse a UTF-8 JSON string with length
    bool parse(const char* pch, unsigned int len, hDictInsert handler = nullptr) {
        if (!pch || len == 0) {
            err_ = JsonbErrType::E_EMPTY_DOCUMENT;
            return false;
        }

        JsonbInBuffer sb(pch, len);
        std::istream in(&sb);
        return parse(in, handler);
    }

    // parse UTF-8 JSON text from an input stream
    bool parse(std::istream& in, hDictInsert handler = nullptr) {
        bool res = false;
        err_ = JsonbErrType::E_NONE;
        stream_pos_ = 0;

        // reset output stream
        writer_.reset();

        trim(in);

        // TODO(wzy): parsePrimitive should be implemented
        if (in.peek() == '{') {
            skipChar(in);
            res = parseObject(in, handler);
        } else if (in.peek() == '[') {
            skipChar(in);
            res = parseArray(in, handler);
        } else {
            res = parsePrimitive(in, handler);
            if (!res) err_ = handle_parse_failure(in);
        }

        trim(in);
        if (res && !in.eof()) {
            err_ = JsonbErrType::E_INVALID_DOCU;
            return false;
        }

        return res;
    }

    JsonbWriterT<OS_TYPE>& getWriter() { return writer_; }

    JsonbErrType getErrorCode() { return err_; }

    JsonbErrInfo getErrorInfo() {
        assert(err_ < JsonbErrType::E_NUM_ERRORS);

        JsonbErrInfo err_info;

        // stream_pos_ always points to the next char, so err_pos is 1-based
        err_info.err_pos = stream_pos_;
        err_info.err_msg = JsonbErrMsg::getErrMsg(err_);

        return err_info;
    }

    // clear error code
    void clearErr() { err_ = JsonbErrType::E_NONE; }

private:
    JsonbErrType handle_parse_value_failure(bool parse_res, std::istream& in) {
        if (parse_res) {
            trim(in);
            if (!in.good()) {
                return JsonbErrType::E_INVALID_DOCU_COMPAT;
            }
        }
        return JsonbErrType::E_INVALID_DOCU;
        ;
    }

    // In case json is determined to be invalid at top level,
    // try to parse literal values.
    // We return a different error code E_INVALID_DOCU_COMPAT
    // in case the input json contains these values.
    // Returning a different error code will cause an
    // auditing on the caller.
    // This is mainly done because 8.0 JSON_VALID considers
    // this as a valid input.
    JsonbErrType handle_parse_failure(std::istream& in) {
        JsonbErrType error = JsonbErrType::E_INVALID_DOCU;
        if (!writer_.writeStartArray()) {
            return error;
        }

        switch (in.peek()) {
        case 'n':
            skipChar(in);
            error = handle_parse_value_failure(parseNull(in), in);
            break;
        case 't':
            skipChar(in);
            error = handle_parse_value_failure(parseTrue(in), in);
            break;
        case 'f':
            skipChar(in);
            error = handle_parse_value_failure(parseFalse(in), in);
            break;
        case '"':
            skipChar(in);
            error = handle_parse_value_failure(parseString(in), in);
            break;
        default:
            if (parseNumber(in)) {
                trim(in);
                if (in.eof()) {
                    error = JsonbErrType::E_INVALID_DOCU_COMPAT;
                }
            }
        }
        if (!writer_.writeEndArray()) {
            return error;
        }

        return error;
    }

    // parse primitive
    bool parsePrimitive(std::istream& in, hDictInsert handler) {
        bool res = false;
        switch (in.peek()) {
        case 'n':
            skipChar(in);
            res = parseNull(in);
            break;
        case 't':
            skipChar(in);
            res = parseTrue(in);
            break;
        case 'f':
            skipChar(in);
            res = parseFalse(in);
            break;
        case '"':
            skipChar(in);
            res = parseString(in);
            break;
        default:
            res = parseNumber(in);
        }

        return res;
    }

    // parse a JSON object (comma-separated list of key-value pairs)
    bool parseObject(std::istream& in, hDictInsert handler) {
        if (!writer_.writeStartObject()) {
            err_ = JsonbErrType::E_OUTPUT_FAIL;
            return false;
        }

        trim(in);

        if (in.peek() == '}') {
            skipChar(in);
            // empty object
            if (!writer_.writeEndObject()) {
                err_ = JsonbErrType::E_OUTPUT_FAIL;
                return false;
            }
            return true;
        }

        while (in.good()) {
            if (nextChar(in) != '"') {
                err_ = JsonbErrType::E_INVALID_OBJ;
                return false;
            }

            if (!parseKVPair(in, handler)) {
                return false;
            }

            trim(in);

            char ch = nextChar(in);
            if (ch == '}') {
                // end of the object
                if (!writer_.writeEndObject()) {
                    err_ = JsonbErrType::E_OUTPUT_FAIL;
                    return false;
                }
                return true;
            } else if (ch != ',') {
                err_ = JsonbErrType::E_INVALID_OBJ;
                return false;
            }

            trim(in);
        }

        err_ = JsonbErrType::E_INVALID_OBJ;
        return false;
    }

    // parse a JSON array (comma-separated list of values)
    bool parseArray(std::istream& in, hDictInsert handler) {
        if (!writer_.writeStartArray()) {
            err_ = JsonbErrType::E_OUTPUT_FAIL;
            return false;
        }

        trim(in);

        if (in.peek() == ']') {
            skipChar(in);
            // empty array
            if (!writer_.writeEndArray()) {
                err_ = JsonbErrType::E_OUTPUT_FAIL;
                return false;
            }
            return true;
        }

        while (in.good()) {
            if (!parseValue(in, handler)) {
                return false;
            }

            trim(in);

            char ch = nextChar(in);
            if (ch == ']') {
                // end of the array
                if (!writer_.writeEndArray()) {
                    err_ = JsonbErrType::E_OUTPUT_FAIL;
                    return false;
                }
                return true;
            } else if (ch != ',') {
                err_ = JsonbErrType::E_INVALID_ARR;
                return false;
            }

            trim(in);
        }

        err_ = JsonbErrType::E_INVALID_ARR;
        return false;
    }

    // parse a key-value pair, separated by ":"
    bool parseKVPair(std::istream& in, hDictInsert handler) {
        if (parseKey(in, handler) && parseValue(in, handler)) {
            return true;
        }

        return false;
    }

    // parse a key (must be string)
    bool parseKey(std::istream& in, hDictInsert handler) {
        char key[JsonbKeyValue::sMaxKeyLen];
        int key_len = 0;
        while (in.good() && in.peek() != '"' && key_len < JsonbKeyValue::sMaxKeyLen) {
            char ch = nextChar(in);
            if (ch == '\\') {
                char escape_buffer[5]; // buffer for escape
                int len;
                if (!parseEscape(in, escape_buffer, len)) {
                    err_ = JsonbErrType::E_INVALID_KEY_STRING;
                    return false;
                }
                if (key_len + len >= JsonbKeyValue::sMaxKeyLen) {
                    err_ = JsonbErrType::E_INVALID_KEY_LENGTH;
                    return false;
                }
                memcpy(key + key_len, escape_buffer, len);
                key_len += len;
            } else {
                key[key_len++] = ch;
            }
        }
        // The JSON key can be an empty string.
        if (!in.good() || in.peek() != '"') {
            if (key_len == JsonbKeyValue::sMaxKeyLen)
                err_ = JsonbErrType::E_INVALID_KEY_LENGTH;
            else
                err_ = JsonbErrType::E_INVALID_KEY_STRING;
            return false;
        }

        skipChar(in); // discard '"'

        int key_id = -1;
        if (handler) {
            key_id = handler(key, key_len);
        }

        if (key_id < 0) {
            writer_.writeKey(key, key_len);
        } else {
            writer_.writeKey(key_id);
        }

        trim(in);

        if (nextChar(in) != ':') {
            err_ = JsonbErrType::E_INVALID_OBJ;
            return false;
        }

        trim(in);
        if (!in.good()) {
            err_ = JsonbErrType::E_INVALID_OBJ;
            return false;
        }

        return true;
    }

    // parse a value
    bool parseValue(std::istream& in, hDictInsert handler) {
        bool res = false;

        switch (in.peek()) {
        case 'N':
        case 'n': {
            skipChar(in);
            res = parseNull(in);
            break;
        }
        case 'T':
        case 't': {
            skipChar(in);
            res = parseTrue(in);
            break;
        }
        case 'F':
        case 'f': {
            skipChar(in);
            res = parseFalse(in);
            break;
        }
        case '"': {
            skipChar(in);
            res = parseString(in);
            break;
        }
        case '{': {
            skipChar(in);
            ++nesting_lvl_;
            if (nesting_lvl_ >= MaxNestingLevel) {
                err_ = JsonbErrType::E_NESTING_LVL_OVERFLOW;
                return false;
            }
            res = parseObject(in, handler);
            if (res) {
                --nesting_lvl_;
            }
            break;
        }
        case '[': {
            skipChar(in);
            ++nesting_lvl_;
            if (nesting_lvl_ >= MaxNestingLevel) {
                err_ = JsonbErrType::E_NESTING_LVL_OVERFLOW;
                return false;
            }
            res = parseArray(in, handler);
            if (res) {
                --nesting_lvl_;
            }
            break;
        }
        default: {
            res = parseNumber(in);
            break;
        }
        }

        return res;
    }

    // parse NULL value
    bool parseNull(std::istream& in) {
        if (tolower(nextChar(in)) == 'u' && tolower(nextChar(in)) == 'l' &&
            tolower(nextChar(in)) == 'l') {
            writer_.writeNull();
            return true;
        }

        err_ = JsonbErrType::E_INVALID_SCALAR_VALUE;
        return false;
    }

    // parse TRUE value
    bool parseTrue(std::istream& in) {
        if (tolower(nextChar(in)) == 'r' && tolower(nextChar(in)) == 'u' &&
            tolower(nextChar(in)) == 'e') {
            writer_.writeBool(true);
            return true;
        }

        err_ = JsonbErrType::E_INVALID_SCALAR_VALUE;
        return false;
    }

    // parse FALSE value
    bool parseFalse(std::istream& in) {
        if (tolower(nextChar(in)) == 'a' && tolower(nextChar(in)) == 'l' &&
            tolower(nextChar(in)) == 's' && tolower(nextChar(in)) == 'e') {
            writer_.writeBool(false);
            return true;
        }

        err_ = JsonbErrType::E_INVALID_SCALAR_VALUE;
        return false;
    }

    /*
    This is a helper function to parse the hex value. hex_num means the
    number of digits needed to be parsed. If less than zero, then it will
    consider all the characters between current and any character in JsonDelim.
  */
    unsigned parseHexHelper(std::istream& in, uint64_t& val, unsigned hex_num = 17) {
        // We can't read more than 17 digits, so when read 17 digits, it's overflow
        val = 0;
        unsigned num_digits = 0;
        char ch = tolower(in.peek());
        while (in.good() && !strchr(kJsonDelim, ch) && num_digits != hex_num) {
            if (ch >= '0' && ch <= '9') {
                val = (val << 4) + (ch - '0');
            } else if (ch >= 'a' && ch <= 'f') {
                val = (val << 4) + (ch - 'a' + 10);
            } else {
                // unrecognized hex digit
                return 0;
            }
            skipChar(in);
            ch = tolower(in.peek());
            ++num_digits;
        }
        return num_digits;
    }

    // parse HEX value
    bool parseHex4(std::istream& in, unsigned& h) {
        uint64_t val;
        if (4 == parseHexHelper(in, val, 4)) {
            h = (unsigned)val;
            return true;
        }
        return false;
    }

    /*
     parse Escape char.
  */
    bool parseEscape(std::istream& in, char* out, int& len) {
        /*
      This is extracted from cJSON implementation.
      This is about the mask of the first byte in UTF-8.
      The mask is defined in:
      http://en.wikipedia.org/wiki/UTF-8#Description
    */
        const unsigned char firstByteMark[6] = {0x00, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC};
        if (!in.good()) {
            return false;
        }
        char c = nextChar(in);
        len = 1;
        switch (c) {
        // \" \\ \/  \b \f \n \r \t
        case '"':
            *out = '"';
            return true;
        case '\\':
            *out = '\\';
            return true;
        case '/':
            *out = '/';
            return true;
        case 'b':
            *out = '\b';
            return true;
        case 'f':
            *out = '\f';
            return true;
        case 'n':
            *out = '\n';
            return true;
        case 'r':
            *out = '\r';
            return true;
        case 't':
            *out = '\t';
            return true;
        case 'u': {
            unsigned uc;
            if (!parseHex4(in, uc)) {
                return false;
            }
            /*
          For DC00 to DFFF, it should be low surrogates for UTF16.
          So if it display in the high bits, it's invalid.
        */
            if (uc >= 0xDC00 && uc <= 0xDFFF) {
                return false;
            }

            /*
          For D800 to DBFF, it's the high surrogates for UTF16.
          So it's utf-16, there must be another one between 0xDC00
          and 0xDFFF.
        */
            if (uc >= 0xD800 && uc <= 0xDBFF) {
                unsigned uc2;

                if (!in.good()) {
                    return false;
                }
                c = nextChar(in);
                if (c != '\\') {
                    return false;
                }

                if (!in.good()) {
                    return false;
                }
                c = nextChar(in);
                if (c != 'u') {
                    return false;
                }

                if (!parseHex4(in, uc2)) {
                    return false;
                }
                /*
            Now we need the low surrogates for UTF16. It should be
            within 0xDC00 and 0xDFFF.
          */
                if (uc2 < 0xDC00 || uc2 > 0xDFFF) return false;
                /*
            For the character that not in the Basic Multilingual Plan,
            it's represented as twelve-character, encoding the UTF-16
            surrogate pair.
            UTF16 is between 0x10000 and 0x10FFFF. The high surrogate
            present the high bits and the low surrogate present the
            lower 10 bits.
            For detailed explanation, please refer to:
            http://www.ecma-international.org/publications/files/ECMA-ST/ECMA-404.pdf
            Then it will be converted to UTF8.
          */
                uc = 0x10000 + (((uc & 0x3FF) << 10) | (uc2 & 0x3FF));
            }

            /*
          Get the length of the unicode.
          Please refer to http://en.wikipedia.org/wiki/UTF-8#Description.
        */
            if (uc < 0x80)
                len = 1;
            else if (uc < 0x800)
                len = 2;
            else if (uc < 0x10000)
                len = 3;
            else
                len = 4;
            out += len;
            /*
          Encode it.
          Please refer to http://en.wikipedia.org/wiki/UTF-8#Description.
          This part of code has a reference to cJSON.
        */
            switch (len) {
            case 4:
                *--out = ((uc | 0x80) & 0xBF);
                uc >>= 6;
                [[fallthrough]];
            case 3:
                *--out = ((uc | 0x80) & 0xBF);
                uc >>= 6;
                [[fallthrough]];
            case 2:
                *--out = ((uc | 0x80) & 0xBF);
                uc >>= 6;
                [[fallthrough]];
            case 1:
                // Mask the first byte according to the standard.
                *--out = (uc | firstByteMark[len - 1]);
            }
            return true;
            break;
        }
        default:
            return false;
            break;
        }
    }

    // parse a string
    bool parseString(std::istream& in) {
        const int BUFFER_LEN = 4096;
        if (!writer_.writeStartString()) {
            err_ = JsonbErrType::E_OUTPUT_FAIL;
            return false;
        }

        // write 4KB at a time
        char buffer[BUFFER_LEN];
        int nread = 0;
        while (in.good()) {
            char ch = nextChar(in);
            if (ch == '"') {
                // write all remaining bytes in the buffer
                if (nread > 0) {
                    if (!writer_.writeString(buffer, nread)) {
                        err_ = JsonbErrType::E_OUTPUT_FAIL;
                        return false;
                    }
                }
                // end writing string
                if (!writer_.writeEndString()) {
                    err_ = JsonbErrType::E_OUTPUT_FAIL;
                    return false;
                }
                return true;
            } else if (ch == '\\') {
                // this is a escape char
                char escape_buffer[5]; // buffer for escape
                int len;
                if (!parseEscape(in, escape_buffer, len)) {
                    err_ = JsonbErrType::E_INVALID_STR;
                    return false;
                }

                // Write each char to the buffer
                for (int i = 0; i != len; ++i) {
                    buffer[nread++] = escape_buffer[i];
                    if (nread == BUFFER_LEN) {
                        if (!writer_.writeString(buffer, nread)) {
                            err_ = JsonbErrType::E_OUTPUT_FAIL;
                            return false;
                        }
                        nread = 0;
                    }
                }
            } else {
                // just a char
                buffer[nread++] = ch;
                if (nread == BUFFER_LEN) {
                    // flush buffer
                    if (!writer_.writeString(buffer, nread)) {
                        err_ = JsonbErrType::E_OUTPUT_FAIL;
                        return false;
                    }
                    nread = 0;
                }
            }
        }

        err_ = JsonbErrType::E_INVALID_STR;
        return false;
    }

    // parse a number
    // Number format can be hex, octal, or decimal (including float).
    // Only decimal can have (+/-) sign prefix.
    bool parseNumber(std::istream& in) {
        bool ret = false;
        switch (in.peek()) {
        case '0': {
            skipChar(in);

            if (in.peek() == 'x' || in.peek() == 'X') {
                skipChar(in);
                ret = parseHex(in);
            } else if (in.peek() == '.') {
                skipChar(in); // remove '.'
                num_buf_[0] = '.';
                ret = parseDouble(in, num_buf_ + 1);
            } else {
                ret = parseOctal(in);
            }

            break;
        }
        case '-': {
            skipChar(in);
            ret = parseDecimal(in, true);
            break;
        }
        case '+':
            skipChar(in);
        // fall through
        default:
            ret = parseDecimal(in);
            break;
        }

        return ret;
    }

    // parse a number in hex format
    bool parseHex(std::istream& in) {
        uint64_t val = 0;
        int num_digits;
        if (0 == (num_digits = parseHexHelper(in, val))) {
            err_ = JsonbErrType::E_INVALID_HEX;
            return false;
        }

        int size = 0;
        if (num_digits <= 2) {
            size = writer_.writeInt8((int8_t)val);
        } else if (num_digits <= 4) {
            size = writer_.writeInt16((int16_t)val);
        } else if (num_digits <= 8) {
            size = writer_.writeInt32((int32_t)val);
        } else if (num_digits <= 16) {
            size = writer_.writeInt64(val);
        } else {
            err_ = JsonbErrType::E_HEX_OVERFLOW;
            return false;
        }

        if (size == 0) {
            err_ = JsonbErrType::E_OUTPUT_FAIL;
            return false;
        }

        return true;
    }

    // parse a number in octal format
    bool parseOctal(std::istream& in) {
        int64_t val = 0;
        char ch = in.peek();
        while (in.good() && !strchr(kJsonDelim, ch)) {
            if (ch >= '0' && ch <= '7') {
                val = val * 8 + (ch - '0');
            } else {
                err_ = JsonbErrType::E_INVALID_OCTAL;
                return false;
            }

            // check if the number overflows
            if (val < 0) {
                err_ = JsonbErrType::E_OCTAL_OVERFLOW;
                return false;
            }

            skipChar(in);
            ch = in.peek();
        }

        int size = 0;
        if (val <= std::numeric_limits<int8_t>::max()) {
            size = writer_.writeInt8((int8_t)val);
        } else if (val <= std::numeric_limits<int16_t>::max()) {
            size = writer_.writeInt16((int16_t)val);
        } else if (val <= std::numeric_limits<int32_t>::max()) {
            size = writer_.writeInt32((int32_t)val);
        } else { // val <= INT64_MAX
            size = writer_.writeInt64(val);
        }

        if (size == 0) {
            err_ = JsonbErrType::E_OUTPUT_FAIL;
            return false;
        }

        return true;
    }

    // parse a number in decimal (including float)
    bool parseDecimal(std::istream& in, bool neg = false) {
        char ch = 0;
        while (in.good() && (ch = in.peek()) == '0') skipChar(in);

        char* pbuf = num_buf_;
        if (neg) *(pbuf++) = '-';

        char* save_pos = pbuf;
        while (in.good() && !strchr(kJsonDelim, ch)) {
            *(pbuf++) = ch;
            if (pbuf == end_buf_) {
                err_ = JsonbErrType::E_DECIMAL_OVERFLOW;
                return false;
            }

            if (ch == '.') {
                skipChar(in); // remove '.'
                return parseDouble(in, pbuf);
            } else if (ch == 'E' || ch == 'e') {
                skipChar(in); // remove 'E'
                return parseExponent(in, pbuf);
            } else if (ch < '0' || ch > '9') {
                err_ = JsonbErrType::E_INVALID_DECIMAL;
                return false;
            }

            skipChar(in);
            ch = in.peek();
        }
        if (save_pos == pbuf) {
            err_ = JsonbErrType::E_INVALID_DECIMAL; // empty input
            return false;
        }

        *pbuf = 0; // set null-terminator
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        int128_t val =
                StringParser::string_to_int<int128_t>(num_buf_, pbuf - num_buf_, &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            VLOG_ROW << "debug string_to_int error for " << num_buf_ << " val=" << val
                     << " parse_result=" << parse_result;
            err_ = JsonbErrType::E_DECIMAL_OVERFLOW;
            return false;
        }

        int size = 0;
        if (val >= std::numeric_limits<int8_t>::min() &&
            val <= std::numeric_limits<int8_t>::max()) {
            size = writer_.writeInt8((int8_t)val);
        } else if (val >= std::numeric_limits<int16_t>::min() &&
                   val <= std::numeric_limits<int16_t>::max()) {
            size = writer_.writeInt16((int16_t)val);
        } else if (val >= std::numeric_limits<int32_t>::min() &&
                   val <= std::numeric_limits<int32_t>::max()) {
            size = writer_.writeInt32((int32_t)val);
        } else if (val >= std::numeric_limits<int64_t>::min() &&
                   val <= std::numeric_limits<int64_t>::max()) {
            size = writer_.writeInt64((int64_t)val);
        } else { // INT128
            size = writer_.writeInt128(val);
        }

        if (size == 0) {
            err_ = JsonbErrType::E_OUTPUT_FAIL;
            return false;
        }

        return true;
    }

    // parse IEEE745 double precision
    bool parseDouble(std::istream& in, char* pbuf) {
        char* save_pos = pbuf;
        char ch = in.peek();
        while (in.good() && !strchr(kJsonDelim, ch)) {
            *(pbuf++) = ch;
            if (pbuf == end_buf_) {
                err_ = JsonbErrType::E_DOUBLE_OVERFLOW;
                return false;
            }

            if (ch == 'e' || ch == 'E') {
                skipChar(in); // remove 'E'
                return parseExponent(in, pbuf);
            } else if (ch < '0' || ch > '9') {
                err_ = JsonbErrType::E_INVALID_DECIMAL;
                return false;
            }

            skipChar(in);
            ch = in.peek();
        }
        if (save_pos == pbuf) {
            err_ = JsonbErrType::E_INVALID_DECIMAL; // empty input
            return false;
        }

        *pbuf = 0; // set null-terminator
        return internConvertBufferToDouble(num_buf_, pbuf - num_buf_);
    }

    // parse the exponent part of a double number
    bool parseExponent(std::istream& in, char* pbuf) {
        char ch = in.peek();
        if (in.good()) {
            if (ch == '+' || ch == '-') {
                *(pbuf++) = ch;
                if (pbuf == end_buf_) {
                    err_ = JsonbErrType::E_DOUBLE_OVERFLOW;
                    return false;
                }
                skipChar(in);
                ch = in.peek();
            }
        }

        char* save_pos = pbuf;
        while (in.good() && !strchr(kJsonDelim, ch)) {
            *(pbuf++) = ch;
            if (pbuf == end_buf_) {
                err_ = JsonbErrType::E_DOUBLE_OVERFLOW;
                return false;
            }

            if (ch < '0' || ch > '9') {
                err_ = JsonbErrType::E_INVALID_EXPONENT;
                return false;
            }

            skipChar(in);
            ch = in.peek();
        }
        if (save_pos == pbuf) {
            err_ = JsonbErrType::E_INVALID_EXPONENT; // empty input
            return false;
        }

        *pbuf = 0; // set null-terminator
        return internConvertBufferToDouble(num_buf_, pbuf - num_buf_);
    }

    // call system function to parse double to string
    bool internConvertBufferToDouble(char* num_buf_, int len) {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        double val = StringParser::string_to_float<double>(num_buf_, len, &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            VLOG_ROW << "debug string_to_float error for " << num_buf_ << " val=" << val
                     << " parse_result=" << parse_result;
            err_ = JsonbErrType::E_DECIMAL_OVERFLOW;
            return false;
        }

        if (writer_.writeDouble(val) == 0) {
            err_ = JsonbErrType::E_OUTPUT_FAIL;
            return false;
        }

        return true;
    }

    void trim(std::istream& in) {
        while (in.good() && strchr(kWhiteSpace, in.peek())) {
            skipChar(in);
        }
    }

    /*
   * Helper functions to keep track of characters read.
   * Do not rely on std::istream's tellg() which may not be implemented.
   */

    char nextChar(std::istream& in) {
        ++stream_pos_;
        return in.get();
    }

    void skipChar(std::istream& in) {
        ++stream_pos_;
        in.ignore();
    }

private:
    JsonbWriterT<OS_TYPE> writer_;
    uint32_t stream_pos_;
    JsonbErrType err_;
    char num_buf_[512]; // buffer to hold number string
    const char* end_buf_ = num_buf_ + sizeof(num_buf_) - 1;
    uint32_t nesting_lvl_ = 0;
};

typedef JsonbParserT<JsonbOutStream> JsonbParser;

} // namespace doris

#endif // JSONB_JSONBJSONPARSER_H
