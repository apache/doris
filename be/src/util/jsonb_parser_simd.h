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
 * This file defines JsonbParserTSIMD (template) and JsonbParser.
 *
 * JsonbParserTSIMD is a template class which implements a JSON parser.
 * JsonbParserTSIMD parses JSON text, and serialize it to JSONB binary format
 * by using JsonbWriterT object. By default, JsonbParserTSIMD creates a new
 * JsonbWriterT object with an output stream object.  However, you can also
 * pass in your JsonbWriterT or any stream object that implements some basic
 * interface of std::ostream (see JsonbStream.h).
 *
 * JsonbParser specializes JsonbParserTSIMD with JsonbOutStream type (see
 * JsonbStream.h). So unless you want to provide own a different output stream
 * type, use JsonbParser object.
 *
 * ** Parsing JSON **
 * JsonbParserTSIMD parses JSON string, and directly serializes into JSONB
 * packed bytes. There are three ways to parse a JSON string: (1) using
 * c-string, (2) using string with len, (3) using std::istream object. You can
 * use custom streambuf to redirect output. JsonbOutBuffer is a streambuf used
 * internally if the input is raw character buffer.
 *
 * You can reuse an JsonbParserTSIMD object to parse/serialize multiple JSON
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

#pragma once
#include <simdjson.h>

#include <cmath>
#include <limits>
#include <string_view>

#include "common/status.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "util/string_parser.hpp"

namespace doris {
using int128_t = __int128;
struct JsonbParser {
    // parse a UTF-8 JSON string with length
    // will reset writer before parse
    static Status parse(const char* pch, size_t len, JsonbWriter& writer) {
        if (!pch || len == 0) {
            return Status::InvalidArgument("Empty JSON document");
        }
        writer.reset();
        try {
            simdjson::ondemand::parser simdjson_parser;
            simdjson::padded_string json_str {pch, len};
            simdjson::ondemand::document doc = simdjson_parser.iterate(json_str);

            // simdjson process top level primitive types specially
            // so some repeated code here
            switch (doc.type()) {
            case simdjson::ondemand::json_type::object:
            case simdjson::ondemand::json_type::array: {
                RETURN_IF_ERROR(parse(doc.get_value(), writer));
                break;
            }
            case simdjson::ondemand::json_type::null: {
                if (writer.writeNull() == 0) {
                    return Status::InvalidArgument("writeNull failed");
                }
                break;
            }
            case simdjson::ondemand::json_type::boolean: {
                if (writer.writeBool(doc.get_bool()) == 0) {
                    return Status::InvalidArgument("writeBool failed");
                }
                break;
            }
            case simdjson::ondemand::json_type::string: {
                RETURN_IF_ERROR(write_string(doc.get_string(), writer));
                break;
            }
            case simdjson::ondemand::json_type::number: {
                simdjson::ondemand::number num;
                simdjson::error_code res = doc.get_number().get(num);
                std::string_view token = doc.raw_json_token();
                // For a root number simdjson reports NUMBER_ERROR / BIGINT_ERROR before it
                // checks for trailing content, and the raw token stops at the next token, so
                // `18446744073709551616 0` would otherwise be accepted as its first token.
                // A root number must reach the end of the document.
                if (token.data() + token.size() != json_str.data() + json_str.size()) {
                    return Status::InvalidArgument(
                            "simdjson get_number failed: trailing content after root number "
                            "{}",
                            quote_token(token));
                }
                RETURN_IF_ERROR(write_number(res, num, token, writer));
                break;
            }
            }
        } catch (simdjson::simdjson_error& e) {
            return Status::InvalidArgument(fmt::format("simdjson parse exception: {}", e.what()));
        }
        return Status::OK();
    }

private:
    // parse json, recursively if necessary, by simdjson
    //  and serialize to binary format by writer
    static Status parse(simdjson::ondemand::value value, JsonbWriter& writer) {
        switch (value.type()) {
        case simdjson::ondemand::json_type::null: {
            if (writer.writeNull() == 0) {
                return Status::InvalidArgument("writeNull failed");
            }
            break;
        }
        case simdjson::ondemand::json_type::boolean: {
            if (writer.writeBool(value.get_bool()) == 0) {
                return Status::InvalidArgument("writeBool failed");
            }
            break;
        }
        case simdjson::ondemand::json_type::string: {
            RETURN_IF_ERROR(write_string(value.get_string(), writer));
            break;
        }
        case simdjson::ondemand::json_type::number: {
            simdjson::ondemand::number num;
            simdjson::error_code res = value.get_number().get(num);
            RETURN_IF_ERROR(write_number(res, num, value.raw_json_token(), writer));
            break;
        }
        case simdjson::ondemand::json_type::object: {
            if (!writer.writeStartObject()) {
                return Status::InvalidArgument("writeStartObject failed");
            }

            for (auto kv : value.get_object()) {
                std::string_view key;
                simdjson::error_code e = kv.unescaped_key().get(key);
                if (e != simdjson::SUCCESS) {
                    return Status::InvalidArgument(fmt::format("simdjson get key failed: {}", e));
                }

                // write key
                if (key.size() > std::numeric_limits<uint8_t>::max()) {
                    return Status::InvalidArgument("key size exceeds max limit: {} , {}",
                                                   key.size(), std::numeric_limits<uint8_t>::max());
                }
                if (!writer.writeKey(key.data(), (uint8_t)key.size())) {
                    return Status::InvalidArgument("writeKey failed : {}", key);
                }

                // parse object value
                RETURN_IF_ERROR(parse(kv.value(), writer));
            }

            if (!writer.writeEndObject()) {
                return Status::InvalidArgument("writeEndObject failed");
                break;
            }

            break;
        }
        case simdjson::ondemand::json_type::array: {
            if (!writer.writeStartArray()) {
                return Status::InvalidArgument("writeStartArray failed");
            }

            for (auto elem : value.get_array()) {
                // parse array element
                RETURN_IF_ERROR(parse(elem.value(), writer));
            }

            if (!writer.writeEndArray()) {
                return Status::InvalidArgument("writeEndArray failed");
            }
            break;
        }
        default: {
            return Status::InvalidArgument("unknown value type: ");
        }

        } // end of switch
        return Status::OK();
    }

    static Status write_string(std::string_view str, JsonbWriter& writer) {
        // start writing string
        if (!writer.writeStartString()) {
            return Status::InvalidArgument("writeStartString failed");
        }

        // write string
        if (str.size() > 0) {
            if (writer.writeString(str.data(), str.size()) == 0) {
                return Status::InvalidArgument("writeString failed");
            }
        }

        // end writing string
        if (!writer.writeEndString()) {
            return Status::InvalidArgument("writeEndString failed");
        }
        return Status::OK();
    }

    // raw_json_token() spans up to the start of the next token, so it may end with JSON
    // whitespace that is not part of the number.
    static std::string_view trim_trailing_whitespace(std::string_view token) {
        while (!token.empty() && (token.back() == ' ' || token.back() == '\t' ||
                                  token.back() == '\n' || token.back() == '\r')) {
            token.remove_suffix(1);
        }
        return token;
    }

    // Error messages quote the offending token so that the bad value can be located, but
    // the token is as long as the input (a malformed row may be a multi-megabyte digit run)
    // and tolerant callers such as json_valid or the error-to-null variants discard the
    // message right away. Keep the quoted part bounded and report the full length instead.
    static std::string quote_token(std::string_view token) {
        constexpr size_t kMaxQuotedTokenLen = 64;
        token = trim_trailing_whitespace(token);
        if (token.size() <= kMaxQuotedTokenLen) {
            return std::string(token);
        }
        return fmt::format("{}... (truncated, {} bytes)", token.substr(0, kMaxQuotedTokenLen),
                           token.size());
    }

    // Matches the JSON number grammar exactly:
    //   -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?
    static bool is_json_number(std::string_view token) {
        size_t i = 0;
        const size_t n = token.size();
        auto skip_digits = [&]() {
            const size_t start = i;
            while (i < n && token[i] >= '0' && token[i] <= '9') {
                ++i;
            }
            return i > start;
        };
        if (i < n && token[i] == '-') {
            ++i;
        }
        if (i < n && token[i] == '0') {
            ++i;
        } else if (!skip_digits()) {
            return false;
        }
        if (i < n && token[i] == '.') {
            ++i;
            if (!skip_digits()) {
                return false;
            }
        }
        if (i < n && (token[i] == 'e' || token[i] == 'E')) {
            ++i;
            if (i < n && (token[i] == '+' || token[i] == '-')) {
                ++i;
            }
            if (!skip_digits()) {
                return false;
            }
        }
        return i == n;
    }

    // According to https://github.com/simdjson/simdjson/pull/2139, integers that do not fit
    // in 64 bits can be handled by parsing the raw_json_token ourselves: simdjson returns
    // NUMBER_ERROR for 18446744073709551616 (one above uint64 max) and BIGINT_ERROR for
    // longer integers such as 18446744073709551616231231.
    // However NUMBER_ERROR is also what simdjson returns for malformed tokens (leading
    // zeros like 01, a trailing dot like 1., 1e, trailing garbage like 1x) and for values
    // beyond the double range. `num` carries nothing usable in any of these cases, so the
    // raw token is first checked against the JSON number grammar and then parsed as int128
    // or double.
    static Status write_number_from_token(simdjson::error_code res, std::string_view raw_string,
                                          JsonbWriter& writer) {
        std::string_view token = trim_trailing_whitespace(raw_string);
        if (!is_json_number(token)) {
            return Status::InvalidArgument("simdjson get_number failed: {}, raw string is: {}",
                                           simdjson::error_message(res), quote_token(token));
        }

        // StringParser::string_to_int silently truncates a fraction, so only a token made of
        // digits may be parsed as an integer.
        if (token.find_first_of(".eE") == std::string_view::npos) {
            StringParser::ParseResult result;
            auto val = StringParser::string_to_int<int128_t>(token.data(), token.size(), &result);
            if (result == StringParser::PARSE_SUCCESS) {
                if (!writer.writeInt128(val)) {
                    return Status::InvalidArgument("writeInt128 failed");
                }
                return Status::OK();
            }
        }

        // Either a floating point number or an integer beyond int128. Converting it to double
        // may lose precision, but for JSON, exchanging data as plain text between different
        // systems may inherently cause precision loss.
        StringParser::ParseResult result;
        double double_val =
                StringParser::string_to_float<double>(token.data(), token.size(), &result);
        if (result != StringParser::PARSE_SUCCESS || !std::isfinite(double_val)) {
            return Status::InvalidArgument("invalid number, raw string is: {}", quote_token(token));
        }
        if (!writer.writeDouble(double_val)) {
            return Status::InvalidArgument("writeDouble failed");
        }
        return Status::OK();
    }

    static Status write_number(simdjson::error_code res, simdjson::ondemand::number num,
                               std::string_view raw_string, JsonbWriter& writer) {
        switch (res) {
        case simdjson::error_code::SUCCESS:
            break;
        case simdjson::error_code::NUMBER_ERROR:
        case simdjson::error_code::BIGINT_ERROR:
            return write_number_from_token(res, raw_string, writer);
        default:
            // simdjson reports no other error for a number token (a root number followed by
            // another token is already rejected by the end-of-document check in parse()), so
            // anything else is reported as is.
            return Status::InvalidArgument("simdjson get_number failed: {}, raw string is: {}",
                                           simdjson::error_message(res), quote_token(raw_string));
        }

        // On success simdjson yields one of three number types:
        // 1. floating_point_number: A binary64 number, which will be converted to jsonb's double type.
        // 2. signed_integer: A signed integer that fits in a 64-bit word using two's complement.
        // 3. unsigned_integer: A positive integer larger or equal to 1<<63.
        //    For these two integer types, we will convert them to jsonb's int8/int16/int32/int64/int128 types according to the specific value.
        switch (num.get_number_type()) {
        case simdjson::ondemand::number_type::floating_point_number: {
            if (writer.writeDouble(num.get_double()) == 0) {
                return Status::InvalidArgument("writeDouble failed");
            }
            break;
        }
        case simdjson::ondemand::number_type::signed_integer:
        case simdjson::ondemand::number_type::unsigned_integer: {
            int128_t val = num.is_int64() ? (int128_t)num.get_int64() : (int128_t)num.get_uint64();
            bool success = false;
            if (val >= std::numeric_limits<int8_t>::min() &&
                val <= std::numeric_limits<int8_t>::max()) {
                success = writer.writeInt8((int8_t)val);
            } else if (val >= std::numeric_limits<int16_t>::min() &&
                       val <= std::numeric_limits<int16_t>::max()) {
                success = writer.writeInt16((int16_t)val);
            } else if (val >= std::numeric_limits<int32_t>::min() &&
                       val <= std::numeric_limits<int32_t>::max()) {
                success = writer.writeInt32((int32_t)val);
            } else if (val >= std::numeric_limits<int64_t>::min() &&
                       val <= std::numeric_limits<int64_t>::max()) {
                success = writer.writeInt64((int64_t)val);
            } else { // INT128
                success = writer.writeInt128(val);
            }

            if (!success) {
                return Status::InvalidArgument("writeInt failed");
            }
            break;
        }
        case simdjson::ondemand::number_type::big_integer: {
            // simdjson never parses a big_integer successfully; integers beyond 64 bits
            // arrive as NUMBER_ERROR / BIGINT_ERROR and are handled by
            // write_number_from_token above.
            __builtin_unreachable();
        }
        }
        return Status::OK();
    }
};
} // namespace doris
