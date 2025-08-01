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

#include "common/status.h"
#include "jsonb_document.h"
#include "jsonb_writer.h"
#include "string_parser.hpp"

namespace doris {
#include "common/compile_check_begin.h"
using int128_t = __int128;
struct JsonbParser {
    // According to https://github.com/simdjson/simdjson/pull/2139
    // For numbers larger than 64 bits, we can obtain the raw_json_token and parse it ourselves.
    // This allows handling numbers larger than 64 bits, such as int128.
    // For example, try to parse a 18446744073709551616, this number is just 1 greater than the maximum value of uint64_t, and simdjson will return a NUMBER_ERROR
    // If try to parse a 18446744073709551616231231, it is obviously a large integer, at this time simdjson will return a BIGINT_ERROR
    static bool parse_number_success(simdjson::error_code error_code) {
        return error_code == simdjson::error_code::SUCCESS ||
               error_code == simdjson::error_code::NUMBER_ERROR ||
               error_code == simdjson::error_code::BIGINT_ERROR;
    }

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
                if (!parse_number_success(res)) {
                    return Status::InvalidArgument(fmt::format("simdjson get_number failed: {}",
                                                               simdjson::error_message(res)));
                }
                // simdjson get_number() returns a number object, which can be
                RETURN_IF_ERROR(
                        write_number(num, doc.get_number_type(), doc.raw_json_token(), writer));
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
            auto res = value.get_number().get(num);
            if (!parse_number_success(res)) {
                return Status::InvalidArgument(fmt::format("simdjson get_number failed: {}",
                                                           simdjson::error_message(res)));
            }

            RETURN_IF_ERROR(
                    write_number(num, value.get_number_type(), value.raw_json_token(), writer));
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

    static Status write_number(simdjson::ondemand::number num,
                               simdjson ::ondemand::number_type num_type,
                               std::string_view raw_string, JsonbWriter& writer) {
        // The simdjson library supports four types of numbers:
        // 1. floating_point_number: A binary64 number, which will be converted to jsonb's double type.
        // 2. signed_integer: A signed integer that fits in a 64-bit word using two's complement.
        // 3. unsigned_integer: A positive integer larger or equal to 1<<63.
        //    For these two integer types, we will convert them to jsonb's int8/int16/int32/int64/int128 types according to the specific value.
        // 4. big_integer: An integer that does not fit in a 64-bit word.
        //    For this type, simdjson cannot handle it directly. We first try to convert it to jsonb's int128 type.
        //    If conversion fails, we attempt to convert it to a double type.
        //    If conversion to double also fails, an error is returned.

        switch (num_type) {
        case simdjson::ondemand::number_type::floating_point_number: {
            double number = num.get_double();
            // When a double exceeds the precision that can be represented by a double type in simdjson, it gets converted to 0.
            // The correct approach, should be to truncate the double value instead.
            if (number == 0) {
                StringParser::ParseResult result;
                number = StringParser::string_to_float<double>(raw_string.data(), raw_string.size(),
                                                               &result);
                if (result != StringParser::PARSE_SUCCESS) {
                    return Status::InvalidArgument("invalid number, raw string is: " +
                                                   std::string(raw_string));
                }
            }

            if (writer.writeDouble(number) == 0) {
                return Status::InvalidArgument("writeDouble failed");
            }

            break;
        }
        case simdjson::fallback::number_type::signed_integer:
        case simdjson::fallback::number_type::unsigned_integer: {
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
        case simdjson::fallback::number_type::big_integer: {
            StringParser::ParseResult result;
            auto val = StringParser::string_to_int<int128_t>(raw_string.data(), raw_string.size(),
                                                             &result);
            if (result != StringParser::PARSE_SUCCESS) {
                // If the string exceeds the range of int128_t, it will attempt to convert it to double.
                // This may result in loss of precision, but for JSON, exchanging data as plain text between different systems may inherently cause precision loss.
                // try parse as double
                double double_val = StringParser::string_to_float<double>(
                        raw_string.data(), raw_string.size(), &result);
                if (result != StringParser::PARSE_SUCCESS) {
                    // if both parse failed, return error
                    return Status::InvalidArgument("invalid number, raw string is: " +
                                                   std::string(raw_string));
                }
                if (!writer.writeDouble(double_val)) {
                    return Status::InvalidArgument("writeDouble failed");
                }
            } else {
                // as int128_t
                if (!writer.writeInt128(val)) {
                    return Status::InvalidArgument("writeInt128 failed");
                }
            }
            break;
        }
        }
        return Status::OK();
    }
};
#include "common/compile_check_end.h"
} // namespace doris
