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
    // parse a UTF-8 JSON string with length
    // will reset writer before parse
    static Status parse(const char* pch, size_t len, JsonbWriter& writer) {
        if (!pch || len == 0) {
            return Status::InternalError("Empty JSON document");
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
                    return Status::InternalError("writeNull failed");
                }
                break;
            }
            case simdjson::ondemand::json_type::boolean: {
                if (writer.writeBool(doc.get_bool()) == 0) {
                    return Status::InternalError("writeBool failed");
                }
                break;
            }
            case simdjson::ondemand::json_type::string: {
                RETURN_IF_ERROR(write_string(doc.get_string(), writer));
                break;
            }
            case simdjson::ondemand::json_type::number: {
                RETURN_IF_ERROR(write_number(doc.get_number(), doc.raw_json_token(), writer));
                break;
            }
            }
        } catch (simdjson::simdjson_error& e) {
            return Status::InternalError(fmt::format("simdjson parse exception: {}", e.what()));
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
                return Status::InternalError("writeNull failed");
            }
            break;
        }
        case simdjson::ondemand::json_type::boolean: {
            if (writer.writeBool(value.get_bool()) == 0) {
                return Status::InternalError("writeBool failed");
            }
            break;
        }
        case simdjson::ondemand::json_type::string: {
            RETURN_IF_ERROR(write_string(value.get_string(), writer));
            break;
        }
        case simdjson::ondemand::json_type::number: {
            RETURN_IF_ERROR(write_number(value.get_number(), value.raw_json_token(), writer));
            break;
        }
        case simdjson::ondemand::json_type::object: {
            if (!writer.writeStartObject()) {
                return Status::InternalError("writeStartObject failed");
            }

            for (auto kv : value.get_object()) {
                std::string_view key;
                simdjson::error_code e = kv.unescaped_key().get(key);
                if (e != simdjson::SUCCESS) {
                    return Status::InternalError(fmt::format("simdjson get key failed: {}", e));
                }

                // write key
                if (key.size() > std::numeric_limits<uint8_t>::max()) {
                    return Status::InternalError("key size exceeds max limit: {} , {}", key.size(),
                                                 std::numeric_limits<uint8_t>::max());
                }
                if (writer.writeKey(key.data(), (uint8_t)key.size()) == 0) {
                    return Status::InternalError("writeKey failed : {}", key);
                }

                // parse object value
                RETURN_IF_ERROR(parse(kv.value(), writer));
            }

            if (!writer.writeEndObject()) {
                return Status::InternalError("writeEndObject failed");
                break;
            }

            break;
        }
        case simdjson::ondemand::json_type::array: {
            if (!writer.writeStartArray()) {
                return Status::InternalError("writeStartArray failed");
            }

            for (auto elem : value.get_array()) {
                // parse array element
                RETURN_IF_ERROR(parse(elem.value(), writer));
            }

            if (!writer.writeEndArray()) {
                return Status::InternalError("writeEndArray failed");
            }
            break;
        }
        default: {
            return Status::InternalError("unknown value type: ");
        }

        } // end of switch
        return Status::OK();
    }

    static Status write_string(std::string_view str, JsonbWriter& writer) {
        // start writing string
        if (!writer.writeStartString()) {
            return Status::InternalError("writeStartString failed");
        }

        // write string
        if (str.size() > 0) {
            if (writer.writeString(str.data(), str.size()) == 0) {
                return Status::InternalError("writeString failed");
            }
        }

        // end writing string
        if (!writer.writeEndString()) {
            return Status::InternalError("writeEndString failed");
        }
        return Status::OK();
    }

    static Status write_number(simdjson::ondemand::number num, std::string_view raw_string,
                               JsonbWriter& writer) {
        if (num.is_double()) {
            double number = num.get_double();
            // When a double exceeds the precision that can be represented by a double type in simdjson, it gets converted to 0.
            // The correct approach, should be to truncate the double value instead.
            if (number == 0) {
                StringParser::ParseResult result;
                number = StringParser::string_to_float<double>(raw_string.data(), raw_string.size(),
                                                               &result);
                if (result != StringParser::PARSE_SUCCESS) {
                    return Status::InternalError("invalid number, raw string is: " +
                                                 std::string(raw_string));
                }
            }

            if (writer.writeDouble(number) == 0) {
                return Status::InternalError("writeDouble failed");
            }
        } else if (num.is_int64() || num.is_uint64()) {
            int128_t val = num.is_int64() ? (int128_t)num.get_int64() : (int128_t)num.get_uint64();
            int size = 0;
            if (val >= std::numeric_limits<int8_t>::min() &&
                val <= std::numeric_limits<int8_t>::max()) {
                size = writer.writeInt8((int8_t)val);
            } else if (val >= std::numeric_limits<int16_t>::min() &&
                       val <= std::numeric_limits<int16_t>::max()) {
                size = writer.writeInt16((int16_t)val);
            } else if (val >= std::numeric_limits<int32_t>::min() &&
                       val <= std::numeric_limits<int32_t>::max()) {
                size = writer.writeInt32((int32_t)val);
            } else if (val >= std::numeric_limits<int64_t>::min() &&
                       val <= std::numeric_limits<int64_t>::max()) {
                size = writer.writeInt64((int64_t)val);
            } else { // INT128
                size = writer.writeInt128(val);
            }

            if (size == 0) {
                return Status::InternalError("writeInt failed");
            }
        } else {
            return Status::InternalError("invalid number: " + std::to_string(num.as_double()));
        }
        return Status::OK();
    }
};
#include "common/compile_check_end.h"
} // namespace doris
