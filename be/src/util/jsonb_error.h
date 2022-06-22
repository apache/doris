/*
 *  Copyright (c) 2014, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 *  this file is copied from 
 *  https://github.com/facebook/mysql-5.6/blob/fb-mysql-5.6.35/fbson/FbsonError.h
 *  and modified by Doris
 */

#ifndef JSONB_JSONBERROR_H
#define JSONB_JSONBERROR_H

#include <stdint.h>

namespace doris {

/*
 * Error codes
 *
 * When adding a new error code, you also must add a corresponding error
 * message in JsonbErrMsg::getErrMsg()
 */
enum class JsonbErrType {
    E_NONE = 0,
    E_INVALID_VER,
    E_EMPTY_DOCUMENT,
    E_OUTPUT_FAIL,
    E_INVALID_DOCU,
    E_INVALID_SCALAR_VALUE,
    E_INVALID_KEY_STRING,
    E_INVALID_KEY_LENGTH,
    E_INVALID_STR,
    E_INVALID_OBJ,
    E_INVALID_ARR,
    E_INVALID_HEX,
    E_INVALID_OCTAL,
    E_INVALID_DECIMAL,
    E_INVALID_EXPONENT,
    E_HEX_OVERFLOW,
    E_OCTAL_OVERFLOW,
    E_DECIMAL_OVERFLOW,
    E_DOUBLE_OVERFLOW,
    E_OUTOFMEMORY,
    E_OUTOFBOUNDARY,
    E_KEYNOTEXIST,
    E_NOTARRAY,
    E_NOTOBJ,
    E_INVALID_OPER,
    E_INVALID_JSONB_OBJ,
    E_NESTING_LVL_OVERFLOW,
    E_INVALID_DOCU_COMPAT,

    // new error code should always be added above
    E_NUM_ERRORS
};

/*
 * Error messages
 */
class JsonbErrMsg {
public:
    static const char* getErrMsg(JsonbErrType err_code) {
        static_assert(sizeof(err_msg_) / sizeof(char*) == (unsigned)JsonbErrType::E_NUM_ERRORS + 1,
                      "JsonbErrMsg::err_msg_ array doesn't match the number of error types");

        return err_msg_[(unsigned)err_code];
    }

private:
    static const constexpr char* const err_msg_[] = {
            "OK", /* E_NONE */

            "Invalid document version",
            "Empty document",
            "Fatal error in writing JSONB",
            "Invalid document: document must be an object or an array",
            "Invalid scalar value",
            "Invalid key string",
            "Key length exceeds maximum size allowed (64 bytes)",
            "Invalid string value",
            "Invalid JSON object",
            "Invalid JSON array",
            "Invalid HEX number",
            "Invalid octal number",
            "Invalid decimal number",
            "Invalid exponent part of a number value",
            "HEX number overflow",
            "Octal number overflow",
            "Decimal number overflow",
            "Double number overflow",
            "Fatal error: out of memory",
            "Out of array boundary",
            "Key not found",
            "Not a JSON array value",
            "Not a JSON object value",
            "Invalid update operation",
            "Invalid JSONB object (internal)",
            "Object or array has too many nesting levels",
            "Invalid document: document must be an object or an array",

            nullptr /* E_NUM_ERRORS */
    };
};

/*
 * Error Info contains position and message of the error
 */
class JsonbErrInfo {
public:
    int32_t err_pos;
    const char* err_msg;

    JsonbErrInfo() : err_pos(0), err_msg(nullptr) {}
};

} // namespace doris

#endif // JSONB_JSONBERROR_H
