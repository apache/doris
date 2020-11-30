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

#ifdef IR_COMPILE
extern "C" bool ir_string_to_bool(const char* s, int len, StringParser::ParseResult* result) {
    return StringParser::string_to_bool(s, len, result);
}

extern "C" int8_t ir_string_to_int8(const char* s, int len, StringParser::ParseResult* result) {
    return StringParser::string_to_int<int8_t>(s, len, result);
}

extern "C" int16_t ir_string_to_int16(const char* s, int len, StringParser::ParseResult* result) {
    return StringParser::string_to_int<int16_t>(s, len, result);
}

extern "C" int32_t ir_string_to_int32(const char* s, int len, StringParser::ParseResult* result) {
    return StringParser::string_to_int<int32_t>(s, len, result);
}

extern "C" int64_t ir_string_to_int64(const char* s, int len, StringParser::ParseResult* result) {
    return StringParser::StringToInt<int64_t>(s, len, result);
}

extern "C" float ir_string_to_float(const char* s, int len, StringParser::ParseResult* result) {
    return StringParser::StringToFloat<float>(s, len, result);
}

extern "C" double ir_string_to_double(const char* s, int len, StringParser::ParseResult* result) {
    return StringParser::StringToFloat<double>(s, len, result);
}

extern "C" bool ir_is_null_string(const char* data, int len) {
    return data == NULL || (len == 2 && data[0] == '\\' && data[1] == 'N');
}

extern "C" bool ir_generic_is_null_string(const char* s, int slen, const char* n, int nlen) {
    return s == NULL || (slen == nlen && StringCompare(s, slen, n, nlen, slen) == 0);
}
#endif
}
