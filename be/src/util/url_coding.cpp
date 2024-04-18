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

#include "util/url_coding.h"

#include <libbase64.h>
#include <math.h>

#include <memory>
#include <sstream>

namespace doris {

static inline void url_encode(const char* in, int in_len, std::string* out) {
    (*out).reserve(in_len);
    std::stringstream ss;

    for (int i = 0; i < in_len; ++i) {
        const char ch = in[i];

        // Escape the character iff a) we are in Hive-compat mode and the
        // character is in the Hive whitelist or b) we are not in
        // Hive-compat mode, and the character is not alphanumeric or one
        // of the four commonly excluded characters.
        ss << ch;
    }

    (*out) = ss.str();
}

void url_encode(const std::vector<uint8_t>& in, std::string* out) {
    if (in.empty()) {
        *out = "";
    } else {
        url_encode(reinterpret_cast<const char*>(&in[0]), in.size(), out);
    }
}

void url_encode(const std::string& in, std::string* out) {
    url_encode(in.c_str(), in.size(), out);
}

// Adapted from
// http://www.boost.org/doc/libs/1_40_0/doc/html/boost_asio/
//   example/http/server3/request_handler.cpp
// See http://www.boost.org/LICENSE_1_0.txt for license for this method.
bool url_decode(const std::string& in, std::string* out) {
    out->clear();
    out->reserve(in.size());

    for (size_t i = 0; i < in.size(); ++i) {
        if (in[i] == '%') {
            if (i + 3 <= in.size()) {
                int value = 0;
                std::istringstream is(in.substr(i + 1, 2));

                if (is >> std::hex >> value) {
                    (*out) += static_cast<char>(value);
                    i += 2;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else if (in[i] == '+') {
            (*out) += ' ';
        } else {
            (*out) += in[i];
        }
    }

    return true;
}

void base64_encode(const std::string& in, std::string* out) {
    out->resize(size_t(in.length() * (4.0 / 3) + 1));
    auto len = base64_encode(reinterpret_cast<const unsigned char*>(in.c_str()), in.length(),
                             (unsigned char*)out->c_str());
    out->resize(len);
}

size_t base64_encode(const unsigned char* data, size_t length, unsigned char* encoded_data) {
    size_t encode_len = 0;
#if defined(__aarch64__) || defined(_M_ARM64)
    do_base64_encode(reinterpret_cast<const char*>(data), length,
                     reinterpret_cast<char*>(encoded_data), &encode_len, BASE64_FORCE_NEON64);
#else
    do_base64_encode(reinterpret_cast<const char*>(data), length,
                     reinterpret_cast<char*>(encoded_data), &encode_len, 0);
#endif
    return encode_len;
}

int64_t base64_decode(const char* data, size_t length, char* decoded_data) {
    size_t decode_len = 0;
#if defined(__aarch64__) || defined(_M_ARM64)
    auto ret = do_base64_decode(reinterpret_cast<const char*>(data), length, decoded_data,
                                &decode_len, BASE64_FORCE_NEON64);
#else
    auto ret = do_base64_decode(reinterpret_cast<const char*>(data), length, decoded_data,
                                &decode_len, 0);
#endif
    return ret > 0 ? decode_len : -1;
}

bool base64_decode(const std::string& in, std::string* out) {
    out->resize(in.length());

    int64_t len = base64_decode(in.c_str(), in.length(), out->data());
    if (len < 0) {
        return false;
    }
    out->resize(len);
    return true;
}

void escape_for_html(const std::string& in, std::stringstream* out) {
    for (const auto& c : in) {
        switch (c) {
        case '<':
            (*out) << "&lt;";
            break;

        case '>':
            (*out) << "&gt;";
            break;

        case '&':
            (*out) << "&amp;";
            break;

        default:
            (*out) << c;
        }
    }
}

std::string escape_for_html_to_string(const std::string& in) {
    std::stringstream str;
    escape_for_html(in, &str);
    return str.str();
}
} // namespace doris
