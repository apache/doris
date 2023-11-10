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

#pragma once

#include <sstream>

#include "arrow/flight/types.h"
#include "arrow/util/base64.h"

namespace doris {
namespace flight {

const char kBearerDefaultToken[] = "bearertoken";
const char kBasicPrefix[] = "Basic ";
const char kBearerPrefix[] = "Bearer ";
const char kAuthHeader[] = "authorization";

// Function to look in CallHeaders for a key that has a value starting with prefix and
// return the rest of the value after the prefix.
std::string FindKeyValPrefixInCallHeaders(const arrow::flight::CallHeaders& incoming_headers,
                                          const std::string& key, const std::string& prefix) {
    // Lambda function to compare characters without case sensitivity.
    auto char_compare = [](const char& char1, const char& char2) {
        return (::toupper(char1) == ::toupper(char2));
    };

    auto iter = incoming_headers.find(key);
    if (iter == incoming_headers.end()) {
        return "";
    }
    const std::string val(iter->second);
    if (val.size() > prefix.length()) {
        if (std::equal(val.begin(), val.begin() + prefix.length(), prefix.begin(), char_compare)) {
            return val.substr(prefix.length());
        }
    }
    return "";
}

void ParseBasicHeader(const arrow::flight::CallHeaders& incoming_headers, std::string& username,
                      std::string& password) {
    std::string encoded_credentials =
            FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBasicPrefix);
    std::stringstream decoded_stream(arrow::util::base64_decode(encoded_credentials));
    std::getline(decoded_stream, username, ':');
    std::getline(decoded_stream, password, ':');
}

} // namespace flight
} // namespace doris
