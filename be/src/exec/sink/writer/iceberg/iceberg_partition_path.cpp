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

#include "exec/sink/writer/iceberg/iceberg_partition_path.h"

namespace doris {

namespace {

bool is_unescaped_url_encoder_char(unsigned char ch) {
    return ('A' <= ch && ch <= 'Z') || ('a' <= ch && ch <= 'z') || ('0' <= ch && ch <= '9') ||
           ch == '.' || ch == '-' || ch == '*' || ch == '_';
}

char hex_digit(unsigned char value) {
    return value < 10 ? static_cast<char>('0' + value) : static_cast<char>('A' + value - 10);
}

} // namespace

std::string IcebergPartitionPath::escape(const std::string& path) {
    std::string escaped;
    escaped.reserve(path.size());
    for (unsigned char ch : path) {
        if (is_unescaped_url_encoder_char(ch)) {
            escaped.push_back(static_cast<char>(ch));
        } else if (ch == ' ') {
            escaped.push_back('+');
        } else {
            escaped.push_back('%');
            escaped.push_back(hex_digit(ch >> 4));
            escaped.push_back(hex_digit(ch & 0x0F));
        }
    }
    return escaped;
}

} // namespace doris
