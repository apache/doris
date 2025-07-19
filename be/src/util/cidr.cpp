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

#include "util/cidr.h"

#include <arpa/inet.h>
#include <sys/socket.h>

#include <algorithm>
#include <cstddef>
#include <exception>
#include <iterator>
#include <ostream>

#include "common/logging.h"

namespace doris {
#include "common/compile_check_begin.h"

constexpr std::uint8_t kIPv4Bits = 32;
constexpr std::uint8_t kIPv6Bits = 128;

bool CIDR::reset(const std::string& cidr_str) {
    auto slash = std::find(std::begin(cidr_str), std::end(cidr_str), '/');
    auto ip = (slash == std::end(cidr_str)) ? cidr_str
                                            : cidr_str.substr(0, slash - std::begin(cidr_str));

    if (inet_pton(AF_INET, ip.c_str(), _address.data())) {
        _family = AF_INET;
        _netmask_len = kIPv4Bits;
    } else if (inet_pton(AF_INET6, ip.c_str(), _address.data())) {
        _family = AF_INET6;
        _netmask_len = kIPv6Bits;
    } else {
        LOG(WARNING) << "Wrong CIDRIP format. network=" << cidr_str;
        return false;
    }

    if (slash == std::end(cidr_str)) {
        return true;
    }

    std::size_t pos;
    std::string suffix = std::string(slash + 1, std::end(cidr_str));
    int len;
    try {
        len = std::stoi(suffix, &pos);
    } catch (const std::exception& e) {
        LOG(WARNING) << "Wrong CIDR format. network=" << cidr_str << ", reason=" << e.what();
        return false;
    }

    if (pos != suffix.size()) {
        LOG(WARNING) << "Wrong CIDR format. network=" << cidr_str;
        return false;
    }

    if (len < 0 || len > _netmask_len) {
        LOG(WARNING) << "Wrong CIDR mask format. network=" << cidr_str;
        return false;
    }
    _netmask_len = static_cast<std::uint8_t>(len);
    return true;
}

bool CIDR::contains(const CIDR& ip) const {
    if ((_family != ip._family) || (_netmask_len > ip._netmask_len)) {
        return false;
    }
    auto bytes = _netmask_len / 8;
    auto cidr_begin = _address.cbegin();
    auto ip_begin = ip._address.cbegin();
    if (!std::equal(cidr_begin, cidr_begin + bytes, ip_begin, ip_begin + bytes)) {
        return false;
    }
    if ((_netmask_len % 8) == 0) {
        return true;
    }
    auto mask = (0xFF << (8 - (_netmask_len % 8))) & 0xFF;
    return (_address[bytes] & mask) == (ip._address[bytes] & mask);
}
#include "common/compile_check_end.h"
} // end namespace doris
