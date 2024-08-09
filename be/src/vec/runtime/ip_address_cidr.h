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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/isIPAddressContainedIn.cpp
// and modified by Doris

#pragma once

#include "util/sse_util.hpp"
#include "vec/common/format_ip.h"
namespace doris {

class IPAddressVariant {
public:
    explicit IPAddressVariant(std::string_view address_str) {
        vectorized::Int64 v4 = 0;
        if (vectorized::parse_ipv4_whole(address_str.begin(), address_str.end(),
                                         reinterpret_cast<unsigned char*>(&v4))) {
            _addr = static_cast<vectorized::UInt32>(v4);
        } else {
            _addr = IPv6AddrType();
            // parse ipv6 in little-endian
            if (!vectorized::parse_ipv6_whole(address_str.begin(), address_str.end(),
                                              std::get<IPv6AddrType>(_addr).data())) {
                throw Exception(ErrorCode::INVALID_ARGUMENT, "Neither IPv4 nor IPv6 address: '{}'",
                                address_str);
            }
        }
    }

    vectorized::UInt32 as_v4() const {
        if (const auto* val = std::get_if<IPv4AddrType>(&_addr)) {
            return *val;
        }
        return 0;
    }

    const vectorized::UInt8* as_v6() const {
        if (const auto* val = std::get_if<IPv6AddrType>(&_addr)) {
            return val->data();
        }
        return nullptr;
    }

private:
    using IPv4AddrType = vectorized::UInt32;
    using IPv6AddrType = std::array<vectorized::UInt8, IPV6_BINARY_LENGTH>;

    std::variant<IPv4AddrType, IPv6AddrType> _addr;
};

struct IPAddressCIDR {
    IPAddressVariant _address;
    vectorized::UInt8 _prefix;
};

bool match_ipv4_subnet(uint32_t addr, uint32_t cidr_addr, uint8_t prefix) {
    uint32_t mask = (prefix >= 32) ? 0xffffffffu : ~(0xffffffffu >> prefix);
    return (addr & mask) == (cidr_addr & mask);
}

#if defined(__SSE2__) || defined(__aarch64__)

bool match_ipv6_subnet(const uint8_t* addr, const uint8_t* cidr_addr, uint8_t prefix) {
    uint16_t mask = _mm_movemask_epi8(
            _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(addr)),
                           _mm_loadu_si128(reinterpret_cast<const __m128i*>(cidr_addr))));
    mask = ~mask;

    if (mask) {
        const auto offset = std::countl_zero(mask);
        if (prefix / 8 != offset) {
            return prefix / 8 < offset;
        }
        auto cmpmask = ~(0xff >> (prefix % 8));
        return (addr[IPV6_BINARY_LENGTH - 1 - offset] & cmpmask) ==
               (cidr_addr[IPV6_BINARY_LENGTH - 1 - offset] & cmpmask);
    } else {
        // All the bytes are equal.
    }
    return true;
}

#else
// ipv6 liitle-endian input
bool match_ipv6_subnet(const uint8_t* addr, const uint8_t* cidr_addr, uint8_t prefix) {
    if (prefix > IPV6_BINARY_LENGTH * 8U) {
        prefix = IPV6_BINARY_LENGTH * 8U;
    }
    size_t i = IPV6_BINARY_LENGTH - 1;

    for (; prefix >= 8; --i, prefix -= 8) {
        if (addr[i] != cidr_addr[i]) {
            return false;
        }
    }

    if (prefix == 0) {
        return true;
    }

    auto mask = ~(0xff >> prefix);
    return (addr[i] & mask) == (cidr_addr[i] & mask);
}
#endif

IPAddressCIDR parse_ip_with_cidr(std::string_view cidr_str) {
    size_t pos_slash = cidr_str.find('/');

    if (pos_slash == 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Error parsing IP address with prefix: {}",
                        std::string(cidr_str));
    }

    if (pos_slash == std::string_view::npos) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "The text does not contain '/': {}",
                        std::string(cidr_str));
    }

    std::string_view addr_str = cidr_str.substr(0, pos_slash);
    IPAddressVariant addr(addr_str);

    uint8_t prefix = 0;
    auto prefix_str = cidr_str.substr(pos_slash + 1);

    const auto* prefix_str_end = prefix_str.data() + prefix_str.size();
    auto [parse_end, parse_error] = std::from_chars(prefix_str.data(), prefix_str_end, prefix);
    uint8_t max_prefix = (addr.as_v6() ? IPV6_BINARY_LENGTH : IPV4_BINARY_LENGTH) * 8;

    if (parse_error != std::errc() || parse_end != prefix_str_end || prefix > max_prefix) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "The CIDR has a malformed prefix bits: {}",
                        std::string(cidr_str));
    }

    return {addr, static_cast<uint8_t>(prefix)};
}

inline bool is_address_in_range(const IPAddressVariant& address, const IPAddressCIDR& cidr) {
    const auto* cidr_v6 = cidr._address.as_v6();
    const auto* addr_v6 = address.as_v6();
    if (cidr_v6) {
        if (addr_v6) {
            return match_ipv6_subnet(addr_v6, cidr_v6, cidr._prefix);
        }
    } else {
        if (!addr_v6) {
            return match_ipv4_subnet(address.as_v4(), cidr._address.as_v4(), cidr._prefix);
        }
    }
    return false;
}

} // namespace doris