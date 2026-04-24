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

#include "util/http_url_security.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

namespace doris {
namespace {

struct IpAddress {
    int family = AF_UNSPEC;
    std::array<uint8_t, 16> bytes {};
    size_t size = 0;
};

struct Cidr {
    IpAddress address;
    int prefix = 0;
};

struct AddrInfoDeleter {
    void operator()(addrinfo* info) const { freeaddrinfo(info); }
};

std::string trim(std::string_view value) {
    size_t begin = 0;
    while (begin < value.size() && std::isspace(static_cast<unsigned char>(value[begin]))) {
        ++begin;
    }
    size_t end = value.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
        --end;
    }
    return std::string(value.substr(begin, end - begin));
}

std::string to_lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return value;
}

std::string normalize_host(std::string_view host) {
    std::string normalized = to_lower(trim(host));
    if (normalized.size() >= 2 && normalized.front() == '[' && normalized.back() == ']') {
        normalized = normalized.substr(1, normalized.size() - 2);
    }
    if (!normalized.empty() && normalized.back() == '.') {
        normalized.pop_back();
    }
    return normalized;
}

bool parse_prefix(std::string_view value, int* prefix) {
    auto trimmed = trim(value);
    auto result = std::from_chars(trimmed.data(), trimmed.data() + trimmed.size(), *prefix);
    return result.ec == std::errc() && result.ptr == trimmed.data() + trimmed.size();
}

bool parse_ip(std::string_view value, IpAddress* address) {
    std::string host = normalize_host(value);
    in_addr ipv4_addr {};
    if (inet_pton(AF_INET, host.c_str(), &ipv4_addr) == 1) {
        address->family = AF_INET;
        address->size = 4;
        std::memcpy(address->bytes.data(), &ipv4_addr, address->size);
        return true;
    }

    in6_addr ipv6_addr {};
    if (inet_pton(AF_INET6, host.c_str(), &ipv6_addr) == 1) {
        address->family = AF_INET6;
        address->size = 16;
        std::memcpy(address->bytes.data(), &ipv6_addr, address->size);
        return true;
    }
    return false;
}

bool parse_cidr(std::string_view entry, Cidr* cidr) {
    size_t slash_pos = entry.find('/');
    if (slash_pos == std::string_view::npos) {
        return false;
    }

    if (!parse_ip(entry.substr(0, slash_pos), &cidr->address)) {
        return false;
    }
    if (!parse_prefix(entry.substr(slash_pos + 1), &cidr->prefix)) {
        return false;
    }
    int max_prefix = cidr->address.family == AF_INET ? 32 : 128;
    return cidr->prefix >= 0 && cidr->prefix <= max_prefix;
}

bool prefix_matches(const IpAddress& address, const Cidr& cidr) {
    if (address.family != cidr.address.family || address.size != cidr.address.size) {
        return false;
    }

    int full_bytes = cidr.prefix / 8;
    int remaining_bits = cidr.prefix % 8;
    if (full_bytes > 0 &&
        std::memcmp(address.bytes.data(), cidr.address.bytes.data(), full_bytes) != 0) {
        return false;
    }
    if (remaining_bits == 0) {
        return true;
    }
    uint8_t mask = static_cast<uint8_t>(0xffU << (8 - remaining_bits));
    return (address.bytes[full_bytes] & mask) == (cidr.address.bytes[full_bytes] & mask);
}

bool sockaddr_to_ip(const sockaddr* addr, socklen_t addrlen, IpAddress* ip) {
    if (addr == nullptr) {
        return false;
    }
    if (addr->sa_family == AF_INET) {
        if (addrlen < sizeof(sockaddr_in)) {
            return false;
        }
        const auto* ipv4 = reinterpret_cast<const sockaddr_in*>(addr);
        ip->family = AF_INET;
        ip->size = 4;
        std::memcpy(ip->bytes.data(), &ipv4->sin_addr, ip->size);
        return true;
    }
    if (addr->sa_family == AF_INET6) {
        if (addrlen < sizeof(sockaddr_in6)) {
            return false;
        }
        const auto* ipv6 = reinterpret_cast<const sockaddr_in6*>(addr);
        ip->family = AF_INET6;
        ip->size = 16;
        std::memcpy(ip->bytes.data(), &ipv6->sin6_addr, ip->size);
        return true;
    }
    return false;
}

std::string ip_to_string(const IpAddress& address) {
    char buffer[INET6_ADDRSTRLEN] = {};
    const void* src = address.bytes.data();
    if (inet_ntop(address.family, src, buffer, sizeof(buffer)) == nullptr) {
        return "<invalid>";
    }
    return buffer;
}

bool is_ipv4_mapped_ipv6(const IpAddress& address) {
    if (address.family != AF_INET6 || address.size != 16) {
        return false;
    }
    for (int i = 0; i < 10; ++i) {
        if (address.bytes[i] != 0) {
            return false;
        }
    }
    return address.bytes[10] == 0xff && address.bytes[11] == 0xff;
}

bool is_unsafe_ipv4(const uint8_t* bytes) {
    uint8_t first = bytes[0];
    uint8_t second = bytes[1];
    uint8_t third = bytes[2];
    return first == 0 || first == 10 || first == 127 ||
           (first == 100 && second >= 64 && second <= 127) || (first == 169 && second == 254) ||
           (first == 172 && second >= 16 && second <= 31) ||
           (first == 192 && second == 0 && (third == 0 || third == 2)) ||
           (first == 192 && second == 88 && third == 99) || (first == 192 && second == 168) ||
           (first == 198 && (second == 18 || second == 19)) ||
           (first == 198 && second == 51 && third == 100) ||
           (first == 203 && second == 0 && third == 113) || first >= 224;
}

bool is_unsafe_ipv6(const IpAddress& address) {
    if (is_ipv4_mapped_ipv6(address)) {
        return is_unsafe_ipv4(address.bytes.data() + 12);
    }

    bool first_12_zero = true;
    bool all_zero = true;
    for (int i = 0; i < 16; ++i) {
        if (address.bytes[i] != 0) {
            all_zero = false;
            if (i < 12) {
                first_12_zero = false;
            }
        }
    }
    if (all_zero) {
        return true;
    }
    bool loopback = first_12_zero && address.bytes[12] == 0 && address.bytes[13] == 0 &&
                    address.bytes[14] == 0 && address.bytes[15] == 1;
    if (loopback || first_12_zero) {
        return true;
    }

    uint8_t first = address.bytes[0];
    uint8_t second = address.bytes[1];
    return (first == 0x20 && second == 0x01 && address.bytes[2] == 0x0d &&
            address.bytes[3] == 0xb8) ||
           ((first & 0xfe) == 0xfc) || (first == 0xfe && (second & 0xc0) == 0x80) || first == 0xff;
}

bool is_unsafe_address(const IpAddress& address) {
    if (address.family == AF_INET) {
        return is_unsafe_ipv4(address.bytes.data());
    }
    if (address.family == AF_INET6) {
        return is_unsafe_ipv6(address);
    }
    return true;
}

bool is_host_allowed(const std::string& host, const std::vector<std::string>& allowlist) {
    for (const auto& entry : allowlist) {
        std::string trimmed = trim(entry);
        if (trimmed.empty() || trimmed.find('/') != std::string::npos) {
            continue;
        }
        if (normalize_host(trimmed) == host) {
            return true;
        }
    }
    return false;
}

bool is_address_allowed(const IpAddress& address, const std::vector<std::string>& allowlist) {
    for (const auto& entry : allowlist) {
        std::string trimmed = trim(entry);
        if (trimmed.empty()) {
            continue;
        }
        Cidr cidr;
        if (trimmed.find('/') != std::string::npos) {
            if (parse_cidr(trimmed, &cidr) && prefix_matches(address, cidr)) {
                return true;
            }
            continue;
        }

        IpAddress allowed_address;
        if (parse_ip(trimmed, &allowed_address) && address.family == allowed_address.family &&
            address.size == allowed_address.size &&
            std::memcmp(address.bytes.data(), allowed_address.bytes.data(), address.size) == 0) {
            return true;
        }
    }
    return false;
}

Status validate_ip_address(const std::string& host, const IpAddress& address,
                           const std::vector<std::string>& allowlist) {
    if (is_address_allowed(address, allowlist) || !is_unsafe_address(address)) {
        return Status::OK();
    }
    return Status::InvalidArgument("HTTP TVF URL resolves to unsafe address: {} -> {}", host,
                                   ip_to_string(address));
}

} // namespace

Status HttpUrlSecurity::validate_url(const std::string& url,
                                     const std::vector<std::string>& allowlist) {
    return validate_url(url, allowlist, nullptr);
}

Status HttpUrlSecurity::validate_url(const std::string& url,
                                     const std::vector<std::string>& allowlist, std::string* host) {
    std::string parsed_host;
    RETURN_IF_ERROR(parse_url_host(url, &parsed_host));
    if (host != nullptr) {
        *host = parsed_host;
    }
    if (is_host_allowed(parsed_host, allowlist)) {
        return Status::OK();
    }

    IpAddress literal_address;
    if (parse_ip(parsed_host, &literal_address)) {
        return validate_ip_address(parsed_host, literal_address, allowlist);
    }

    addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* result = nullptr;
    int code = getaddrinfo(parsed_host.c_str(), nullptr, &hints, &result);
    if (code != 0) {
        return Status::InvalidArgument("Failed to resolve HTTP TVF URL host: {}, error: {}",
                                       parsed_host, gai_strerror(code));
    }
    std::unique_ptr<addrinfo, AddrInfoDeleter> result_guard(result);
    for (addrinfo* item = result; item != nullptr; item = item->ai_next) {
        IpAddress address;
        if (!sockaddr_to_ip(item->ai_addr, item->ai_addrlen, &address)) {
            return Status::InvalidArgument(
                    "HTTP TVF URL resolved to unsupported address family: {}", parsed_host);
        }
        RETURN_IF_ERROR(validate_ip_address(parsed_host, address, allowlist));
    }
    return Status::OK();
}

Status HttpUrlSecurity::validate_resolved_address(const std::string& host, const sockaddr* addr,
                                                  socklen_t addrlen,
                                                  const std::vector<std::string>& allowlist) {
    std::string normalized_host = normalize_host(host);
    if (is_host_allowed(normalized_host, allowlist)) {
        return Status::OK();
    }

    IpAddress address;
    if (!sockaddr_to_ip(addr, addrlen, &address)) {
        return Status::InvalidArgument("HTTP TVF URL resolved to unsupported address family: {}",
                                       normalized_host);
    }
    return validate_ip_address(normalized_host, address, allowlist);
}

std::vector<std::string> HttpUrlSecurity::parse_allowlist(std::string_view allowlist) {
    std::vector<std::string> result;
    size_t start = 0;
    while (start <= allowlist.size()) {
        size_t comma = allowlist.find(',', start);
        if (comma == std::string_view::npos) {
            comma = allowlist.size();
        }
        std::string entry = trim(allowlist.substr(start, comma - start));
        if (!entry.empty()) {
            result.emplace_back(std::move(entry));
        }
        start = comma + 1;
    }
    return result;
}

Status HttpUrlSecurity::parse_url_host(const std::string& url, std::string* host) {
    size_t scheme_end = url.find("://");
    if (scheme_end == std::string::npos) {
        return Status::InvalidArgument("Invalid HTTP TVF URL: {}", url);
    }
    std::string scheme = to_lower(url.substr(0, scheme_end));
    if (scheme != "http" && scheme != "https") {
        return Status::InvalidArgument("HTTP TVF only supports http and https URLs: {}", url);
    }

    size_t authority_begin = scheme_end + 3;
    size_t authority_end = url.find_first_of("/?#", authority_begin);
    std::string authority = authority_end == std::string::npos
                                    ? url.substr(authority_begin)
                                    : url.substr(authority_begin, authority_end - authority_begin);
    if (authority.empty()) {
        return Status::InvalidArgument("HTTP TVF URL host is empty: {}", url);
    }
    if (authority.find('@') != std::string::npos) {
        return Status::InvalidArgument("HTTP TVF URL must not include user info: {}", url);
    }

    std::string parsed_host;
    if (authority.front() == '[') {
        size_t close = authority.find(']');
        if (close == std::string::npos) {
            return Status::InvalidArgument("Invalid HTTP TVF IPv6 URL host: {}", url);
        }
        parsed_host = authority.substr(1, close - 1);
        if (close + 1 < authority.size() && authority[close + 1] != ':') {
            return Status::InvalidArgument("Invalid HTTP TVF URL authority: {}", url);
        }
    } else {
        size_t colon = authority.find(':');
        if (colon != std::string::npos && authority.find(':', colon + 1) != std::string::npos) {
            return Status::InvalidArgument(
                    "HTTP TVF IPv6 URL host must be enclosed in brackets: {}", url);
        }
        parsed_host = colon == std::string::npos ? authority : authority.substr(0, colon);
    }

    *host = normalize_host(parsed_host);
    if (host->empty()) {
        return Status::InvalidArgument("HTTP TVF URL host is empty: {}", url);
    }
    return Status::OK();
}

} // namespace doris
