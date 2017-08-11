// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include "compat.h"
#include "serialization.h"
#include "common/logging.h"
#include <cstdlib>
#include <cstring>
#include <assert.h>

extern "C" {
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
}

#include "inet_addr.h"
#include "string_ext.h"
namespace palo {
InetAddr::InetAddr() {
    //HT_EXPECT(sizeof(sockaddr_in) == sizeof(InetAddr), error::UNPOSSIBLE);
    assert(sizeof(sockaddr_in) == sizeof(InetAddr));
    memset(this, 0, sizeof(InetAddr));
}

InetAddr::InetAddr(const std::string &host, uint16_t port) {
    //HT_EXPECT(sizeof(sockaddr_in) == sizeof(InetAddr), error::UNPOSSIBLE);
    //HT_EXPECT(initialize(this, host.c_str(), port), error::BAD_DOMAIN_NAME);
    assert(sizeof(sockaddr_in) == sizeof(InetAddr));
    initialize(this, host.c_str(), port);
}

InetAddr::InetAddr(const std::string &endpoint) {
    //HT_EXPECT(sizeof(sockaddr_in) == sizeof(InetAddr), error::UNPOSSIBLE);
    //HT_EXPECT(initialize(this, endpoint.c_str()), error::BAD_DOMAIN_NAME);
    assert(sizeof(sockaddr_in) == sizeof(InetAddr));
    initialize(this, endpoint.c_str());
}

InetAddr::InetAddr(uint32_t ip32, uint16_t port) {
    //HT_EXPECT(sizeof(sockaddr_in) == sizeof(InetAddr), error::UNPOSSIBLE);
    assert(sizeof(sockaddr_in) == sizeof(InetAddr));
    initialize(this, ip32, port);
}

bool InetAddr::initialize(sockaddr_in *addr, const char *host, uint16_t port) {
    memset(addr, 0, sizeof(struct sockaddr_in));
    if (parse_ipv4(host, port, *addr)) {
        return true;
    }
    else {
        // Let's hope this is not broken in the glibc we're using
        struct hostent hent;
        struct hostent *he = 0;
        char hbuf[2048];
        int err = 0;
        if (gethostbyname_r(host, &hent, hbuf, sizeof(hbuf), &he, &err) != 0
                || he == 0) {
            LOG(ERROR) << "gethostbyname '%s': error: %d" << host << err;
            return false;
        }
        memcpy(&addr->sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
        if (addr->sin_addr.s_addr == 0) {
            uint8_t *ip = (uint8_t *)&addr->sin_addr.s_addr;
            ip[0] = 127;
            ip[3] = 1;
        }
    }
    addr->sin_family = AF_INET;
    addr->sin_port = htons(port);
    return true;
}

bool
InetAddr::is_ipv4(const char *ipin) {
    const char *ptr = ipin;
    const char *end = ipin + strlen(ipin);
    char* last = 0;
    int component = 0;
    int num_components = 0;
    int base = 10;
    while (ptr < end) {
        component = strtol(ptr, &last, base);
        num_components++;
        if (last == end) {
            break;
        }
        if (*last != '.' || last > end || component > 255
                || component < 0 || num_components > 4) {
            return false;
        }
        ptr = last + 1;
    }
    if (num_components != 4 || component > 255 || component < 0) {
        return false;
    }
    return true;
}

bool
InetAddr::parse_ipv4(const char *ipin, uint16_t port, sockaddr_in &addr,
                     int base) {
    uint8_t *ip = (uint8_t *)&addr.sin_addr.s_addr;
    const char* ipstr = ipin;
    const char* end = ipin + strlen(ipin);
    char* last = 0;
    int64_t n = strtoll(ipstr, &last, base);
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (last == end && n > 0 && n < UINT32_MAX) {
        addr.sin_addr.s_addr = htonl(n);
        return true;
    }
    *ip++ = n;
    if (last > end || *last != '.') {
        return false;
    }
    ipstr = last + 1;
    *ip++ = strtol(ipstr, &last, base);
    if (last >= end || *last != '.') {
        return false;
    }
    ipstr = last + 1;
    *ip++ = strtol(ipstr, &last, base);
    if (last >= end || *last != '.') {
        return false;
    }
    ipstr = last + 1;
    *ip++ = strtol(ipstr, &last, base);
    if (last != end) {
        return false;
    }
    if (addr.sin_addr.s_addr == 0) {
        uint8_t *ip = (uint8_t *)&addr.sin_addr.s_addr;
        ip[0] = 127;
        ip[3] = 1;
    }
    return true;
}

Endpoint InetAddr::parse_endpoint(const char *endpoint, int default_port) {
    const char *colon = strchr(endpoint, ':');
    if (colon) {
        std::string host = std::string(endpoint, colon - endpoint);
        return Endpoint(host, atoi(colon + 1));
    }
    return Endpoint(endpoint, default_port);
}

bool InetAddr::initialize(sockaddr_in *addr, const char *addr_str) {
    Endpoint e = parse_endpoint(addr_str);
    if (e.port) {
        return initialize(addr, e.host.c_str(), e.port);
    }
    return initialize(addr, "localhost", atoi(addr_str));
}

bool InetAddr::initialize(sockaddr_in *addr, uint32_t haddr, uint16_t port) {
    memset(addr, 0, sizeof(sockaddr_in));
    addr->sin_family = AF_INET;
    addr->sin_addr.s_addr = htonl(haddr);
    addr->sin_port = htons(port);
    return true;
}

std::string InetAddr::format(const sockaddr_in &addr, int sep) {
    // inet_ntoa is not thread safe on many platforms and deprecated
    const uint8_t *ip = (uint8_t *)&addr.sin_addr.s_addr;
    return palo::format("%d.%d.%d.%d%c%d", (int)ip[0], (int)ip[1],
                        (int)ip[2], (int)ip[3], sep, (int)ntohs(addr.sin_port));
}

std::string InetAddr::format_ipaddress(const sockaddr_in &addr) {
    // inet_ntoa is not thread safe on many platforms and deprecated
    const uint8_t *ip = (uint8_t *)&addr.sin_addr.s_addr;
    return palo::format("%d.%d.%d.%d", (int)ip[0], (int)ip[1],
                        (int)ip[2], (int)ip[3]);
}

std::string InetAddr::hex(const sockaddr_in &addr, int sep) {
    return palo::format("%x%c%x", ntohl(addr.sin_addr.s_addr), sep,
                        ntohs(addr.sin_port));
}

size_t InetAddr::encoded_length() const {
    size_t length = encoded_length_internal();
    return 1 + serialization::encoded_length_vi32(length) + length;
}

/**
 * @details
 * Encoding is as follows:
 * <table>
 * <tr>
 * <th>Encoding</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>1 byte</td>
 * <td>Encoding version as returned by encoding_version()</td>
 * </tr>
 * <tr>
 * <td>vint</td>
 * <td>Length of encoded object as returned by encoded_length_internal()</td>
 * </tr>
 * <tr>
 * <td>variable</td>
 * <td>Object encoded with encode_internal()</td>
 * </tr>
 * </table>
 */
void InetAddr::encode(uint8_t **bufp) const {
    serialization::encode_i8(bufp, encoding_version());
    serialization::encode_vi32(bufp, encoded_length_internal());
    encode_internal(bufp);
}

void InetAddr::decode(const uint8_t **bufp, size_t *remainp) {
    uint8_t version = serialization::decode_i8(bufp, remainp);
    //if (version > encoding_version())
    //HT_THROWF(error::PROTOCOL_ERROR, "Unsupported InetAddr version %d", (int)version);
    size_t encoding_length = serialization::decode_vi32(bufp, remainp);
    const uint8_t *end = *bufp + encoding_length;
    size_t tmp_remain = encoding_length;
    decode_internal(version, bufp, &tmp_remain);
    *remainp -= encoding_length;
    // If encoding is longer than we expect, that means we're decoding a newer
    // version, so skip the newer portion that we don't know about
    if (*bufp < end) {
        *bufp = end;
    }
}

void InetAddr::legacy_decode(const uint8_t **bufp, size_t *remainp) {
    serialization::decode_i8(bufp, remainp);
    sin_family = serialization::decode_i8(bufp, remainp);
    sin_port = serialization::decode_i16(bufp, remainp);
    sin_addr.s_addr = serialization::decode_i32(bufp, remainp);
}

uint8_t InetAddr::encoding_version() const {
    return 1;
}

size_t InetAddr::encoded_length_internal() const {
    return 8;
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr>
/// <th>Encoding</th>
/// <th>Description</th>
/// </tr>
/// <tr><td>i8</td><td>sizeof(sockaddr_in)</td></tr>
/// <tr><td>i8</td><td>Address family (sin_family)</td></tr>
/// <tr><td>i16</td><td>Port</td></tr>
/// <tr><td>i16</td><td>Address (sin_addr.s_addr)</td></tr>
/// </table>
void InetAddr::encode_internal(uint8_t **bufp) const {
    *(*bufp)++ = sizeof(sockaddr_in);
    *(*bufp)++ = sin_family;
    serialization::encode_i16(bufp, sin_port);
    serialization::encode_i32(bufp, sin_addr.s_addr);
}

void InetAddr::decode_internal(uint8_t version, const uint8_t **bufp,
                               size_t *remainp) {
    serialization::decode_i8(bufp, remainp);
    sin_family = serialization::decode_i8(bufp, remainp);
    sin_port = serialization::decode_i16(bufp, remainp);
    sin_addr.s_addr = serialization::decode_i32(bufp, remainp);
}

std::ostream &operator<<(std::ostream &out, const Endpoint &e) {
    out << e.host << ':' << e.port;
    return out;
}

std::ostream &operator<<(std::ostream &out, const sockaddr_in &a) {
    out << InetAddr::format(a);
    return out;
}

} // namespace palo
