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

#include "common/logging.h"
#include "gutil/strings/split.h"

namespace doris {

CIDR::CIDR() : _address(0), _netmask(0xffffffff) {}

void CIDR::reset() {
    _address = 0;
    _netmask = 0xffffffff;
}

bool CIDR::reset(const std::string& cidr_str) {
    reset();

    // check if have mask
    std::string cidr_format_str = cidr_str;
    int32_t have_mask = cidr_str.find("/");
    if (have_mask == -1) {
        cidr_format_str.assign(cidr_str + "/32");
    }
    VLOG_CRITICAL << "cidr format str: " << cidr_format_str;

    std::vector<std::string> cidr_items = strings::Split(cidr_format_str, "/");
    if (cidr_items.size() != 2) {
        LOG(WARNING) << "wrong CIDR format. network=" << cidr_str;
        return false;
    }

    if (cidr_items[1].empty()) {
        LOG(WARNING) << "wrong CIDR mask format. network=" << cidr_str;
        return false;
    }

    char* endptr = nullptr;
    int32_t mask_length = strtol(cidr_items[1].c_str(), &endptr, 10);
    if (errno != 0 && mask_length == 0) {
        char errmsg[64];
        // Ignore unused return value
        auto ret = strerror_r(errno, errmsg, 64);
        LOG(WARNING) << "wrong CIDR mask format. network=" << cidr_str
                     << ", mask_length=" << mask_length << ", errno=" << errno
                     << ", errmsg=" << errmsg << ", strerror_r returns=" << ret;
        return false;
    }
    if (mask_length <= 0 || mask_length > 32) {
        LOG(WARNING) << "wrong CIDR mask format. network=" << cidr_str
                     << ", mask_length=" << mask_length;
        return false;
    }

    uint32_t address = 0;
    if (!ip_to_int(cidr_items[0], &address)) {
        LOG(WARNING) << "wrong CIDR IP value. network=" << cidr_str;
        return false;
    }
    _address = address;

    _netmask = 0xffffffff;
    _netmask = _netmask << (32 - mask_length);
    return true;
}

bool CIDR::ip_to_int(const std::string& ip_str, uint32_t* value) {
    struct in_addr addr;
    int flag = inet_aton(ip_str.c_str(), &addr);
    if (flag == 0) {
        return false;
    }
    *value = ntohl(addr.s_addr);
    return true;
}

bool CIDR::contains(uint32_t ip_int) {
    if ((_address & _netmask) == (ip_int & _netmask)) {
        return true;
    }
    return false;
}

bool CIDR::contains(const std::string& ip) {
    uint32_t ip_int = 0;
    if (!ip_to_int(ip, &ip_int)) {
        return false;
    }

    return contains(ip_int);
}

} // end namespace doris
