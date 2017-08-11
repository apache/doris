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

#ifndef BDG_PALO_BE_SRC_RPC_SOCK_ADDR_MAP_H
#define BDG_PALO_BE_SRC_RPC_SOCK_ADDR_MAP_H

#include <unordered_map>
#include "inet_addr.h"

namespace palo {

/// Hash functor class for InetAddr.
class SockAddrHash {
public:
    size_t operator () (const InetAddr &addr) const {
        return (size_t)(addr.sin_addr.s_addr ^ addr.sin_port);
    }
};

/// Equality predicate functor class for InetAddr.
struct SockAddrEqual {
    bool operator()(const InetAddr &addr1, const InetAddr &addr2) const {
        return (addr1.sin_addr.s_addr == addr2.sin_addr.s_addr)
            && (addr1.sin_port == addr2.sin_port);
    }
};

/// Unordered map specialization for InetAddr keys.
template<typename TypeT, typename addr = InetAddr>
class SockAddrMap : public std::unordered_map<addr, TypeT, SockAddrHash, SockAddrEqual> {
};

} // namespace palo
#endif //BDG_PALO_BE_SRC_RPC_SOCK_ADDR_MAP_H
