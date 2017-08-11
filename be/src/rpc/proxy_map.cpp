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
#include "proxy_map.h"
#include "string_ext.h"

namespace palo {

void ProxyMap::update_mapping(const std::string &proxy, const std::string &hostname,
                              const InetAddr &addr, ProxyMapT &invalidated_map,
                              ProxyMapT &new_map) {
    std::lock_guard<std::mutex> lock(m_mutex);
    ProxyMapT::iterator iter = m_forward_map.find(proxy);
    if (iter != m_forward_map.end() && (*iter).second.addr == addr) {
        if ((*iter).second.hostname != hostname) {
            (*iter).second.hostname = hostname;
        }
        return;
    }
    invalidate_old_mapping(proxy, addr, invalidated_map);
    new_map[proxy] = ProxyAddressInfo(hostname, addr);
    m_forward_map[proxy] = ProxyAddressInfo(hostname, addr);
    m_reverse_map[addr] = proxy;
}

void ProxyMap::update_mappings(std::string &mappings, ProxyMapT &invalidated_map,
                               ProxyMapT &new_map) {
    std::lock_guard<std::mutex> lock(m_mutex);
    char *line = 0;
    char *proxy = 0;
    char *hostname = 0;
    char *addr_str = 0;
    char *end_nl = 0;
    char *end_tab = 0;
    for (line = strtok_r((char *)mappings.c_str(), "\n", &end_nl); line;
            line = strtok_r(0, "\n", &end_nl)) {
        proxy = strtok_r(line, "\t", &end_tab);
        assert(proxy);
        hostname = strtok_r(0, "\t", &end_tab);
        assert(hostname);
        addr_str = strtok_r(0, "\t", &end_tab);
        assert(addr_str);
        InetAddr addr(addr_str);
        ProxyMapT::iterator iter = m_forward_map.find(proxy);
        if (!strcmp(hostname, "--DELETED--")) {
            if (iter != m_forward_map.end()) {
                if ((*iter).second.addr != addr) {
                    LOG(WARNING) << "Proxy map removal message for %s contains %s, but map "
                                 << "contains %s" << proxy << addr_str
                                 << (*iter).second.addr.format().c_str();
                }
                invalidate(proxy, invalidated_map);
            }
            else {
                LOG(WARNING) << "Removal message received for %s, but not in map." << proxy;
            }
        }
        else {
            if (iter != m_forward_map.end() && (*iter).second.addr == addr) {
                if ((*iter).second.hostname != hostname) {
                    (*iter).second.hostname = hostname;
                }
                continue;
            }
            invalidate_old_mapping(proxy, addr, invalidated_map);
            new_map[proxy] = ProxyAddressInfo(hostname, addr);
            m_forward_map[proxy] = ProxyAddressInfo(hostname, addr);
            m_reverse_map[addr] = proxy;
        }
    }
}

void ProxyMap::remove_mapping(const std::string &proxy, ProxyMapT &remove_map) {
    std::lock_guard<std::mutex> lock(m_mutex);
    invalidate(proxy, remove_map);
    return;
}

bool ProxyMap::get_mapping(const std::string &proxy, std::string &hostname, InetAddr &addr) {
    std::lock_guard<std::mutex> lock(m_mutex);
    ProxyMapT::iterator iter = m_forward_map.find(proxy);
    if (iter == m_forward_map.end()) {
        return false;
    }
    addr = (*iter).second.addr;
    hostname = (*iter).second.hostname;
    return true;
}

std::string ProxyMap::get_proxy(InetAddr &addr) {
    std::lock_guard<std::mutex> lock(m_mutex);
    SockAddrMap<std::string>::iterator iter = m_reverse_map.find(addr);
    if (iter != m_reverse_map.end()) {
        return (*iter).second;
    }
    return "";
}

CommBufPtr ProxyMap::create_update_message() {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::string payload;
    CommHeader header;
    header.flags |= CommHeader::FLAGS_BIT_PROXY_MAP_UPDATE;
    for (ProxyMapT::iterator iter = m_forward_map.begin(); iter != m_forward_map.end(); ++iter) {
        payload += (*iter).first + "\t" 
                   + (*iter).second.hostname + "\t" 
                   + (*iter).second.addr.format() + "\n";
    }
    CommBufPtr cbuf = std::make_shared<CommBuf>(header, payload.length());
    if (payload.length()) {
        cbuf->append_bytes((uint8_t *)payload.c_str(), payload.length());
    }
    return cbuf;
}

void
ProxyMap::invalidate_old_mapping(const std::string &proxy, const InetAddr &addr,
                                 ProxyMapT &invalidated_map) {
    ProxyMapT::iterator iter;
    SockAddrMap<std::string>::iterator rev_iter;
    // Invalidate entries from forward map, if changed
    if ((iter = m_forward_map.find(proxy)) != m_forward_map.end()) {
        if ((*iter).second.addr != addr) {
            invalidated_map[(*iter).first] = (*iter).second;
            m_reverse_map.erase((*iter).second.addr);
            m_forward_map.erase((*iter).first);
        }
    }
    // Invalidate entries from reverse map, if changed
    if ((rev_iter = m_reverse_map.find(addr)) != m_reverse_map.end()) {
        if ((*rev_iter).second != proxy) {
            invalidated_map[(*rev_iter).second] = ProxyAddressInfo("unknown", (*rev_iter).first);
            m_forward_map.erase((*rev_iter).second);
            m_reverse_map.erase((*rev_iter).first);
        }
    }
}

void ProxyMap::invalidate(const std::string &proxy, ProxyMapT &invalidated_map) {
    ProxyMapT::iterator iter;
    SockAddrMap<std::string>::iterator rev_iter;
    InetAddr addr;
    // Invalidate entries from forward map
    if ((iter = m_forward_map.find(proxy)) != m_forward_map.end()) {
        (*iter).second.hostname = "--DELETED--";
        invalidated_map[(*iter).first] = (*iter).second;
        addr = (*iter).second.addr;
        m_forward_map.erase((*iter).first);
    }
    else
        return;
    // Invalidate entries from reverse map
    if ((rev_iter = m_reverse_map.find(addr)) != m_reverse_map.end()) {
        if ((*rev_iter).second == proxy) {
            m_reverse_map.erase((*rev_iter).first);
        }
    }
}

std::string ProxyMap::to_str() {
    std::lock_guard<std::mutex> lock(m_mutex);
    ProxyMapT::iterator iter;
    std::string str;
    for (iter = m_forward_map.begin(); iter != m_forward_map.end(); ++iter) {
        str += format("(%s,%s,%s),", (*iter).first.c_str(),
                      (*iter).second.hostname.c_str(), (*iter).second.addr.format().c_str());
    }
    return str;
}

} //namespace palo
