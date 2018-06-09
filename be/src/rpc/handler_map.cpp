// Copyright (C) 2007-2016 Hypertable, Inc.
//
// This file is part of Hypertable.
// 
// Hypertable is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 3
// of the License, or any later version.
//
// Hypertable is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.

#include "compat.h"
#include "io_handler_accept.h"
#include "handler_map.h"
#include "reactor_factory.h"

#include <chrono>

namespace palo {

void HandlerMap::insert_handler(IOHandlerAccept *handler) {
    std::lock_guard<std::mutex> lock(m_mutex);
    assert(m_accept_handler_map.find(handler->get_address())
           == m_accept_handler_map.end());
    m_accept_handler_map[handler->get_local_address()] = handler;
}

void HandlerMap::insert_handler(IOHandlerData *handler, bool checkout) {
    std::lock_guard<std::mutex> lock(m_mutex);
    assert(m_data_handler_map.find(handler->get_address())
           == m_data_handler_map.end());
    m_data_handler_map[handler->get_address()] = handler;
    if (checkout) {
        handler->increment_reference_count();
    }
}

void HandlerMap::insert_handler(IOHandlerDatagram *handler) {
    std::lock_guard<std::mutex> lock(m_mutex);
    assert(m_datagram_handler_map.find(handler->get_local_address())
           == m_datagram_handler_map.end());
    m_datagram_handler_map[handler->get_local_address()] = handler;
}

void HandlerMap::insert_handler(IOHandlerRaw *handler) {
    std::lock_guard<std::mutex> lock(m_mutex);
    assert(m_raw_handler_map.find(handler->get_address())
           == m_raw_handler_map.end());
    m_raw_handler_map[handler->get_address()] = handler;
}

int HandlerMap::checkout_handler(const CommAddress &addr,
                                 IOHandlerAccept **handler) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if ((*handler = lookup_accept_handler(addr.inet)) == 0) {
        return error::COMM_NOT_CONNECTED;
    }
    assert(!(*handler)->is_decomissioned());
    (*handler)->increment_reference_count();
    return error::OK;
}

int HandlerMap::checkout_handler(const CommAddress &addr,
                                 IOHandlerData **handler) {
    std::lock_guard<std::mutex> lock(m_mutex);
    InetAddr inet_addr;
    int error = 0;
    if ((error = translate_address(addr, &inet_addr)) != error::OK) {
        return error;
    }
    if ((*handler = lookup_data_handler(inet_addr)) == 0) {
        return error::COMM_NOT_CONNECTED;
    }
    assert(!(*handler)->is_decomissioned());
    (*handler)->increment_reference_count();
    return error::OK;
}

int HandlerMap::checkout_handler(const CommAddress &addr,
                                 IOHandlerDatagram **handler) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if ((*handler = lookup_datagram_handler(addr.inet)) == 0) {
        return error::COMM_NOT_CONNECTED;
    }
    assert(!(*handler)->is_decomissioned());
    (*handler)->increment_reference_count();
    return error::OK;
}

int HandlerMap::checkout_handler(const CommAddress &addr,
                                 IOHandlerRaw **handler) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if ((*handler = lookup_raw_handler(addr.inet)) == 0) {
        return error::COMM_NOT_CONNECTED;
    }
    assert(!(*handler)->is_decomissioned());
    (*handler)->increment_reference_count();
    return error::OK;
}

void HandlerMap::decrement_reference_count(IOHandler *handler) {
    std::lock_guard<std::mutex> lock(m_mutex);
    handler->decrement_reference_count();
}

int HandlerMap::contains_data_handler(const CommAddress &addr) {
    std::lock_guard<std::mutex> lock(m_mutex);
    IOHandlerData* handler = 0;
    InetAddr inet_addr;
    int error = 0;
    if ((error = translate_address(addr, &inet_addr)) != error::OK) {
        return error;
    }
    if ((handler = lookup_data_handler(inet_addr)) == 0) {
        return error::COMM_NOT_CONNECTED;
    }
    return error::OK;
}

int HandlerMap::set_alias(const InetAddr &addr, const InetAddr &alias) {
    std::lock_guard<std::mutex> lock(m_mutex);
    SockAddrMap<IOHandlerData *>::iterator iter;
    if (m_data_handler_map.find(alias) != m_data_handler_map.end()) {
        return error::COMM_CONFLICTING_ADDRESS;
    }
    if ((iter = m_data_handler_map.find(addr)) == m_data_handler_map.end()) {
        return error::COMM_NOT_CONNECTED;
    }
    (*iter).second->set_alias(alias);
    m_data_handler_map[alias] = (*iter).second;
    return error::OK;
}

int HandlerMap::remove_handler_unlocked(IOHandler *handler) {
    SockAddrMap<IOHandlerAccept *>::iterator aiter;
    SockAddrMap<IOHandlerData *>::iterator diter;
    SockAddrMap<IOHandlerDatagram *>::iterator dgiter;
    SockAddrMap<IOHandlerRaw *>::iterator riter;
    InetAddr local_addr = handler->get_local_address();
    InetAddr remote_addr;
    int error = 0;
    if ((error = translate_address(handler->get_address(), &remote_addr)) != error::OK) {
        return error;
    }
    if ((diter = m_data_handler_map.find(remote_addr)) != m_data_handler_map.end()) {
        assert(handler == diter->second);
        m_data_handler_map.erase(diter);
        // Remove alias
        remote_addr = handler->get_alias();
        if ((diter = m_data_handler_map.find(remote_addr)) != m_data_handler_map.end()) {
            assert(handler == diter->second);
            m_data_handler_map.erase(diter);
        }
    }
    else if ((dgiter = m_datagram_handler_map.find(local_addr))
             != m_datagram_handler_map.end()) {
        assert(handler == dgiter->second);
        m_datagram_handler_map.erase(dgiter);
    }
    else if ((aiter = m_accept_handler_map.find(local_addr))
             != m_accept_handler_map.end()) {
        assert(handler == aiter->second);
        m_accept_handler_map.erase(aiter);
    }
    else if ((riter = m_raw_handler_map.find(remote_addr))
             != m_raw_handler_map.end()) {
        assert(handler == riter->second);
        m_raw_handler_map.erase(riter);
    }
    else {
        return error::COMM_NOT_CONNECTED;
    }
    return error::OK;
}

int HandlerMap::remove_handler(IOHandler *handler) {
    std::lock_guard<std::mutex> lock(m_mutex);
    return remove_handler_unlocked(handler);
}

void HandlerMap::decomission_handler_unlocked(IOHandler *handler) {
    if (remove_handler_unlocked(handler) != error::OK) {
        assert(m_decomissioned_handlers.count(handler) > 0);
        return;
    }
    m_decomissioned_handlers.insert(handler);
    handler->decomission();
}

void HandlerMap::decomission_all() {
    std::lock_guard<std::mutex> lock(m_mutex);
    SockAddrMap<IOHandlerAccept *>::iterator aiter;
    SockAddrMap<IOHandlerData *>::iterator diter;
    SockAddrMap<IOHandlerDatagram *>::iterator dgiter;
    SockAddrMap<IOHandlerRaw *>::iterator riter;
    // IOHandlerData
    for (diter = m_data_handler_map.begin(); diter != m_data_handler_map.end(); ++diter) {
        m_decomissioned_handlers.insert(diter->second);
        diter->second->decomission();
    }
    m_data_handler_map.clear();
    // IOHandlerDatagram
    for (dgiter = m_datagram_handler_map.begin();
            dgiter != m_datagram_handler_map.end(); ++dgiter) {
        m_decomissioned_handlers.insert(dgiter->second);
        dgiter->second->decomission();
    }
    m_datagram_handler_map.clear();
    // IOHandlerAccept
    for (aiter = m_accept_handler_map.begin();
            aiter != m_accept_handler_map.end(); ++aiter) {
        m_decomissioned_handlers.insert(aiter->second);
        aiter->second->decomission();
    }
    m_accept_handler_map.clear();
    // IOHandlerRaw
    for (riter = m_raw_handler_map.begin();
            riter != m_raw_handler_map.end(); ++riter) {
        m_decomissioned_handlers.insert(riter->second);
        riter->second->decomission();
    }
    m_raw_handler_map.clear();
}

bool HandlerMap::destroy_ok(IOHandler *handler) {
    std::lock_guard<std::mutex> lock(m_mutex);
    bool is_decomissioned = m_decomissioned_handlers.count(handler) > 0;
    assert(!is_decomissioned || handler->is_decomissioned());
    return is_decomissioned && handler->reference_count() == 0;
}

bool HandlerMap::translate_proxy_address(const CommAddress &proxy_addr, InetAddr *addr) {
    InetAddr inet_addr;
    std::string hostname;
    assert(proxy_addr.is_proxy());
    if (!m_proxy_map.get_mapping(proxy_addr.proxy, hostname, inet_addr)) {
        return false;
    }
    if (addr) {
        *addr = inet_addr;
    }
    return true;
}

void HandlerMap::wait_for_empty() {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cond.wait(lock, [this]() {
        return m_decomissioned_handlers.empty();
    });
}

void HandlerMap::purge_handler(IOHandler *handler) {
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        assert(m_decomissioned_handlers.count(handler) > 0);
        assert(handler->reference_count() == 0);
        m_decomissioned_handlers.erase(handler);
        if (m_decomissioned_handlers.empty()) {
            m_cond.notify_all();
        }
    }
    handler->disconnect();
    delete handler;
}

int HandlerMap::add_proxy(
        const std::string &proxy, 
        const std::string &hostname, 
        const InetAddr &addr) {
    std::lock_guard<std::mutex> lock(m_mutex);
    ProxyMapT new_map;
    ProxyMapT invalidated_map;
    m_proxy_map.update_mapping(proxy, hostname, addr, invalidated_map, new_map);
    for (const auto &v : new_map) {
        IOHandler *handler = lookup_data_handler(v.second.addr);
        if (handler)
            handler->set_proxy(v.first);
    }
    return propagate_proxy_map(new_map);
}

int HandlerMap::remove_proxy(const std::string &proxy) {
    std::lock_guard<std::mutex> lock(m_mutex);
    ProxyMapT remove_map;
    m_proxy_map.remove_mapping(proxy, remove_map);
    if (!remove_map.empty()) {
        IOHandler *handler = 0;
        for (const auto &v : remove_map) {
            handler = lookup_data_handler(v.second.addr);
            if (handler)
                decomission_handler_unlocked(handler);
        }
        return propagate_proxy_map(remove_map);
    }
    return error::OK;
}

void HandlerMap::get_proxy_map(ProxyMapT &proxy_map) {
    m_proxy_map.get_map(proxy_map);
}

void HandlerMap::update_proxy_map(const char *message, size_t message_len) {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::string mappings(message, message_len);
    ProxyMapT new_map;
    ProxyMapT invalidated_map;
    assert(!ReactorFactory::proxy_master);
    m_proxy_map.update_mappings(mappings, invalidated_map, new_map);
    for (const auto &v : invalidated_map) {
        IOHandler *handler = lookup_data_handler(v.second.addr);
        if (handler) {
            if (v.second.hostname == "--DELETED--")
                decomission_handler_unlocked(handler);
        }
    }
    for (const auto &v : new_map) {
        IOHandler *handler = lookup_data_handler(v.second.addr);
        if (handler)
            handler->set_proxy(v.first);
    }
    //LOG(INFO) << "Updated proxy map = %s", m_proxy_map.to_str().c_str();
    m_proxies_loaded = true;
    m_cond_proxy.notify_all();
}

int32_t HandlerMap::propagate_proxy_map(IOHandlerData *handler) {
    std::lock_guard<std::mutex> lock(m_mutex);
    assert(ReactorFactory::proxy_master);
    CommBufPtr comm_buf = m_proxy_map.create_update_message();
    comm_buf->write_header_and_reset();
    return handler->send_message(comm_buf);
}

bool HandlerMap::wait_for_proxy_map(Timer &timer) {
    std::unique_lock<std::mutex> lock(m_mutex);
    timer.start();
    auto drop_time = std::chrono::steady_clock::now() +
                     std::chrono::milliseconds(timer.remaining());
    return m_cond_proxy.wait_until(lock, drop_time,
    [this]() {
        return m_proxies_loaded;
    });
}

int HandlerMap::propagate_proxy_map(ProxyMapT &mappings) {
    int last_error = error::OK;
    if (mappings.empty())
        return error::OK;
    SockAddrMap<IOHandlerData *>::iterator iter;
    std::string mapping;
    for (const auto &v : mappings) {
        mapping += v.first + "\t" + v.second.hostname + "\t" + InetAddr::format(v.second.addr) + "\n";
    }
    uint8_t *buffer = new uint8_t [ mapping.length() + 1 ];
    strcpy((char *)buffer, mapping.c_str());
    boost::shared_array<uint8_t> payload(buffer);
    CommHeader header;
    header.flags |= CommHeader::FLAGS_BIT_PROXY_MAP_UPDATE;
    std::vector<IOHandler *> decomission;
    for (iter = m_data_handler_map.begin(); iter != m_data_handler_map.end(); ++iter) {
        IOHandlerData *handler = iter->second;
        if (handler) {
            CommBufPtr comm_buf = std::make_shared<CommBuf>(header, 0, payload, mapping.length()+1);
            comm_buf->write_header_and_reset();
            int error = handler->send_message(comm_buf);
            if (error != error::OK) {
                decomission.push_back(handler);
                LOG(ERROR) << "Unable to propagate proxy mappings to %s - %s"
                           << InetAddr(handler->get_address()).format().c_str()
                           << error::get_text(error);
                last_error = error;
            }
        }
    }
    // Decomission any handlers that failed
    for (auto handler : decomission) {
        decomission_handler_unlocked(handler);
    }
    return last_error;
}

int HandlerMap::translate_address(const CommAddress &addr, InetAddr *inet_addr) {
    std::string hostname;
    assert(addr.is_set());
    if (addr.is_proxy()) {
        if (!m_proxy_map.get_mapping(addr.proxy, hostname, *inet_addr))
            return error::COMM_INVALID_PROXY;
    }
    else
        memcpy(inet_addr, &addr.inet, sizeof(InetAddr));
    return error::OK;
}

IOHandlerAccept *HandlerMap::lookup_accept_handler(const InetAddr &addr) {
    SockAddrMap<IOHandlerAccept *>::iterator iter = m_accept_handler_map.find(addr);
    if (iter != m_accept_handler_map.end())
        return iter->second;
    return 0;
}

IOHandlerData *HandlerMap::lookup_data_handler(const InetAddr &addr) {
    SockAddrMap<IOHandlerData *>::iterator iter = m_data_handler_map.find(addr);
    if (iter != m_data_handler_map.end())
        return iter->second;
    return 0;
}

IOHandlerDatagram *HandlerMap::lookup_datagram_handler(const InetAddr &addr) {
    SockAddrMap<IOHandlerDatagram *>::iterator iter = m_datagram_handler_map.find(addr);
    if (iter != m_datagram_handler_map.end())
        return iter->second;
    return 0;
}

IOHandlerRaw *HandlerMap::lookup_raw_handler(const InetAddr &addr) {
    SockAddrMap<IOHandlerRaw *>::iterator iter = m_raw_handler_map.find(addr);
    if (iter != m_raw_handler_map.end())
        return iter->second;
    return 0;
}

} //namespace palo
