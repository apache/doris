// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "comm.h"

extern "C" {
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
}

#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>
#include "compat.h"
#include "error.h"
#include "file_utils.h"
#include "inet_addr.h"
#include "io_handler_accept.h"
#include "io_handler_data.h"
#include "reactor_factory.h"
#include "reactor_runner.h"
#include "service/backend_options.h"
#include "scope_guard.h"

namespace palo {

std::atomic<uint32_t> Comm::ms_next_request_id(1);
Comm *Comm::ms_instance = NULL;
std::mutex Comm::ms_mutex;

Comm::Comm() {
    if (ReactorFactory::ms_reactors.size() == 0) {
        LOG(ERROR) << "reactor_factory::initialize must be called before creating "
                   << "rpc::comm object";
        abort();
    }
    InetAddr::initialize(&m_local_addr, BackendOptions::get_localhost().c_str(), 0);
    ReactorFactory::get_timer_reactor(m_timer_reactor);
    m_handler_map = ReactorRunner::handler_map;
}

Comm::~Comm() {
    m_handler_map->decomission_all();
    // wait for all decomissioned handlers to get purged by Reactor
    m_handler_map->wait_for_empty();
    // Since Comm is a singleton, this is OK
    ReactorFactory::destroy();
}

void Comm::destroy() {
    if (ms_instance) {
        delete ms_instance;
        ms_instance = 0;
    }
}

int Comm::register_socket(int sd, const CommAddress &addr,
                          RawSocketHandler *handler) {
    IOHandlerRaw *io_handler = 0;
    if (m_handler_map->checkout_handler(addr, &io_handler) == error::OK) {
        m_handler_map->decrement_reference_count(io_handler);
        return error::ALREADY_EXISTS;
    }
    assert(addr.is_inet());
    io_handler = new IOHandlerRaw(sd, addr.inet, handler);
    m_handler_map->insert_handler(io_handler);
    int32_t error;
    if ((error = io_handler->start_epolling(poll_event::READ | poll_event::WRITE)) != error::OK) {
        delete io_handler;
        LOG(ERROR) << "register socket to epoll failed."
                   << "[addr=" << addr.to_str().c_str() << ", "
                   << "socket=" << sd << ", "
                   << "error=" << error::get_text(error) << "]";
    }
    return error::OK;
}

int
Comm::connect(const CommAddress &addr, const DispatchHandlerPtr &default_handler) {
    int sd = -1;
    int error = m_handler_map->contains_data_handler(addr);
    uint16_t port;
    if (error == error::OK) {
        return error::COMM_ALREADY_CONNECTED;
    } else if (error != error::COMM_NOT_CONNECTED) {
        return error;
    }
    while (true) {
        if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
            LOG(ERROR) << "create socket failed. [error=" << strerror(errno) << "]";
            return error::COMM_SOCKET_ERROR;
        }
        // Get arbitray ephemeral port that won't conflict with our reserved ports
        port = (uint16_t)(49152 + std::uniform_int_distribution<>(0, 16382)(ReactorFactory::rng));
        m_local_addr.sin_port = htons(port);
        // bind socket to local address
        if ((::bind(sd, (const sockaddr *)&m_local_addr, sizeof(sockaddr_in))) < 0) {
            if (errno == EADDRINUSE) {
                ::close(sd);
                continue;
            }
            LOG(ERROR) << "bind socket failed. [addr=" << m_local_addr.format().c_str()
                       << ", error=" << strerror(errno) << "]";
            return error::COMM_BIND_ERROR;
        }
        break;
    }
    return connect_socket(sd, addr, default_handler);
}

int
Comm::connect(const CommAddress &addr, const CommAddress &local_addr,
              const DispatchHandlerPtr &default_handler) {
    int sd = -1;
    int error = m_handler_map->contains_data_handler(addr);
    assert(local_addr.is_inet());
    if (error == error::OK) {
        return error::COMM_ALREADY_CONNECTED;
    } else if (error != error::COMM_NOT_CONNECTED) {
        return error;
    }
    if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        LOG(ERROR) << "create socket failed. [error=" << strerror(errno) << "]";
        return error::COMM_SOCKET_ERROR;
    }
    // bind socket to local address
    if ((::bind(sd, (const sockaddr *)&local_addr.inet, sizeof(sockaddr_in))) < 0) {
        LOG(ERROR) << "bind socket failed. [addr=" << local_addr.to_str().c_str() << ", "
                   << "error=" << strerror(errno) << "]";
        return error::COMM_BIND_ERROR;
    }
    return connect_socket(sd, addr, default_handler);
}

int Comm::set_alias(const InetAddr &addr, const InetAddr &alias) {
    return m_handler_map->set_alias(addr, alias);
}

int Comm::listen(const CommAddress &addr, ConnectionHandlerFactoryPtr &chf) {
    DispatchHandlerPtr null_handler(0);
    return listen(addr, chf, null_handler);
}

int Comm::add_proxy(const std::string &proxy, const std::string &hostname, const InetAddr &addr) {
    assert(ReactorFactory::proxy_master);
    return m_handler_map->add_proxy(proxy, hostname, addr);
}

int Comm::remove_proxy(const std::string &proxy) {
    assert(ReactorFactory::proxy_master);
    return m_handler_map->remove_proxy(proxy);
}

bool Comm::translate_proxy(const std::string &proxy, InetAddr *addr) {
    CommAddress proxy_addr;
    proxy_addr.set_proxy(proxy);
    return m_handler_map->translate_proxy_address(proxy_addr, addr);
}

void Comm::get_proxy_map(ProxyMapT &proxy_map) {
    m_handler_map->get_proxy_map(proxy_map);
}

bool Comm::wait_for_proxy_load(Timer &timer) {
    return m_handler_map->wait_for_proxy_map(timer);
}

int
Comm::listen(const CommAddress &addr, ConnectionHandlerFactoryPtr &chf,
             const DispatchHandlerPtr &default_handler) {
    IOHandlerAccept *handler = 0;
    int one = 1;
    int sd = -1;
    int32_t error = 0;
    assert(addr.is_inet());
    if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        LOG(ERROR) << "create socket failed."
                   << "[socket=" << sd << ", error=" << strerror(errno) << "]";
        return error::COMM_SOCKET_ERROR;
    }
    // Set to non-blocking
    FileUtils::set_flags(sd, O_NONBLOCK);
    if (setsockopt(sd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0) {
        LOG(ERROR) << "set socket TCP_NODELAY option failed."
                   << "[socket=" << sd << ", error=" << strerror(errno) << "]";
    }
    if (setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
        LOG(ERROR) << "set socket SO_REUSEADDR option failed."
                   << "[socket=" << sd << ", [error=" << strerror(errno) << "]";
    }
    int bind_attempts = 0;
    while ((::bind(sd, (const sockaddr *)&addr.inet, sizeof(sockaddr_in))) < 0) {
        if (bind_attempts == 24) {
            LOG(ERROR) << "bind socket failed. [addr=" << addr.to_str().c_str() << ", "
                       << "error=" << strerror(errno) << "]";
            return error::COMM_BIND_ERROR;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        bind_attempts++;
    }
    if (::listen(sd, 1000) < 0) {
        LOG(ERROR) << "listen socket failed."
                   << "[socket=" << sd << ", addr=" << addr.to_str().c_str() << ", "
                   << "error=" << strerror(errno) << "]";
        return error::COMM_LISTEN_ERROR;
    }
    handler = new IOHandlerAccept(sd, default_handler, m_handler_map, chf);
    m_handler_map->insert_handler(handler);
    if ((error = handler->start_epolling()) != error::OK) {
        delete handler;
        LOG(ERROR) << "register socket to epoll failed."
                   << "[addr=" << addr.to_str().c_str() << ", "
                   << "socket=" << sd << ", "
                   << "error=" << error::get_text(error) << "]";
    }
    return error;
}

int
Comm::send_request(const CommAddress &addr, uint32_t timeout_ms,
                   CommBufPtr &cbuf, DispatchHandler *resp_handler) {
    IOHandlerData *data_handler = 0;
    int error = 0;
    if ((error = m_handler_map->checkout_handler(addr, &data_handler)) != error::OK) {
        LOG(WARNING) << "No connection. [addr=" << addr.to_str().c_str() << ", "
                     << "error=" << error::get_text(error) << "]";
        return error;
    }
    HT_ON_OBJ_SCOPE_EXIT(*
        m_handler_map.get(), 
        &HandlerMap::decrement_reference_count, 
        data_handler);
    return send_request(data_handler, timeout_ms, cbuf, resp_handler);
}

int Comm::send_request(IOHandlerData *data_handler, uint32_t timeout_ms,
                       CommBufPtr &cbuf, DispatchHandler *resp_handler) {
    cbuf->header.flags |= CommHeader::FLAGS_BIT_REQUEST;
    if (resp_handler == 0) {
        cbuf->header.flags |= CommHeader::FLAGS_BIT_IGNORE_RESPONSE;
        cbuf->header.id = 0;
    }
    else {
        cbuf->header.id = ms_next_request_id++;
        if (cbuf->header.id == 0) {
            cbuf->header.id = ms_next_request_id++;
        }
    }
    cbuf->header.timeout_ms = timeout_ms;
    cbuf->write_header_and_reset();
    int error = data_handler->send_message(cbuf, timeout_ms, resp_handler);
    if (error != error::OK) {
        m_handler_map->decomission_handler(data_handler);
    }

    return error;
}

int Comm::send_response(const CommAddress &addr, CommBufPtr &cbuf) {
    IOHandlerData *data_handler = 0;
    int error = 0;
    if ((error = m_handler_map->checkout_handler(addr, &data_handler)) != error::OK) {
        LOG(ERROR) << "No connection. [addr=" << addr.to_str().c_str()  << ", "
                   << "error=" << error::get_text(error) << "]";
        return error;
    }
    HT_ON_OBJ_SCOPE_EXIT(
        *m_handler_map.get(), 
        &HandlerMap::decrement_reference_count, 
        data_handler);
    cbuf->header.flags &= CommHeader::FLAGS_MASK_REQUEST;
    cbuf->write_header_and_reset();
    error = data_handler->send_message(cbuf);
    if (error != error::OK) {
        m_handler_map->decomission_handler(data_handler);
    }
    return error;
}

int
Comm::create_datagram_receive_socket(CommAddress &addr, int tos,
                                     const DispatchHandlerPtr &dhp) {
    IOHandlerDatagram *handler = 0;
    int sd = -1;
    int32_t error = 0;
    assert(addr.is_inet());
    if ((sd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        LOG(ERROR) << "create socket failed."
                   << "[socket=" << sd << ", error=" << strerror(errno) << "]";
        return error::COMM_SOCKET_ERROR;
    }
    // Set to non-blocking
    FileUtils::set_flags(sd, O_NONBLOCK);
    int bufsize = 4*32768;
    if (setsockopt(sd, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
        LOG(ERROR) << "set socket SO_SNDBUF option failed. [error=" << strerror(errno) << "]";
    }
    if (setsockopt(sd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
        LOG(ERROR) << "set sockopt SO_RCVBUF option failed. [error=" << strerror(errno) << "]";
    }
    Reactor::Priority reactor_priority {Reactor::Priority::NORMAL};
    if (tos) {
        int opt = tos;
        setsockopt(sd, SOL_IP, IP_TOS, &opt, sizeof(opt));
        opt = tos;
        setsockopt(sd, SOL_SOCKET, SO_PRIORITY, &opt, sizeof(opt));
        reactor_priority = Reactor::Priority::HIGH;
    }
    int bind_attempts = 0;
    while ((::bind(sd, (const sockaddr *)&addr.inet, sizeof(sockaddr_in))) < 0) {
        if (bind_attempts == 24) {
            LOG(ERROR) << "bind socket failed. [addr=" << addr.to_str().c_str() << ", "
                       << "error=" << strerror(errno) << "]";
            return error::COMM_BIND_ERROR;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10000));
        bind_attempts++;
    }
    handler = new IOHandlerDatagram(sd, dhp, reactor_priority);
    addr.set_inet( handler->get_address() );
    m_handler_map->insert_handler(handler);
    if ((error = handler->start_epolling()) != error::OK) {
        delete handler;
        LOG(ERROR) << "register socket to epoll failed."
                   << "[addr=" << addr.to_str().c_str() << ", "
                   << "socket=" << sd << ", "
                   << "error=" << error::get_text(error) << "]";
    }

    return error;
}

int
Comm::send_datagram(const CommAddress &addr, const CommAddress &send_addr,
                    CommBufPtr &cbuf) {
    IOHandlerDatagram *handler = 0;
    int error = 0;
    assert(addr.is_inet());
    if ((error = m_handler_map->checkout_handler(send_addr, &handler)) != error::OK) {
        LOG(ERROR) << "Datagram send/local address %s not registered" <<
                   send_addr.to_str().c_str();
        return error;
    }
    HT_ON_OBJ_SCOPE_EXIT(*m_handler_map.get(), &HandlerMap::decrement_reference_count, handler);
    cbuf->header.flags |= (CommHeader::FLAGS_BIT_REQUEST |
                           CommHeader::FLAGS_BIT_IGNORE_RESPONSE);
    cbuf->write_header_and_reset();
    error = handler->send_message(addr.inet, cbuf);
    if (error != error::OK) {
        m_handler_map->decomission_handler(handler);
    }
    return error;
}

int Comm::set_timer(uint32_t duration_millis, const DispatchHandlerPtr &handler) {
    ExpireTimer timer;
    timer.expire_time = ClockT::now() + std::chrono::milliseconds(duration_millis);
    timer.handler = handler;
    m_timer_reactor->add_timer(timer);
    return error::OK;
}

int
Comm::set_timer_absolute(ClockT::time_point expire_time, const DispatchHandlerPtr &handler) {
    ExpireTimer timer;
    timer.expire_time = expire_time;
    timer.handler = handler;
    m_timer_reactor->add_timer(timer);
    return error::OK;
}

void Comm::cancel_timer(const DispatchHandlerPtr &handler) {
    m_timer_reactor->cancel_timer(handler);
}

void Comm::close_socket(const CommAddress &addr) {
    IOHandler *handler = 0;
    IOHandlerAccept *accept_handler = 0;
    IOHandlerData *data_handler = 0;
    IOHandlerDatagram *datagram_handler = 0;
    IOHandlerRaw *raw_handler = 0;
    if (m_handler_map->checkout_handler(addr, &data_handler) == error::OK) {
        handler = data_handler;
    } else if (m_handler_map->checkout_handler(addr, &datagram_handler) == error::OK) {
        handler = datagram_handler;
    } else if (m_handler_map->checkout_handler(addr, &accept_handler) == error::OK) {
        handler = accept_handler;
    } else if (m_handler_map->checkout_handler(addr, &raw_handler) == error::OK) {
        handler = raw_handler;
    } else {
        return;
    }
    HT_ON_OBJ_SCOPE_EXIT(*m_handler_map.get(), &HandlerMap::decrement_reference_count, handler);
    m_handler_map->decomission_handler(handler);
}

void Comm::find_available_tcp_port(InetAddr &addr) {
    int one = 1;
    int sd = -1;
    InetAddr check_addr;
    uint16_t starting_port = ntohs(addr.sin_port);
    for (size_t i=0; i<15; i++) {
        if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
            LOG(FATAL) << "create socket failed. [error=" << strerror(errno) << "]";
        }
        if (setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
            LOG(FATAL) << "set socket SO_REUSEADDR option for TCP failed."
                       << "[error=" << strerror(errno) << "]";
        }
        check_addr = addr;
        check_addr.sin_port = htons(starting_port+i);
        if (::bind(sd, (const sockaddr *)&check_addr, sizeof(sockaddr_in)) == 0) {
            ::close(sd);
            addr.sin_port = check_addr.sin_port;
            return;
        }
        ::close(sd);
    }
    LOG(FATAL) << "find available TCP port failed."
               << "[range from " << (int)addr.sin_port << " "
               << "to " << (int)addr.sin_port+14 << "]";
}

void Comm::find_available_udp_port(InetAddr &addr) {
    int one = 1;
    int sd = -1;
    InetAddr check_addr;
    uint16_t starting_port = ntohs(addr.sin_port);
    for (size_t i=0; i<15; i++) {
        if ((sd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
            LOG(FATAL) << "create socket failed. [error=" << strerror(errno) << "]";
        }
        if (setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
            LOG(FATAL) << "set socket SO_REUSEADDR option for UDP failed."
                       << "[error=" << strerror(errno) << "]";
        }
        check_addr = addr;
        check_addr.sin_port = htons(starting_port+i);
        if (::bind(sd, (const sockaddr *)&addr, sizeof(sockaddr_in)) == 0) {
            ::close(sd);
            addr.sin_port = check_addr.sin_port;
            return;
        }
        ::close(sd);
    }
    LOG(FATAL) << "find available UDP port failed."
               << "[range from " << (int)addr.sin_port << " "
               << "to " << (int)addr.sin_port+14 << "]";
}

/**
 *  ----- Private methods -----
 */
int
Comm::connect_socket(int sd, const CommAddress &addr,
                     const DispatchHandlerPtr &default_handler) {
    IOHandlerData *handler = 0;
    int32_t error = 0;
    int one = 1;
    CommAddress connectable_addr;
    if (addr.is_proxy()) {
        InetAddr inet_addr;
        if (!m_handler_map->translate_proxy_address(addr, &inet_addr))
            return error::COMM_INVALID_PROXY;
        connectable_addr.set_inet(inet_addr);
    }
    else
        connectable_addr = addr;
    // Set to non-blocking
    FileUtils::set_flags(sd, O_NONBLOCK);
    if (setsockopt(sd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0) {
        LOG(ERROR) << "set socket TCP_NODELAY option failed. [error=" << strerror(errno) << "]";
    }
    handler = new IOHandlerData(sd, connectable_addr.inet, default_handler);
    if (addr.is_proxy())
        handler->set_proxy(addr.proxy);
    m_handler_map->insert_handler(handler);
    while (::connect(sd, (struct sockaddr *)&connectable_addr.inet, sizeof(struct sockaddr_in))
            < 0) {
        if (errno == EINTR) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            continue;
        }
        else if (errno == EINPROGRESS) {
            LOG(INFO) << "socket are in progress and starting to poll. [socket=" << sd << "]";
            error = handler->start_epolling(poll_event::READ | poll_event::WRITE);
            if (error == error::COMM_POLL_ERROR) {
                LOG(ERROR) << "register socket to epoll failed."
                           << "[addr=" << connectable_addr.to_str().c_str() << ", "
                           << "socket=" << sd << ", "
                           << "error=" << error::get_text(error) << "]";
                m_handler_map->remove_handler(handler);
                delete handler;
            }
            return error;
        }
        m_handler_map->remove_handler(handler);
        delete handler;
        LOG(ERROR) << "connect to addr failed. [addr=" << connectable_addr.to_str().c_str() << ", "
                   << "socket=" << sd << ", error=" << strerror(errno) << "]";
        return error::COMM_CONNECT_ERROR;
    }
    error = handler->start_epolling(poll_event::READ|poll_event::WRITE);
    if (error != error::OK) {
        LOG(ERROR) << "register socket to epoll failed."
                   << "[addr=" << connectable_addr.to_str().c_str() << ", "
                   << "socket=" << sd << ", "
                   << "error=" << error::get_text(error) << "]";
        m_handler_map->remove_handler(handler);
        delete handler;
    }
    return error;
}

} //nemespace palo
