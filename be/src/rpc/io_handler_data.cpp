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
#include "io_handler_data.h"
#include "reactor_runner.h"
#include "error.h"
#include "file_utils.h"
#include "inet_addr.h"

#include <cassert>
#include <chrono>
#include <iostream>

extern "C" {
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
}

namespace palo {

ssize_t
IOHandlerData::et_socket_read(int fd, void *vptr, size_t n, int *errnop, bool *eofp) {
    size_t nleft = n;
    ssize_t nread = 0;
    char *ptr = (char *)vptr;
    while (nleft > 0) {
        if ((nread = ::read(fd, ptr, nleft)) < 0) {
            if (errno == EINTR) {
                nread = 0; /* and call read() again */
                continue;
            }
            *errnop = errno;
            if (*errnop == EAGAIN || nleft < n) {
                break;     /* already read something, most likely EAGAIN */
            }
            return -1;   /* other errors and nothing read */
        }
        else if (nread == 0) {
            *eofp = true;
            break;
        }
        nleft -= nread;
        ptr   += nread;
    }
    return n - nleft;
}

ssize_t
IOHandlerData::et_socket_writev(int fd, const iovec *vector, int count, int *errnop) {
    ssize_t nwritten = 0;
    while ((nwritten = writev(fd, vector, count)) <= 0) {
        if (errno == EINTR) {
            nwritten = 0; /* and call write() again */
            continue;
        }
        *errnop = errno;
        return -1;
    }
    return nwritten;
}

bool
IOHandlerData::handle_event(struct epoll_event *event,
                            ClockT::time_point arrival_time) {
    int error = 0;
    bool eof = false;
    try {
        if (event->events & EPOLLOUT) {
            if (handle_write_readiness()) {
                handle_disconnect();
                return true;
            }
        }
        if (event->events & EPOLLIN) {
            size_t nread = 0;
            while (true) {
                if (!m_got_header) {
                    nread = et_socket_read(m_sd, m_message_header_ptr,
                                           m_message_header_remaining, &error, &eof);
                    if (nread == (size_t)-1) {
                        if (errno != ECONNREFUSED) {
                            LOG(ERROR) << "read data from socket failed."
                                << "[socket=" << m_sd << ", "
                                << "remaining_message=" << (int)m_message_header_remaining << ", "
                                << "error=" << strerror(errno) << "]";
                        }
                        else {
                            test_and_set_error(error::COMM_CONNECT_ERROR);
                        }
                        handle_disconnect();
                        return true;
                    }
                    else if (nread < m_message_header_remaining) {
                        m_message_header_remaining -= nread;
                        m_message_header_ptr += nread;
                        if (error == EAGAIN) {
                            break;
                        }
                        error = 0;
                    }
                    else {
                        m_message_header_ptr += nread;
                        handle_message_header(arrival_time);
                    }
                    if (eof) {
                        break;
                    }
                }
                else { // got header
                    nread = et_socket_read(m_sd, m_message_ptr, m_message_remaining,
                                           &error, &eof);
                    if (nread == (size_t)-1) {
                        LOG(ERROR) << "read data from socket failed."
                                   << "[socket=" << m_sd << ", "
                                   << "remaining_message=" << (int)m_message_header_remaining
                                   << "error=" << strerror(errno) << "]";
                        handle_disconnect();
                        return true;
                    }
                    else if (nread < m_message_remaining) {
                        m_message_ptr += nread;
                        m_message_remaining -= nread;
                        if (error == EAGAIN) {
                            break;
                        }
                        error = 0;
                    }
                    else {
                        handle_message_body();
                    }
                    if (eof) {
                        break;
                    }
                }
            }
        }
        if (ReactorFactory::ms_epollet) {
            if (event->events & EPOLLRDHUP) {
                LOG(ERROR) << "received EPOLLRDHUP on socket."
                           << "[socket=" << m_sd << ", "
                           << "addr=" << inet_ntoa(m_addr.sin_addr) << ", "
                           << "port=" << ntohs(m_addr.sin_port) << "]";
                handle_disconnect();
                return true;
            }
        }
        else {
            if (eof) {
                LOG(ERROR) << "received EOF on socket."
                           << "[socket=" << m_sd << ", "
                           << "addr=" << inet_ntoa(m_addr.sin_addr) << ", "
                           << "port=" << ntohs(m_addr.sin_port) << "]";
                handle_disconnect();
                return true;
            }
        }
        if (event->events & EPOLLERR) {
            LOG(ERROR) << "received EPOLLERR on socket."
                       << "[socket=" << m_sd << ", "
                       << "addr=" << inet_ntoa(m_addr.sin_addr) << ", "
                       << "port=" << ntohs(m_addr.sin_port) << "]";
            handle_disconnect();
            return true;
        }
        if (event->events & EPOLLHUP) {
            LOG(ERROR) << "received EPOLLHUP on socket."
                       << "[socket=" << m_sd << ", "
                       << "addr=" << inet_ntoa(m_addr.sin_addr) << ", "
                       << "port=" << ntohs(m_addr.sin_port) << "]";
            handle_disconnect();
            return true;
        }
    }
    catch (palo::Exception &e) {
        LOG(ERROR)  << e ;
        handle_disconnect();
        return true;
    }
    return false;
}

void IOHandlerData::handle_message_header(ClockT::time_point arrival_time) {
    size_t header_len = (size_t)m_message_header[1];
    // check to see if there is any variable length header
    // after the fixed length portion that needs to be read
    if (header_len > (size_t)(m_message_header_ptr - m_message_header)) {
        m_message_header_remaining = header_len - (size_t)(m_message_header_ptr
                                     - m_message_header);
        return;
    }
    m_event = std::make_shared<Event>(Event::MESSAGE, m_addr);
    m_event->load_message_header(m_message_header, header_len);
    m_event->arrival_time = arrival_time;
    m_message_aligned = false;
    if (m_event->header.alignment > 0) {
        void *vptr = 0;
        posix_memalign(&vptr, m_event->header.alignment,
                       m_event->header.total_len - header_len);
        m_message = (uint8_t *)vptr;
        m_message_aligned = true;
    } else {
        m_message = new uint8_t[m_event->header.total_len - header_len];
    }
    m_message_ptr = m_message;
    m_message_remaining = m_event->header.total_len - header_len;
    m_message_header_remaining = 0;
    m_got_header = true;
}

void IOHandlerData::handle_message_body() {
    DispatchHandler *dh {};
    if (m_event->header.flags & CommHeader::FLAGS_BIT_PROXY_MAP_UPDATE) {
        ReactorRunner::handler_map->update_proxy_map((const char *)m_message,
                m_event->header.total_len - m_event->header.header_len);
        free_message_buffer();
        m_event.reset();
    }
    else if ((m_event->header.flags & CommHeader::FLAGS_BIT_REQUEST) == 0 &&
             (m_event->header.id == 0
              || !m_reactor->remove_request(m_event->header.id, dh))) {
        if ((m_event->header.flags & CommHeader::FLAGS_BIT_IGNORE_RESPONSE) == 0) {
            LOG(WARNING) << "received response for non-pending event."
                         << "[request_id=" << m_event->header.id << ", "
                         << "header_version=" << m_event->header.version << ", "
                         << "total_len=" << m_event->header.total_len << "]";
        }
        free_message_buffer();
        m_event.reset();
    }
    else {
        m_event->payload = m_message;
        m_event->payload_len = m_event->header.total_len
                               - m_event->header.header_len;
        m_event->payload_aligned = m_message_aligned;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_event->set_proxy(m_proxy);
        }
        deliver_event(m_event, dh);
    }
    reset_incoming_message_state();
}

void IOHandlerData::handle_disconnect() {
    ReactorRunner::handler_map->decomission_handler(this);
}

bool IOHandlerData::handle_write_readiness() {
    bool deliver_conn_estab_event = false;
    bool rval = true;
    int error = error::OK;
    while (true) {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_connected == false) {
            socklen_t name_len = sizeof(m_local_addr);
            int sockerr = 0;
            socklen_t sockerr_len = sizeof(sockerr);
            if (getsockopt(m_sd, SOL_SOCKET, SO_ERROR, &sockerr, &sockerr_len) < 0) {
                LOG(ERROR) << "get SO_ERROR socket option failed."
                           << "[error=" << strerror(errno) << "]";
            }
            if (sockerr) {
                LOG(ERROR) << "connect completion error."
                           << "[error=" << strerror(sockerr) << "]";
                break;
            }
            int bufsize = 4*32768;
            if (setsockopt(m_sd, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize,
                           sizeof(bufsize)) < 0) {
                LOG(ERROR) << "set SO_SNDBUF socket failed."
                           << "[error=" << strerror(errno) << "]";
            }
            if (setsockopt(m_sd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize,
                           sizeof(bufsize)) < 0) {
                LOG(ERROR) << "set SO_RCVBUF socket option failed."
                           << "[error=" << strerror(errno) << "]";
            }
            int one = 1;
            if (setsockopt(m_sd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)) < 0) {
                LOG(ERROR) << "set SO_KEEPALIVE socket option failed."
                           << "[error=" << strerror(errno) << "]";
            }
            if (getsockname(m_sd, (struct sockaddr *)&m_local_addr, &name_len) < 0) {
                LOG(ERROR) << "get socket name failed."
                           << "[socket=" << m_sd << ", "
                           << "error=" << strerror(errno) << "]";
                break;
            }
            //LOG(INFO) << "Connection established.";
            m_connected = true;
            deliver_conn_estab_event = true;
        }
        //LOG(INFO) << "about to flush send queue";
        if ((error = flush_send_queue()) != error::OK) {
            LOG(ERROR) << "flush send queue failed.";
            if (m_error == error::OK)
                m_error = error;
            return true;
        }
        //LOG(INFO) << "about to remove poll interest";
        if (m_send_queue.empty()) {
            if ((error = remove_epoll_interest(poll_event::WRITE)) != error::OK) {
                if (m_error == error::OK)
                    m_error = error;
                return true;
            }
        }
        rval = false;
        break;
    }
    if (deliver_conn_estab_event) {
        if (ReactorFactory::proxy_master) {
            if ((error = ReactorRunner::handler_map->propagate_proxy_map(this))
                    != error::OK) {
                LOG(ERROR) << "Problem sending proxy map to %s - %s"
                           << m_addr.format().c_str() << error::get_text(error);
                return true;
            }
        }
        EventPtr event = std::make_shared<Event>(Event::CONNECTION_ESTABLISHED, m_addr,
                         m_proxy, error::OK);
        deliver_event(event);
    }
    return rval;
}

int
IOHandlerData::send_message(CommBufPtr &cbp, uint32_t timeout_ms,
                            DispatchHandler *disp_handler) {
    std::lock_guard<std::mutex> lock(m_mutex);
    bool initially_empty = m_send_queue.empty() ? true : false;
    int error = error::OK;
    if (m_decomissioned)
        return error::COMM_NOT_CONNECTED;
    // If request, Add message ID to request cache
    if (cbp->header.id != 0 && disp_handler != 0
            && cbp->header.flags & CommHeader::FLAGS_BIT_REQUEST) {
        auto expire_time = ClockT::now() + std::chrono::milliseconds(timeout_ms);
        m_reactor->add_request(cbp->header.id, this, disp_handler, expire_time);
    }
    //LOG(INFO) << "About to send message of size %d", cbp->header.total_len;
    m_send_queue.push_back(cbp);
    if (m_connected) {
        if ((error = flush_send_queue()) != error::OK) {
            LOG(ERROR) << "flush send queue failed."
                       << "[error=" << error::get_text(error) << "]";
            ReactorRunner::handler_map->decomission_handler(this);
            if (m_error == error::OK) {
                m_error = error;
            }
            return error;
        }
    }
    if (initially_empty && !m_send_queue.empty()) {
        error = add_epoll_interest(poll_event::WRITE);
        if (error) {
            LOG(ERROR) << "add write interest failed. [error=" << (unsigned)error << "]";
        }
    }
    else if (!initially_empty && m_send_queue.empty()) {
        error = remove_epoll_interest(poll_event::WRITE);
        if (error) {
            LOG(ERROR) << "remove write interest failed. [error=" << (unsigned)error << "]";
        }
    }
    // Set m_error if not already set
    if (error != error::OK && m_error == error::OK) {
        m_error = error;
    }
    return error;
}

int IOHandlerData::flush_send_queue() {
    ssize_t nwritten = 0;
    ssize_t towrite = 0;
    ssize_t remaining = 0;
    struct iovec vec[2];
    int count = 0;
    int error = 0;
    while (!m_send_queue.empty()) {
        CommBufPtr &cbp = m_send_queue.front();
        count = 0;
        towrite = 0;
        remaining = cbp->data.size - (cbp->data_ptr - cbp->data.base);
        if (remaining > 0) {
            vec[0].iov_base = (void *)cbp->data_ptr;
            vec[0].iov_len = remaining;
            towrite = remaining;
            ++count;
        }
        if (cbp->ext.base != 0) {
            remaining = cbp->ext.size - (cbp->ext_ptr - cbp->ext.base);
            if (remaining > 0) {
                vec[count].iov_base = (void *)cbp->ext_ptr;
                vec[count].iov_len = remaining;
                towrite += remaining;
                ++count;
            }
        }
        nwritten = et_socket_writev(m_sd, vec, count, &error);
        if (nwritten == (ssize_t)-1) {
            if (error == EAGAIN)
                return error::OK;
            LOG(ERROR) << "write socket failed."
                       << "[socket=" << m_sd << ", "
                       << "towrite=" << (int)towrite << ", "
                       << "error=" << strerror(errno) << "]";
            return error::COMM_BROKEN_CONNECTION;
        }
        else if (nwritten < towrite) {
            if (nwritten == 0) {
                if (error == EAGAIN)
                    break;
                if (error) {
                    LOG(ERROR) << "write socket failed."
                               << "[socket=" << m_sd << ", "
                               << "towrite=" << (int)towrite << ", "
                               << "error=" << strerror(errno) << "]";
                    return error::COMM_BROKEN_CONNECTION;
                }
                continue;
            }
            remaining = cbp->data.size - (cbp->data_ptr - cbp->data.base);
            if (remaining > 0) {
                if (nwritten < remaining) {
                    cbp->data_ptr += nwritten;
                    if (error == EAGAIN) {
                        break;
                    }
                    error = 0;
                    continue;
                }
                else {
                    nwritten -= remaining;
                    cbp->data_ptr += remaining;
                }
            }
            if (cbp->ext.base != 0) {
                cbp->ext_ptr += nwritten;
                if (error == EAGAIN) {
                    break;
                }
                error = 0;
                continue;
            }
        }
        // buffer written successfully, now remove from queue (destroys buffer)
        m_send_queue.pop_front();
    }
    return error::OK;
}

} //namespace palo
