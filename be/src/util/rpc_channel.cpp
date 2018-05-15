// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

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

#include "util/rpc_channel.h"

#include "rpc/error.h"
#include "rpc/protocol.h"
#include "rpc/serialization.h"

namespace palo {

RpcChannel::RpcChannel(Comm* comm, ConnectionManagerPtr conn_mgr, uint64_t command)
        : _comm(comm), _conn_mgr(conn_mgr), _command(command) {
}

RpcChannel::~RpcChannel() {
}

Status RpcChannel::init(const std::string& host, int port,
                        uint32_t connect_timeout_ms,
                        uint32_t rpc_timeout_ms) {
    // Initialize InetAddr
    struct sockaddr_in sockaddr_in;
    if (!InetAddr::initialize(&sockaddr_in, host.c_str(), port)) {
        std::stringstream ss;
        ss << "invalid inet address: host=" << host << ", port=" << port;
        return Status(ss.str());
    }
    _addr.set_inet(sockaddr_in);

    // connect to remote servcie
    _connect_timeout_ms = connect_timeout_ms;
    _conn_mgr->add(_addr, _connect_timeout_ms, "PaloBeDataStreamMgr");

    _rpc_timeout_ms = rpc_timeout_ms;
    return Status::OK;
}

Status RpcChannel::get_response(const uint8_t** data, uint32_t* size) {
    RETURN_IF_ERROR(_wait_for_last_sent());
    DCHECK(_last_event != nullptr);
    *data = _last_event->payload;
    *size = _last_event->payload_len;
    return Status::OK;
}

Status RpcChannel::send_message(const uint8_t* data, uint32_t size) {
    // make sure 
    RETURN_IF_ERROR(_wait_for_last_sent());
    return _send_message(data, size);
}

Status RpcChannel::_send_message(const uint8_t* data, uint32_t size) {
    DCHECK(!_rpc_in_flight);

    CommHeader header(_command);
    CommBufPtr new_comm_buf = std::make_shared<CommBuf>(header, size);
    new_comm_buf->append_bytes(data, size);

    auto res = _comm->send_request(_addr, _rpc_timeout_ms, new_comm_buf, this);
    if (res != error::OK) {
        LOG(WARNING) << "fail to send_request, addr=" << _addr.to_str()
            << ", res=" << res << ", message=" << error::get_text(res);
        // sleep 10ms to wait ConnectionManager to be notify
        usleep(10 * 1000);
        _conn_mgr->add(_addr, _connect_timeout_ms, "PaloBeDataStreamMgr");
        bool is_connected = _conn_mgr->wait_for_connection(_addr, _connect_timeout_ms);
        if (!is_connected) {
            LOG(WARNING) << "fail to wait_for_connection, addr=" << _addr.to_str();
            _conn_mgr->remove(_addr);
            _rpc_status = Status(TStatusCode::THRIFT_RPC_ERROR, "connection to remote PaloBe failed");
            return _rpc_status;
        }
        res = _comm->send_request(_addr, _rpc_timeout_ms, new_comm_buf, this);
        if (res != error::OK) {
            LOG(WARNING) << "fail to send_request, addr=" << _addr.to_str()
                << ", res=" << res << ", message=" << error::get_text(res);
            _rpc_status = Status(TStatusCode::THRIFT_RPC_ERROR, "fail to send_request");
            return _rpc_status;
        }
    }
    _cbp = new_comm_buf;
    _rpc_in_flight = true;
    return Status::OK;
}

Status RpcChannel::_wait_for_last_sent() {
    if (!_rpc_in_flight) {
        return _rpc_status;
    }
    int retry_times = 1;
    while (true) {
        EventPtr event;
        {
            std::unique_lock<std::mutex> l(_lock);
            auto duration = std::chrono::milliseconds(2 * _rpc_timeout_ms);
            if (_cond.wait_for(l, duration, [this]() { return !this->_events.empty(); })) {
                event = _events.front();
                _events.pop_front();
            }
        }
        if (event == nullptr) {
            LOG(WARNING) << "it's so weird, wait reponse event timeout, request="
                << _cbp->header.id << ", addr=" << _addr.to_str();
            _rpc_in_flight = false;
            if (retry_times-- > 0) {
                // timeout to receive response, to get user data to resend
                const uint8_t* data = nullptr;
                uint32_t size = 0;
                _cbp->get_user_data(&data, &size);
                RETURN_IF_ERROR(_send_message(data, size));
            } else {
                LOG(WARNING) << "fail to send batch, _add=" << _addr.to_str()
                    << ", request_id="<< _cbp->header.id;
                _rpc_status = Status(TStatusCode::THRIFT_RPC_ERROR, "fail to send batch");
                break;
            }
            continue;
        }
        if (event->type == Event::MESSAGE) {
            if (event->header.id != _cbp->header.id) {
                LOG(WARNING) << "receive event id not equal with in-flight request, request_id="
                    << _cbp->header.id << ", event=" << event->to_str();
                continue;
            }
            // response recept
            _rpc_in_flight = false;
            _last_event = event;
            return Status::OK;
        } else if (event->type == Event::DISCONNECT || event->type == Event::ERROR) {
            if (event->header.id != 0 && event->header.id != _cbp->header.id) {
                LOG(WARNING) << "receive event id not equal with in-flight request, request_id="
                    << _cbp->header.id << ", event=" << event->to_str();
                continue;
            }
            LOG(WARNING) << "receive response failed, request_id=" << _cbp->header.id
                << ", event=" << event->to_str();
            _rpc_in_flight = false;
            // error happend when receving response, we need to retry last request
            if (retry_times-- > 0) {
                // timeout to receive response
                const uint8_t* data = nullptr;
                uint32_t size = 0;
                _cbp->get_user_data(&data, &size);
                RETURN_IF_ERROR(_send_message(data, size));
            } else {
                LOG(WARNING) << "fail to send batch, request_id="<< _cbp->header.id
                    << ", event=" << event->to_str();
                _rpc_status = Status(TStatusCode::THRIFT_RPC_ERROR, "fail to send batch");
                break;
            }
        } else {
            _rpc_in_flight = false;
            LOG(ERROR) << "recevie unexpect event, event=" << event->to_str();
            _rpc_status = Status(TStatusCode::THRIFT_RPC_ERROR, "fail to send batch");
            break;
        }
    }

    return _rpc_status;
}

}
