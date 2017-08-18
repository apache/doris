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

#ifndef BDG_PALO_BE_SERVICE_RECEIVER_DISPATCHER_H
#define BDG_PALO_BE_SERVICE_RECEIVER_DISPATCHER_H

#include <arpa/inet.h>

#include "rpc/application_handler.h"
#include "rpc/application_queue.h"
#include "rpc/compat.h"
#include "rpc/comm.h"
#include "rpc/connection_handler_factory.h"
#include "rpc/dispatch_handler.h"
#include "rpc/error.h"
#include "rpc/event.h"
#include "rpc/inet_addr.h"
#include "rpc/reactor_factory.h"
#include "rpc/serialization.h"
#include "rpc/sock_addr_map.h"
#include "runtime/data_stream_mgr.h"
#include "util/thrift_util.h"

namespace palo {

class DataStreamMgr;

class HandlerFactory : public ConnectionHandlerFactory {
public:
    HandlerFactory(DispatchHandlerPtr &dhp)
        : _dispatch_handler_ptr(dhp) { return; }

    virtual void get_instance(DispatchHandlerPtr &dhp) {
        dhp = _dispatch_handler_ptr;
    }

private:
    DispatchHandlerPtr _dispatch_handler_ptr;
};

class Dispatcher : public DispatchHandler {
public:
    Dispatcher(ExecEnv* exec_env, Comm *comm, ApplicationQueue *app_queue)
        : _exec_env(exec_env), _comm(comm) {}

    ~Dispatcher() {}

    virtual void handle(EventPtr &event_ptr) {
        if (event_ptr->type == Event::CONNECTION_ESTABLISHED) {
            LOG(INFO) << "Connection Established.";
        }
        else if (event_ptr->type == Event::DISCONNECT) {
            if (event_ptr->error != 0) {
                LOG(INFO) << "Disconnect : " << error::get_text(event_ptr->error);
            } else {
                LOG(INFO) << "Disconnect";
            }
        }
        else if (event_ptr->type == Event::ERROR) {
            LOG(ERROR) << "Error: " << error::get_text(event_ptr->error);
        }
        else if (event_ptr->type == Event::MESSAGE) {
            const uint8_t *buf_ptr = (uint8_t*)event_ptr->payload;
            uint32_t sz = event_ptr->payload_len;

            TTransmitDataParams params;
            deserialize_thrift_msg(buf_ptr, &sz, false, &params);

            TTransmitDataResult return_val;
            if (params.__isset.packet_seq) {
                return_val.__set_packet_seq(params.packet_seq);
                return_val.__set_dest_fragment_instance_id(params.dest_fragment_instance_id);
                return_val.__set_dest_node_id(params.dest_node_id);
                // TODO: how to return Error Status?
                return_val.status.status_code = TStatusCode::OK;
            }

            uint8_t* buf_res = 0;
            uint32_t size = 0;
            ThriftSerializer thrift_serializer(false, 100);
            thrift_serializer.serialize(&return_val, &size, &buf_res);

            CommHeader header;
            header.initialize_from_request_header(event_ptr->header);
            CommBufPtr response(new CommBuf(header, size));
            response->append_bytes(buf_res, size);

            bool buffer_overflow = false;
            if (params.row_batch.num_rows > 0) {
                _exec_env->stream_mgr()->add_data(
                        params.dest_fragment_instance_id,
                        params.dest_node_id,
                        params.row_batch,
                        params.sender_id,
                        &buffer_overflow,
                        std::make_pair(event_ptr->addr, response));
            }

            if (params.eos) {
                _exec_env->stream_mgr()->close_sender(
                        params.dest_fragment_instance_id,
                        params.dest_node_id,
                        params.sender_id, 
                        params.be_number);
            }

            if (params.eos || !buffer_overflow) {
                VLOG(3) << "send last response to client";
                int error = _comm->send_response(event_ptr->addr, response);
                if (error != error::OK) {
                    LOG(ERROR) << "Comm::send_response returned" << error::get_text(error);
                }
            }
        }
    }

private:
    ExecEnv* _exec_env;
    Comm* _comm;

};

} //namespace palo
#endif //BDG_PALO_BE_SERVICE_RECEIVER_DISPATCHER_H
