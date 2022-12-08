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

#pragma once

#include <brpc/http_method.h>
#include <gen_cpp/internal_service.pb.h>

#include "common/config.h"
#include "common/status.h"
#include "exception.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/brpc_client_cache.h"

namespace doris {

// When the tuple/block data is greater than 2G, embed the tuple/block data
// and the request serialization string in the attachment, and use "http" brpc.
// "http"brpc requires that only one of request and attachment be non-null.
//
// 2G: In the default "baidu_std" brpcd, upper limit of the request and attachment length is 2G.
constexpr size_t MIN_HTTP_BRPC_SIZE = (1ULL << 31);

// Embed column_values and brpc request serialization string in controller attachment.
template <typename Params, typename Closure>
inline Status request_embed_attachment_contain_block(Params* brpc_request, Closure* closure) {
    auto block = brpc_request->block();
    Status st = request_embed_attachment(brpc_request, block.column_values(), closure);
    block.set_column_values("");
    return st;
}

inline bool enable_http_send_block(
        const PTransmitDataParams& request,
        bool transfer_large_data_by_brpc = config::transfer_large_data_by_brpc) {
    if (!config::transfer_large_data_by_brpc) {
        return false;
    }
    if (!request.has_block() || !request.block().has_column_values()) {
        return false;
    }
    if (request.ByteSizeLong() < MIN_HTTP_BRPC_SIZE) {
        return false;
    }
    return true;
}

template <typename Closure>
inline void transmit_block(PBackendService_Stub& stub, Closure* closure,
                           const PTransmitDataParams& params) {
    closure->cntl.http_request().Clear();
    stub.transmit_block(&closure->cntl, &params, &closure->result, closure);
}

template <typename Closure>
inline Status transmit_block_http(RuntimeState* state, Closure* closure,
                                  PTransmitDataParams& params, TNetworkAddress brpc_dest_addr) {
    RETURN_IF_ERROR(request_embed_attachment_contain_block(&params, closure));

    std::string brpc_url =
            fmt::format("http://{}:{}", brpc_dest_addr.hostname, brpc_dest_addr.port);
    std::shared_ptr<PBackendService_Stub> brpc_http_stub =
            state->exec_env()->brpc_internal_client_cache()->get_new_client_no_cache(brpc_url,
                                                                                     "http");
    closure->cntl.http_request().uri() = brpc_url + "/PInternalServiceImpl/transmit_block_by_http";
    closure->cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
    closure->cntl.http_request().set_content_type("application/json");
    brpc_http_stub->transmit_block_by_http(&closure->cntl, nullptr, &closure->result, closure);

    return Status::OK();
}

// TODO(zxy) delete in v1.3 version
// Transfer RowBatch in ProtoBuf Request to Controller Attachment.
// This can avoid reaching the upper limit of the ProtoBuf Request length (2G),
// and it is expected that performance can be improved.
template <typename Params, typename Closure>
inline void request_row_batch_transfer_attachment(Params* brpc_request,
                                                  const std::string& tuple_data, Closure* closure) {
    auto row_batch = brpc_request->mutable_row_batch();
    row_batch->set_tuple_data("");
    brpc_request->set_transfer_by_attachment(true);
    butil::IOBuf attachment;
    attachment.append(tuple_data);
    closure->cntl.request_attachment().swap(attachment);
}

// TODO(zxy) delete in v1.3 version
// Transfer Block in ProtoBuf Request to Controller Attachment.
// This can avoid reaching the upper limit of the ProtoBuf Request length (2G),
// and it is expected that performance can be improved.
template <typename Params, typename Closure>
inline void request_block_transfer_attachment(Params* brpc_request,
                                              const std::string& column_values, Closure* closure) {
    auto block = brpc_request->mutable_block();
    block->set_column_values("");
    brpc_request->set_transfer_by_attachment(true);
    butil::IOBuf attachment;
    attachment.append(column_values);
    closure->cntl.request_attachment().swap(attachment);
}

// TODO(zxy) delete in v1.3 version
// Controller Attachment transferred to RowBatch in ProtoBuf Request.
template <typename Params>
inline void attachment_transfer_request_row_batch(const Params* brpc_request,
                                                  brpc::Controller* cntl) {
    Params* req = const_cast<Params*>(brpc_request);
    if (req->has_row_batch() && req->transfer_by_attachment()) {
        auto rb = req->mutable_row_batch();
        const butil::IOBuf& io_buf = cntl->request_attachment();
        CHECK(io_buf.size() > 0) << io_buf.size() << ", row num: " << req->row_batch().num_rows();
        io_buf.copy_to(rb->mutable_tuple_data(), io_buf.size(), 0);
    }
}

// TODO(zxy) delete in v1.3 version
// Controller Attachment transferred to Block in ProtoBuf Request.
template <typename Params>
inline void attachment_transfer_request_block(const Params* brpc_request, brpc::Controller* cntl) {
    Params* req = const_cast<Params*>(brpc_request);
    if (req->has_block() && req->transfer_by_attachment()) {
        auto block = req->mutable_block();
        const butil::IOBuf& io_buf = cntl->request_attachment();
        CHECK(io_buf.size() > 0) << io_buf.size();
        io_buf.copy_to(block->mutable_column_values(), io_buf.size(), 0);
    }
}

// Embed tuple_data and brpc request serialization string in controller attachment.
template <typename Params, typename Closure>
inline Status request_embed_attachment_contain_tuple(Params* brpc_request, Closure* closure) {
    auto row_batch = brpc_request->row_batch();
    Status st = request_embed_attachment(brpc_request, row_batch.tuple_data(), closure);
    row_batch.set_tuple_data("");
    return st;
}

template <typename Params, typename Closure>
inline Status request_embed_attachment(Params* brpc_request, const std::string& data,
                                       Closure* closure) {
    butil::IOBuf attachment;

    // step1: serialize brpc_request to string, and append to attachment.
    std::string req_str;
    brpc_request->SerializeToString(&req_str);
    int64_t req_str_size = req_str.size();
    attachment.append(&req_str_size, sizeof(req_str_size));
    attachment.append(req_str);

    // step2: append data to attachment and put it in the closure.
    int64_t data_size = data.size();
    attachment.append(&data_size, sizeof(data_size));
    try {
        attachment.append(data);
    } catch (...) {
        std::exception_ptr p = std::current_exception();
        LOG(WARNING) << "Try to alloc " << data_size
                     << " bytes for append data to attachment failed. "
                     << get_current_exception_type_name(p);
        return Status::MemoryAllocFailed("request embed attachment failed to memcpy {} bytes",
                                         data_size);
    }
    // step3: attachment add to closure.
    closure->cntl.request_attachment().swap(attachment);
    return Status::OK();
}

// Extract the brpc request and tuple data from the controller attachment,
// and put the tuple data into the request.
template <typename Params>
inline Status attachment_extract_request_contain_tuple(const Params* brpc_request,
                                                       brpc::Controller* cntl) {
    Params* req = const_cast<Params*>(brpc_request);
    auto rb = req->mutable_row_batch();
    return attachment_extract_request(req, cntl, rb->mutable_tuple_data());
}

// Extract the brpc request and block from the controller attachment,
// and put the block into the request.
template <typename Params>
inline Status attachment_extract_request_contain_block(const Params* brpc_request,
                                                       brpc::Controller* cntl) {
    Params* req = const_cast<Params*>(brpc_request);
    auto block = req->mutable_block();
    return attachment_extract_request(req, cntl, block->mutable_column_values());
}

template <typename Params>
inline Status attachment_extract_request(const Params* brpc_request, brpc::Controller* cntl,
                                         std::string* data) {
    const butil::IOBuf& io_buf = cntl->request_attachment();

    // step1: deserialize request string to brpc_request from attachment.
    int64_t req_str_size;
    io_buf.copy_to(&req_str_size, sizeof(req_str_size), 0);
    std::string req_str;
    io_buf.copy_to(&req_str, req_str_size, sizeof(req_str_size));
    Params* req = const_cast<Params*>(brpc_request);
    req->ParseFromString(req_str);

    // step2: extract data from attachment.
    int64_t data_size;
    io_buf.copy_to(&data_size, sizeof(data_size), sizeof(req_str_size) + req_str_size);
    try {
        io_buf.copy_to(data, data_size, sizeof(data_size) + sizeof(req_str_size) + req_str_size);
    } catch (...) {
        std::exception_ptr p = std::current_exception();
        LOG(WARNING) << "Try to alloc " << data_size
                     << " bytes for extract data from attachment failed. "
                     << get_current_exception_type_name(p);
        return Status::MemoryAllocFailed("attachment extract request failed to memcpy {} bytes",
                                         data_size);
    }
    return Status::OK();
}

} // namespace doris
