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

#include "util/stack_util.h"

namespace doris {

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

} // namespace doris
