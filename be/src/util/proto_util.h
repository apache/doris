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

namespace doris {

const int64_t MAX_REQUEST_BYTE_SIZE = 64 * 1024 * 1024;

inline void row_batch_transfer_attachment(doris::PRowBatch* row_batch, butil::IOBuf* attachment) {
    row_batch->set_tuple_data_size(row_batch->tuple_data().size());
    attachment->append(row_batch->tuple_data());
    row_batch->clear_tuple_data();
    row_batch->set_tuple_data("");
}

template <typename Params, typename Closure>
inline void request_transfer_attachment(Params* brpc_request, Closure* closure) {
    if (config::brpc_request_transfer_attachment == true &&
        brpc_request->row_batch().ByteSizeLong() > MAX_REQUEST_BYTE_SIZE) {
        butil::IOBuf attachment;
        auto row_batch = brpc_request->mutable_row_batch();
        row_batch_transfer_attachment(row_batch, &attachment);
        closure->cntl.request_attachment().append(attachment);
    }
}

template <typename Params>
inline void attachment_transfer_request(const Params* brpc_request, brpc::Controller* cntl) {
    if (cntl->request_attachment().size() > 0) {
        Params* req = const_cast<Params*>(brpc_request);
        const butil::IOBuf& io_buf = cntl->request_attachment();
        auto rb = req->mutable_row_batch();
        io_buf.copy_to(rb->mutable_tuple_data(), rb->tuple_data_size(), 0);
    }
}

} // namespace doris