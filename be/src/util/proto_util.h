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

#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/brpc_client_cache.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/network_util.h"

namespace doris {

// When the tuple/block data is greater than 2G, embed the tuple/block data
// and the request serialization string in the attachment, and use "http" brpc.
// "http"brpc requires that only one of request and attachment be non-null.
//
// 2G: In the default "baidu_std" brpcd, upper limit of the request and attachment length is 2G.
constexpr size_t MIN_HTTP_BRPC_SIZE = (1ULL << 31);

// Attachment layout shared by every http send path:
//
//   [int64 request_size][serialized request][int64 payload_size][payload]...
//
// The serialized request carries everything but the block payloads (PBlock::column_values).
// One payload section follows per payload block of the request, in block order, see
// payload_blocks(). A request with a single block therefore keeps the layout older
// receivers expect; receivers older than this layout read exactly one payload into the
// singular `block`, so a multi-payload attachment (repeated `blocks`) is only understood by
// receivers that carry it. Such requests could not be sent at all before (plain brpc rejected
// them for exceeding its size limit), so no working mixed-version case is lost.

// The blocks whose column_values travel as attachment payloads. A generic request carries one
// block; PTransmitDataParams carries either the singular `block` or the repeated `blocks`.
template <typename Params>
std::vector<PBlock*> payload_blocks(Params* brpc_request) {
    return {brpc_request->mutable_block()};
}

inline std::vector<PBlock*> payload_blocks(PTransmitDataParams* brpc_request) {
    if (brpc_request->has_block()) {
        DCHECK(brpc_request->blocks().empty());
        return {brpc_request->mutable_block()};
    }
    std::vector<PBlock*> blocks;
    blocks.reserve(brpc_request->blocks_size());
    for (auto& block : *brpc_request->mutable_blocks()) {
        blocks.push_back(&block);
    }
    return blocks;
}

// Append one `[int64 size][bytes]` section to the attachment.
inline Status attachment_append_section(butil::IOBuf* attachment, const std::string& data) {
    int64_t data_size = data.size();
    attachment->append(&data_size, sizeof(data_size));
    try {
        attachment->append(data);
    } catch (...) {
        LOG(WARNING) << "Try to alloc " << data_size
                     << " bytes for append data to attachment failed. ";
        return Status::MemoryAllocFailed("request embed attachment failed to memcpy {} bytes",
                                         data_size);
    }
    return Status::OK();
}

// Embed `brpc_request` followed by `payloads`, one section each, in the controller attachment.
template <typename Params, typename Closure>
Status request_embed_attachmentv2(Params* brpc_request,
                                  const std::vector<const std::string*>& payloads,
                                  std::unique_ptr<Closure>& closure) {
    std::string req_str;
    if (!brpc_request->SerializeToString(&req_str)) {
        return Status::InternalError("failed to serialize the request");
    }
    butil::IOBuf attachment;
    RETURN_IF_ERROR(attachment_append_section(&attachment, req_str));
    for (const auto* payload : payloads) {
        RETURN_IF_ERROR(attachment_append_section(&attachment, *payload));
    }
    closure->cntl_->request_attachment().swap(attachment);
    return Status::OK();
}

template <typename Params, typename Closure>
Status request_embed_attachmentv2(Params* brpc_request, const std::string& data,
                                  std::unique_ptr<Closure>& closure) {
    return request_embed_attachmentv2(brpc_request, std::vector<const std::string*> {&data},
                                      closure);
}

// Embed a request that owns its block: column_values is moved into the attachment and the
// request keeps the field present but empty.
template <typename Params, typename Closure>
Status request_embed_attachment_contain_blockv2(Params* brpc_request,
                                                std::unique_ptr<Closure>& closure) {
    auto* block = brpc_request->mutable_block();
    std::string column_values = std::move(*block->mutable_column_values());
    block->mutable_column_values()->clear();
    return request_embed_attachmentv2(brpc_request, column_values, closure);
}

// Embed a request whose blocks only borrow their column_values (see
// ExchangeSinkBuffer / borrow_block()). Each payload is detached from the request while the
// request is serialized, appended by reference and attached back, so the owner of the payload
// is never read through the request's presence bits nor mutated: the same owner may be
// serialized by other channels at the same time.
template <typename Closure>
Status request_embed_attachment_borrowed_blocksv2(PTransmitDataParams* brpc_request,
                                                  std::unique_ptr<Closure>& closure) {
    std::vector<PBlock*> blocks = payload_blocks(brpc_request);
    std::vector<std::string*> payloads;
    payloads.reserve(blocks.size());
    // Registered before any payload is detached: if a placeholder allocation below throws, the
    // payloads detached so far are attached back and the placeholders already installed are freed
    // by set_allocated_column_values().
    Defer attach_back([&]() {
        for (size_t i = 0; i < payloads.size(); ++i) {
            blocks[i]->set_allocated_column_values(payloads[i]);
        }
    });
    for (auto* block : blocks) {
        payloads.push_back(block->release_column_values());
        DCHECK(payloads.back() != nullptr);
        // Keep the field present on the wire: receivers that take the payload slot before
        // parsing the request rely on the serialized field to restore its presence bit.
        block->set_column_values("");
    }
    return request_embed_attachmentv2(
            brpc_request, std::vector<const std::string*>(payloads.begin(), payloads.end()),
            closure);
}

inline bool enable_http_send_block(const PTransmitDataParams& request) {
    if (!config::transfer_large_data_by_brpc) {
        return false;
    }
    if (!request.has_block() && request.blocks().empty()) {
        return false;
    }
    // Force the http attachment path in tests without needing an actual >= 2G request,
    // so regression tests can exercise transmit_block_httpv2.
    DBUG_EXECUTE_IF("proto_util.enable_http_send_block.always_http", { return true; });
    return request.ByteSizeLong() >= MIN_HTTP_BRPC_SIZE;
}

template <typename Closure>
void transmit_blockv2(PBackendService_Stub* stub, std::unique_ptr<Closure> closure) {
    closure->cntl_->http_request().Clear();
    stub->transmit_block(closure->cntl_.get(), closure->request_.get(), closure->response_.get(),
                         closure.get());
    closure.release();
}

template <typename Closure>
void transmit_serialized_block(PBackendService_Stub* stub, std::unique_ptr<Closure> closure) {
    // SerializedRequest bypasses brpc's protobuf serializer, including request compression.
    // Exchange compresses column_values before preparing the shared protobuf fragment.
    DCHECK_EQ(closure->cntl_->request_compress_type(), brpc::COMPRESS_TYPE_NONE);
    static const auto* method = PBackendService::descriptor()->FindMethodByName("transmit_block");
    closure->cntl_->http_request().Clear();
    stub->channel()->CallMethod(method, closure->cntl_.get(), closure->request_.get(),
                                closure->response_.get(), closure.get());
    closure.release();
}

template <typename Closure>
Status transmit_block_httpv2(ExecEnv* exec_env, std::unique_ptr<Closure> closure,
                             TNetworkAddress brpc_dest_addr) {
    RETURN_IF_ERROR(request_embed_attachment_borrowed_blocksv2(closure->request_.get(), closure));

    std::string host = brpc_dest_addr.hostname;
    auto* dns_cache = ExecEnv::GetInstance()->dns_cache();
    if (dns_cache == nullptr) {
        LOG(WARNING) << "DNS cache is not initialized, skipping hostname resolve";
    } else if (!is_valid_ip(brpc_dest_addr.hostname)) {
        Status status = dns_cache->get(brpc_dest_addr.hostname, &host);
        if (!status.ok()) {
            LOG(WARNING) << "failed to get ip from host " << brpc_dest_addr.hostname << ": "
                         << status.to_string();
            return Status::InternalError("failed to get ip from host {}", brpc_dest_addr.hostname);
        }
    }
    //format an ipv6 address
    std::string brpc_url = get_brpc_http_url(brpc_dest_addr.hostname, brpc_dest_addr.port);

    std::shared_ptr<PBackendService_Stub> brpc_http_stub =
            exec_env->brpc_internal_client_cache()->get_new_client_no_cache(brpc_url, "http");
    if (brpc_http_stub == nullptr) {
        return Status::InternalError("failed to open brpc http client to {}", brpc_url);
    }
    closure->cntl_->http_request().uri() =
            brpc_url + "/PInternalServiceImpl/transmit_block_by_http";
    closure->cntl_->http_request().set_method(brpc::HTTP_METHOD_POST);
    closure->cntl_->http_request().set_content_type("application/json");
    brpc_http_stub->transmit_block_by_http(closure->cntl_.get(), nullptr, closure->response_.get(),
                                           closure.get());
    closure.release();

    return Status::OK();
}

// Read the next `[int64 size][bytes]` section at `*offset` into `data`. Every size is checked
// against the attachment length, so a truncated or mis-framed attachment fails here instead of
// being accepted with a short or empty payload.
inline Status attachment_read_section(const butil::IOBuf& io_buf, size_t* offset,
                                      std::string* data) {
    int64_t size;
    if (io_buf.size() < *offset + sizeof(size)) {
        return Status::InternalError("attachment truncated: size prefix at offset {} exceeds {}",
                                     *offset, io_buf.size());
    }
    io_buf.copy_to(&size, sizeof(size), *offset);
    *offset += sizeof(size);
    if (size < 0 || io_buf.size() - *offset < static_cast<size_t>(size)) {
        return Status::InternalError(
                "attachment truncated: section of {} bytes at offset {} exceeds {}", size, *offset,
                io_buf.size());
    }
    try {
        io_buf.copy_to(data, size, *offset);
    } catch (...) {
        LOG(WARNING) << "Try to alloc " << size
                     << " bytes for extract data from attachment failed. ";
        return Status::MemoryAllocFailed("attachment extract request failed to memcpy {} bytes",
                                         size);
    }
    *offset += size;
    return Status::OK();
}

// Extract the brpc request and the payload of each of its blocks from the controller attachment.
template <typename Params>
Status attachment_extract_request_contain_block(Params* brpc_request, brpc::Controller* cntl) {
    const butil::IOBuf& io_buf = cntl->request_attachment();
    size_t offset = 0;
    std::string req_str;
    RETURN_IF_ERROR(attachment_read_section(io_buf, &offset, &req_str));
    if (!brpc_request->ParseFromString(req_str)) {
        return Status::InternalError("failed to parse the request from attachment");
    }
    // Take the payload slots only after parsing: ParseFromString() resets presence bits, and
    // mutable_column_values() marks the field present again, so a later PBlock::CopyFrom() keeps
    // the payload whether or not the serialized request carried the field.
    for (PBlock* block : payload_blocks(brpc_request)) {
        RETURN_IF_ERROR(attachment_read_section(io_buf, &offset, block->mutable_column_values()));
    }
    return Status::OK();
}

} // namespace doris
