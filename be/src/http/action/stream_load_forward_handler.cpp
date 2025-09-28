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

#include "http/action/stream_load_forward_handler.h"

#include <event2/buffer.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include <event2/keyvalq_struct.h>

#include "common/config.h"
#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "util/byte_buffer.h"

namespace doris {

int StreamLoadForwardHandler::on_header(HttpRequest* req) {
    std::ostringstream headers_info;
    const auto& headers = req->headers();
    for (const auto& header : headers) {
        headers_info << header.first << ":" << header.second << " ";
    }

    std::ostringstream params_info;
    const auto* params = req->params();
    for (const auto& param : *params) {
        params_info << param.first << "=" << param.second << " ";
    }

    LOG(INFO) << "StreamLoadForward request started - "
              << "path: " << req->raw_path() << ", remote: " << req->remote_host() << ", headers: ["
              << headers_info.str() << "]"
              << ", params: [" << params_info.str() << "]";

    std::shared_ptr<StreamLoadForwardContext> ctx(new StreamLoadForwardContext());
    req->set_handler_ctx(ctx);

    auto it = params->find("forward_to");
    if (it == params->end()) {
        LOG(WARNING) << "StreamLoadForward failed - missing forward_to parameter, path: "
                     << req->raw_path();
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST,
                                "Missing required parameter 'forward_to'. "
                                "Usage: ?forward_to=host:port");
        return HttpStatus::BAD_REQUEST;
    }

    std::string target_host;
    int target_port;
    Status st = parse_forward_target(it->second, target_host, target_port);
    if (!st.ok()) {
        LOG(WARNING) << "StreamLoadForward failed - invalid forward target: " << st.to_string()
                     << ", path: " << req->raw_path();
        HttpChannel::send_reply(
                req, HttpStatus::BAD_REQUEST,
                "Invalid forward_to parameter: " + st.to_string() + ". Expected format: host:port");
        return HttpStatus::BAD_REQUEST;
    }

    ctx->target_host = target_host;
    ctx->target_port = target_port;
    ctx->original_req = req;

    Status init_st = init_forward_request(req, target_host, target_port, ctx);
    if (!init_st.ok()) {
        LOG(WARNING) << "StreamLoadForward failed - failed to initialize forward request: "
                     << init_st.to_string() << ", target: " << target_host << ":" << target_port
                     << ", path: " << req->raw_path();
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                "Failed to initialize forward request: " + init_st.to_string());
        return HttpStatus::INTERNAL_SERVER_ERROR;
    }

    return HttpStatus::OK;
}

void StreamLoadForwardHandler::handle(HttpRequest* req) {
    auto ctx = std::static_pointer_cast<StreamLoadForwardContext>(req->handler_ctx());
    if (!ctx) {
        LOG(WARNING) << "StreamLoadForward failed - context not found, path: " << req->raw_path();
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                "Internal error: context not found");
        return;
    }

    auto* forward_req = ctx->get_forward_request();
    if (!forward_req) {
        LOG(WARNING) << "Forward request not ready, path: " << req->raw_path();
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                "Internal error: forward request not initialized");
        return;
    }

    setup_forward_headers(req, forward_req, ctx->target_host, ctx->target_port);

    if (!ctx->request_data_chunks.empty()) {
        evbuffer* output = evhttp_request_get_output_buffer(forward_req);
        while (!ctx->request_data_chunks.empty()) {
            const auto& bb = ctx->request_data_chunks.front();
            if (evbuffer_add(output, bb->ptr, bb->limit) != 0) {
                LOG(WARNING) << "Failed to add buffered data to output buffer, chunk size: "
                             << bb->limit << ", total size: " << ctx->total_request_size
                             << ", path: " << req->raw_path();
                HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                        "Failed to prepare forward data");
                return;
            }
            ctx->request_data_chunks.pop_front();
        }
    }

    if (evhttp_make_request(ctx->conn, forward_req, EVHTTP_REQ_PUT,
                            build_forward_url(req).c_str()) != 0) {
        LOG(WARNING) << "Failed to make forward request to " << ctx->target_host << ":"
                     << ctx->target_port << ", path: " << req->raw_path();
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                "Failed to forward request to target server: " + ctx->target_host +
                                        ":" + std::to_string(ctx->target_port));
        return;
    }

    LOG(INFO) << "StreamLoadForward request sent - data size: " << ctx->total_request_size
              << ", target: " << ctx->target_host << ":" << ctx->target_port
              << ", path: " << req->raw_path();
}

void StreamLoadForwardHandler::on_chunk_data(HttpRequest* req) {
    auto ctx = std::static_pointer_cast<StreamLoadForwardContext>(req->handler_ctx());
    if (!ctx) {
        LOG(WARNING) << "No context found for chunk data";
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                "Internal error: context not found");
        return;
    }

    evbuffer* input = evhttp_request_get_input_buffer(req->get_evhttp_request());
    while (evbuffer_get_length(input) > 0) {
        size_t remaining_length = evbuffer_get_length(input);
        ByteBufferPtr bb;
        Status st = ByteBuffer::allocate(remaining_length, &bb);
        if (!st.ok()) {
            LOG(WARNING) << "Failed to allocate ByteBuffer: " << st.to_string()
                         << ", path: " << req->raw_path();
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                    "Failed to allocate memory for request data");
            return;
        }

        auto remove_bytes = evbuffer_remove(input, bb->ptr, bb->capacity);
        bb->pos = remove_bytes;
        bb->flip();

        ctx->request_data_chunks.emplace_back(bb);
        ctx->total_request_size += remove_bytes;
    }
}

Status StreamLoadForwardHandler::init_forward_request(
        HttpRequest* req, const std::string& target_host, int target_port,
        std::shared_ptr<StreamLoadForwardContext>& ctx) {
    ctx->original_req = req;

    struct event_base* ev_base =
            evhttp_connection_get_base(evhttp_request_get_connection(req->get_evhttp_request()));

    struct evhttp_connection* conn = evhttp_connection_base_new(ev_base,
                                                                nullptr, // dns base
                                                                target_host.c_str(), target_port);

    if (!conn) {
        return Status::InternalError("Failed to create connection to target server");
    }

    evhttp_connection_set_closecb(conn, forward_connection_close_cb, ctx.get());
    ctx->conn = conn;

    struct evhttp_request* forward_req = evhttp_request_new(forward_request_done, ctx.get());
    if (!forward_req) {
        evhttp_connection_free(conn);
        return Status::InternalError("Failed to create forward request");
    }

    evhttp_request_set_chunked_cb(forward_req, forward_request_chunked_cb);

    ctx->set_forward_request(forward_req);
    return Status::OK();
}

void StreamLoadForwardHandler::forward_request_done(struct evhttp_request* req, void* arg) {
    auto* ctx = static_cast<StreamLoadForwardContext*>(arg);

    if (!req) {
        LOG(ERROR) << "Forward request failed - no response";
        evhttp_send_error(ctx->original_req->get_evhttp_request(), 503,
                          "Backend server unavailable");
        return;
    }

    int response_code = evhttp_request_get_response_code(req);
    const char* response_reason = evhttp_request_get_response_code_line(req);

    LOG(INFO) << "StreamLoadForward completed - "
              << "status: " << response_code
              << ", reason: " << (response_reason ? response_reason : "Unknown")
              << ", response_size: " << ctx->response_data.size() << " bytes"
              << ", path: " << ctx->original_req->raw_path();

    send_complete_response(req, ctx, response_code);
}

void StreamLoadForwardHandler::forward_request_chunked_cb(struct evhttp_request* req, void* arg) {
    auto* ctx = static_cast<StreamLoadForwardContext*>(arg);
    struct evbuffer* input_buffer = evhttp_request_get_input_buffer(req);
    if (input_buffer) {
        size_t data_len = evbuffer_get_length(input_buffer);
        if (data_len > 0) {
            // Read all available data and append to our response buffer
            char* data = (char*)evbuffer_pullup(input_buffer, data_len);
            if (data) {
                ctx->response_data.append(data, data_len);

                // Remove the data from the buffer since we've copied it
                evbuffer_drain(input_buffer, data_len);
            } else {
                LOG(WARNING) << "Failed to pullup " << data_len << " bytes from input buffer";
            }
        }
    }
}

void StreamLoadForwardHandler::send_complete_response(struct evhttp_request* req,
                                                      StreamLoadForwardContext* ctx,
                                                      int response_code) {
    struct evbuffer* response_body = evbuffer_new();
    if (!response_body) {
        LOG(ERROR) << "Failed to create response buffer";
        HttpChannel::send_reply(ctx->original_req, HttpStatus::INTERNAL_SERVER_ERROR,
                                "Internal error: failed to create response buffer");
        return;
    }

    size_t body_len = ctx->response_data.size();
    if (body_len > 0) {
        evbuffer_add(response_body, ctx->response_data.c_str(), body_len);
    }

    struct evkeyvalq* input_headers = evhttp_request_get_input_headers(req);
    struct evkeyvalq* output_headers =
            evhttp_request_get_output_headers(ctx->original_req->get_evhttp_request());

    size_t final_body_len = evbuffer_get_length(response_body);
    evhttp_add_header(output_headers, "Content-Length", std::to_string(final_body_len).c_str());

    copy_response_headers(input_headers, output_headers);

    evhttp_send_reply(ctx->original_req->get_evhttp_request(), response_code,
                      evhttp_request_get_response_code_line(req), response_body);

    evbuffer_free(response_body);
}

void StreamLoadForwardHandler::copy_response_headers(struct evkeyvalq* input_headers,
                                                     struct evkeyvalq* output_headers) {
    // Copy headers from upstream, excluding specific ones we manage ourselves
    for (struct evkeyval* header = input_headers->tqh_first; header != nullptr;
         header = header->next.tqe_next) {
        if (strcasecmp(header->key, "Transfer-Encoding") == 0 ||
            strcasecmp(header->key, "Content-Length") == 0 ||
            strcasecmp(header->key, "Date") == 0 || strcasecmp(header->key, "Server") == 0 ||
            strcasecmp(header->key, "Content-Type") == 0) {
            continue;
        }
        const char* value = header->value ? header->value : "";
        evhttp_add_header(output_headers, header->key, value);
    }
}

void StreamLoadForwardHandler::forward_connection_close_cb(struct evhttp_connection* conn,
                                                           void* arg) {
    auto* ctx = static_cast<StreamLoadForwardContext*>(arg);
    if (!ctx) {
        LOG(WARNING) << "Context is null in connection close callback";
        return;
    }

    ctx->handle_connection_close();
}

Status StreamLoadForwardHandler::parse_forward_target(const std::string& forward_to,
                                                      std::string& host, int& port) {
    size_t pos = forward_to.find(':');
    if (pos == std::string::npos) {
        return Status::InvalidArgument("Invalid forward_to format, should be host:port, got: {}",
                                       forward_to);
    }

    host = forward_to.substr(0, pos);
    std::string port_str = forward_to.substr(pos + 1);

    try {
        port = std::stoi(port_str);
    } catch (const std::exception& e) {
        LOG(WARNING) << "Exception while parsing port: " << port_str << ", what(): " << e.what();
        return Status::InvalidArgument("Invalid port number in forward_to: {}, exception: {}",
                                       port_str, e.what());
    }

    if (port <= 0 || port > 65535) {
        return Status::InvalidArgument("Port number must be between 1 and 65535, got: {}", port);
    }

    return Status::OK();
}

std::string StreamLoadForwardHandler::build_forward_url(HttpRequest* req) {
    std::string url;
    const std::string& raw_path = req->raw_path();

    // Parse /api/{db}/{table}/ part
    size_t pos = raw_path.find("/_stream_load_forward");
    if (pos != std::string::npos) {
        // Keep path prefix, replace _stream_load_forward with _stream_load
        url = raw_path.substr(0, pos) + "/_stream_load";
    } else {
        // If not found, use original path
        url = raw_path;
    }

    // Remove forward_to parameter, keep other parameters
    const auto& params = req->params();
    std::vector<std::string> query_parts;
    for (const auto& param : *params) {
        if (param.first != "forward_to") {
            query_parts.push_back(param.first + "=" + param.second);
        }
    }

    if (!query_parts.empty()) {
        std::ostringstream oss;
        for (size_t i = 0; i < query_parts.size(); ++i) {
            if (i != 0) {
                oss << "&";
            }
            oss << query_parts[i];
        }
        url += "?" + oss.str();
    }

    return url;
}

void StreamLoadForwardHandler::setup_forward_headers(HttpRequest* req,
                                                     struct evhttp_request* forward_req,
                                                     const std::string& target_host,
                                                     int target_port) {
    struct evkeyvalq* input_headers = evhttp_request_get_input_headers(req->get_evhttp_request());
    struct evkeyvalq* output_headers = evhttp_request_get_output_headers(forward_req);

    // Copy all headers from original request, except Host, Transfer-Encoding, and Content-Length
    for (struct evkeyval* header = input_headers->tqh_first; header != nullptr;
         header = header->next.tqe_next) {
        // Skip headers that conflict with libevent's automatic handling
        if (strcasecmp(header->key, "Host") == 0 ||
            strcasecmp(header->key, "Transfer-Encoding") == 0 ||
            strcasecmp(header->key, "Content-Length") == 0) {
            continue;
        }
        evhttp_add_header(output_headers, header->key, header->value);
    }

    // Set new Host header
    evhttp_add_header(output_headers, "Host",
                      fmt::format("{}:{}", target_host, target_port).c_str());
    // Add forwarding related headers
    evhttp_add_header(output_headers, "X-Forwarded-For", req->remote_host());
    evhttp_add_header(output_headers, "X-Forwarded-Proto", "http");
    evhttp_add_header(output_headers, "X-Forwarded-Host",
                      evhttp_request_get_host(req->get_evhttp_request()));
}

} // namespace doris
