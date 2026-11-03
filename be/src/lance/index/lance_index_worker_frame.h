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
// specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <thrift/TConfiguration.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

namespace doris::lance {

// Frame format of the isolated lance index worker pipe (gensrc/thrift/
// LanceIndexWorker.thrift): one uint32 big-endian length prefix followed by
// exactly that many payload bytes holding exactly one thrift-compact-encoded
// struct - TLanceIndexWorkerDispatch on the worker's stdin; the handshake
// frame and then at most one TLanceIndexWorkerResult on its stdout. Shared by
// both endpoints of the pipe: the worker library reads whole frames off its
// file descriptors, the supervisor assembles them incrementally off its poll
// loop. The discipline is identical on both sides and inherited from the
// closed dispatcher series codec: the length prefix is checked against the
// caller's cap BEFORE any allocation, the decoder recursion is depth-limited,
// container reads are element-limited, and trailing bytes after the one struct
// are a protocol violation (exactly one struct per frame).

// The thrift decoder recursion cap: a frame is FE-derived data at most one
// re-serialization away, and a deeply nested one must be rejected by limit,
// never by stack overflow.
inline constexpr int LANCE_WORKER_FRAME_DECODE_DEPTH_LIMIT = 16;

// Max elements read per thrift container (the dispatch's storage options and
// debug points maps/lists are bounded far below this; the limit exists so a
// hostile frame's container headers cannot drive unbounded allocation).
inline constexpr int32_t LANCE_WORKER_FRAME_CONTAINER_LIMIT = 1024;

// Encodes one struct (TLanceIndexWorkerDispatch / TLanceIndexWorkerResult /
// the handshake) to thrift-compact payload bytes, without the length prefix.
template <typename T>
bool lance_worker_encode_compact(const T& value, std::vector<uint8_t>* out);

// Decodes exactly one struct from thrift-compact payload bytes. Fails on any
// decode error, on nesting beyond the depth limit, and on trailing bytes after
// the struct - a frame is one struct or it is not a frame.
template <typename T>
bool lance_worker_decode_compact(const uint8_t* data, uint32_t size, T* out);

template <typename T>
bool lance_worker_decode_compact(const std::string& frame, T* out) {
    return lance_worker_decode_compact(reinterpret_cast<const uint8_t*>(frame.data()),
                                       static_cast<uint32_t>(frame.size()), out);
}

template <typename T>
bool lance_worker_decode_compact(const std::vector<uint8_t>& frame, T* out) {
    return lance_worker_decode_compact(frame.data(), static_cast<uint32_t>(frame.size()), out);
}

// Reads the uint32 big-endian length prefix.
uint32_t lance_worker_frame_be32(const uint8_t* header);

// Prepends the 4-byte big-endian length prefix of `payload`, producing the
// complete wire frame.
void lance_worker_frame_wrap(const std::vector<uint8_t>& payload, std::vector<uint8_t>* out);

// Blocking whole-frame I/O on a file descriptor (the worker side of the pipe).
//
// read: reads the length prefix, rejects a zero or over-cap length BEFORE any
// allocation, then reads exactly that many payload bytes; a partial or
// oversized frame is a malformed protocol and never reaches the decoder.
// write: the full prefix + payload, retrying on EINTR; false on any short or
// failed write (a torn frame is no frame).
bool lance_worker_read_frame(int fd, uint32_t cap, std::vector<uint8_t>* out);
bool lance_worker_write_frame(int fd, const std::vector<uint8_t>& payload);

// Incremental length-prefixed frame assembly for poll-loop readers (the
// supervisor side of the pipe). The internal buffer can never grow past
// 4 + cap + one read chunk, because poll_frame() is consulted after every
// append.
struct LanceWorkerFrameAssembler {
    uint32_t cap = 0;
    std::string buf;

    // -1 malformed (zero/oversized length), 0 need more bytes, 1 one frame out.
    int poll_frame(std::string* payload_out);
};

// ── Template implementations ──

template <typename T>
bool lance_worker_encode_compact(const T& value, std::vector<uint8_t>* out) {
    try {
        auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
        apache::thrift::protocol::TCompactProtocol protocol(transport);
        value.write(&protocol);
        uint8_t* buffer = nullptr;
        uint32_t size = 0;
        transport->getBuffer(&buffer, &size);
        out->assign(buffer, buffer + size);
    } catch (...) {
        return false;
    }
    return true;
}

template <typename T>
bool lance_worker_decode_compact(const uint8_t* data, uint32_t size, T* out) {
    try {
        auto config = std::make_shared<apache::thrift::TConfiguration>();
        config->setMaxMessageSize(static_cast<int>(size));
        config->setRecursionLimit(LANCE_WORKER_FRAME_DECODE_DEPTH_LIMIT);
        auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>(
                const_cast<uint8_t*>(data), size,
                apache::thrift::transport::TMemoryBuffer::OBSERVE, config);
        apache::thrift::protocol::TCompactProtocol protocol(
                transport, static_cast<int32_t>(size), LANCE_WORKER_FRAME_CONTAINER_LIMIT);
        out->read(&protocol);
        // Exactly one struct per frame: trailing bytes are a protocol violation.
        return transport->available_read() == 0;
    } catch (...) {
        return false;
    }
}

} // namespace doris::lance
