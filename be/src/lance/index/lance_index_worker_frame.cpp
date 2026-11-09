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

#include "lance/index/lance_index_worker_frame.h"

#include <cerrno>
#include <unistd.h>

namespace doris::lance {
namespace {

size_t read_up_to(int fd, uint8_t* buffer, size_t size) {
    size_t total = 0;
    while (total < size) {
        ssize_t count = ::read(fd, buffer + total, size - total);
        if (count == 0) {
            break; // EOF before the frame completed
        }
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        total += static_cast<size_t>(count);
    }
    return total;
}

bool write_full(int fd, const uint8_t* data, size_t size) {
    size_t total = 0;
    while (total < size) {
        ssize_t count = ::write(fd, data + total, size - total);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        total += static_cast<size_t>(count);
    }
    return true;
}

} // namespace

uint32_t lance_worker_frame_be32(const uint8_t* header) {
    return (static_cast<uint32_t>(header[0]) << 24) | (static_cast<uint32_t>(header[1]) << 16) |
           (static_cast<uint32_t>(header[2]) << 8) | static_cast<uint32_t>(header[3]);
}

void lance_worker_frame_wrap(const std::vector<uint8_t>& payload, std::vector<uint8_t>* out) {
    uint32_t length = static_cast<uint32_t>(payload.size());
    out->clear();
    out->reserve(payload.size() + 4);
    out->push_back(static_cast<uint8_t>(length >> 24));
    out->push_back(static_cast<uint8_t>(length >> 16));
    out->push_back(static_cast<uint8_t>(length >> 8));
    out->push_back(static_cast<uint8_t>(length));
    out->insert(out->end(), payload.begin(), payload.end());
}

bool lance_worker_read_frame(int fd, uint32_t cap, std::vector<uint8_t>* out) {
    uint8_t header[4];
    if (read_up_to(fd, header, sizeof(header)) != sizeof(header)) {
        return false;
    }
    uint32_t length = lance_worker_frame_be32(header);
    if (length == 0 || length > cap) {
        return false;
    }
    out->resize(length);
    return read_up_to(fd, out->data(), length) == length;
}

bool lance_worker_write_frame(int fd, const std::vector<uint8_t>& payload) {
    uint32_t length = static_cast<uint32_t>(payload.size());
    uint8_t header[4] = {static_cast<uint8_t>(length >> 24), static_cast<uint8_t>(length >> 16),
                         static_cast<uint8_t>(length >> 8), static_cast<uint8_t>(length)};
    return write_full(fd, header, sizeof(header)) && write_full(fd, payload.data(), length);
}

int LanceWorkerFrameAssembler::poll_frame(std::string* payload_out) {
    if (buf.size() < 4) {
        return 0;
    }
    const uint32_t length = lance_worker_frame_be32(reinterpret_cast<const uint8_t*>(buf.data()));
    if (length == 0 || length > cap) {
        return -1;
    }
    if (buf.size() < 4 + static_cast<size_t>(length)) {
        return 0;
    }
    payload_out->assign(buf, 4, length);
    buf.erase(0, 4 + static_cast<size_t>(length));
    return 1;
}

} // namespace doris::lance
