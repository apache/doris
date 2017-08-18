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

#include "compat.h"
#include "comm_buf.h"
#include "comm_header.h"
#include "protocol.h"
#include "response_callback.h"
#include "error.h"

#include <limits>

namespace palo {

int ResponseCallback::error(int error, const std::string& msg) {
    CommHeader header;
    header.initialize_from_request_header(m_event->header);
    CommBufPtr cbp;
    size_t max_msg_size = std::numeric_limits<int16_t>::max();
    if (msg.length() < max_msg_size) {
        cbp = Protocol::create_error_message(header, error, msg.c_str());
    } else {
        cbp = Protocol::create_error_message(header, error, msg.substr(0, max_msg_size).c_str());
    }
    return m_comm->send_response(m_event->addr, cbp);
}

int ResponseCallback::response_ok() {
    CommHeader header;
    header.initialize_from_request_header(m_event->header);
    CommBufPtr cbp(new CommBuf(header, 4));
    cbp->append_i32(error::OK);
    return m_comm->send_response(m_event->addr, cbp);
}

} //namespace palo
