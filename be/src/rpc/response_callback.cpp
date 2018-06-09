// Copyright (C) 2007-2016 Hypertable, Inc.
//
// This file is part of Hypertable.
// 
// Hypertable is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 3
// of the License, or any later version.
//
// Hypertable is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.

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
