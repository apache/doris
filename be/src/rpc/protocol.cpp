// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include "compat.h"
#include "comm_buf.h"
#include "event.h"
#include "protocol.h"
#include "error.h"
#include "common/logging.h"
#include "serialization.h"

#include <cassert>
#include <iostream>

namespace palo {

int32_t Protocol::response_code(const Event *event) {
    if (event->type == Event::ERROR) {
        return event->error;
    }
    const uint8_t *msg = event->payload;
    size_t remaining = event->payload_len;
    try {
        return serialization::decode_i32(&msg, &remaining);
    } catch (Exception &e) {
        return e.code();
    }
}

std::string Protocol::string_format_message(const Event *event) {
    int error = error::OK;
    if (event == 0) {
        return std::string("NULL event");
    }
    const uint8_t *msg = event->payload;
    size_t remaining = event->payload_len;
    if (event->type != Event::MESSAGE) {
        return event->to_str();
    }
    try {
        error = serialization::decode_i32(&msg, &remaining);
        if (error == error::OK) {
            return error::get_text(error);
        }
        uint16_t len;
        const char *str = serialization::decode_str16(&msg, &remaining, &len);
        return std::string(str, len > 150 ? 150 : len);
    } catch (Exception &e) {
        return format("%s - %s", e.what(), error::get_text(e.code()));
    }
}

CommBufPtr
Protocol::create_error_message(CommHeader &header, int error, const char *msg) {
    CommBufPtr cbuf 
        = std::make_shared<CommBuf>(header, 4 + serialization::encoded_length_str16(msg));
    cbuf->append_i32(error);
    cbuf->append_str16(msg);
    return cbuf;
}

} //namespace palo
