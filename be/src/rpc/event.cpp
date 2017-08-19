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
#include <iostream>

extern "C" {
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
}

#include "error.h"
#include "string_ext.h"
#include "reactor_runner.h"
#include "event.h"

namespace palo {

std::string Event::to_str() const {
    std::string dstr;
    dstr = "Event: type=";

    if (type == CONNECTION_ESTABLISHED) {
        dstr += "CONNECTION_ESTABLISHED";
    } else if (type == DISCONNECT) {
        dstr += "DISCONNECT";
    } else if (type == MESSAGE) {
        dstr += "MESSAGE";
        dstr += (std::string)" version=" + (int)header.version;
        dstr += (std::string)" total_len=" + (int)header.total_len;
        dstr += (std::string)" header_len=" + (int)header.header_len;
        dstr += (std::string)" header_checksum=" + (int)header.header_checksum;
        dstr += (std::string)" flags=" + (int)header.flags;
        dstr += (std::string)" id=" + (int)header.id;
        dstr += (std::string)" gid=" + (int)header.gid;
        dstr += (std::string)" timeout_ms=" + (int)header.timeout_ms;
        dstr += (std::string)" payload_checksum=" + (int)header.payload_checksum;
        dstr += (std::string)" command=" + (int)header.command;
    } else if (type == TIMER) {
        dstr += "TIMER";
    } else if (type == ERROR) {
        dstr += "ERROR";
    } else {
        dstr += (int)type;
    }

    if (error != error::OK) {
        dstr += (std::string)" \"" + error::get_text(error) + "\"";
    }

    if (type != TIMER) {
        dstr += " from=";
        dstr += addr.format();
    }

    return dstr;
}

} //namespace palo
