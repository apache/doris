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
