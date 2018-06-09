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
#include "poll_event.h"

#include <string>

namespace palo {

namespace poll_event {

std::string to_string(int events) {
    std::string str;
    std::string or_str;
    if (events & poll_event::READ) {
        str.append("READ");
        or_str = std::string("|");
    }
    if (events & poll_event::PRI) {
        str += or_str + "PRI";
        or_str = std::string("|");
    }
    if (events & poll_event::WRITE) {
        str += or_str + "WRITE";
        or_str = std::string("|");
    }
    if (events & poll_event::ERROR) {
        str += or_str + "ERROR";
        or_str = std::string("|");
    }
    if (events & poll_event::HUP) {
        str += or_str + "HUP";
        or_str = std::string("|");
    }
    if (events & poll_event::REMOVE) {
        str += or_str + "REMOVE";
        or_str = std::string("|");
    }
    if (events & poll_event::RDHUP) {
        str += or_str + "RDHUP";
        or_str = std::string("|");
    }
    return str;
}

} //namespace poll_event

} //namespace palo
