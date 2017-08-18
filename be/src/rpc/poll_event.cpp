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
