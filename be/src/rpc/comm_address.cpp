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
#include "comm_address.h"

namespace palo {

std::string CommAddress::to_str() const {
    if (m_type == PROXY) {
        return proxy;
    } else if (m_type == INET) {
        return InetAddr::format(inet);
    }
    return "[NULL]";

}

} //namespace palo
