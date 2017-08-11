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

#ifndef BDG_PALO_BE_SRC_RPC_POLL_EVENT_H
#define BDG_PALO_BE_SRC_RPC_POLL_EVENT_H

namespace palo {

/// Polling event constants (mechanism-independent).
namespace poll_event {

/// Enumeration for poll interest constants.
    enum Flags {
        /// Data available to read
        READ   = 0x01,
        /// Urgent data available to read
        PRI    = 0x02,
        /// Writing can be performed without blocking
        WRITE  = 0x04,
        /// %Error condition
        ERROR  = 0x08,
        /// Hang up
        HUP    = 0x10,
        REMOVE = 0x1000,
        /// Stream socket peer closed connection
        RDHUP  = 0x2000
    };
    /// Returns a string representation of polling events
    /// @param events Bitmaks of polling events (::Flags)
    /// @return string representation of polling events
    std::string to_string(int events);
}

} // namespace palo
#endif //BDG_PALO_BE_SRC_RPC_POLL_EVENT_H
