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
