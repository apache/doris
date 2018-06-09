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

#ifndef BDG_PALO_BE_SRC_RPC_CHECKSUM_H
#define BDG_PALO_BE_SRC_RPC_CHECKSUM_H

namespace palo {

/** Compute fletcher32 checksum for arbitary data. See
 * http://en.wikipedia.org/wiki/Fletcher%27s_checksum for more information
 * about the algorithm. Fletcher32 is the default checksum used in palo.
 *
 * @param data Pointer to the input data
 * @param len Input data length in bytes
 * @return The calculated checksum
 */
    extern uint32_t fletcher32(const void *data, size_t len);
} // namespace palo

#endif //BDG_PALO_BE_SRC_RPC_CHECKSUM_H
