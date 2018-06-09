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

#pragma once

namespace palo {

#define RPC_COMMAND_TRANSMIT_DATA           0
#define RPC_COMMAND_TABLET_WRITER_OPEN      1
#define RPC_COMMAND_TABLET_WRITER_ADD_BATCH 2
#define RPC_COMMAND_TABLET_WRITER_CLOSE     3
#define RPC_COMMAND_TABLET_WRITER_CANCEL    4

}
