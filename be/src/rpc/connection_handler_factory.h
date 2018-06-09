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

#ifndef BDG_PALO_BE_SRC_RPC_CONNECTION_HANDLER_FACTORY_H
#define BDG_PALO_BE_SRC_RPC_CONNECTION_HANDLER_FACTORY_H

#include "dispatch_handler.h"

namespace palo {
/** Abstract class for creating default application dispatch handlers.
 */
class ConnectionHandlerFactory {
public:
    /** Destructor */
    virtual ~ConnectionHandlerFactory() { }

    /** Creates a connection dispatch handler.
     * @param dhp Reference to created dispatch handler
     */
    virtual void get_instance(DispatchHandlerPtr &dhp) = 0;

};
/// Smart pointer to ConnectionHandlerFactory
typedef std::shared_ptr<ConnectionHandlerFactory> ConnectionHandlerFactoryPtr;

} //namespace palo

#endif //BDG_PALO_BE_SRC_RPC_CONNECTION_HANDLER_FACTORY_H
