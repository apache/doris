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

#ifndef BDG_PALO_BE_SRC_RPC_APPLICATION_QUEUE_INTERFACE_H
#define BDG_PALO_BE_SRC_RPC_APPLICATION_QUEUE_INTERFACE_H

#include "application_handler.h"

namespace palo {
/** @addtogroup palo
 *  @{
 */

/**
 * Abstract interface for application queue.
 */

class ApplicationQueueInterface {
public:
    /** Adds an application handler to queue.
    */
    virtual void add(ApplicationHandler *app_handler) = 0;

    /** Adds an application handler to queue without locking.
     * This method is similar to #add except that it does not do any
     * locking to serialize access to the queue.  It is for situations
     * where access serialization is handled by the caller.
     */
    virtual void add_unlocked(ApplicationHandler *app_handler) = 0;
};

/// Smart pointer to ApplicationQueueInterface
typedef std::shared_ptr<ApplicationQueueInterface> ApplicationQueueInterfacePtr;

}

#endif //BDG_PALO_BE_SRC_RPC_APPLICATION_QUEUE_INTERFACE_H
