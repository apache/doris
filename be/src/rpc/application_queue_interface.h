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
