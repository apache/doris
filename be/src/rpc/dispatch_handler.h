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

#ifndef BDG_PALO_BE_SRC_RPC_DISPATCH_HANDLER_H 
#define BDG_PALO_BE_SRC_RPC_DISPATCH_HANDLER_H 

#include "event.h"
#include <memory>

namespace palo {

class IOHandler;

/** Abstract base class for application dispatch handlers registered with
 * AsyncComm.  Dispatch handlers are the mechanism by which an application
 * is notified of communication events.
 */
class DispatchHandler : public std::enable_shared_from_this<DispatchHandler> {
    public:
        /** Destructor
        */
        virtual ~DispatchHandler() { return; }
        /** Callback method.  When the Comm layer needs to deliver an event to the
         * application, this method is called to do so.  The set of event types
         * include, CONNECTION_ESTABLISHED, DISCONNECT, MESSAGE, ERROR, and TIMER.
         *
         * @param event_ptr smart pointer to Event object
         */
        virtual void handle(EventPtr &event_ptr) = 0;
};

/// Smart pointer to DispatchHandler
typedef std::shared_ptr<DispatchHandler> DispatchHandlerPtr;

} //namespace palo

#endif //BDG_PALO_BE_SRC_RPC_DISPATCH_HANDLER_H 
