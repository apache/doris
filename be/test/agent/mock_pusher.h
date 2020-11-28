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

#ifndef DORIS_BE_SRC_AGENT_MOCK_MOCK_PUSHER_H
#define DORIS_BE_SRC_AGENT_MOCK_MOCK_PUSHER_H

#include "gmock/gmock.h"

namespace doris {

class MockPusher : public Pusher {
public:
    MockPusher(const TPushReq& push_req);
    MOCK_METHOD0(init, AgentStatus());
    MOCK_METHOD1(process, AgentStatus(std::vector<TTabletInfo>* tablet_infos));
}; // class MockPusher
} // namespace doris
#endif // DORIS_BE_SRC_AGENT_SERVICE_PUSHER_H
