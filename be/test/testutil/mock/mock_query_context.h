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

#pragma once
#include "runtime/query_context.h"

namespace doris {

inline TQueryOptions create_fake_query_options() {
    TQueryOptions query_options;
    query_options.query_type = TQueryType::EXTERNAL;
    return query_options;
}

struct MockQueryContext : public QueryContext {
    MockQueryContext()
            : QueryContext(TUniqueId {}, ExecEnv::GetInstance(), create_fake_query_options(),
                           TNetworkAddress {}, true, TNetworkAddress {},
                           QuerySource::GROUP_COMMIT_LOAD) {}
};

} // namespace doris
