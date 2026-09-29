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

#include <functional>
#include <mutex>
#include <optional>
#include <vector>

#include "common/status.h"

namespace doris {

// One-shot completion for the EOS barrier of one tablets channel. Sender accounting
// and final close remain in TabletsChannel/CloudTabletsChannel. This object only
// owns RPC callbacks, never a channel or request, so pending RPCs cannot keep a
// cancelled/expired load channel alive.
class EosCompletion {
public:
    using Callback = std::function<void(const Status&)>;

    ~EosCompletion();

    // Register only after all synchronous access to the RPC objects has ended.
    // May invoke callback inline if close or cancellation has already completed.
    void add_waiter(Callback callback);

    // The first terminal result wins. Invoke callbacks outside the mutex.
    void complete(const Status& status);

private:
    std::mutex _mutex;
    std::optional<Status> _status;
    std::vector<Callback> _waiters;
};

} // namespace doris
