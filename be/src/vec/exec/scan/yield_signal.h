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

#include <atomic>
#include <boost/asio.hpp>
#include <mutex>

namespace doris::vectorized {

class YieldSignal {
public:
    YieldSignal();
    ~YieldSignal();

    void set_with_delay(std::chrono::nanoseconds max_run_nanos,
                        boost::asio::io_context* io_context);
    void reset();
    bool is_set() const;
    void yield_immediately_for_termination();

private:
    std::atomic<bool> _yield;
    long _running_sequence;
    bool _termination_started;
    std::shared_ptr<boost::asio::steady_timer> _yield_timer;
    mutable std::mutex _mutex;
};

} // namespace doris::vectorized
