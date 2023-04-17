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

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "vec/core/block.h"
#include "vec/exec/scan/scanner_context.h"

namespace doris::vectorized {

class SharedScannerController {
public:
    std::pair<bool, int> should_build_scanner_and_queue_id(int my_node_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _scanner_parallel.find(my_node_id);

        if (it == _scanner_parallel.cend()) {
            _scanner_parallel.insert({my_node_id, 0});
            return {true, 0};
        } else {
            auto queue_id = it->second;
            _scanner_parallel[my_node_id] = queue_id + 1;
            return {false, queue_id + 1};
        }
    }

    void set_scanner_context(int my_node_id,
                             const std::shared_ptr<ScannerContext> scanner_context) {
        std::lock_guard<std::mutex> lock(_mutex);
        _scanner_context.insert({my_node_id, scanner_context});
    }

    bool scanner_context_is_ready(int my_node_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        return _scanner_context.find(my_node_id) != _scanner_context.end();
    }

    std::shared_ptr<ScannerContext> get_scanner_context(int my_node_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        return _scanner_context[my_node_id];
    }

private:
    std::mutex _mutex;
    std::map<int /*node id*/, int /*parallel*/> _scanner_parallel;
    std::map<int /*node id*/, std::shared_ptr<ScannerContext>> _scanner_context;
};

} // namespace doris::vectorized