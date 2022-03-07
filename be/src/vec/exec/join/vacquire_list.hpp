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

#include <memory>
#include <vector>

namespace doris::vectorized {

template <typename Element, int batch_size = 8>
struct AcquireList {
    using Batch = Element[batch_size];

    Element& acquire(Element&& element) {
        if (_current_batch == nullptr) {
            _current_batch.reset(new Element[batch_size]);
        }
        if (current_full()) {
            _lst.emplace_back(std::move(_current_batch));
            _current_batch.reset(new Element[batch_size]);
            _current_offset = 0;
        }

        auto base_addr = _current_batch.get();
        base_addr[_current_offset] = std::move(element);
        auto& ref = base_addr[_current_offset];
        _current_offset++;
        return ref;
    }

    void remove_last_element() { _current_offset--; }

private:
    bool current_full() { return _current_offset == batch_size; }
    std::vector<std::unique_ptr<Element[]>> _lst;
    std::unique_ptr<Element[]> _current_batch;
    int _current_offset = 0;
};
} // namespace doris::vectorized
