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

#include <thrift/protocol/TProtocol.h>

#include <cstddef>
#include <cstdint>

namespace doris {

class ThriftContainerMemoryChecker {
public:
    virtual ~ThriftContainerMemoryChecker() = default;
    virtual void reserve_container_memory(uint32_t count, size_t element_size) = 0;
};

template <typename Container>
void reserve_thrift_container_memory(apache::thrift::protocol::TProtocol* protocol,
                                     const Container*, uint32_t count) {
    // The generated target type, rather than the untrusted wire tag, defines the allocation made
    // by vector::resize. Unknown fields never reach this generated allocation hook.
    if (auto* checker = dynamic_cast<ThriftContainerMemoryChecker*>(protocol); checker != nullptr) {
        checker->reserve_container_memory(count, sizeof(typename Container::value_type));
    }
}

} // namespace doris
