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

#include <thrift/TToString.h>
#include <thrift/protocol/TProtocol.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <utility>
#include <vector>

namespace doris {

class ThriftContainerMemoryCharge {
public:
    virtual ~ThriftContainerMemoryCharge() = default;
};

class ThriftContainerMemoryChecker {
public:
    virtual ~ThriftContainerMemoryChecker() = default;
    virtual std::shared_ptr<ThriftContainerMemoryCharge> reserve_container_memory(
            uint32_t count, size_t element_size) = 0;
    virtual void retain_temporary_container_charge(
            std::shared_ptr<ThriftContainerMemoryCharge> charge) = 0;
};

template <typename T>
class ThriftMemoryTrackedVector : public std::vector<T> {
    using Base = std::vector<T>;

public:
    using Base::Base;
    using Base::operator=;

    ThriftMemoryTrackedVector() = default;
    ThriftMemoryTrackedVector(const ThriftMemoryTrackedVector&) = default;
    ThriftMemoryTrackedVector(ThriftMemoryTrackedVector&&) noexcept = default;
    ThriftMemoryTrackedVector& operator=(const ThriftMemoryTrackedVector&) = default;
    ThriftMemoryTrackedVector& operator=(ThriftMemoryTrackedVector&&) noexcept = default;

    ThriftMemoryTrackedVector& operator=(const Base& other) {
        Base::operator=(other);
        _memory_charge.reset();
        return *this;
    }

    ThriftMemoryTrackedVector& operator=(Base&& other) noexcept {
        Base::operator=(std::move(other));
        _memory_charge.reset();
        return *this;
    }

    void set_thrift_memory_charge(std::shared_ptr<ThriftContainerMemoryCharge> charge) {
        _memory_charge = std::move(charge);
    }

    void swap(ThriftMemoryTrackedVector& other) noexcept {
        Base::swap(other);
        _memory_charge.swap(other._memory_charge);
    }

    friend bool operator==(const ThriftMemoryTrackedVector& lhs,
                           const ThriftMemoryTrackedVector& rhs) {
        return static_cast<const Base&>(lhs) == static_cast<const Base&>(rhs);
    }

    friend bool operator<(const ThriftMemoryTrackedVector& lhs,
                          const ThriftMemoryTrackedVector& rhs) {
        return static_cast<const Base&>(lhs) < static_cast<const Base&>(rhs);
    }

    friend std::ostream& operator<<(std::ostream& out, const ThriftMemoryTrackedVector& values) {
        return out << apache::thrift::to_string(static_cast<const Base&>(values));
    }

private:
    std::shared_ptr<ThriftContainerMemoryCharge> _memory_charge;
};

template <typename Container>
void reserve_thrift_container_memory(apache::thrift::protocol::TProtocol* protocol,
                                     Container* container, uint32_t count) {
    // The generated target type, rather than the untrusted wire tag, defines the allocation made
    // by vector::resize. Unknown fields never reach this generated allocation hook.
    if (auto* checker = dynamic_cast<ThriftContainerMemoryChecker*>(protocol); checker != nullptr) {
        const size_t elements = std::max<size_t>(count, container->capacity());
        auto charge =
                checker->reserve_container_memory(elements, sizeof(typename Container::value_type));
        if constexpr (requires { container->set_thrift_memory_charge(charge); }) {
            // Generated fields retain the admission charge until the decoded allocation dies.
            container->set_thrift_memory_charge(std::move(charge));
        } else {
            checker->retain_temporary_container_charge(std::move(charge));
        }
    }
}

template <typename T>
void swap(ThriftMemoryTrackedVector<T>& lhs, ThriftMemoryTrackedVector<T>& rhs) noexcept {
    lhs.swap(rhs);
}

} // namespace doris
