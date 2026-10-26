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

#include <glog/logging.h>

#include <memory>
#include <vector>

namespace doris {

// AuxiliaryDataSet keeps shared ownership of payload objects referenced by
// lightweight views. It intentionally stores shared_ptr<const void> so current
// users can keep arbitrary owners alive without introducing a common base class.
// If future users need type-specific operations such as memory accounting, this
// can evolve to a typed holder/base-class model similar to DuckDB's
// AuxiliaryDataHolder.
//
// Owners are de-duplicated by shared_ptr ownership identity. De-duplication is
// not required for correctness, but avoids retaining the same owner repeatedly
// across chained derived columns.
class AuxiliaryDataSet {
public:
    using ConstOwnerPtr = std::shared_ptr<const void>;

    void clear() { _refs.clear(); }

    template <typename T>
    T* add_owner(std::shared_ptr<T> owner) {
        T* ptr = owner.get();
        add_owner_ref(ConstOwnerPtr(std::move(owner)));
        return ptr;
    }

    void add_owners_from(const AuxiliaryDataSet& other) {
        for (const auto& ref : other._refs) {
            add_owner_ref(ref);
        }
    }

private:
    void add_owner_ref(ConstOwnerPtr owner) {
        DCHECK(owner != nullptr);
        for (const auto& ref : _refs) {
            if (same_owner(ref, owner)) {
                return;
            }
        }
        _refs.push_back(std::move(owner));
    }

    static bool same_owner(const ConstOwnerPtr& lhs, const ConstOwnerPtr& rhs) {
        return !lhs.owner_before(rhs) && !rhs.owner_before(lhs);
    }

    std::vector<ConstOwnerPtr> _refs;
};

} // namespace doris
