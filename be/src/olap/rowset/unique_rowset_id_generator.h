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

#include "olap/rowset/rowset_id_generator.h"
#include "util/spinlock.h"
#include "util/uid_util.h"

namespace doris {

class UniqueRowsetIdGenerator : public RowsetIdGenerator {
public:    
    UniqueRowsetIdGenerator(const UniqueId& backend_uid);
    ~UniqueRowsetIdGenerator() {}

    RowsetId next_id() override;

    bool id_in_use(const RowsetId& rowset_id) const override;

    void release_id(const RowsetId& rowset_id) override;

private:
    mutable SpinLock _lock;
    const UniqueId _backend_uid;
    const int64_t _version = 2; // modify it when create new version id generator
    std::atomic<int64_t> _inc_id;
    std::unordered_set<RowsetId, RowsetIdHash> _valid_rowset_ids;

    DISALLOW_COPY_AND_ASSIGN(UniqueRowsetIdGenerator);
}; // UniqueRowsetIdGenerator

} // namespace doris
