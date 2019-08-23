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
    UniqueRowsetIdGenerator(UniqueId backend_uid);
    ~UniqueRowsetIdGenerator() {}

    // generator a id according to data dir
    // rowsetid is not globally unique, it is dir level
    // it saves the batch end id into meta env
    OLAPStatus next_id(RowsetId* rowset_id); 

    bool id_in_use(RowsetId& rowset_id);

    void release_id(RowsetId& rowset_id);

private:
    SpinLock _lock;
    UniqueId _backend_uid;
    int64_t _version = 2; // modify it when create new version id generator
    int64_t _inc_id = 0;
    std::set<RowsetId> _valid_rowset_ids; 
}; // FeBasedRowsetIdGenerator

} // namespace doris
