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

#include <mutex>

#include "olap/olap_define.h"
#include "olap/olap_common.h"

namespace doris {

class OlapMeta;

class RowsetIdGenerator {
public:    
    RowsetIdGenerator(OlapMeta* meta) : _meta(meta) { }
    ~RowsetIdGenerator() {}

    // This function would try to restore sate from meta first,
    // If there is no such state, will initialize a state, and store 
    // it into meta.
    OLAPStatus init();

    // generator a id according to data dir
    // rowsetid is not globally unique, it is dir level
    // it saves the batch end id into meta env
    OLAPStatus get_next_id(RowsetId* rowset_id); 

    OLAPStatus set_next_id(RowsetId new_rowset_id);

private:
    OlapMeta* _meta = nullptr;

    std::mutex _lock;
    RowsetId _next_id = -1;
    RowsetId _id_batch_end = -1;
}; // RowsetIdGenerator

} // namespace doris
