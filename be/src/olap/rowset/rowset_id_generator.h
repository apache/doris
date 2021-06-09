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

#include "olap/olap_common.h"
#include "olap/olap_define.h"

namespace doris {

class OlapMeta;

// all implementations must be thread-safe
class RowsetIdGenerator {
public:
    RowsetIdGenerator() {}
    virtual ~RowsetIdGenerator() {}

    // generate and return the next global unique rowset id
    virtual RowsetId next_id() = 0;

    // check whether the rowset id is useful or validate
    // for example, during gc logic, gc thread finds a file
    // and it could not find it under rowset list. but it maybe in use
    // during load procedure. Gc thread will check it using this method.
    virtual bool id_in_use(const RowsetId& rowset_id) const = 0;

    // remove the rowsetid from useful rowsetid list.
    virtual void release_id(const RowsetId& rowset_id) = 0;
}; // RowsetIdGenerator

} // namespace doris
