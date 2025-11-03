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

#include "common/global_types.h"
#include "runtime/descriptors.h"
#include "vec/core/block.h"

namespace doris {

class RuntimeState;
class Status;
class TScanRangeParams;

namespace pipeline {

class VDataGenFunctionInf {
public:
    VDataGenFunctionInf(TupleId tuple_id, const TupleDescriptor* tuple_desc)
            : _tuple_id(tuple_id), _tuple_desc(tuple_desc) {}

    virtual ~VDataGenFunctionInf() = default;

    // Should set function parameters in this method
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) = 0;
    virtual Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) = 0;
    Status close(RuntimeState* state) { return Status::OK(); }

    void set_tuple_desc(const TupleDescriptor* tuple_desc) { _tuple_desc = tuple_desc; }

protected:
    TupleId _tuple_id;
    // Descriptor of tuples generated
    const TupleDescriptor* _tuple_desc = nullptr;
};

} // namespace pipeline

} // namespace doris
