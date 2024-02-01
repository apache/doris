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

#include <cstdint>
#include <vector>

#include "common/global_types.h"
#include "vec/exec/data_gen_functions/vdata_gen_function_inf.h"

namespace doris {

class TupleDescriptor;
class RuntimeState;
class Status;
class TScanRangeParams;

namespace vectorized {
class Block;

class VNumbersTVF : public VDataGenFunctionInf {
public:
    VNumbersTVF(TupleId tuple_id, const TupleDescriptor* tuple_desc);
    ~VNumbersTVF() override = default;

    Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

private:
    bool _use_const = false;
    int64_t _const_value = 0;
    int64_t _total_numbers = 0;
    // Number of returned columns, actually only 1 column
    int _slot_num = 1;
    int64_t _next_number = 0;
};

} // namespace vectorized

} // namespace doris
