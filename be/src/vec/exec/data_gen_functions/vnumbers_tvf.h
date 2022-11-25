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

#include "runtime/descriptors.h"
#include "vec/exec/data_gen_functions/vdata_gen_function_inf.h"

namespace doris {

class TextConverter;
class Tuple;
class TupleDescriptor;
class RuntimeState;
class MemPool;
class Status;

namespace vectorized {

class VNumbersTVF : public VDataGenFunctionInf {
public:
    VNumbersTVF(TupleId tuple_id, const TupleDescriptor* tuple_desc);
    ~VNumbersTVF() = default;

    Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

protected:
    int64_t _total_numbers;
    // Number of returned columns, actually only 1 column
    int _slot_num = 1;
    int64_t _cur_offset = 0;
};

} // namespace vectorized

} // namespace doris
