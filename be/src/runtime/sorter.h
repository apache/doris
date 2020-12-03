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

#ifndef DORIS_BE_RUNTIME_SORTER_H
#define DORIS_BE_RUNTIME_SORTER_H

#include "common/status.h"

namespace doris {

class RowBatch;
class RuntimeState;
// Interface to sort rows
// 1. create one sorter
// 2. add data need be sorted through 'add_batch'
// 3. call 'input_done' when all data were added.
// 4. call 'get_next' fetch data which is sorted.
class Sorter {
public:
    virtual ~Sorter() {}

    virtual Status prepare(RuntimeState* state) { return Status::OK(); }

    // Add data to be sorted.
    virtual Status add_batch(RowBatch* batch) { return Status::OK(); }

    // call when all data be added
    virtual Status input_done() = 0;

    // fetch data already sorted,
    // client must insure that call this function AFTER call input_done
    virtual Status get_next(RowBatch* batch, bool* eos) = 0;

    virtual Status close(RuntimeState* state) { return Status::OK(); }
};

} // namespace doris

#endif
