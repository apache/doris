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

#include "vec/common/sort/sorter.h"

namespace doris::vectorized {

///TODO: implement the Sorter interface
struct MockSorter : public Sorter {
    MockSorter() = default;
    Status append_block(Block* block) override { return Status::OK(); }

    Status prepare_for_read(bool is_spill) override { return Status::OK(); }

    Status get_next(RuntimeState* state, Block* block, bool* eos) override {
        *eos = true;
        return Status::OK();
    }

    size_t data_size() const override { return 0; }
};

} // namespace doris::vectorized