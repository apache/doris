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
#include "vtablet_sink.h"

namespace doris {

namespace stream_load {

class GroupCommitVOlapTableSink : public VOlapTableSink {
public:
    GroupCommitVOlapTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                              const std::vector<TExpr>& texprs, Status* status);

    void handle_block(vectorized::Block* input_block, int64_t rows, int64_t filter_rows) override;
};

} // namespace stream_load
} // namespace doris
