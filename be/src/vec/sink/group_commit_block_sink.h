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
#include "exec/data_sink.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/sink/async_writer_sink.h"
#include "vec/sink/volap_table_sink.h"
#include "vec/sink/writer/vgroup_commit_block_writer.h"

namespace doris {

class OlapTableSchemaParam;
class MemTracker;
class LoadBlockQueue;

namespace vectorized {

inline constexpr char GROUP_COMMIT_BLOCK_SINK[] = "GroupCommitBlockSink";

class GroupCommitBlockSink
        : public AsyncWriterSink<VGroupCommitBlockWriter, GROUP_COMMIT_BLOCK_SINK> {
public:
    GroupCommitBlockSink(ObjectPool* pool, const RowDescriptor& row_desc,
                         const std::vector<TExpr>& texprs);

    ~GroupCommitBlockSink() override;

    Status init(const TDataSink& sink) override;

    Status close(RuntimeState* state, Status close_status) override;
};

} // namespace vectorized
} // namespace doris
