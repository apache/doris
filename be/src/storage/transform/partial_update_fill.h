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

#include "storage/transform/block_transform.h"

namespace doris::segment_v2 {

// Fixed partial update (UPDATE_FIXED_COLUMNS): widen the narrow input to the
// full schema, probe each key against the load's rowset snapshot, and fill the
// missing columns from history or defaults. Delete-bitmap marks are written
// right away inside apply(); probe counters land in ctx.partial_update_stats.
class FixedPartialUpdateFillStage : public BlockTransform {
public:
    Status apply(TransformExecContext& ctx, Block* block) const override;
    std::string_view name() const override { return "FixedPartialUpdateFill"; }
};

} // namespace doris::segment_v2
