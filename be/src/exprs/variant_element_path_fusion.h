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

#include "common/status.h"
#include "exprs/vexpr.h"

namespace doris {

class VectorizedFnCall;
class VariantElementPathFusionPlan;

// Builds an immutable plan after the expression tree is complete. Unsupported V1 and dynamic
// shapes and a single selector publish a null plan and retain the ordinary expression path.
Status build_variant_element_path_fusion_plan(
        const VectorizedFnCall& expression,
        std::shared_ptr<const VariantElementPathFusionPlan>* output);

// Executes consecutive constant element_at calls as one tokenized Variant V2 lookup. The FE
// expression tree and function names stay unchanged, so old BEs retain the original behavior during
// rolling upgrades. executed is false without evaluating any child unless the whole path is a
// strictly supported V2 candidate.
Status try_execute_variant_element_path_fusion(
        const std::shared_ptr<const VariantElementPathFusionPlan>& plan, VExprContext* context,
        const Block* block, const Selector* selector, size_t count, ColumnPtr* output,
        bool* executed);

} // namespace doris
