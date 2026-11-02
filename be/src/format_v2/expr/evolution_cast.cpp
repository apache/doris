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

#include "format_v2/expr/evolution_cast.h"

#include "core/data_type/data_type_nullable.h"
#include "exprs/vexpr_context.h"

namespace doris::format {

Status EvolutionCast::prepare(RuntimeState* state, const RowDescriptor& desc,
                              VExprContext* context) {
    RETURN_IF_ERROR(Cast::prepare(state, desc, context));
    if (remove_nullable(_children[0]->data_type())->get_primitive_type() == TYPE_DATEV2 &&
        remove_nullable(_data_type)->get_primitive_type() == TYPE_TIMESTAMP_NS) {
        // Iceberg DATE promotion must fail outside the nanosecond range, even in non-strict SQL
        // sessions. Otherwise a localized IS NOT NULL filter can silently discard retained rows.
        context->fn_context(_fn_context_index)->set_enable_strict_mode(true);
    }
    return Status::OK();
}

std::string EvolutionCast::debug_string() const {
    return "Evolution" + Cast::debug_string();
}

} // namespace doris::format
