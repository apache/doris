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
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "cctz/time_zone.h"
#include "paimon/defs.h"
#include "paimon/predicate/literal.h"
#include "runtime/define_primitive_type.h"
#include "vec/exprs/vexpr_fwd.h"

namespace paimon {
class Predicate;
} // namespace paimon

namespace doris {
class RuntimeState;
class SlotDescriptor;
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class PaimonPredicateConverter {
public:
    PaimonPredicateConverter(const std::vector<SlotDescriptor*>& file_slot_descs,
                             RuntimeState* state);

    std::shared_ptr<paimon::Predicate> build(const VExprContextSPtrs& conjuncts);

private:
    struct FieldMeta {
        int32_t index = -1;
        paimon::FieldType field_type = paimon::FieldType::UNKNOWN;
        const SlotDescriptor* slot_desc = nullptr;
    };

    std::shared_ptr<paimon::Predicate> _convert_expr(const VExprSPtr& expr);
    std::shared_ptr<paimon::Predicate> _convert_compound(const VExprSPtr& expr);
    std::shared_ptr<paimon::Predicate> _convert_in(const VExprSPtr& expr);
    std::shared_ptr<paimon::Predicate> _convert_binary(const VExprSPtr& expr);
    std::shared_ptr<paimon::Predicate> _convert_is_null(const VExprSPtr& expr,
                                                        const std::string& fn_name);
    std::shared_ptr<paimon::Predicate> _convert_like_prefix(const VExprSPtr& expr);

    std::optional<FieldMeta> _resolve_field(const VExprSPtr& expr) const;
    std::optional<paimon::Literal> _convert_literal(const VExprSPtr& expr,
                                                    const SlotDescriptor& slot_desc,
                                                    paimon::FieldType field_type) const;
    std::optional<std::string> _extract_string_literal(const VExprSPtr& expr) const;

    static std::string _normalize_name(std::string_view name);
    static std::optional<std::string> _next_prefix(const std::string& prefix);
    static int32_t _seconds_to_days(int64_t seconds);
    static bool _is_integer_type(PrimitiveType type);
    static bool _is_string_type(PrimitiveType type);
    static bool _is_decimal_type(PrimitiveType type);
    static bool _is_date_type(PrimitiveType type);
    static bool _is_datetime_type(PrimitiveType type);
    static std::optional<paimon::FieldType> _to_paimon_field_type(PrimitiveType type,
                                                                  uint32_t precision);

    std::unordered_map<std::string, int32_t> _field_index_by_name;
    RuntimeState* _state = nullptr;
    cctz::time_zone _gmt_tz;
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
