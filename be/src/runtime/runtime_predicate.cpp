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

#include "runtime/runtime_predicate.h"

#include <memory>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/exception.h"
#include "common/status.h"
#include "olap/accept_null_predicate.h"
#include "olap/column_predicate.h"
#include "olap/predicate_creator.h"
#include "runtime/define_primitive_type.h"

namespace doris::vectorized {

RuntimePredicate::RuntimePredicate(const TTopnFilterDesc& desc)
        : _nulls_first(desc.null_first), _is_asc(desc.is_asc) {
    DCHECK(!desc.target_node_id_to_target_expr.empty());
    for (auto p : desc.target_node_id_to_target_expr) {
        _contexts[p.first].expr = p.second;
    }

    _type = thrift_to_type(desc.target_node_id_to_target_expr.begin()
                                   ->second.nodes[0]
                                   .type.types[0]
                                   .scalar_type.type);
    if (!_init(_type)) {
        std::stringstream ss;
        desc.target_node_id_to_target_expr.begin()->second.nodes[0].printTo(ss);
        throw Exception(ErrorCode::INTERNAL_ERROR, "meet invalid type, type={}, expr={}",
                        type_to_string(_type), ss.str());
    }

    // For ASC  sort, create runtime predicate col_name <= max_top_value
    // since values that > min_top_value are large than any value in current topn values
    // For DESC sort, create runtime predicate col_name >= min_top_value
    // since values that < min_top_value are less than any value in current topn values
    _pred_constructor = _is_asc ? create_comparison_predicate<PredicateType::LE>
                                : create_comparison_predicate<PredicateType::GE>;
}

Status RuntimePredicate::init_target(
        int32_t target_node_id, phmap::flat_hash_map<int, SlotDescriptor*> slot_id_to_slot_desc,
        const int column_id) {
    if (column_id < 0) {
        return Status::OK();
    }
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    check_target_node_id(target_node_id);
    // order by abs(col1) limit x;
    // cannot be used min-max filter, no need create predicate.
    // but can used in VTopNPred.execute_column
    if (target_is_slot(target_node_id)) {
        _contexts[target_node_id].col_name =
                slot_id_to_slot_desc[get_texpr(target_node_id).nodes[0].slot_ref.slot_id]
                        ->col_name();
        _contexts[target_node_id].col_data_type =
                slot_id_to_slot_desc[get_texpr(target_node_id).nodes[0].slot_ref.slot_id]->type();
        _contexts[target_node_id].predicate = SharedPredicate::create_shared(
                cast_set<uint32_t>(column_id), _contexts[target_node_id].col_name);
    }
    _detected_target = true;
    return Status::OK();
}

bool RuntimePredicate::_init(PrimitiveType type) {
    return is_int_or_bool(type) || is_decimal(type) || is_string_type(type) || is_date_type(type) ||
           is_time_type(type) || is_ip(type) || is_varbinary(type);
}

Status RuntimePredicate::update(const Field& value) {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    // skip null value
    if (value.is_null()) {
        return Status::OK();
    }

    bool updated = false;

    if (UNLIKELY(_orderby_extrem.is_null())) {
        _orderby_extrem = value;
        updated = true;
    } else {
        if ((_is_asc && value < _orderby_extrem) || (!_is_asc && value > _orderby_extrem)) {
            _orderby_extrem = value;
            updated = true;
        }
    }

    _has_value = true;

    if (!updated) {
        return Status::OK();
    }
    for (auto p : _contexts) {
        auto ctx = p.second;
        if (ctx.predicate == nullptr) {
            // 1. `init_target` will not create predicate. example : `order by abs(col1) limit x;`
            // So don't need create new `ColumnPredicate`,
            // but need update `_orderby_extrem` for  `VTopNPred.execute_column`
            // 2. this `RuntimePredicate` will associate multiple scan nodes.
            // When the sort node is updated, some scan nodes may not have called `init_target` yet.
            // example:
            //SELECT subq1.pk AS pk1 FROM (
            //    ( SELECT t1.pk  FROM tb AS t1 )
            //    UNION ALL
            //    ( SELECT t1.pk  FROM tb AS t1  ORDER BY t1.pk ))
            //    subq1
            //WHERE subq1.pk <> (
            //    SELECT t1.pk  FROM tb AS t1  ORDER BY t1.pk LIMIT 1
            //) ORDER BY 1 LIMIT 1 ;
            continue;
        }
        std::shared_ptr<ColumnPredicate> pred =
                _pred_constructor(ctx.predicate->column_id(), ctx.col_name, ctx.col_data_type,
                                  _orderby_extrem, false);

        // For NULLS FIRST, wrap a AcceptNullPredicate to return true for NULL
        // since ORDER BY ASC/DESC should get NULL first but pred returns NULL
        // and NULL in where predicate will be treated as FALSE
        if (_nulls_first) {
            pred = AcceptNullPredicate::create_shared(pred);
        }

        ((SharedPredicate*)ctx.predicate.get())->set_nested(pred);
    }
    return Status::OK();
}

} // namespace doris::vectorized
