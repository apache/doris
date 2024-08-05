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

#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>

#include "common/status.h"
#include "exec/olap_common.h"
#include "olap/shared_predicate.h"
#include "olap/tablet_schema.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "util/binary_cast.hpp"
#include "vec/common/arena.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
class ColumnPredicate;

namespace vectorized {

class RuntimePredicate {
public:
    RuntimePredicate(const TTopnFilterDesc& desc);

    void init_target(int32_t target_node_id,
                     phmap::flat_hash_map<int, SlotDescriptor*> slot_id_to_slot_desc);

    bool enable() const {
        // when sort node and scan node are not in the same fragment, predicate will be disabled
        std::shared_lock<std::shared_mutex> rlock(_rwlock);
        return _detected_source && _detected_target;
    }

    void set_detected_source() {
        std::unique_lock<std::shared_mutex> wlock(_rwlock);
        _detected_source = true;
    }

    Status set_tablet_schema(int32_t target_node_id, TabletSchemaSPtr tablet_schema) {
        std::unique_lock<std::shared_mutex> wlock(_rwlock);
        check_target_node_id(target_node_id);
        if (_contexts[target_node_id].tablet_schema) {
            return Status::OK();
        }
        RETURN_IF_ERROR(tablet_schema->have_column(_contexts[target_node_id].col_name));
        _contexts[target_node_id].tablet_schema = tablet_schema;
        int64_t index = DORIS_TRY(_contexts[target_node_id].get_field_index())
                                _contexts[target_node_id]
                                        .predicate = SharedPredicate::create_shared(index);
        return Status::OK();
    }

    std::shared_ptr<ColumnPredicate> get_predicate(int32_t target_node_id) {
        std::shared_lock<std::shared_mutex> rlock(_rwlock);
        check_target_node_id(target_node_id);
        return _contexts.find(target_node_id)->second.predicate;
    }

    Status update(const Field& value);

    bool has_value() const {
        std::shared_lock<std::shared_mutex> rlock(_rwlock);
        return _has_value;
    }

    Field get_value() const {
        std::shared_lock<std::shared_mutex> rlock(_rwlock);
        return _orderby_extrem;
    }

    std::string get_col_name(int32_t target_node_id) const {
        check_target_node_id(target_node_id);
        return _contexts.find(target_node_id)->second.col_name;
    }

    bool is_asc() const { return _is_asc; }

    bool nulls_first() const { return _nulls_first; }

    bool target_is_slot(int32_t target_node_id) const {
        check_target_node_id(target_node_id);
        return _contexts.find(target_node_id)->second.target_is_slot();
    }

    const TExpr& get_texpr(int32_t target_node_id) const {
        check_target_node_id(target_node_id);
        return _contexts.find(target_node_id)->second.expr;
    }

private:
    void check_target_node_id(int32_t target_node_id) const {
        if (!_contexts.contains(target_node_id)) {
            std::string msg = "context target node ids: [";
            bool first = true;
            for (auto p : _contexts) {
                if (first) {
                    first = false;
                } else {
                    msg += ',';
                }
                msg += std::to_string(p.first);
            }
            msg += "], input target node is: " + std::to_string(target_node_id);
            DCHECK(false) << msg;
        }
    }
    struct TargetContext {
        TExpr expr;
        std::string col_name;
        TabletSchemaSPtr tablet_schema;
        std::shared_ptr<ColumnPredicate> predicate;

        Result<int32_t> get_field_index() {
            const auto& column = *DORIS_TRY(tablet_schema->column(col_name));
            return tablet_schema->field_index(column.unique_id());
        }

        bool target_is_slot() const { return expr.nodes[0].node_type == TExprNodeType::SLOT_REF; }
    };

    bool _init(PrimitiveType type);

    mutable std::shared_mutex _rwlock;

    bool _nulls_first;
    bool _is_asc;
    std::map<int32_t, TargetContext> _contexts;

    Field _orderby_extrem {Field::Types::Null};
    Arena _predicate_arena;
    std::function<std::string(const Field&)> _get_value_fn;
    std::function<ColumnPredicate*(const TabletColumn&, int, const std::string&, bool,
                                   vectorized::Arena*)>
            _pred_constructor;
    bool _detected_source = false;
    bool _detected_target = false;
    bool _has_value = false;
};

} // namespace vectorized
} // namespace doris
