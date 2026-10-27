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
#include <unordered_set>

#include "common/status.h"
#include "core/arena.h"
#include "core/binary_cast.hpp"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "core/types.h"
#include "core/value/vdatetime_value.h"
#include "storage/olap_scan_common.h"
#include "storage/predicate/shared_predicate.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
class ColumnPredicate;

class RuntimePredicate {
public:
    RuntimePredicate(const TTopnFilterDesc& desc,
                     const std::unordered_set<int32_t>& strict_target_node_ids);

    Status init_target(int32_t target_node_id,
                       phmap::flat_hash_map<int, SlotDescriptor*> slot_id_to_slot_desc,
                       const int column_id);

    bool enable() const {
        // when sort node and scan node are not in the same fragment, predicate will be disabled
        std::shared_lock<std::shared_mutex> rlock(_rwlock);
        return _detected_source && _detected_target;
    }

    void set_detected_source() {
        std::unique_lock<std::shared_mutex> wlock(_rwlock);
        // Source detection does not reset bounds. Recursive rebuilds call reset_bounds().
        _detected_source = true;
    }

    // Recursive fragment rebuild calls this after the old tasks finish, before new tasks run.
    void reset_bounds();

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

    bool is_strict(int32_t target_node_id) const {
        check_target_node_id(target_node_id);
        return _contexts.find(target_node_id)->second.is_strict;
    }

    const char* comparison_function(int32_t target_node_id) const {
        if (_is_asc) {
            return is_strict(target_node_id) ? "lt" : "le";
        }
        return is_strict(target_node_id) ? "gt" : "ge";
    }

    TExprOpcode::type comparison_opcode(int32_t target_node_id) const {
        if (_is_asc) {
            return is_strict(target_node_id) ? TExprOpcode::LT : TExprOpcode::LE;
        }
        return is_strict(target_node_id) ? TExprOpcode::GT : TExprOpcode::GE;
    }

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
        // Fixed before QueryContext is published. Targets share a bound, not its inclusiveness.
        bool is_strict = false;
        std::string col_name;
        DataTypePtr col_data_type;
        std::shared_ptr<ColumnPredicate> predicate;
        std::function<std::shared_ptr<ColumnPredicate>(const int, const std::string&,
                                                       const DataTypePtr&, const Field&, bool)>
                pred_constructor;

        bool target_is_slot() const {
            return expr.nodes[0].node_type == TExprNodeType::SLOT_REF &&
                   expr.nodes[0].slot_ref.is_virtual_slot == false;
        }
    };

    bool _init(PrimitiveType type);

    mutable std::shared_mutex _rwlock;

    bool _nulls_first;
    bool _is_asc;
    std::map<int32_t, TargetContext> _contexts;

    Field _orderby_extrem {PrimitiveType::TYPE_NULL};
    bool _detected_source = false;
    bool _detected_target = false;
    bool _has_value = false;
    PrimitiveType _type;
};

} // namespace doris
