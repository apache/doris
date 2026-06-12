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

#include "core/data_type/data_type.h"
#include "exprs/vslot_ref.h"

namespace doris {

class TableSlotRef : public VSlotRef {
    ENABLE_FACTORY_CREATOR(TableSlotRef);

public:
    TableSlotRef(int slot_id, int column_id, int column_uniq_id, const DataTypePtr& type,
                 const std::string& column_name)
            : VSlotRef(slot_id, column_id, column_uniq_id), _cname(column_name) {
        _data_type = type;
    }

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override {
        if (_prepared) {
            return Status::OK();
        }
        _prepared = true;
        _prepare_finished = true;
        return Status::OK();
    }

    const std::string& expr_name() const override { return _cname; }
    const std::string& column_name() const override { return _cname; }
    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = TableSlotRef::create_shared(slot_id(), column_id(), column_uniq_id(),
                                                   data_type(), column_name());
        return Status::OK();
    }

private:
    const std::string _cname;
};

} // namespace doris
