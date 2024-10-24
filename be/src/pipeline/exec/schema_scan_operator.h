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

#include <stdint.h>

#include "common/status.h"
#include "exec/schema_scanner.h"
#include "operator.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

class SchemaScanOperatorX;
class SchemaScanLocalState final : public PipelineXLocalState<> {
public:
    ENABLE_FACTORY_CREATOR(SchemaScanLocalState);

    SchemaScanLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<>(state, parent) {
        _finish_dependency =
                std::make_shared<Dependency>(parent->operator_id(), parent->node_id(),
                                             parent->get_name() + "_FINISH_DEPENDENCY", true);
        _data_dependency = std::make_shared<Dependency>(parent->operator_id(), parent->node_id(),
                                                        parent->get_name() + "_DEPENDENCY", true);
    }
    ~SchemaScanLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;

    Status open(RuntimeState* state) override;

    Dependency* finishdependency() override { return _finish_dependency.get(); }
    std::vector<Dependency*> dependencies() const override { return {_data_dependency.get()}; }

private:
    friend class SchemaScanOperatorX;

    SchemaScannerParam _scanner_param;
    std::unique_ptr<SchemaScanner> _schema_scanner;

    std::shared_ptr<Dependency> _finish_dependency;
    std::shared_ptr<Dependency> _data_dependency;
};

class SchemaScanOperatorX final : public OperatorX<SchemaScanLocalState> {
public:
    using Base = OperatorX<SchemaScanLocalState>;
    SchemaScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                        const DescriptorTbl& descs);
    ~SchemaScanOperatorX() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    [[nodiscard]] bool is_source() const override { return true; }

private:
    friend class SchemaScanLocalState;

    const std::string _table_name;

    std::shared_ptr<SchemaScannerCommonParam> _common_scanner_param;
    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;

    // Descriptor of dest tuples
    const TupleDescriptor* _dest_tuple_desc = nullptr;
    // Tuple index in tuple row.
    int _tuple_idx;
    // slot num need to fill in and return
    int _slot_num;

    std::unique_ptr<SchemaScanner> _schema_scanner;
};

} // namespace doris::pipeline