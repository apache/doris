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
#include "operator.h"

namespace doris {

namespace pipeline {

// used for VMysqlTableSink, VJdbcTableSink and VOdbcTableSink.
class TableSinkOperatorBuilder final : public DataSinkOperatorBuilder<DataSink> {
public:
    TableSinkOperatorBuilder(int32_t id, DataSink* sink)
            : DataSinkOperatorBuilder(id, "TableSinkOperator", sink) {}

    OperatorPtr build_operator() override;
};

class TableSinkOperator final : public DataSinkOperator<DataSink> {
public:
    TableSinkOperator(OperatorBuilderBase* operator_builder, DataSink* sink)
            : DataSinkOperator(operator_builder, sink) {}

    bool can_write() override { return _sink->can_write(); }
};

OperatorPtr TableSinkOperatorBuilder::build_operator() {
    return std::make_shared<TableSinkOperator>(this, _sink);
}

} // namespace pipeline
} // namespace doris
