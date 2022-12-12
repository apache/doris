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

#include "operator.h"
#include "vec/sink/vmysql_table_sink.h"

namespace doris {

namespace pipeline {

class MysqlTableSinkOperatorBuilder final
        : public DataSinkOperatorBuilder<vectorized::VMysqlTableSink> {
public:
    MysqlTableSinkOperatorBuilder(int32_t id, DataSink* sink)
            : DataSinkOperatorBuilder(id, "MysqlTableSinkOperator", sink) {};

    OperatorPtr build_operator() override;
};

class MysqlTableSinkOperator final : public DataSinkOperator<MysqlTableSinkOperatorBuilder> {
public:
    MysqlTableSinkOperator(OperatorBuilderBase* operator_builder, DataSink* sink)
            : DataSinkOperator(operator_builder, sink) {};

    bool can_write() override { return true; }
};

OperatorPtr MysqlTableSinkOperatorBuilder::build_operator() {
    return std::make_shared<MysqlTableSinkOperator>(this, _sink);
}

} // namespace pipeline
} // namespace doris
