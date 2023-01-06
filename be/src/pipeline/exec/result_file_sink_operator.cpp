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

#include "result_file_sink_operator.h"

#include "vec/sink/vresult_file_sink.h"

namespace doris::pipeline {

ResultFileSinkOperatorBuilder::ResultFileSinkOperatorBuilder(int32_t id, DataSink* sink)
        : DataSinkOperatorBuilder(id, "ResultSinkOperator", sink) {};

OperatorPtr ResultFileSinkOperatorBuilder::build_operator() {
    return std::make_shared<ResultFileSinkOperator>(this, _sink);
}

ResultFileSinkOperator::ResultFileSinkOperator(OperatorBuilderBase* operator_builder,
                                               DataSink* sink)
        : DataSinkOperator(operator_builder, sink) {};
} // namespace doris::pipeline