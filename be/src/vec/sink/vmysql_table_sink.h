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
#include <vector>

#include "common/status.h"
#include "vec/sink/vmysql_table_writer.h"
#include "vec/sink/vtable_sink.h"

namespace doris {
namespace vectorized {

// This class is a sinker, which put input data to mysql table
class VMysqlTableSink : public VTableSink {
public:
    VMysqlTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                    const std::vector<TExpr>& t_exprs);

    Status init(const TDataSink& thrift_sink) override;

    Status open(RuntimeState* state) override;

    Status send(RuntimeState* state, vectorized::Block* block) override;

    Status close(RuntimeState* state, Status exec_status) override;

private:
    MysqlConnInfo _conn_info;
    std::unique_ptr<VMysqlTableWriter> _writer;
};
} // namespace vectorized
} // namespace doris
