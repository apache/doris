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

#ifndef DORIS_BE_SRC_RUNTIME_EXPORT_SINK_H
#define DORIS_BE_SRC_RUNTIME_EXPORT_SINK_H

#include <vector>

#include "common/status.h"
#include "exec/data_sink.h"
#include "util/runtime_profile.h"

namespace doris {

class RowDescriptor;
class TExpr;
class RuntimeState;
class RuntimeProfile;
class ExprContext;
class FileWriter;
class TupleRow;

// This class is a sinker, which put export data to external storage by broker.
class ExportSink : public DataSink {
public:
    ExportSink(ObjectPool* pool, const RowDescriptor& row_desc, const std::vector<TExpr>& t_exprs);

    virtual ~ExportSink();

    virtual Status init(const TDataSink& thrift_sink) override;

    virtual Status prepare(RuntimeState* state) override;

    virtual Status open(RuntimeState* state) override;

    virtual Status send(RuntimeState* state, RowBatch* batch) override;

    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    virtual Status close(RuntimeState* state, Status exec_status) override;

    virtual RuntimeProfile* profile() override { return _profile; }

private:
    Status open_file_writer();
    Status gen_row_buffer(TupleRow* row, std::stringstream* ss);
    std::string gen_file_name();
    Status write_csv_header();

    RuntimeState* _state;

    // owned by RuntimeState
    ObjectPool* _pool;
    const RowDescriptor& _row_desc;
    const std::vector<TExpr>& _t_output_expr;

    std::vector<ExprContext*> _output_expr_ctxs;

    TExportSink _t_export_sink;
    std::unique_ptr<FileWriter> _file_writer;

    RuntimeProfile* _profile;

    RuntimeProfile::Counter* _bytes_written_counter;
    RuntimeProfile::Counter* _rows_written_counter;
    RuntimeProfile::Counter* _write_timer;
    bool _header_sent;
};

} // end namespace doris

#endif // DORIS_BE_SRC_RUNTIME_EXPORT_SINK_H
