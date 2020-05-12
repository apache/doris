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

#include "runtime/result_writer.h"
#include "gen_cpp/DataSinks_types.h"

namespace doris {

class ExprContext;
class FileWriter;
class ParquetWriterWrapper;
class RowBatch;
class RuntimeState;
class TupleRow;

struct ResultFileOptions {
    bool is_local_file;
    std::string file_path;
    TFileFormatType::type file_format;
    std::string column_separator;
    std::string line_delimiter;
    std::vector<TNetworkAddress> broker_addresses;
    std::map<std::string, std::string> broker_properties;

    ResultFileOptions(const TResultFileSinkOptions& t_opt, bool is_local) {
        file_path = t_opt.file_path;
        file_format = t_opt.file_format;
        column_separator = t_opt.__isset.column_separator ? t_opt.column_separator : "\t";
        line_delimiter = t_opt.__isset.line_delimiter ? t_opt.line_delimiter : "\n";

        if (t_opt.__isset.broker_addresses) {
            broker_addresses = t_opt.broker_addresses;
        }
        if (t_opt.__isset.broker_properties) {
            broker_properties = t_opt.broker_properties;
        }

        is_local_file = is_local;
    }
};

// write result to file
class FileResultWriter : public ResultWriter {
public:
    FileResultWriter(const ResultFileOptions* file_option, const std::vector<ExprContext*>& output_expr_ctxs);
    virtual ~FileResultWriter();

    virtual Status init(RuntimeState* state) override;
    // convert one row batch to mysql result and
    // append this batch to the result sink
    virtual Status append_row_batch(RowBatch* batch) override;

    virtual Status close() override;

private:
    const ResultFileOptions* _file_opts;

    Status _write_csv_file(const RowBatch& batch);
    Status _write_one_row_as_csv(TupleRow* row);

    // If the result file format is CSV, this writer is owned by this FileResultWriter.
    // If the result file format is Parquet, this writer is owned by ParquetWriterWrapper.
    FileWriter* _file_writer = nullptr;
    // parquet file writer
    ParquetWriterWrapper* _parquet_writer = nullptr;
    const std::vector<ExprContext*>& _output_expr_ctxs;
};

} // end of namespace

