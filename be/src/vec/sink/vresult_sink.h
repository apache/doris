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
#include "vec/sink/vresult_writer.h"

namespace doris {
class ObjectPool;
class RuntimeState;
class RuntimeProfile;
class BufferControlBlock;
class ExprContext;
class ResultWriter;
class MemTracker;
struct ResultFileOptions;
namespace pipeline {
class ResultSinkOperator;
}
namespace vectorized {
class VExprContext;

struct ResultFileOptions {
    // [[deprecated]]
    bool is_local_file;
    std::string file_path;
    TFileFormatType::type file_format;
    std::string column_separator;
    std::string line_delimiter;
    size_t max_file_size_bytes = 1 * 1024 * 1024 * 1024; // 1GB
    std::vector<TNetworkAddress> broker_addresses;
    std::map<std::string, std::string> broker_properties;
    std::string success_file_name;
    std::vector<std::vector<std::string>> schema;       //not use in outfile with parquet format
    std::map<std::string, std::string> file_properties; //not use in outfile with parquet format

    std::vector<TParquetSchema> parquet_schemas;
    TParquetCompressionType::type parquet_commpression_type;
    TParquetVersion::type parquet_version;
    bool parquert_disable_dictionary;
    //note: use outfile with parquet format, have deprecated 9:schema and 10:file_properties
    //But in order to consider the compatibility when upgrading, so add a bool to check
    //Now the code version is 1.1.2, so when the version is after 1.2, could remove this code.
    bool is_refactor_before_flag = false;
    std::string orc_schema;

    ResultFileOptions(const TResultFileSinkOptions& t_opt) {
        file_path = t_opt.file_path;
        file_format = t_opt.file_format;
        column_separator = t_opt.__isset.column_separator ? t_opt.column_separator : "\t";
        line_delimiter = t_opt.__isset.line_delimiter ? t_opt.line_delimiter : "\n";
        max_file_size_bytes =
                t_opt.__isset.max_file_size_bytes ? t_opt.max_file_size_bytes : max_file_size_bytes;

        is_local_file = true;
        if (t_opt.__isset.broker_addresses) {
            broker_addresses = t_opt.broker_addresses;
            is_local_file = false;
        }
        if (t_opt.__isset.broker_properties) {
            broker_properties = t_opt.broker_properties;
        }
        if (t_opt.__isset.success_file_name) {
            success_file_name = t_opt.success_file_name;
        }
        if (t_opt.__isset.schema) {
            schema = t_opt.schema;
            is_refactor_before_flag = true;
        }
        if (t_opt.__isset.file_properties) {
            file_properties = t_opt.file_properties;
        }
        if (t_opt.__isset.parquet_schemas) {
            is_refactor_before_flag = false;
            parquet_schemas = t_opt.parquet_schemas;
        }
        if (t_opt.__isset.parquet_compression_type) {
            parquet_commpression_type = t_opt.parquet_compression_type;
        }
        if (t_opt.__isset.parquet_disable_dictionary) {
            parquert_disable_dictionary = t_opt.parquet_disable_dictionary;
        }
        if (t_opt.__isset.parquet_version) {
            parquet_version = t_opt.parquet_version;
        }
        if (t_opt.__isset.orc_schema) {
            orc_schema = t_opt.orc_schema;
        }
    }
};

class VResultSink : public DataSink {
public:
    friend class pipeline::ResultSinkOperator;
    VResultSink(const RowDescriptor& row_desc, const std::vector<TExpr>& select_exprs,
                const TResultSink& sink, int buffer_size);

    virtual ~VResultSink();

    virtual Status prepare(RuntimeState* state) override;
    virtual Status open(RuntimeState* state) override;

    virtual Status send(RuntimeState* state, Block* block, bool eos = false) override;
    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    virtual Status close(RuntimeState* state, Status exec_status) override;
    virtual RuntimeProfile* profile() override { return _profile; }

    void set_query_statistics(std::shared_ptr<QueryStatistics> statistics) override;

    const RowDescriptor& row_desc() { return _row_desc; }

private:
    Status prepare_exprs(RuntimeState* state);
    TResultSinkType::type _sink_type;
    // set file options when sink type is FILE
    std::unique_ptr<ResultFileOptions> _file_opts;

    // Owned by the RuntimeState.
    const RowDescriptor& _row_desc;

    // Owned by the RuntimeState.
    const std::vector<TExpr>& _t_output_expr;
    std::vector<vectorized::VExprContext*> _output_vexpr_ctxs;

    std::shared_ptr<BufferControlBlock> _sender;
    std::shared_ptr<VResultWriter> _writer;
    RuntimeProfile* _profile; // Allocated from _pool
    int _buf_size;            // Allocated from _pool
};
} // namespace vectorized

} // namespace doris
