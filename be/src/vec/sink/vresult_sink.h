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
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <stddef.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/data_sink.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
class RuntimeState;
class RuntimeProfile;
class BufferControlBlock;
class QueryStatistics;
class ResultWriter;
class RowDescriptor;
class TExpr;

namespace pipeline {
class ResultSinkOperator;
}
namespace vectorized {
class Block;

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

    bool delete_existing_files = false;
    std::string file_suffix;

    ResultFileOptions(const TResultFileSinkOptions& t_opt) {
        file_path = t_opt.file_path;
        file_format = t_opt.file_format;
        column_separator = t_opt.__isset.column_separator ? t_opt.column_separator : "\t";
        line_delimiter = t_opt.__isset.line_delimiter ? t_opt.line_delimiter : "\n";
        max_file_size_bytes =
                t_opt.__isset.max_file_size_bytes ? t_opt.max_file_size_bytes : max_file_size_bytes;
        delete_existing_files =
                t_opt.__isset.delete_existing_files ? t_opt.delete_existing_files : false;
        file_suffix = t_opt.file_suffix;

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

constexpr int RESULT_SINK_BUFFER_SIZE = 4096;

class VResultSink : public DataSink {
public:
    friend class pipeline::ResultSinkOperator;
    VResultSink(const RowDescriptor& row_desc, const std::vector<TExpr>& select_exprs,
                const TResultSink& sink, int buffer_size);

    ~VResultSink() override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status send(RuntimeState* state, Block* block, bool eos = false) override;
    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    Status close(RuntimeState* state, Status exec_status) override;

    void set_query_statistics(std::shared_ptr<QueryStatistics> statistics) override;

private:
    Status prepare_exprs(RuntimeState* state);
    Status second_phase_fetch_data(RuntimeState* state, Block* final_block);
    TResultSinkType::type _sink_type;
    // set file options when sink type is FILE
    std::unique_ptr<ResultFileOptions> _file_opts;

    // Owned by the RuntimeState.
    const std::vector<TExpr>& _t_output_expr;
    VExprContextSPtrs _output_vexpr_ctxs;

    std::shared_ptr<BufferControlBlock> _sender;
    std::shared_ptr<ResultWriter> _writer;
    int _buf_size; // Allocated from _pool

    // for fetch data by rowids
    TFetchOption _fetch_option;
};
} // namespace vectorized

} // namespace doris
