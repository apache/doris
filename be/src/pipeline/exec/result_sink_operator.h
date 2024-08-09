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

#include <gen_cpp/PlanNodes_types.h>
#include <stdint.h>

#include "operator.h"
#include "runtime/buffer_control_block.h"
#include "runtime/result_writer.h"

namespace doris {
class BufferControlBlock;

namespace pipeline {

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
    TFileCompressType::type orc_compression_type;

    bool delete_existing_files = false;
    std::string file_suffix;
    //Bring BOM when exporting to CSV format
    bool with_bom = false;

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
        with_bom = t_opt.with_bom;

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
        if (t_opt.__isset.orc_compression_type) {
            orc_compression_type = t_opt.orc_compression_type;
        }
    }
};

constexpr int RESULT_SINK_BUFFER_SIZE = 4096 * 8;

class ResultSinkLocalState final : public PipelineXSinkLocalState<BasicSharedState> {
    ENABLE_FACTORY_CREATOR(ResultSinkLocalState);
    using Base = PipelineXSinkLocalState<BasicSharedState>;

public:
    ResultSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;
    RuntimeProfile::Counter* blocks_sent_counter() { return _blocks_sent_counter; }
    RuntimeProfile::Counter* rows_sent_counter() { return _rows_sent_counter; }

private:
    friend class ResultSinkOperatorX;

    vectorized::VExprContextSPtrs _output_vexpr_ctxs;

    std::shared_ptr<BufferControlBlock> _sender = nullptr;
    std::shared_ptr<ResultWriter> _writer = nullptr;
    RuntimeProfile::Counter* _blocks_sent_counter = nullptr;
    RuntimeProfile::Counter* _rows_sent_counter = nullptr;
};

class ResultSinkOperatorX final : public DataSinkOperatorX<ResultSinkLocalState> {
public:
    ResultSinkOperatorX(int operator_id, const RowDescriptor& row_desc,
                        const std::vector<TExpr>& select_exprs, const TResultSink& sink);
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

private:
    friend class ResultSinkLocalState;

    Status _second_phase_fetch_data(RuntimeState* state, vectorized::Block* final_block);
    TResultSinkType::type _sink_type;
    int _result_sink_buffer_size_rows;
    // set file options when sink type is FILE
    std::unique_ptr<ResultFileOptions> _file_opts = nullptr;

    // Owned by the RuntimeState.
    const RowDescriptor& _row_desc;

    // Owned by the RuntimeState.
    const std::vector<TExpr>& _t_output_expr;
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;

    // for fetch data by rowids
    TFetchOption _fetch_option;

    std::shared_ptr<BufferControlBlock> _sender = nullptr;
};

} // namespace pipeline
} // namespace doris
