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

#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/result_writer.h"
#include "runtime/runtime_state.h"

namespace doris {

class ExprContext;
class FileWriter;
class ParquetWriterWrapper;
class RowBatch;
class RuntimeProfile;
class TupleRow;

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
    std::string success_file_name = "";
    std::vector<std::vector<std::string>> schema;
    std::map<std::string, std::string> file_properties;

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
        }
        if (t_opt.__isset.file_properties) {
            file_properties = t_opt.file_properties;
        }
    }
};

class BufferControlBlock;
// write result to file
class FileResultWriter final : public ResultWriter {
public:
    // [[deprecated]]
    FileResultWriter(const ResultFileOptions* file_option,
                     const std::vector<ExprContext*>& output_expr_ctxs,
                     RuntimeProfile* parent_profile, BufferControlBlock* sinker,
                     bool output_object_data);
    FileResultWriter(const ResultFileOptions* file_option,
                     const TStorageBackendType::type storage_type,
                     const TUniqueId fragment_instance_id,
                     const std::vector<ExprContext*>& output_expr_ctxs,
                     RuntimeProfile* parent_profile, BufferControlBlock* sinker,
                     RowBatch* output_batch, bool output_object_data);
    virtual ~FileResultWriter();

    virtual Status init(RuntimeState* state) override;
    virtual Status append_row_batch(const RowBatch* batch) override;
    virtual Status close() override;

    // file result writer always return statistic result in one row
    virtual int64_t get_written_rows() const override { return 1; }

    std::string gen_types();
    Status write_csv_header();

private:
    Status _write_csv_file(const RowBatch& batch);
    Status _write_parquet_file(const RowBatch& batch);
    Status _write_one_row_as_csv(TupleRow* row);

    // if buffer exceed the limit, write the data buffered in _plain_text_outstream via file_writer
    // if eos, write the data even if buffer is not full.
    Status _flush_plain_text_outstream(bool eos);
    void _init_profile();

    Status _create_file_writer(const std::string& file_name);
    Status _create_next_file_writer();
    Status _create_success_file();
    // get next export file name
    Status _get_next_file_name(std::string* file_name);
    Status _get_success_file_name(std::string* file_name);
    Status _get_file_url(std::string* file_url);
    std::string _file_format_to_name();
    // close file writer, and if !done, it will create new writer for next file.
    // if only_close is true, this method will just close the file writer and return.
    Status _close_file_writer(bool done, bool only_close = false);
    // create a new file if current file size exceed limit
    Status _create_new_file_if_exceed_size();
    // send the final statistic result
    Status _send_result();
    // save result into batch rather than send it
    Status _fill_result_batch();

private:
    RuntimeState* _state; // not owned, set when init
    const ResultFileOptions* _file_opts;
    TStorageBackendType::type _storage_type;
    TUniqueId _fragment_instance_id;
    const std::vector<ExprContext*>& _output_expr_ctxs;

    // If the result file format is plain text, like CSV, this _file_writer is owned by this FileResultWriter.
    // If the result file format is Parquet, this _file_writer is owned by _parquet_writer.
    FileWriter* _file_writer = nullptr;
    // parquet file writer
    ParquetWriterWrapper* _parquet_writer = nullptr;
    // Used to buffer the export data of plain text
    // TODO(cmy): I simply use a stringstrteam to buffer the data, to avoid calling
    // file writer's write() for every single row.
    // But this cannot solve the problem of a row of data that is too large.
    // For example: bitmap_to_string() may return large volumn of data.
    // And the speed is relative low, in my test, is about 6.5MB/s.
    std::stringstream _plain_text_outstream;
    static const size_t OUTSTREAM_BUFFER_SIZE_BYTES;

    // current written bytes, used for split data
    int64_t _current_written_bytes = 0;
    // the suffix idx of export file name, start at 0
    int _file_idx = 0;

    RuntimeProfile* _parent_profile; // profile from result sink, not owned
    // total time cost on append batch operation
    RuntimeProfile::Counter* _append_row_batch_timer = nullptr;
    // tuple convert timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _convert_tuple_timer = nullptr;
    // file write timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _file_write_timer = nullptr;
    // time of closing the file writer
    RuntimeProfile::Counter* _writer_close_timer = nullptr;
    // number of written rows
    RuntimeProfile::Counter* _written_rows_counter = nullptr;
    // bytes of written data
    RuntimeProfile::Counter* _written_data_bytes = nullptr;

    // _sinker and _output_batch are not owned by FileResultWriter
    BufferControlBlock* _sinker = nullptr;
    RowBatch* _output_batch = nullptr;
    // set to true if the final statistic result is sent
    bool _is_result_sent = false;
    bool _header_sent = false;
};

} // namespace doris
