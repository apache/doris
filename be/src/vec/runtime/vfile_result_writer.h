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

#include "io/file_writer.h"
#include "runtime/file_result_writer.h"
#include "vec/runtime/vparquet_writer.h"
#include "vec/sink/vresult_sink.h"

namespace doris::vectorized {
class VFileWriterWrapper;

// write result to file
class VFileResultWriter final : public VResultWriter {
public:
    VFileResultWriter(const ResultFileOptions* file_option,
                      const TStorageBackendType::type storage_type,
                      const TUniqueId fragment_instance_id,
                      const std::vector<VExprContext*>& _output_vexpr_ctxs,
                      RuntimeProfile* parent_profile, BufferControlBlock* sinker,
                      Block* output_block, bool output_object_data,
                      const RowDescriptor& output_row_descriptor);
    virtual ~VFileResultWriter() = default;

    Status append_block(Block& block) override;
    Status append_row_batch(const RowBatch* batch) override {
        return Status::NotSupported("append_row_batch is not supported in VFileResultWriter!");
    };

    Status init(RuntimeState* state) override;
    Status close() override;

    // file result writer always return statistic result in one row
    int64_t get_written_rows() const override { return 1; }

    std::string gen_types();
    Status write_csv_header();

private:
    Status _write_file(const Block& block);
    Status _write_csv_file(const Block& block);

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
    Status _fill_result_block();

    RuntimeState* _state; // not owned, set when init
    const ResultFileOptions* _file_opts;
    TStorageBackendType::type _storage_type;
    TUniqueId _fragment_instance_id;
    const std::vector<VExprContext*>& _output_vexpr_ctxs;

    // If the result file format is plain text, like CSV, this _file_writer is owned by this FileResultWriter.
    // If the result file format is Parquet, this _file_writer is owned by _parquet_writer.
    std::unique_ptr<doris::FileWriter> _file_writer_impl;
    // Used to buffer the export data of plain text
    // TODO(cmy): I simply use a stringstrteam to buffer the data, to avoid calling
    // file writer's write() for every single row.
    // But this cannot solve the problem of a row of data that is too large.
    // For example: bitmap_to_string() may return large volume of data.
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
    Block* _output_block = nullptr;
    // set to true if the final statistic result is sent
    bool _is_result_sent = false;
    bool _header_sent = false;
    RowDescriptor _output_row_descriptor;
    // parquet/orc file writer
    std::unique_ptr<VFileWriterWrapper> _vfile_writer;
};
} // namespace doris::vectorized
