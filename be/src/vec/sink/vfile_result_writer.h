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
#include "runtime/file_result_writer.h"
#include "runtime/primitive_type.h"
#include "util/mysql_row_buffer.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/sink/result_writer.h"
namespace doris {
class BufferControlBlock;
class RowBatch;
class TFetchDataResult;
class FileResultWriter;
namespace vectorized {

class VExprContext;

class VFileResultWriter final : public FileResultWriter, public VResultWriter {
public:
    VFileResultWriter(const ResultFileOptions* file_option,
                      const std::vector<VExprContext*>* output_vexpr_ctxs,
                      RuntimeProfile* parent_profile, BufferControlBlock* sinker,
                      bool output_object_data);

    VFileResultWriter(const ResultFileOptions* file_option,
                      const TStorageBackendType::type storage_type,
                      const TUniqueId fragment_instance_id,
                      const std::vector<VExprContext*>* output_vexpr_ctxs,
                      RuntimeProfile* parent_profile, BufferControlBlock* sinker,
                      MutableBlock* output_batch, bool output_object_data);

    virtual ~VFileResultWriter();

    virtual Status init(RuntimeState* state) override;

    virtual Status append_row_batch(const RowBatch* batch) override;

    virtual Status append_block(Block& block) override;

    virtual Status close() override;

    // if buffer exceed the limit, write the data buffered in _plain_text_outstream via file_writer
    // if eos, write the data even if buffer is not full.
    Status _flush_plain_text_outstream(bool eos) override;
    Status _create_file_writer(const std::string& file_name) override;
    // save result into batch rather than send it
    Status _fill_result_batch() override;

    // close file writer, and if !done, it will create new writer for next file.
    // if only_close is true, this method will just close the file writer and return.
    Status _close_file_writer(bool done, bool only_close = false) override;

    Status _write_csv_file(Block& block);
    Status _write_parquet_file(Block& block);
    Status _write_one_row_as_csv(Block& block, size_t row);
private:
    const std::vector<VExprContext*>* _output_vexpr_ctxs;
    fmt::memory_buffer _insert_stmt_buffer;
    MutableBlock* _mutable_block;
    Block* _output_block = nullptr;
    MutableColumns _result_columns;
    std::vector<int> _column_ids;
};
} // namespace vectorized
} // namespace doris