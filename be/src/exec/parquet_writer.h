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

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <stdint.h>

#include <map>
#include <string>

#include "common/status.h"
#include "exprs/expr_context.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/row_batch.h"
#include "runtime/tuple.h"

namespace doris {
class FileWriter;

class ParquetOutputStream : public arrow::io::OutputStream {
public:
    ParquetOutputStream(FileWriter* file_writer);
    ParquetOutputStream(FileWriter* file_writer, const int64_t& written_len);
    virtual ~ParquetOutputStream();

    arrow::Status Write(const void* data, int64_t nbytes) override;
    // return the current write position of the stream
    arrow::Result<int64_t> Tell() const override;
    arrow::Status Close() override;

    bool closed() const override { return _is_closed; }

    int64_t get_written_len();

    void set_written_len(int64_t written_len);

private:
    FileWriter* _file_writer; // not owned
    int64_t _cur_pos = 0;     // current write position
    bool _is_closed = false;
    int64_t _written_len = 0;
};

// a wrapper of parquet output stream
class ParquetWriterWrapper {
public:
    ParquetWriterWrapper(FileWriter* file_writer, const std::vector<ExprContext*>& output_expr_ctxs,
                         const std::map<std::string, std::string>& properties,
                         const std::vector<std::vector<std::string>>& schema,
                         bool output_object_data);
    virtual ~ParquetWriterWrapper();

    Status write(const RowBatch& row_batch);

    Status init_parquet_writer();

    Status _write_one_row(TupleRow* row);

    void close();

    void parse_properties(const std::map<std::string, std::string>& propertie_map);

    Status parse_schema(const std::vector<std::vector<std::string>>& schema);

    parquet::RowGroupWriter* get_rg_writer();

    int64_t written_len();

private:
    std::shared_ptr<ParquetOutputStream> _outstream;
    std::shared_ptr<parquet::WriterProperties> _properties;
    std::shared_ptr<parquet::schema::GroupNode> _schema;
    std::unique_ptr<parquet::ParquetFileWriter> _writer;
    const std::vector<ExprContext*>& _output_expr_ctxs;
    std::vector<std::vector<std::string>> _str_schema;
    int64_t _cur_writed_rows = 0;
    parquet::RowGroupWriter* _rg_writer;
    const int64_t _max_row_per_group = 10;
    bool _output_object_data;
};

} // namespace doris
