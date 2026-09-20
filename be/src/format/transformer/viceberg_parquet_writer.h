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

#include "format/table/iceberg/iceberg_arrow_block_convertor.h"
#include "format/table/iceberg/schema.h"
#include "format/transformer/vparquet_writer.h"

namespace doris {
#include "common/compile_check_begin.h"

class VIcebergParquetWriter final : public VParquetWriter {
public:
    VIcebergParquetWriter(RuntimeState* state, io::FileWriter* file_writer,
                          const VExprContextSPtrs& output_vexpr_ctxs,
                          std::vector<std::string> column_names, bool output_object_data,
                          const ParquetFileOptions& parquet_options,
                          const std::string* iceberg_schema_json,
                          const iceberg::Schema& iceberg_schema);

    Status collect_file_statistics_after_close(TIcebergColumnStats* stats);

protected:
    Status _parse_schema(std::shared_ptr<arrow::Schema>* schema) override;
    const ArrowBlockConvertor& _get_arrow_block_convertor() const override {
        return _iceberg_arrow_block_convertor;
    }

private:
    iceberg::IcebergArrowBlockConvertor _iceberg_arrow_block_convertor;
};

} // namespace doris
#include "common/compile_check_end.h"
