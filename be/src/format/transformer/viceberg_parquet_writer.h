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

#include <cstdint>
#include <optional>
#include <vector>

#include "format/table/iceberg/iceberg_arrow_block_convertor.h"
#include "format/table/iceberg/nan_value_counter.h"
#include "format/table/iceberg/schema.h"
#include "format/transformer/vparquet_writer.h"

namespace doris {

class VIcebergParquetWriter final : public VParquetWriter {
public:
    VIcebergParquetWriter(RuntimeState* state, io::FileWriter* file_writer,
                          const VExprContextSPtrs& output_vexpr_ctxs,
                          std::vector<std::string> column_names, bool output_object_data,
                          const ParquetFileOptions& parquet_options,
                          const std::string* iceberg_schema_json,
                          const iceberg::Schema& iceberg_schema,
                          const std::vector<int32_t>& nan_count_field_ids = {});

    Status open() override;

    Status write(const Block& block) override;

    Status collect_file_statistics_after_close(TIcebergColumnStats* stats);

protected:
    std::unique_ptr<ArrowBlockConvertor> _create_arrow_block_convertor(
            DataTypes types, std::vector<std::string> names, const std::string& timezone_name,
            const cctz::time_zone& timezone) const override;

private:
    const iceberg::Schema& _iceberg_schema;
    std::string _iceberg_schema_json;
    const std::vector<int32_t> _nan_count_field_ids;
    // Built at open(), once the writer is past schema validation. See iceberg::NanValueCounter for why a
    // reported zero is a claim and which fields are counted at all.
    std::optional<iceberg::NanValueCounter> _nan_value_counter;
};

} // namespace doris
