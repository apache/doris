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
#include <map>
#include <utility>
#include <vector>

#include "format/table/iceberg/iceberg_arrow_block_convertor.h"
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
                          const iceberg::Schema& iceberg_schema, bool collect_column_stats = true);

    Status open() override;

    Status write(const Block& block) override;

    Status collect_file_statistics_after_close(TIcebergColumnStats* stats);

protected:
    std::unique_ptr<ArrowBlockConvertor> _create_arrow_block_convertor(
            DataTypes types, std::vector<std::string> names, const std::string& timezone_name,
            const cctz::time_zone& timezone) const override;

private:
    void _init_nan_value_counts();
    void _count_nan_values(const Block& block);

    const iceberg::Schema& _iceberg_schema;
    std::string _iceberg_schema_json;
    const bool _collect_column_stats;

    // Parquet column statistics carry no NaN count, so unlike every other metric reported by
    // collect_file_statistics_after_close this one cannot be read back from the footer -- it is
    // accumulated here while the rows go past. See _init_nan_value_counts for why only some fields
    // are listed, and why a reported zero is a claim and not a default.
    // (block column position, iceberg field id) of the columns counted for this file.
    std::vector<std::pair<size_t, int32_t>> _nan_counted_columns;
    std::map<int, int64_t> _nan_value_counts;
};

} // namespace doris
