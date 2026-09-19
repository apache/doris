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

#include <cctz/time_zone.h>

#include <array>
#include <cstdint>
#include <memory>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "core/string_ref.h"

// This file will convert Doris Block to/from Arrow's RecordBatch
// Block is used by Doris query engine to exchange data between
// each execute node.

namespace arrow {

class MemoryPool;
class RecordBatch;
class Schema;

} // namespace arrow

namespace doris {

// ORC and Arrow Iceberg writers share this parser so textual and binary UUID inputs always use
// the same canonical 16-byte representation.
Status parse_iceberg_uuid_to_bytes(StringRef uuid, std::array<uint8_t, 16>* bytes);

// One converter owns both batch orchestration and its format-specific column bindings.
// Per-call state stays local so converters can be reused without retaining input blocks.
class ArrowBlockConvertor {
public:
    virtual ~ArrowBlockConvertor() = default;

    Status convert_to_arrow(const Block& block, const std::shared_ptr<arrow::Schema>& schema,
                            arrow::MemoryPool* pool, std::shared_ptr<arrow::RecordBatch>* result,
                            const cctz::time_zone& timezone_obj, size_t start_row = 0,
                            size_t end_row = 0) const;

    virtual Status convert_from_arrow(const std::shared_ptr<arrow::RecordBatch>& batch,
                                      const DataTypes& types, Block* block,
                                      const cctz::time_zone& timezone_obj) const;

protected:
    virtual Status write_column(const std::shared_ptr<const IDataType>& type,
                                const DataTypeSerDe& serde, const IColumn& column,
                                const NullMap* null_map, const std::shared_ptr<arrow::Field>& field,
                                arrow::ArrayBuilder* array_builder, int64_t start, int64_t end,
                                const cctz::time_zone& ctz) const = 0;

    Status write_plain_arrow_column(const std::shared_ptr<const IDataType>& type,
                                    const DataTypeSerDe& serde, const IColumn& column,
                                    const NullMap* null_map,
                                    const std::shared_ptr<arrow::Field>& field,
                                    arrow::ArrayBuilder* array_builder, int64_t start, int64_t end,
                                    const cctz::time_zone& ctz) const;
};

class ArrowFlightArrowBlockConvertor final : public ArrowBlockConvertor {
public:
    Status convert_from_arrow(const std::shared_ptr<arrow::RecordBatch>& batch,
                              const DataTypes& types, Block* block,
                              const cctz::time_zone& timezone_obj) const override;

protected:
    Status write_column(const std::shared_ptr<const IDataType>& type, const DataTypeSerDe& serde,
                        const IColumn& column, const NullMap* null_map,
                        const std::shared_ptr<arrow::Field>& field,
                        arrow::ArrayBuilder* array_builder, int64_t start, int64_t end,
                        const cctz::time_zone& ctz) const override;
};

const ArrowFlightArrowBlockConvertor& arrow_flight_block_convertor();

Status convert_to_arrow_batch(const Block& block, const std::shared_ptr<arrow::Schema>& schema,
                              arrow::MemoryPool* pool, std::shared_ptr<arrow::RecordBatch>* result,
                              const cctz::time_zone& timezone_obj);

Status convert_to_arrow_batch(const Block& block, const std::shared_ptr<arrow::Schema>& schema,
                              arrow::MemoryPool* pool, std::shared_ptr<arrow::RecordBatch>* result,
                              const cctz::time_zone& timezone_obj, size_t start_row,
                              size_t end_row);

Status convert_to_arrow_batch(const Block& block, const std::shared_ptr<arrow::Schema>& schema,
                              arrow::MemoryPool* pool, std::shared_ptr<arrow::RecordBatch>* result,
                              const cctz::time_zone& timezone_obj, size_t start_row, size_t end_row,
                              const ArrowBlockConvertor& convertor);

Status make_zero_column_arrow_batch(const std::shared_ptr<arrow::Schema>& schema, int64_t rows,
                                    std::shared_ptr<arrow::RecordBatch>* result);

Status convert_from_arrow_batch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                const DataTypes& types, Block* block,
                                const cctz::time_zone& timezone_obj);

} // namespace doris
