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

#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "core/block/block.h"
#include "exprs/vexpr_fwd.h"

// This file will convert Doris RowBatch to/from Arrow's RecordBatch
// RowBatch is used by Doris query engine to exchange data between
// each execute node.

namespace arrow {

class DataType;
class Field;
class RecordBatch;
class Schema;

} // namespace arrow

namespace doris {

constexpr size_t MAX_ARROW_UTF8 = (1ULL << 31); // 2G

class RowDescriptor;

// Each protocol owns its schema source and conversion rules. Table formats retain their
// authoritative schemas instead of reconstructing field IDs or physical layouts from Doris types.
class ArrowSchemaConvertor {
public:
    virtual ~ArrowSchemaConvertor() = default;
    virtual Status get_arrow_schema(std::shared_ptr<arrow::Schema>* result) const = 0;
};

class DorisArrowSchemaConvertor : public ArrowSchemaConvertor {
public:
    explicit DorisArrowSchemaConvertor(std::string timezone) : _timezone(std::move(timezone)) {}
    DorisArrowSchemaConvertor(const Block& header, std::string timezone)
            : _header(header.clone_empty()), _timezone(std::move(timezone)) {}

    Status get_arrow_schema(std::shared_ptr<arrow::Schema>* result) const override;
    Status get_arrow_schema_from_block(const Block& block,
                                       std::shared_ptr<arrow::Schema>* result) const;
    Status get_arrow_schema_from_expr_ctxs(const VExprContextSPtrs& output_vexpr_ctxs,
                                           std::shared_ptr<arrow::Schema>* result) const;
    virtual Status convert_to_arrow_type(const DataTypePtr& type,
                                         std::shared_ptr<arrow::DataType>* result) const;

protected:
    virtual std::string timestamp_timezone(PrimitiveType type) const;
    virtual std::shared_ptr<arrow::Field> make_field(const std::string& name,
                                                     const std::shared_ptr<arrow::DataType>& type,
                                                     bool nullable, PrimitiveType primitive) const;
    virtual std::shared_ptr<arrow::Field> make_child_field(
            const std::string& name, const std::shared_ptr<arrow::DataType>& type, bool nullable,
            PrimitiveType primitive) const;

private:
    Block _header;
    const std::string _timezone;
};

class ArrowFlightSchemaConvertor : public DorisArrowSchemaConvertor {
public:
    explicit ArrowFlightSchemaConvertor(std::string timezone, bool native_variant = false)
            : DorisArrowSchemaConvertor(std::move(timezone)), _native_variant(native_variant) {}
    ArrowFlightSchemaConvertor(const Block& header, std::string timezone,
                               bool native_variant = false)
            : DorisArrowSchemaConvertor(header, std::move(timezone)),
              _native_variant(native_variant) {}

    Status convert_to_arrow_type(const DataTypePtr& type,
                                 std::shared_ptr<arrow::DataType>* result) const override;

protected:
    std::string timestamp_timezone(PrimitiveType type) const override;

private:
    const bool _native_variant;
};

// Old FEs require the pre-capability metadata layout, including metadata-free nested fields.
class LegacyArrowFlightSchemaConvertor final : public ArrowFlightSchemaConvertor {
public:
    using ArrowFlightSchemaConvertor::ArrowFlightSchemaConvertor;

protected:
    std::shared_ptr<arrow::Field> make_field(const std::string& name,
                                             const std::shared_ptr<arrow::DataType>& type,
                                             bool nullable, PrimitiveType primitive) const override;
    std::shared_ptr<arrow::Field> make_child_field(const std::string& name,
                                                   const std::shared_ptr<arrow::DataType>& type,
                                                   bool nullable,
                                                   PrimitiveType primitive) const override;
};

Status register_arrow_variant_extension();

std::shared_ptr<arrow::Field> create_arrow_field_with_metadata(
        const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
        bool is_nullable, PrimitiveType primitive_type);

Status serialize_record_batch(const arrow::RecordBatch& record_batch, std::string* result);

Status serialize_arrow_schema(std::shared_ptr<arrow::Schema>* schema, std::string* result);

} // namespace doris
