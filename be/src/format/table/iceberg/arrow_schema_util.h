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

#include <arrow/type.h>

#include "format/arrow/arrow_row_batch.h"
#include "format/table/iceberg/schema.h"

namespace doris::iceberg {
#include "common/compile_check_begin.h"

class IcebergArrowSchemaConvertor final : public ArrowSchemaConvertor {
public:
    IcebergArrowSchemaConvertor(const Schema& schema, std::string timezone,
                                std::string schema_json = {})
            : _schema(schema),
              _timezone(std::move(timezone)),
              _schema_json(std::move(schema_json)) {}
    Status get_arrow_schema(std::shared_ptr<arrow::Schema>* result) const override;
    Status convert_fields(std::vector<std::shared_ptr<arrow::Field>>& fields) const;

private:
    static const char* PARQUET_FIELD_ID;
    static const char* ORIGINAL_TYPE;
    static const char* MAP_TYPE_VALUE;
    static const char* UUID_TYPE_VALUE;

    Status convert_to_arrow_field(const iceberg::NestedField& field,
                                  std::shared_ptr<arrow::Field>* arrow_field) const;
    const Schema& _schema;
    const std::string _timezone;
    const std::string _schema_json;
};

#include "common/compile_check_end.h"
} // namespace doris::iceberg
