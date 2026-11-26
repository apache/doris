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

#include "format/arrow/arrow_block_convertor.h"

namespace doris::iceberg {

class Schema;

class IcebergArrowBlockConvertor final : public ArrowBlockConvertor {
public:
    using ArrowBlockConvertor::ArrowBlockConvertor;
    IcebergArrowBlockConvertor(const Schema& schema, const std::string* schema_json,
                               std::string timezone_name, const cctz::time_zone& timezone)
            : ArrowBlockConvertor(nullptr, timezone),
              _schema(&schema),
              _schema_json(schema_json == nullptr ? "" : *schema_json),
              _timezone_name(std::move(timezone_name)) {}

    Status init() override;

protected:
    Status write_column(const std::shared_ptr<const IDataType>& type, const DataTypeSerDe& serde,
                        const IColumn& column, const NullMap* null_map,
                        const std::shared_ptr<arrow::Field>& field,
                        arrow::ArrayBuilder* array_builder, int64_t start, int64_t end,
                        const cctz::time_zone& ctz) const override;

private:
    const Schema* _schema = nullptr;
    std::string _schema_json;
    std::string _timezone_name;
};

} // namespace doris::iceberg
