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

#include <shared_mutex>

#include "vec/exec/format/table/iceberg/schema.h"

namespace doris {
namespace iceberg {

class ArrowSchemaUtil {
public:
    static Status convert(const Schema* schema, const std::string& timezone,
                          std::vector<std::shared_ptr<arrow::Field>>& fields);

private:
    static const char* PARQUET_FIELD_ID;
    static const char* ORIGINAL_TYPE;
    static const char* MAP_TYPE_VALUE;

    static Status convert_to(const iceberg::NestedField& field,
                             std::shared_ptr<arrow::Field>* arrow_field,
                             const std::string& timezone);
};

} // namespace iceberg
} // namespace doris
