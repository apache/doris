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

#include <arrow/result.h>

#include <memory>

namespace arrow {
class Schema;
}
namespace parquet {
class WriterProperties;
class ArrowWriterProperties;
namespace schema {
class GroupNode;
}
} // namespace parquet
namespace doris {
namespace iceberg {
class Type;
class Schema;
} // namespace iceberg
bool contains_spatial_type(iceberg::Type* type);
// Check schema-only CRS restrictions before an output file or stream can be created.
arrow::Status validate_iceberg_spatial_crs_for_write(const iceberg::Type& type);
// Copy the physical schema, replacing only spatial leaf annotations.
arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> make_iceberg_parquet_schema(
        const std::shared_ptr<arrow::Schema>& arrow_schema, const iceberg::Schema& iceberg_schema,
        const std::shared_ptr<::parquet::WriterProperties>& writer_properties,
        const std::shared_ptr<::parquet::ArrowWriterProperties>& arrow_properties);
} // namespace doris
