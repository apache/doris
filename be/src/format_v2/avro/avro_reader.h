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

#include "format_v2/file_reader.h"

namespace doris::format::avro {

// Physical Avro OCF decoding. Field identities and absolute row positions are exposed to the
// ordinary mapper; schema evolution, defaults and delete semantics remain in the table layer.
class AvroReader final : public FileReader {
public:
    AvroReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
               std::unique_ptr<io::FileDescription>& file_description,
               std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
               bool enable_mapping_timestamp_tz = false, bool enable_mapping_varbinary = false,
               std::optional<GlobalRowIdContext> global_rowid_context = std::nullopt,
               bool hive_compatible = false);
    ~AvroReader() override;

    Status init(RuntimeState* state) override;
    Status get_schema(std::vector<ColumnDefinition>* file_schema) const override;
    std::unique_ptr<TableColumnMapper> create_column_mapper(
            TableColumnMapperOptions options) const override;
    Status validate_read_schema(const std::string& schema, bool match_record_names,
                                const std::string& projected_schema = {}) const override;
    bool supports_rowid_fetch() const override { return true; }
    Status open(std::shared_ptr<FileScanRequest> request) override;
    void set_batch_size(size_t batch_size) override;
    Status get_block(Block* file_block, size_t* rows, bool* eof) override;
    Status close() override;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace doris::format::avro
