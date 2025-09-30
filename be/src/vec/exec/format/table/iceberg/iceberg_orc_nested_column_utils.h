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
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "vec/exec/format/table/table_format_reader.h"

namespace orc {
class Type;
} // namespace orc

namespace doris {
namespace vectorized {

class IcebergOrcNestedColumnUtils {
public:
    struct SchemaAndColumnResult {
        std::shared_ptr<TableSchemaChangeHelper::Node> schema_node;
        std::set<uint64_t> column_ids;

        SchemaAndColumnResult(std::shared_ptr<TableSchemaChangeHelper::Node> node,
                              std::set<uint64_t> ids)
                : schema_node(std::move(node)), column_ids(std::move(ids)) {}
    };

    static SchemaAndColumnResult _extract_schema_and_columns_efficiently(
            const orc::Type* orc_type,
            const std::unordered_map<int, std::vector<std::vector<int>>>& paths_by_field_id,
            const std::unordered_map<int, std::string>& field_id_to_table_name);

    static void _build_iceberg_id_mapping(const orc::Type* orc_type,
                                          std::map<int, const orc::Type*>& iceberg_id_to_orc_type);

    static void _build_iceberg_id_mapping_recursive(
            const orc::Type* orc_type, std::map<int, const orc::Type*>& iceberg_id_to_orc_type);

    static std::shared_ptr<TableSchemaChangeHelper::Node> _build_table_schema_node_from_type(
            const orc::Type& type, const std::vector<std::vector<int>>& field_paths);

    static std::shared_ptr<TableSchemaChangeHelper::Node> _build_full_table_schema_node(
            const orc::Type& type);

    static void _extract_nested_column_ids_efficiently(const orc::Type& type,
                                                       const std::vector<std::vector<int>>& paths,
                                                       std::set<uint64_t>& column_ids);

private:
    static constexpr const char* ICEBERG_ORC_ATTRIBUTE = "iceberg.id";
};

} // namespace vectorized
} // namespace doris