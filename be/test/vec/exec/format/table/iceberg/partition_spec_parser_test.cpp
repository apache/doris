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

#include "vec/exec/format/table/iceberg/partition_spec_parser.h"

#include <gtest/gtest.h>

#include "vec/exec/format/table/iceberg/schema.h"
#include "vec/exec/format/table/iceberg/types.h"

namespace doris {
namespace iceberg {

class PartitionSpecParserTest : public testing::Test {
public:
    PartitionSpecParserTest() = default;
    virtual ~PartitionSpecParserTest() = default;
};

TEST(PartitionSpecParserTest, test_from_json_with_field_id) {
    std::string specString =
            "{\n"
            "  \"spec-id\" : 1,\n"
            "  \"fields\" : [ {\n"
            "    \"name\" : \"identity1\",\n"
            "    \"transform\" : \"identity\",\n"
            "    \"source-id\" : 1,\n"
            "    \"field-id\" : 1001\n"
            "  }, {\n"
            "    \"name\" : \"identity2\",\n"
            "    \"transform\" : \"identity\",\n"
            "    \"source-id\" : 2,\n"
            "    \"field-id\" : 1000\n"
            "  } ]\n"
            "}";
    std::vector<NestedField> columns;
    columns.emplace_back(false, 3, "id", std::make_unique<IntegerType>(), std::nullopt);
    columns.emplace_back(false, 4, "data", std::make_unique<StringType>(), std::nullopt);
    std::shared_ptr<Schema> schema = std::make_shared<Schema>(std::move(columns));
    std::unique_ptr<PartitionSpec> spec = PartitionSpecParser::from_json(schema, specString);

    EXPECT_EQ(2, spec->fields().size());
    EXPECT_EQ(1001, spec->fields()[0].field_id());
    EXPECT_EQ(1000, spec->fields()[1].field_id());
}

TEST(PartitionSpecParserTest, test_from_json_without_field_id) {
    std::string specString =
            "{\n"
            "  \"spec-id\" : 1,\n"
            "  \"fields\" : [ {\n"
            "    \"name\" : \"identity1\",\n"
            "    \"transform\" : \"identity\",\n"
            "    \"source-id\" : 1\n"
            "  }, {\n"
            "    \"name\" : \"identity2\",\n"
            "    \"transform\" : \"identity\",\n"
            "    \"source-id\" : 2\n"
            "  } ]\n"
            "}";
    std::vector<NestedField> columns;
    columns.emplace_back(false, 3, "id", std::make_unique<IntegerType>(), std::nullopt);
    columns.emplace_back(false, 4, "data", std::make_unique<StringType>(), std::nullopt);
    std::shared_ptr<Schema> schema = std::make_shared<Schema>(std::move(columns));
    std::unique_ptr<PartitionSpec> spec = PartitionSpecParser::from_json(schema, specString);

    EXPECT_EQ(2, spec->fields().size());
    EXPECT_EQ(1000, spec->fields()[0].field_id());
    EXPECT_EQ(1001, spec->fields()[1].field_id());
}

} // namespace iceberg
} // namespace doris
