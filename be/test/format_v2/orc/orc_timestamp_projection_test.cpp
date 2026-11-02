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

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <orc/OrcFile.hh>

#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "format_v2/orc/orc_reader.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"

namespace doris {

class OrcTimestampProjectionTest : public testing::Test {
protected:
    void SetUp() override {
        TimezoneUtils::load_timezones_to_cache();
        path = (std::filesystem::temp_directory_path() /
                ("orc_timestamp_projection_" + std::to_string(getpid()) + ".orc"))
                       .string();
    }
    void TearDown() override { std::filesystem::remove(path); }
    std::string path;
};

TEST_F(OrcTimestampProjectionTest, OnlyProjectedTimestampLeavesReadWriterTimezones) {
    auto type = ::orc::Type::buildTypeFromString(
            "struct<ns:timestamp,s:struct<i:int,unused:timestamp>>");
    const_cast<::orc::Type*>(type->getSubtype(0))->setAttribute("iceberg.timestamp-unit", "NANOS");
    auto output = ::orc::writeLocalFile(path);
    ::orc::WriterOptions options;
    options.setTimezoneName("UTC");
    options.setStripeSize(0);
    auto writer = ::orc::createWriter(*type, output.get(), options);
    constexpr size_t stripes = 32;
    for (size_t i = 0; i < stripes; ++i) {
        auto batch = writer->createRowBatch(1);
        auto& root = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& ns = dynamic_cast<::orc::TimestampVectorBatch&>(*root.fields[0]);
        auto& record = dynamic_cast<::orc::StructVectorBatch&>(*root.fields[1]);
        auto& number = dynamic_cast<::orc::LongVectorBatch&>(*record.fields[0]);
        auto& timestamp = dynamic_cast<::orc::TimestampVectorBatch&>(*record.fields[1]);
        root.numElements = ns.numElements = record.numElements = number.numElements =
                timestamp.numElements = 1;
        number.data[0] = i;
        ns.data[0] = timestamp.data[0] = 1704153600;
        ns.nanoseconds[0] = timestamp.nanoseconds[0] = 123456000;
        writer->add(*batch);
    }
    writer->close();
    output->close();

    for (bool project_timestamp : {false, true}) {
        for (bool predicate_column : {false, true}) {
            SCOPED_TRACE(testing::Message() << project_timestamp << ", " << predicate_column);
            RuntimeProfile profile("timestamp_projection");
            auto properties = std::make_shared<io::FileSystemProperties>();
            properties->system_type = TFileType::FILE_LOCAL;
            auto description = std::make_unique<io::FileDescription>();
            description->path = path;
            description->file_size = std::filesystem::file_size(path);
            format::orc::OrcReader reader(properties, description, nullptr, &profile);
            RuntimeState state {TQueryOptions(), TQueryGlobals()};
            ASSERT_TRUE(reader.init(&state).ok());
            std::vector<format::ColumnDefinition> schema;
            ASSERT_TRUE(reader.get_schema(&schema).ok());
            auto request = std::make_shared<format::FileScanRequest>();
            auto projection = format::LocalColumnIndex::partial_local(1);
            projection.children = {format::LocalColumnIndex::local(0)};
            if (project_timestamp) {
                projection.children.push_back(format::LocalColumnIndex::local(1));
            }
            (predicate_column ? request->predicate_columns : request->non_predicate_columns)
                    .push_back(projection);
            ASSERT_TRUE(reader.open(request).ok());
            DataTypePtr projected_type = schema[1].type;
            if (!project_timestamp) {
                const auto& structure =
                        assert_cast<const DataTypeStruct&>(*remove_nullable(projected_type));
                projected_type = make_nullable(std::make_shared<DataTypeStruct>(
                        DataTypes {structure.get_elements()[0]}, Strings {"i"}));
            }
            bool eof = false;
            size_t total = 0;
            while (!eof) {
                Block block;
                block.insert({projected_type->create_column(), projected_type, "s"});
                size_t rows = 0;
                auto status = reader.get_block(&block, &rows, &eof);
                ASSERT_TRUE(status.ok()) << status;
                const auto& record = assert_cast<const ColumnStruct&>(
                        assert_cast<const ColumnNullable&>(*block.get_by_position(0).column)
                                .get_nested_column());
                const auto& values = assert_cast<const ColumnInt32&>(
                        assert_cast<const ColumnNullable&>(record.get_column(0))
                                .get_nested_column());
                for (size_t row = 0; row < rows; ++row) {
                    EXPECT_EQ(values.get_element(row), total + row);
                    if (project_timestamp) {
                        const auto& struct_type = assert_cast<const DataTypeStruct&>(
                                *remove_nullable(schema[1].type));
                        EXPECT_EQ(
                                struct_type.get_elements()[1]->to_string(
                                        record.get_column(1), row, DataTypeSerDe::FormatOptions {}),
                                "2024-01-02 00:00:00.123456");
                    }
                }
                total += rows;
            }
            EXPECT_EQ(total, stripes);
            // An unread plain timestamp must not turn a nested integer projection into
            // per-stripe timezone discovery just because another file column is NANOS.
            EXPECT_EQ(profile.get_counter("OrcTimestampStripeFootersRead")->value(),
                      project_timestamp ? stripes : 0);
            if (!project_timestamp) {
                EXPECT_EQ(profile.get_counter("OrcRowReadersCreated")->value(), 1);
            }
        }
    }
}

} // namespace doris
