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

#include <memory>

#include "orc/ColumnPrinter.hh"
#include "orc_memory_stream_test.h"
#include "vec/core/types.h"
#include "vec/exec/format/orc/vorc_reader.h"

namespace doris {
namespace vectorized {
class OrcReaderInitColumnTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}
};
TEST_F(OrcReaderInitColumnTest, InitReadColumn) {
    {
        using namespace orc;
        size_t rowCount = 10;
        MemoryOutputStream memStream(100 * 1024 * 1024);
        MemoryPool* pool = getDefaultPool();
        auto type = std::unique_ptr<Type>(Type::buildTypeFromString("struct<col1:int,col2:int>"));
        WriterOptions options;
        options.setMemoryPool(pool);
        auto writer = createWriter(*type, &memStream, options);
        auto batch = writer->createRowBatch(rowCount);
        writer->add(*batch);
        writer->close();

        auto inStream =
                std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
        ReaderOptions readerOptions;
        readerOptions.setMemoryPool(*pool);
        auto orc_reader = createReader(std::move(inStream), readerOptions);

        TFileScanRangeParams params;
        TFileRangeDesc range;
        auto reader = OrcReader::create_unique(params, range, "", nullptr, true);
        reader->_reader = std::move(orc_reader);
        std::vector<std::string> tmp;
        tmp.emplace_back("col1");

        reader->_table_column_names = &tmp;
        Status st = reader->_init_read_columns();
        std::cout << "st =" << st << "\n";
        std::list<std::string> ans;
        ans.emplace_back("col1");
        ASSERT_EQ(ans, reader->_read_file_cols);
        ASSERT_EQ(ans, reader->_read_table_cols);
    }
}

TEST_F(OrcReaderInitColumnTest, CheckAcidSchemaTest) {
    using namespace orc;
    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto _reader = OrcReader::create_unique(params, range, "", nullptr, true);
    // 1. Test standard ACID schema
    {
        // Create standard ACID structure
        auto acid_type = createStructType();
        acid_type->addStructField("operation", createPrimitiveType(orc::TypeKind::INT));
        acid_type->addStructField("originalTransaction", createPrimitiveType(orc::TypeKind::LONG));
        acid_type->addStructField("bucket", createPrimitiveType(orc::TypeKind::INT));
        acid_type->addStructField("rowId", createPrimitiveType(orc::TypeKind::LONG));
        acid_type->addStructField("currentTransaction", createPrimitiveType(orc::TypeKind::LONG));
        acid_type->addStructField("row", createStructType());

        ASSERT_TRUE(_reader->_check_acid_schema(*acid_type));
    }

    // 2. Test case-insensitive field names
    {
        auto acid_type = createStructType();
        acid_type->addStructField("OPERATION", createPrimitiveType(orc::TypeKind::INT));
        acid_type->addStructField("OriginalTransaction", createPrimitiveType(orc::TypeKind::LONG));
        acid_type->addStructField("Bucket", createPrimitiveType(orc::TypeKind::INT));
        acid_type->addStructField("ROWID", createPrimitiveType(orc::TypeKind::LONG));
        acid_type->addStructField("currentTRANSACTION", createPrimitiveType(orc::TypeKind::LONG));
        acid_type->addStructField("ROW", createStructType());

        ASSERT_TRUE(_reader->_check_acid_schema(*acid_type));
    }

    // 3. Test non-ACID schema - field count mismatch
    {
        auto non_acid_type = createStructType();
        non_acid_type->addStructField("operation", createPrimitiveType(orc::TypeKind::INT));
        non_acid_type->addStructField("originalTransaction",
                                      createPrimitiveType(orc::TypeKind::LONG));
        // Only added two fields

        ASSERT_FALSE(_reader->_check_acid_schema(*non_acid_type));
    }

    // 4. Test non-ACID schema - field name mismatch
    {
        auto wrong_name_type = createStructType();
        wrong_name_type->addStructField("operation", createPrimitiveType(orc::TypeKind::INT));
        wrong_name_type->addStructField("wrongName", createPrimitiveType(orc::TypeKind::LONG));
        wrong_name_type->addStructField("bucket", createPrimitiveType(orc::TypeKind::INT));
        wrong_name_type->addStructField("rowId", createPrimitiveType(orc::TypeKind::LONG));
        wrong_name_type->addStructField("currentTransaction",
                                        createPrimitiveType(orc::TypeKind::LONG));
        wrong_name_type->addStructField("row", createStructType());

        ASSERT_FALSE(_reader->_check_acid_schema(*wrong_name_type));
    }

    // 5. Test non-struct type
    {
        auto int_type = createPrimitiveType(orc::TypeKind::INT);
        ASSERT_FALSE(_reader->_check_acid_schema(*int_type));

        auto string_type = createPrimitiveType(orc::TypeKind::STRING);
        ASSERT_FALSE(_reader->_check_acid_schema(*string_type));
    }
}

TEST_F(OrcReaderInitColumnTest, RemoveAcidTest) {
    using namespace orc;
    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto _reader = OrcReader::create_unique(params, range, "", nullptr, true);
    // 1. Test removing ACID info from ACID schema
    {
        // Create ACID schema
        auto acid_type = createStructType();
        acid_type->addStructField("operation", createPrimitiveType(orc::TypeKind::INT));
        acid_type->addStructField("originalTransaction", createPrimitiveType(orc::TypeKind::LONG));
        acid_type->addStructField("bucket", createPrimitiveType(orc::TypeKind::INT));
        acid_type->addStructField("rowId", createPrimitiveType(orc::TypeKind::LONG));
        acid_type->addStructField("currentTransaction", createPrimitiveType(orc::TypeKind::LONG));

        // Create actual data structure
        auto row_type = createStructType();
        row_type->addStructField("id", createPrimitiveType(orc::TypeKind::INT));
        row_type->addStructField("name", createPrimitiveType(orc::TypeKind::STRING));
        acid_type->addStructField("row", std::move(row_type));

        // Verify that after removing ACID we get the type of the row field
        const orc::Type& removed_type = _reader->remove_acid(*acid_type);
        ASSERT_EQ(removed_type.getKind(), orc::TypeKind::STRUCT);
        ASSERT_EQ(removed_type.getSubtypeCount(), 2); // id and name fields
        ASSERT_EQ(removed_type.getFieldName(0), "id");
        ASSERT_EQ(removed_type.getFieldName(1), "name");
    }

    // 2. Test that non-ACID schema remains unchanged
    {
        // Create normal schema
        auto normal_type = createStructType();
        normal_type->addStructField("field1", createPrimitiveType(orc::TypeKind::INT));
        normal_type->addStructField("field2", createPrimitiveType(orc::TypeKind::STRING));

        const orc::Type& result_type = _reader->remove_acid(*normal_type);
        ASSERT_EQ(&result_type, normal_type.get()); // Should return the same type
        ASSERT_EQ(result_type.getSubtypeCount(), 2);
        ASSERT_EQ(result_type.getFieldName(0), "field1");
        ASSERT_EQ(result_type.getFieldName(1), "field2");
    }

    // 3. Test primitive types (non-struct) remain unchanged
    {
        auto int_type = createPrimitiveType(orc::TypeKind::INT);
        const orc::Type& result_type = _reader->remove_acid(*int_type);
        ASSERT_EQ(&result_type, int_type.get());
        ASSERT_EQ(result_type.getKind(), orc::TypeKind::INT);
    }

    // 4. Test complex nested ACID schema
    {
        // Create nested ACID schema
        auto acid_type = createStructType();
        acid_type->addStructField("operation", createPrimitiveType(orc::TypeKind::INT));
        acid_type->addStructField("originalTransaction", createPrimitiveType(orc::TypeKind::LONG));
        acid_type->addStructField("bucket", createPrimitiveType(orc::TypeKind::INT));
        acid_type->addStructField("rowId", createPrimitiveType(orc::TypeKind::LONG));
        acid_type->addStructField("currentTransaction", createPrimitiveType(orc::TypeKind::LONG));

        // Create complex row structure
        auto row_type = createStructType();

        // Add basic fields
        row_type->addStructField("id", createPrimitiveType(orc::TypeKind::INT));

        // Add array field
        auto array_type = createListType(createPrimitiveType(orc::TypeKind::STRING));
        row_type->addStructField("tags", std::move(array_type));

        // Add Map field
        auto map_type = createMapType(createPrimitiveType(orc::TypeKind::STRING),
                                      createPrimitiveType(orc::TypeKind::INT));
        row_type->addStructField("properties", std::move(map_type));

        acid_type->addStructField("row", std::move(row_type));

        // Verify structure after removing ACID
        const orc::Type& removed_type = _reader->remove_acid(*acid_type);
        ASSERT_EQ(removed_type.getKind(), orc::TypeKind::STRUCT);
        ASSERT_EQ(removed_type.getSubtypeCount(), 3); // id, tags, properties
        ASSERT_EQ(removed_type.getFieldName(0), "id");
        ASSERT_EQ(removed_type.getFieldName(1), "tags");
        ASSERT_EQ(removed_type.getFieldName(2), "properties");

        // Verify field types
        ASSERT_EQ(removed_type.getSubtype(0)->getKind(), orc::TypeKind::INT);
        ASSERT_EQ(removed_type.getSubtype(1)->getKind(), orc::TypeKind::LIST);
        ASSERT_EQ(removed_type.getSubtype(2)->getKind(), orc::TypeKind::MAP);
    }
}

} // namespace vectorized
} // namespace doris
