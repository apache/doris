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

#include "core/types.h"
#include "format/orc/orc_memory_stream_test.h"
#include "format/orc/vorc_reader.h"
#include "orc/ColumnPrinter.hh"

namespace doris {
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
        auto reader = OrcReader::create_unique(params, range, 4064, "", nullptr, nullptr, true);
        reader->_reader = std::move(orc_reader);
        std::vector<std::string> tmp;
        tmp.emplace_back("col1");

        reader->_table_column_names = tmp;
        Status st = reader->_init_read_columns();
        std::cout << "st =" << st << "\n";
        // _init_read_columns builds _type_map; _read_file_cols is populated later
        // in _do_init_reader's standalone path when _table_column_names is set.
        ASSERT_TRUE(reader->_type_map.contains("col1"));
        ASSERT_FALSE(reader->_type_map.contains("nonexistent"));
    }
}

} // namespace doris
