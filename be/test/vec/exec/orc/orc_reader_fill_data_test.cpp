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
#include "vec/columns/column_array.h"
#include "vec/columns/column_struct.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exec/format/orc/vorc_reader.h"

namespace doris {
namespace vectorized {
class OrcReaderFillDataTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}
};

std::unique_ptr<orc::LongVectorBatch> create_long_batch(size_t size,
                                                        const std::vector<int64_t>& values,
                                                        const std::vector<bool>& nulls = {}) {
    auto batch = std::make_unique<orc::LongVectorBatch>(size, *orc::getDefaultPool());
    batch->resize(size);
    batch->notNull.resize(size);

    bool has_nulls = nulls.size() == size;
    for (size_t i = 0; i < size; ++i) {
        if (has_nulls) {
            batch->notNull[i] = !nulls[i];
        } else {
            batch->notNull[i] = true;
        }

        if (batch->notNull[i]) {
            batch->data[i] = values[i];
        }
    }

    if (has_nulls) {
        batch->hasNulls = true;
    } else {
        batch->hasNulls = false;
    }
    return batch;
}

TEST_F(OrcReaderFillDataTest, TestFillLongColumn) {
    std::vector<int64_t> values = {1, 2, 3, 4, 5};
    auto batch = create_long_batch(values.size(), values);
    auto column = ColumnInt64::create();
    auto data_type = std::make_shared<DataTypeInt64>();

    auto orc_type_ptr = createPrimitiveType(orc::TypeKind::LONG);

    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto reader = OrcReader::create_unique(params, range, "", nullptr, true);

    MutableColumnPtr xx = column->assume_mutable();

    Status status = reader->_fill_doris_data_column<false>(
            "test_long", xx, data_type, orc_type_ptr.get(), batch.get(), values.size());

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(column->size(), values.size());

    for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_EQ(column->get_int(i), values[i]);
    }
}

TEST_F(OrcReaderFillDataTest, TestFillLongColumnWithNull) {
    std::vector<int64_t> values = {1, 2, 3, 4, 5};
    std::vector<bool> nulls = {false, true, false, true, false};
    auto batch = create_long_batch(values.size(), values, nulls);
    auto column = ColumnInt64::create();
    auto data_type = std::make_shared<DataTypeInt64>();

    auto orc_type_ptr = createPrimitiveType(orc::TypeKind::LONG);

    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto reader = OrcReader::create_unique(params, range, "", nullptr, true);

    MutableColumnPtr xx = column->assume_mutable();

    Status status = reader->_fill_doris_data_column<false>(
            "test_long_with_null", xx, data_type, orc_type_ptr.get(), batch.get(), values.size());

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(column->size(), values.size());

    for (size_t i = 0; i < values.size(); ++i) {
        if (!nulls[i]) {
            ASSERT_EQ(column->get_int(i), values[i]);
        }
    }
}

TEST_F(OrcReaderFillDataTest, ComplexTypeConversionTest) {
    // Array类型测试
    {
        using namespace orc;
        std::unique_ptr<orc::Type> type(orc::Type::buildTypeFromString("struct<col1:array<int>>"));

        WriterOptions options;
        options.setMemoryPool(orc::getDefaultPool());

        MemoryOutputStream memStream(100 * 1024 * 1024);
        std::unique_ptr<orc::Writer> writer = orc::createWriter(*type, &memStream, options);

        std::unique_ptr<orc::ColumnVectorBatch> batch = writer->createRowBatch(1024);
        orc::StructVectorBatch* structBatch = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        orc::ListVectorBatch* listBatch =
                dynamic_cast<orc::ListVectorBatch*>(structBatch->fields[0]);
        orc::LongVectorBatch* intBatch =
                dynamic_cast<orc::LongVectorBatch*>(listBatch->elements.get());
        int64_t* data = intBatch->data.data();
        int64_t* offsets = listBatch->offsets.data();
        uint64_t rowCount = 20;
        uint64_t offset = 0;
        uint64_t maxListLength = 5;
        for (uint64_t i = 0; i < rowCount; ++i) {
            offsets[i] = static_cast<int64_t>(offset);
            for (uint64_t length = i % maxListLength + 1; length != 0; --length) {
                data[offset++] = static_cast<int64_t>(i);
            }
        }
        offsets[rowCount] = static_cast<int64_t>(offset);

        structBatch->numElements = rowCount;
        listBatch->numElements = rowCount;

        TFileScanRangeParams params;
        TFileRangeDesc range;
        auto reader = OrcReader::create_unique(params, range, "", nullptr, true);

        auto doris_struct_type = std::make_shared<DataTypeStruct>(
                std::vector<DataTypePtr> {
                        std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>())},
                std::vector<std::string> {"col1"});
        MutableColumnPtr doris_column = doris_struct_type->create_column()->assume_mutable();

        Status status = reader->_fill_doris_data_column<false>(
                "test", doris_column, doris_struct_type, type.get(), structBatch, rowCount);

        ASSERT_TRUE(status.ok());
        std::string line;
        std::unique_ptr<orc::ColumnPrinter> printer = orc::createColumnPrinter(line, type.get());
        printer->reset(*structBatch);

        for (int i = 0; i < rowCount; i++) {
            line.clear();
            printer->printRow(i);
            std::cout << "line = " << line << "\n";
        }
        Block block {std::vector<ColumnWithTypeAndName> {
                {doris_column->get_ptr(), doris_struct_type, "cc"}}};
        std::cout << block.dump_data() << "\n";

        ASSERT_EQ(block.dump_data(),
                  "+-----------------------------+\n"
                  "|cc(Struct(col1:Array(Int32)))|\n"
                  "+-----------------------------+\n"
                  "|                        {[0]}|\n"
                  "|                     {[1, 1]}|\n"
                  "|                  {[2, 2, 2]}|\n"
                  "|               {[3, 3, 3, 3]}|\n"
                  "|            {[4, 4, 4, 4, 4]}|\n"
                  "|                        {[5]}|\n"
                  "|                     {[6, 6]}|\n"
                  "|                  {[7, 7, 7]}|\n"
                  "|               {[8, 8, 8, 8]}|\n"
                  "|            {[9, 9, 9, 9, 9]}|\n"
                  "|                       {[10]}|\n"
                  "|                   {[11, 11]}|\n"
                  "|               {[12, 12, 12]}|\n"
                  "|           {[13, 13, 13, 13]}|\n"
                  "|       {[14, 14, 14, 14, 14]}|\n"
                  "|                       {[15]}|\n"
                  "|                   {[16, 16]}|\n"
                  "|               {[17, 17, 17]}|\n"
                  "|           {[18, 18, 18, 18]}|\n"
                  "|       {[19, 19, 19, 19, 19]}|\n"
                  "+-----------------------------+\n");
    }

    {
        using namespace orc;
        auto type = std::unique_ptr<Type>(Type::buildTypeFromString("struct<col1:int,col2:int>"));

        size_t rowCount = 10;
        MemoryOutputStream memStream(100 * 1024 * 1024);
        WriterOptions options;
        options.setMemoryPool(getDefaultPool());
        auto writer = createWriter(*type, &memStream, options);
        auto batch = writer->createRowBatch(rowCount);
        auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
        auto& longBatch1 = dynamic_cast<LongVectorBatch&>(*structBatch.fields[0]);
        auto& longBatch2 = dynamic_cast<LongVectorBatch&>(*structBatch.fields[1]);
        structBatch.numElements = rowCount;
        longBatch1.numElements = rowCount;
        longBatch2.numElements = rowCount;
        for (size_t i = 0; i < rowCount; ++i) {
            longBatch1.data[i] = static_cast<int64_t>(i * 100);
            longBatch2.data[i] = static_cast<int64_t>(i * 300);
        }

        std::string line;
        std::unique_ptr<orc::ColumnPrinter> printer = orc::createColumnPrinter(line, type.get());
        printer->reset(structBatch);

        for (int i = 0; i < rowCount; i++) {
            line.clear();
            printer->printRow(i);
            std::cout << "line = " << line << "\n";
        }

        TFileScanRangeParams params;
        TFileRangeDesc range;
        auto reader = OrcReader::create_unique(params, range, "", nullptr, true);

        auto doris_struct_type = std::make_shared<DataTypeStruct>(
                std::vector<DataTypePtr> {std::make_shared<DataTypeInt32>(),
                                          std::make_shared<DataTypeInt32>()},
                std::vector<std::string> {"col1", "col2"});
        MutableColumnPtr doris_column = doris_struct_type->create_column()->assume_mutable();

        Status status = reader->_fill_doris_data_column<false>(
                "test", doris_column, doris_struct_type, type.get(), &structBatch, rowCount);

        ASSERT_TRUE(status.ok());

        Block block {std::vector<ColumnWithTypeAndName> {
                {doris_column->get_ptr(), doris_struct_type, "cc"}}};
        std::cout << block.dump_data() << "\n";

        ASSERT_EQ(block.dump_data(),
                  "+----------------------------------+\n"
                  "|cc(Struct(col1:Int32, col2:Int32))|\n"
                  "+----------------------------------+\n"
                  "|                            {0, 0}|\n"
                  "|                        {100, 300}|\n"
                  "|                        {200, 600}|\n"
                  "|                        {300, 900}|\n"
                  "|                       {400, 1200}|\n"
                  "|                       {500, 1500}|\n"
                  "|                       {600, 1800}|\n"
                  "|                       {700, 2100}|\n"
                  "|                       {800, 2400}|\n"
                  "|                       {900, 2700}|\n"
                  "+----------------------------------+\n");
    }

    {
        using namespace orc;

        const uint64_t maxPrecision = 18;
        MemoryOutputStream memStream(100 * 1024 * 102);
        MemoryPool* pool = getDefaultPool();
        std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:decimal(18,5)>"));
        WriterOptions options;
        options.setMemoryPool(pool);

        uint64_t rowCount = 5;
        std::unique_ptr<Writer> writer = createWriter(*type, &memStream, options);
        std::unique_ptr<ColumnVectorBatch> batch =
                writer->createRowBatch(2 * rowCount + 2 * maxPrecision);
        StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
        Decimal64VectorBatch* decBatch =
                dynamic_cast<Decimal64VectorBatch*>(structBatch->fields[0]);
        decBatch->scale = 5;
        decBatch->precision = 18;
        // write positive decimals
        for (uint64_t i = 0; i < rowCount; ++i) {
            decBatch->values[i] = static_cast<int64_t>(i + 10000);
        }

        // write negative decimals
        for (uint64_t i = rowCount; i < 2 * rowCount; ++i) {
            decBatch->values[i] = static_cast<int64_t>(i - 10000);
        }

        // write all precision decimals
        int64_t dec = 0;
        for (uint64_t i = 2 * rowCount; i < 2 * rowCount + 2 * maxPrecision; i += 2) {
            dec = dec * 10 + 9;
            decBatch->values[i] = dec;
            decBatch->values[i + 1] = -dec;
        }
        rowCount = 2 * (rowCount + maxPrecision);
        structBatch->numElements = decBatch->numElements = rowCount;

        std::string line;
        std::unique_ptr<orc::ColumnPrinter> printer = orc::createColumnPrinter(line, type.get());
        printer->reset(*structBatch);

        for (int i = 0; i < rowCount; i++) {
            line.clear();
            printer->printRow(i);
            std::cout << "line = " << line << "\n";
        }

        TFileScanRangeParams params;
        TFileRangeDesc range;
        auto reader = OrcReader::create_unique(params, range, "", nullptr, true);

        auto doris_struct_type = std::make_shared<DataTypeStruct>(
                std::vector<DataTypePtr> {std::make_shared<DataTypeDecimal<Decimal64>>(18, 5)},
                std::vector<std::string> {"col1"});
        MutableColumnPtr doris_column = doris_struct_type->create_column()->assume_mutable();
        reader->_decimal_scale_params.resize(0);
        reader->_decimal_scale_params_index = 0;
        Status status = reader->_fill_doris_data_column<false>(
                "test", doris_column, doris_struct_type, type.get(), structBatch, rowCount);

        ASSERT_TRUE(status.ok());

        Block block {std::vector<ColumnWithTypeAndName> {
                {doris_column->get_ptr(), doris_struct_type, "cc"}}};
        std::cout << block.dump_data() << "\n";
        ASSERT_EQ(block.dump_data(),
                  "+-------------------------------+\n"
                  "|cc(Struct(col1:Decimal(18, 5)))|\n"
                  "+-------------------------------+\n"
                  "|                      {0.10000}|\n"
                  "|                      {0.10001}|\n"
                  "|                      {0.10002}|\n"
                  "|                      {0.10003}|\n"
                  "|                      {0.10004}|\n"
                  "|                     {-0.09995}|\n"
                  "|                     {-0.09994}|\n"
                  "|                     {-0.09993}|\n"
                  "|                     {-0.09992}|\n"
                  "|                     {-0.09991}|\n"
                  "|                      {0.00009}|\n"
                  "|                     {-0.00009}|\n"
                  "|                      {0.00099}|\n"
                  "|                     {-0.00099}|\n"
                  "|                      {0.00999}|\n"
                  "|                     {-0.00999}|\n"
                  "|                      {0.09999}|\n"
                  "|                     {-0.09999}|\n"
                  "|                      {0.99999}|\n"
                  "|                     {-0.99999}|\n"
                  "|                      {9.99999}|\n"
                  "|                     {-9.99999}|\n"
                  "|                     {99.99999}|\n"
                  "|                    {-99.99999}|\n"
                  "|                    {999.99999}|\n"
                  "|                   {-999.99999}|\n"
                  "|                   {9999.99999}|\n"
                  "|                  {-9999.99999}|\n"
                  "|                  {99999.99999}|\n"
                  "|                 {-99999.99999}|\n"
                  "|                 {999999.99999}|\n"
                  "|                {-999999.99999}|\n"
                  "|                {9999999.99999}|\n"
                  "|               {-9999999.99999}|\n"
                  "|               {99999999.99999}|\n"
                  "|              {-99999999.99999}|\n"
                  "|              {999999999.99999}|\n"
                  "|             {-999999999.99999}|\n"
                  "|             {9999999999.99999}|\n"
                  "|            {-9999999999.99999}|\n"
                  "|            {99999999999.99999}|\n"
                  "|           {-99999999999.99999}|\n"
                  "|           {999999999999.99999}|\n"
                  "|          {-999999999999.99999}|\n"
                  "|          {9999999999999.99999}|\n"
                  "|         {-9999999999999.99999}|\n"
                  "+-------------------------------+\n");
    }

    {
        using namespace orc;
        size_t rowCount = 10;
        MemoryOutputStream memStream(100 * 1024 * 1024);
        MemoryPool* pool = getDefaultPool();
        auto type = std::unique_ptr<Type>(Type::buildTypeFromString("map<int,float>"));
        WriterOptions options;
        options.setMemoryPool(pool);
        auto writer = createWriter(*type, &memStream, options);
        auto batch = writer->createRowBatch(rowCount * 10);
        auto& mapBatch = dynamic_cast<MapVectorBatch&>(*batch);
        int64_t* offsets = mapBatch.offsets.data();
        auto& keyBatch = dynamic_cast<LongVectorBatch&>(*(mapBatch.keys));
        auto& valueBatch = dynamic_cast<DoubleVectorBatch&>(*(mapBatch.elements));

        mapBatch.numElements = rowCount;
        uint64_t Offset = 0;

        for (size_t i = 0; i < rowCount; ++i) {
            offsets[i] = static_cast<int64_t>(Offset);
            for (int j = 0; j < i / 2; j++) {
                keyBatch.data[Offset] = i * 100;
                valueBatch.data[Offset] = i * 3.;
                Offset++;
            }
        }
        offsets[rowCount] = static_cast<int64_t>(Offset);

        keyBatch.numElements = Offset;
        valueBatch.numElements = Offset;

        std::string line;
        std::unique_ptr<orc::ColumnPrinter> printer = orc::createColumnPrinter(line, type.get());
        printer->reset(mapBatch);

        for (int i = 0; i < rowCount; i++) {
            line.clear();

            printer->printRow(i);
            std::cout << "line = " << line << "\n";
        }

        TFileScanRangeParams params;
        TFileRangeDesc range;
        auto reader = OrcReader::create_unique(params, range, "", nullptr, true);

        auto doris_struct_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(),
                                                               std::make_shared<DataTypeFloat32>());
        MutableColumnPtr doris_column = doris_struct_type->create_column()->assume_mutable();

        Status status = reader->_fill_doris_data_column<false>(
                "test", doris_column, doris_struct_type, type.get(), &mapBatch, rowCount);

        ASSERT_TRUE(status.ok());

        Block block {std::vector<ColumnWithTypeAndName> {
                {doris_column->get_ptr(), doris_struct_type, "cc"}}};
        std::cout << block.dump_data() << "\n";
        ASSERT_EQ(block.dump_data(),
                  "+-----------------------+\n"
                  "|cc(Map(Int32, Float32))|\n"
                  "+-----------------------+\n"
                  "|                     {}|\n"
                  "|                     {}|\n"
                  "|                {200:6}|\n"
                  "|                {300:9}|\n"
                  "|               {400:12}|\n"
                  "|               {500:15}|\n"
                  "|               {600:18}|\n"
                  "|               {700:21}|\n"
                  "|               {800:24}|\n"
                  "|               {900:27}|\n"
                  "+-----------------------+\n");
    }
}
} // namespace vectorized
} // namespace doris