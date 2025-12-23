/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cstring>

#include "Reader.hh"
#include "orc/Reader.hh"

#include "Adaptor.hh"
#include "MemoryInputStream.hh"
#include "MemoryOutputStream.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

namespace orc {

  using ::testing::ElementsAreArray;

  static const int DEFAULT_MEM_STREAM_SIZE = 1024 * 1024;  // 1M

  TEST(TestReader, testWriterVersions) {
    EXPECT_EQ("original", writerVersionToString(WriterVersion_ORIGINAL));
    EXPECT_EQ("HIVE-8732", writerVersionToString(WriterVersion_HIVE_8732));
    EXPECT_EQ("HIVE-4243", writerVersionToString(WriterVersion_HIVE_4243));
    EXPECT_EQ("HIVE-12055", writerVersionToString(WriterVersion_HIVE_12055));
    EXPECT_EQ("HIVE-13083", writerVersionToString(WriterVersion_HIVE_13083));
    EXPECT_EQ("future - 99", writerVersionToString(static_cast<WriterVersion>(99)));
  }

  TEST(TestReader, testCompressionNames) {
    EXPECT_EQ("none", compressionKindToString(CompressionKind_NONE));
    EXPECT_EQ("zlib", compressionKindToString(CompressionKind_ZLIB));
    EXPECT_EQ("snappy", compressionKindToString(CompressionKind_SNAPPY));
    EXPECT_EQ("lzo", compressionKindToString(CompressionKind_LZO));
    EXPECT_EQ("lz4", compressionKindToString(CompressionKind_LZ4));
    EXPECT_EQ("zstd", compressionKindToString(CompressionKind_ZSTD));
    EXPECT_EQ("unknown - 99", compressionKindToString(static_cast<CompressionKind>(99)));
  }

  TEST(TestRowReader, computeBatchSize) {
    uint64_t rowIndexStride = 100;
    uint64_t rowsInCurrentStripe = 100 * 8 + 50;
    std::vector<uint64_t> nextSkippedRows = {0, 0, 400, 400, 0, 0, 800, 800, 0};

    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(1024, 0, rowsInCurrentStripe, rowIndexStride,
                                                 nextSkippedRows));
    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(1024, 50, rowsInCurrentStripe, rowIndexStride,
                                                 nextSkippedRows));
    EXPECT_EQ(200, RowReaderImpl::computeBatchSize(1024, 200, rowsInCurrentStripe, rowIndexStride,
                                                   nextSkippedRows));
    EXPECT_EQ(150, RowReaderImpl::computeBatchSize(1024, 250, rowsInCurrentStripe, rowIndexStride,
                                                   nextSkippedRows));
    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(1024, 550, rowsInCurrentStripe, rowIndexStride,
                                                 nextSkippedRows));
    EXPECT_EQ(100, RowReaderImpl::computeBatchSize(1024, 700, rowsInCurrentStripe, rowIndexStride,
                                                   nextSkippedRows));
    EXPECT_EQ(50, RowReaderImpl::computeBatchSize(50, 700, rowsInCurrentStripe, rowIndexStride,
                                                  nextSkippedRows));
    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(50, 810, rowsInCurrentStripe, rowIndexStride,
                                                 nextSkippedRows));
    EXPECT_EQ(0, RowReaderImpl::computeBatchSize(50, 900, rowsInCurrentStripe, rowIndexStride,
                                                 nextSkippedRows));
  }

  TEST(TestRowReader, advanceToNextRowGroup) {
    uint64_t rowIndexStride = 100;
    uint64_t rowsInCurrentStripe = 100 * 8 + 50;
    std::vector<uint64_t> nextSkippedRows = {0, 0, 400, 400, 0, 0, 800, 800, 0};

    EXPECT_EQ(200, RowReaderImpl::advanceToNextRowGroup(0, rowsInCurrentStripe, rowIndexStride,
                                                        nextSkippedRows));
    EXPECT_EQ(200, RowReaderImpl::advanceToNextRowGroup(150, rowsInCurrentStripe, rowIndexStride,
                                                        nextSkippedRows));
    EXPECT_EQ(250, RowReaderImpl::advanceToNextRowGroup(250, rowsInCurrentStripe, rowIndexStride,
                                                        nextSkippedRows));
    EXPECT_EQ(350, RowReaderImpl::advanceToNextRowGroup(350, rowsInCurrentStripe, rowIndexStride,
                                                        nextSkippedRows));
    EXPECT_EQ(350, RowReaderImpl::advanceToNextRowGroup(350, rowsInCurrentStripe, rowIndexStride,
                                                        nextSkippedRows));
    EXPECT_EQ(600, RowReaderImpl::advanceToNextRowGroup(500, rowsInCurrentStripe, rowIndexStride,
                                                        nextSkippedRows));
    EXPECT_EQ(699, RowReaderImpl::advanceToNextRowGroup(699, rowsInCurrentStripe, rowIndexStride,
                                                        nextSkippedRows));
    EXPECT_EQ(799, RowReaderImpl::advanceToNextRowGroup(799, rowsInCurrentStripe, rowIndexStride,
                                                        nextSkippedRows));
    EXPECT_EQ(850, RowReaderImpl::advanceToNextRowGroup(800, rowsInCurrentStripe, rowIndexStride,
                                                        nextSkippedRows));
    EXPECT_EQ(850, RowReaderImpl::advanceToNextRowGroup(900, rowsInCurrentStripe, rowIndexStride,
                                                        nextSkippedRows));
  }

  void CheckFileWithSargs(const char* fileName, const char* softwareVersion) {
    std::stringstream ss;
    if (const char* example_dir = std::getenv("ORC_EXAMPLE_DIR")) {
      ss << example_dir;
    } else {
      ss << "../../../examples";
    }
    // Read a file with bloom filters written by CPP writer in version 1.6.11.
    ss << "/" << fileName;
    ReaderOptions readerOpts;
    readerOpts.setReaderMetrics(nullptr);
    std::unique_ptr<Reader> reader =
        createReader(readLocalFile(ss.str().c_str(), readerOpts.getReaderMetrics()), readerOpts);
    EXPECT_EQ(WriterId::ORC_CPP_WRITER, reader->getWriterId());
    EXPECT_EQ(softwareVersion, reader->getSoftwareVersion());

    // Create SearchArgument with a EQUALS predicate which can leverage the bloom filters.
    RowReaderOptions rowReaderOpts;
    std::unique_ptr<SearchArgumentBuilder> sarg = SearchArgumentFactory::newBuilder();
    // Integer value 18000000000 has an inconsistent hash before the fix of ORC-1024.
    sarg->equals(1, PredicateDataType::LONG, Literal(static_cast<int64_t>(18000000000L)));
    std::unique_ptr<SearchArgument> final_sarg = sarg->build();
    rowReaderOpts.searchArgument(std::move(final_sarg));
    std::unique_ptr<RowReader> rowReader = reader->createRowReader(rowReaderOpts);

    // Make sure bad bloom filters won't affect the results.
    std::unique_ptr<ColumnVectorBatch> batch = rowReader->createRowBatch(1024);
    EXPECT_TRUE(rowReader->next(*batch));
    EXPECT_EQ(5, batch->numElements);
    EXPECT_FALSE(rowReader->next(*batch));
  }

  TEST(TestRowReader, testSkipBadBloomFilters) {
    CheckFileWithSargs("bad_bloom_filter_1.6.11.orc", "ORC C++ 1.6.11");
    CheckFileWithSargs("bad_bloom_filter_1.6.0.orc", "ORC C++");
  }

  void verifySelection(const std::unique_ptr<Reader>& reader,
                       const RowReaderOptions::IdReadIntentMap& idReadIntentMap,
                       const std::vector<uint32_t>& expectedSelection) {
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.includeTypesWithIntents(idReadIntentMap);
    std::unique_ptr<RowReader> rowReader = reader->createRowReader(rowReaderOpts);
    std::vector<bool> expected(reader->getType().getMaximumColumnId() + 1, false);
    for (auto id : expectedSelection) {
      expected[id] = true;
    }
    ASSERT_THAT(rowReader->getSelectedColumns(), ElementsAreArray(expected));
  }

  std::unique_ptr<Reader> createNestedListMemReader(MemoryOutputStream& memStream) {
    MemoryPool* pool = getDefaultPool();

    auto type = std::unique_ptr<Type>(
        Type::buildTypeFromString("struct<"
                                  "int_array:array<int>,"
                                  "int_array_array_array:array<array<array<int>>>"
                                  ">"));
    WriterOptions options;
    options.setStripeSize(1024 * 1024)
        .setCompressionBlockSize(1024)
        .setCompression(CompressionKind_NONE)
        .setMemoryPool(pool)
        .setRowIndexStride(1000);

    auto writer = createWriter(*type, &memStream, options);
    auto batch = writer->createRowBatch(100);
    auto& type0StructBatch = dynamic_cast<StructVectorBatch&>(*batch);
    auto& type1ListBatch = dynamic_cast<ListVectorBatch&>(*type0StructBatch.fields[0]);
    auto& type2LongBatch = dynamic_cast<LongVectorBatch&>(*type1ListBatch.elements);
    auto& type3ListBatch = dynamic_cast<ListVectorBatch&>(*type0StructBatch.fields[1]);
    auto& type4ListBatch = dynamic_cast<ListVectorBatch&>(*type3ListBatch.elements);
    auto& type5ListBatch = dynamic_cast<ListVectorBatch&>(*type4ListBatch.elements);
    auto& type6LongBatch = dynamic_cast<LongVectorBatch&>(*type5ListBatch.elements);

    type6LongBatch.numElements = 3;
    type6LongBatch.data[0] = 1;
    type6LongBatch.data[1] = 2;
    type6LongBatch.data[2] = 3;

    type5ListBatch.numElements = 3;
    type5ListBatch.offsets[0] = 0;
    type5ListBatch.offsets[1] = 1;
    type5ListBatch.offsets[2] = 2;
    type5ListBatch.offsets[3] = 3;

    type4ListBatch.numElements = 3;
    type4ListBatch.offsets[0] = 0;
    type4ListBatch.offsets[1] = 1;
    type4ListBatch.offsets[2] = 2;
    type4ListBatch.offsets[3] = 3;

    type3ListBatch.numElements = 1;
    type3ListBatch.offsets[0] = 0;
    type3ListBatch.offsets[1] = 3;

    type2LongBatch.numElements = 2;
    type2LongBatch.data[0] = -1;
    type2LongBatch.data[1] = -2;

    type1ListBatch.numElements = 1;
    type1ListBatch.offsets[0] = 0;
    type1ListBatch.offsets[1] = 2;

    type0StructBatch.numElements = 1;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    ReaderOptions readerOptions;
    readerOptions.setMemoryPool(*pool);
    return createReader(std::move(inStream), readerOptions);
  }

  TEST(TestReadIntent, testListAll) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedListMemReader(memStream);

    // select all of int_array.
    verifySelection(reader, {{1, ReadIntent_ALL}}, {0, 1, 2});
  }

  TEST(TestReadIntent, testListOffsets) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedListMemReader(memStream);

    // select only the offsets of int_array.
    verifySelection(reader, {{1, ReadIntent_OFFSETS}}, {0, 1});

    // select only the offsets of int_array and the outermost offsets of
    // int_array_array_array.
    verifySelection(reader, {{1, ReadIntent_OFFSETS}, {3, ReadIntent_OFFSETS}}, {0, 1, 3});

    // select the entire offsets of int_array_array_array without the elements.
    verifySelection(reader, {{3, ReadIntent_OFFSETS}, {5, ReadIntent_OFFSETS}}, {0, 3, 4, 5});
  }

  TEST(TestReadIntent, testListAllAndOffsets) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedListMemReader(memStream);

    // select all of int_array and only the outermost offsets of int_array_array_array.
    verifySelection(reader, {{1, ReadIntent_ALL}, {3, ReadIntent_OFFSETS}}, {0, 1, 2, 3});
  }

  TEST(TestReadIntent, testListConflictingIntent) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedListMemReader(memStream);

    // test conflicting ReadIntent on nested list.
    verifySelection(reader, {{3, ReadIntent_OFFSETS}, {5, ReadIntent_ALL}}, {0, 3, 4, 5, 6});
    verifySelection(reader, {{3, ReadIntent_ALL}, {5, ReadIntent_OFFSETS}}, {0, 3, 4, 5, 6});
  }

  TEST(TestReadIntent, testRowBatchContent) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedListMemReader(memStream);

    // select all of int_array and only the offsets of int_array_array.
    RowReaderOptions::IdReadIntentMap idReadIntentMap = {{1, ReadIntent_ALL},
                                                         {3, ReadIntent_OFFSETS}};
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.includeTypesWithIntents(idReadIntentMap);
    std::unique_ptr<RowReader> rowReader = reader->createRowReader(rowReaderOpts);

    // Read a row batch.
    std::unique_ptr<ColumnVectorBatch> batch = rowReader->createRowBatch(1024);
    EXPECT_TRUE(rowReader->next(*batch));
    EXPECT_EQ(1, batch->numElements);
    auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);

    // verify content of int_array selection.
    auto& intArrayBatch = dynamic_cast<ListVectorBatch&>(*structBatch.fields[0]);
    auto& innerLongBatch = dynamic_cast<LongVectorBatch&>(*intArrayBatch.elements);
    EXPECT_EQ(1, intArrayBatch.numElements);
    EXPECT_NE(nullptr, intArrayBatch.offsets.data());
    EXPECT_EQ(0, intArrayBatch.offsets.data()[0]);
    EXPECT_EQ(2, intArrayBatch.offsets.data()[1]);
    EXPECT_EQ(2, innerLongBatch.numElements);
    EXPECT_NE(nullptr, innerLongBatch.data.data());
    EXPECT_EQ(-1, innerLongBatch.data.data()[0]);
    EXPECT_EQ(-2, innerLongBatch.data.data()[1]);

    // verify content of int_array_array_array selection.
    auto& intArrayArrayArrayBatch = dynamic_cast<ListVectorBatch&>(*structBatch.fields[1]);
    EXPECT_EQ(1, intArrayArrayArrayBatch.numElements);
    EXPECT_NE(nullptr, intArrayArrayArrayBatch.offsets.data());
    EXPECT_EQ(0, intArrayArrayArrayBatch.offsets.data()[0]);
    EXPECT_EQ(3, intArrayArrayArrayBatch.offsets.data()[1]);
    EXPECT_EQ(nullptr, intArrayArrayArrayBatch.elements);
  }

  std::unique_ptr<Reader> createNestedMapMemReader(MemoryOutputStream& memStream) {
    MemoryPool* pool = getDefaultPool();

    auto type = std::unique_ptr<Type>(
        Type::buildTypeFromString("struct<"
                                  "id:int,"
                                  "single_map:map<string,string>,"
                                  "nested_map:map<string,map<string,map<string,string>>>"
                                  ">"));
    WriterOptions options;
    options.setStripeSize(1024 * 1024)
        .setCompressionBlockSize(1024)
        .setCompression(CompressionKind_NONE)
        .setMemoryPool(pool)
        .setRowIndexStride(1000);

    auto writer = createWriter(*type, &memStream, options);
    auto batch = writer->createRowBatch(100);
    auto& type0StructBatch = dynamic_cast<StructVectorBatch&>(*batch);
    auto& type1LongBatch = dynamic_cast<LongVectorBatch&>(*type0StructBatch.fields[0]);
    auto& type2MapBatch = dynamic_cast<MapVectorBatch&>(*type0StructBatch.fields[1]);
    auto& type3StringBatch = dynamic_cast<StringVectorBatch&>(*type2MapBatch.keys);
    auto& type4StringBatch = dynamic_cast<StringVectorBatch&>(*type2MapBatch.elements);
    auto& type5MapBatch = dynamic_cast<MapVectorBatch&>(*type0StructBatch.fields[2]);
    auto& type6StringBatch = dynamic_cast<StringVectorBatch&>(*type5MapBatch.keys);
    auto& type7MapBatch = dynamic_cast<MapVectorBatch&>(*type5MapBatch.elements);
    auto& type8StringBatch = dynamic_cast<StringVectorBatch&>(*type7MapBatch.keys);
    auto& type9MapBatch = dynamic_cast<MapVectorBatch&>(*type7MapBatch.elements);
    auto& type10StringBatch = dynamic_cast<StringVectorBatch&>(*type9MapBatch.keys);
    auto& type11StringBatch = dynamic_cast<StringVectorBatch&>(*type9MapBatch.elements);

    std::string map2Key = "k0";
    std::string map2Element = "v0";
    std::string map5Key = "k1";
    std::string map7Key = "k2";
    std::string map9Key = "k3";
    std::string map9Element = "v3";

    type11StringBatch.numElements = 1;
    type11StringBatch.data[0] = const_cast<char*>(map9Element.c_str());
    type11StringBatch.length[0] = static_cast<int64_t>(map9Element.length());

    type10StringBatch.numElements = 1;
    type10StringBatch.data[0] = const_cast<char*>(map9Key.c_str());
    type10StringBatch.length[0] = static_cast<int64_t>(map9Key.length());

    type9MapBatch.numElements = 1;
    type9MapBatch.offsets[0] = 0;
    type9MapBatch.offsets[1] = 1;

    type8StringBatch.numElements = 1;
    type8StringBatch.data[0] = const_cast<char*>(map7Key.c_str());
    type8StringBatch.length[0] = static_cast<int64_t>(map7Key.length());

    type7MapBatch.numElements = 1;
    type7MapBatch.offsets[0] = 0;
    type7MapBatch.offsets[1] = 1;

    type6StringBatch.numElements = 1;
    type6StringBatch.data[0] = const_cast<char*>(map5Key.c_str());
    type6StringBatch.length[0] = static_cast<int64_t>(map5Key.length());

    type5MapBatch.numElements = 1;
    type5MapBatch.offsets[0] = 0;
    type5MapBatch.offsets[1] = 1;

    type4StringBatch.numElements = 1;
    type4StringBatch.data[0] = const_cast<char*>(map2Element.c_str());
    type4StringBatch.length[0] = static_cast<int64_t>(map2Element.length());

    type3StringBatch.numElements = 1;
    type3StringBatch.data[0] = const_cast<char*>(map2Key.c_str());
    type3StringBatch.length[0] = static_cast<int64_t>(map2Key.length());

    type2MapBatch.numElements = 1;
    type2MapBatch.offsets[0] = 0;
    type2MapBatch.offsets[1] = 1;

    type1LongBatch.numElements = 1;
    type1LongBatch.data[0] = 0;

    type0StructBatch.numElements = 1;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    ReaderOptions readerOptions;
    readerOptions.setMemoryPool(*pool);
    return createReader(std::move(inStream), readerOptions);
  }

  TEST(TestReadIntent, testMapAll) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedMapMemReader(memStream);

    // select all of single_map.
    verifySelection(reader, {{2, ReadIntent_ALL}}, {0, 2, 3, 4});
  }

  TEST(TestReadIntent, testMapOffsets) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedMapMemReader(memStream);

    // select only the offsets of single_map.
    verifySelection(reader, {{2, ReadIntent_OFFSETS}}, {0, 2});

    // select only the offsets of single_map and the outermost offsets of nested_map.
    verifySelection(reader, {{2, ReadIntent_OFFSETS}, {5, ReadIntent_OFFSETS}}, {0, 2, 5});

    // select the entire offsets of nested_map without the map items of the innermost map.
    verifySelection(reader, {{5, ReadIntent_OFFSETS}, {9, ReadIntent_OFFSETS}}, {0, 5, 7, 9});
  }

  TEST(TestReadIntent, testMapAllAndOffsets) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedMapMemReader(memStream);

    // select all of single_map and only the outermost offsets of nested_map.
    verifySelection(reader, {{2, ReadIntent_ALL}, {5, ReadIntent_OFFSETS}}, {0, 2, 3, 4, 5});
  }

  TEST(TestReadIntent, testMapConflictingIntent) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedMapMemReader(memStream);

    // test conflicting ReadIntent on nested_map.
    verifySelection(reader, {{5, ReadIntent_OFFSETS}, {9, ReadIntent_ALL}}, {0, 5, 7, 9, 10, 11});
    verifySelection(reader, {{5, ReadIntent_ALL}, {9, ReadIntent_OFFSETS}},
                    {0, 5, 6, 7, 8, 9, 10, 11});
    verifySelection(reader, {{5, ReadIntent_OFFSETS}, {7, ReadIntent_ALL}, {9, ReadIntent_OFFSETS}},
                    {0, 5, 7, 8, 9, 10, 11});
  }

  TEST(TestReadIntent, testMapRowBatchContent) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedMapMemReader(memStream);

    // select all of single_map and only the offsets of nested_map.
    RowReaderOptions::IdReadIntentMap idReadIntentMap = {{2, ReadIntent_ALL},
                                                         {5, ReadIntent_OFFSETS}};
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.includeTypesWithIntents(idReadIntentMap);
    std::unique_ptr<RowReader> rowReader = reader->createRowReader(rowReaderOpts);

    // Read a row batch.
    std::unique_ptr<ColumnVectorBatch> batch = rowReader->createRowBatch(1024);
    EXPECT_TRUE(rowReader->next(*batch));
    EXPECT_EQ(1, batch->numElements);
    auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);

    // verify content of single_map selection.
    auto& mapBatch = dynamic_cast<MapVectorBatch&>(*structBatch.fields[0]);
    auto& keyBatch = dynamic_cast<StringVectorBatch&>(*mapBatch.keys);
    auto& valueBatch = dynamic_cast<StringVectorBatch&>(*mapBatch.elements);
    EXPECT_EQ(1, mapBatch.numElements);
    EXPECT_NE(nullptr, mapBatch.offsets.data());
    EXPECT_EQ(0, mapBatch.offsets.data()[0]);
    EXPECT_EQ(1, mapBatch.offsets.data()[1]);
    // verify key content.
    EXPECT_EQ(1, keyBatch.numElements);
    EXPECT_NE(nullptr, keyBatch.length.data());
    EXPECT_NE(nullptr, keyBatch.data.data());
    EXPECT_EQ(2, keyBatch.length.data()[0]);
    EXPECT_EQ(0, strncmp("k0", keyBatch.data.data()[0], 2));
    // verify value content.
    EXPECT_EQ(1, valueBatch.numElements);
    EXPECT_NE(nullptr, valueBatch.length.data());
    EXPECT_NE(nullptr, valueBatch.data.data());
    EXPECT_EQ(2, valueBatch.length.data()[0]);
    EXPECT_EQ(0, strncmp("v0", valueBatch.data.data()[0], 2));

    // verify content of nested_map selection.
    auto& nestedMapBatch = dynamic_cast<MapVectorBatch&>(*structBatch.fields[1]);
    EXPECT_EQ(1, nestedMapBatch.numElements);
    EXPECT_NE(nullptr, nestedMapBatch.offsets.data());
    EXPECT_EQ(0, nestedMapBatch.offsets.data()[0]);
    EXPECT_EQ(1, nestedMapBatch.offsets.data()[1]);
    EXPECT_EQ(nullptr, nestedMapBatch.keys);
    EXPECT_EQ(nullptr, nestedMapBatch.elements);
  }

  std::unique_ptr<Reader> createNestedUnionMemReader(MemoryOutputStream& memStream) {
    MemoryPool* pool = getDefaultPool();

    auto type = std::unique_ptr<Type>(
        Type::buildTypeFromString("struct<"
                                  "id:int,"
                                  "single_union:uniontype<int,string>,"
                                  "nested_union:uniontype<uniontype<int,uniontype<int,string>>,int>"
                                  ">"));
    WriterOptions options;
    options.setStripeSize(1024 * 1024)
        .setCompressionBlockSize(1024)
        .setCompression(CompressionKind_NONE)
        .setMemoryPool(pool)
        .setRowIndexStride(1000);

    auto writer = createWriter(*type, &memStream, options);
    auto batch = writer->createRowBatch(100);
    auto& type0StructBatch = dynamic_cast<StructVectorBatch&>(*batch);
    auto& type1LongBatch = dynamic_cast<LongVectorBatch&>(*type0StructBatch.fields[0]);
    auto& type2UnionBatch = dynamic_cast<UnionVectorBatch&>(*type0StructBatch.fields[1]);
    auto& type3LongBatch = dynamic_cast<LongVectorBatch&>(*type2UnionBatch.children[0]);
    auto& type4StringBatch = dynamic_cast<StringVectorBatch&>(*type2UnionBatch.children[1]);
    auto& type5UnionBatch = dynamic_cast<UnionVectorBatch&>(*type0StructBatch.fields[2]);
    auto& type6UnionBatch = dynamic_cast<UnionVectorBatch&>(*type5UnionBatch.children[0]);
    auto& type7LongBatch = dynamic_cast<LongVectorBatch&>(*type6UnionBatch.children[0]);
    auto& type8UnionBatch = dynamic_cast<UnionVectorBatch&>(*type6UnionBatch.children[1]);
    auto& type9LongBatch = dynamic_cast<LongVectorBatch&>(*type8UnionBatch.children[0]);
    auto& type10StringBatch = dynamic_cast<StringVectorBatch&>(*type8UnionBatch.children[1]);
    auto& type11LongBatch = dynamic_cast<LongVectorBatch&>(*type5UnionBatch.children[1]);

    std::string string4Element = "s1";
    std::string string10Element = "n1";

    // first row
    type1LongBatch.data[0] = 0;
    type2UnionBatch.tags[0] = 0;
    type2UnionBatch.offsets[0] = 0;
    type3LongBatch.data[0] = 0;
    type5UnionBatch.tags[0] = 0;
    type5UnionBatch.offsets[0] = 0;
    type6UnionBatch.tags[0] = 1;
    type6UnionBatch.offsets[0] = 0;
    type8UnionBatch.tags[0] = 0;
    type8UnionBatch.offsets[0] = 0;
    type9LongBatch.data[0] = 1;

    // second row
    type1LongBatch.data[1] = 1;
    type2UnionBatch.tags[1] = 1;
    type2UnionBatch.offsets[1] = 0;
    type4StringBatch.data[0] = const_cast<char*>(string4Element.c_str());
    type4StringBatch.length[0] = static_cast<int64_t>(string4Element.length());
    type5UnionBatch.tags[1] = 0;
    type5UnionBatch.offsets[1] = 1;
    type6UnionBatch.tags[1] = 1;
    type6UnionBatch.offsets[1] = 1;
    type8UnionBatch.tags[1] = 1;
    type8UnionBatch.offsets[1] = 0;
    type10StringBatch.data[0] = const_cast<char*>(string10Element.c_str());
    type10StringBatch.length[0] = static_cast<int64_t>(string10Element.length());

    // update numElements
    type11LongBatch.numElements = 0;
    type10StringBatch.numElements = 1;
    type9LongBatch.numElements = 1;
    type8UnionBatch.numElements = 2;
    type7LongBatch.numElements = 0;
    type6UnionBatch.numElements = 2;
    type5UnionBatch.numElements = 2;
    type4StringBatch.numElements = 1;
    type3LongBatch.numElements = 1;
    type2UnionBatch.numElements = 2;
    type1LongBatch.numElements = 2;
    type0StructBatch.numElements = 2;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    ReaderOptions readerOptions;
    readerOptions.setMemoryPool(*pool);
    readerOptions.setReaderMetrics(nullptr);
    return createReader(std::move(inStream), readerOptions);
  }

  TEST(TestReadIntent, testUnionAll) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedUnionMemReader(memStream);

    // select all of single_union.
    verifySelection(reader, {{2, ReadIntent_ALL}}, {0, 2, 3, 4});
  }

  TEST(TestReadIntent, testUnionOffsets) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedUnionMemReader(memStream);

    // select only the offsets of single_union.
    verifySelection(reader, {{2, ReadIntent_OFFSETS}}, {0, 2});

    // select only the offsets of single_union and the outermost offsets of nested_union.
    verifySelection(reader, {{2, ReadIntent_OFFSETS}, {5, ReadIntent_OFFSETS}}, {0, 2, 5});

    // select only the offsets of single_union and the innermost offsets of nested_union.
    verifySelection(reader, {{2, ReadIntent_OFFSETS}, {8, ReadIntent_OFFSETS}},
                    {0, 2, 5, 6, 7, 8, 11});
  }

  TEST(TestReadIntent, testUnionAllAndOffsets) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedUnionMemReader(memStream);

    // select all of single_union and only the outermost offsets of nested_union.
    verifySelection(reader, {{2, ReadIntent_ALL}, {5, ReadIntent_OFFSETS}}, {0, 2, 3, 4, 5});
  }

  TEST(TestReadIntent, testUnionConflictingIntent) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedUnionMemReader(memStream);

    // test conflicting ReadIntent on nested_union.
    verifySelection(reader, {{5, ReadIntent_OFFSETS}, {8, ReadIntent_ALL}},
                    {0, 5, 6, 7, 8, 9, 10, 11});
    verifySelection(reader, {{5, ReadIntent_ALL}, {8, ReadIntent_OFFSETS}},
                    {0, 5, 6, 7, 8, 9, 10, 11});
    verifySelection(reader, {{5, ReadIntent_OFFSETS}, {6, ReadIntent_ALL}, {8, ReadIntent_OFFSETS}},
                    {0, 5, 6, 7, 8, 9, 10, 11});
  }

  TEST(TestReadIntent, testUnionRowBatchContent) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Reader> reader = createNestedUnionMemReader(memStream);

    // select all of single_union and only the offsets of nested_union.
    RowReaderOptions::IdReadIntentMap idReadIntentMap = {{2, ReadIntent_ALL},
                                                         {5, ReadIntent_OFFSETS}};
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.includeTypesWithIntents(idReadIntentMap);
    std::unique_ptr<RowReader> rowReader = reader->createRowReader(rowReaderOpts);

    // Read a row batch.
    std::unique_ptr<ColumnVectorBatch> batch = rowReader->createRowBatch(1024);
    EXPECT_TRUE(rowReader->next(*batch));
    EXPECT_EQ(2, batch->numElements);
    auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);

    // verify content of single_union selection.
    auto& unionBatch = dynamic_cast<UnionVectorBatch&>(*structBatch.fields[0]);
    EXPECT_EQ(2, unionBatch.numElements);
    EXPECT_EQ(2, unionBatch.children.size());
    auto& longBatch = dynamic_cast<LongVectorBatch&>(*unionBatch.children[0]);
    auto& stringBatch = dynamic_cast<StringVectorBatch&>(*unionBatch.children[1]);
    EXPECT_EQ(1, longBatch.numElements);
    EXPECT_EQ(1, stringBatch.numElements);
    EXPECT_NE(nullptr, unionBatch.tags.data());
    EXPECT_NE(nullptr, unionBatch.offsets.data());
    EXPECT_NE(nullptr, longBatch.data.data());
    EXPECT_NE(nullptr, stringBatch.length.data());
    // verify content of the first row.
    EXPECT_EQ(0, unionBatch.tags.data()[0]);
    EXPECT_EQ(0, unionBatch.offsets.data()[0]);
    EXPECT_EQ(0, longBatch.data.data()[0]);
    // verify content of the second row.
    EXPECT_EQ(1, unionBatch.tags.data()[1]);
    EXPECT_EQ(0, unionBatch.offsets.data()[1]);
    EXPECT_EQ(2, stringBatch.length.data()[0]);
    EXPECT_EQ(0, strncmp("s1", stringBatch.data.data()[0], 2));

    // verify content of nested_union selection.
    auto& nestedUnionBatch = dynamic_cast<UnionVectorBatch&>(*structBatch.fields[1]);
    EXPECT_EQ(2, nestedUnionBatch.numElements);
    EXPECT_EQ(0, nestedUnionBatch.children.size());
    EXPECT_NE(nullptr, nestedUnionBatch.tags.data());
    EXPECT_NE(nullptr, nestedUnionBatch.offsets.data());
    // verify that tags and offsets are still read.
    EXPECT_EQ(0, nestedUnionBatch.tags.data()[0]);
    EXPECT_EQ(0, nestedUnionBatch.tags.data()[1]);
    EXPECT_EQ(0, nestedUnionBatch.offsets.data()[0]);
    EXPECT_EQ(1, nestedUnionBatch.offsets.data()[1]);
  }

  TEST(TestReadIntent, testSeekOverEmptyPresentStream) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    uint64_t rowCount = 5000;
    {
      auto type = std::unique_ptr<Type>(
          Type::buildTypeFromString("struct<col1:struct<col2:int>,col3:struct<col4:int>,"
                                    "col5:array<int>,col6:map<int,int>>"));
      WriterOptions options;
      options.setStripeSize(1024 * 1024)
          .setCompressionBlockSize(1024)
          .setCompression(CompressionKind_NONE)
          .setMemoryPool(pool)
          .setRowIndexStride(1000);

      // the child columns of the col3,col5,col6 have the empty present stream
      auto writer = createWriter(*type, &memStream, options);
      auto batch = writer->createRowBatch(rowCount);
      auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
      auto& structBatch1 = dynamic_cast<StructVectorBatch&>(*structBatch.fields[0]);
      auto& structBatch2 = dynamic_cast<StructVectorBatch&>(*structBatch.fields[1]);
      auto& listBatch = dynamic_cast<ListVectorBatch&>(*structBatch.fields[2]);
      auto& mapBatch = dynamic_cast<MapVectorBatch&>(*structBatch.fields[3]);

      auto& longBatch1 = dynamic_cast<LongVectorBatch&>(*structBatch1.fields[0]);
      auto& longBatch2 = dynamic_cast<LongVectorBatch&>(*structBatch2.fields[0]);
      auto& longBatch3 = dynamic_cast<LongVectorBatch&>(*listBatch.elements);
      auto& longKeyBatch = dynamic_cast<LongVectorBatch&>(*mapBatch.keys);
      auto& longValueBatch = dynamic_cast<LongVectorBatch&>(*mapBatch.elements);

      structBatch.numElements = rowCount;
      structBatch1.numElements = rowCount;
      structBatch2.numElements = rowCount;
      listBatch.numElements = rowCount;
      mapBatch.numElements = rowCount;
      longBatch1.numElements = rowCount;
      longBatch2.numElements = rowCount;
      longBatch3.numElements = rowCount;
      longKeyBatch.numElements = rowCount;
      longValueBatch.numElements = rowCount;

      structBatch1.hasNulls = false;
      structBatch2.hasNulls = true;
      listBatch.hasNulls = true;
      mapBatch.hasNulls = true;
      longBatch1.hasNulls = false;
      longBatch2.hasNulls = true;
      longBatch3.hasNulls = true;
      longKeyBatch.hasNulls = true;
      longValueBatch.hasNulls = true;
      for (uint64_t i = 0; i < rowCount; ++i) {
        longBatch1.data[i] = static_cast<int64_t>(i);
        longBatch1.notNull[i] = 1;

        structBatch2.notNull[i] = 0;
        listBatch.notNull[i] = 0;
        listBatch.offsets[i] = 0;
        mapBatch.notNull[i] = 0;
        longBatch2.notNull[i] = 0;
        longBatch3.notNull[i] = 0;
        longKeyBatch.notNull[i] = 0;
        longValueBatch.notNull[i] = 0;
      }
      writer->add(*batch);
      writer->close();
    }
    {
      auto inStream =
          std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
      ReaderOptions readerOptions;
      readerOptions.setMemoryPool(*pool);
      std::unique_ptr<Reader> reader = createReader(std::move(inStream), readerOptions);
      EXPECT_EQ(rowCount, reader->getNumberOfRows());
      std::unique_ptr<RowReader> rowReader = reader->createRowReader(RowReaderOptions());
      auto batch = rowReader->createRowBatch(1000);
      // seek over the empty present stream
      rowReader->seekToRow(2000);
      EXPECT_TRUE(rowReader->next(*batch));
      EXPECT_EQ(1000, batch->numElements);
      auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
      auto& structBatch1 = dynamic_cast<StructVectorBatch&>(*structBatch.fields[0]);
      auto& structBatch2 = dynamic_cast<StructVectorBatch&>(*structBatch.fields[1]);
      auto& listBatch = dynamic_cast<ListVectorBatch&>(*structBatch.fields[2]);
      auto& mapBatch = dynamic_cast<MapVectorBatch&>(*structBatch.fields[3]);

      auto& longBatch1 = dynamic_cast<LongVectorBatch&>(*structBatch1.fields[0]);
      for (uint64_t i = 0; i < 1000; ++i) {
        EXPECT_EQ(longBatch1.data[i], static_cast<int64_t>(i + 2000));
        EXPECT_TRUE(longBatch1.notNull[i]);
        EXPECT_FALSE(structBatch2.notNull[i]);
        EXPECT_FALSE(listBatch.notNull[i]);
        EXPECT_FALSE(mapBatch.notNull[i]);
      }
    }
  }
}  // namespace orc
