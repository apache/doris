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

#include "orc/ColumnPrinter.hh"
#include "orc/OrcFile.hh"

#include "Adaptor.hh"
#include "ToolTest.hh"
#include "gzip.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

#ifdef __clang__
DIAGNOSTIC_IGNORE("-Wmissing-variable-declarations")
#endif

namespace orc {

  class OrcFileDescription {
   public:
    std::string filename;
    std::string json;
    std::string typeString;
    std::string formatVersion;
    std::string softwareVersion;
    uint64_t rowCount;
    uint64_t contentLength;
    uint64_t stripeCount;
    CompressionKind compression;
    size_t compressionSize;
    uint64_t rowIndexStride;
    std::map<std::string, std::string> userMeta;

    OrcFileDescription(const std::string& _filename, const std::string& _json,
                       const std::string& _typeString, const std::string& _version,
                       const std::string& _softwareVersion, uint64_t _rowCount,
                       uint64_t _contentLength, uint64_t _stripeCount, CompressionKind _compression,
                       size_t _compressionSize, uint64_t _rowIndexStride,
                       const std::map<std::string, std::string>& _meta)
        : filename(_filename),
          json(_json),
          typeString(_typeString),
          formatVersion(_version),
          softwareVersion(_softwareVersion),
          rowCount(_rowCount),
          contentLength(_contentLength),
          stripeCount(_stripeCount),
          compression(_compression),
          compressionSize(_compressionSize),
          rowIndexStride(_rowIndexStride),
          userMeta(_meta) {
      // PASS
    }

    friend std::ostream& operator<<(std::ostream& stream, OrcFileDescription const& obj);
  };

  std::ostream& operator<<(std::ostream& stream, OrcFileDescription const& obj) {
    stream << obj.filename;
    return stream;
  }

  class FileParam : public testing::TestWithParam<OrcFileDescription> {
   public:
    ~FileParam() override;

    std::string getFilename() {
      return findExample(GetParam().filename);
    }

    std::string getJsonFilename() {
      return findExample("expected/" + GetParam().json);
    }
  };

  FileParam::~FileParam() {
    // PASS
  }

  TEST_P(FileParam, Metadata) {
    orc::ReaderOptions readerOpts;
    std::unique_ptr<Reader> reader =
        createReader(readLocalFile(getFilename(), readerOpts.getReaderMetrics()), readerOpts);
    std::unique_ptr<RowReader> rowReader = reader->createRowReader();

    EXPECT_EQ(GetParam().compression, reader->getCompression());
    EXPECT_EQ(GetParam().compressionSize, reader->getCompressionSize());
    EXPECT_EQ(GetParam().stripeCount, reader->getNumberOfStripes());
    EXPECT_EQ(GetParam().rowCount, reader->getNumberOfRows());
    EXPECT_EQ(GetParam().rowIndexStride, reader->getRowIndexStride());
    EXPECT_EQ(GetParam().contentLength, reader->getContentLength());
    EXPECT_EQ(GetParam().formatVersion, reader->getFormatVersion().toString());
    EXPECT_EQ(GetParam().softwareVersion, reader->getSoftwareVersion());
    EXPECT_EQ(getFilename(), reader->getStreamName());
    EXPECT_EQ(GetParam().userMeta.size(), reader->getMetadataKeys().size());
    for (std::map<std::string, std::string>::const_iterator itr = GetParam().userMeta.begin();
         itr != GetParam().userMeta.end(); ++itr) {
      ASSERT_EQ(true, reader->hasMetadataValue(itr->first));
      std::string val = reader->getMetadataValue(itr->first);
      EXPECT_EQ(itr->second, val);
    }
    EXPECT_EQ(true, !reader->hasMetadataValue("foo"));
    EXPECT_EQ(18446744073709551615UL, rowReader->getRowNumber());

    EXPECT_EQ(GetParam().typeString, reader->getType().toString());
  }

  TEST_P(FileParam, Contents) {
    orc::ReaderOptions readerOpts;
    std::unique_ptr<RowReader> rowReader =
        createReader(readLocalFile(getFilename(), readerOpts.getReaderMetrics()), readerOpts)
            ->createRowReader();

    unsigned long rowCount = 0;
    std::unique_ptr<ColumnVectorBatch> batch = rowReader->createRowBatch(1024);
    std::string line;
    std::unique_ptr<orc::ColumnPrinter> printer =
        orc::createColumnPrinter(line, &rowReader->getSelectedType());
    GzipTextReader expected(getJsonFilename());
    std::string expectedLine;
    while (rowReader->next(*batch)) {
      EXPECT_EQ(rowCount, rowReader->getRowNumber());
      printer->reset(*batch);
      for (size_t i = 0; i < batch->numElements; ++i) {
        ASSERT_EQ(true, expected.nextLine(expectedLine));
        line.clear();
        printer->printRow(i);
        EXPECT_EQ(expectedLine, line) << "wrong output at row " << (rowCount + i);
      }
      rowCount += batch->numElements;
    }
    EXPECT_EQ(GetParam().rowCount, rowCount);
    EXPECT_EQ(GetParam().rowCount, rowReader->getRowNumber());
  }

  std::map<std::string, std::string> makeMetadata();

  INSTANTIATE_TEST_SUITE_P(
      TestMatchParam, FileParam,
      testing::Values(
          OrcFileDescription("TestOrcFile.columnProjection.orc",
                             "TestOrcFile.columnProjection.jsn.gz",
                             "struct<int1:int,string1:string>", "0.12", "ORC Java", 21000, 428406,
                             5, CompressionKind_NONE, 262144, 1000,
                             std::map<std::string, std::string>()),
          OrcFileDescription("TestOrcFile.emptyFile.orc", "TestOrcFile.emptyFile.jsn.gz",
                             "struct<boolean1:boolean,byte1:tinyint,"
                             "short1:smallint,int1:int,long1:bigint,"
                             "float1:float,double1:double,"
                             "bytes1:binary,string1:string,"
                             "middle:struct<list:array<struct<"
                             "int1:int,string1:string>>>,"
                             "list:array<struct<int1:int,string1:"
                             "string>>,map:map<string,struct<int1:"
                             "int,string1:string>>>",
                             "0.12", "ORC Java", 0, 3, 0, CompressionKind_NONE, 262144, 10000,
                             std::map<std::string, std::string>()),
          OrcFileDescription("TestOrcFile.metaData.orc", "TestOrcFile.metaData.jsn.gz",
                             "struct<boolean1:boolean,byte1:tinyint,"
                             "short1:smallint,int1:int,long1:bigint,"
                             "float1:float,double1:double,"
                             "bytes1:binary,string1:string,"
                             "middle:struct<list:array<struct<"
                             "int1:int,string1:string>>>,"
                             "list:array<struct<int1:int,string1:"
                             "string>>,map:map<string,struct<int1:"
                             "int,string1:string>>>",
                             "0.12", "ORC Java", 1, 980, 1, CompressionKind_NONE, 262144, 10000,
                             makeMetadata()),
          OrcFileDescription("TestOrcFile.test1.orc", "TestOrcFile.test1.jsn.gz",
                             "struct<boolean1:boolean,byte1:tinyint,"
                             "short1:smallint,int1:int,long1:bigint,"
                             "float1:float,double1:double,"
                             "bytes1:binary,string1:string,"
                             "middle:struct<list:array<struct<"
                             "int1:int,string1:string>>>,"
                             "list:array<struct<int1:int,string1:"
                             "string>>,map:map<string,struct<int1:"
                             "int,string1:string>>>",
                             "0.12", "ORC Java", 2, 1015, 1, CompressionKind_ZLIB, 10000, 10000,
                             std::map<std::string, std::string>()),
          OrcFileDescription("TestOrcFile.testMemoryManagementV11"
                             ".orc",
                             "TestOrcFile.testMemoryManagementV11"
                             ".jsn.gz",
                             "struct<int1:int,string1:string>", "0.11", "ORC Java", 2500, 18779, 25,
                             CompressionKind_NONE, 262144, 0, std::map<std::string, std::string>()),
          OrcFileDescription("TestOrcFile.testMemoryManagementV12"
                             ".orc",
                             "TestOrcFile.testMemoryManagementV12"
                             ".jsn.gz",
                             "struct<int1:int,string1:string>", "0.12", "ORC Java", 2500, 10618, 4,
                             CompressionKind_NONE, 262144, 0, std::map<std::string, std::string>()),
          OrcFileDescription("TestOrcFile.testPredicatePushdown.orc",
                             "TestOrcFile.testPredicatePushdown"
                             ".jsn.gz",
                             "struct<int1:int,string1:string>", "0.12", "ORC Java", 3500, 15529, 1,
                             CompressionKind_NONE, 262144, 1000,
                             std::map<std::string, std::string>()),
          OrcFileDescription("TestOrcFile.testSeek.orc", "TestOrcFile.testSeek.jsn.gz",
                             "struct<boolean1:boolean,byte1:tinyint,"
                             "short1:smallint,int1:int,long1:bigint,"
                             "float1:float,double1:double,bytes1:"
                             "binary,string1:string,middle:struct<"
                             "list:array<struct<int1:int,string1:"
                             "string>>>,list:array<struct<int1:int,"
                             "string1:string>>,map:map<string,"
                             "struct<int1:int,string1:string>>>",
                             "0.12", "ORC Java", 32768, 1896379, 7, CompressionKind_ZLIB, 65536,
                             1000, std::map<std::string, std::string>()),
          OrcFileDescription("TestOrcFile.testSnappy.orc", "TestOrcFile.testSnappy.jsn.gz",
                             "struct<int1:int,string1:string>", "0.12", "ORC Java", 10000, 126061,
                             2, CompressionKind_SNAPPY, 100, 10000,
                             std::map<std::string, std::string>()),
          OrcFileDescription("TestOrcFile.testStringAndBinaryStat"
                             "istics.orc",
                             "TestOrcFile.testStringAndBinaryStat"
                             "istics.jsn.gz",
                             "struct<bytes1:binary,string1:string>", "0.12", "ORC Java", 4, 185, 1,
                             CompressionKind_ZLIB, 10000, 10000,
                             std::map<std::string, std::string>()),
          OrcFileDescription("TestOrcFile.testStripeLevelStats.orc",
                             "TestOrcFile.testStripeLevelStats"
                             ".jsn.gz",
                             "struct<int1:int,string1:string>", "0.12", "ORC Java", 11000, 597, 3,
                             CompressionKind_ZLIB, 10000, 10000,
                             std::map<std::string, std::string>()),
          OrcFileDescription("TestOrcFile.testTimestamp.orc", "TestOrcFile.testTimestamp.jsn.gz",
                             "timestamp", "0.11", "ORC Java", 12, 188, 1, CompressionKind_ZLIB,
                             10000, 10000, std::map<std::string, std::string>()),
          OrcFileDescription("TestOrcFile.testUnionAndTimestamp.orc",
                             "TestOrcFile.testUnionAndTimestamp"
                             ".jsn.gz",
                             "struct<time:timestamp,union:uniontype"
                             "<int,string>,decimal:decimal(38,18)>",
                             "0.12", "ORC Java", 5077, 20906, 2, CompressionKind_NONE, 262144,
                             10000, std::map<std::string, std::string>()),
          OrcFileDescription("TestOrcFile.testWithoutIndex.orc",
                             "TestOrcFile.testWithoutIndex.jsn.gz",
                             "struct<int1:int,string1:string>", "0.12", "ORC Java", 50000, 214643,
                             10, CompressionKind_SNAPPY, 1000, 0,
                             std::map<std::string, std::string>()),
          OrcFileDescription("decimal.orc", "decimal.jsn.gz", "struct<_col0:decimal(10,5)>", "0.12",
                             "ORC Java", 6000, 16186, 1, CompressionKind_NONE, 262144, 10000,
                             std::map<std::string, std::string>()),
          OrcFileDescription("demo-11-none.orc", "demo-12-zlib.jsn.gz",
                             ("struct<_col0:int,_col1:string,"
                              "_col2:string,_col3:string,_col4:int,"
                              "_col5:string,_col6:int,_col7:int,"
                              "_col8:int>"),
                             "0.11", "ORC Java", 1920800, 5069718, 385, CompressionKind_NONE,
                             262144, 10000, std::map<std::string, std::string>()),
          OrcFileDescription("demo-11-zlib.orc", "demo-12-zlib.jsn.gz",
                             ("struct<_col0:int,_col1:string,"
                              "_col2:string,_col3:string,_col4:int,"
                              "_col5:string,_col6:int,_col7:int,"
                              "_col8:int>"),
                             "0.11", "ORC Java", 1920800, 396823, 385, CompressionKind_ZLIB, 262144,
                             10000, std::map<std::string, std::string>()),
          OrcFileDescription("demo-12-zlib.orc", "demo-12-zlib.jsn.gz",
                             ("struct<_col0:int,_col1:string,"
                              "_col2:string,_col3:string,_col4:int,"
                              "_col5:string,_col6:int,_col7:int,"
                              "_col8:int>"),
                             "0.12", "ORC Java", 1920800, 45592, 1, CompressionKind_ZLIB, 262144,
                             10000, std::map<std::string, std::string>()),
          OrcFileDescription("nulls-at-end-snappy.orc", "nulls-at-end-snappy.jsn.gz",
                             ("struct<_col0:tinyint,_col1:smallint,"
                              "_col2:int,_col3:bigint,_col4:float,"
                              "_col5:double,_col6:boolean>"),
                             "0.12", "ORC Java", 70000, 366347, 1, CompressionKind_SNAPPY, 262144,
                             10000, std::map<std::string, std::string>()),
          OrcFileDescription("orc-file-11-format.orc", "orc-file-11-format.jsn.gz",
                             ("struct<boolean1:boolean,"
                              "byte1:tinyint,short1:smallint,"
                              "int1:int,long1:bigint,float1:float,"
                              "double1:double,bytes1:binary,"
                              "string1:string,middle:struct<list:"
                              "array<struct<int1:int,"
                              "string1:string>>>,list:array<struct"
                              "<int1:int,string1:string>>,map:map"
                              "<string,struct<int1:int,string1:"
                              "string>>,ts:timestamp,"
                              "decimal1:decimal(0,0)>"),
                             "0.11", "ORC Java", 7500, 372542, 2, CompressionKind_NONE, 262144,
                             10000, std::map<std::string, std::string>()),
          OrcFileDescription("orc_split_elim_new.orc", "orc_split_elim_new.jsn.gz",
                             ("struct<userid:bigint,string1:string,"
                              "subtype:double,"
                              "decimal1:decimal(16,6),"
                              "ts:timestamp>"),
                             "0.12", "ORC Java 1.8.0-SNAPSHOT", 25000, 1980, 1,
                             CompressionKind_ZLIB, 262144, 10000,
                             std::map<std::string, std::string>()),
          OrcFileDescription("orc_split_elim_cpp.orc", "orc_split_elim_cpp.jsn.gz",
                             ("struct<userid:bigint,string1:string,"
                              "subtype:double,"
                              "decimal1:decimal(16,6),"
                              "ts:timestamp>"),
                             "0.12", "ORC C++ 1.8.0-SNAPSHOT", 25000, 2942, 1, CompressionKind_ZLIB,
                             65536, 10000, std::map<std::string, std::string>()),
          OrcFileDescription("orc_index_int_string.orc", "orc_index_int_string.jsn.gz",
                             ("struct<_col0:int,_col1:varchar(4)>"), "0.12", "ORC Java", 6000,
                             11280, 1, CompressionKind_ZLIB, 262144, 2000,
                             std::map<std::string, std::string>()),
          OrcFileDescription("over1k_bloom.orc", "over1k_bloom.jsn.gz",
                             "struct<_col0:tinyint,_col1:smallint,"
                             "_col2:int,_col3:bigint,_col4:float,"
                             "_col5:double,_col6:boolean,"
                             "_col7:string,_col8:timestamp,"
                             "_col9:decimal(4,2),_col10:binary>",
                             "0.12", "ORC Java", 2098, 41780, 2, CompressionKind_ZLIB, 262144,
                             10000, std::map<std::string, std::string>()),
          OrcFileDescription("TestVectorOrcFile.testLz4.orc", "TestVectorOrcFile.testLz4.jsn.gz",
                             "struct<x:bigint,y:int,z:bigint>", "0.12", "ORC Java", 10000, 120952,
                             2, CompressionKind_LZ4, 1000, 10000,
                             std::map<std::string, std::string>()),
          OrcFileDescription("TestVectorOrcFile.testLzo.orc", "TestVectorOrcFile.testLzo.jsn.gz",
                             "struct<x:bigint,y:int,z:bigint>", "0.12", "ORC Java", 10000, 120955,
                             2, CompressionKind_LZO, 1000, 10000,
                             std::map<std::string, std::string>())));

#ifdef HAS_PRE_1970
  INSTANTIATE_TEST_SUITE_P(TestMatch1900, FileParam,
                           testing::Values(OrcFileDescription(
                               "TestOrcFile.testDate1900.orc", "TestOrcFile.testDate1900.jsn.gz",
                               "struct<time:timestamp,date:date>", "0.12", "ORC Java", 70000, 30478,
                               8, CompressionKind_ZLIB, 10000, 10000,
                               std::map<std::string, std::string>())));
#endif

#ifdef HAS_POST_2038
  INSTANTIATE_TEST_SUITE_P(TestMatch2038, FileParam,
                           testing::Values(OrcFileDescription(
                               "TestOrcFile.testDate2038.orc", "TestOrcFile.testDate2038.jsn.gz",
                               "struct<time:timestamp,date:date>", "0.12", "ORC Java", 212000,
                               94762, 28, CompressionKind_ZLIB, 10000, 10000,
                               std::map<std::string, std::string>())));
#endif

  TEST(TestMatch, columnSelectionTest) {
    ReaderOptions readerOpts;
    RowReaderOptions rowReaderOpts;
    std::list<uint64_t> includes;
    for (uint64_t i = 0; i < 9; i += 2) {
      includes.push_back(i);
    }
    rowReaderOpts.include(includes);
    std::string filename = findExample("demo-11-none.orc");
    std::unique_ptr<Reader> reader =
        createReader(readLocalFile(filename, readerOpts.getReaderMetrics()), readerOpts);
    std::unique_ptr<RowReader> rowReader = reader->createRowReader(rowReaderOpts);

    EXPECT_EQ(CompressionKind_NONE, reader->getCompression());
    EXPECT_EQ(256 * 1024, reader->getCompressionSize());
    EXPECT_EQ(385, reader->getNumberOfStripes());
    EXPECT_EQ(1920800, reader->getNumberOfRows());
    EXPECT_EQ(10000, reader->getRowIndexStride());
    EXPECT_EQ(5069718, reader->getContentLength());
    EXPECT_EQ(filename, reader->getStreamName());
    EXPECT_THAT(reader->getMetadataKeys(), testing::IsEmpty());
    EXPECT_FALSE(reader->hasMetadataValue("foo"));
    EXPECT_EQ(18446744073709551615UL, rowReader->getRowNumber());

    const Type& rootType = reader->getType();
    EXPECT_EQ(0, rootType.getColumnId());
    EXPECT_EQ(STRUCT, rootType.getKind());
    ASSERT_EQ(9, rootType.getSubtypeCount());
    EXPECT_EQ("_col0", rootType.getFieldName(0));
    EXPECT_EQ("_col1", rootType.getFieldName(1));
    EXPECT_EQ("_col2", rootType.getFieldName(2));
    EXPECT_EQ("_col3", rootType.getFieldName(3));
    EXPECT_EQ("_col4", rootType.getFieldName(4));
    EXPECT_EQ("_col5", rootType.getFieldName(5));
    EXPECT_EQ("_col6", rootType.getFieldName(6));
    EXPECT_EQ("_col7", rootType.getFieldName(7));
    EXPECT_EQ("_col8", rootType.getFieldName(8));
    EXPECT_EQ(INT, rootType.getSubtype(0)->getKind());
    EXPECT_EQ(STRING, rootType.getSubtype(1)->getKind());
    EXPECT_EQ(STRING, rootType.getSubtype(2)->getKind());
    EXPECT_EQ(STRING, rootType.getSubtype(3)->getKind());
    EXPECT_EQ(INT, rootType.getSubtype(4)->getKind());
    EXPECT_EQ(STRING, rootType.getSubtype(5)->getKind());
    EXPECT_EQ(INT, rootType.getSubtype(6)->getKind());
    EXPECT_EQ(INT, rootType.getSubtype(7)->getKind());
    EXPECT_EQ(INT, rootType.getSubtype(8)->getKind());
    for (unsigned int i = 0; i < 9; ++i) {
      EXPECT_EQ(i + 1, rootType.getSubtype(i)->getColumnId()) << "fail on " << i;
    }

    const std::vector<bool> selected = rowReader->getSelectedColumns();
    EXPECT_EQ(true, selected[0]) << "fail on " << 0;
    for (size_t i = 1; i < 10; ++i) {
      EXPECT_EQ(i % 2 == 1 ? true : false, selected[i]) << "fail on " << i;
    }

    unsigned long rowCount = 0;
    std::unique_ptr<ColumnVectorBatch> batch = rowReader->createRowBatch(1024);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    ASSERT_TRUE(structBatch != nullptr);
    LongVectorBatch* longVector = dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);
    ASSERT_TRUE(longVector != nullptr);
    int64_t* idCol = longVector->data.data();
    while (rowReader->next(*batch)) {
      EXPECT_EQ(rowCount, rowReader->getRowNumber());
      for (unsigned int i = 0; i < batch->numElements; ++i) {
        EXPECT_EQ(rowCount + i + 1, idCol[i]) << "Bad id for " << i;
      }
      rowCount += batch->numElements;
    }
    EXPECT_EQ(1920800, rowCount);
    EXPECT_EQ(1920800, rowReader->getRowNumber());
  }

  TEST(TestMatch, stripeInformationTest) {
    ReaderOptions opts;
    std::string filename = findExample("demo-11-none.orc");
    std::unique_ptr<Reader> reader =
        createReader(readLocalFile(filename, opts.getReaderMetrics()), opts);

    EXPECT_EQ(385, reader->getNumberOfStripes());

    std::unique_ptr<StripeInformation> stripeInfo = reader->getStripe(7);
    EXPECT_EQ(92143, stripeInfo->getOffset());
    EXPECT_EQ(13176, stripeInfo->getLength());
    EXPECT_EQ(234, stripeInfo->getIndexLength());
    EXPECT_EQ(12673, stripeInfo->getDataLength());
    EXPECT_EQ(269, stripeInfo->getFooterLength());
    EXPECT_EQ(5000, stripeInfo->getNumberOfRows());
  }

  TEST(TestMatch, readRangeTest) {
    ReaderOptions opts;
    RowReaderOptions fullOpts, lastOpts, oobOpts, offsetOpts;
    // stripes[N-1]
    lastOpts.range(5067085, 1);
    // stripes[N]
    oobOpts.range(5067086, 4096);
    // stripes[7, 16]
    offsetOpts.range(80000, 130722);
    std::string filename = findExample("demo-11-none.orc");
    std::unique_ptr<Reader> reader =
        createReader(readLocalFile(filename, opts.getReaderMetrics()), opts);
    std::unique_ptr<RowReader> fullReader = reader->createRowReader(fullOpts);
    std::unique_ptr<RowReader> lastReader = reader->createRowReader(lastOpts);
    std::unique_ptr<RowReader> oobReader = reader->createRowReader(oobOpts);
    std::unique_ptr<RowReader> offsetReader = reader->createRowReader(offsetOpts);

    std::unique_ptr<ColumnVectorBatch> oobBatch = oobReader->createRowBatch(5000);
    EXPECT_FALSE(oobReader->next(*oobBatch));

    // advance fullReader to align with offsetReader
    std::unique_ptr<ColumnVectorBatch> fullBatch = fullReader->createRowBatch(5000);
    for (int i = 0; i < 7; ++i) {
      EXPECT_TRUE(fullReader->next(*fullBatch));
      EXPECT_EQ(5000, fullBatch->numElements);
    }

    StructVectorBatch* fullStructBatch = dynamic_cast<StructVectorBatch*>(fullBatch.get());
    ASSERT_TRUE(fullStructBatch != nullptr);
    LongVectorBatch* fullLongVector = dynamic_cast<LongVectorBatch*>(fullStructBatch->fields[0]);
    ASSERT_TRUE(fullLongVector != nullptr);
    int64_t* fullId = fullLongVector->data.data();

    std::unique_ptr<ColumnVectorBatch> offsetBatch = offsetReader->createRowBatch(5000);
    StructVectorBatch* offsetStructBatch = dynamic_cast<StructVectorBatch*>(offsetBatch.get());
    ASSERT_TRUE(offsetStructBatch != nullptr);
    LongVectorBatch* offsetLongVector =
        dynamic_cast<LongVectorBatch*>(offsetStructBatch->fields[0]);
    ASSERT_TRUE(offsetLongVector != nullptr);
    int64_t* offsetId = offsetLongVector->data.data();

    for (int i = 7; i < 17; ++i) {
      EXPECT_TRUE(fullReader->next(*fullBatch));
      EXPECT_TRUE(offsetReader->next(*offsetBatch));
      EXPECT_EQ(fullBatch->numElements, offsetBatch->numElements);
      for (unsigned j = 0; j < fullBatch->numElements; ++j) {
        EXPECT_EQ(fullId[j], offsetId[j]);
      }
    }
    EXPECT_FALSE(offsetReader->next(*offsetBatch));

    // advance fullReader to align with lastReader
    for (int i = 17; i < 384; ++i) {
      EXPECT_TRUE(fullReader->next(*fullBatch));
      EXPECT_EQ(5000, fullBatch->numElements);
    }

    std::unique_ptr<ColumnVectorBatch> lastBatch = lastReader->createRowBatch(5000);
    StructVectorBatch* lastStructBatch = dynamic_cast<StructVectorBatch*>(lastBatch.get());
    ASSERT_TRUE(lastStructBatch != nullptr);
    LongVectorBatch* lastLongVector = dynamic_cast<LongVectorBatch*>(lastStructBatch->fields[0]);
    ASSERT_TRUE(lastLongVector != nullptr);
    int64_t* lastId = lastLongVector->data.data();

    EXPECT_TRUE(fullReader->next(*fullBatch));
    EXPECT_TRUE(lastReader->next(*lastBatch));
    EXPECT_EQ(fullBatch->numElements, lastBatch->numElements);
    for (unsigned i = 0; i < fullBatch->numElements; ++i) {
      EXPECT_EQ(fullId[i], lastId[i]);
    }
    EXPECT_FALSE(fullReader->next(*fullBatch));
    EXPECT_FALSE(lastReader->next(*lastBatch));
  }

  TEST(TestMatch, columnStatistics) {
    orc::ReaderOptions opts;
    std::string filename = findExample("demo-11-none.orc");
    std::unique_ptr<orc::Reader> reader =
        orc::createReader(orc::readLocalFile(filename, opts.getReaderMetrics()), opts);

    // corrupt stats test
    EXPECT_EQ(true, reader->hasCorrectStatistics());

    // test column statistics
    std::unique_ptr<orc::Statistics> stats = reader->getStatistics();
    EXPECT_EQ(10, stats->getNumberOfColumns());

    // 6th real column, start from 1
    std::unique_ptr<orc::ColumnStatistics> col_6 = reader->getColumnStatistics(6);
    const orc::StringColumnStatistics* strStats =
        dynamic_cast<const orc::StringColumnStatistics*>(col_6.get());
    ASSERT_TRUE(strStats != nullptr);

    EXPECT_EQ("Good", strStats->getMinimum());
    EXPECT_EQ("Unknown", strStats->getMaximum());

    // 7th real column
    std::unique_ptr<orc::ColumnStatistics> col_7 = reader->getColumnStatistics(7);
    const orc::IntegerColumnStatistics* intStats =
        dynamic_cast<const orc::IntegerColumnStatistics*>(col_7.get());
    ASSERT_TRUE(intStats != nullptr);
    EXPECT_EQ(0, intStats->getMinimum());
    EXPECT_EQ(6, intStats->getMaximum());
    EXPECT_EQ(5762400, intStats->getSum());
  }

  TEST(TestMatch, stripeStatistics) {
    orc::ReaderOptions opts;
    std::string filename = findExample("demo-11-none.orc");
    std::unique_ptr<orc::Reader> reader =
        orc::createReader(orc::readLocalFile(filename, opts.getReaderMetrics()), opts);

    // test stripe statistics
    EXPECT_EQ(385, reader->getNumberOfStripeStatistics());

    // stripe[384]: 385th stripe, last stripe
    unsigned long stripeIdx = 384;
    std::unique_ptr<orc::Statistics> stripeStats = reader->getStripeStatistics(stripeIdx);
    EXPECT_EQ(10, stripeStats->getNumberOfColumns());

    // 6th real column
    const orc::StringColumnStatistics* col_6 =
        dynamic_cast<const orc::StringColumnStatistics*>(stripeStats->getColumnStatistics(6));
    ASSERT_TRUE(col_6 != nullptr);
    EXPECT_EQ("Unknown", col_6->getMinimum());
    EXPECT_EQ("Unknown", col_6->getMaximum());

    // 7th real column
    const orc::IntegerColumnStatistics* col_7 =
        dynamic_cast<const orc::IntegerColumnStatistics*>(stripeStats->getColumnStatistics(7));
    ASSERT_TRUE(col_7 != nullptr);
    EXPECT_EQ(6, col_7->getMinimum());
    EXPECT_EQ(6, col_7->getMaximum());
    EXPECT_EQ(4800, col_7->getSum());
  }

  TEST(TestMatch, corruptStatistics) {
    orc::ReaderOptions opts;
    // read the file has corrupt statistics
    std::string filename = findExample("orc_split_elim.orc");
    std::unique_ptr<orc::Reader> reader =
        orc::createReader(orc::readLocalFile(filename, opts.getReaderMetrics()), opts);

    EXPECT_EQ(true, !reader->hasCorrectStatistics());

    // 2nd real column, string
    std::unique_ptr<orc::ColumnStatistics> col_2 = reader->getColumnStatistics(2);
    const orc::StringColumnStatistics& strStats =
        dynamic_cast<const orc::StringColumnStatistics&>(*(col_2.get()));
    EXPECT_EQ(true, !strStats.hasMinimum());
    EXPECT_EQ(true, !strStats.hasMaximum());

    // stripe statistics
    unsigned long stripeIdx = 1;
    std::unique_ptr<orc::Statistics> stripeStats = reader->getStripeStatistics(stripeIdx);

    // 4th real column, Decimal
    const orc::DecimalColumnStatistics* col_4 =
        dynamic_cast<const orc::DecimalColumnStatistics*>(stripeStats->getColumnStatistics(4));
    ASSERT_TRUE(col_4 != nullptr);
    EXPECT_EQ(true, !col_4->hasMinimum());
    EXPECT_EQ(true, !col_4->hasMaximum());
  }

  TEST(TestMatch, noStripeStatistics) {
    orc::ReaderOptions opts;
    // read the file has no stripe statistics
    std::string filename = findExample("orc-file-11-format.orc");
    std::unique_ptr<orc::Reader> reader =
        orc::createReader(orc::readLocalFile(filename, opts.getReaderMetrics()), opts);

    EXPECT_EQ(0, reader->getNumberOfStripeStatistics());
  }

  TEST(TestMatch, seekToRow) {
    /* Test with a regular file */
    {
      orc::ReaderOptions readerOpts;
      std::string filename = findExample("demo-11-none.orc");
      std::unique_ptr<orc::Reader> reader = orc::createReader(
          orc::readLocalFile(filename, readerOpts.getReaderMetrics()), readerOpts);
      std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader();
      EXPECT_EQ(1920800, reader->getNumberOfRows());

      std::unique_ptr<orc::ColumnVectorBatch> batch =
          rowReader->createRowBatch(5000);  // Stripe size
      rowReader->next(*batch);
      EXPECT_EQ(5000, batch->numElements);
      EXPECT_EQ(0, rowReader->getRowNumber());

      // We only load data till the end of the current stripe
      rowReader->seekToRow(11000);
      rowReader->next(*batch);
      EXPECT_EQ(4000, batch->numElements);
      EXPECT_EQ(11000, rowReader->getRowNumber());

      // We only load data till the end of the current stripe
      rowReader->seekToRow(99999);
      rowReader->next(*batch);
      EXPECT_EQ(1, batch->numElements);
      EXPECT_EQ(99999, rowReader->getRowNumber());

      // Skip more rows than available
      rowReader->seekToRow(1920800);
      rowReader->next(*batch);
      EXPECT_EQ(0, batch->numElements);
      EXPECT_EQ(1920800, rowReader->getRowNumber());
    }

    /* Test with a portion of the file */
    {
      orc::ReaderOptions readerOpts;
      orc::RowReaderOptions rowReaderOpts;
      std::string filename = findExample("demo-11-none.orc");
      rowReaderOpts.range(13126, 13145);  // Read only the second stripe (rows 5000..9999)

      std::unique_ptr<orc::Reader> reader = orc::createReader(
          orc::readLocalFile(filename, readerOpts.getReaderMetrics()), readerOpts);
      std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader(rowReaderOpts);
      EXPECT_EQ(1920800, reader->getNumberOfRows());

      std::unique_ptr<orc::ColumnVectorBatch> batch =
          rowReader->createRowBatch(5000);  // Stripe size
      rowReader->next(*batch);
      EXPECT_EQ(5000, batch->numElements);

      rowReader->seekToRow(7000);
      rowReader->next(*batch);
      EXPECT_EQ(3000, batch->numElements);
      EXPECT_EQ(7000, rowReader->getRowNumber());

      rowReader->seekToRow(1000);
      rowReader->next(*batch);
      EXPECT_EQ(0, batch->numElements);
      EXPECT_EQ(10000, rowReader->getRowNumber());

      rowReader->seekToRow(11000);
      rowReader->next(*batch);
      EXPECT_EQ(0, batch->numElements);
      EXPECT_EQ(10000, rowReader->getRowNumber());
    }

    /* Test with an empty file */
    {
      orc::ReaderOptions readerOpts;
      std::string filename = findExample("TestOrcFile.emptyFile.orc");
      std::unique_ptr<orc::Reader> reader = orc::createReader(
          orc::readLocalFile(filename, readerOpts.getReaderMetrics()), readerOpts);
      std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader();
      EXPECT_EQ(0, reader->getNumberOfRows());

      std::unique_ptr<orc::ColumnVectorBatch> batch = rowReader->createRowBatch(5000);
      rowReader->next(*batch);
      EXPECT_EQ(0, batch->numElements);

      rowReader->seekToRow(0);
      rowReader->next(*batch);
      EXPECT_EQ(0, batch->numElements);
      EXPECT_EQ(0, rowReader->getRowNumber());

      rowReader->seekToRow(1);
      rowReader->next(*batch);
      EXPECT_EQ(0, batch->numElements);
      EXPECT_EQ(0, rowReader->getRowNumber());
    }
  }

  TEST(TestMatch, futureFormatVersion) {
    std::string filename = findExample("version1999.orc");
    orc::ReaderOptions opts;
    std::ostringstream errorMsg;
    opts.setErrorStream(errorMsg);
    std::unique_ptr<orc::Reader> reader =
        orc::createReader(orc::readLocalFile(filename, opts.getReaderMetrics()), opts);
    EXPECT_EQ(
        ("Warning: ORC file " + filename + " was written in an unknown format version 19.99\n"),
        errorMsg.str());
    EXPECT_EQ("19.99", reader->getFormatVersion().toString());
  }

  TEST(TestMatch, selectColumns) {
    orc::ReaderOptions readerOpts;
    orc::RowReaderOptions rowReaderOpts;
    std::string filename = findExample("TestOrcFile.testSeek.orc");

    // All columns
    std::unique_ptr<orc::Reader> reader =
        orc::createReader(orc::readLocalFile(filename, readerOpts.getReaderMetrics()), readerOpts);
    std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader(rowReaderOpts);
    std::vector<bool> c = rowReader->getSelectedColumns();
    EXPECT_EQ(24, c.size());
    for (unsigned int i = 0; i < c.size(); i++) {
      EXPECT_TRUE(c[i]);
    }
    std::unique_ptr<orc::ColumnVectorBatch> batch = rowReader->createRowBatch(1);
    std::string line;
    std::unique_ptr<orc::ColumnPrinter> printer =
        createColumnPrinter(line, &rowReader->getSelectedType());
    rowReader->next(*batch);
    printer->reset(*batch);
    printer->printRow(0);
    std::ostringstream expected;
    expected << "{\"boolean1\": true, \"byte1\": -76, "
             << "\"short1\": 21684, \"int1\": -941468492, "
             << "\"long1\": -6863419716327549772, \"float1\": 0.7762409, "
             << "\"double1\": 0.77624090391187, \"bytes1\": [123, 108, 207, 27, 93, "
             << "157, 139, 233, 181, 90, 14, 60, 34, 120, 26, 119, 231, 50, 155, 121], "
             << "\"string1\": \"887336a7\", \"middle\": {\"list\": [{\"int1\": "
             << "-941468492, \"string1\": \"887336a7\"}, {\"int1\": -1598014431, "
             << "\"string1\": \"ba419d35-x\"}]}, \"list\": [], \"map\": [{\"key\": "
             << "\"ba419d35-x\", \"value\": {\"int1\": -1598014431, \"string1\": "
             << "\"ba419d35-x\"}}, {\"key\": \"887336a7\", \"value\": {\"int1\": "
             << "-941468492, \"string1\": \"887336a7\"}}]}";
    EXPECT_EQ(expected.str(), line);

    // Int column #2
    std::list<uint64_t> cols;
    cols.push_back(1);
    rowReaderOpts.include(cols);
    rowReader = reader->createRowReader(rowReaderOpts);
    c = rowReader->getSelectedColumns();
    for (unsigned int i = 1; i < c.size(); i++) {
      if (i == 2)
        EXPECT_TRUE(c[i]);
      else
        EXPECT_TRUE(!c[i]);
    }
    batch = rowReader->createRowBatch(1);
    line.clear();
    printer = createColumnPrinter(line, &rowReader->getSelectedType());
    rowReader->next(*batch);
    printer->reset(*batch);
    printer->printRow(0);
    std::string expectedInt("{\"byte1\": -76}");
    EXPECT_EQ(expectedInt, line);

    // Struct column #10
    cols.clear();
    cols.push_back(9);
    rowReaderOpts.include(cols);
    rowReader = reader->createRowReader(rowReaderOpts);
    c = rowReader->getSelectedColumns();
    for (unsigned int i = 1; i < c.size(); i++) {
      if (i >= 10 && i <= 14)
        EXPECT_TRUE(c[i]);
      else
        EXPECT_TRUE(!c[i]);
    }
    batch = rowReader->createRowBatch(1);
    line.clear();
    printer = createColumnPrinter(line, &rowReader->getSelectedType());
    rowReader->next(*batch);
    printer->reset(*batch);
    printer->printRow(0);
    std::ostringstream expectedStruct;
    expectedStruct << "{\"middle\": {\"list\": "
                   << "[{\"int1\": -941468492, \"string1\": \"887336a7\"}, "
                   << "{\"int1\": -1598014431, \"string1\": \"ba419d35-x\"}]}}";
    EXPECT_EQ(expectedStruct.str(), line);

    // Array column #11
    cols.clear();
    cols.push_back(10);
    rowReaderOpts.include(cols);
    rowReader = reader->createRowReader(rowReaderOpts);
    c = rowReader->getSelectedColumns();
    for (unsigned int i = 1; i < c.size(); i++) {
      if (i >= 15 && i <= 18)
        EXPECT_TRUE(c[i]);
      else
        EXPECT_TRUE(!c[i]);
    }
    batch = rowReader->createRowBatch(1);
    line.clear();
    printer = createColumnPrinter(line, &rowReader->getSelectedType());
    rowReader->next(*batch);
    printer->reset(*batch);
    printer->printRow(0);
    std::string expectedArray("{\"list\": []}");
    EXPECT_EQ(expectedArray, line);

    // Map column #12
    cols.clear();
    cols.push_back(11);
    rowReaderOpts.include(cols);
    rowReader = reader->createRowReader(rowReaderOpts);
    c = rowReader->getSelectedColumns();
    for (unsigned int i = 1; i < c.size(); i++) {
      if (i >= 19 && i <= 23)
        EXPECT_TRUE(c[i]);
      else
        EXPECT_TRUE(!c[i]);
    }
    batch = rowReader->createRowBatch(1);
    line.clear();
    printer = createColumnPrinter(line, &rowReader->getSelectedType());
    rowReader->next(*batch);
    printer->reset(*batch);
    printer->printRow(0);
    std::ostringstream expectedMap;
    expectedMap << "{\"map\": [{\"key\": \"ba419d35-x\", \"value\": {\"int1\":"
                << " -1598014431, \"string1\": \"ba419d35-x\"}}, {\"key\": "
                << "\"887336a7\", \"value\": {\"int1\": -941468492, \"string1\": "
                << "\"887336a7\"}}]}";
    EXPECT_EQ(expectedMap.str(), line);

    // Map column #12
    // two subtypes with column id:
    // map<string(20),struct(21)<int1(22):int,string1(23):string>
    cols.clear();
    cols.push_back(20);
    cols.push_back(22);
    cols.push_back(23);
    rowReaderOpts.includeTypes(cols);
    rowReader = reader->createRowReader(rowReaderOpts);
    c = rowReader->getSelectedColumns();
    for (unsigned int i = 1; i < c.size(); i++) {
      if (i >= 19 && i <= 23)
        EXPECT_TRUE(c[i]);
      else
        EXPECT_TRUE(!c[i]);
    }
    batch = rowReader->createRowBatch(1);
    line.clear();
    printer = createColumnPrinter(line, &rowReader->getSelectedType());
    rowReader->next(*batch);
    printer->reset(*batch);
    printer->printRow(0);
    std::ostringstream expectedMapWithColumnId;
    expectedMapWithColumnId << "{\"map\": [{\"key\": \"ba419d35-x\", \"value\": {\"int1\":"
                            << " -1598014431, \"string1\": \"ba419d35-x\"}}, {\"key\": "
                            << "\"887336a7\", \"value\": {\"int1\": -941468492, \"string1\": "
                            << "\"887336a7\"}}]}";
    EXPECT_EQ(expectedMapWithColumnId.str(), line);

    // Map column #12 again, to test map key is automatically included
    // two subtypes with column id:
    // map<string(20),struct(21)<int1(22):int,string1(23):string>
    cols.clear();
    cols.push_back(22);
    cols.push_back(23);
    rowReaderOpts.includeTypes(cols);
    rowReader = reader->createRowReader(rowReaderOpts);
    c = rowReader->getSelectedColumns();
    for (unsigned int i = 1; i < c.size(); i++) {
      if (i == 19 || (i >= 21 && i <= 23))
        EXPECT_TRUE(c[i]);
      else
        EXPECT_TRUE(!c[i]);
    }
    batch = rowReader->createRowBatch(1);
    std::ostringstream expectedMapSchema;
    expectedMapSchema
        << "Struct vector <0 of 1; Map vector <key not selected, "
        << "Struct vector <0 of 1; Long vector <0 of 1>; Byte vector <0 of 1>; > with 0 of 1>; >";
    EXPECT_EQ(expectedMapSchema.str(), batch->toString());
    EXPECT_EQ(45, batch->getMemoryUsage());

    // Struct column #10, with field name: middle
    std::list<std::string> colNames;
    colNames.push_back("middle.list.int1");
    colNames.push_back("middle.list.string1");
    rowReaderOpts.include(colNames);
    rowReader = reader->createRowReader(rowReaderOpts);
    c = rowReader->getSelectedColumns();
    for (unsigned int i = 1; i < c.size(); i++) {
      if (i >= 10 && i <= 14)
        EXPECT_TRUE(c[i]);
      else
        EXPECT_TRUE(!c[i]);
    }
    batch = rowReader->createRowBatch(1);
    line.clear();
    printer = createColumnPrinter(line, &rowReader->getSelectedType());
    rowReader->next(*batch);
    printer->reset(*batch);
    printer->printRow(0);
    std::ostringstream expectedStructWithColumnName;
    expectedStructWithColumnName << "{\"middle\": {\"list\": "
                                 << "[{\"int1\": -941468492, \"string1\": \"887336a7\"}, "
                                 << "{\"int1\": -1598014431, \"string1\": \"ba419d35-x\"}]}}";
    EXPECT_EQ(expectedStructWithColumnName.str(), line);
  }

  TEST(Reader, memoryUse) {
    std::string filename = findExample("TestOrcFile.testSeek.orc");
    std::unique_ptr<orc::Reader> reader;
    std::unique_ptr<orc::RowReader> rowReader;
    std::unique_ptr<orc::ColumnVectorBatch> batch;
    orc::ReaderOptions readerOpts;
    orc::RowReaderOptions rowReaderOpts;
    std::list<uint64_t> cols;

    // Int column
    cols.push_back(1);
    rowReaderOpts.include(cols);
    reader =
        orc::createReader(orc::readLocalFile(filename, readerOpts.getReaderMetrics()), readerOpts);
    rowReader = reader->createRowReader(rowReaderOpts);
    EXPECT_EQ(483517, reader->getMemoryUseByFieldId(cols));
    batch = rowReader->createRowBatch(1);
    EXPECT_EQ(10, batch->getMemoryUsage());
    batch = rowReader->createRowBatch(1000);
    EXPECT_EQ(10000, batch->getMemoryUsage());
    EXPECT_FALSE(batch->hasVariableLength());

    // Binary column
    cols.clear();
    cols.push_back(7);
    rowReaderOpts.include(cols);
    reader =
        orc::createReader(orc::readLocalFile(filename, readerOpts.getReaderMetrics()), readerOpts);
    rowReader = reader->createRowReader(rowReaderOpts);
    EXPECT_EQ(835906, reader->getMemoryUseByFieldId(cols));
    batch = rowReader->createRowBatch(1);
    EXPECT_EQ(18, batch->getMemoryUsage());
    EXPECT_FALSE(batch->hasVariableLength());

    // String column
    cols.clear();
    cols.push_back(8);
    rowReaderOpts.include(cols);
    reader =
        orc::createReader(orc::readLocalFile(filename, readerOpts.getReaderMetrics()), readerOpts);
    rowReader = reader->createRowReader(rowReaderOpts);
    EXPECT_EQ(901442, reader->getMemoryUseByFieldId(cols));
    batch = rowReader->createRowBatch(1);
    EXPECT_EQ(18, batch->getMemoryUsage());
    EXPECT_FALSE(batch->hasVariableLength());

    // Struct column (with a List subcolumn)
    cols.clear();
    cols.push_back(9);
    rowReaderOpts.include(cols);
    reader =
        orc::createReader(orc::readLocalFile(filename, readerOpts.getReaderMetrics()), readerOpts);
    rowReader = reader->createRowReader(rowReaderOpts);
    EXPECT_EQ(1294658, reader->getMemoryUseByFieldId(cols));
    batch = rowReader->createRowBatch(1);
    EXPECT_EQ(46, batch->getMemoryUsage());
    EXPECT_TRUE(batch->hasVariableLength());

    // List column
    cols.clear();
    cols.push_back(10);
    rowReaderOpts.include(cols);
    reader =
        orc::createReader(orc::readLocalFile(filename, readerOpts.getReaderMetrics()), readerOpts);
    rowReader = reader->createRowReader(rowReaderOpts);
    EXPECT_EQ(1229122, reader->getMemoryUseByFieldId(cols));
    batch = rowReader->createRowBatch(1);
    EXPECT_EQ(45, batch->getMemoryUsage());
    EXPECT_TRUE(batch->hasVariableLength());

    // Map column
    cols.clear();
    cols.push_back(11);
    rowReaderOpts.include(cols);
    reader =
        orc::createReader(orc::readLocalFile(filename, readerOpts.getReaderMetrics()), readerOpts);
    rowReader = reader->createRowReader(rowReaderOpts);
    EXPECT_EQ(1491266, reader->getMemoryUseByFieldId(cols));
    batch = rowReader->createRowBatch(1);
    EXPECT_EQ(62, batch->getMemoryUsage());
    EXPECT_TRUE(batch->hasVariableLength());

    // All columns
    cols.clear();
    for (uint64_t c = 0; c < 12; ++c) {
      cols.push_back(c);
    }
    rowReaderOpts.include(cols);
    reader =
        orc::createReader(orc::readLocalFile(filename, readerOpts.getReaderMetrics()), readerOpts);
    rowReader = reader->createRowReader(rowReaderOpts);
    EXPECT_EQ(4112706, reader->getMemoryUseByFieldId(cols));
    batch = rowReader->createRowBatch(1);
    EXPECT_EQ(248, batch->getMemoryUsage());
    EXPECT_TRUE(batch->hasVariableLength());
  }

  std::map<std::string, std::string> makeMetadata() {
    std::map<std::string, std::string> result;
    result["my.meta"] = "\x01\x02\x03\x04\x05\x06\x07\xff\xfe\x7f\x80";
    result["clobber"] = "\x05\x07\x0b\x0d\x11\x13";
    const unsigned char buffer[] = {
        96,  180, 32,  187, 56,  81,  217, 212, 122, 203, 147, 61,  190, 112, 57,  155, 246, 201,
        45,  163, 58,  240, 29,  79,  183, 112, 233, 140, 3,   37,  244, 29,  62,  186, 248, 152,
        109, 167, 18,  200, 43,  205, 77,  85,  75,  240, 181, 64,  35,  194, 155, 98,  77,  233,
        239, 156, 47,  147, 30,  252, 88,  15,  154, 251, 8,   27,  18,  225, 7,   177, 232, 5,
        242, 180, 245, 240, 241, 208, 12,  45,  15,  98,  99,  70,  112, 146, 28,  80,  88,  103,
        255, 32,  246, 168, 51,  94,  152, 175, 135, 37,  56,  85,  134, 180, 31,  239, 242, 5,
        180, 224, 90,  0,   8,   35,  247, 139, 95,  143, 92,  2,   67,  156, 232, 246, 122, 120,
        29,  144, 203, 230, 191, 26,  231, 242, 188, 64,  164, 151, 9,   160, 108, 14,  49,  73,
        155, 240, 41,  105, 202, 66,  210, 3,   229, 102, 188, 198, 150, 222, 8,   250, 1,   2,
        160, 253, 46,  35,  48,  176, 150, 74,  187, 124, 68,  48,  32,  222, 28,  173, 9,   191,
        214, 56,  31,  251, 148, 218, 175, 187, 144, 196, 237, 145, 160, 97,  58,  209, 220, 75,
        71,  3,   175, 132, 193, 214, 59,  26,  135, 105, 33,  198, 213, 134, 157, 97,  204, 185,
        142, 209, 58,  230, 192, 154, 19,  252, 145, 225, 73,  34,  243, 1,   207, 139, 207, 147,
        67,  21,  166, 4,   157, 47,  7,   217, 131, 250, 169, 27,  143, 78,  114, 101, 236, 184,
        21,  167, 203, 171, 193, 69,  12,  183, 43,  60,  116, 16,  119, 23,  170, 36,  172, 66,
        242, 91,  108, 103, 132, 118, 125, 14,  53,  70,  196, 247, 37,  1,   145, 163, 182, 170,
        162, 182, 77,  18,  110, 85,  131, 176, 76,  17,  50,  89,  201, 72,  225, 208, 179, 155,
        185, 86,  12,  213, 64,  155, 110, 202, 254, 219, 200, 172, 175, 238, 167, 77,  183, 248,
        90,  223, 148, 190, 154, 133, 161, 221, 75,  3,   170, 136, 131, 29,  210, 156, 64,  120,
        129, 11,  58,  40,  210, 45,  102, 128, 182, 79,  203, 177, 178, 55,  194, 68,  18,  52,
        206, 171, 191, 218, 216, 124, 49,  21,  72,  246, 121, 2,   116, 185, 46,  106, 89,  29,
        58,  177, 166, 11,  115, 64,  11,  196, 116, 197, 45,  60,  188, 242, 251, 174, 114, 182,
        230, 212, 159, 176, 177, 133, 19,  54,  250, 44,  84,  12,  223, 191, 120, 200, 219, 73,
        44,  101, 231, 91,  1,   242, 86,  10,  157, 196, 86,  254, 164, 3,   66,  134, 86,  158,
        48,  134, 234, 100, 151, 36,  149, 156, 68,  8,   146, 218, 235, 114, 76,  6,   229, 19,
        58,  201, 170, 148, 16,  238, 186, 45,  84,  254, 138, 253, 248, 80,  125, 33,  19,  226,
        2,   106, 147, 122, 228, 57,  152, 43,  206, 121, 204, 36,  14,  38,  74,  246, 205, 67,
        204, 48,  37,  102, 107, 185, 179, 127, 129, 231, 20,  21,  103, 173, 104, 201, 130, 73,
        128, 219, 236, 205, 20,  219, 32,  168, 112, 88,  1,   113, 21,  182, 16,  82,  57,  102,
        75,  118, 131, 96,  156, 24,  178, 230, 82,  218, 235, 107, 179, 120, 187, 63,  120, 128,
        92,  187, 129, 182, 154, 249, 239, 253, 13,  66,  111, 145, 188, 18,  34,  248, 23,  137,
        195, 144, 219, 40,  101, 90,  30,  2,   29,  96,  158, 157, 25,  12,  222, 85,  168, 201,
        35,  237, 85,  18,  244, 72,  205, 0,   24,  151, 115, 7,   95,  78,  212, 28,  87,  207,
        209, 123, 139, 190, 52,  2,   21,  185, 209, 248, 226, 186, 91,  123, 35,  200, 54,  253,
        59,  89,  143, 89,  220, 21,  119, 196, 157, 151, 25,  184, 177, 147, 91,  128, 45,  32,
        152, 163, 112, 27,  106, 65,  71,  222, 102, 86,  183, 253, 36,  79,  11,  33,  22,  190,
        57,  233, 40,  217, 234, 84,  4,   186, 183, 105, 85,  13,  246, 140, 86,  48,  227, 219,
        154, 2,   253, 245, 209, 49,  130, 27,  150, 90,  189, 48,  247, 209, 133, 115, 52,  22,
        177, 69,  12,  237, 253, 3,   174, 217, 74,  188, 227, 213, 6,   248, 240, 0,   139, 40,
        121, 189, 39,  22,  210, 78,  95,  141, 155, 182, 188, 127, 23,  136, 50,  15,  115, 77,
        90,  104, 55,  235, 130, 241, 252, 79,  85,  60,  247, 70,  138, 139, 90,  240, 208, 165,
        207, 223, 54,  19,  46,  197, 250, 49,  33,  156, 0,   163, 227, 139, 104, 148, 156, 232,
        107, 6,   11,  38,  177, 188, 99,  11,  39,  114, 53,  250, 170, 178, 143, 165, 54,  168,
        3,   82,  2,   136, 46,  127, 132, 245, 39,  53,  8,   50,  183, 129, 196, 69,  203, 125,
        221, 220, 75,  133, 165, 11,  85,  51,  102, 209, 201, 93,  140, 62,  231, 96,  186, 105,
        70,  122, 120, 4,   111, 141, 220, 91,  55,  180, 196, 21,  73,  55,  1,   233, 160, 82,
        217, 81,  160, 70,  186, 207, 251, 2,   21,  167, 243, 244, 173, 73,  119, 179, 108, 45,
        221, 194, 204, 113, 207, 190, 145, 114, 114, 189, 43,  62,  119, 155, 172, 71,  73,  123,
        222, 80,  46,  60,  228, 51,  229, 145, 135, 85,  152, 99,  68,  212, 96,  24,  130, 253,
        85,  233, 201, 56,  209, 85,  202, 2,   218, 42,  157, 135, 181, 148, 16,  122, 0,   1,
        192, 42,  13,  152, 157, 54,  208, 67,  183, 89,  5,   15,  237, 166, 143, 228, 253, 63,
        51,  193, 49,  111, 45,  76,  60,  34,  122, 56,  16,  175, 204, 109, 163, 7,   81,  95,
        98,  98,  3,   176, 210, 97,  62,  97,  194, 56,  147, 104, 49,  69,  75,  174, 26,  166,
        97,  90,  176, 204, 132, 43,  164, 94,  248, 171, 60,  143, 223, 88,  243, 250, 139, 189,
        116, 106, 229, 216, 246, 180, 249, 228, 94,  165, 148, 214, 32,  29,  120, 148, 50,  95,
        204, 0,   21,  223, 192, 130, 110, 177, 133, 10,  141, 63,  221, 79,  208, 177, 227, 165,
        69,  121, 76,  138, 241, 231, 3,   157, 67,  149, 29,  249, 144, 163, 34,  5,   177, 90,
        23,  157, 207, 59,  205, 105, 17,  141, 244, 6,   237, 108, 194, 224, 175, 115, 99,  176,
        73,  25,  78,  173, 104, 163, 90,  161, 171, 115, 1,   77,  71,  204, 93,  209, 42,  92,
        79,  248, 134, 238, 185, 189, 41,  78,  155, 81,  245, 102, 165, 161, 60,  245, 208, 105,
        215, 200, 156, 80,  249, 39,  109, 202, 174, 11,  23,  192, 253, 242, 228, 5,   151, 61,
        178, 48,  178, 91,  35,  105, 32,  63,  92,  31,  146, 225, 101, 237, 187, 27,  154, 182,
        4,   86,  70,  58,  62,  234, 219, 238, 252, 181, 158, 168, 17,  211, 164, 79,  12,  22,
        202, 150, 251, 117, 143, 135, 137, 184, 169, 5,   166, 127, 209, 45,  232, 222, 164, 137,
        84,  95,  39,  29,  140, 34,  175, 77,  103, 199, 22,  175, 142, 238, 38,  204, 148, 135,
        22,  97,  80,  99,  131, 209, 10,  110, 169, 151, 217, 77,  22,  13,  211, 196, 203, 240,
        73,  64,  176, 65,  46,  195, 189, 136, 228, 13,  47,  11,  191, 118, 213, 54,  140, 68,
        243, 158, 192, 78,  111, 85,  155, 134, 217, 132, 158, 35,  35,  64,  128, 51,  239, 49,
        161, 95,  76,  188, 142, 106, 237, 81,  147, 97,  85,  23,  213, 114, 117, 58,  133, 96,
        185, 67,  8,   196, 113, 114, 31,  144, 76,  48,  181, 159, 167, 115, 30,  23,  58,  76,
        96,  47,  183, 19,  234, 37,  43,  194, 58,  195, 128, 33,  120, 49,  237, 11,  142, 230,
        42,  181, 195, 150, 8,   22,  31,  218, 88,  209, 166, 197, 104, 228, 0,   114, 22,  181,
        229, 21,  222, 136, 185, 163, 236, 240, 158, 167, 236, 21,  174, 18,  105, 188, 124, 184,
        6,   136, 39,  236, 158, 185, 97,  185, 201, 18,  108, 57,  229, 44,  103, 188, 64,  45,
        200, 197, 71,  247, 94,  153, 43,  226, 126, 159, 221, 223, 62,  247, 181, 89,  237, 101,
        57,  238, 24,  83,  100, 252, 113, 212, 82,  2,   149, 177, 109, 147, 207, 152, 105, 10,
        6,   246, 175, 154, 40,  85,  251, 150, 130, 114, 234, 69,  195, 0,   42,  61,  185, 54,
        96,  131, 52,  128, 205, 92,  92,  127, 218, 241, 171, 148, 200, 158, 68,  126, 190, 55,
        105, 251, 67,  90,  197, 19,  234, 232, 175, 17,  21,  97,  215, 11,  245, 4,   173, 94,
        10,  192, 235, 149, 50,  70,  45,  84,  95,  166, 173, 12,  54,  171, 19,  56,  73,  242,
        10,  75,  178, 73,  237, 203, 77,  225, 40,  206, 97,  16,  39,  189, 165, 91,  52,  80,
        236, 57,  153, 127, 42,  236, 57,  110, 219, 35,  139, 189, 122, 217, 84,  219, 136, 154,
        107, 83,  56,  173, 5,   174, 77,  186, 194, 170, 21,  139, 112, 8,   202, 77,  40,  135,
        137, 120, 197, 202, 23,  160, 75,  201, 27,  31,  45,  183, 41,  9,   76,  159, 235, 57,
        237, 128, 52,  122, 241, 222, 232, 63,  152, 60,  185, 23,  134, 45,  12,  10,  144, 157,
        235, 181, 97,  242, 249, 234, 35,  237, 35,  111, 102, 44,  61,  28,  2,   194, 192, 209,
        253, 239, 139, 149, 236, 194, 193, 154, 84,  226, 118, 213, 190, 61,  24,  172, 239, 71,
        191, 90,  69,  164, 55,  115, 196, 127, 160, 116, 138, 34,  53,  88,  127, 217, 14,  187,
        112, 14,  247, 68,  167, 236, 7,   143, 216, 246, 193, 190, 169, 191, 249, 242, 170, 40,
        199, 52,  36,  121, 132, 5,   111, 170, 71,  38,  234, 210, 164, 180, 106, 131, 157, 235,
        135, 101, 71,  54,  74,  177, 12,  176, 90,  244, 49,  71,  234, 7,   173, 234, 121, 117,
        24,  120, 192, 104, 16,  9,   255, 117, 216, 51,  230, 219, 245, 49,  113, 2,   236, 60,
        228, 42,  117, 147, 95,  52,  171, 205, 163, 61,  9,   247, 106, 65,  163, 38,  46,  180,
        237, 84,  86,  53,  174, 234, 50,  156, 96,  35,  214, 175, 158, 104, 90,  191, 232, 24,
        42,  224, 166, 12,  245, 111, 215, 209, 210, 219, 213, 190, 144, 251, 127, 171, 220, 34,
        47,  75,  98,  151, 203, 109, 154, 251, 166, 62,  196, 192, 221, 122, 192, 24,  69,  112,
        6,   60,  96,  212, 62,  8,   196, 49,  95,  38,  31,  138, 79,  6,   22,  154, 205, 131,
        155, 149, 149, 79,  202, 223, 30,  96,  238, 152, 60,  190, 92,  33,  128, 146, 215, 95,
        114, 177, 108, 178, 58,  133, 107, 0,   196, 195, 152, 152, 142, 159, 131, 176, 21,  252,
        231, 249, 201, 37,  184, 182, 74,  190, 228, 38,  14,  36,  186, 17,  228, 27,  252, 246,
        100, 82,  220, 128, 34,  18,  136, 206, 4,   101, 253, 176, 91,  18,  28,  220, 8,   250,
        1,   205, 172, 178, 200, 244, 226, 221, 187, 184, 63,  232, 49,  140, 65,  194, 109, 87,
        165, 129, 63,  171, 82,  79,  82,  16,  25,  15,  115, 201, 132, 189, 106, 89,  185, 207,
        66,  76,  50,  118, 89,  133, 226, 229, 148, 205, 220, 163, 208, 244, 91,  210, 31,  73,
        224, 174, 105, 177, 250, 84,  120, 93,  201, 113, 34,  31,  217, 34,  21,  251, 182, 8,
        95,  129, 95,  181, 94,  0,   100, 145, 189, 230, 170, 154, 141, 156, 216, 141, 204, 42,
        26,  119, 41,  53,  199, 241, 111, 89,  10,  33,  60,  152, 44,  195, 245, 177, 252, 58,
        73,  34,  171, 176, 77,  8,   200, 61,  174, 60,  169, 164, 145, 66,  138, 83,  24,  22,
        81,  58,  5,   119, 94,  133, 244, 213, 213, 161, 10,  104, 53,  225, 167, 56,  166, 3,
        123, 47,  66,  50,  93,  193, 136, 94,  35,  75,  206, 253, 197, 124, 161, 66,  100, 147,
        123, 127, 46,  98,  245, 59,  32,  43,  56,  171, 118, 79,  240, 72,  42,  95,  118, 152,
        19,  231, 234, 237, 17,  60,  94,  121, 128, 210, 210, 100, 214, 137, 25,  50,  200, 151,
        86,  160, 221, 90,  103, 23,  73,  227, 70,  108, 96,  79,  24,  33,  188, 70,  12,  113,
        99,  214, 40,  68,  77,  138, 101, 86,  184, 171, 136, 129, 41,  116, 198, 65,  236, 27,
        218, 209, 66,  36,  12,  135, 133, 239, 177, 67,  173, 116, 107, 38,  20,  224, 12,  177,
        170, 11,  189, 176, 28,  218, 39,  50,  27,  94,  148, 243, 246, 100, 218, 54,  132, 91,
        198, 112, 215, 96,  27,  197, 67,  199, 76,  177, 184, 134, 95,  18,  50,  161, 163, 9,
        7,   1,   238, 86,  243, 75,  23,  246, 219, 8,   103, 165, 180, 191, 160, 223, 109, 201,
        6,   142, 215, 218, 53,  110, 116, 69,  105, 180, 100, 152, 194, 155, 193, 184, 229, 15,
        113, 192, 39,  19,  100, 107, 179, 161, 148, 48,  231, 24,  114, 74,  119, 209, 4,   38,
        114, 215, 181, 62,  231, 167, 45,  95,  44,  127, 15,  32,  170, 152, 225, 230, 162, 202,
        16,  38,  165, 199, 193, 164, 142, 49,  108, 86,  68,  131, 131, 150, 145, 249, 106, 214,
        137, 92,  226, 178, 211, 113, 61,  216, 240, 166, 104, 208, 233, 142, 211, 66,  88,  141,
        22,  144, 170, 222, 199, 158, 153, 30,  7,   60,  230, 247, 159, 125, 117, 11,  204, 227,
        17,  48,  83,  106, 250, 24,  134, 117, 96,  17,  192, 218, 153, 213, 79,  44,  246, 213,
        242, 209, 117, 188, 224, 123, 8,   161, 185, 12,  194, 241, 131, 199, 33,  93,  231, 66,
        36,  244, 50,  168, 241, 158, 11,  215, 108, 82,  226, 88,  80,  190, 182, 109, 46,  122,
        93,  37,  82,  19,  190, 189, 254, 90,  172, 51,  163, 251, 87,  224, 91,  179, 47,  148,
        137, 140, 235, 74,  135, 183, 41,  186, 58,  176, 176, 251, 233, 47,  27,  4,   99,  113,
        93,  212, 169, 123, 208, 193, 28,  118, 105, 197, 159, 229, 93,  196, 154, 179, 36,  93,
        213, 188, 91,  219, 90,  70,  8,   40,  35,  119, 205, 58,  158, 166, 172, 69,  184, 27,
        117, 44,  115, 55,  142, 64,  136, 28,  63,  229, 152, 254, 93,  0,   244, 240, 232, 89,
        250, 61,  200, 68,  43,  1,   45,  198, 171, 59,  196, 61,  19,  17,  164, 204, 118, 211,
        88,  30,  190, 42,  243, 165, 124, 5,   136, 201, 184, 91,  53,  140, 136, 167, 122, 246,
        165, 61,  11,  5,   244, 58,  181, 53,  22,  138, 124, 158, 83,  191, 218, 235, 128, 178,
        79,  42,  68,  186, 235, 11,  166, 240, 210, 168, 23,  167, 234, 248, 8,   81,  196, 7,
        41,  37,  134, 104, 20,  40,  146, 186, 31,  154, 241, 131, 30,  4,   235, 121, 113, 37,
        178, 2,   121, 209, 167, 46,  221, 196, 45,  37,  33,  91,  137, 182, 189, 245, 41,  218,
        207, 233, 36,  97,  82,  5,   197, 64,  127, 76,  52,  57,  135, 50,  247, 55,  161, 96,
        170, 49,  179, 205, 86,  36,  142, 13,  61,  147, 102, 55,  163, 207, 2,   230, 123, 139,
        73,  13,  247, 80,  156, 19,  243, 194, 144, 140, 185, 137, 191, 35,  29,  59,  152, 67,
        116, 132, 68,  146, 220, 248, 160, 40,  197, 139, 215, 213, 236, 118, 195, 33,  73,  94,
        3,   11,  200, 105, 154, 138, 57,  37,  43,  118, 116, 159, 46,  94,  188, 55,  10,  194,
        174, 63,  43,  240, 68,  96,  113, 111, 90,  196, 101, 158, 183, 233, 85,  44,  137, 66,
        52,  44,  153, 145, 110, 11,  80,  135, 60,  155, 8,   224, 251, 170, 179, 26,  137, 225,
        12,  167, 100, 144, 51,  150, 54,  227, 77,  127, 200, 39,  147, 89,  245, 37,  207, 106,
        200, 65,  50,  108, 42,  223, 2,   171, 8,   103, 14,  216, 129, 209, 3,   4,   56,  56,
        61,  142, 35,  253, 52,  42,  165, 34,  106, 158, 245, 253, 62,  190, 171, 68,  223, 116,
        136, 37,  166, 237, 116, 66,  99,  235, 159, 122, 186, 99,  233, 82,  177, 171, 124, 222,
        190, 95,  203, 197, 67,  34,  82,  56,  136, 18,  62,  255, 141, 240, 135, 193, 244, 31,
        86,  50,  100, 78,  177, 241, 176, 135, 106, 83,  124, 209, 117, 39,  112, 238, 42,  156,
        84,  127, 173, 147, 94,  4,   219, 222, 84,  43,  126, 46,  6,   84,  26,  155, 5,   209,
        75,  42,  38,  149, 29,  158, 163, 43,  166, 126, 74,  92,  73,  193, 250, 95,  170, 85,
        125, 10,  145, 101, 124, 249, 102, 112, 206, 197, 0,   103, 21,  104, 68,  139, 163, 7,
        92,  195, 30,  194, 140, 121, 217, 133, 194, 223, 31,  38,  116, 167, 7,   99,  162, 140,
        161, 162, 95,  4,   65,  171, 156, 108, 46,  230, 55,  147, 182, 156, 244, 137, 173, 70,
        6,   21,  196, 61,  192, 127, 108, 164, 54,  2,   232, 100, 6,   247, 209, 72,  41,  97,
        177, 254, 84,  37,  66,  80,  3,   141, 138, 74,  165, 201, 116, 146, 216, 189, 228, 34,
        135, 165, 38,  180, 163, 171, 0,   112, 231, 243, 11,  131, 172, 7,   147, 151, 230, 179,
        50,  44,  76,  84,  84,  23,  163, 21,  108, 87,  43,  233, 41,  225, 236, 191, 213, 51,
        111, 66,  63,  127, 35,  81,  204, 165, 112, 35,  251, 135, 223, 58,  55,  189, 247, 41,
        194, 179, 24,  55,  230, 64,  11,  61,  193, 151, 217, 230, 5,   164, 211, 90,  229, 239,
        205, 253, 147, 31,  37,  187, 133, 95,  36,  131, 4,   31,  178, 152, 225, 14,  159, 119,
        190, 29,  232, 72,  79,  45,  203, 105, 196, 40,  13,  43,  152, 105, 172, 124, 143, 98,
        29,  113, 128, 166, 109, 242, 243, 149, 106, 105, 45,  182, 203, 187, 244, 181, 22,  206,
        53,  241, 84,  174, 175, 47,  221, 71,  9,   243, 187, 38,  202, 15,  205, 211, 124, 158,
        197, 209, 162, 55,  210, 167, 181, 184, 158, 109, 194, 156, 77,  152, 42,  7,   153, 94,
        180, 91,  10,  113, 74,  39,  167, 61,  151, 123, 80,  62,  172, 23,  12,  228, 195, 68,
        227, 182, 147, 240, 18,  55,  114, 167, 41,  230, 160, 192, 212, 231, 209, 160, 219, 109,
        30,  48,  84,  206, 26,  46,  228, 72,  167, 244, 117, 76,  29,  42,  86,  117, 28,  10,
        148, 140, 132, 176, 171, 97,  199, 53,  211, 72,  128, 171, 159, 249, 216, 161, 89,  99,
        28,  182, 106, 201, 130, 200, 8,   197, 236, 35,  44,  114, 130, 92,  131, 7,   235, 245,
        182, 168, 28,  38,  119, 48,  145, 200, 37,  192, 159, 149, 127, 135, 141, 255, 234, 99,
        84,  213, 33,  37,  44,  236, 39,  49,  218, 171, 182, 144, 100, 176, 147, 101, 220, 102,
        55,  108, 166, 131, 68,  106, 30,  103, 69,  24,  110, 98,  13,  171, 136, 147, 239, 55,
        112, 177, 78,  127, 178, 56,  243, 90,  68,  121, 109, 247, 198, 235, 154, 166, 151, 27,
        85,  186, 255, 74,  95,  104, 248, 201, 161, 208, 172, 212, 246, 226, 91,  209, 109, 215,
        178, 21,  121, 173, 45,  179, 214, 170, 5,   197, 192, 117, 97,  189, 180, 184, 189, 139,
        51,  50,  63,  167, 25,  230, 141, 134, 141, 156, 225, 118, 146, 214, 241, 151, 119, 126,
        142, 113, 64,  130, 45,  67,  218, 185, 119, 47,  66,  240, 154, 154, 91,  90,  167, 221,
        11,  173, 207, 21,  97,  50,  205, 73,  145, 192, 245, 218, 146, 221, 226, 73,  133, 142,
        162, 208, 174, 75,  149, 56,  230, 186, 214, 74,  37,  130, 231, 122, 176, 247, 92,  113,
        240, 223, 79,  210, 129, 20,  145, 189, 99,  46,  11,  17,  96,  215, 221, 245, 20,  251,
        98,  150, 7,   158, 110, 125, 22,  106, 119, 215, 103, 153, 83,  186, 15,  181, 136, 16,
        61,  49,  13,  113, 247, 50,  63,  198, 242, 211, 87,  108, 123, 221, 143, 201, 20,  183,
        254, 59,  98,  36,  153, 175, 50,  231, 77,  190, 101, 76,  67,  13,  229, 9,   85,  176,
        77,  230, 103, 212, 62,  115, 25,  26,  100, 236, 52,  86,  252, 55,  147, 193, 118, 23,
        102, 224, 55,  200, 43,  116, 238, 209, 102, 214, 90,  234, 17,  249, 219, 38,  234, 97,
        172, 180, 53,  78,  230, 243, 157, 88,  182, 243, 124, 56,  111, 208, 137, 183, 64,  86,
        189, 213, 120, 89,  207, 110, 197, 75,  236, 172, 10,  49,  44,  158, 138, 208, 173, 156,
        41,  209, 209, 119, 187, 250, 122, 4,   160, 165, 148, 47,  60,  14,  241, 154, 41,  133,
        139, 182, 16,  185, 98,  213, 181, 233, 95,  219, 134, 240, 129, 70,  235, 0,   116, 80,
        135, 71,  203, 72,  16,  41,  16,  6,   188, 144, 103, 141, 247, 123, 184, 53,  107, 195,
        232, 96,  174, 248, 91,  202, 53,  120, 240, 48,  254, 125, 217, 107, 27,  123, 244, 249,
        249, 106, 191, 83,  118, 242, 142, 101, 46,  48,  118, 1,   201, 6,   64,  20,  58,  179,
        141, 93,  144, 221, 37,  13,  200, 239, 106, 184, 126, 169, 90,  51,  75,  12,  181, 50,
        198, 156, 167, 100, 238, 245, 173, 167, 96,  62,  61,  23,  144, 214, 77,  175, 114, 17,
        223, 119, 193, 40,  174, 65,  55,  54,  241, 184, 38,  33,  41,  9,   40,  190, 84,  41,
        171, 105, 165, 43,  45,  105, 107, 248, 93,  18,  34,  199, 27,  120, 192, 134, 248, 244,
        144, 151, 213, 192, 155, 133, 100, 227, 56,  67,  191, 74,  154, 142, 171, 166, 151, 162,
        110, 254, 62,  63,  66,  99,  181, 64,  176, 113, 99,  111, 143, 56,  207, 237, 103, 33,
        16,  7,   13,  53,  250, 160, 209, 140, 106, 191, 139, 205, 206, 236, 38,  50,  203, 40,
        85,  61,  145, 166, 184, 114, 92,  178, 58,  94,  142, 125, 4,   117, 75,  48,  224, 242,
        140, 193, 73,  69,  37,  11,  187, 218, 247, 92,  156, 73,  43,  247, 156, 134, 225, 201,
        160, 172, 187, 122, 197, 21,  136, 28,  69,  58,  236, 219, 209, 22,  20,  63,  52,  92,
        170, 113, 253, 204, 170, 220, 220, 255, 17,  80,  149, 97,  205, 127, 56,  70,  202, 176,
        139, 98,  94,  78,  198, 125, 3,   16,  133, 170, 192, 130, 68,  33,  226, 79,  234, 27,
        97,  66,  52,  250, 48,  131, 177, 199, 46,  244, 52,  36,  6,   149, 255, 249, 242, 77,
        169, 22,  47,  149, 216, 137, 88,  115, 80,  28,  221, 15,  109, 126, 84,  26,  184, 76,
        106, 53,  212, 243, 178, 48,  173, 65,  158, 150, 39,  242, 219, 173, 18,  243, 14,  251,
        87,  245, 226, 67,  210, 109, 237, 65,  70,  1,   0,   25,  192, 14,  254, 217, 215, 156,
        173, 142, 157, 152, 241, 126, 27,  192, 79,  56,  172, 116, 103, 148, 198, 244, 100, 243,
        103, 191, 91,  14,  136, 109, 61,  203, 101, 255, 37,  151, 6,   0,   41,  137, 102, 154,
        210, 249, 139, 134, 34,  168, 34,  221, 130, 45,  156, 137, 215, 129, 83,  196, 153, 129,
        27,  255, 56,  221, 7,   58,  209, 175, 163, 187, 252, 203, 76,  56,  145, 248, 55,  180,
        174, 173, 167, 202, 163, 63,  130, 177, 193, 121, 251, 73,  20,  207, 59,  107, 181, 126,
        201, 250, 113, 80,  97,  121, 245, 107, 246, 123, 13,  108, 116, 242, 194, 115, 111, 196,
        154, 44,  216, 56,  32,  49,  104, 216, 6,   73,  247, 54,  90,  0,   19,  115, 15,  169,
        233, 104, 87,  235, 241, 175, 84,  88,  174, 99,  239, 129, 51,  215, 208, 157, 48,  93,
        133, 15,  203, 226, 242, 117, 179, 93,  6,   195, 16,  174, 13,  147, 88,  138, 69,  13,
        45,  24,  139, 82,  224, 49,  204, 106, 177, 65,  89,  122, 209, 63,  154, 58,  243, 135,
        156, 94,  166, 71,  97,  247, 46,  133, 93,  157, 191, 200, 215, 129, 126, 58,  36,  16,
        29,  109, 19,  46,  42,  49,  220, 7,   53,  40,  108, 69,  216, 226, 174, 233, 93,  16,
        36,  121, 160, 164, 6,   62,  162, 228, 233, 35,  28,  183, 171, 204, 11,  58,  168, 151,
        227, 141, 108, 178, 139, 150, 47,  226, 20,  235, 105, 220, 54,  29,  213, 70,  223, 159,
        61,  20,  122, 46,  237, 216, 35,  177, 160, 241, 99,  80,  229, 45,  164, 215, 154, 167,
        54,  38,  99,  33,  101, 229, 251, 225, 230, 3,   240, 179, 37,  22,  120, 154, 51,  17,
        71,  34,  20,  199, 37,  154, 115, 201, 204, 114, 60,  18,  87,  233, 75,  64,  124, 124,
        45,  100, 104, 205, 244, 192, 179, 120, 240, 230, 48,  100, 174, 108, 6,   23,  83,  155,
        135, 66,  156, 62,  243, 84,  250, 193, 115, 189, 23,  158, 135, 197, 242, 215, 30,  36,
        59,  62,  17,  48,  133, 118, 78,  7,   64,  192, 249, 203, 166, 236, 219, 49,  211, 55,
        64,  174, 54,  248, 219, 102, 152, 155, 103, 24,  222, 175, 162, 225, 108, 41,  116, 169,
        107, 2,   93,  29,  50,  37,  112, 39,  188, 62,  59,  242, 36,  234, 221, 151, 198, 111,
        206, 229, 255, 89,  154, 21,  63,  10,  231, 78,  137, 26,  96,  177, 236, 5,   15,  93,
        63,  25,  47,  189, 164, 18,  87,  122, 42,  70,  242, 43,  137, 185, 175, 20,  52,  178,
        177, 1,   8,   245, 63,  186, 107, 10,  28,  153, 151, 174, 181, 168, 25,  25,  103, 150,
        82,  175, 110, 129, 153, 86,  251, 6,   224, 120, 69,  215, 81,  247, 230, 223, 79,  62,
        11,  215, 62,  25,  230, 210, 193, 96,  163, 18,  227, 10,  177, 184, 29,  122, 228, 212,
        181, 238, 232, 123, 47,  176, 108, 58,  38,  137, 116, 65,  205, 212, 114, 142, 0,   165,
        105, 109, 255, 75,  170, 68,  184, 120, 94,  214, 68,  223, 245, 108, 216, 168, 77,  247,
        97,  29,  157, 82,  180, 54,  159, 78,  97,  184, 234, 20,  5,   176, 113, 22,  173, 15,
        41,  108, 35,  155, 166, 209, 100, 202, 87,  108, 240, 52,  65,  127, 98,  166, 203, 95,
        178, 112, 153, 129, 141, 122, 76,  42,  117, 151, 17,  146, 106, 111, 87,  200, 165, 51,
        122, 163, 3,   175, 185, 44,  254, 246, 205, 150, 162, 213, 125, 61,  12,  76,  170, 166,
        247, 78,  178, 132, 205, 25,  190, 64,  154, 23,  197, 60,  51,  55,  8,   198, 9,   33,
        113, 248, 152, 245, 3,   13,  152, 102, 32,  110, 94,  84,  66,  248, 234, 171, 115, 136,
        90,  173, 16,  100, 227, 113, 60,  214, 181, 72,  61,  122, 66,  253, 207, 23,  103, 24,
        11,  105, 61,  226, 242, 32,  125, 46,  232, 152, 211, 122, 229, 153, 193, 38,  47,  79,
        176, 137, 123, 4,   136, 3,   200, 30,  221, 33,  231, 102, 170, 182, 206, 181, 141, 75,
        26,  106, 238, 10,  39,  64,  196, 216, 191, 135, 122, 175, 17,  149, 177, 0,   117, 103,
        253, 21,  108, 86,  240, 182, 183, 190, 178, 117, 28,  105, 10,  220, 172, 132, 109, 232,
        232, 170, 244, 102, 75,  199, 80,  12,  33,  148, 89,  19,  11,  115, 216, 43,  174, 234,
        241, 6,   21,  27,  100, 119, 80,  3,   57,  208, 200, 94,  104, 166, 230, 66,  60,  43,
        184, 125, 227, 51,  231, 115, 95,  173, 118, 216, 209, 168, 34,  47,  62,  231, 184, 140,
        186, 216, 232, 205, 34,  68,  8,   224, 130, 231, 23,  124, 155, 0,   64,  156, 38,  53,
        204, 198, 61,  142, 204, 152, 22,  51,  233, 40,  150, 176, 76,  181, 66,  128, 154, 208,
        212, 31,  228, 82,  16,  196, 76,  154, 180, 151, 246, 166, 225, 109, 25,  135, 180, 125,
        82,  224, 200, 13,  31,  229, 158, 199, 104, 193, 157, 60,  193, 173, 223, 19,  216, 25,
        159, 76,  91,  236, 75,  7,   105, 24,  47,  17,  189, 19,  70,  144, 150, 78,  121, 67,
        82,  56,  94,  181, 204, 146, 1,   51,  69,  152, 216, 165, 89,  97,  108, 220, 138, 13,
        204, 236, 110, 182, 1,   254, 135, 38,  187, 123, 122, 27,  180, 160, 63,  152, 193, 207,
        81,  19,  184, 157, 79,  254, 216, 76,  244, 207, 234, 59,  39,  105, 196, 28,  207, 150,
        154, 229, 223, 122, 157, 251, 164, 157, 117, 126, 101, 224, 24,  114, 90,  119, 159, 218,
        104, 213, 253, 157, 28,  78,  64,  147, 101, 15,  101, 227, 153, 4,   18,  194, 31,  1,
        187, 69,  11,  19,  173, 92,  53,  153, 152, 197, 191, 140, 168, 201, 217, 173, 232, 136,
        140, 223, 200, 44,  5,   230, 7,   213, 90,  241, 211, 156, 32,  8,   170, 142, 132, 48,
        192, 76,  238, 200, 215, 25,  128, 17,  88,  123, 106, 212, 62,  164, 58,  48,  146, 7,
        86,  98,  161, 46,  132, 31,  31,  165, 164, 187, 63,  158, 153, 233, 6,   44,  106, 108,
        83,  205, 175, 115, 73,  47,  216, 210, 39,  138, 62,  231, 223, 57,  143, 69,  84,  198,
        181, 189, 135, 150, 158, 179, 22,  116, 2,   22,  77,  174, 103, 117, 235, 197, 110, 74,
        169, 162, 156, 144, 210, 68,  52,  10,  166, 35,  159, 71,  225, 139, 193, 188, 117, 111,
        143, 21,  15,  252, 131, 22,  85,  56,  27,  147, 100, 121, 228, 77,  186, 174, 224, 46,
        239, 202, 240, 187, 18,  113, 223, 55,  45,  153, 143, 196, 164, 119, 116, 246, 203, 210,
        31,  95,  83,  138, 16,  25,  229, 133, 132, 146, 80,  55,  180, 123, 51,  140, 153, 105,
        1,   237, 86,  222, 196, 210, 67,  78,  162, 150, 255, 64,  51,  26,  200, 216, 111, 58,
        167, 15,  123, 218, 29,  82,  86,  105, 229, 129, 216, 13,  113, 9,   135, 35,  115, 211,
        84,  24,  71,  20,  85,  250, 19,  97,  242, 93,  9,   97,  69,  255, 179, 216, 150, 111,
        118, 155, 73,  73,  17,  76,  26,  151, 234, 102, 105, 212, 99,  151, 45,  5,   22,  187,
        115, 13,  193, 90,  246, 135, 92,  74,  104, 202, 16,  218, 138, 143, 117, 215, 187, 145,
        199, 106, 187, 237, 214, 151, 163, 53,  130, 109, 200, 64,  247, 79,  8,   190, 53,  192,
        243, 228, 18,  164, 218, 178, 37,  79,  15,  175, 30,  49,  209, 225, 219, 46,  127, 34,
        188, 158, 227, 5,   110, 227, 61,  95,  251, 252, 244, 19,  102, 47,  223, 56,  14,  61,
        150, 175, 180, 185, 194, 213, 222, 147, 198, 59,  62,  210, 177, 109, 170, 238, 11,  111,
        202, 118, 203, 81,  232, 27,  106, 23,  93,  157, 119, 227, 182, 200, 7,   34,  114, 105,
        162, 150, 238, 33,  102, 211, 244, 250, 157, 27,  32,  253, 178, 184, 196, 127, 84,  103,
        242, 163, 115, 7,   117, 186, 185, 20,  226, 73,  142, 48,  52,  57,  147, 152, 202, 186,
        148, 44,  17,  29,  20,  137, 206, 78,  164, 202, 206, 97,  222, 220, 100, 200, 105, 224,
        239, 39,  104, 17,  154, 235, 255, 240, 194, 239, 222, 174, 4,   104, 196, 137, 186, 65,
        235, 47,  82,  116, 94,  127, 55,  7,   19,  254, 155, 128, 124, 50,  170, 12,  140, 135,
        250, 187, 36,  99,  147, 169, 175, 129, 239, 76,  129, 170, 167, 162, 123, 195, 47,  60,
        147, 198, 138, 215, 193, 213, 242, 155, 48,  72,  226, 211, 239, 242, 146, 81,  57,  59,
        247, 79,  150, 198, 230, 3,   233, 217, 161, 90,  35,  93,  168, 247, 169, 151, 227, 171,
        92,  110, 134, 246, 221, 156, 77,  134, 160, 17,  1,   64,  110, 124, 139, 105, 7,   9,
        54,  42,  44,  77,  235, 36,  194, 184, 54,  16,  60,  180, 153, 191, 119, 162, 240, 93,
        46,  163, 246, 58,  116, 104, 181, 100, 105, 227, 239, 126, 218, 2,   224, 57,  16,  210,
        162, 198, 61,  129, 199, 83,  155, 58,  245, 62,  79,  125, 92,  183, 163, 219, 156, 146,
        128, 215, 19,  129, 193, 32,  17,  230, 203, 132, 200, 182, 112, 205, 126, 70,  255, 172,
        254, 77,  70,  8,   210, 224, 64,  43,  150, 2,   15,  59,  145, 13,  160, 62,  98,  75,
        132, 227, 143, 5,   251, 238, 2,   35,  184, 137, 121, 136, 116, 234, 27,  189, 118, 236,
        92,  235, 77,  122, 248, 231, 26,  225, 72,  81,  244, 49,  55,  28,  178, 179, 14,  9,
        109, 57,  71,  36,  107, 213, 72,  60,  141, 28,  80,  177, 149, 170, 84,  31,  156, 234,
        89,  153, 192, 30,  32,  32,  93,  190, 90,  162, 191, 156, 112, 142, 138, 140, 207, 194,
        157, 241, 186, 78,  63,  231, 149, 45,  193, 204, 120, 72,  248, 78,  91,  240, 208, 204,
        7,   219, 5,   62,  75,  147, 21,  168, 82,  11,  158, 105, 32,  176, 122, 208, 82,  55,
        3,   76,  95,  49,  78,  70,  16,  157, 78,  253, 185, 222, 109, 242, 22,  226, 212, 60,
        216, 34,  143, 39,  74,  137, 88,  99,  125, 18,  189, 163, 189, 140, 88,  86,  170, 234,
        65,  230, 177, 169, 48,  143, 135, 119, 145, 187, 58,  14,  251, 108, 133, 40,  0,   208,
        177, 222, 78,  102, 12,  104, 57,  187, 251, 67,  61,  216, 78,  112, 213, 153, 184, 65,
        85,  50,  105, 18,  133, 191, 126, 203, 109, 171, 167, 61,  170, 163, 15,  185, 119, 145,
        233, 178, 84,  179, 86,  223, 183, 54,  151, 204, 250, 126, 13,  208, 23,  137, 64,  76,
        107, 151, 167, 16,  97,  20,  110, 203, 199, 172, 93,  165, 175, 158, 173, 69,  135, 243,
        126, 155, 81,  70,  118, 80,  209, 198, 49,  111, 243, 26,  224, 242, 180, 196, 25,  142,
        210, 39,  73,  244, 4,   136, 191, 193, 54,  219, 40,  54,  223, 124, 106, 90,  151, 55,
        236, 31,  49,  104, 178, 98,  13,  85,  173, 59,  170, 231, 70,  243, 255, 253, 97,  128,
        1,   191, 110, 235, 49,  176, 84,  178, 127, 8,   207, 164, 154, 147, 108, 168, 37,  162,
        111, 231, 169, 26,  82,  255, 231, 69,  27,  23,  235, 139, 19,  121, 210, 242, 231, 10,
        40,  187, 189, 49,  28,  118, 81,  156, 111, 72,  87,  58,  108, 78,  98,  211, 179, 229,
        223, 22,  220, 160, 37,  37,  222, 247, 151, 235, 129, 214, 162, 151, 134, 118, 232, 195,
        127, 138, 196, 33,  176, 197, 214, 78,  248, 3,   201, 40,  56,  119, 6,   97,  76,  92,
        115, 174, 226, 253, 91,  95,  137, 249, 222, 139, 29,  114, 114, 138, 7,   254, 16,  196,
        249, 40,  181, 27,  112, 9,   231, 147, 235, 23,  64,  104, 206, 244, 90,  162, 70,  100,
        170, 232, 92,  98,  79,  200, 159, 27,  242, 195, 36,  219, 160, 69,  108, 122, 250, 246,
        173, 23,  191, 197, 161, 104, 21,  29,  78,  198, 174, 192, 31,  128, 161, 100, 223, 248,
        174, 86,  38,  23,  215, 170, 221, 44,  104, 167, 46,  249, 101, 180, 64,  236, 115, 193,
        213, 194, 218, 153, 140, 34,  18,  12,  194, 201, 164, 87,  233, 192, 135, 17,  172, 39,
        205, 209, 75,  253, 228, 177, 231, 119, 20,  151, 235, 171, 56,  125, 37,  41,  137, 111,
        155, 183, 106, 32,  76,  129, 188, 27,  199, 228, 182, 246, 148, 167, 99,  149, 28,  164,
        176, 229, 70,  201, 229, 169, 36,  21,  251, 170, 210, 199, 148, 141, 100, 70,  168, 22,
        26,  99,  149, 136, 187, 143, 119, 209, 31,  145, 251, 176, 17,  101, 124, 150, 123, 60,
        115, 81,  149, 130, 49,  24,  68,  5,   227, 112, 33,  169, 92,  204, 81,  108, 18,  3,
        120, 152, 254, 247, 248, 22,  62,  3,   128, 94,  147, 180, 187, 41,  169, 178, 158, 189,
        118, 215, 132, 34,  66,  232, 226, 41,  222, 250, 87,  175, 162, 238, 168, 251, 240, 143,
        169, 204, 157, 20,  210, 199, 59,  114, 192, 142, 85,  25,  210, 160, 221, 81,  193, 107,
        4,   103, 11,  41,  238, 134, 88,  56,  205, 206, 102, 38,  63,  44,  35,  95,  164, 0,
        6,   128, 145, 131, 70,  187, 61,  226, 117, 138, 38,  113, 204, 248, 160, 144, 212, 19,
        30,  215, 194, 1,   165, 3,   197, 16,  47,  158, 58,  221, 14,  249, 45,  236, 179, 48,
        143, 13,  141, 140, 5,   159, 64,  79,  155, 253, 123, 98,  200, 170, 108, 204, 201, 248,
        22,  240, 71,  40,  159, 130, 39,  144, 250, 183, 236, 37,  97,  211, 224, 190, 174, 126,
        174, 219, 192, 58,  201, 170, 56,  35,  70,  152, 213, 45,  228, 17,  68,  133, 102, 10,
        204, 107, 4,   100, 164, 220, 253, 121, 88,  162, 2,   192, 228, 186, 212, 248, 56,  107,
        20,  158, 54,  9,   3,   87,  205, 244, 78,  109, 89,  162, 14,  15,  35,  135, 83,  224,
        124, 92,  174, 54,  30,  222, 194, 165, 45,  56,  141, 28,  152, 115, 222, 226, 28,  92,
        134, 104, 227, 237, 134, 139, 152, 116, 33,  163, 235, 69,  120, 176, 146, 232, 96,  202,
        250, 139, 113, 165, 75,  212, 36,  93,  255, 91,  174, 130, 148, 175, 171, 219, 4,   142,
        207, 145, 129, 7,   242, 114, 209, 222, 58,  188, 149, 101, 9,   50,  79,  237, 100, 161,
        254, 219, 152, 19,  180, 79,  172, 165, 148, 183, 33,  35,  25,  178, 177, 158, 81,  129,
        202, 100, 153, 117, 70,  208, 244, 90,  179, 183, 152, 202, 117, 69,  195, 246, 26,  254,
        25,  247, 40,  67,  226, 32,  236, 59,  243, 59,  118, 46,  71,  28,  103, 99,  139, 85,
        172, 214, 215, 139, 251, 0,   190, 122, 128, 51,  169, 219, 147, 50,  223, 232, 19,  98,
        165, 202, 227, 35,  209, 36,  173, 143, 94,  201, 120, 248, 90,  242, 96,  159, 248, 194,
        146, 109, 127, 72,  231, 63,  18,  149, 27,  225, 195, 24,  132, 233, 53,  184, 51,  160,
        227, 170, 171, 103, 120, 141, 34,  183, 152, 82,  186, 56,  156, 56,  191, 197, 36,  121,
        15,  190, 108, 65,  131, 230, 167, 205, 52,  1,   130, 20,  218, 83,  152, 180, 5,   71,
        28,  22,  126, 118, 246, 29,  247, 215, 46,  224, 220, 56,  194, 192, 225, 220, 58,  47,
        204, 46,  200, 115, 2,   2,   176, 170, 45,  38,  160, 97,  24,  78,  73,  183, 24,  92,
        232, 200, 181, 81,  68,  89,  213, 171, 174, 97,  74,  19,  249, 234, 174, 231, 85,  147,
        113, 246, 221, 73,  82,  249, 235, 6,   129, 225, 6,   0,   114, 182, 108, 62,  123, 34,
        255, 53,  172, 87,  5,   142, 187, 156, 181, 85,  18,  131, 209, 78,  37,  171, 32,  164,
        199, 176, 25,  128, 165, 110, 167, 87,  127, 167, 4,   248, 47,  7,   32,  199, 247, 183,
        76,  227, 169, 5,   107, 35,  152, 137, 211, 15,  106, 79,  36,  91,  217, 135, 11,  214,
        42,  64,  239, 161, 40,  159, 39,  171, 158, 37,  64,  164, 96,  201, 156, 1,   14,  200,
        111, 48,  18,  61,  45,  116, 173, 165, 16,  23,  46,  184, 163, 183, 124, 98,  145, 188,
        202, 165, 187, 146, 247, 196, 247, 180, 36,  152, 222, 80,  230, 247, 140, 245, 7,   11,
        139, 32,  0,   196, 146, 182, 60,  112, 222, 38,  73,  152, 180, 196, 182, 118, 92,  226,
        80,  150, 243, 149, 149, 30,  255, 255, 179, 255, 141, 2,   49,  207, 2,   217, 154, 100,
        46,  225, 207, 25,  196, 9,   5,   95,  114, 119, 176, 106, 123, 32,  32,  141, 58,  211,
        152, 115, 47,  43,  250, 108, 91,  207, 255, 229, 55,  35,  85,  198, 56,  135, 135, 240,
        237, 3,   103, 3,   130, 172, 113, 50,  162, 251, 195, 71,  38,  151, 40,  73,  140, 223,
        196, 204, 242, 99,  20,  15,  255, 192, 47,  29,  31,  37,  87,  232, 150, 26,  234, 197,
        129, 222, 37,  97,  98,  174, 250, 7,   208, 27,  209, 186, 198, 99,  30,  152, 205, 79,
        198, 31,  172, 133, 201, 146, 218, 241, 7,   228, 114, 42,  77,  15,  144, 240, 238, 198,
        187, 44,  104, 59,  244, 98,  53,  108, 168, 198, 153, 48,  137, 41,  160, 133, 115, 99,
        235, 214, 179, 122, 179, 59,  198, 35,  20,  181, 38,  229, 95,  155, 3,   37,  22,  148,
        142, 118, 139, 247, 140, 53,  202, 236, 225, 87,  224, 152, 137, 17,  254, 63,  184, 144,
        75,  147, 119, 86,  4,   78,  106, 6,   1,   120, 150, 85,  239, 176, 176, 186, 27,  172,
        42,  6,   203, 52,  83,  53,  250, 190, 233, 86,  144, 123, 130, 214, 17,  1,   22,  213,
        171, 150, 95,  20,  209, 34,  223, 215, 69,  86,  85,  167, 195, 156, 68,  176, 184, 174,
        238, 183, 19,  93,  239, 216, 212, 206, 56,  175, 29,  132, 13,  27,  214, 229, 184, 105,
        192, 40,  124, 200, 74,  54,  50,  151, 117, 254, 31,  45,  52,  123, 60,  167, 134, 154,
        42,  72,  31,  231, 226, 201, 220, 202, 131, 175, 24,  177, 45,  172, 50,  72,  142, 35,
        152, 73,  202, 54,  183, 214, 214, 83,  225, 129, 54,  18,  99,  243, 26,  233, 108, 58,
        121, 6,   141, 73,  151, 115, 74,  33,  142, 137, 128, 171, 121, 195, 170, 183, 66,  133,
        33,  250, 225, 210, 235, 205, 176, 39,  235, 154, 185, 193, 217, 247, 79,  252, 195, 160,
        250, 167, 5,   183, 4,   55,  118, 243, 163, 47,  96,  79,  188, 70,  226, 90,  116, 88,
        126, 250, 192, 203, 21,  174, 253, 39,  40,  136, 191, 251, 230, 83,  38,  123, 42,  255,
        55,  228, 213, 17,  249, 79,  127, 143, 51,  62,  60,  167, 129, 84,  75,  175, 246, 68,
        13,  54,  196, 8,   63,  158, 44,  255, 178, 207, 154, 30,  91,  87,  62,  24,  210, 243,
        64,  41,  47,  63,  41,  131, 4,   56,  138, 128, 8,   45,  119, 36,  175, 250, 197, 1,
        168, 215, 209, 161, 181, 192, 236, 246, 127, 194, 149, 250, 149, 252, 143, 125, 123, 150,
        149, 205, 35,  79,  134, 2,   237, 233, 77,  28,  16,  37,  133, 84,  18,  237, 236, 220,
        213, 185, 149, 119, 66,  205, 145, 222, 187, 79,  92,  139, 212, 246, 231, 160, 23,  166,
        138, 145, 55,  62,  132, 207, 246, 177, 92,  203, 206, 114, 49,  254, 201, 42,  195, 160,
        146, 130, 241, 3,   213, 235, 1,   45,  143, 51,  21,  213, 114, 76,  0,   124, 168, 32,
        210, 199, 91,  180, 27,  229, 82,  41,  163, 4,   155, 166, 157, 226, 35,  112, 143, 99,
        231, 148, 191, 73,  98,  106, 246, 26,  90,  214, 137, 208, 5,   243, 249, 164, 238, 103,
        214, 254, 186, 221, 57,  83,  44,  122, 194, 94,  236, 186, 93,  139, 234, 142, 118, 224,
        235, 107, 122, 191, 8,   219, 239, 51,  151, 94,  33,  74,  145, 19,  215, 190, 66,  218,
        240, 114, 176, 146, 233, 29,  168, 67,  32,  1,   74,  40,  183, 243, 41,  180, 107, 31,
        73,  195, 174, 31,  171, 64,  185, 69,  220, 187, 216, 182, 205, 230, 35,  112, 46,  180,
        241, 212, 86,  219, 150, 95,  104, 20,  228, 69,  1,   243, 204, 243, 0,   232, 255, 93,
        18,  167, 198, 240, 153, 165, 245, 14,  97,  217, 91,  57,  1,   0,   205, 65,  186, 135,
        9,   102, 25,  150, 118, 35,  186, 36,  14,  221, 162, 0,   120, 153, 161, 220, 158, 202,
        70,  168, 243, 13,  169, 182, 236, 71,  89,  203, 7,   254, 217, 215, 104, 126, 37,  122,
        173, 112, 129, 58,  156, 89,  184, 225, 148, 191, 178, 133, 104, 102, 150, 80,  216, 126,
        114, 98,  7,   197, 187, 197, 227, 225, 129, 72,  116, 210, 201, 225, 48,  207, 230, 82,
        141, 162, 97,  139, 212, 198, 162, 204, 243, 215, 240, 210, 238, 70,  46,  224, 32,  80,
        125, 111, 65,  184, 191, 184, 152, 228, 53,  221, 116, 247, 93,  130, 107, 191, 205, 135,
        47,  34,  81,  158, 187, 237, 238, 205, 169, 250, 116, 35,  209, 133, 75,  224, 209, 16,
        102, 238, 120, 197, 15,  214, 228, 80,  191, 43,  96,  128, 195, 153, 16,  205, 165, 246,
        33,  174, 224, 220, 10,  227, 125, 112, 139, 104, 165, 221, 74,  115, 143, 42,  82,  107,
        239, 201, 33,  107, 198, 232, 65,  244, 28,  187, 35,  100, 7,   61,  57,  126, 71,  250,
        121, 176, 169, 76,  210, 214, 94,  65,  151, 19,  19,  206, 174, 100, 225, 84,  221, 57,
        148, 2,   71,  94,  69,  107, 150, 48,  228, 176, 172, 16,  90,  178, 54,  11,  248, 254,
        200, 76,  31,  176, 61,  108, 19,  14,  25,  28,  230, 139, 27,  220, 45,  122, 94,  165,
        193, 77,  163, 100, 45,  230, 153, 187, 132, 37,  205, 76,  146, 160, 68,  73,  226, 159,
        119, 209, 113, 184, 191, 38,  115, 167, 229, 70,  248, 160, 113, 181, 165, 24,  131, 9,
        215, 81,  109, 28,  227, 218, 56,  241, 64,  189, 190, 208, 119, 174, 178, 36,  84,  8,
        63,  164, 240, 125, 183, 98,  66,  65,  16,  81,  27,  211, 28,  72,  92,  230, 19,  1,
        203, 98,  159, 185, 240, 131, 203, 178, 2,   72,  118, 77,  168, 138, 255, 137, 57,  206,
        49,  10,  186, 86,  73,  96,  223, 99,  15,  199, 57,  192, 19,  102, 65,  201, 204, 13,
        52,  135, 245, 104, 123, 157, 231, 1,   155, 18,  221, 208, 33,  90,  219, 223, 246, 86,
        178, 223, 197, 111, 238, 209, 110, 226, 93,  249, 129, 216, 219, 228, 196, 13,  33,  160,
        44,  235, 101, 182, 34,  190, 7,   224, 235, 208, 250, 31,  237, 9,   223, 48,  49,  136,
        204, 222, 1,   3,   235, 249, 121, 43,  180, 96,  247, 242, 216, 191, 55,  15,  249, 64,
        114, 10,  81,  60,  207, 137, 211, 106, 124, 242, 203, 179, 157, 169, 108, 208, 123, 103,
        117, 124, 75,  16,  72,  83,  78,  23,  39,  13,  14,  112, 140, 218, 135, 248, 86,  119,
        123, 14,  207, 2,   225, 70,  253, 62,  23,  10,  181, 4,   105, 209, 225, 21,  207, 58,
        188, 23,  234, 96,  218, 88,  249, 50,  48,  173, 160, 65,  63,  150, 143, 117, 93,  33,
        67,  121, 84,  66,  192, 55,  61,  216, 220, 122, 41,  172, 14,  71,  227, 132, 17,  76,
        92,  131, 147, 194, 218, 11,  192, 155, 244, 171, 32,  141, 150, 212, 40,  80,  222, 30,
        78,  135, 17,  201, 234, 34,  215, 182, 200, 11,  183, 65,  173, 11,  120, 130, 77,  165,
        100, 197, 47,  14,  24,  47,  54,  213, 229, 78,  252, 142, 27,  132, 138, 210, 5,   204,
        42,  42,  105, 136, 239, 188, 185, 111, 204, 57,  181, 255, 234, 163, 184, 193, 248, 85,
        24,  168, 77,  74,  75,  177, 145, 46,  134, 113, 211, 134, 49,  185, 60,  191, 42,  82,
        42,  222, 104, 114, 33,  24,  40,  24,  1,   64,  114, 61,  105, 90,  33,  176, 216, 255,
        250, 31,  154, 37,  85,  209, 154, 3,   111, 237, 63,  27,  86,  116, 83,  5,   44,  150,
        129, 96,  217, 64,  127, 122, 100, 133, 149, 177, 213, 221, 185, 138, 101, 96,  46,  193,
        192, 127, 228, 72,  248, 95,  89,  206, 145, 84,  249, 108, 105, 124, 104, 165, 39,  213,
        34,  234, 194, 8,   213, 52,  4,   41,  203, 59,  243, 86,  75,  141, 73,  248, 203, 245,
        122, 116, 51,  28,  27,  101, 223, 86,  170, 125, 114, 66,  196, 94,  255, 33,  231, 245,
        67,  165, 203, 130, 89,  42,  252, 121, 81,  205, 195, 208, 253, 150, 61,  195, 122, 104,
        47,  63,  123, 62,  249, 165, 226, 220, 45,  57,  252, 103, 236, 90,  61,  96,  222, 219,
        163, 14,  199, 182, 131, 132, 41,  160, 59,  69,  48,  234, 95,  179, 134, 179, 120, 219,
        227, 138, 30,  116, 202, 204, 87,  187, 155, 240, 232, 35,  216, 150, 96,  236, 152, 84,
        138, 14,  117, 237, 219, 147, 2,   59,  24,  38,  241, 123, 52,  244, 202, 253, 48,  2,
        3,   64,  29,  43,  98,  75,  171, 227, 215, 112, 55,  141, 31,  25,  241, 71,  6,   73,
        71,  207, 152, 195, 93,  175, 105, 201, 67,  0,   106, 136, 71,  172, 2,   39,  179, 124,
        75,  43,  176, 172, 207, 62,  68,  203, 192, 80,  170, 70,  84,  193, 124, 139, 122, 191,
        235, 175, 47,  66,  234, 154, 192, 180, 52,  131, 251, 108, 81,  34,  59,  9,   102, 154,
        60,  122, 154, 171, 152, 100, 135, 162, 237, 121, 140, 130, 20,  250, 5,   183, 185, 35,
        165, 197, 199, 92,  238, 211, 18,  108, 131, 129, 221, 204, 1,   219, 56,  124, 187, 72,
        48,  87,  194, 77,  9,   74,  28,  195, 231, 10,  238, 78,  147, 36,  161, 250, 156, 161,
        14,  8,   87,  235, 166, 137, 11,  204, 248, 195, 25,  114, 34,  141, 55,  216, 117, 253,
        41,  238, 231, 46,  0,   158, 112, 217, 119, 98,  66,  15,  13,  181, 83,  22,  97,  132,
        113, 154, 154, 209, 135, 246, 11,  139, 185, 189, 203, 158, 35,  34,  150, 134, 57,  73,
        76,  44,  88,  124, 60,  33,  159, 230, 168, 83,  32,  131, 131, 196, 105, 186, 210, 238,
        43,  148, 95,  154, 99,  212, 20,  97,  46,  112, 166, 210, 116, 85,  176, 160, 247, 29,
        91,  95,  1,   244, 205, 168, 249, 240, 168, 55,  107, 29,  5,   72,  40,  131, 118, 110,
        115, 170, 67,  15,  94,  248, 44,  215, 143, 197, 238, 103, 163, 113, 48,  77,  104, 141,
        76,  89,  10,  65,  68,  109, 38,  251, 252, 225, 6,   195, 134, 104, 51,  243, 58,  239,
        96,  208, 118, 100, 146, 248, 191, 82,  52,  20,  63,  132, 6,   14,  150, 156, 160, 67,
        244, 241, 158, 181, 68,  179, 16,  209, 162, 117, 95,  6,   109, 213, 68,  75,  170, 62,
        27,  73,  179, 130, 135, 184, 9,   116, 193, 200, 156, 159, 173, 248, 188, 42,  171, 79,
        121, 225, 212, 28,  186, 101, 167, 122, 184, 254, 255, 193, 131, 129, 220, 107, 179, 83,
        255, 99,  86,  218, 181, 202, 207, 231, 72,  96,  248, 248, 120, 51,  38,  32,  165, 140,
        131, 193, 48,  144, 77,  254, 231, 235, 178, 54,  26,  30,  72,  213, 255, 186, 236, 68,
        176, 109, 220, 15,  43,  219, 96,  16,  4,   73,  227, 155, 28,  18,  132, 172, 20,  107,
        230, 201, 58,  199, 90,  252, 48,  36,  239, 239, 113, 93,  33,  139, 181, 186, 31,  3,
        51,  27,  215, 242, 164, 92,  175, 185, 147, 78,  163, 20,  221, 29,  220, 45,  151, 17,
        118, 7,   10,  152, 38,  196, 66,  83,  192, 229, 122, 156, 241, 207, 228, 163, 197, 151,
        127, 213, 192, 55,  169, 71,  0,   109, 14,  132, 226, 217, 130, 80,  34,  196, 170, 135,
        2,   42,  227, 136, 40,  204, 233, 225, 51,  68,  227, 218, 67,  223, 17,  188, 70,  234,
        193, 43,  248, 117, 154, 236, 116, 198, 229, 105, 19,  212, 167, 227, 103, 121, 202, 91,
        170, 5,   162, 16,  207, 202, 122, 168, 90,  19,  168, 16,  212, 99,  130, 38,  145, 32,
        107, 148, 61,  234, 138, 204, 177, 49,  169, 189, 70,  236, 239, 54,  123, 110, 120, 7,
        7,   108, 23,  168, 12,  228, 234, 113, 111, 86,  88,  69,  187, 111, 220, 240, 1,   22,
        162, 40,  165, 204, 238, 214, 109, 253, 79,  63,  208, 199, 240, 112, 209, 101, 205, 232,
        3,   125, 77,  19,  129, 95,  242, 107, 25,  86,  64,  202, 221, 84,  65,  184, 205, 10,
        200, 126, 209, 31,  109, 16,  242, 138, 197, 5,   152, 140, 209, 61,  7,   142, 158, 3,
        25,  62,  194, 88,  68,  243, 89,  38,  215, 93,  223, 23,  238, 54,  29,  193, 95,  11,
        204, 136, 91,  31,  103, 159, 16,  136, 254, 30,  153, 97,  123, 195, 71,  40,  128, 185,
        185, 63,  225, 2,   21,  123, 216, 174, 135, 12,  230, 228, 187, 165, 118, 166, 53,  4,
        119, 26,  21,  11,  82,  25,  98,  103, 217, 43,  131, 17,  167, 79,  233, 42,  5,   224,
        66,  202, 237, 167, 171, 62,  176, 58,  123, 62,  120, 115, 194, 117, 76,  9,   165, 88,
        111, 207, 72,  234, 126, 164, 81,  57,  247, 36,  18,  107, 228, 26,  235, 224, 193, 121,
        194, 13,  30,  59,  189, 111, 99,  2,   137, 215, 113, 54,  123, 195, 27,  138, 89,  4,
        135, 170, 181, 70,  241, 207, 225, 194, 20,  24,  210, 174, 93,  255, 139, 190, 100, 245,
        56,  117, 205, 122, 84,  180, 36,  247, 224, 88,  192, 88,  223, 68,  228, 166, 19,  138,
        94,  200, 7,   77,  30,  62,  156, 239, 173, 32,  218, 10,  206, 205, 25,  53,  206, 53,
        88,  117, 217, 77,  169, 146, 236, 193, 80,  231, 215, 20,  140, 188, 201, 203, 46,  114,
        253, 191, 72,  156, 1,   11,  200, 114, 55,  140, 69,  135, 132, 234, 10,  10,  182, 220,
        125, 191, 210, 3,   242, 235, 121, 248, 3,   185, 0,   82,  14,  138, 163, 55,  75,  148,
        141, 249, 111, 171, 146, 186, 229, 128, 143, 234, 138, 154, 20,  168, 231, 203, 200, 228,
        78,  221, 53,  165, 85,  56,  151, 17,  134, 204, 117, 16,  78,  37,  233, 141, 97,  70,
        101, 83,  78,  171, 149, 110, 157, 6,   4,   169, 189, 57,  199, 19,  238, 143, 75,  212,
        99,  143, 100, 148, 245, 35,  202, 193, 231, 47,  145, 184, 90,  19,  73,  192, 155, 22,
        191, 242, 154, 99,  199, 47,  227, 94,  238, 24,  224, 179, 78,  11,  9,   203, 4,   220,
        65,  166, 111, 217, 204, 102, 102, 49,  5,   172, 108, 40,  162, 41,  58,  163, 218, 227,
        85,  22,  128, 119, 255, 213, 11,  162, 167, 82,  29,  75,  85,  22,  59,  246, 38,  214,
        72,  141, 43,  128, 186, 15,  14,  15,  8,   234, 184, 216, 110, 146, 216, 194, 28,  179,
        225, 150, 90,  105, 16,  60,  12,  43,  250, 97,  84,  147, 34,  210, 7,   169, 9,   149,
        144, 51,  7,   66,  166, 56,  236, 164, 68,  22,  46,  250, 75,  39,  28,  178, 37,  194,
        25,  57,  181, 91,  219, 162, 223, 41,  140, 253, 122, 210, 11,  241, 48,  241, 64,  24,
        135, 39,  17,  111, 248, 63,  106, 130, 204, 125, 96,  185, 135, 56,  166, 65,  144, 79,
        162, 102, 63,  74,  2,   233, 97,  174, 163, 94,  60,  205, 53,  0,   31,  32,  100, 160,
        241, 125, 82,  149, 76,  71,  254, 57,  115, 144, 110, 202, 80,  127, 62,  217, 192, 137,
        18,  160, 66,  162, 35,  58,  61,  164, 122, 94,  152, 9,   165, 96,  151, 114, 227, 34,
        57,  23,  109, 13,  177, 32,  126, 11,  129, 218, 100, 201, 15,  244, 196, 147, 218, 132,
        69,  74,  106, 107, 51,  188, 76,  0,   155, 98,  176, 208, 201, 227, 66,  240, 199, 57,
        40,  164, 181, 150, 207, 108, 9,   189, 118, 224, 13,  85,  210, 226, 164, 40,  188, 216,
        154, 69,  197, 120, 141, 192, 63,  82,  232, 59,  131, 20,  73,  98,  17,  203, 69,  62,
        9,   30,  233, 21,  93,  178, 93,  31,  62,  239, 64,  58,  23,  244, 124, 152, 220, 217,
        201, 232, 112, 22,  214, 236, 106, 255, 214, 199, 247, 167, 56,  75,  249, 119, 3,   70,
        181, 200, 18,  51,  181, 222, 254, 55,  140, 73,  188, 226, 219, 178, 32,  154, 100, 243,
        94,  152, 228, 42,  117, 217, 169, 238, 233, 3,   170, 202, 27,  20,  77,  228, 2,   0,
        10,  187, 38,  71,  241, 178, 207, 80,  173, 179, 104, 240, 240, 80,  254, 18,  85,  68,
        207, 84,  123, 221, 135, 130, 191, 220, 55,  240, 49,  153, 246, 31,  237, 180, 227, 161,
        85,  117, 63,  204, 125, 118, 249, 128, 166, 241, 105, 134, 99,  161, 178, 246, 122, 147,
        149, 219, 230, 146, 35,  88,  155, 243, 41,  33,  5,   152, 199, 207, 17,  189, 41,  10,
        104, 251, 49,  135, 172, 167, 7,   100, 179, 23,  144, 187, 125, 136, 109, 148, 147, 158,
        237, 75,  13,  85,  205, 32,  69,  77,  68,  110, 118, 89,  190, 233, 2,   112, 240, 32,
        70,  83,  112, 39,  250, 213, 0,   176, 33,  214, 184, 105, 133, 81,  105, 210, 164, 178,
        134, 53,  140, 70,  51,  137, 91,  247, 197, 34,  237, 125, 28,  155, 18,  177, 245, 232,
        26,  128, 60,  81,  42,  3,   247, 101, 208, 40,  173, 141, 192, 201, 237, 169, 215, 69,
        81,  131, 18,  0,   24,  68,  176, 228, 31,  206, 90,  110, 23,  192, 49,  155, 162, 125,
        216, 12,  254, 71,  97,  127, 150, 95,  119, 148, 14,  7,   146, 49,  189, 142, 124, 176,
        34,  2,   149, 148, 100, 138, 253, 177, 13,  246, 184, 73,  52,  245, 31,  124, 115, 57,
        56,  29,  66,  30,  143, 67,  234, 238, 200, 142, 124, 181, 71,  242, 104, 213, 66,  217,
        221, 20,  205, 200, 67,  8,   50,  115, 24,  106, 220, 110, 83,  213, 211, 5,   148, 88,
        229, 71,  246, 198, 83,  201, 245, 113, 148, 255, 253, 232, 68,  100, 128, 179, 111, 236,
        214, 122, 252, 181, 24,  33,  104, 159, 110, 126, 134, 253, 180, 42,  124, 129, 154, 50,
        53,  169, 118, 215, 133, 223, 11,  89,  206, 48,  197, 73,  21,  44,  24,  123, 68,  237,
        34,  34,  54,  159, 230, 5,   176, 186, 140, 34,  157, 126, 180, 93,  193, 51,  173, 74,
        126, 88,  81,  193, 87,  75,  42,  5,   148, 86,  18,  82,  51,  168, 201, 248, 81,  171,
        180, 161, 21,  157, 76,  52,  196, 209, 18,  23,  83,  190, 47,  202, 10,  11,  160, 27,
        240, 128, 175, 25,  190, 101, 253, 166, 203, 170, 194, 4,   231, 187, 35,  51,  210, 18,
        230, 190, 68,  233, 251, 186, 153, 39,  244, 198, 190, 6,   93,  238, 90,  219, 90,  142,
        2,   83,  200, 145, 27,  116, 104, 90,  49,  178, 83,  221, 152, 135, 146, 160, 218, 127,
        167, 237, 201, 3,   217, 140, 130, 80,  3,   203, 85,  152, 181, 25,  171, 109, 150, 125,
        174, 208, 57,  51,  122, 210, 183, 235, 67,  216, 76,  91,  153, 177, 194, 37,  59,  7,
        29,  59,  22,  128, 158, 28,  159, 251, 75,  14,  73,  67,  61,  219, 104, 179, 87,  238,
        1,   10,  152, 8,   234, 201, 81,  73,  235, 252, 237, 6,   60,  34,  228, 76,  203, 208,
        80,  70,  152, 244, 46,  87,  35,  235, 199, 163, 55,  234, 8,   161, 249, 218, 143, 195,
        243, 240, 166, 225, 146, 251, 61,  140, 39,  144, 183, 177, 182, 191, 207, 17,  157, 17,
        116, 132, 183, 13,  244, 170, 61,  199, 55,  55,  238, 42,  15,  104, 235, 201, 145, 12,
        224, 107, 186, 2,   5,   147, 2,   118, 221, 151, 229, 109, 106, 164, 181, 231, 159, 81,
        183, 193, 104, 130, 240, 35,  71,  154, 217, 35,  1,   131, 30,  136, 139, 54,  225, 211,
        60,  180, 149, 251, 29,  27,  239, 124, 46,  76,  143, 192, 112, 239, 181, 168, 152, 234,
        231, 161, 186, 125, 133, 125, 125, 26,  155, 92,  234, 121, 35,  32,  44,  42,  36,  194,
        215, 204, 95,  243, 27,  83,  34,  121, 94,  14,  201, 210, 193, 248, 242, 131, 217, 12,
        0,   27,  156, 3,   28,  97,  107, 71,  100, 41,  161, 113, 199, 191, 147, 65,  59,  198,
        154, 110, 149, 242, 190, 51,  131, 247, 181, 250, 139, 149, 20,  219, 8,   216, 136, 120,
        248, 207, 144, 172, 125, 141, 138, 65,  122, 73,  47,  61,  155, 41,  112, 122, 143, 225,
        183, 127, 164, 117, 124, 86,  79,  139, 221, 58,  178, 44,  121, 253, 74,  235, 215, 212,
        186, 80,  99,  190, 158, 139, 75,  87,  46,  223, 119, 184, 190, 58,  172, 169, 163, 133,
        100, 90,  129, 131, 197, 188, 229, 227, 27,  94,  112, 177, 64,  142, 92,  184, 37,  58,
        118, 146, 3,   84,  182, 212, 200, 247, 84,  175, 58,  175, 43,  60,  59,  41,  135, 22,
        187, 150, 220, 33,  217, 155, 144, 161, 103, 62,  23,  245, 206, 220, 171, 226, 42,  93,
        135, 18,  95,  199, 133, 57,  223, 240, 155, 92,  79,  149, 108, 247, 201, 97,  168, 186,
        156, 216, 14,  144, 99,  98,  150, 157, 189, 72,  201, 161, 150, 206, 12,  250, 164, 129,
        10,  185, 137, 7,   132, 251, 44,  168, 93,  36,  254, 70,  172, 216, 36,  71,  23,  139,
        217, 7,   243, 248, 116, 173, 192, 78,  51,  86,  66,  212, 117, 143, 161, 159, 187, 246,
        47,  241, 58,  168, 44,  167, 76,  102, 31,  243, 14,  143, 26,  207, 248, 73,  63,  25,
        143, 194, 99,  10,  233, 190, 59,  176, 115, 186, 138, 173, 254, 126, 21,  28,  164, 168,
        208, 229, 163, 252, 142, 20,  29,  224, 74,  203, 80,  193, 0,   198, 231, 211, 236, 111,
        144, 219, 157, 145, 202, 199, 11,  190, 198, 60,  100, 115, 54,  19,  227, 130, 133, 144,
        87,  113, 109, 37,  29,  54,  179, 1,   173, 54,  74,  95,  118, 253, 121, 173, 33,  239,
        186, 221, 238, 163, 16,  240, 129, 233, 33,  39,  35,  206, 203, 54,  46,  78,  158, 102,
        208, 49,  148, 181, 52,  157, 43,  195, 130, 12,  74,  16,  20,  129, 133, 103, 254, 21,
        80,  193, 128, 30,  193, 26,  241, 196, 124, 211, 156, 54,  24,  106, 224, 128, 154, 162,
        117, 128, 60,  37,  115, 90,  43,  235, 217, 95,  146, 68,  141, 171, 126, 152, 174, 226,
        213, 105, 213, 162, 204, 20,  123, 131, 248, 24,  146, 202, 81,  231, 197, 230, 218, 6,
        18,  33,  154, 37,  100, 114, 237, 16,  241, 232, 93,  163, 8,   137, 227, 220, 248, 1,
        178, 200, 123, 49,  86,  250, 108, 239, 48,  14,  108, 80,  175, 244, 10,  191, 73,  100,
        192, 44,  114, 62,  2,   92,  62,  248, 193, 9,   145, 1,   168, 139, 173, 23,  161, 7,
        219, 88,  186, 109, 102, 38,  39,  130, 254, 93,  109, 240, 230, 224, 139, 158, 64,  135,
        158, 200, 221, 218, 192, 244, 60,  31,  216, 56,  167, 33,  195, 13,  235, 74,  10,  214,
        28,  239, 166, 31,  122, 231, 45,  151, 184, 133, 117, 209, 85,  251, 38,  101, 46,  137,
        31,  52,  45,  46,  157, 34,  3,   116, 13,  83,  212, 226, 176, 79,  41,  70,  15,  22,
        154, 124, 164, 19,  97,  212, 197, 119, 97,  140, 254, 38,  25,  158, 173, 158, 4,   251,
        178, 226, 79,  210, 126, 4,   193, 65,  67,  45,  86,  119, 121, 214, 200, 249, 13,  45,
        66,  5,   211, 234, 50,  132, 16,  31,  141, 8,   109, 90,  104, 107, 245, 131, 48,  213,
        107, 211, 101, 37,  30,  145, 187, 189, 114, 96,  236, 136, 8,   138, 69,  59,  143, 121,
        87,  195, 74,  230, 109, 77,  21,  15,  255, 203, 231, 2,   100, 245, 252, 171, 166, 205,
        224, 33,  216, 68,  122, 28,  22,  177, 26,  185, 11,  158, 249, 6,   196, 254, 27,  26,
        51,  75,  144, 232, 51,  43,  48,  94,  12,  179, 170, 187, 171, 196, 236, 206, 91,  59,
        134, 134, 99,  169, 42,  201, 195, 170, 166, 248, 163, 116, 250, 232, 74,  254, 125, 105,
        103, 7,   25,  55,  151, 171, 200, 199, 32,  41,  234, 164, 214, 21,  126, 168, 89,  182,
        6,   114, 45,  18,  66,  117, 167, 185, 223, 174, 145, 213, 173, 13,  120, 2,   17,  2,
        234, 154, 247, 211, 152, 151, 68,  178, 189, 243, 191, 100, 69,  56,  220, 204, 135, 8,
        8,   243, 106, 81,  200, 161, 112, 254, 168, 188, 204, 223, 41,  199, 179, 118, 175, 24,
        80,  181, 170, 251, 119, 53,  153, 144, 106, 152, 193, 156, 110, 2,   76,  22,  210, 189,
        54,  142, 117, 176, 41,  12,  75,  205, 151, 114, 6,   13,  189, 73,  198, 171, 142, 128,
        101, 182, 209, 222, 11,  114, 45,  39,  232, 119, 242, 223, 111, 146, 173, 163, 117, 232,
        158, 82,  227, 218, 55,  181, 153, 209, 191, 44,  235, 153, 169, 79,  18,  202, 33,  154,
        149, 43,  125, 156, 37,  109, 244, 43,  86,  226, 56,  81,  141, 51,  169, 137, 156, 143,
        5,   117, 89,  207, 170, 127, 215, 28,  116, 171, 7,   16,  180, 47,  117, 179, 112, 16,
        184, 46,  223, 127, 230, 15,  226, 49,  143, 34,  161, 0,   74,  190, 164, 205, 33,  6,
        69,  47,  76,  151, 176, 62,  237, 109, 170, 137, 103, 59,  171, 2,   60,  229, 86,  41,
        126, 159, 61,  9,   180, 190, 18,  223, 84,  23,  218, 160, 202, 214, 171, 221, 113, 204,
        223, 96,  236, 3,   154, 57,  72,  156, 53,  119, 189, 98,  142, 172, 23,  47,  223, 207,
        157, 224, 57,  119, 67,  120, 18,  183, 228, 129, 205, 184, 146, 183, 200, 106, 135, 187,
        11,  115, 171, 131, 251, 158, 86,  210, 209, 130, 59,  30,  225, 207, 255, 154, 136, 104,
        232, 164, 216, 111, 202, 39,  238, 160, 200, 71,  247, 26,  87,  37,  249, 213, 204, 171,
        77,  16,  203, 192, 251, 40,  218, 194, 95,  209, 125, 2,   40,  146, 99,  233, 97,  2,
        230, 42,  37,  159, 217, 203, 58,  10,  96,  182, 87,  9,   157, 37,  95,  143, 33,  75,
        0,   126, 233, 64,  250, 86,  148, 124, 114, 213, 94,  31,  0,   78,  79,  79,  222, 171,
        188, 103, 37,  48,  242, 25,  135, 37,  248, 43,  201, 102, 126, 30,  100, 113, 223, 59,
        89,  234, 155, 103, 117, 168, 48,  180, 178, 58,  35,  117, 89,  197, 128, 226, 143, 119,
        68,  57,  227, 238, 78,  14,  8,   80,  192, 90,  193, 1,   100, 162, 163, 120, 57,  94,
        116, 120, 206, 152, 147, 169, 54,  85,  42,  86,  144, 120, 19,  99,  62,  206, 56,  224,
        76,  128, 251, 235, 161, 87,  69,  139, 212, 197, 6,   191, 205, 144, 101, 220, 184, 119,
        103, 245, 6,   58,  183, 235, 138, 35,  144, 193, 33,  211, 70,  5,   69,  3,   102, 203,
        97,  194, 49,  102, 114, 200, 52,  199, 181, 175, 114, 151, 75,  40,  18,  3,   10,  73,
        189, 60,  47,  246, 169, 114, 6,   229, 65,  165, 111, 45,  114, 7,   51,  136, 91,  213,
        86,  46,  225, 115, 110, 169, 124, 107, 27,  26,  32,  226, 191, 146, 204, 127, 15,  192,
        70,  59,  90,  187, 83,  189, 66,  74,  43,  106, 248, 135, 173, 68,  134, 6,   70,  29,
        237, 15,  147, 187, 75,  94,  62,  253, 39,  202, 198, 17,  10,  42,  17,  170, 214, 42,
        24,  75,  111, 46,  1,   39,  12,  20,  163, 215, 188, 178, 161, 61,  191, 80,  127, 254,
        5,   8,   191, 218, 123, 54,  100, 108, 192, 225, 250, 11,  159, 198, 32,  103, 77,  33,
        72,  35,  22,  227, 209, 71,  206, 205, 179, 45,  195, 11,  59,  1,   125, 216, 24,  109,
        81,  44,  177, 108, 134, 55,  180, 112, 120, 22,  112, 47,  193, 108, 184, 206, 50,  45,
        91,  19,  179, 17,  64,  241, 139, 188, 116, 249, 37,  142, 99,  243, 134, 29,  141, 233,
        131, 199, 53,  191, 77,  133, 1,   130, 226, 123, 237, 139, 236, 88,  124, 251, 156, 103,
        146, 19,  194, 154, 14,  104, 17,  186, 224, 105, 154, 25,  43,  154, 2,   100, 58,  218,
        113, 159, 64,  135, 241, 111, 232, 168, 201, 201, 255, 210, 172, 125, 247, 147, 19,  14,
        159, 203, 141, 177, 112, 138, 78,  106, 147, 76,  34,  124, 193, 212, 81,  244, 140, 12,
        98,  186, 140, 231, 112, 24,  201, 156, 35,  87,  63,  226, 132, 82,  242, 34,  126, 13,
        25,  93,  191, 210, 33,  108, 109, 167, 141, 54,  137, 243, 188, 174, 7,   154, 34,  150,
        166, 58,  77,  250, 40,  77,  63,  235, 52,  242, 134, 63,  14,  44,  133, 88,  72,  217,
        4,   216, 7,   212, 165, 80,  138, 105, 150, 212, 172, 80,  213, 55,  164, 231, 22,  234,
        128, 223, 80,  53,  230, 137, 13,  232, 174, 84,  130, 18,  222, 110, 192, 9,   114, 210,
        213, 161, 76,  212, 89,  121, 158, 133, 55,  189, 140, 211, 65,  214, 202, 133, 208, 12,
        223, 229, 237, 202, 131, 113, 131, 128, 189, 56,  172, 139, 26,  159, 43,  75,  179, 164,
        52,  199, 246, 144, 186, 76,  255, 50,  0,   243, 35,  107, 159, 192, 242, 228, 194, 64,
        173, 204, 29,  200, 146, 187, 117, 191, 55,  215, 123, 148, 157, 165, 204, 129, 176, 90,
        56,  155, 10,  86,  24,  237, 70,  159, 164, 238, 197, 183, 218, 222, 23,  227, 139, 159,
        121, 169, 153, 244, 163, 59,  220, 128, 211, 45,  186, 64,  205, 15,  108, 212, 2,   16,
        99,  232, 130, 209, 70,  224, 159, 2,   183, 217, 187, 41,  43,  62,  121, 158, 237, 206,
        181, 1,   41,  115, 64,  118, 11,  122, 24,  47,  119, 19,  252, 15,  79,  206, 35,  27,
        244, 246, 90,  32,  145, 55,  118, 149, 255, 220, 93,  169, 24,  253, 78,  247, 129, 226,
        45,  106, 190, 92,  81,  86,  92,  107, 235, 67,  94,  236, 37,  184, 157, 40,  70,  69,
        210, 210, 229, 208, 45,  199, 17,  25,  21,  63,  155, 44,  200, 30,  112, 139, 74,  123,
        197, 199, 131, 127, 176, 222, 164, 226, 78,  206, 56,  140, 9,   137, 152, 44,  246, 78,
        127, 250, 215, 40,  181, 188, 57,  235, 36,  37,  183, 16,  21,  70,  166, 215, 58,  172,
        137, 159, 76,  226, 5,   140, 62,  246, 223, 238, 54,  15,  2,   212, 228, 87,  10,  80,
        27,  172, 202, 63,  128, 132, 117, 56,  254, 85,  163, 79,  47,  191, 180, 135, 19,  60,
        133, 249, 43,  165, 183, 91,  179, 0,   184, 162, 237, 99,  61,  44,  62,  72,  27,  64,
        12,  82,  15,  231, 212, 134, 147, 94,  247, 63,  62,  65,  203, 62,  63,  208, 1,   254,
        11,  10,  73,  133, 42,  53,  81,  151, 235, 54,  106, 23,  106, 133, 187, 45,  67,  11,
        220, 225, 54,  217, 139, 72,  65,  24,  218, 101, 144, 141, 196, 88,  138, 115, 231, 49,
        182, 77,  105, 63,  10,  186, 191, 129, 125, 7,   195, 86,  255, 239, 79,  88,  91,  104,
        198, 237, 25,  142, 184, 215, 24,  191, 254, 220, 211, 216, 176, 142, 167, 54,  180, 238,
        41,  193, 177, 183, 134, 162, 33,  208, 20,  151, 217, 77,  238, 191, 232, 55,  76,  60,
        117, 52,  87,  56,  61,  110, 238, 135, 122, 201, 202, 58,  85,  125, 4,   23,  134, 232,
        10,  23,  18,  149, 122, 116, 28,  243, 196, 33,  22,  171, 33,  243, 48,  180, 94,  122,
        145, 168, 54,  194, 80,  56,  79,  188, 144, 153, 154, 185, 219, 200, 76,  64,  155, 221,
        18,  102, 43,  112, 37,  62,  215, 95,  63,  190, 161, 149, 194, 23,  15,  237, 228, 170,
        241, 171, 168, 210, 81,  232, 66,  145, 32,  105, 240, 146, 3,   115, 36,  144, 15,  223,
        90,  251, 152, 171, 182, 191, 144, 220, 16,  72,  80,  126, 119, 247, 78,  146, 88,  3,
        164, 181, 76,  161, 23,  92,  211, 178, 84,  20,  250, 218, 69,  22,  222, 204, 41,  89,
        82,  160, 91,  96,  118, 47,  242, 172, 147, 51,  15,  233, 200, 124, 62,  53,  24,  91,
        89,  45,  83,  20,  221, 234, 89,  42,  62,  67,  221, 9,   63,  113, 34,  170, 117, 42,
        102, 152, 22,  20,  48,  74,  15,  0,   163, 53,  243, 82,  198, 54,  209, 39,  124, 42,
        178, 14,  199, 11,  222, 84,  44,  34,  240, 146, 136, 128, 225, 36,  94,  254, 224, 229,
        25,  174, 220, 11,  76,  118, 61,  19,  226, 54,  62,  108, 218, 152, 42,  27,  191, 132,
        254, 53,  232, 167, 57,  220, 70,  207, 187, 24,  35,  210, 251, 71,  39,  236, 56,  136,
        113, 157, 188, 131, 165, 134, 182, 87,  231, 60,  96,  211, 101, 222, 235, 215, 86,  227,
        123, 98,  83,  56,  175, 252, 13,  209, 18,  192, 168, 169, 155, 106, 207, 99,  176, 59,
        188, 244, 30,  58,  77,  168, 180, 214, 80,  200, 228, 12,  206, 13,  250, 58,  114, 62,
        128, 147, 68,  34,  88,  176, 235, 17,  174, 36,  154, 126, 129, 108, 165, 166, 178, 92,
        123, 210, 71,  101, 101, 240, 159, 50,  171, 95,  93,  19,  216, 232, 131, 90,  247, 173,
        1,   228, 223, 125, 209, 118, 54,  67,  56,  130, 23,  3,   132, 88,  68,  93,  130, 52,
        250, 57,  196, 172, 165, 212, 49,  90,  87,  98,  33,  200, 254, 203, 199, 43,  50,  177,
        150, 18,  41,  218, 3,   84,  7,   124, 244, 188, 128, 197, 143, 83,  175, 18,  113, 47,
        6,   233, 102, 247, 217, 228, 88,  218, 113, 248, 102, 10,  110, 96,  67,  13,  104, 162,
        181, 228, 52,  147, 61,  6,   240, 61,  222, 31,  174, 255, 180, 217, 140, 153, 208, 148,
        41,  216, 59,  245, 15,  182, 31,  113, 248, 211, 85,  34,  120, 161, 67,  179, 95,  46,
        232, 227, 209, 191, 162, 73,  67,  98,  20,  32,  16,  159, 167, 118, 72,  41,  110, 17,
        152, 39,  57,  140, 245, 99,  248, 197, 255, 63,  72,  155, 132, 18,  87,  207, 241, 17,
        82,  232, 4,   159, 9,   135, 30,  54,  7,   228, 220, 133, 170, 58,  103, 50,  123, 159,
        107, 57,  143, 7,   186, 29,  130, 69,  199, 39,  60,  242, 30,  36,  40,  159, 246, 70,
        127, 45,  45,  194, 113, 31,  113, 85,  63,  94,  248, 45,  180, 242, 52,  57,  50,  68,
        243, 122, 83,  40,  233, 249, 30,  93,  74,  34,  165, 209, 93,  158, 88,  39,  29,  67,
        73,  239, 137, 0,   141, 105, 31,  61,  52,  206, 146, 61,  43,  169, 249, 144, 64,  24,
        112, 195, 219, 141, 197, 15,  11,  232, 55,  225, 231, 167, 33,  227, 86,  182, 213, 83,
        138, 184, 25,  111, 80,  135, 115, 203, 120, 134, 43,  54,  118, 104, 222, 51,  33,  70,
        91,  144, 226, 163, 132, 241, 117, 252, 80,  218, 68,  28,  246, 147, 63,  212, 205, 175,
        225, 156, 189, 64,  77,  113, 141, 85,  64,  155, 158, 11,  243, 177, 60,  194, 116, 132,
        214, 175, 191, 86,  106, 132, 37,  70,  166, 36,  75,  129, 212, 95,  197, 227, 49,  17,
        143, 54,  18,  21,  146, 162, 132, 154, 42,  222, 252, 176, 80,  59,  219, 26,  118, 103,
        154, 204, 236, 158, 63,  124, 94,  30,  37,  38,  93,  195, 182, 7,   89,  157, 254, 0,
        129, 176, 6,   40,  241, 126, 20,  247, 202, 48,  206, 118, 190, 200, 72,  2,   241, 47,
        223, 122, 191, 82,  127, 46,  14,  130, 30,  170, 123, 249, 18,  54,  201, 151, 143, 78,
        73,  6,   168, 152, 122, 24,  7,   42,  175, 56,  56,  193, 112, 137, 173, 34,  199, 12,
        236, 144, 192, 161, 186, 205, 246, 141, 100, 79,  70,  188, 140, 93,  131, 10,  138, 255,
        185, 149, 22,  176, 215, 163, 131, 214, 9,   59,  11,  29,  100, 208, 239, 15,  98,  76,
        78,  35,  129, 191, 16,  208, 123, 147, 25,  232, 200, 252, 179, 69,  39,  240, 34,  1,
        183, 255, 65,  243, 65,  220, 163, 110, 151, 85,  72,  180, 100, 208, 49,  1,   7,   23,
        52,  220, 14,  181, 106, 192, 148, 47,  175, 168, 40,  216, 118, 15,  65,  245, 218, 140,
        0,   165, 169, 58,  246, 118, 79,  14,  59,  45,  185, 250, 39,  20,  207, 87,  73,  123,
        90,  194, 55,  45,  168, 21,  97,  59,  116, 137, 97,  91,  86,  130, 18,  150, 187, 123,
        43,  112, 93,  14,  224, 175, 47,  228, 179, 207, 130, 237, 155, 51,  185, 249, 83,  94,
        38,  59,  64,  41,  93,  226, 113, 129, 48,  253, 166, 49,  56,  39,  114, 108, 196, 118,
        81,  159, 250, 186, 49,  110, 79,  18,  21,  223, 66,  227, 97,  0,   162, 114, 114, 199,
        164, 192, 194, 73,  125, 139, 210, 195, 254, 69,  31,  101, 63,  227, 15,  249, 180, 137,
        93,  98,  190, 214, 238, 130, 92,  193, 44,  108, 114, 33,  107, 86,  186, 84,  26,  20,
        100, 160, 12,  59,  225, 43,  22,  80,  238, 66,  159, 61,  86,  200, 165, 127, 34,  57,
        0,   9,   251, 63,  83,  172, 206, 229, 225, 69,  128, 199, 49,  120, 62,  127, 178, 144,
        72,  29,  81,  158, 57,  107, 9,   77,  182, 232, 171, 220, 215, 183, 72,  176, 85,  237,
        203, 99,  218, 1,   81,  89,  225, 227, 51,  208, 227, 20,  132, 193, 205, 96,  94,  10,
        55,  27,  92,  83,  0,   184, 199, 133, 173, 21,  105, 13,  70,  95,  92,  4,   249, 251,
        121, 220, 53,  91,  65,  147, 235, 105, 36,  86,  201, 240, 222, 182, 105, 96,  40,  197,
        230, 253, 131, 255, 226, 157, 42,  155, 119, 55,  31,  85,  225, 88,  220, 231, 106, 136,
        239, 127, 240, 39,  157, 123, 27,  191, 8,   157, 247, 206, 2,   108, 170, 74,  29,  153,
        99,  184, 243, 218, 200, 154, 6,   73,  43,  169, 66,  181, 29,  98,  199, 150, 30,  35,
        217, 171, 174, 195, 35,  75,  174, 104, 4,   131, 70,  53,  8,   143, 41,  74,  84,  5,
        128, 26,  84,  37,  82,  225, 53,  4,   29,  96,  197, 228, 149, 248, 42,  230, 118, 220,
        15,  243, 112, 95,  74,  227, 83,  99,  107, 253, 242, 48,  127, 74,  53,  128, 33,  248,
        100, 24,  132, 142, 2,   117, 218, 29,  108, 153, 194, 119, 239, 219, 189, 200, 233, 24,
        117, 64,  117, 148, 160, 162, 178, 36,  201, 162, 148, 103, 215, 94,  223, 86,  195, 140,
        214, 224, 106, 7,   87,  9,   58,  84,  89,  1,   178, 176, 152, 250, 44,  117, 94,  147,
        182, 113, 102, 108, 45,  47,  89,  19,  184, 156, 100, 20,  202, 74,  239, 141, 205, 178,
        73,  175, 15,  254, 59,  124, 137, 62,  153, 142, 151, 223, 200, 94,  240, 244, 61,  31,
        201, 41,  36,  117, 85,  1,   87,  96,  50,  204, 126, 221, 209, 135, 142, 85,  32,  255,
        234, 138, 110, 3,   186, 130, 226, 205, 230, 20,  68,  128, 58,  30,  94,  102, 17,  242,
        24,  159, 133, 118, 219, 128, 128, 69,  54,  31,  40,  94,  50,  241, 17,  58,  163, 44,
        191, 220, 192, 250, 39,  44,  221, 51,  179, 141, 212, 98,  43,  8,   187, 81,  106, 174,
        105, 9,   249, 229, 49,  237, 130, 191, 108, 22,  137, 201, 254, 113, 234, 127, 229, 25,
        97,  76,  154, 206, 102, 89,  180, 231, 32,  155, 207, 20,  178, 72,  194, 11,  186, 71,
        234, 187, 193, 20,  219, 129, 244, 64,  105, 84,  34,  201, 139, 116, 168, 171, 140, 80,
        5,   57,  172, 56,  222, 88,  55,  170, 9,   156, 145, 23,  28,  203, 254, 245, 46,  108,
        239, 207, 239, 204, 98,  172, 82,  152, 238, 149, 121, 42,  124, 109, 91,  190, 15,  213,
        235, 88,  170, 144, 104, 135, 199, 154, 96,  233, 253, 192, 156, 133, 252, 110, 200, 19,
        120, 136, 224, 150, 208, 7,   242, 138, 98,  207, 137, 49,  82,  200, 180, 211, 52,  74,
        84,  221, 20,  230, 173, 79,  237, 231, 189, 225, 186, 119, 8,   207, 212, 91,  75,  236,
        246, 30,  57,  7,   239, 40,  242, 98,  144, 138, 22,  145, 181, 242, 213, 176, 114, 65,
        215, 6,   197, 149, 193, 90,  198, 21,  84,  157, 173, 124, 85,  219, 168, 215, 100, 59,
        4,   24,  196, 37,  7,   113, 220, 119, 96,  38,  28,  10,  155, 16,  99,  79,  243, 223,
        187, 253, 31,  128, 91,  46,  211, 242, 77,  243, 53,  194, 109, 207, 57,  216, 83,  217,
        153, 174, 8,   122, 62,  56,  3,   142, 142, 7,   129, 149, 215, 32,  165, 131, 169, 129,
        227, 160, 37,  150, 85,  154, 109, 255, 3,   142, 87,  197, 164, 66,  76,  228, 12,  244,
        169, 184, 253, 16,  14,  111, 226, 228, 69,  34,  250, 23,  173, 145, 38,  41,  27,  125,
        185, 182, 37,  77,  87,  157, 222, 173, 209, 250, 202, 65,  84,  119, 209, 75,  23,  227,
        34,  121, 166, 144, 7,   24,  71,  18,  87,  124, 16,  220, 61,  187, 188, 38,  183, 34,
        36,  95,  128, 245, 60,  226, 226, 53,  193, 226, 58,  208, 11,  224, 151, 165, 204, 87,
        89,  106, 39,  231, 55,  155, 22,  202, 169, 111, 107, 135, 127, 221, 151, 21,  164, 135,
        178, 91,  5,   175, 0,   158, 60,  213, 172, 243, 121, 54,  223, 42,  107, 80,  132, 220,
        193, 157, 174, 106, 145, 35,  81,  132, 95,  253, 221, 202, 228, 153, 229, 141, 202, 235,
        212, 198, 130, 97,  15,  208, 158, 180, 199, 219, 227, 58,  28,  252, 205, 225, 165, 183,
        187, 120, 44,  139, 60,  113, 104, 33,  143, 62,  244, 140, 106, 219, 185, 33,  140, 9,
        60,  8,   27,  132, 255, 108, 88,  14,  157, 86,  23,  92,  219, 61,  255, 39,  250, 71,
        126, 243, 68,  163, 40,  199, 218, 24,  68,  14,  133, 169, 45,  86,  91,  159, 204, 136,
        142, 105, 216, 45,  219, 232, 192, 16,  30,  215, 205, 79,  241, 190, 148, 118, 17,  223,
        229, 246, 0,   145, 91,  54,  227, 171, 13,  123, 152, 97,  209, 218, 137, 171, 99,  85,
        239, 91,  203, 27,  50,  183, 164, 18,  75,  51,  110, 161, 97,  33,  49,  5,   75,  216,
        211, 55,  213, 122, 39,  93,  52,  9,   11,  139, 4,   59,  109, 68,  69,  104, 161, 73,
        62,  244, 156, 133, 239, 74,  1,   92,  167, 225, 42,  112, 88,  40,  119, 201, 142, 221,
        76,  233, 130, 71,  163, 119, 164, 123, 96,  56,  229, 218, 142, 63,  36,  185, 221, 238,
        97,  8,   199, 107, 240, 9,   35,  159, 218, 61,  54,  165, 88,  149, 159, 186, 204, 70,
        132, 48,  77,  179, 24,  55,  130, 27,  65,  52,  133, 222, 38,  84,  134, 69,  60,  32,
        11,  25,  35,  85,  45,  219, 161, 72,  88,  125, 24,  17,  79,  31,  220, 235, 146, 16,
        101, 41,  23,  153, 80,  56,  117, 189, 71,  89,  26,  83,  98,  15,  228, 190, 101, 243,
        154, 89,  111, 69,  95,  6,   1,   24,  212, 6,   159, 138, 231, 187, 140, 197, 28,  178,
        178, 14,  31,  173, 167, 110, 251, 65,  62,  226, 46,  251, 214, 139, 246, 228, 250, 33,
        36,  18,  124, 124, 157, 183, 4,   173, 179, 42,  198, 73,  162, 132, 67,  149, 218, 194,
        190, 79,  148, 36,  88,  49,  172, 246, 155, 195, 117, 8,   192, 201, 134, 123, 250, 83,
        155, 168, 6,   215, 205, 223, 192, 6,   211, 145, 67,  189, 119, 26,  156, 209, 11,  72,
        63,  113, 158, 199, 136, 133, 244, 102, 226, 141, 194, 235, 197, 82,  41,  54,  228, 164,
        1,   1,   26,  186, 178, 71,  215, 19,  192, 34,  97,  1,   15,  254, 3,   239, 63,  18,
        169, 132, 12,  212, 168, 181, 175, 246, 119, 82,  204, 199, 161, 15,  153, 173, 232, 184,
        26,  83,  135, 218, 112, 40,  194, 146, 80,  17,  185, 220, 75,  254, 189, 142, 170, 134,
        250, 158, 118, 218, 129, 40,  170, 84,  132, 193, 25,  206, 52,  234, 65,  19,  190, 175,
        47,  160, 139, 158, 23,  104, 251, 109, 236, 74,  182, 174, 11,  201, 233, 110, 86,  169,
        165, 204, 181, 101, 153, 122, 168, 41,  121, 145, 89,  167, 78,  199, 59,  126, 23,  177,
        119, 169, 101, 4,   160, 98,  58,  31,  76,  93,  36,  82,  97,  22,  125, 207, 201, 217,
        47,  133, 196, 233, 42,  233, 8,   214, 233, 178, 221, 64,  218, 40,  12,  243, 25,  108,
        231, 48,  37,  13,  242, 200, 131, 77,  80,  152, 242, 32,  43,  61,  89,  58,  77,  89,
        41,  141, 209, 125, 169, 186, 215, 48,  216, 228, 80,  206, 60,  40,  207, 35,  5,   42,
        30,  221, 128, 158, 208, 23,  197, 38,  46,  123, 109, 150, 3,   152, 80,  181, 166, 84,
        136, 197, 88,  193, 43,  67,  242, 157, 119, 207, 165, 51,  156, 14,  76,  28,  185, 171,
        236, 232, 9,   35,  248, 13,  135, 221, 249, 47,  236, 63,  65,  124, 233, 209, 51,  231,
        128, 111, 174, 172, 118, 120, 113, 3,   98,  109, 204, 17,  112, 133, 114, 97,  166, 234,
        150, 149, 7,   80,  163, 246, 60,  140, 250, 215, 13,  254, 93,  181, 154, 1,   162, 224,
        108, 54,  246, 44,  1,   21,  99,  74,  11,  246, 121, 248, 216, 235, 232, 236, 15,  239,
        117, 103, 247, 36,  56,  32,  159, 32,  245, 138, 206, 235, 25,  215, 142, 165, 95,  147,
        215, 194, 178, 96,  148, 34,  108, 125, 175, 132, 15,  104, 6,   81,  80,  178, 195, 58,
        178, 102, 41,  241, 80,  165, 134, 185, 68,  214, 147, 135, 223, 39,  119, 197, 96,  123,
        57,  187, 46,  159, 141, 169, 101, 229, 23,  134, 21,  12,  191, 238, 30,  30,  156, 192,
        224, 246, 191, 29,  93,  147, 59,  112, 211, 227, 40,  111, 26,  174, 205, 187, 84,  174,
        171, 219, 52,  191, 179, 12,  49,  108, 250, 108, 12,  191, 204, 193, 215, 185, 138, 209,
        39,  69,  197, 129, 67,  220, 100, 88,  230, 233, 70,  188, 118, 63,  147, 111, 162, 121,
        11,  214, 244, 197, 249, 214, 243, 210, 242, 225, 233, 83,  142, 194, 222, 219, 118, 57,
        204, 58,  91,  114, 107, 140, 61,  209, 20,  21,  167, 35,  143, 61,  151, 90,  3,   249,
        53,  254, 206, 133, 40,  49,  155, 198, 132, 145, 227, 128, 25,  76,  69,  152, 110, 233,
        23,  41,  113, 252, 163, 130, 153, 6,   99,  206, 145, 190, 55,  40,  154, 238, 186, 221,
        240, 218, 227, 21,  186, 96,  211, 177, 122, 146, 183, 173, 180, 215, 81,  218, 68,  45,
        169, 76,  241, 173, 255, 173, 122, 6,   190, 226, 10,  73,  113, 95,  174, 174, 42,  197,
        145, 242, 213, 99,  234, 5,   116, 144, 120, 199, 27,  68,  60,  209, 3,   228, 139, 188,
        82,  143, 149, 91,  77,  143, 216, 109, 162, 53,  41,  56,  216, 200, 255, 23,  66,  157,
        190, 204, 93,  182, 184, 5,   215, 116, 131, 244, 201, 188, 22,  185, 66,  13,  103, 96,
        182, 213, 78,  119, 122, 232, 14,  104, 4,   17,  14,  111, 241, 220, 208, 86,  185, 207,
        168, 196, 13,  252, 104, 242, 87,  107, 150, 52,  240, 216, 61,  149, 55,  87,  39,  4,
        219, 81,  109, 204, 0,   32,  91,  113, 221, 119, 252, 137, 18,  28,  45,  14,  111, 226,
        220, 181, 117, 36,  226, 64,  133, 55,  166, 204, 73,  226, 21,  210, 19,  194, 58,  89,
        193, 245, 152, 210, 223, 206, 164, 254, 11,  64,  99,  134, 102, 169, 246, 137, 179, 55,
        38,  55,  13,  181, 245, 175, 214, 114, 60,  40,  36,  56,  205, 192, 101, 247, 10,  47,
        176, 223, 45,  76,  41,  202, 24,  254, 249, 27,  112, 122, 96,  29,  99,  206, 14,  103,
        192, 83,  11,  188, 181, 17,  207, 39,  99,  92,  101, 96,  11,  10,  121, 61,  128, 101,
        230, 132, 78,  213, 214, 133, 39,  68,  188, 187, 234, 129, 226, 246, 100, 220, 56,  173,
        12,  104, 12,  135, 155, 164, 31,  57,  137, 28,  158, 238, 138, 5,   174, 108, 189, 45,
        185, 216, 158, 213, 70,  153, 229, 71,  255, 240, 126, 50,  117, 95,  195, 173, 160, 195,
        249, 57,  211, 196, 133, 243, 141, 6,   188, 43,  129, 160, 78,  237, 150, 99,  124, 95,
        79,  204, 206, 151, 183, 131, 115, 199, 237, 29,  205, 184, 229, 208, 102, 125, 61,  215,
        150, 222, 184, 19,  139, 168, 97,  236, 161, 144, 56,  2,   9,   245, 36,  154, 246, 192,
        32,  237, 233, 105, 30,  199, 19,  45,  14,  181, 174, 184, 100, 136, 100, 46,  7,   163,
        241, 185, 209, 156, 136, 205, 41,  23,  181, 188, 138, 226, 0,   145, 30,  85,  76,  42,
        94,  153, 243, 198, 109, 220, 0,   215, 78,  75,  45,  195, 104, 43,  234, 183, 250, 34,
        138, 5,   44,  53,  146, 176, 81,  166, 99,  74,  205, 181, 74,  79,  116, 153, 187, 117,
        144, 0,   114, 173, 166, 130, 66,  235, 87,  173, 249, 208, 76,  177, 97,  136, 148, 24,
        213, 238, 57,  50,  235, 234, 195, 193, 87,  90,  39,  38,  51,  70,  42,  160, 123, 211,
        145, 76,  139, 145, 105, 51,  160, 214, 169, 96,  124, 166, 64,  116, 89,  154, 125, 185,
        56,  194, 232, 108, 42,  28,  248, 247, 16,  29,  42,  220, 119, 11,  57,  101, 255, 187,
        68,  42,  235, 25,  109, 158, 126, 147, 9,   33,  27,  75,  167, 117, 61,  88,  210, 217,
        62,  217, 122, 192, 130, 65,  121, 13,  39,  59,  197, 173, 45,  90,  155, 55,  230, 87,
        226, 155, 189, 24,  226, 113, 191, 14,  150, 179, 194, 163, 238, 242, 82,  126, 0,   217,
        167, 183, 49,  142, 46,  244, 210, 27,  183, 99,  254, 208, 34,  182, 253, 55,  196, 23,
        8,   202, 9,   249, 208, 143, 227, 144, 234, 227, 234, 228, 129, 244, 21,  107, 85,  0,
        149, 140, 158, 147, 209, 70,  167, 84,  52,  207, 102, 100, 209, 220, 243, 21,  101, 68,
        140, 134, 144, 215, 19,  253, 203, 39,  255, 148, 84,  41,  80,  211, 71,  199, 106, 117,
        198, 166, 144, 207, 54,  207, 40,  87,  87,  65,  239, 114, 97,  2,   2,   102, 11,  34,
        175, 71,  130, 7,   197, 24,  97,  108, 206, 232, 150, 12,  228, 116, 115, 106, 120, 237,
        69,  160, 153, 242, 125, 158, 126, 36,  45,  92,  0,   164, 108, 131, 124, 55,  222, 94,
        74,  144, 45,  245, 178, 0,   180, 50,  130, 167, 203, 26,  165, 200, 1,   39,  74,  173,
        158, 143, 5,   242, 34,  99,  207, 145, 32,  62,  228, 114, 114, 167, 151, 49,  28,  54,
        53,  3,   59,  214, 42,  149, 81,  145, 136, 112, 16,  143, 17,  236, 200, 132, 81,  42,
        56,  113, 208, 107, 102, 218, 133, 160, 20,  142, 106, 42,  135, 141, 114, 87,  166, 66,
        150, 124, 161, 246, 153, 7,   155, 167, 133, 48,  61,  37,  244, 213, 142, 25,  121, 204,
        209, 140, 35,  134, 97,  151, 135, 198, 58,  80,  252, 51,  213, 234, 56,  235, 231, 16,
        1,   74,  206, 79,  114, 182, 244, 129, 151, 20,  230, 168, 139, 156, 201, 236, 35,  138,
        54,  212, 235, 91,  246, 163, 44,  62,  141, 113, 179, 62,  102, 226, 236, 39,  93,  243,
        33,  27,  24,  24,  182, 60,  38,  54,  175, 52,  9,   251, 35,  214, 56,  166, 208, 50,
        240, 41,  76,  220, 246, 65,  11,  164, 242, 151, 175, 223, 250, 22,  137, 113, 136, 163,
        96,  24,  216, 66,  201, 6,   34,  120, 83,  238, 78,  148, 179, 171, 80,  121, 210, 85,
        27,  184, 45,  175, 118, 199, 249, 172, 224, 244, 166, 182, 86,  193, 251, 132, 170, 235,
        186, 207, 247, 138, 149, 142, 33,  47,  212, 57,  180, 105, 75,  60,  109, 217, 127, 179,
        112, 254, 206, 179, 75,  41,  173, 241, 72,  98,  31,  207, 239, 168, 99,  4,   63,  183,
        159, 3,   163, 5,   218, 100, 170, 45,  171, 158, 65,  157, 124, 156, 112, 160, 13,  71,
        253, 110, 192, 107, 90,  13,  2,   95,  29,  159, 11,  231, 7,   36,  145, 3,   133, 169,
        148, 232, 57,  61,  27,  56,  77,  197, 163, 35,  156, 21,  164, 11,  187, 107, 100, 83,
        58,  242, 25,  173, 77,  127, 151, 116, 45,  50,  188, 62,  191, 195, 7,   54,  89,  16,
        138, 34,  47,  70,  59,  231, 47,  73,  205, 194, 206, 140, 228, 207, 203, 203, 221, 1,
        190, 247, 39,  35,  230, 98,  156, 126, 12,  57,  53,  13,  118, 169, 135, 148, 230, 63,
        165, 19,  246, 119, 238, 182, 131, 243, 75,  1,   163, 146, 207, 54,  83,  180, 6,   62,
        57,  142, 50,  46,  107, 94,  87,  163, 241, 239, 56,  217, 87,  134, 76,  7,   209, 78,
        240, 223, 171, 113, 190, 169, 140, 255, 58,  138, 16,  239, 2,   133, 129, 105, 65,  148,
        159, 61,  125, 205, 159, 218, 107, 0,   201, 185, 105, 151, 56,  89,  45,  213, 28,  150,
        68,  69,  6,   222, 81,  154, 160, 144, 159, 151, 238, 93,  36,  221, 176, 233, 186, 243,
        72,  106, 22,  188, 105, 239, 34,  98,  179, 191, 79,  140, 46,  201, 92,  37,  196, 46,
        144, 199, 210, 224, 180, 158, 5,   175, 58,  79,  104, 227, 95,  37,  41,  116, 202, 127,
        199, 233, 199, 232, 97,  86,  184, 181, 62,  121, 227, 189, 43,  26,  146, 145, 23,  199,
        103, 153, 28,  241, 32,  170, 215, 141, 229, 130, 115, 60,  232, 61,  87,  225, 44,  3,
        114, 251, 72,  64,  46,  87,  36,  177, 214, 178, 197, 252, 6,   155, 152, 172, 223, 207,
        50,  190, 208, 178, 135, 86,  201, 224, 153, 13,  242, 220, 101, 155, 173, 69,  172, 47,
        227, 162, 38,  81,  20,  136, 162, 178, 217, 132, 43,  214, 206, 36,  15,  191, 106, 23,
        185, 160, 198, 146, 225, 155, 82,  154, 100, 135, 235, 130, 116, 121, 198, 135, 134, 60,
        104, 8,   81,  89,  237, 109, 204, 111, 188, 93,  24,  177, 127, 32,  230, 145, 117, 162,
        68,  137, 153, 6,   59,  196, 154, 6,   172, 14,  68,  137, 109, 117, 8,   180, 235, 146,
        70,  129, 16,  255, 81,  51,  43,  151, 48,  135, 55,  103, 215, 88,  221, 188, 203, 70,
        4,   77,  59,  200, 176, 144, 78,  255, 123, 209, 96,  31,  196, 47,  8,   60,  23,  235,
        137, 175, 5,   179, 95,  29,  71,  100, 97,  162, 221, 68,  79,  80,  182, 69,  227, 81,
        117, 113, 218, 162, 181, 233, 140, 197, 85,  98,  255, 13,  13,  210, 52,  132, 24,  64,
        51,  162, 248, 47,  123, 137, 204, 147, 114, 17,  94,  56,  190, 254, 128, 104, 96,  42,
        195, 193, 225, 170, 208, 46,  24,  138, 49,  99,  116, 177, 188, 141, 152, 166, 232, 174,
        2,   129, 8,   201, 227, 41,  136, 177, 136, 137, 73,  55,  93,  246, 26,  54,  100, 207,
        101, 227, 230, 38,  67,  212, 65,  124, 7,   67,  188, 129, 35,  146, 198, 126, 88,  58,
        56,  51,  216, 158, 18,  235, 191, 176, 211, 205, 226, 224, 81,  113, 191, 33,  30,  143,
        114, 67,  232, 180, 182, 227, 147, 85,  22,  53,  13,  230, 149, 163, 104, 153, 16,  113,
        195, 136, 42,  217, 199, 170, 156, 141, 150, 31,  79,  229, 27,  130, 63,  153, 136, 77,
        239, 98,  85,  35,  236, 17,  14,  66,  185, 56,  80,  3,   13,  41,  20,  179, 166, 188,
        194, 150, 240, 131, 85,  156, 91,  253, 91,  68,  225, 199, 148, 64,  200, 173, 251, 239,
        200, 7,   119, 208, 211, 85,  46,  75,  113, 37,  19,  116, 235, 75,  81,  151, 37,  234,
        102, 143, 248, 164, 255, 159, 28,  152, 84,  199, 219, 156, 197, 248, 148, 234, 232, 219,
        191, 175, 41,  7,   15,  189, 225, 173, 39,  40,  133, 170, 27,  64,  210, 8,   82,  62,
        79,  117, 169, 141, 99,  224, 88,  169, 182, 247, 73,  88,  118, 230, 145, 183, 199, 161,
        67,  112, 52,  137, 21,  200, 116, 211, 216, 123, 89,  211, 245, 185, 52,  219, 137, 207,
        50,  38,  79,  23,  22,  120, 244, 155, 41,  164, 20,  90,  116, 2,   209, 193, 214, 51,
        157, 201, 182, 54,  251, 160, 237, 150, 4,   222, 189, 207, 244, 106, 132, 245, 46,  4,
        239, 58,  219, 122, 126, 197, 217, 99,  2,   198, 242, 149, 36,  239, 160, 148, 232, 70,
        162, 196, 53,  203, 116, 116, 155, 120, 182, 185, 240, 227, 183, 237, 145, 171, 126, 36,
        229, 145, 169, 153, 36,  7,   178, 76,  143, 110, 204, 172, 44,  1,   161, 79,  11,  122,
        209, 98,  164, 243, 39,  47,  99,  134, 207, 210, 15,  100, 98,  34,  67,  82,  91,  4,
        241, 182, 185, 170, 30,  134, 129, 49,  167, 57,  11,  249, 254, 209, 30,  209, 83,  183,
        32,  81,  155, 135, 250, 4,   192, 193, 210, 16,  114, 139, 96,  6,   103, 133, 45,  10,
        235, 40,  60,  20,  2,   248, 111, 99,  209, 136, 206, 208, 70,  131, 131, 44,  173, 14,
        114, 30,  190, 137, 24,  30,  189, 8,   141, 144, 230, 89,  69,  59,  161, 183, 7,   127,
        234, 227, 82,  127, 250, 13,  238, 103, 191, 224, 72,  39,  127, 117, 15,  86,  106, 157,
        204, 201, 101, 182, 229, 50,  177, 91,  175, 242, 5,   159, 223, 189, 145, 193, 182, 213,
        36,  141, 248, 127, 36,  51,  41,  172, 78,  79,  136, 151, 230, 190, 61,  34,  30,  76,
        128, 213, 113, 138, 250, 161, 106, 70,  21,  191, 42,  254, 202, 165, 173, 192, 246, 250,
        128, 88,  72,  206, 42,  191, 180, 91,  153, 101, 26,  202, 146, 199, 189, 156, 177, 230,
        139, 28,  59,  78,  21,  232, 112, 76,  14,  81,  41,  148, 214, 28,  255, 217, 132, 173,
        117, 30,  218, 57,  175, 162, 208, 100, 221, 182, 105, 219, 99,  42,  224, 121, 117, 30,
        38,  141, 32,  81,  120, 233, 221, 150, 36,  138, 117, 172, 76,  22,  104, 243, 51,  144,
        151, 21,  86,  164, 15,  83,  136, 252, 89,  17,  28,  60,  41,  197, 249, 211, 122, 95,
        124, 213, 68,  62,  145, 88,  216, 128, 25,  146, 142, 55,  53,  111, 131, 240, 73,  23,
        173, 106, 221, 253, 205, 49,  45,  224, 255, 36,  45,  42,  196, 69,  247, 101, 142, 248,
        235, 16,  194, 164, 246, 223, 71,  238, 59,  40,  235, 12,  163, 2,   49,  28,  118, 107,
        217, 100, 253, 162, 174, 115, 14,  194, 199, 41,  90,  234, 28,  15,  126, 13,  240, 114,
        178, 150, 71,  11,  177, 125, 10,  16,  92,  6,   133, 96,  191, 203, 0,   191, 122, 117,
        133, 8,   69,  185, 80,  117, 164, 71,  5,   89,  239, 82,  197, 83,  157, 132, 20,  169,
        206, 119, 96,  159, 207, 8,   140, 101, 243, 18,  164, 24,  118, 207, 230, 122, 52,  166,
        176, 210, 254, 86,  149, 189, 30,  221, 200, 77,  248, 58,  139, 143, 134, 78,  46,  165,
        182, 232, 154, 184, 231, 3,   141, 38,  154, 79,  197, 16,  53,  74,  61,  57,  9,   89,
        201, 134, 152, 92,  33,  38,  117, 21,  91,  10,  67,  174, 253, 164, 250, 193, 246, 64,
        134, 91,  104, 34,  253, 220, 45,  125, 251, 15,  207, 9,   46,  155, 211, 131, 148, 140,
        32,  17,  106, 248, 136, 15,  124, 218, 23,  32,  206, 37,  95,  10,  123, 67,  69,  3,
        15,  77,  187, 238, 201, 206, 68,  59,  140, 4,   156, 251, 165, 156, 183, 242, 154, 242,
        234, 181, 90,  214, 12,  10,  189, 130, 132, 223, 224, 207, 25,  70,  230, 102, 33,  110,
        68,  4,   252, 150, 74,  160, 61,  111, 135, 164, 251, 46,  186, 233, 171, 251, 228, 200,
        47,  77,  167, 29,  139, 180, 246, 58,  58,  70,  45,  123, 164, 140, 2,   88,  93,  5,
        102, 99,  133, 44,  172, 187, 166, 10,  3,   123, 205, 220, 107, 71,  23,  95,  215, 104,
        139, 225, 39,  41,  230, 37,  50,  174, 159, 98,  56,  197, 80,  234, 76,  92,  119, 176,
        129, 25,  152, 155, 143, 67,  162, 185, 53,  14,  31,  229, 166, 58,  17,  45,  241, 126,
        188, 70,  219, 127, 254, 221, 16,  56,  194, 145, 217, 50,  200, 217, 138, 150, 82,  73,
        76,  214, 82,  13,  159, 244, 229, 104, 99,  97,  159, 166, 108, 57,  64,  82,  130, 19,
        196, 96,  147, 197, 36,  172, 125, 95,  101, 235, 190, 1,   121, 221, 170, 191, 88,  116,
        43,  151, 206, 28,  101, 86,  40,  81,  142, 73,  92,  210, 247, 55,  133, 175, 46,  230,
        236, 218, 176, 200, 235, 94,  214, 0,   89,  36,  119, 25,  5,   3,   65,  71,  129, 176,
        137, 94,  150, 157, 91,  169, 218, 89,  49,  54,  236, 47,  67,  68,  18,  77,  244, 133,
        45,  135, 111, 157, 95,  29,  111, 91,  252, 235, 135, 77,  104, 53,  133, 58,  69,  181,
        167, 193, 235, 60,  100, 251, 55,  119, 70,  193, 148, 107, 253, 95,  178, 2,   53,  52,
        158, 187, 128, 71,  180, 118, 40,  113, 50,  19,  187, 165, 133, 148, 30,  190, 163, 217,
        142, 73,  182, 40,  90,  77,  242, 149, 217, 69,  53,  254, 111, 54,  75,  54,  207, 10,
        250, 69,  97,  3,   84,  12,  145, 70,  27,  245, 168, 156, 122, 112, 178, 55,  79,  36,
        112, 14,  178, 238, 28,  87,  35,  71,  176, 17,  65,  233, 225, 238, 108, 238, 47,  100,
        131, 82,  110, 95,  192, 8,   115, 156, 204, 140, 63,  113, 253, 4,   86,  29,  221, 55,
        110, 83,  32,  46,  116, 156, 126, 197, 158, 64,  254, 220, 5,   18,  59,  171, 216, 131,
        161, 3,   32,  48,  90,  253, 118, 82,  76,  239, 65,  208, 128, 191, 1,   56,  130, 57,
        250, 153, 164, 81,  242, 100, 121, 141, 195, 219, 59,  164, 97,  209, 221, 118, 244, 189,
        199, 92,  193, 53,  223, 175, 186, 35,  44,  92,  213, 147, 212, 167, 166, 66,  121, 58,
        71,  109, 247, 16,  116, 185, 7,   216, 85,  176, 11,  123, 1,   72,  28,  42,  63,  4,
        235, 41,  245, 83,  119, 121, 191, 163, 85,  194, 166, 65,  103, 115, 47,  224, 105, 228,
        130, 37,  101, 66,  96,  236, 177, 170, 154, 141, 69,  113, 19,  98,  185, 21,  1,   34,
        121, 52,  163, 151, 119, 255, 187, 67,  253, 230, 224, 212, 17,  23,  102, 141, 224, 164,
        147, 9,   206, 236, 160, 86,  146, 3,   43,  157, 40,  15,  95,  145, 80,  136, 91,  122,
        96,  245, 88,  6,   108, 183, 103, 242, 8,   193, 246, 149, 73,  251, 139, 22,  81,  15,
        108, 96,  55,  64,  130, 219, 40,  244, 217, 35,  156, 138, 47,  22,  108, 28,  122, 209,
        10,  15,  176, 93,  185, 201, 241, 124, 48,  0,   154, 224, 159, 49,  93,  211, 204, 108,
        43,  228, 64,  147, 189, 186, 3,   184, 225, 215, 169, 97,  228, 105, 211, 128, 201, 35,
        100, 254, 90,  214, 111, 193, 42,  91,  17,  255, 99,  63,  205, 157, 151, 68,  8,   170,
        196, 77,  174, 80,  172, 34,  89,  137, 179, 212, 227, 248, 5,   56,  156, 120, 188, 65,
        160, 68,  221, 63,  115, 185, 0,   221, 27,  47,  228, 129, 176, 51,  88,  153, 203, 196,
        188, 191, 116, 97,  98,  184, 253, 60,  214, 175, 202, 84,  124, 120, 162, 59,  248, 104,
        135, 19,  141, 55,  121, 204, 46,  124, 120, 7,   250, 190, 9,   49,  0,   20,  35,  196,
        182, 194, 7,   253, 133, 128, 167, 184, 194, 67,  24,  201, 49,  44,  128, 219, 213, 116,
        16,  10,  50,  40,  54,  199, 244, 147, 31,  235, 82,  143, 70,  106, 59,  62,  140, 115,
        57,  165, 198, 41,  170, 125, 233, 84,  251, 7,   135, 117, 79,  37,  235, 140, 172, 73,
        211, 29,  171, 3,   69,  30,  101, 88,  239, 141, 49,  237, 56,  150, 206, 61,  8,   52,
        180, 62,  193, 112, 195, 73,  58,  38,  111, 231, 124, 195, 84,  231, 206, 77,  32,  124,
        184, 119, 230, 144, 229, 92,  36,  83,  96,  214, 131, 233, 37,  96,  188, 189, 143, 204,
        223, 239, 40,  45,  243, 47,  21,  46,  91,  199, 183, 15,  148, 212, 59,  227, 202, 173,
        34,  59,  136, 209, 120, 113, 134, 90,  42,  122, 227, 53,  5,   59,  17,  238, 86,  175,
        35,  56,  112, 55,  189, 97,  181, 192, 75,  127, 145, 87,  186, 71,  85,  213, 15,  95,
        154, 81,  202, 225, 245, 39,  45,  91,  28,  198, 211, 39,  189, 113, 86,  106, 40,  157,
        234, 91,  1,   225, 226, 32,  4,   179, 253, 119, 97,  3,   161, 44,  6,   126, 205, 124,
        187, 207, 201, 131, 141, 211, 236, 231, 201, 99,  63,  164, 61,  104, 127, 207, 105, 226,
        188, 216, 225, 167, 122, 78,  220, 122, 19,  62,  156, 27,  41,  193, 131, 206, 70,  177,
        88,  89,  233, 88,  219, 249, 215, 77,  148, 117, 20,  84,  206, 70,  114, 159, 250, 184,
        144, 45,  145, 81,  35,  216, 99,  253, 17,  220, 169, 254, 30,  18,  65,  34,  126, 134,
        200, 87,  231, 146, 117, 142, 104, 2,   166, 14,  83,  33,  34,  70,  233, 224, 255, 79,
        65,  168, 6,   197, 42,  190, 92,  142, 101, 105, 78,  34,  252, 187, 89,  75,  117, 4,
        11,  17,  100, 109, 122, 119, 88,  100, 180, 168, 140, 20,  215, 19,  43,  182, 82,  147,
        73,  118, 31,  38,  133, 7,   74,  219, 74,  168, 201, 22,  178, 134, 232, 40,  185, 219,
        105, 10,  32,  90,  13,  96,  206, 105, 129, 2,   117, 130, 186, 72,  19,  176, 76,  95,
        77,  164, 70,  209, 26,  140, 74,  11,  29,  183, 126, 151, 34,  227, 164, 250, 201, 55,
        13,  78,  226, 199, 152, 11,  112, 48,  211, 85,  142, 69,  140, 138, 200, 119, 18,  15,
        66,  219, 128, 243, 243, 229, 166, 12,  131, 228, 240, 211, 212, 14,  155, 3,   45,  174,
        106, 59,  21,  61,  216, 27,  15,  213, 96,  198, 147, 100, 200, 186, 116, 38,  4,   78,
        53,  85,  29,  15,  103, 206, 39,  121, 80,  171, 30,  29,  161, 62,  121, 29,  225, 15,
        39,  221, 31,  87,  156, 101, 179, 227, 66,  186, 203, 214, 226, 87,  191, 174, 2,   117,
        171, 249, 183, 114, 2,   45,  4,   175, 187, 110, 252, 238, 200, 165, 193, 61,  70,  194,
        230, 165, 29,  167, 159, 103, 203, 64,  27,  231, 85,  138, 90,  108, 74,  234, 33,  141,
        85,  42,  62,  194, 241, 77,  94,  161, 179, 177, 206, 83,  200, 228, 59,  182, 117, 149,
        194, 85,  220, 216, 36,  21,  153, 107, 140, 19,  145, 60,  123, 199, 46,  109, 194, 224,
        160, 152, 190, 172, 32,  209, 23,  72,  211, 190, 107, 13,  177, 214, 211, 36,  104, 223,
        101, 185, 54,  215, 230, 161, 63,  13,  239, 145, 1,   131, 8,   178, 212, 222, 110, 178,
        230, 222, 138, 134, 165, 209, 152, 16,  161, 38,  188, 102, 12,  175, 188, 179, 224, 253,
        22,  32,  86,  185, 58,  92,  189, 106, 142, 167, 115, 94,  117, 239, 51,  1,   186, 126,
        158, 10,  187, 9,   155, 135, 172, 147, 149, 243, 247, 6,   210, 2,   177, 176, 143, 232,
        46,  116, 110, 164, 84,  185, 164, 80,  13,  127, 58,  203, 93,  244, 85,  43,  31,  110,
        184, 188, 152, 14,  180, 116, 93,  195, 236, 18,  175, 104, 206, 115, 48,  52,  172, 199,
        97,  50,  128, 210, 37,  109, 50,  174, 204, 223, 28,  210, 228, 116, 105, 168, 181, 57,
        157, 203, 38,  193, 186, 88,  237, 183, 249, 222, 186, 129, 46,  143, 60,  141, 93,  193,
        223, 152, 155, 0,   134, 222, 137, 144, 125, 253, 83,  201, 0,   155, 10,  79,  254, 42,
        219, 88,  170, 97,  17,  204, 218, 155, 36,  249, 115, 96,  80,  219, 19,  103, 233, 21,
        173, 51,  243, 207, 50,  198, 184, 148, 182, 16,  73,  189, 134, 216, 166, 105, 10,  5,
        119, 3,   177, 57,  39,  181, 24,  91,  166, 52,  203, 68,  2,   60,  135, 88,  248, 157,
        159, 242, 216, 90,  220, 21,  74,  176, 211, 120, 21,  200, 150, 53,  176, 107, 246, 248,
        223, 153, 62,  81,  120, 155, 197, 249, 60,  247, 254, 28,  125, 221, 51,  226, 21,  183,
        184, 22,  233, 179, 237, 140, 45,  67,  186, 35,  35,  145, 113, 28,  192, 69,  129, 88,
        160, 210, 244, 202, 197, 128, 33,  88,  184, 194, 178, 86,  197, 74,  85,  213, 214, 234,
        82,  50,  137, 150, 70,  213, 143, 44,  128, 154, 96,  95,  123, 244, 166, 165, 255, 108,
        149, 94,  161, 40,  56,  243, 180, 234, 243, 166, 4,   222, 220, 138, 112, 189, 185, 83,
        238, 237, 71,  109, 149, 123, 101, 35,  80,  83,  121, 183, 193, 198, 99,  231, 85,  197,
        253, 28,  36,  216, 143, 214, 214, 11,  107, 29,  46,  36,  148, 169, 202, 7,   103, 170,
        181, 136, 254, 14,  35,  154, 250, 149, 199, 131, 64,  168, 182, 221, 18,  90,  193, 14,
        182, 239, 34,  29,  161, 93,  100, 56,  176, 111, 214, 155, 141, 132, 250, 73,  95,  127,
        158, 65,  194, 12,  213, 118, 187, 236, 99,  138, 58,  64,  160, 154, 27,  112, 5,   88,
        70,  193, 94,  178, 164, 155, 115, 122, 224, 111, 19,  85,  200, 15,  111, 130, 153, 233,
        98,  204, 198, 29,  159, 205, 159, 14,  168, 90,  205, 237, 221, 62,  97,  54,  93,  11,
        202, 172, 228, 211, 78,  20,  191, 235, 251, 88,  197, 209, 137, 101, 23,  13,  189, 70,
        206, 163, 163, 5,   20,  7,   192, 210, 206, 84,  97,  99,  22,  40,  142, 170, 210, 208,
        96,  240, 85,  234, 55,  118, 215, 109, 142, 45,  83,  240, 52,  211, 252, 163, 44,  151,
        245, 135, 234, 110, 61,  242, 12,  196, 49,  211, 162, 8,   201, 186, 16,  49,  64,  6,
        15,  93,  93,  250, 58,  150, 170, 254, 140, 131, 85,  48,  26,  200, 174, 155, 228, 169,
        87,  41,  165, 81,  203, 7,   145, 35,  185, 180, 112, 191, 123, 92,  187, 166, 21,  45,
        233, 221, 97,  4,   89,  169, 174, 54,  189, 205, 118, 205, 67,  2,   139, 186, 241, 0,
        244, 11,  123, 15,  117, 3,   31,  27,  25,  80,  196, 226, 63,  138, 63,  129, 244, 83,
        27,  248, 251, 128, 120, 172, 217, 187, 48,  86,  12,  25,  39,  69,  50,  91,  23,  34,
        55,  230, 202, 182, 191, 184, 17,  236, 34,  49,  35,  142, 145, 216, 243, 153, 215, 149,
        234, 94,  56,  175, 238, 37,  80,  127, 216, 242, 253, 96,  46,  248, 26,  255, 75,  50,
        40,  97,  198, 2,   133, 184, 102, 145, 154, 248, 46,  195, 255, 40,  67,  215, 68,  128,
        79,  158, 147, 19,  104, 82,  64,  3,   37,  182, 159, 75,  14,  252, 160, 213, 123, 158,
        54,  27,  200, 51,  39,  72,  222, 178, 215, 27,  166, 254, 135, 206, 77,  190, 85,  28,
        167, 237, 222, 252, 93,  234, 41,  173, 212, 153, 138, 130, 185, 5,   135, 180, 15,  219,
        130, 11,  229, 233, 227, 13,  190, 176, 219, 134, 71,  88,  216, 68,  152, 168, 43,  8,
        124, 199, 129, 1,   20,  23,  219, 34,  193, 184, 203, 76,  98,  172, 169, 137, 34,  101,
        79,  125, 224, 147, 240, 185, 146, 132, 163, 241, 133, 1,   111, 217, 206, 99,  82,  203,
        104, 221, 156, 126, 10,  167, 222, 53,  24,  29,  207, 46,  164, 210, 104, 119, 101, 152,
        203, 194, 102, 88,  1,   165, 222, 232, 103, 233, 116, 74,  174, 106, 233, 137, 107, 121,
        8,   248, 155, 119, 171, 215, 219, 126, 9,   13,  33,  217, 20,  220, 65,  189, 83,  89,
        131, 115, 137, 125, 196, 15,  252, 241, 135, 229, 86,  40,  250, 112, 17,  233, 99,  12,
        143, 26,  231, 215, 131, 163, 143, 31,  203, 168, 128, 76,  194, 50,  122, 136, 164, 30,
        169, 237, 138, 155, 55,  64,  110, 155, 143, 235, 132, 227, 156, 45,  1,   223, 194, 4,
        28,  216, 109, 200, 183, 71,  135, 177, 53,  168, 126, 186, 223, 175, 49,  40,  221, 180,
        249, 101, 220, 223, 131, 150, 136, 119, 48,  176, 42,  6,   111, 159, 39,  73,  153, 66,
        215, 108, 169, 49,  74,  156, 234, 47,  229, 161, 230, 114, 156, 22,  109, 161, 150, 69,
        195, 148, 246, 88,  10,  113, 29,  131, 164, 67,  24,  110, 149, 110, 138, 16,  15,  205,
        169, 73,  93,  19,  130, 76,  75,  106, 224, 95,  24,  165, 153, 177, 82,  29,  87,  213,
        157, 209, 131, 248, 76,  145, 14,  43,  7,   175, 109, 167, 50,  69,  113, 120, 58,  88,
        129, 108, 84,  62,  223, 62,  225, 35,  81,  120, 197, 105, 255, 122, 92,  68,  19,  137,
        5,   255, 218, 1,   57,  137, 209, 202, 100, 115, 191, 143, 41,  143, 106, 35,  233, 44,
        25,  174, 150, 249, 41,  103, 197, 244, 175, 210, 67,  233, 19,  170, 163, 124, 86,  210,
        153, 213, 118, 142, 72,  239, 104, 224, 148, 202, 150, 171, 103, 204, 136, 12,  101, 227,
        16,  34,  145, 8,   76,  30,  77,  58,  143, 159, 171, 229, 60,  175, 58,  128, 112, 242,
        86,  5,   10,  107, 43,  147, 77,  15,  30,  38,  162, 126, 99,  200, 223, 15,  121, 157,
        132, 8,   251, 199, 247, 233, 217, 67,  243, 7,   82,  217, 76,  26,  89,  253, 240, 176,
        96,  236, 219, 149, 212, 160, 42,  53,  9,   146, 251, 174, 100, 136, 144, 18,  212, 5,
        92,  85,  154, 225, 227, 74,  186, 117, 232, 52,  240, 176, 154, 212, 236, 104, 215, 69,
        64,  187, 51,  14,  177, 148, 164, 81,  75,  249, 75,  86,  223, 38,  118, 138, 147, 44,
        90,  47,  113, 159, 140, 74,  16,  114, 187, 107, 72,  79,  255, 207, 144, 150, 194, 131,
        29,  136, 187, 239, 188, 73,  89,  228, 42,  10,  43,  250, 24,  104, 8,   9,   26,  86,
        59,  121, 128, 65,  14,  234, 53,  206, 109, 114, 228, 80,  238, 2,   241, 15,  253, 241,
        88,  27,  29,  159, 71,  55,  74,  188, 181, 47,  161, 226, 121, 223, 74,  129, 91,  85,
        100, 121, 52,  65,  11,  232, 208, 47,  127, 149, 52,  212, 245, 152, 84,  198, 175, 244,
        202, 234, 160, 130, 169, 201, 46,  160, 249, 231, 178, 230, 146, 239, 174, 104, 15,  133,
        117, 68,  222, 154, 88,  63,  211, 41,  49,  232, 252, 23,  30,  67,  204, 50,  178, 17,
        102, 73,  18,  21,  41,  51,  173, 85,  104, 132, 253, 24,  78,  92,  205, 176, 10,  230,
        55,  144, 30,  75,  100, 204, 44,  97,  147, 3,   107, 27,  166, 200, 19,  130, 212, 204,
        58,  220, 139, 107, 244, 48,  170, 116, 195, 227, 27,  90,  68,  236, 145, 177, 73,  32,
        152, 137, 213, 212, 70,  18,  71,  235, 65,  162, 10,  201, 50,  65,  131, 38,  231, 126,
        162, 129, 46,  138, 255, 22,  66,  134, 241, 121, 153, 167, 236, 151, 147, 92,  126, 54,
        70,  138, 7,   112, 251, 192, 90,  136, 17,  91,  254, 221, 132, 132, 178, 136, 90,  217,
        218, 100, 164, 176, 107, 135, 184, 147, 132, 143, 8,   161, 192, 216, 119, 181, 210, 90,
        121, 8,   70,  161, 62,  103, 200, 71,  155, 184, 184, 68,  232, 62,  239, 83,  4,   196,
        124, 67,  6,   134, 240, 75,  69,  188, 201, 235, 109, 130, 215, 172, 18,  127, 195, 158,
        45,  27,  164, 162, 81,  70,  26,  87,  165, 47,  29,  55,  58,  3,   231, 237, 131, 210,
        244, 129, 108, 130, 218, 27,  229, 202, 197, 51,  31,  50,  254, 183, 23,  225, 121, 55,
        160, 193, 149, 10,  173, 33,  7,   242, 192, 246, 223, 129, 42,  238, 185, 238, 117, 202,
        201, 127, 8,   119, 83,  22,  43,  32,  212, 162, 41,  102, 207, 249, 241, 233, 185, 169,
        108, 151, 149, 205, 69,  193, 142, 48,  19,  143, 75,  128, 121, 107, 128, 215, 212, 50,
        185, 112, 95,  60,  89,  54,  169, 80,  87,  184, 41,  214, 104, 37,  108, 61,  113, 92,
        174, 145, 144, 171, 68,  42,  26,  14,  7,   236, 124, 26,  53,  252, 53,  1,   178, 246,
        82,  221, 60,  23,  96,  221, 236, 92,  91,  11,  21,  241, 139, 111, 70,  133, 117, 63,
        239, 240, 122, 161, 140, 194, 79,  86,  178, 73,  136, 228, 173, 212, 186, 216, 190, 45,
        98,  191, 85,  70,  157, 204, 241, 83,  150, 26,  8,   49,  66,  190, 33,  30,  35,  78,
        13,  36,  30,  221, 208, 234, 118, 98,  231, 142, 65,  82,  70,  31,  119, 129, 111, 150,
        14,  84,  31,  196, 136, 200, 47,  53,  240, 57,  65,  189, 177, 154, 170, 4,   35,  196,
        116, 12,  74,  202, 108, 215, 200, 62,  80,  184, 221, 131, 29,  10,  225, 236, 234, 47,
        30,  96,  142, 178, 236, 142, 205, 16,  37,  121, 194, 27,  44,  178, 231, 252, 79,  95,
        198, 101, 230, 16,  50,  23,  117, 100, 80,  178, 30,  121, 214, 63,  85,  120, 75,  53,
        150, 190, 60,  104, 98,  207, 163, 115, 26,  254, 89,  100, 88,  85,  87,  37,  102, 59,
        64,  84,  71,  143, 170, 115, 33,  248, 124, 191, 242, 156, 9,   173, 194, 25,  247, 130,
        177, 143, 27,  75,  99,  29,  249, 167, 57,  221, 49,  107, 199, 131, 82,  118, 252, 94,
        246, 18,  25,  93,  17,  3,   252, 162, 158, 86,  58,  59,  190, 149, 61,  97,  92,  213,
        145, 98,  245, 22,  75,  65,  176, 232, 187, 36,  144, 147, 98,  205, 15,  97,  138, 181,
        113, 68,  60,  13,  210, 15,  57,  210, 8,   167, 152, 216, 2,   179, 193, 11,  17,  82,
        236, 17,  101, 168, 168, 192, 173, 81,  236, 179, 35,  11,  203, 159, 8,   152, 196, 128,
        211, 105, 241, 60,  29,  62,  52,  64,  56,  213, 37,  161, 146, 23,  253, 97,  52,  195,
        42,  36,  7,   116, 48,  156, 102, 142, 137, 17,  247, 38,  101, 73,  91,  243, 6,   243,
        244, 79,  235, 240, 53,  218, 130, 163, 180, 62,  98,  19,  168, 163, 143, 216, 62,  201,
        103, 32,  165, 208, 66,  110, 64,  209, 55,  238, 166, 159, 18,  192, 50,  236, 120, 58,
        171, 216, 179, 138, 236, 54,  134, 51,  209, 157, 89,  134, 77,  149, 240, 219, 79,  233,
        203, 237, 242, 55,  21,  214, 195, 227, 31,  206, 181, 107, 252, 219, 199, 253, 36,  20,
        163, 208, 212, 103, 72,  90,  192, 39,  43,  136, 205, 235, 228, 184, 203, 82,  134, 116,
        30,  252, 217, 31,  48,  128, 147, 201, 122, 12,  237, 87,  87,  225, 55,  110, 63,  106,
        31,  19,  45,  11,  124, 108, 36,  255, 216, 6,   61,  241, 234, 111, 54,  180, 104, 46,
        184, 28,  130, 94,  90,  19,  71,  166, 99,  102, 106, 253, 187, 149, 213, 233, 192, 165,
        235, 252, 191, 247, 97,  0,   242, 4,   3,   225, 130, 127, 167, 127, 32,  72,  254, 109,
        234, 112, 209, 11,  112, 40,  130, 66,  193, 130, 161, 173, 204, 53,  31,  186, 141, 26,
        197, 28,  13,  88,  128, 119, 240, 240, 124, 251, 70,  79,  181, 211, 216, 150, 135, 18,
        60,  76,  208, 15,  207, 179, 161, 65,  85,  53,  106, 46,  31,  208, 10,  158, 133, 57,
        96,  157, 173, 161, 189, 0,   249, 65,  254, 166, 185, 48,  196, 129, 153, 116, 100, 253,
        230, 81,  198, 114, 153, 156, 174, 199, 43,  245, 57,  31,  166, 195, 127, 134, 172, 53,
        165, 60,  126, 221, 107, 217, 134, 252, 16,  41,  236, 240, 14,  22,  10,  249, 133, 117,
        132, 244, 18,  83,  13,  187, 124, 227, 144, 114, 2,   251, 38,  112, 187, 193, 14,  71,
        193, 172, 26,  51,  233, 230, 176, 59,  10,  184, 198, 148, 175, 115, 58,  57,  120, 49,
        65,  24,  209, 227, 183, 10,  133, 102, 20,  88,  151, 185, 85,  150, 211, 91,  181, 128,
        48,  225, 241, 200, 229, 152, 165, 76,  235, 138, 145, 140, 217, 142, 234, 200, 38,  235,
        97,  207, 86,  59,  183, 108, 50,  73,  245, 47,  34,  121, 124, 141, 96,  27,  210, 36,
        140, 117, 186, 128, 188, 92,  148, 184, 255, 65,  79,  223, 12,  107, 43,  9,   51,  166,
        17,  234, 189, 195, 1,   240, 47,  85,  246, 21,  175, 84,  7,   98,  126, 120, 25,  214,
        22,  217, 1,   130, 50,  125, 88,  228, 80,  21,  133, 220, 52,  228, 105, 29,  44,  20,
        130, 36,  231, 53,  144, 147, 174, 94,  91,  216, 158, 201, 91,  83,  216, 60,  204, 202,
        209, 35,  21,  136, 49,  177, 49,  112, 255, 73,  203, 201, 223, 89,  246, 87,  146, 71,
        26,  117, 43,  235, 44,  51,  37,  12,  30,  141, 186, 85,  154, 132, 0,   108, 55,  226,
        6,   21,  200, 99,  84,  48,  134, 156, 51,  17,  203, 235, 51,  247, 73,  231, 71,  38,
        234, 207, 149, 200, 131, 226, 140, 253, 71,  115, 203, 110, 149, 66,  130, 167, 53,  150,
        146, 135, 172, 177, 203, 196, 233, 130, 70,  213, 51,  45,  24,  80,  156, 108, 214, 225,
        175, 201, 201, 102, 96,  191, 117, 108, 169, 145, 231, 77,  55,  60,  215, 200, 149, 40,
        227, 48,  37,  60,  48,  55,  213, 136, 65,  239, 90,  38,  186, 61,  74,  44,  217, 68,
        170, 88,  213, 213, 16,  65,  226, 54,  227, 70,  187, 88,  92,  200, 7,   246, 207, 188,
        242, 140, 144, 179, 93,  36,  212, 199, 158, 186, 3,   212, 145, 18,  153, 40,  233, 158,
        14,  204, 215, 131, 33,  26,  231, 52,  96,  180, 245, 116, 37,  11,  226, 199, 168, 230,
        36,  28,  201, 103, 245, 100, 9,   231, 203, 185, 218, 20,  19,  250, 8,   200, 217, 108,
        42,  147, 246, 242, 197, 48,  133, 49,  221, 56,  108, 62,  196, 115, 160, 156, 145, 87,
        208, 160, 147, 116, 22,  167, 201, 209, 159, 94,  7,   133, 153, 29,  16,  95,  103, 172,
        241, 118, 220, 38,  159, 221, 59,  48,  113, 237, 231, 47,  148, 97,  208, 164, 90,  17,
        226, 228, 251, 132, 134, 168, 60,  62,  255, 51,  54,  161, 139, 112, 15,  154, 87,  203,
        137, 97,  63,  109, 159, 223, 42,  125, 209, 87,  11,  53,  26,  132, 178, 49,  182, 55,
        138, 50,  118, 218, 70,  123, 212, 233, 186, 58,  143, 123, 216, 30,  170, 23,  95,  169,
        34,  24,  9,   247, 88,  242, 197, 5,   21,  130, 41,  37,  116, 25,  39,  7,   173, 60,
        196, 56,  116, 124, 59,  56,  164, 207, 112, 2,   135, 40,  236, 181, 50,  127, 123, 36,
        208, 253, 26,  143, 143, 123, 252, 95,  100, 146, 245, 167, 123, 36,  177, 33,  120, 230,
        157, 94,  197, 49,  152, 68,  152, 76,  18,  49,  235, 127, 207, 111, 242, 217, 233, 99,
        83,  120, 39,  115, 233, 37,  109, 118, 158, 158, 166, 219, 146, 229, 132, 240, 44,  73,
        96,  65,  204, 86,  214, 58,  206, 0,   119, 149, 11,  72,  166, 79,  44,  4,   239, 162,
        25,  37,  49,  124, 120, 5,   164, 187, 73,  158, 19,  148, 90,  253, 88,  178, 1,   143,
        107, 102, 63,  236, 174, 130, 126, 137, 227, 151, 109, 43,  131, 234, 63,  49,  81,  44,
        143, 145, 197, 81,  123, 34,  124, 11,  145, 186, 22,  78,  141, 220, 233, 109, 5,   73,
        62,  178, 64,  206, 180, 191, 106, 135, 85,  173, 89,  142, 39,  186, 13,  122, 12,  50,
        213, 71,  104, 117, 88,  198, 140, 35,  184, 192, 174, 37,  204, 226, 207, 219, 91,  243,
        60,  20,  253, 90,  116, 83,  154, 69,  191, 205, 64,  165, 218, 84,  93,  206, 54,  56,
        251, 163, 173, 49,  63,  33,  243, 203, 166, 130, 168, 24,  107, 145, 7,   27,  2,   191,
        124, 166, 233, 23,  236, 176, 27,  233, 3,   92,  233, 214, 70,  96,  9,   137, 3,   152,
        211, 226, 95,  24,  134, 183, 187, 118, 36,  91,  5,   180, 218, 222, 245, 36,  97,  64,
        126, 139, 180, 219, 150, 3,   165, 169, 203, 70,  212, 225, 50,  225, 152, 83,  171, 94,
        196, 46,  97,  81,  202, 203, 69,  98,  236, 68,  39,  170, 207, 6,   60,  232, 196, 51,
        191, 7,   145, 204, 226, 203, 244, 229, 152, 84,  166, 101, 137, 174, 156, 156, 44,  173,
        183, 37,  200, 183, 32,  55,  188, 184, 55,  34,  139, 135, 42,  185, 99,  211, 1,   106,
        230, 75,  196, 60,  120, 240, 140, 185, 120, 26,  174, 52,  170, 66,  216, 192, 253, 81,
        98,  35,  198, 110, 80,  235, 243, 119, 104, 197, 97,  93,  27,  7,   54,  189, 228, 177,
        101, 170, 200, 120, 54,  54,  127, 178, 1,   218, 48,  169, 215, 45,  37,  28,  240, 169,
        3,   95,  232, 135, 37,  84,  136, 180, 92,  138, 239, 16,  181, 249, 230, 215, 80,  0,
        227, 108, 247, 241, 193, 23,  85,  141, 121, 91,  221, 100, 245, 42,  239, 240, 120, 103,
        169, 163, 180, 220, 20,  146, 82,  6,   224, 133, 117, 45,  12,  91,  33,  6,   226, 23,
        87,  225, 238, 252, 235, 66,  132, 14,  173, 151, 232, 45,  225, 59,  100, 101, 178, 201,
        229, 26,  200, 29,  208, 105, 87,  150, 89,  140, 187, 144, 111, 74,  237, 13,  59,  247,
        103, 188, 22,  164, 60,  19,  116, 46,  22,  250, 103, 126, 193, 118, 72,  118, 200, 61,
        51,  79,  0,   68,  135, 170, 149, 240, 180, 249, 193, 46,  141, 201, 21,  56,  20,  231,
        67,  196, 77,  217, 192, 152, 233, 87,  227, 83,  8,   145, 25,  233, 125, 122, 236, 183,
        92,  201, 249, 124, 39,  184, 37,  216, 201, 61,  89,  18,  5,   156, 19,  9,   226, 245,
        77,  13,  195, 191, 81,  132, 196, 137, 31,  155, 100, 223, 39,  55,  144, 180, 134, 115,
        38,  163, 176, 250, 233, 24,  5,   251, 241, 102, 65,  74,  213, 14,  254, 18,  245, 88,
        193, 179, 113, 151, 57,  252, 101, 69,  243, 10,  79,  122, 75,  131, 113, 91,  107, 138,
        123, 233, 15,  7,   228, 183, 42,  84,  126, 36,  31,  238, 31,  156, 81,  154, 214, 63,
        101, 37,  62,  124, 57,  46,  186, 197, 217, 90,  170, 141, 238, 231, 32,  38,  164, 157,
        113, 199, 56,  224, 102, 47,  218, 184, 8,   104, 76,  161, 15,  222, 161, 23,  114, 160,
        198, 139, 102, 9,   89,  243, 220, 10,  187, 61,  14,  156, 165, 242, 44,  163, 247, 126,
        205, 114, 62,  74,  27,  34,  23,  125, 11,  121, 206, 224, 200, 111, 115, 189, 219, 121,
        228, 201, 241, 112, 76,  74,  143, 45,  6,   42,  47,  74,  82,  100, 38,  159, 116, 115,
        229, 102, 247, 250, 14,  234, 251, 164, 204, 173, 182, 80,  22,  216, 189, 87,  59,  99,
        168, 221, 128, 224, 131, 144, 150, 217, 133, 175, 149, 196, 67,  34,  153, 74,  141, 179,
        48,  83,  109, 232, 77,  152, 78,  241, 7,   12,  31,  225, 236, 82,  108, 167, 211, 60,
        170, 143, 3,   201, 157, 168, 120, 21,  82,  243, 140, 41,  37,  205, 219, 229, 192, 3,
        221, 107, 244, 126, 20,  143, 180, 33,  157, 215, 155, 208, 13,  251, 48,  36,  26,  55,
        139, 21,  219, 120, 203, 225, 184, 8,   67,  123, 80,  249, 250, 160, 84,  248, 75,  255,
        148, 166, 10,  117, 203, 151, 177, 66,  200, 2,   18,  167, 6,   70,  41,  57,  2,   237,
        87,  13,  61,  134, 9,   115, 35,  113, 109, 125, 78,  69,  129, 145, 85,  110, 119, 216,
        85,  193, 180, 107, 50,  185, 240, 170, 243, 135, 192, 70,  227, 185, 139, 2,   220, 89,
        94,  30,  187, 115, 181, 113, 168, 70,  70,  5,   23,  179, 75,  139, 0,   18,  85,  197,
        141, 159, 172, 136, 145, 123, 219, 20,  169, 115, 59,  141, 122, 101, 238, 241, 211, 112,
        73,  238, 7,   161, 34,  123, 15,  157, 84,  33,  243, 111, 79,  99,  173, 65,  57,  247,
        152, 24,  126, 162, 51,  205, 93,  223, 163, 85,  115, 236, 252, 119, 81,  176, 81,  126,
        71,  128, 101, 42,  248, 146, 59,  18,  157, 83,  125, 116, 21,  180, 62,  108, 18,  146,
        146, 58,  101, 194, 5,   210, 220, 239, 25,  223, 195, 111, 49,  114, 89,  108, 182, 121,
        118, 53,  72,  57,  47,  169, 122, 228, 251, 210, 194, 69,  156, 21,  142, 99,  59,  156,
        240, 61,  63,  87,  225, 56,  0,   173, 151, 110, 226, 99,  146, 228, 25,  141, 110, 119,
        57,  160, 54,  244, 166, 196, 229, 242, 5,   232, 34,  102, 72,  76,  221, 8,   170, 230,
        96,  155, 98,  145, 54,  200, 93,  70,  255, 233, 95,  154, 101, 137, 76,  105, 214, 234,
        193, 133, 108, 12,  152, 215, 252, 232, 47,  72,  228, 142, 71,  244, 118, 54,  137, 11,
        5,   75,  16,  251, 244, 232, 100, 154, 202, 190, 195, 206, 149, 114, 97,  176, 160, 82,
        238, 21,  254, 253, 45,  235, 68,  137, 104, 116, 86,  58,  70,  237, 191, 94,  30,  4,
        223, 170, 28,  50,  156, 242, 70,  22,  63,  103, 157, 91,  205, 153, 251, 249, 192, 212,
        61,  158, 223, 14,  93,  100, 9,   14,  120, 214, 99,  59,  146, 251, 32,  190, 33,  163,
        191, 19,  99,  54,  182, 244, 37,  206, 198, 198, 238, 32,  11,  61,  116, 90,  187, 36,
        61,  234, 233, 213, 65,  37,  180, 45,  68,  162, 139, 160, 37,  248, 124, 61,  123, 182,
        113, 5,   238, 9,   95,  219, 10,  227, 95,  249, 55,  10,  130, 222, 2,   98,  38,  74,
        94,  249, 185, 235, 214, 116, 234, 185, 75,  14,  255, 95,  148, 85,  248, 0,   69,  32,
        242, 18,  196, 225, 110, 225, 65,  121, 3,   15,  152, 122, 68,  221, 134, 149, 62,  249,
        57,  79,  121, 38,  249, 255, 216, 120, 189, 226, 105, 218, 249, 14,  219, 14,  180, 12,
        159, 212, 142, 214, 175, 136, 185, 119, 155, 50,  41,  15,  82,  28,  100, 131, 104, 81,
        227, 188, 123, 91,  22,  136, 246, 110, 97,  160, 252, 18,  55,  219, 140, 124, 173, 70,
        28,  238, 209, 167, 214, 228, 228, 73,  199, 225, 55,  236, 93,  95,  103, 138, 192, 9,
        183, 39,  101, 52,  90,  105, 58,  82,  196, 160, 132, 223, 17,  168, 134, 18,  46,  31,
        117, 156, 139, 177, 182, 49,  196, 241, 239, 104, 116, 190, 99,  156, 249, 79,  143, 55,
        167, 137, 253, 74,  83,  210, 10,  195, 88,  118, 73,  234, 44,  212, 75,  118, 157, 2,
        248, 243, 82,  40,  44,  133, 146, 70,  133, 187, 232, 186, 225, 106, 60,  68,  180, 51,
        96,  203, 83,  222, 164, 172, 104, 11,  77,  74,  0,   186, 123, 182, 173, 196, 247, 47,
        20,  164, 208, 115, 41,  167, 212, 66,  222, 68,  86,  222, 79,  119, 97,  33,  204, 220,
        127, 162, 9,   190, 57,  20,  143, 224, 203, 85,  185, 184, 144, 190, 27,  57,  163, 19,
        184, 207, 80,  180, 154, 174, 118, 117, 195, 65,  192, 220, 147, 48,  233, 33,  247, 156,
        57,  151, 201, 234, 196, 8,   66,  50,  107, 74,  171, 181, 253, 20,  126, 254, 27,  236,
        65,  66,  161, 165, 138, 28,  166, 254, 249, 180, 124, 96,  31,  198, 160, 164, 209, 7,
        97,  58,  52,  197, 254, 159, 160, 211, 48,  216, 42,  14,  181, 61,  142, 217, 112, 234,
        32,  155, 49,  147, 6,   33,  219, 24,  19,  155, 40,  176, 159, 139, 173, 8,   189, 43,
        112, 201, 202, 175, 87,  207, 82,  51,  158, 53,  84,  108, 224, 158, 135, 23,  228, 47,
        245, 73,  190, 188, 138, 17,  79,  164, 125, 53,  179, 137, 209, 74,  206, 18,  110, 180,
        93,  68,  237, 212, 144, 104, 249, 62,  225, 96,  196, 126, 90,  38,  179, 53,  99,  155,
        222, 147, 31,  152, 140, 198, 176, 161, 167, 70,  59,  162, 198, 83,  231, 240, 215, 244,
        35,  39,  246, 96,  24,  103, 96,  45,  163, 227, 100, 253, 51,  227, 140, 113, 7,   244,
        64,  232, 161, 68,  57,  167, 115, 15,  124, 146, 167, 147, 80,  40,  167, 155, 88,  123,
        93,  8,   231, 219, 194, 236, 223, 127, 219, 152, 184, 223, 151, 229, 224, 98,  76,  130,
        200, 140, 101, 162, 51,  98,  236, 213, 125, 32,  52,  14,  66,  239, 212, 77,  108, 155,
        58,  169, 45,  228, 44,  249, 166, 137, 87,  92,  7,   164, 77,  169, 87,  244, 201, 100,
        254, 203, 107, 163, 230, 194, 56,  201, 105, 68,  191, 233, 3,   226, 160, 180, 13,  236,
        136, 62,  66,  177, 5,   44,  38,  149, 226, 34,  43,  12,  242, 254, 231, 234, 155, 141,
        246, 209, 251, 94,  150, 231, 87,  86,  70,  167, 159, 192, 115, 22,  171, 109, 153, 223,
        240, 90,  255, 193, 99,  102, 81,  152, 58,  108, 106, 182, 31,  243, 127, 37,  222, 222,
        40,  57,  93,  49,  209, 144, 233, 40,  5,   146, 8,   161, 94,  37,  67,  21,  43,  207,
        175, 136, 24,  103, 215, 239, 247, 156, 18,  107, 51,  115, 90,  223, 143, 235, 27,  34,
        198, 46,  248, 58,  157, 255, 42,  69,  102, 235, 101, 56,  150, 64,  254, 120, 146, 213,
        232, 86,  115, 238, 60,  99,  44,  167, 5,   32,  210, 177, 23,  185, 74,  72,  165, 219,
        202, 4,   177, 219, 44,  83,  223, 235, 254, 143, 212, 241, 24,  100, 13,  129, 183, 22,
        123, 247, 100, 204, 17,  237, 10,  62,  171, 193, 6,   56,  241, 164, 78,  174, 222, 37,
        78,  39,  75,  71,  119, 147, 23,  130, 195, 90,  225, 92,  16,  168, 225, 199, 62,  133,
        21,  92,  229, 143, 57,  111, 167, 88,  136, 206, 161, 162, 231, 10,  13,  92,  81,  151,
        233, 51,  115, 215, 244, 125, 30,  2,   189, 53,  104, 250, 252, 19,  26,  240, 36,  147,
        202, 168, 94,  129, 217, 51,  109, 36,  93,  41,  209, 31,  188, 109, 144, 115, 146, 160,
        204, 210, 77,  87,  117, 254, 36,  164, 70,  243, 49,  49,  29,  14,  45,  119, 96,  146,
        207, 146, 16,  229, 105, 198, 163, 123, 15,  75,  124, 196, 225, 242, 181, 115, 61,  189,
        222, 102, 99,  141, 169, 254, 25,  74,  57,  224, 247, 66,  103, 17,  128, 174, 29,  167,
        197, 184, 178, 131, 68,  90,  15,  175, 213, 28,  215, 116, 172, 142, 23,  68,  31,  163,
        21,  43,  167, 204, 119, 144, 4,   33,  47,  214, 96,  89,  219, 176, 138, 0,   207, 23,
        138, 105, 130, 200, 239, 50,  163, 216, 222, 222, 47,  246, 174, 111, 95,  207, 2,   84,
        232, 43,  61,  229, 250, 198, 214, 138, 196, 14,  87,  248, 240, 51,  45,  5,   95,  249,
        251, 114, 76,  148, 11,  198, 171, 82,  166, 163, 189, 124, 247, 217, 49,  116, 215, 20,
        13,  165, 47,  140, 7,   166, 99,  111, 176, 208, 10,  92,  8,   20,  64,  206, 73,  164,
        214, 182, 4,   95,  60,  15,  76,  103, 208, 58,  12,  94,  87,  26,  49,  233, 88,  249,
        7,   23,  123, 135, 205, 83,  45,  95,  47,  187, 151, 136, 213, 48,  73,  204, 39,  219,
        189, 107, 142, 94,  141, 145, 199, 223, 242, 78,  50,  176, 229, 201, 218, 111, 250, 228,
        169, 144, 119, 96,  92,  152, 182, 223, 111, 194, 249, 187, 243, 155, 214, 251, 228, 85,
        109, 204, 195, 15,  49,  134, 99,  183, 53,  215, 203, 174, 69,  154, 203, 88,  127, 78,
        107, 226, 10,  27,  90,  134, 86,  28,  190, 181, 187, 88,  24,  125, 17,  24,  246, 146,
        222, 201, 25,  105, 207, 92,  217, 20,  90,  59,  91,  156, 249, 141, 11,  52,  250, 57,
        30,  163, 67,  251, 209, 118, 128, 185, 214, 232, 186, 133, 105, 91,  177, 221, 43,  249,
        62,  192, 54,  72,  104, 177, 169, 26,  90,  91,  64,  178, 114, 179, 5,   157, 127, 85,
        231, 49,  32,  161, 46,  203, 100, 159, 187, 182, 53,  243, 121, 145, 12,  134, 200, 70,
        160, 185, 166, 162, 136, 145, 225, 130, 10,  95,  159, 202, 127, 219, 178, 216, 6,   39,
        20,  60,  189, 114, 157, 172, 222, 175, 98,  142, 155, 185, 171, 40,  153, 117, 105, 227,
        205, 172, 26,  147, 39,  173, 63,  217, 182, 148, 86,  102, 34,  253, 224, 145, 62,  250,
        84,  173, 83,  72,  76,  203, 4,   54,  172, 178, 254, 22,  171, 204, 89,  227, 236, 173,
        233, 200, 123, 97,  119, 94,  236, 25,  199, 237, 207, 73,  235, 177, 59,  144, 85,  253,
        31,  255, 61,  46,  208, 184, 110, 123, 75,  127, 5,   55,  168, 226, 208, 27,  175, 139,
        228, 247, 1,   129, 240, 202, 128, 128, 110, 147, 62,  226, 212, 183, 202, 40,  32,  11,
        203, 80,  176, 128, 204, 205, 50,  118, 120, 59,  90,  240, 123, 196, 162, 72,  20,  82,
        169, 175, 119, 250, 162, 171, 95,  132, 19,  219, 189, 11,  31,  54,  138, 212, 174, 244,
        210, 76,  235, 59,  214, 195, 12,  158, 65,  129, 58,  212, 27,  23,  164, 3,   157, 188,
        194, 249, 52,  154, 235, 164, 110, 188, 195, 175, 170, 180, 226, 71,  169, 243, 160, 33,
        19,  223, 176, 190, 168, 20,  95,  216, 254, 167, 78,  232, 190, 52,  193, 78,  103, 200,
        149, 157, 212, 60,  60,  97,  254, 26,  18,  148, 190, 76,  50,  127, 209, 238, 171, 105,
        28,  147, 139, 104, 27,  117, 234, 149, 9,   242, 145, 113, 53,  195, 136, 169, 239, 40,
        146, 240, 12,  93,  147, 28,  35,  54,  20,  72,  137, 73,  186, 87,  75,  199, 167, 245,
        193, 161, 115, 182, 148, 237, 93,  227, 240, 44,  76,  73,  62,  115, 90,  127, 145, 62,
        123, 40,  15,  50,  195, 206, 7,   129, 129, 30,  169, 54,  167, 20,  181, 238, 96,  251,
        214, 98,  233, 60,  125, 196, 88,  116, 71,  49,  240, 35,  249, 3,   97,  250, 92,  71,
        222, 121, 233, 248, 180, 104, 237, 53,  84,  3,   176, 116, 134, 2,   43,  56,  172, 68,
        215, 227, 95,  69,  18,  120, 140, 29,  231, 84,  162, 180, 8,   114, 141, 192, 35,  205,
        198, 44,  146, 13,  185, 94,  222, 46,  165, 29,  84,  247, 94,  139, 138, 78,  148, 95,
        174, 72,  205, 187, 179, 238, 80,  44,  180, 163, 21,  22,  125, 8,   95,  105, 87,  46,
        217, 186, 96,  85,  113, 183, 244, 85,  254, 146, 56,  80,  206, 141, 171, 140, 125, 178,
        231, 38,  52,  8,   40,  84,  188, 198, 19,  43,  19,  220, 191, 117, 172, 79,  203, 135,
        25,  224, 20,  78,  135, 66,  24,  43,  165, 157, 159, 34,  176, 26,  194, 61,  136, 12,
        205, 161, 92,  42,  212, 0,   24,  124, 146, 130, 142, 187, 167, 199, 152, 2,   159, 198,
        237, 174, 114, 214, 215, 225, 162, 250, 127, 178, 181, 128, 101, 31,  49,  2,   80,  251,
        5,   200, 231, 63,  90,  25,  148, 239, 242, 66,  93,  227, 231, 203, 15,  16,  36,  13,
        159, 24,  209, 66,  7,   5,   232, 60,  249, 100, 135, 130, 140, 18,  187, 167, 22,  218,
        231, 152, 120, 182, 154, 238, 13,  208, 30,  88,  167, 14,  32,  172, 194, 171, 206, 163,
        95,  4,   182, 129, 250, 205, 36,  152, 26,  68,  100, 118, 207, 166, 204, 126, 21,  235,
        6,   241, 120, 87,  222, 171, 124, 118, 235, 96,  140, 97,  62,  141, 189, 71,  44,  179,
        4,   150, 3,   129, 68,  153, 63,  208, 178, 248, 223, 251, 149, 95,  142, 220, 244, 247,
        162, 54,  91,  190, 32,  168, 241, 158, 74,  146, 248, 143, 142, 236, 193, 122, 66,  52,
        33,  176, 223, 125, 18,  81,  189, 152, 216, 170, 254, 185, 190, 241, 74,  15,  139, 197,
        235, 27,  143, 76,  31,  214, 18,  109, 211, 50,  15,  232, 163, 226, 92,  154, 172, 42,
        195, 212, 67,  234, 176, 141, 24,  227, 30,  210, 155, 252, 208, 156, 143, 18,  79,  73,
        110, 136, 145, 223, 36,  119, 132, 240, 12,  7,   97,  47,  97,  107, 254, 123, 22,  86,
        251, 101, 14,  173, 94,  91,  189, 13,  55,  47,  225, 140, 213, 22,  236, 113, 184, 152,
        97,  105, 10,  173, 7,   7,   62,  104, 143, 20,  15,  17,  28,  231, 180, 49,  12,  227,
        216, 36,  187, 194, 31,  37,  202, 116, 11,  21,  211, 144, 250, 12,  156, 26,  104, 137,
        67,  99,  248, 5,   25,  225, 193, 14,  1,   50,  137, 248, 172, 16,  175, 173, 6,   197,
        101, 106, 200, 43,  99,  51,  96,  197, 49,  228, 155, 111, 242, 22,  17,  120, 27,  117,
        182, 104, 212, 212, 179, 73,  136, 14,  127, 156, 237, 163, 243, 47,  138, 86,  109, 105,
        176, 119, 83,  142, 111, 115, 90,  99,  199, 57,  231, 205, 179, 155, 115, 102, 220, 117,
        231, 195, 246, 134, 139, 178, 40,  209, 56,  105, 51,  148, 223, 105, 173, 27,  114, 213,
        120, 212, 135, 25,  63,  232, 225, 249, 18,  1,   46,  79,  97,  151, 146, 205, 105, 90,
        200, 254, 219, 116, 115, 137, 126, 139, 87,  108, 135, 75,  5,   204, 62,  223, 5,   193,
        124, 63,  59,  14,  255, 136, 28,  56,  34,  122, 112, 79,  161, 55,  162, 55,  23,  212,
        163, 84,  229, 64,  162, 129, 12,  193, 91,  215, 230, 193, 105, 70,  73,  99,  216, 154,
        47,  206, 67,  13,  194, 69,  128, 34,  210, 178, 122, 120, 65,  109, 173, 194, 186, 80,
        163, 170, 84,  144, 68,  141, 51,  52,  128, 214, 161, 122, 190, 61,  58,  2,   1,   24,
        84,  32,  133, 84,  82,  139, 37,  108, 253, 141, 216, 23,  128, 237, 207, 70,  0,   124,
        150, 192, 96,  75,  199, 48,  164, 29,  152, 195, 80,  27,  131, 213, 130, 111, 135, 124,
        217, 11,  107, 71,  120, 0,   81,  141, 148, 141, 175, 210, 101, 16,  169, 157, 196, 195,
        241, 201, 194, 180, 51,  216, 114, 2,   79,  29,  75,  222, 80,  31,  199, 29,  137, 147,
        173, 36,  168, 137, 1,   67,  97,  246, 72,  150, 218, 214, 199, 45,  101, 190, 106, 156,
        1,   71,  245, 102, 187, 151, 133, 103, 240, 35,  33,  132, 176, 200, 105, 17,  81,  105,
        101, 142, 24,  228, 244, 166, 11,  225, 6,   204, 190, 148, 133, 91,  153, 203, 128, 214,
        115, 132, 184, 178, 58,  88,  60,  215, 132, 184, 21,  72,  37,  215, 62,  220, 152, 6,
        212, 204, 33,  64,  174, 24,  73,  208, 128, 222, 53,  114, 234, 186, 247, 208, 235, 133,
        68,  5,   65,  70,  157, 200, 148, 29,  222, 180, 26,  194, 186, 255, 222, 36,  133, 227,
        99,  216, 138, 83,  197, 115, 50,  136, 135, 233, 26,  127, 121, 236, 235, 147, 253, 215,
        9,   253, 191, 57,  158, 147, 102, 127, 238, 246, 124, 102, 164, 146, 79,  89,  183, 146,
        200, 57,  147, 28,  223, 129, 132, 91,  20,  248, 77,  65,  185, 224, 200, 210, 71,  118,
        105, 188, 233, 59,  180, 55,  175, 16,  111, 105, 20,  253, 59,  150, 33,  202, 127, 16,
        219, 243, 49,  4,   127, 174, 76,  227, 248, 166, 79,  234, 6,   245, 166, 200, 38,  252,
        34,  32,  162, 188, 220, 100, 205, 116, 53,  205, 75,  48,  191, 183, 218, 39,  241, 62,
        173, 203, 18,  189, 103, 189, 6,   130, 15,  125, 162, 202, 39,  74,  97,  95,  253, 190,
        99,  255, 94,  38,  246, 175, 1,   166, 65,  9,   73,  237, 230, 159, 73,  125, 136, 99,
        86,  108, 202, 192, 253, 12,  13,  6,   119, 31,  48,  132, 141, 82,  129, 221, 175, 10,
        135, 50,  89,  34,  151, 95,  152, 6,   8,   128, 217, 33,  221, 177, 129, 161, 107, 208,
        88,  24,  122, 56,  70,  179, 96,  219, 32,  128, 56,  138, 154, 244, 33,  7,   12,  132,
        121, 201, 197, 99,  164, 67,  249, 98,  5,   206, 19,  65,  47,  200, 12,  21,  227, 113,
        6,   110, 86,  158, 177, 216, 141, 67,  170, 152, 178, 176, 149, 2,   62,  234, 197, 94,
        217, 240, 106, 183, 55,  61,  17,  95,  77,  103, 71,  213, 36,  125, 242, 105, 140, 113,
        55,  224, 9,   215, 244, 221, 95,  101, 17,  157, 103, 106, 125, 23,  26,  22,  241, 118,
        110, 8,   164, 220, 115, 159, 65,  169, 123, 216, 77,  150, 15,  51,  186, 184, 57,  142,
        163, 248, 252, 192, 1,   113, 21,  248, 38,  95,  175, 0,   48,  47,  234, 240, 246, 32,
        65,  239, 191, 107, 15,  62,  90,  201, 2,   25,  128, 54,  47,  43,  253, 142, 150, 71,
        108, 62,  142, 215, 72,  253, 51,  67,  15,  214, 172, 180, 146, 41,  204, 140, 83,  107,
        102, 188, 69,  203, 26,  32,  113, 7,   254, 244, 2,   178, 234, 183, 242, 225, 188, 3,
        156, 122, 23,  140, 85,  203, 253, 98,  111, 250, 28,  96,  166, 153, 236, 94,  147, 170,
        33,  202, 9,   1,   65,  212, 242, 194, 235, 45,  212, 56,  173, 222, 134, 8,   30,  204,
        120, 88,  231, 192, 43,  138, 174, 205, 91,  137, 115, 102, 192, 120, 116, 44,  119, 249,
        149, 127, 64,  119, 63,  21,  0,   182, 20,  114, 13,  142, 223, 172, 220, 130, 186, 189,
        28,  163, 242, 80,  80,  138, 174, 17,  40,  228, 133, 78,  182, 201, 26,  184, 215, 12,
        195, 235, 164, 234, 199, 63,  89,  93,  71,  87,  249, 145, 137, 247, 38,  214, 206, 230,
        65,  163, 180, 245, 103, 2,   89,  216, 208, 55,  246, 149, 185, 104, 3,   212, 228, 103,
        63,  162, 58,  151, 20,  138, 36,  160, 171, 79,  148, 136, 25,  72,  43,  63,  131, 230,
        198, 4,   10,  17,  145, 145, 210, 228, 70,  74,  195, 247, 153, 170, 27,  189, 69,  204,
        90,  0,   247, 233, 58,  143, 87,  90,  33,  244, 219, 35,  123, 111, 96,  126, 70,  45,
        71,  177, 91,  144, 201, 245, 121, 186, 84,  53,  157, 253, 108, 253, 2,   40,  136, 85,
        151, 27,  112, 158, 38,  11,  151, 174, 223, 219, 22,  90,  206, 237, 121, 255, 194, 32,
        15,  185, 207, 187, 253, 151, 43,  43,  59,  184, 70,  40,  250, 215, 228, 186, 95,  99,
        220, 248, 194, 98,  88,  207, 141, 156, 248, 141, 156, 197, 61,  71,  38,  117, 38,  170,
        197, 231, 207, 7,   15,  173, 55,  56,  16,  245, 136, 104, 228, 149, 237, 238, 243, 141,
        113, 110, 175, 148, 46,  2,   36,  207, 131, 122, 145, 232, 14,  229, 76,  21,  184, 128,
        250, 199, 55,  69,  201, 145, 175, 57,  104, 28,  117, 47,  219, 35,  28,  23,  126, 43,
        109, 134, 149, 231, 84,  163, 128, 205, 207, 239, 44,  162, 183, 110, 39,  135, 127, 233,
        56,  184, 200, 81,  217, 101, 132, 39,  229, 251, 43,  169, 52,  154, 199, 124, 104, 61,
        239, 216, 227, 255, 228, 179, 77,  114, 195, 230, 227, 3,   154, 178, 24,  184, 5,   231,
        219, 202, 160, 89,  144, 73,  216, 94,  247, 235, 225, 155, 213, 18,  154, 45,  105, 199,
        123, 147, 176, 216, 117, 26,  184, 229, 232, 183, 203, 68,  222, 86,  136, 76,  64,  24,
        199, 127, 61,  53,  151, 86,  118, 109, 237, 110, 30,  139, 183, 17,  246, 75,  155, 138,
        141, 200, 39,  65,  28,  129, 206, 206, 142, 74,  39,  176, 82,  16,  198, 206, 221, 228,
        41,  164, 210, 28,  144, 178, 72,  159, 120, 225, 108, 168, 76,  215, 107, 61,  1,   54,
        191, 60,  205, 91,  148, 235, 157, 179, 181, 163, 89,  19,  26,  106, 120, 157, 87,  149,
        32,  14,  106, 216, 3,   119, 40,  4,   165, 93,  150, 227, 91,  251, 171, 70,  218, 141,
        52,  40,  124, 26,  192, 19,  124, 93,  188, 29,  134, 163, 251, 240, 12,  214, 215, 113,
        188, 191, 41,  157, 108, 124, 57,  100, 168, 233, 144, 254, 126, 95,  244, 7,   223, 159,
        186, 123, 158, 43,  112, 93,  178, 108, 200, 54,  134, 157, 106, 144, 116, 105, 161, 182,
        33,  229, 161, 87,  116, 253, 114, 238, 160, 87,  31,  16,  153, 109, 95,  155, 38,  23,
        70,  196, 54,  40,  91,  105, 1,   208, 0,   111, 103, 143, 119, 139, 120, 12,  192, 133,
        195, 63,  41,  65,  117, 157, 69,  184, 82,  149, 97,  113, 235, 245, 51,  243, 3,   33,
        102, 229, 91,  59,  249, 154, 156, 86,  197, 16,  247, 174, 173, 182, 32,  123, 223, 136,
        252, 97,  185, 238, 148, 44,  229, 165, 186, 99,  141, 233, 62,  87,  47,  16,  189, 157,
        187, 64,  198, 247, 90,  136, 86,  67,  34,  19,  200, 166, 150, 213, 192, 137, 18,  173,
        239, 24,  2,   197, 48,  62,  146, 237, 224, 109, 14,  86,  143, 211, 47,  136, 236, 164,
        56,  227, 148, 255, 253, 20,  141, 123, 160, 27,  208, 94,  170, 156, 174, 77,  70,  73,
        110, 220, 225, 4,   60,  184, 243, 120, 42,  179, 66,  1,   120, 113, 23,  244, 223, 8,
        191, 7,   94,  184, 37,  234, 44,  88,  34,  203, 205, 56,  48,  118, 115, 179, 254, 252,
        101, 41,  221, 46,  9,   253, 102, 32,  109, 227, 225, 113, 196, 129, 16,  25,  135, 182,
        160, 113, 47,  177, 220, 110, 32,  97,  183, 2,   45,  167, 211, 3,   204, 101, 100, 16,
        164, 8,   14,  203, 234, 28,  52,  202, 74,  109, 57,  249, 22,  16,  98,  218, 144, 160,
        128, 187, 219, 195, 193, 147, 123, 127, 42,  208, 112, 61,  48,  215, 75,  255, 53,  244,
        61,  157, 227, 195, 235, 200, 168, 11,  7,   69,  14,  235, 26,  124, 120, 198, 247, 83,
        173, 6,   192, 215, 52,  90,  151, 99,  127, 236, 156, 135, 216, 72,  28,  113, 117, 241,
        134, 210, 112, 39,  9,   126, 27,  255, 53,  251, 115, 123, 24,  26,  202, 139, 182, 102,
        5,   36,  220, 78,  163, 71,  102, 189, 119, 236, 215, 73,  164, 243, 196, 42,  198, 231,
        28,  198, 86,  77,  207, 23,  229, 160, 240, 215, 221, 144, 170, 171, 21,  18,  255, 202,
        130, 156, 29,  51,  152, 69,  158, 91,  224, 139, 143, 103, 133, 140, 78,  86,  93,  126,
        81,  194, 134, 236, 80,  190, 159, 123, 77,  229, 192, 211, 223, 235, 21,  218, 58,  131,
        68,  218, 19,  19,  202, 126, 137, 181, 171, 106, 65,  217, 36,  147, 169, 186, 207, 39,
        85,  185, 61,  52,  186, 181, 199, 219, 110, 191, 233, 248, 20,  169, 136, 60,  223, 170,
        185, 70,  176, 90,  218, 236, 103, 222, 116, 27,  146, 101, 128, 100, 97,  18,  214, 127,
        111, 254, 205, 232, 148, 82,  161, 25,  26,  242, 246, 6,   245, 105, 56,  150, 145, 71,
        216, 35,  11,  15,  103, 188, 158, 98,  29,  65,  225, 171, 1,   43,  75,  78,  219, 29,
        83,  91,  113, 243, 124, 217, 33,  233, 182, 219, 134, 170, 190, 103, 15,  231, 131, 25,
        238, 6,   194, 252, 108, 61,  251, 230, 68,  4,   208, 178, 221, 170, 157, 32,  165, 146,
        177, 137, 36,  196, 55,  163, 20,  110, 103, 13,  13,  133, 71,  177, 75,  158, 71,  76,
        248, 30,  170, 87,  158, 166, 64,  221, 35,  5,   179, 6,   24,  103, 83,  16,  28,  215,
        54,  163, 11,  107, 71,  167, 5,   4,   170, 116, 151, 174, 124, 6,   145, 101, 238, 199,
        254, 19,  89,  83,  225, 175, 241, 124, 189, 162, 78,  138, 169, 251, 163, 20,  196, 141,
        78,  235, 77,  242, 119, 3,   61,  179, 128, 132, 202, 207, 9,   175, 201, 31,  177, 210,
        175, 148, 110, 232, 26,  232, 102, 175, 114, 242, 190, 205, 77,  183, 175, 195, 222, 198,
        201, 19,  125, 141, 248, 127, 122, 239, 246, 45,  88,  169, 17,  37,  9,   236, 205, 140,
        200, 72,  117, 45,  158, 191, 35,  108, 235, 31,  63,  151, 215, 209, 178, 220, 234, 234,
        157, 97,  60,  206, 142, 214, 119, 166, 70,  99,  208, 181, 32,  99,  244, 147, 247, 52,
        91,  23,  86,  76,  146, 137, 74,  249, 250, 188, 173, 192, 236, 251, 228, 194, 238, 161,
        14,  255, 232, 253, 55,  11,  199, 193, 200, 68,  225, 244, 202, 113, 90,  218, 157, 57,
        111, 66,  85,  104, 46,  156, 232, 187, 63,  122, 50,  241, 54,  145, 239, 39,  222, 49,
        83,  93,  23,  72,  165, 206, 100, 225, 43,  108, 81,  162, 70,  137, 95,  247, 97,  134,
        251, 141, 210, 188, 114, 255, 91,  214, 248, 135, 66,  6,   130, 218, 143, 44,  203, 17,
        10,  231, 103, 88,  85,  247, 120, 94,  148, 228, 117, 86,  180, 32,  17,  236, 140, 183,
        106, 171, 108, 167, 225, 27,  2,   25,  114, 240, 152, 171, 85,  49,  67,  223, 102, 112,
        89,  228, 13,  0,   255, 92,  120, 129, 52,  112, 193, 11,  34,  247, 220, 26,  232, 31,
        224, 128, 12,  76,  61,  231, 228, 200, 7,   75,  167, 191, 245, 53,  13,  169, 208, 54,
        135, 134, 98,  80,  222, 54,  130, 142, 163, 233, 214, 173, 5,   221, 167, 141, 183, 15,
        232, 151, 198, 90,  155, 109, 122, 107, 55,  218, 33,  12,  135, 61,  54,  60,  226, 99,
        230, 193, 110, 66,  146, 102, 54,  30,  158, 161, 46,  208, 48,  120, 108, 251, 192, 179,
        172, 122, 35,  240, 104, 49,  197, 48,  123, 182, 26,  99,  219, 7,   127, 99,  194, 146,
        49,  57,  49,  102, 197, 232, 49,  178, 27,  108, 53,  181, 167, 54,  58,  31,  118, 129,
        85,  129, 99,  141, 249, 151, 84,  120, 227, 229, 102, 170, 98,  136, 168, 138, 206, 246,
        74,  50,  173, 173, 31,  166, 164, 109, 86,  41,  42,  92,  19,  91,  223, 159, 100, 114,
        211, 40,  2,   247, 197, 135, 171, 120, 82,  90,  18,  162, 105, 62,  154, 225, 103, 145,
        88,  81,  143, 252, 8,   84,  196, 158, 34,  253, 124, 97,  254, 198, 220, 12,  32,  171,
        255, 169, 73,  30,  62,  142, 224, 82,  49,  127, 181, 92,  101, 213, 218, 84,  188, 99,
        158, 242, 51,  105, 229, 0,   182, 87,  118, 139, 112, 97,  174, 214, 69,  94,  149, 199,
        243, 168, 48,  92,  169, 245, 68,  86,  114, 255, 189, 246, 105, 153, 80,  28,  9,   139,
        231, 15,  170, 223, 108, 129, 233, 233, 172, 166, 252, 253, 147, 226, 227, 110, 247, 237,
        42,  79,  244, 210, 158, 23,  95,  91,  199, 34,  171, 13,  184, 233, 205, 94,  137, 128,
        11,  20,  43,  76,  192, 213, 192, 236, 177, 153, 166, 180, 35,  131, 96,  112, 152, 135,
        188, 44,  205, 137, 122, 126, 202, 175, 25,  217, 5,   107, 26,  154, 48,  59,  25,  93,
        146, 130, 222, 42,  126, 37,  115, 191, 26,  66,  201, 139, 215, 162, 227, 232, 145, 10,
        27,  95,  46,  79,  233, 3,   215, 160, 54,  99,  242, 157, 29,  125, 63,  48,  218, 183,
        85,  130, 171, 53,  48,  31,  58,  136, 77,  242, 238, 53,  72,  234, 182, 181, 168, 179,
        71,  228, 153, 253, 75,  56,  19,  170, 164, 4,   99,  159, 235, 196, 40,  10,  79,  159,
        251, 14,  89,  183, 102, 152, 252, 241, 178, 229, 76,  140, 51,  178, 56,  204, 20,  184,
        218, 57,  65,  175, 160, 192, 148, 174, 122, 183, 195, 141, 133, 118, 173, 176, 35,  220,
        106, 53,  240, 179, 69,  201, 82,  176, 183, 224, 36,  126, 65,  119, 250, 58,  59,  78,
        4,   127, 105, 185, 167, 251, 69,  204, 23,  200, 197, 250, 104, 129, 114, 123, 241, 159,
        81,  154, 49,  45,  14,  75,  86,  85,  253, 165, 210, 255, 52,  181, 76,  171, 72,  253,
        196, 166, 176, 146, 243, 99,  206, 158, 74,  160, 64,  162, 236, 75,  72,  244, 196, 150,
        198, 231, 127, 254, 101, 0,   34,  204, 20,  52,  242, 250, 168, 99,  215, 233, 182, 96,
        172, 157, 250, 10,  194, 150, 58,  21,  13,  180, 198, 32,  221, 35,  124, 18,  51,  192,
        228, 141, 187, 65,  100, 172, 250, 41,  134, 177, 28,  84,  58,  43,  247, 156, 206, 42,
        27,  110, 248, 2,   74,  63,  90,  213, 191, 152, 195, 128, 106, 83,  42,  217, 2,   143,
        14,  66,  103, 199, 33,  132, 129, 229, 28,  184, 47,  62,  48,  74,  117, 113, 39,  43,
        216, 211, 49,  46,  165, 1,   252, 253, 1,   76,  157, 93,  113, 237, 69,  123, 195, 17,
        97,  179, 41,  241, 82,  26,  210, 130, 131, 200, 67,  20,  185, 100, 179, 251, 44,  113,
        58,  61,  140, 185, 55,  17,  253, 207, 22,  112, 42,  175, 155, 227, 89,  170, 234, 207,
        94,  8,   2,   146, 180, 155, 205, 40,  152, 219, 27,  25,  251, 96,  32,  87,  87,  26,
        48,  102, 59,  30,  19,  34,  12,  60,  1,   176, 224, 6,   207, 98,  160, 34,  5,   73,
        81,  253, 175, 102, 94,  242, 17,  171, 60,  108, 105, 178, 113, 217, 117, 130, 206, 192,
        28,  117, 98,  127, 9,   143, 41,  89,  150, 101, 34,  194, 198, 91,  20,  254, 88,  65,
        196, 9,   223, 161, 216, 208, 33,  4,   97,  157, 114, 186, 253, 61,  252, 255, 210, 242,
        76,  90,  206, 44,  50,  145, 207, 147, 81,  5,   132, 38,  140, 14,  197, 44,  141, 195,
        247, 223, 194, 29,  113, 110, 247, 183, 234, 141, 16,  141, 225, 34,  104, 217, 203, 156,
        74,  142, 57,  23,  90,  72,  134, 189, 227, 124, 177, 197, 108, 32,  88,  235, 189, 135,
        231, 35,  184, 66,  89,  100, 202, 120, 35,  109, 125, 254, 71,  148, 185, 183, 91,  19,
        183, 191, 12,  200, 72,  90,  170, 13,  143, 151, 234, 8,   248, 235, 31,  89,  205, 152,
        119, 67,  47,  96,  16,  149, 177, 50,  51,  134, 12,  173, 3,   55,  226, 254, 57,  87,
        122, 170, 61,  221, 77,  107, 236, 197, 75,  80,  219, 144, 50,  87,  41,  161, 90,  31,
        202, 71,  174, 178, 22,  214, 7,   181, 201, 101, 120, 130, 162, 68,  142, 13,  32,  226,
        193, 229, 238, 22,  2,   33,  76,  236, 72,  242, 150, 86,  247, 31,  118, 152, 31,  251,
        161, 100, 32,  240, 194, 165, 188, 48,  139, 14,  107, 118, 60,  3,   74,  13,  127, 78,
        94,  11,  169, 66,  83,  190, 3,   8,   200, 247, 1,   88,  54,  69,  201, 178, 2,   91,
        87,  61,  21,  106, 71,  32,  76,  49,  148, 233, 11,  105, 95,  168, 20,  168, 80,  117,
        249, 135, 44,  49,  36,  87,  109, 236, 41,  19,  135, 214, 12,  161, 229, 83,  139, 169,
        24,  117, 66,  58,  169, 57,  108, 213, 133, 3,   204, 245, 199, 147, 201, 188, 176, 8,
        115, 71,  243, 244, 176, 204, 146, 12,  208, 190, 30,  83,  78,  181, 49,  46,  176, 227,
        108, 6,   151, 104, 79,  185, 163, 163, 29,  194, 178, 35,  72,  123, 179, 185, 172, 153,
        137, 107, 158, 10,  148, 116, 67,  90,  47,  95,  191, 102, 205, 204, 236, 107, 252, 102,
        156, 28,  169, 43,  124, 219, 136, 37,  199, 21,  246, 100, 134, 222, 245, 55,  237, 85,
        212, 136, 173, 79,  37,  213, 24,  75,  37,  40,  56,  13,  205, 71,  198, 15,  100, 189,
        244, 121, 130, 165, 51,  211, 226, 244, 163, 178, 56,  149, 85,  202, 190, 218, 78,  128,
        202, 129, 121, 58,  24,  82,  55,  102, 131, 201, 161, 76,  76,  248, 204, 99,  190, 105,
        108, 106, 47,  136, 138, 190, 28,  222, 255, 45,  180, 68,  16,  72,  220, 124, 230, 132,
        90,  183, 111, 127, 87,  22,  255, 109, 77,  118, 217, 93,  188, 19,  37,  229, 45,  127,
        172, 36,  110, 81,  116, 38,  206, 78,  58,  241, 188, 139, 33,  41,  113, 11,  238, 150,
        202, 248, 89,  134, 72,  38,  175, 9,   223, 168, 56,  0,   59,  154, 48,  129, 128, 14,
        202, 35,  85,  177, 81,  212, 242, 177, 203, 208, 59,  207, 208, 190, 29,  32,  172, 19,
        145, 118, 181, 197, 240, 170, 148, 19,  81,  104, 14,  119, 166, 33,  9,   223, 73,  167,
        17,  197, 167, 81,  25,  43,  101, 39,  45,  71,  218, 116, 147, 108, 114, 79,  99,  124,
        154, 192, 62,  6,   119, 5,   218, 1,   185, 160, 17,  217, 220, 16,  110, 4,   41,  220,
        185, 176, 244, 2,   72,  6,   73,  125, 62,  87,  167, 221, 48,  239, 239, 237, 54,  33,
        76,  41,  217, 148, 151, 215, 153, 223, 245, 101, 199, 66,  107, 18,  213, 205, 192, 145,
        93,  55,  251, 209, 208, 86,  225, 44,  8,   81,  225, 92,  56,  106, 128, 133, 59,  201,
        160, 132, 21,  254, 175, 120, 186, 184, 130, 213, 63,  105, 244, 10,  113, 181, 67,  162,
        49,  96,  67,  146, 62,  243, 202, 181, 154, 141, 89,  116, 144, 254, 59,  37,  152, 104,
        215, 7,   54,  0,   41,  135, 74,  194, 144, 195, 222, 204, 184, 72,  118, 19,  232, 230,
        96,  95,  229, 7,   214, 68,  50,  247, 211, 217, 79,  82,  4,   223, 113, 33,  192, 34,
        62,  250, 202, 168, 231, 148, 170, 187, 181, 239, 90,  75,  91,  146, 92,  119, 210, 65,
        107, 149, 248, 4,   173, 0,   202, 44,  26,  191, 51,  85,  21,  69,  66,  243, 67,  37,
        33,  147, 196, 61,  217, 69,  123, 208, 29,  101, 207, 28,  22,  58,  51,  58,  172, 40,
        183, 199, 30,  218, 54,  124, 8,   183, 139, 115, 221, 56,  76,  193, 186, 19,  190, 47,
        136, 66,  61,  66,  34,  67,  136, 111, 152, 71,  86,  7,   227, 111, 241, 203, 191, 16,
        215, 123, 142, 177, 227, 85,  89,  52,  151, 194, 1,   4,   169, 103, 103, 250, 159, 211,
        84,  158, 130, 234, 38,  106, 110, 31,  73,  169, 172, 152, 166, 180, 46,  102, 153, 168,
        112, 252, 34,  34,  65,  213, 154, 143, 27,  97,  228, 131, 96,  78,  15,  193, 230, 133,
        238, 2,   150, 94,  25,  243, 146, 140, 209, 131, 103, 210, 174, 107, 195, 139, 76,  210,
        64,  212, 253, 22,  146, 62,  171, 235, 38,  235, 163, 232, 216, 139, 5,   29,  101, 118,
        167, 153, 162, 41,  192, 210, 88,  214, 136, 120, 64,  71,  13,  249, 32,  153, 245, 117,
        169, 239, 50,  57,  173, 241, 214, 47,  33,  253, 228, 69,  31,  212, 214, 69,  54,  221,
        250, 88,  202, 105, 151, 13,  76,  94,  62,  15,  99,  254, 33,  154, 235, 0,   46,  131,
        18,  192, 95,  73,  246, 57,  229, 245, 147, 144, 162, 123, 116, 22,  134, 13,  148, 189,
        124, 12,  246, 234, 222, 194, 45,  138, 118, 105, 40,  183, 28,  20,  204, 204, 108, 10,
        117, 34,  202, 136, 115, 87,  152, 104, 192, 50,  114, 19,  45,  159, 143, 172, 79,  196,
        143, 247, 40,  137, 187, 248, 148, 157, 185, 158, 230, 114, 29,  197, 142, 9,   55,  198,
        26,  106, 89,  2,   6,   97,  80,  182, 123, 193, 89,  253, 212, 33,  23,  87,  169, 187,
        11,  18,  43,  155, 138, 130, 137, 220, 6,   115, 120, 164, 51,  250, 22,  160, 27,  117,
        251, 29,  180, 118, 248, 75,  26,  57,  148, 144, 225, 113, 141, 25,  87,  11,  3,   249,
        85,  32,  238, 43,  177, 175, 228, 151, 171, 157, 228, 141, 39,  226, 177, 84,  183, 38,
        118, 97,  68,  231, 252, 101, 121, 111, 110, 155, 226, 236, 161, 128, 166, 62,  61,  23,
        26,  131, 146, 99,  239, 142, 170, 200, 92,  110, 15,  128, 119, 22,  33,  104, 253, 206,
        128, 175, 20,  79,  6,   106, 195, 231, 117, 254, 75,  126, 4,   136, 49,  252, 250, 51,
        51,  218, 243, 147, 171, 160, 12,  99,  149, 80,  135, 188, 162, 185, 112, 153, 5,   184,
        237, 78,  102, 28,  251, 1,   217, 222, 53,  115, 91,  155, 39,  145, 16,  18,  170, 36,
        153, 16,  207, 14,  181, 230, 187, 33,  63,  5,   191, 106, 73,  90,  19,  196, 11,  177,
        66,  170, 37,  83,  227, 10,  242, 140, 251, 248, 0,   56,  210, 10,  192, 100, 251, 246,
        1,   78,  231, 176, 23,  7,   146, 126, 252, 222, 142, 137, 70,  56,  214, 170, 203, 101,
        40,  159, 192, 179, 121, 50,  229, 52,  206, 65,  78,  161, 216, 197, 167, 203, 214, 66,
        34,  62,  208, 23,  226, 104, 177, 59,  125, 103, 49,  122, 170, 237, 243, 255, 136, 131,
        209, 142, 130, 225, 36,  221, 132, 198, 30,  123, 86,  241, 92,  78,  138, 85,  16,  134,
        223, 164, 117, 253, 109, 29,  76,  200, 101, 210, 163, 245, 126, 171, 60,  92,  0,   159,
        18,  70,  56,  191, 138, 33,  197, 41,  149, 254, 45,  247, 150, 214, 127, 106, 177, 237,
        166, 184, 238, 240, 228, 224, 164, 114, 201, 144, 247, 133, 107, 127, 100, 132, 83,  159,
        145, 16,  45,  85,  92,  212, 30,  110, 76,  55,  111, 236, 170, 199, 10,  190, 202, 26,
        209, 175, 136, 116, 8,   153, 222, 187, 51,  254, 95,  27,  84,  50,  205, 64,  202, 117,
        144, 126, 36,  25,  212, 68,  2,   121, 243, 193, 82,  207, 145, 25,  28,  148, 67,  39,
        202, 68,  122, 131, 172, 72,  185, 153, 204, 217, 251, 178, 93,  108, 233, 180, 58,  173,
        47,  184, 173, 202, 169, 120, 137, 241, 132, 246, 120, 165, 132, 193, 20,  26,  132, 67,
        142, 22,  218, 25,  224, 187, 173, 74,  173, 196, 124, 201, 13,  72,  152, 75,  139, 232,
        133, 114, 143, 35,  54,  56,  219, 210, 89,  189, 224, 94,  46,  178, 117, 18,  251, 149,
        202, 65,  249, 198, 1,   146, 62,  85,  129, 82,  119, 57,  217, 150, 80,  154, 151, 68,
        118, 159, 2,   7,   146, 132, 152, 163, 184, 13,  98,  210, 209, 33,  88,  230, 208, 180,
        167, 91,  17,  220, 45,  51,  169, 107, 123, 87,  168, 216, 169, 74,  169, 20,  234, 92,
        143, 100, 159, 29,  27,  46,  56,  96,  213, 209, 108, 84,  212, 79,  113, 25,  55,  133,
        218, 208, 115, 76,  118, 250, 53,  212, 33,  201, 250, 237, 88,  100, 123, 161, 200, 209,
        210, 205, 64,  82,  91,  86,  122, 213, 182, 71,  51,  64,  53,  13,  7,   229, 218, 45,
        116, 190, 178, 192, 46,  199, 44,  140, 82,  118, 15,  205, 221, 142, 24,  95,  165, 115,
        15,  23,  13,  16,  149, 73,  70,  222, 125, 230, 104, 30,  60,  103, 67,  1,   98,  15,
        84,  230, 250, 136, 164, 25,  99,  25,  25,  184, 12,  98,  249, 252, 135, 233, 66,  22,
        212, 253, 8,   200, 111, 22,  99,  19,  93,  156, 86,  128, 224, 197, 234, 216, 75,  87,
        93,  119, 114, 60,  174, 223, 161, 119, 174, 92,  248, 83,  74,  196, 99,  228, 90,  212,
        127, 188, 145, 4,   85,  28,  66,  41,  107, 58,  157, 40,  171, 66,  79,  223, 78,  157,
        65,  49,  80,  92,  158, 143, 68,  171, 237, 110, 126, 26,  33,  41,  157, 89,  67,  111,
        252, 233, 66,  122, 255, 246, 235, 76,  197, 48,  146, 92,  43,  240, 105, 172, 219, 140,
        132, 9,   247, 65,  2,   226, 63,  158, 197, 151, 34,  222, 251, 111, 204, 202, 180, 114,
        247, 142, 169, 17,  224, 100, 1,   90,  183, 26,  51,  47,  183, 150, 129, 45,  219, 75,
        248, 174, 173, 21,  180, 9,   231, 60,  238, 244, 87,  172, 154, 19,  3,   69,  126, 77,
        137, 202, 187, 86,  121, 54,  161, 80,  206, 252, 152, 86,  62,  16,  81,  203, 10,  120,
        224, 252, 62,  194, 137, 192, 200, 203, 26,  112, 161, 121, 101, 232, 245, 249, 163, 170,
        27,  203, 38,  11,  192, 40,  49,  112, 25,  131, 33,  127, 144, 57,  61,  16,  1,   182,
        233, 59,  10,  78,  254, 77,  123, 252, 47,  50,  203, 61,  225, 90,  221, 230, 43,  55,
        212, 23,  154, 30,  165, 99,  147, 163, 230, 194, 68,  160, 223, 223, 97,  25,  140, 98,
        53,  86,  65,  133, 255, 136, 76,  95,  95,  254, 152, 143, 142, 213, 53,  82,  129, 119,
        26,  141, 100, 49,  245, 14,  115, 115, 169, 93,  248, 198, 198, 157, 80,  213, 239, 202,
        204, 79,  39,  227, 77,  178, 19,  35,  30,  42,  60,  66,  123, 74,  162, 60,  102, 31,
        233, 66,  203, 118, 67,  219, 232, 176, 235, 243, 6,   12,  154, 234, 226, 52,  87,  174,
        158, 30,  134, 161, 32,  166, 204, 48,  129, 249, 180, 173, 255, 144, 134, 245, 81,  138,
        109, 164, 33,  84,  107, 159, 176, 15,  31,  17,  97,  200, 220, 191, 137, 138, 145, 137,
        205, 162, 31,  3,   110, 225, 108, 148, 167, 84,  34,  164, 141, 93,  164, 56,  195, 32,
        59,  74,  206, 58,  106, 165, 189, 98,  64,  201, 8,   1,   19,  225, 209, 235, 113, 85,
        130, 82,  141, 171, 110, 178, 220, 132, 14,  92,  178, 1,   168, 44,  5,   4,   128, 45,
        33,  131, 60,  91,  27,  245, 92,  133, 134, 66,  206, 25,  109, 181, 226, 24,  64,  194,
        115, 138, 247, 126, 227, 169, 193, 141, 216, 181, 171, 234, 13,  94,  225, 47,  123, 142,
        33,  24,  135, 87,  236, 74,  215, 188, 80,  142, 21,  149, 203, 115, 110, 195, 97,  147,
        35,  48,  1,   54,  18,  117, 202, 237, 96,  57,  111, 87,  234, 52,  101, 222, 208, 72,
        98,  144, 245, 86,  38,  208, 136, 100, 195, 249, 39,  9,   210, 104, 103, 185, 46,  107,
        74,  192, 31,  193, 133, 211, 120, 69,  75,  9,   207, 67,  67,  32,  227, 93,  166, 38,
        138, 122, 11,  154, 117, 87,  252, 218, 10,  0,   75,  30,  152, 249, 37,  125, 7,   229,
        49,  40,  206, 133, 55,  191, 4,   204, 47,  15,  222, 204, 89,  193, 210, 250, 83,  134,
        150, 115, 95,  234, 33,  206, 240, 129, 101, 118, 126, 227, 113, 167, 56,  235, 48,  78,
        153, 201, 55,  123, 166, 53,  152, 121, 243, 125, 161, 188, 118, 16,  109, 147, 86,  201,
        228, 155, 125, 158, 62,  176, 229, 15,  0,   35,  20,  233, 29,  215, 48,  113, 150, 134,
        0,   55,  138, 251, 77,  175, 37,  99,  113, 158, 1,   17,  250, 232, 180, 146, 153, 33,
        157, 109, 27,  102, 48,  141, 101, 247, 49,  83,  17,  59,  89,  16,  88,  222, 43,  127,
        179, 174, 156, 224, 165, 141, 17,  20,  248, 231, 116, 208, 243, 24,  94,  251, 152, 195,
        196, 151, 108, 117, 12,  199, 36,  159, 166, 8,   13,  211, 223, 90,  102, 182, 216, 8,
        86,  59,  201, 139, 19,  49,  97,  181, 187, 110, 100, 14,  5,   130, 204, 68,  142, 92,
        209, 157, 162, 14,  199, 121, 67,  93,  241, 60,  27,  140, 216, 74,  43,  157, 134, 165,
        160, 233, 237, 223, 38,  118, 94,  67,  194, 73,  252, 79,  215, 224, 172, 120, 157, 74,
        99,  27,  96,  58,  214, 156, 123, 251, 185, 244, 177, 105, 38,  131, 8,   48,  4,   116,
        237, 135, 0,   24,  97,  180, 61,  59,  116, 161, 250, 97,  120, 10,  199, 5,   143, 129,
        38,  110, 161, 32,  145, 196, 216, 208, 157, 1,   73,  170, 56,  220, 206, 174, 70,  145,
        41,  191, 160, 31,  56,  105, 242, 62,  107, 242, 97,  195, 64,  152, 252, 175, 159, 130,
        238, 143, 116, 196, 226, 246, 118, 100, 213, 172, 10,  100, 143, 226, 148, 13,  25,  159,
        24,  127, 252, 20,  0,   218, 0,   204, 166, 54,  175, 87,  249, 253, 29,  123, 189, 208,
        100, 108, 50,  255, 208, 177, 229, 91,  161, 81,  209, 251, 176, 182, 175, 134, 15,  122,
        153, 98,  121, 170, 55,  124, 23,  66,  39,  127, 104, 144, 207, 132, 152, 1,   57,  46,
        13,  122, 42,  98,  238, 67,  67,  129, 28,  188, 187, 240, 185, 81,  231, 221, 193, 170,
        48,  20,  213, 74,  24,  141, 226, 134, 43,  107, 87,  208, 216, 134, 118, 227, 165, 70,
        165, 217, 255, 226, 213, 132, 158, 251, 184, 202, 116, 132, 175, 243, 15,  106, 66,  101,
        170, 163, 145, 47,  53,  146, 22,  227, 112, 158, 223, 68,  166, 217, 100, 187, 82,  54,
        46,  144, 221, 67,  63,  107, 27,  252, 212, 92,  23,  238, 10,  151, 51,  94,  242, 78,
        180, 164, 53,  139, 247, 120, 177, 211, 132, 248, 161, 199, 57,  179, 251, 16,  165, 228,
        76,  220, 138, 46,  59,  38,  207, 105, 149, 0,   251, 113, 41,  160, 84,  146, 232, 171,
        118, 71,  203, 204, 24,  170, 18,  29,  232, 214, 207, 222, 65,  40,  187, 14,  199, 111,
        238, 244, 131, 100, 180, 179, 191, 120, 105, 94,  39,  200, 125, 92,  140, 118, 43,  185,
        62,  75,  176, 151, 68,  86,  223, 169, 165, 200, 221, 70,  168, 244, 153, 28,  194, 151,
        78,  245, 48,  139, 87,  248, 32,  249, 2,   27,  233, 12,  60,  224, 88,  203, 34,  54,
        104, 224, 5,   72,  230, 191, 219, 149, 89,  153, 171, 75,  222, 115, 241, 170, 114, 171,
        120, 128, 151, 118, 138, 38,  212, 89,  220, 154, 202, 248, 166, 5,   36,  177, 143, 148,
        192, 168, 33,  74,  218, 247, 185, 36,  111, 180, 127, 33,  158, 255, 241, 4,   200, 24,
        147, 126, 166, 243, 129, 195, 127, 221, 193, 34,  121, 243, 178, 39,  15,  175, 143, 234,
        127, 206, 38,  45,  208, 4,   110, 14,  64,  220, 37,  56,  83,  183, 84,  115, 77,  6,
        11,  241, 98,  43,  83,  21,  30,  244, 226, 29,  86,  16,  69,  98,  110, 182, 55,  190,
        161, 180, 63,  169, 217, 158, 247, 186, 189, 49,  243, 168, 202, 241, 27,  131, 230, 184,
        216, 78,  254, 35,  103, 66,  59,  44,  43,  29,  36,  42,  31,  83,  67,  124, 53,  255,
        94,  88,  192, 232, 238, 7,   218, 237, 179, 56,  34,  200, 127, 13,  245, 217, 184, 234,
        223, 51,  8,   37,  162, 247, 241, 219, 175, 163, 95,  81,  96,  47,  183, 71,  62,  60,
        178, 130, 194, 74,  178, 190, 86,  174, 39,  254, 138, 90,  30,  73,  163, 132, 44,  16,
        241, 13,  119, 92,  180, 120, 95,  75,  138, 17,  65,  36,  9,   68,  56,  108, 38,  65,
        3,   87,  89,  130, 218, 89,  0,   86,  182, 75,  27,  108, 184, 182, 91,  170, 47,  17,
        37,  12,  244, 151, 167, 230, 77,  56,  85,  164, 150, 88,  33,  67,  250, 162, 63,  55,
        26,  166, 121, 91,  60,  77,  144, 89,  227, 229, 255, 202, 228, 122, 182, 20,  142, 127,
        193, 21,  158, 75,  15,  211, 66,  126, 2,   14,  123, 64,  49,  149, 107, 235, 32,  12,
        30,  9,   9,   156, 135, 79,  220, 250, 53,  204, 121, 81,  231, 121, 141, 164, 239, 93,
        155, 18,  154, 30,  17,  183, 128, 72,  235, 116, 216, 145, 201, 51,  255, 125, 42,  94,
        161, 250, 162, 131, 23,  208, 137, 135, 121, 82,  34,  15,  107, 49,  192, 174, 94,  126,
        69,  232, 151, 29,  197, 223, 109, 77,  131, 26,  52,  160, 65,  104, 212, 67,  57,  80,
        81,  32,  254, 104, 3,   93,  125, 94,  51,  165, 137, 166, 48,  110, 248, 0,   97,  138,
        16,  133, 186, 26,  238, 247, 74,  220, 224, 117, 231, 109, 7,   50,  255, 171, 158, 201,
        8,   186, 201, 225, 220, 223, 43,  189, 27,  99,  203, 140, 20,  48,  239, 118, 86,  75,
        97,  249, 126, 105, 187, 111, 67,  194, 121, 157, 16,  142, 17,  219, 79,  171, 155, 186,
        238, 73,  49,  99,  9,   31,  75,  201, 250, 68,  247, 137, 90,  162, 55,  156, 67,  108,
        168, 133, 118, 65,  85,  92,  200, 51,  125, 231, 100, 248, 145, 128, 102, 88,  125, 8,
        76,  233, 14,  193, 240, 162, 65,  4,   226, 128, 210, 53,  192, 232, 247, 247, 178, 227,
        193, 38,  62,  211, 216, 38,  22,  99,  15,  157, 52,  39,  238, 46,  22,  87,  86,  217,
        232, 37,  187, 129, 144, 34,  38,  99,  79,  133, 131, 90,  78,  105, 44,  142, 223, 49,
        101, 216, 247, 43,  254, 163, 190, 227, 166, 4,   0,   164, 103, 141, 103, 90,  211, 20,
        9,   234, 224, 193, 0,   29,  37,  63,  68,  187, 97,  39,  233, 186, 7,   191, 223, 166,
        207, 218, 91,  7,   77,  100, 135, 154, 48,  121, 8,   124, 226, 179, 117, 189, 127, 197,
        20,  34,  125, 128, 178, 219, 133, 107, 241, 229, 129, 124, 105, 223, 41,  83,  217, 97,
        194, 216, 138, 12,  236, 194, 247, 195, 224, 244, 3,   115, 225, 178, 130, 208, 50,  217,
        89,  102, 200, 89,  94,  237, 190, 12,  117, 77,  245, 122, 248, 243, 37,  169, 206, 219,
        205, 105, 118, 7,   73,  242, 99,  77,  43,  205, 117, 48,  24,  15,  33,  161, 248, 125,
        235, 212, 177, 207, 164, 153, 188, 199, 237, 153, 37,  1,   122, 146, 247, 253, 91,  166,
        64,  245, 74,  136, 16,  135, 203, 232, 219, 188, 147, 28,  91,  162, 41,  192, 125, 37,
        223, 113, 144, 47,  224, 133, 234, 0,   155, 139, 55,  204, 144, 201, 94,  42,  242, 244,
        206, 27,  152, 225, 50,  176, 93,  228, 98,  29,  250, 152, 226, 157, 209, 109, 225, 117,
        110, 138, 42,  223, 161, 171, 84,  150, 146, 241, 172, 55,  151, 166, 114, 75,  161, 182,
        163, 229, 210, 63,  48,  94,  24,  6,   40,  33,  104, 3,   2,   239, 39,  53,  40,  79,
        224, 168, 232, 134, 243, 212, 186, 3,   251, 174, 50,  22,  226, 249, 93,  102, 103, 2,
        247, 20,  140, 177, 0,   70,  88,  139, 66,  65,  17,  185, 171, 11,  16,  228, 10,  46,
        217, 142, 223, 94,  226, 234, 248, 58,  8,   70,  199, 175, 55,  108, 174, 22,  55,  223,
        31,  61,  152, 85,  217, 214, 35,  169, 62,  191, 206, 208, 77,  250, 124, 29,  2,   137,
        151, 222, 102, 237, 248, 32,  168, 215, 128, 154, 172, 229, 76,  83,  123, 88,  226, 205,
        8,   40,  203, 130, 52,  129, 25,  238, 208, 207, 171, 48,  131, 191, 102, 162, 233, 175,
        70,  174, 34,  83,  36,  136, 15,  155, 212, 136, 115, 57,  55,  176, 53,  193, 105, 8,
        74,  198, 213, 78,  105, 21,  72,  72,  34,  24,  31,  223, 174, 126, 191, 49,  221, 33,
        41,  158, 21,  199, 41,  56,  92,  30,  90,  77,  223, 236, 210, 107, 229, 242, 75,  178,
        34,  206, 199, 34,  248, 238, 154, 218, 140, 128, 224, 97,  10,  34,  58,  212, 199, 192,
        123, 45,  31,  157, 80,  223, 171, 53,  205, 72,  65,  180, 167, 44,  155, 27,  179, 123,
        224, 22,  45,  148, 47,  67,  92,  138, 57,  125, 35,  97,  151, 199, 78,  111, 243, 85,
        230, 237, 250, 136, 153, 111, 12,  229, 121, 95,  251, 211, 42,  26,  237, 204, 55,  241,
        28,  32,  153, 126, 24,  113, 88,  251, 208, 75,  250, 180, 98,  206, 43,  195, 175, 158,
        102, 127, 72,  9,   53,  66,  141, 172, 92,  47,  188, 40,  153, 249, 97,  200, 51,  92,
        145, 50,  111, 169, 7,   173, 42,  45,  23,  178, 241, 239, 37,  111, 215, 80,  113, 92,
        179, 122, 51,  216, 15,  5,   204, 100, 56,  36,  251, 28,  120, 29,  238, 76,  161, 34,
        217, 191, 93,  148, 43,  44,  80,  224, 129, 236, 134, 48,  55,  4,   175, 69,  67,  223,
        30,  252, 37,  227, 125, 179, 73,  106, 235, 251, 235, 169, 77,  191, 173, 118, 104, 155,
        247, 232, 36,  128, 207, 232, 128, 233, 85,  57,  74,  239, 130, 228, 245, 218, 250, 67,
        212, 33,  40,  213, 194, 162, 232, 98,  118, 249, 85,  27,  243, 157, 146, 149, 138, 239,
        160, 14,  197, 159, 82,  31,  135, 219, 78,  129, 23,  108, 10,  61,  48,  158, 111, 72,
        104, 49,  147, 150, 64,  125, 167, 161, 247, 168, 134, 95,  103, 71,  31,  22,  201, 122,
        42,  147, 207, 30,  39,  225, 153, 108, 240, 33,  134, 152, 0,   122, 251, 13,  203, 193,
        118, 201, 123, 189, 70,  209, 109, 12,  225, 109, 191, 236, 246, 31,  60,  112, 110, 166,
        39,  203, 164, 202, 176, 54,  77,  94,  160, 174, 207, 135, 150, 133, 7,   96,  178, 144,
        3,   218, 115, 190, 227, 250, 12,  90,  40,  253, 245, 169, 19,  241, 44,  214, 246, 51,
        29,  247, 4,   199, 61,  188, 233, 73,  91,  87,  133, 134, 24,  248, 38,  43,  246, 95,
        158, 44,  61,  200, 47,  99,  19,  32,  84,  87,  167, 26,  98,  244, 237, 118, 172, 240,
        110, 106, 132, 157, 194, 83,  164, 150, 88,  184, 249, 23,  188, 253, 3,   236, 123, 59,
        81,  112, 114, 66,  40,  228, 94,  83,  75,  36,  168, 223, 74,  12,  58,  185, 144, 124,
        243, 79,  193, 222, 82,  14,  157, 117, 129, 12,  73,  225, 36,  47,  197, 202, 204, 112,
        107, 189, 169, 132, 45,  39,  171, 218, 103, 58,  123, 198, 46,  116, 12,  80,  89,  18,
        130, 112, 134, 129, 205, 179, 109, 152, 90,  4,   56,  43,  221, 129, 93,  28,  215, 239,
        21,  122, 255, 102, 144, 119, 121, 2,   53,  87,  7,   189, 141, 74,  77,  161, 251, 23,
        80,  189, 173, 181, 173, 233, 76,  183, 187, 4,   35,  214, 208, 78,  35,  210, 192, 51,
        214, 152, 124, 36,  176, 192, 112, 15,  165, 150, 105, 140, 151, 58,  63,  181, 12,  58,
        198, 167, 21,  44,  123, 158, 69,  116, 53,  18,  15,  139, 61,  25,  78,  88,  118, 216,
        94,  102, 157, 39,  137, 28,  173, 203, 203, 230, 110, 29,  210, 215, 64,  139, 154, 216,
        80,  93,  46,  221, 108, 162, 162, 101, 37,  180, 145, 113, 16,  111, 49,  59,  71,  55,
        74,  201, 245, 25,  89,  60,  29,  82,  9,   87,  50,  184, 90,  104, 209, 249, 176, 140,
        121, 114, 56,  150, 97,  119, 62,  206, 177, 241, 207, 97,  188, 243, 151, 145, 5,   109,
        218, 243, 172, 228, 1,   10,  3,   144, 196, 125, 12,  198, 125, 92,  136, 9,   69,  35,
        112, 194, 116, 194, 76,  220, 20,  239, 9,   195, 72,  66,  118, 233, 58,  37,  200, 189,
        204, 81,  148, 118, 189, 28,  145, 121, 175, 220, 128, 6,   205, 147, 92,  171, 88,  89,
        8,   103, 142, 124, 204, 185, 215, 224, 38,  102, 55,  94,  165, 77,  195, 50,  226, 47,
        30,  229, 164, 32,  64,  60,  24,  135, 233, 237, 175, 52,  149, 177, 22,  186, 0,   139,
        144, 138, 226, 155, 196, 190, 229, 8,   105, 110, 255, 248, 146, 67,  52,  29,  56,  52,
        147, 193, 40,  74,  73,  5,   242, 152, 215, 105, 74,  158, 224, 180, 236, 250, 171, 75,
        81,  199, 44,  34,  221, 97,  224, 236, 38,  80,  63,  140, 96,  71,  234, 210, 79,  91,
        173, 25,  150, 46,  79,  29,  61,  61,  216, 176, 191, 13,  152, 130, 201, 76,  72,  64,
        249, 128, 138, 23,  186, 251, 243, 36,  101, 198, 237, 142, 20,  20,  107, 197, 108, 40,
        50,  240, 24,  76,  72,  36,  60,  213, 179, 45,  76,  49,  73,  233, 244, 200, 99,  23,
        138, 60,  249, 127, 164, 250, 41,  245, 24,  197, 237, 59,  245, 31,  58,  44,  87,  140,
        194, 69,  172, 202, 42,  197, 101, 92,  145, 57,  222, 110, 151, 4,   178, 7,   111, 38,
        224, 229, 142, 158, 23,  104, 47,  216, 39,  232, 35,  237, 5,   230, 141, 79,  23,  157,
        208, 12,  97,  234, 88,  218, 238, 7,   148, 101, 206, 16,  100, 172, 182, 142, 241, 5,
        176, 81,  69,  251, 147, 160, 47,  221, 102, 127, 36,  194, 50,  163, 155, 100, 191, 191,
        43,  72,  25,  162, 142, 51,  123, 41,  93,  155, 86,  7,   136, 96,  67,  248, 211, 58,
        235, 97,  163, 1,   249, 29,  180, 118, 73,  128, 36,  169, 58,  104, 105, 105, 136, 93,
        252, 238, 221, 251, 211, 44,  180, 206, 113, 6,   100, 139, 147, 223, 83,  38,  72,  151,
        63,  205, 161, 129, 2,   23,  42,  164, 242, 242, 197, 116, 11,  181, 25,  196, 173, 79,
        179, 212, 203, 43,  43,  107, 153, 69,  211, 179, 28,  193, 235, 254, 215, 63,  1,   229,
        49,  152, 15,  13,  168, 79,  199, 39,  168, 168, 100, 191, 107, 197, 220, 155, 187, 201,
        26,  166, 122, 24,  244, 33,  172, 210, 221, 185, 176, 159, 31,  19,  29,  87,  89,  25,
        39,  139, 75,  77,  205, 106, 32,  18,  189, 247, 164, 74,  189, 42,  7,   139, 97,  145,
        236, 162, 196, 166, 115, 118, 115, 138, 250, 64,  221, 15,  50,  157, 67,  43,  0,   148,
        223, 5,   213, 102, 25,  63,  84,  99,  75,  127, 158, 82,  56,  155, 197, 124, 120, 212,
        135, 44,  116, 199, 195, 136, 201, 119, 233, 187, 8,   213, 233, 180, 26,  173, 205, 69,
        63,  232, 167, 236, 250, 13,  239, 210, 35,  16,  28,  209, 102, 72,  245, 73,  183, 220,
        99,  220, 18,  67,  111, 40,  254, 89,  224, 233, 0,   17,  152, 137, 32,  138, 79,  12,
        212, 159, 238, 71,  12,  228, 163, 251, 27,  27,  31,  239, 224, 8,   242, 199, 251, 69,
        219, 245, 164, 67,  14,  180, 210, 28,  136, 227, 56,  76,  112, 113, 51,  84,  243, 171,
        70,  15,  159, 64,  175, 54,  106, 59,  31,  80,  65,  4,   86,  251, 231, 247, 141, 246,
        117, 242, 200, 247, 54,  112, 128, 90,  35,  143, 126, 214, 59,  160, 224, 70,  106, 203,
        247, 87,  191, 169, 97,  58,  81,  217, 47,  133, 112, 179, 171, 144, 102, 72,  11,  2,
        251, 91,  108, 120, 245, 166, 52,  47,  101, 229, 80,  254, 242, 252, 242, 104, 10,  125,
        111, 153, 218, 80,  219, 47,  249, 222, 193, 184, 11,  212, 156, 202, 120, 151, 175, 160,
        101, 163, 38,  81,  245, 99,  142, 36,  56,  10,  54,  139, 191, 60,  175, 45,  197, 56,
        185, 154, 41,  115, 20,  175, 222, 52,  185, 168, 15,  156, 173, 227, 105, 194, 223, 18,
        212, 196, 207, 140, 211, 234, 132, 106, 80,  65,  177, 72,  178, 245, 113, 237, 72,  75,
        184, 17,  48,  226, 91,  184, 103, 64,  209, 66,  234, 15,  137, 123, 206, 153, 117, 249,
        106, 151, 232, 152, 184, 80,  207, 156, 115, 37,  134, 235, 198, 85,  39,  241, 163, 209,
        235, 118, 235, 156, 57,  27,  67,  176, 209, 41,  18,  22,  225, 77,  61,  152, 222, 198,
        243, 74,  115, 65,  143, 17,  97,  75,  181, 84,  250, 3,   176, 109, 69,  34,  76,  0,
        111, 206, 198, 250, 165, 34,  182, 86,  57,  130, 157, 227, 123, 163, 131, 230, 214, 147,
        192, 241, 86,  113, 74,  46,  234, 2,   35,  220, 156, 73,  251, 243, 17,  188, 55,  100,
        191, 132, 49,  59,  97,  146, 112, 12,  157, 17,  237, 124, 78,  4,   117, 254, 56,  146,
        96,  205, 122, 120, 153, 148, 242, 180, 176, 80,  182, 75,  79,  57,  87,  117, 223, 205,
        159, 199, 174, 192, 125, 40,  58,  231, 112, 16,  81,  83,  105, 76,  244, 145, 241, 153,
        9,   71,  20,  229, 230, 98,  92,  186, 58,  41,  134, 25,  129, 60,  189, 106, 230, 151,
        81,  39,  119, 106, 174, 162, 170, 161, 152, 249, 170, 81,  121, 17,  56,  18,  100, 243,
        92,  31,  58,  155, 25,  160, 26,  151, 186, 96,  150, 34,  186, 49,  184, 28,  53,  180,
        40,  208, 11,  179, 151, 86,  71,  232, 149, 251, 67,  10,  224, 133, 149, 103, 198, 127,
        129, 241, 239, 230, 147, 216, 104, 1,   94,  164, 38,  132, 9,   231, 221, 117, 178, 187,
        230, 161, 52,  88,  73,  188, 25,  15,  36,  225, 89,  94,  56,  7,   251, 82,  16,  186,
        247, 174, 89,  137, 29,  59,  160, 132, 13,  183, 120, 134, 19,  125, 236, 252, 2,   123,
        140, 186, 255, 242, 95,  223, 50,  204, 236, 169, 130, 166, 43,  210, 56,  148, 248, 190,
        212, 132, 88,  205, 93,  150, 87,  242, 83,  56,  255, 174, 224, 65,  253, 130, 10,  241,
        123, 184, 240, 147, 99,  241, 8,   21,  45,  245, 136, 166, 193, 182, 117, 178, 24,  70,
        155, 241, 156, 243, 205, 112, 169, 228, 39,  134, 228, 76,  168, 115, 229, 20,  37,  55,
        199, 113, 241, 120, 249, 18,  129, 202, 223, 232, 134, 69,  130, 234, 108, 124, 90,  48,
        145, 126, 212, 168, 207, 105, 245, 94,  24,  43,  190, 217, 170, 106, 48,  198, 149, 152,
        160, 75,  158, 147, 177, 252, 159, 14,  137, 48,  249, 141, 41,  149, 224, 240, 194, 32,
        247, 71,  104, 91,  237, 115, 103, 122, 24,  187, 23,  5,   194, 48,  139, 67,  191, 188,
        210, 171, 240, 123, 190, 75,  111, 141, 194, 2,   177, 192, 4,   135, 97,  53,  83,  13,
        143, 13,  164, 96,  80,  65,  167, 158, 57,  207, 111, 154, 169, 48,  172, 210, 184, 55,
        72,  120, 127, 110, 101, 233, 48,  83,  207, 252, 243, 16,  98,  235, 185, 32,  202, 41,
        244, 102, 222, 90,  45,  125, 24,  105, 132, 162, 204, 194, 207, 22,  156, 57,  223, 61,
        37,  181, 57,  76,  99,  12,  55,  36,  80,  38,  252, 149, 191, 12,  235, 186, 185, 57,
        199, 57,  67,  97,  91,  182, 139, 113, 29,  87,  91,  34,  198, 189, 104, 126, 225, 48,
        106, 162, 219, 83,  197, 195, 59,  188, 186, 185, 225, 213, 208, 75,  164, 68,  39,  138,
        159, 185, 185, 43,  32,  46,  231, 75,  116, 18,  50,  202, 238, 217, 117, 87,  50,  224,
        120, 248, 45,  63,  1,   106, 196, 78,  28,  54,  85,  56,  105, 155, 165, 235, 93,  13,
        107, 167, 136, 29,  168, 126, 255, 69,  90,  182, 11,  30,  178, 181, 187, 174, 223, 109,
        31,  163, 72,  191, 213, 247, 221, 244, 135, 167, 35,  86,  197, 154, 33,  211, 69,  100,
        40,  48,  87,  83,  212, 221, 229, 204, 116, 77,  204, 232, 47,  209, 81,  102, 100, 216,
        163, 181, 55,  90,  12,  30,  183, 53,  100, 126, 126, 68,  190, 5,   61,  28,  80,  231,
        62,  243, 83,  136, 183, 29,  150, 120, 82,  18,  27,  34,  211, 134, 132, 193, 206, 210,
        5,   45,  96,  242, 75,  157, 78,  224, 168, 201, 52,  236, 169, 186, 48,  215, 229, 192,
        94,  154, 190, 88,  78,  49,  64,  193, 149, 39,  39,  211, 3,   174, 157, 48,  3,   168,
        55,  106, 116, 251, 204, 206, 222, 249, 69,  7,   116, 42,  202, 88,  109, 197, 33,  205,
        129, 11,  165, 159, 107, 204, 168, 104, 171, 74,  203, 46,  231, 62,  113, 157, 101, 201,
        144, 65,  16,  92,  19,  24,  136, 160, 138, 132, 143, 79,  32,  83,  24,  50,  195, 2,
        94,  144, 228, 147, 142, 17,  115, 84,  57,  20,  17,  62,  170, 26,  119, 211, 36,  3,
        192, 192, 31,  34,  16,  39,  147, 72,  97,  84,  153, 16,  181, 240, 56,  252, 201, 12,
        233, 63,  122, 8,   137, 50,  90,  55,  98,  107, 38,  68,  119, 229, 192, 106, 160, 191,
        24,  188, 235, 95,  233, 128, 90,  99,  15,  230, 9,   123, 107, 61,  237, 129, 153, 112,
        75,  26,  53,  139, 189, 65,  211, 5,   233, 59,  131, 197, 165, 115, 137, 95,  43,  181,
        169, 175, 7,   254, 2,   248, 164, 222, 182, 32,  62,  93,  248, 50,  88,  26,  11,  145,
        130, 220, 39,  168, 96,  206, 216, 79,  126, 181, 69,  35,  144, 56,  232, 38,  32,  226,
        128, 101, 31,  244, 9,   57,  116, 107, 219, 214, 106, 56,  132, 150, 138, 49,  3,   62,
        69,  80,  241, 224, 172, 219, 118, 176, 119, 155, 35,  56,  120, 165, 90,  119, 87,  40,
        21,  152, 157, 100, 239, 101, 163, 169, 233, 165, 66,  60,  169, 99,  5,   205, 114, 15,
        102, 78,  109, 36,  225, 208, 84,  45,  183, 40,  153, 212, 105, 60,  190, 237, 185, 249,
        250, 214, 7,   219, 46,  44,  45,  156, 62,  228, 145, 36,  131, 213, 23,  138, 133, 52,
        245, 29,  176, 159, 217, 157, 131, 29,  58,  10,  245, 215, 61,  127, 1,   90,  253, 9,
        227, 182, 96,  131, 169, 62,  222, 151, 212, 24,  221, 241, 186, 231, 126, 186, 71,  114,
        173, 222, 16,  181, 241, 88,  172, 57,  145, 103, 32,  213, 210, 108, 141, 8,   5,   245,
        49,  115, 102, 101, 7,   206, 12,  114, 210, 159, 105, 1,   50,  213, 88,  19,  189, 29,
        119, 193, 104, 51,  146, 141, 177, 192, 54,  235, 53,  80,  216, 83,  123, 148, 166, 117,
        242, 70,  169, 242, 70,  191, 73,  49,  21,  232, 207, 168, 253, 51,  254, 162, 23,  116,
        16,  79,  124, 24,  188, 102, 28,  103, 173, 64,  177, 143, 158, 23,  165, 219, 240, 153,
        97,  244, 211, 188, 58,  205, 96,  151, 205, 221, 119, 39,  32,  253, 81,  119, 255, 70,
        94,  147, 224, 149, 232, 200, 1,   129, 84,  205, 51,  134, 207, 222, 39,  21,  98,  43,
        140, 64,  42,  249, 11,  124, 26,  88,  224, 32,  141, 249, 29,  199, 248, 95,  156, 136,
        3,   229, 190, 178, 81,  40,  162, 57,  104, 107, 9,   131, 116, 137, 27,  16,  160, 240,
        166, 135, 226, 65,  8,   119, 111, 123, 184, 183, 200, 238, 178, 199, 12,  71,  193, 22,
        34,  182, 123, 180, 147, 207, 119, 64,  42,  105, 25,  38,  9,   223, 204, 73,  14,  42,
        133, 190, 154, 175, 253, 195, 245, 29,  86,  65,  36,  121, 230, 162, 216, 252, 237, 79,
        89,  3,   210, 61,  204, 114, 90,  46,  3,   235, 157, 139, 162, 210, 238, 40,  34,  128,
        123, 146, 5,   160, 4,   46,  1,   76,  196, 122, 136, 135, 117, 212, 122, 185, 77,  230,
        67,  133, 206, 67,  252, 214, 23,  111, 228, 206, 168, 87,  15,  9,   59,  100, 100, 164,
        130, 224, 109, 149, 235, 57,  204, 2,   112, 173, 232, 129, 200, 160, 158, 102, 139, 124,
        184, 39,  54,  96,  223, 231, 41,  103, 222, 98,  113, 172, 228, 15,  15,  224, 196, 62,
        84,  3,   65,  105, 37,  237, 74,  188, 170, 203, 142, 4,   77,  157, 206, 154, 128, 50,
        227, 144, 27,  193, 255, 60,  143, 48,  56,  69,  215, 1,   122, 163, 204, 51,  7,   64,
        26,  115, 122, 90,  70,  182, 233, 68,  141, 183, 43,  237, 129, 12,  3,   108, 67,  18,
        88,  72,  185, 175, 119, 125, 235, 20,  147, 63,  134, 182, 68,  227, 188, 46,  7,   115,
        91,  61,  42,  102, 149, 247, 141, 241, 224, 183, 149, 222, 104, 173, 192, 113, 243, 39,
        160, 176, 53,  26,  41,  174, 25,  4,   161, 226, 191, 232, 180, 163, 172, 251, 239, 239,
        42,  114, 8,   247, 21,  74,  182, 181, 168, 216, 133, 236, 136, 142, 16,  14,  53,  240,
        234, 247, 155, 197, 203, 109, 179, 53,  247, 67,  205, 114, 122, 51,  186, 83,  133, 127,
        246, 222, 196, 218, 232, 254, 231, 12,  123, 56,  13,  5,   128, 208, 152, 49,  159, 238,
        243, 208, 175, 52,  123, 103, 147, 43,  49,  92,  32,  109, 62,  70,  145, 75,  226, 23,
        94,  131, 154, 242, 0,   63,  63,  221, 78,  251, 135, 106, 56,  226, 79,  1,   35,  133,
        158, 176, 215, 144, 31,  16,  119, 188, 68,  150, 20,  69,  119, 236, 85,  1,   72,  3,
        98,  227, 49,  223, 144, 115, 33,  235, 100, 224, 151, 57,  19,  111, 244, 242, 108, 50,
        112, 160, 131, 58,  249, 240, 184, 116, 175, 113, 182, 22,  13,  53,  181, 84,  213, 12,
        78,  188, 101, 195, 104, 138, 136, 166, 104, 145, 194, 165, 121, 49,  249, 201, 171, 231,
        150, 4,   234, 86,  248, 78,  230, 115, 249, 40,  79,  65,  253, 105, 87,  30,  100, 169,
        0,   198, 101, 35,  16,  138, 132, 38,  157, 210, 173, 5,   146, 205, 166, 242, 39,  145,
        135, 190, 14,  208, 146, 21,  233, 76,  227, 212, 135, 27,  196, 39,  129, 168, 113, 6,
        9,   244, 103, 203, 138, 149, 41,  105, 187, 108, 137, 230, 90,  159, 209, 161, 233, 140,
        79,  129, 202, 219, 162, 110, 23,  91,  61,  28,  61,  92,  118, 253, 175, 199, 207, 197,
        4,   189, 41,  23,  225, 215, 242, 251, 146, 86,  18,  166, 141, 73,  30,  205, 127, 162,
        186, 147, 195, 136, 30,  134, 205, 35,  65,  250, 216, 161, 211, 212, 96,  229, 16,  64,
        207, 144, 106, 117, 110, 192, 201, 120, 234, 95,  223, 142, 231, 16,  144, 100, 205, 86,
        200, 164, 20,  184, 35,  47,  138, 182, 188, 199, 0,   35,  101, 178, 243, 20,  203, 66,
        160, 63,  58,  12,  231, 76,  230, 187, 160, 254, 235, 221, 16,  17,  50,  102, 22,  63,
        113, 27,  106, 60,  138, 208, 104, 37,  80,  251, 253, 233, 197, 215, 80,  195, 235, 235,
        83,  79,  125, 163, 35,  246, 120, 129, 182, 245, 100, 37,  0,   3,   5,   168, 50,  238,
        111, 166, 221, 6,   33,  23,  51,  229, 92,  82,  83,  12,  39,  161, 35,  30,  95,  185,
        15,  27,  72,  142, 65,  214, 252, 184, 249, 225, 188, 83,  187, 215, 81,  120, 180, 195,
        202, 177, 86,  90,  104, 241, 104, 172, 126, 39,  155, 114, 63,  48,  120, 239, 77,  201,
        252, 89,  172, 209, 153, 40,  246, 111, 108, 4,   76,  175, 190, 207, 96,  247, 75,  217,
        19,  219, 12,  117, 67,  113, 108, 71,  63,  100, 57,  43,  224, 213, 7,   153, 185, 168,
        23,  42,  25,  109, 45,  205, 46,  129, 123, 227, 157, 244, 207, 35,  95,  51,  130, 128,
        12,  252, 236, 25,  121, 21,  161, 111, 62,  30,  135, 173, 126, 254, 147, 114, 14,  55,
        32,  116, 250, 82,  249, 229, 97,  135, 239, 14,  158, 130, 30,  221, 54,  117, 218, 46,
        122, 114, 243, 224, 6,   76,  184, 70,  231, 104, 19,  225, 195, 161, 187, 116, 102, 235,
        144, 203, 45,  223, 26,  22,  248, 107, 10,  40,  95,  110, 95,  216, 193, 242, 150, 150,
        188, 80,  237, 63,  247, 241, 3,   80,  167, 229, 22,  111, 92,  117, 228, 92,  166, 141,
        248, 127, 232, 179, 242, 17,  234, 33,  74,  255, 69,  1,   241, 97,  74,  254, 175, 187,
        66,  6,   104, 180, 48,  44,  120, 221, 124, 82,  204, 28,  24,  39,  90,  23,  191, 150,
        99,  221, 126, 71,  167, 189, 236, 118, 61,  163, 122, 47,  144, 73,  115, 179, 0,   197,
        88,  44,  120, 211, 153, 189, 27,  239, 240, 114, 244, 164, 32,  111, 200, 99,  135, 248,
        50,  173, 21,  229, 234, 194, 125, 128, 94,  43,  200, 54,  17,  76,  28,  215, 76,  222,
        56,  61,  103, 181, 65,  44,  189, 136, 130, 172, 73,  19,  180, 164, 81,  168, 15,  176,
        69,  109, 103, 151, 213, 228, 145, 219, 149, 63,  252, 19,  13,  190, 20,  230, 54,  210,
        64,  82,  16,  12,  178, 25,  3,   165, 119, 22,  131, 32,  159, 249, 123, 221, 176, 252,
        243, 222, 180, 41,  8,   157, 127, 141, 255, 23,  157, 109, 78,  102, 200, 8,   255, 71,
        78,  115, 76,  0,   136, 135, 129, 217, 126, 91,  68,  92,  180, 131, 136, 14,  44,  100,
        10,  207, 172, 141, 70,  114, 115, 12,  245, 38,  215, 8,   106, 197, 74,  78,  14,  221,
        99,  197, 126, 204, 217, 113, 177, 149, 52,  67,  179, 59,  133, 87,  148, 20,  145, 234,
        122, 193, 240, 48,  103, 223, 15,  161, 201, 78,  111, 78,  93,  60,  197, 101, 49,  51,
        208, 53,  252, 144, 211, 29,  93,  190, 98,  120, 123, 250, 191, 2,   8,   145, 156, 1,
        220, 10,  54,  70,  197, 197, 215, 172, 213, 187, 51,  11,  164, 62,  247, 39,  79,  228,
        155, 218, 231, 65,  248, 81,  112, 121, 204, 201, 90,  87,  182, 175, 200, 119, 182, 179,
        252, 48,  115, 14,  113, 29,  115, 185, 248, 201, 215, 74,  220, 53,  117, 205, 152, 104,
        144, 68,  203, 179, 208, 228, 53,  215, 178, 153, 75,  144, 30,  156, 164, 160, 133, 170,
        208, 26,  16,  58,  146, 109, 110, 237, 199, 58,  180, 15,  128, 245, 8,   148, 194, 159,
        232, 214, 188, 135, 181, 61,  233, 182, 159, 233, 216, 250, 38,  193, 53,  192, 218, 144,
        85,  179, 171, 69,  178, 43,  139, 72,  222, 121, 83,  198, 227, 35,  183, 61,  8,   221,
        27,  252, 4,   34,  143, 142, 198, 58,  193, 97,  162, 24,  100, 128, 245, 208, 243, 218,
        233, 21,  146, 5,   75,  97,  244, 81,  234, 76,  224, 29,  85,  243, 242, 169, 191, 195,
        234, 229, 14,  48,  150, 129, 220, 233, 133, 30,  122, 57,  159, 192, 144, 135, 69,  163,
        29,  158, 135, 156, 77,  77,  141, 63,  33,  143, 124, 243, 37,  23,  204, 163, 6,   86,
        56,  98,  77,  156, 215, 120, 118, 139, 124, 160, 62,  212, 133, 197, 218, 37,  57,  73,
        32,  232, 235, 154, 198, 156, 177, 142, 216, 30,  248, 85,  178, 236, 15,  72,  247, 47,
        59,  73,  158, 221, 73,  149, 233, 150, 21,  96,  168, 227, 75,  215, 26,  194, 171, 212,
        103, 90,  187, 61,  152, 124, 84,  116, 162, 23,  215, 128, 251, 76,  83,  186, 173, 47,
        60,  213, 31,  37,  30,  79,  10,  197, 147, 144, 235, 212, 131, 202, 63,  31,  120, 161,
        95,  76,  107, 1,   17,  105, 31,  186, 106, 19,  7,   60,  213, 209, 126, 44,  220, 94,
        171, 127, 105, 11,  250, 199, 37,  234, 171, 47,  166, 67,  251, 219, 244, 88,  233, 186,
        226, 233, 203, 167, 89,  75,  1,   104, 27,  233, 170, 71,  127, 165, 46,  168, 39,  27,
        255, 103, 106, 148, 97,  57,  91,  5,   10,  3,   29,  83,  35,  218, 109, 97,  147, 250,
        115, 251, 48,  246, 102, 211, 82,  206, 183, 203, 249, 144, 65,  151, 50,  209, 163, 150,
        101, 91,  236, 99,  162, 98,  219, 239, 141, 100, 40,  90,  130, 166, 135, 57,  73,  193,
        34,  18,  122, 77,  99,  127, 198, 236, 193, 182, 186, 8,   111, 10,  125, 20,  68,  217,
        111, 143, 102, 53,  65,  12,  160, 34,  119, 121, 194, 61,  65,  219, 173, 142, 27,  242,
        177, 5,   48,  193, 134, 103, 208, 54,  12,  224, 243, 202, 182, 130, 85,  73,  189, 201,
        127, 160, 93,  212, 167, 1,   156, 94,  212, 177, 9,   128, 161, 19,  23,  33,  55,  95,
        27,  0,   66,  117, 112, 131, 18,  185, 30,  199, 234, 45,  5,   201, 39,  14,  150, 60,
        25,  196, 165, 160, 2,   67,  120, 245, 110, 142, 208, 159, 192, 235, 168, 248, 241, 237,
        82,  139, 172, 233, 216, 198, 249, 58,  29,  133, 65,  240, 202, 92,  41,  195, 187, 242,
        140, 97,  252, 13,  85,  238, 14,  97,  19,  120, 63,  97,  83,  73,  79,  88,  151, 68,
        181, 234, 0,   33,  14,  147, 117, 176, 188, 27,  5,   85,  79,  4,   123, 9,   238, 48,
        31,  105, 8,   150, 91,  170, 249, 145, 241, 108, 119, 115, 43,  171, 43,  183, 178, 101,
        66,  182, 147, 55,  80,  169, 51,  74,  198, 224, 154, 196, 20,  48,  9,   169, 128, 163,
        42,  246, 188, 223, 234, 137, 238, 76,  198, 36,  104, 161, 119, 86,  176, 140, 13,  46,
        74,  10,  9,   100, 40,  32,  247, 30,  250, 23,  68,  103, 143, 79,  242, 100, 152, 246,
        111, 123, 103, 180, 72,  27,  0,   4,   231, 202, 95,  90,  120, 204, 1,   59,  52,  27,
        149, 138, 102, 43,  102, 151, 116, 214, 175, 214, 140, 238, 3,   217, 69,  117, 160, 6,
        151, 167, 98,  121, 83,  183, 245, 245, 15,  60,  109, 90,  50,  182, 59,  129, 254, 23,
        56,  83,  50,  221, 98,  26,  181, 97,  130, 105, 70,  2,   54,  148, 28,  238, 85,  210,
        60,  143, 47,  163, 48,  135, 41,  89,  215, 175, 207, 92,  154, 210, 12,  48,  138, 32,
        238, 90,  140, 220, 103, 210, 224, 82,  103, 55,  69,  139, 19,  151, 217, 119, 120, 155,
        220, 25,  96,  186, 204, 109, 179, 54,  89,  189, 101, 151, 126, 222, 1,   213, 22,  213,
        198, 21,  148, 219, 165, 103, 1,   16,  214, 129, 150, 74,  2,   32,  59,  210, 171, 14,
        11,  71,  83,  63,  30,  115, 65,  2,   129, 152, 79,  43,  187, 177, 197, 141, 120, 67,
        10,  91,  71,  142, 233, 169, 144, 103, 12,  52,  22,  161, 95,  243, 101, 218, 106, 217,
        16,  156, 88,  159, 252, 141, 91,  187, 198, 241, 222, 43,  182, 162, 8,   239, 143, 53,
        145, 38,  25,  88,  66,  137, 91,  186, 208, 138, 252, 180, 46,  47,  27,  210, 112, 191,
        204, 6,   19,  220, 232, 229, 205, 211, 11,  17,  254, 34,  87,  255, 83,  201, 58,  65,
        92,  187, 157, 85,  134, 148, 201, 72,  148, 219, 110, 149, 119, 206, 194, 109, 241, 139,
        181, 248, 41,  69,  72,  132, 113, 232, 159, 107, 130, 47,  44,  38,  107, 179, 146, 254,
        112, 134, 23,  51,  50,  143, 251, 80,  87,  4,   34,  59,  190, 67,  37,  135, 242, 164,
        213, 254, 33,  238, 109, 77,  7,   211, 222, 250, 122, 238, 147, 117, 139, 74,  6,   211,
        114, 77,  251, 110, 184, 164, 36,  175, 45,  122, 21,  73,  141, 222, 41,  14,  60,  212,
        74,  167, 213, 242, 115, 134, 116, 110, 31,  32,  135, 142, 9,   48,  19,  252, 103, 125,
        32,  153, 119, 92,  238, 227, 196, 73,  249, 193, 83,  153, 80,  233, 196, 151, 133, 35,
        193, 21,  105, 79,  88,  9,   102, 229, 104, 48,  40,  132, 128, 98,  33,  109, 185, 130,
        237, 162, 86,  112, 56,  251, 116, 255, 230, 40,  234, 85,  242, 24,  210, 253, 177, 86,
        132, 114, 36,  85,  191, 31,  36,  177, 71,  161, 138, 56,  0,   57,  220, 102, 84,  152,
        49,  189, 66,  134, 196, 181, 211, 61,  235, 193, 102, 238, 141, 84,  141, 243, 211, 76,
        125, 133, 150, 79,  5,   174, 206, 207, 84,  104, 63,  248, 42,  58,  141, 64,  135, 24,
        67,  115, 23,  237, 73,  31,  112, 20,  38,  220, 130, 43,  202, 105, 102, 75,  160, 202,
        197, 82,  14,  167, 50,  150, 148, 198, 168, 163, 78,  5,   160, 84,  81,  178, 84,  111,
        255, 87,  149, 12,  37,  169, 20,  1,   197, 228, 18,  52,  86,  172, 213, 35,  124, 48,
        11,  127, 85,  217, 130, 192, 43,  28,  255, 234, 245, 31,  170, 122, 234, 107, 176, 165,
        73,  5,   26,  115, 38,  250, 107, 161, 139, 242, 73,  4,   10,  35,  194, 128, 228, 242,
        208, 114, 229, 154, 170, 76,  112, 68,  64,  188, 136, 143, 81,  115, 159, 163, 13,  249,
        169, 34,  51,  171, 191, 30,  120, 186, 146, 2,   50,  213, 171, 203, 143, 123, 34,  197,
        185, 169, 80,  235, 189, 52,  213, 170, 57,  48,  71,  178, 140, 115, 53,  124, 124, 133,
        38,  69,  125, 148, 241, 118, 166, 156, 249, 10,  29,  243, 142, 97,  219, 63,  33,  190,
        207, 164, 229, 173, 223, 240, 248, 86,  218, 172, 167, 84,  46,  55,  9,   36,  183, 28,
        186, 123, 95,  184, 98,  10,  193, 81,  66,  41,  210, 248, 122, 52,  168, 9,   25,  110,
        238, 37,  170, 101, 91,  105, 198, 98,  132, 160, 245, 121, 73,  222, 129, 54,  18,  117,
        182, 127, 33,  8,   174, 61,  242, 88,  159, 189, 123, 41,  233, 134, 197, 110, 1,   228,
        27,  8,   174, 219, 143, 209, 120, 102, 155, 57,  238, 77,  103, 5,   205, 224, 201, 167,
        150, 23,  166, 189, 241, 176, 4,   48,  177, 73,  245, 59,  130, 215, 71,  179, 100, 61,
        178, 176, 93,  158, 215, 72,  248, 239, 123, 204, 186, 179, 171, 155, 217, 111, 236, 235,
        233, 35,  253, 90,  251, 31,  199, 65,  154, 239, 177, 32,  115, 89,  36,  241, 169, 161,
        109, 212, 24,  82,  95,  120, 50,  5,   154, 112, 1,   57,  212, 238, 89,  61,  170, 182,
        39,  228, 180, 186, 250, 215, 80,  226, 164, 228, 113, 89,  211, 238, 143, 87,  78,  90,
        150, 236, 31,  238, 205, 173, 234, 222, 4,   70,  33,  255, 78,  234, 125, 156, 234, 121,
        231, 98,  190, 70,  82,  147, 249, 89,  231, 99,  181, 20,  47,  76,  155, 252, 197, 104,
        99,  61,  251, 167, 55,  100, 167, 200, 179, 20,  252, 180, 206, 205, 101, 70,  184, 201,
        213, 147, 112, 243, 158, 155, 27,  58,  121, 201, 89,  246, 232, 180, 217, 100, 174, 65,
        185, 125, 183, 5,   167, 184, 116, 76,  116, 50,  117, 218, 162, 20,  15,  11,  92,  237,
        152, 25,  62,  22,  211, 49,  177, 252, 207, 49,  149, 77,  119, 56,  237, 166, 151, 150,
        121, 185, 205, 105, 41,  212, 187, 33,  0,   143, 21,  17,  155, 92,  52,  134, 99,  92,
        173, 113, 247, 49,  39,  228, 95,  29,  60,  89,  199, 66,  117, 127, 128, 103, 114, 246,
        234, 225, 80,  105, 254, 126, 124, 142, 218, 121, 197, 15,  16,  121, 121, 156, 117, 178,
        12,  213, 226, 158, 235, 192, 83,  193, 128, 86,  148, 156, 74,  43,  234, 175, 142, 247,
        233, 185, 202, 68,  30,  200, 30,  68,  145, 246, 165, 66,  231, 94,  210, 22,  48,  122,
        239, 172, 44,  174, 82,  185, 147, 232, 200, 181, 16,  170, 22,  149, 180, 228, 7,   63,
        63,  139, 168, 90,  116, 241, 18,  80,  254, 118, 62,  131, 59,  253, 124, 245, 5,   15,
        116, 203, 222, 205, 225, 58,  118, 22,  113, 54,  116, 68,  129, 225, 104, 142, 128, 244,
        13,  157, 240, 99,  221, 3,   85,  54,  93,  116, 6,   121, 36,  18,  36,  97,  217, 13,
        254, 5,   235, 167, 242, 175, 228, 30,  40,  210, 153, 58,  92,  205, 243, 215, 57,  255,
        159, 18,  244, 95,  209, 18,  79,  166, 226, 12,  181, 2,   110, 5,   58,  162, 114, 224,
        209, 40,  226, 170, 43,  253, 37,  253, 38,  181, 129, 208, 60,  155, 207, 190, 204, 69,
        55,  107, 227, 192, 44,  202, 26,  102, 139, 25,  132, 212, 193, 67,  150, 245, 240, 144,
        176, 11,  52,  254, 150, 111, 86,  185, 227, 181, 238, 12,  131, 54,  20,  120, 248, 113,
        5,   41,  132, 85,  159, 51,  108, 237, 195, 89,  75,  157, 180, 251, 199, 188, 66,  39,
        50,  196, 213, 141, 175, 37,  240, 148, 254, 189, 79,  52,  243, 181, 2,   11,  26,  59,
        33,  78,  182, 177, 23,  129, 33,  16,  120, 56,  8,   179, 158, 121, 125, 30,  166, 230,
        150, 147, 191, 252, 53,  162, 182, 195, 26,  61,  204, 146, 61,  179, 9,   17,  2,   41,
        142, 145, 102, 129, 171, 231, 104, 115, 236, 180, 80,  155, 51,  222, 58,  205, 183, 66,
        255, 14,  119, 43,  55,  228, 162, 178, 223, 151, 85,  126, 25,  122, 176, 78,  0,   234,
        94,  189, 68,  132, 164, 77,  136, 69,  44,  5,   36,  190, 40,  216, 174, 222, 167, 154,
        30,  0,   212, 10,  204, 240, 134, 174, 184, 156, 56,  170, 210, 243, 199, 209, 162, 213,
        144, 179, 143, 145, 4,   103, 249, 66,  216, 139, 143, 28,  191, 66,  115, 213, 73,  24,
        1,   22,  32,  157, 224, 16,  148, 214, 187, 241, 193, 129, 41,  222, 82,  232, 125, 220,
        113, 158, 227, 181, 126, 68,  125, 181, 25,  192, 19,  188, 250, 178, 111, 26,  45,  5,
        7,   252, 102, 41,  16,  147, 74,  223, 92,  34,  220, 101, 190, 208, 240, 249, 141, 7,
        72,  139, 44,  202, 119, 18,  0,   28,  116, 220, 187, 70,  187, 77,  181, 4,   5,   206,
        191, 52,  5,   178, 10,  46,  246, 84,  128, 23,  62,  176, 119, 116, 86,  230, 232, 61,
        125, 37,  181, 167, 90,  222, 216, 4,   89,  134, 104, 5,   253, 71,  144, 13,  182, 7,
        6,   98,  187, 19,  102, 171, 224, 36,  135, 106, 60,  3,   212, 235, 50,  233, 64,  246,
        225, 184, 154, 30,  158, 115, 196, 55,  139, 197, 19,  246, 200, 84,  148, 90,  28,  12,
        47,  118, 156, 60,  17,  255, 132, 177, 239, 99,  131, 250, 144, 229, 83,  225, 150, 154,
        163, 171, 107, 201, 33,  66,  72,  56,  202, 36,  46,  29,  91,  218, 85,  160, 254, 193,
        198, 105, 42,  173, 124, 48,  192, 219, 189, 251, 141, 80,  238, 211, 131, 194, 14,  228,
        232, 120, 8,   166, 237, 90,  117, 174, 146, 195, 220, 30,  212, 217, 57,  85,  30,  57,
        120, 107, 29,  189, 84,  84,  128, 225, 218, 162, 226, 134, 132, 18,  57,  167, 184, 48,
        136, 2,   131, 43,  42,  124, 194, 152, 178, 135, 237, 121, 198, 137, 66,  219, 51,  213,
        225, 132, 179, 255, 25,  141, 99,  78,  253, 148, 35,  31,  217, 95,  149, 175, 238, 3,
        100, 62,  217, 178, 111, 66,  33,  229, 121, 255, 183, 228, 124, 73,  61,  74,  191, 165,
        142, 254, 49,  82,  163, 166, 233, 169, 104, 79,  49,  111, 49,  124, 212, 48,  250, 45,
        166, 152, 202, 128, 5,   49,  71,  96,  218, 166, 92,  190, 210, 171, 180, 123, 119, 27,
        58,  239, 129, 186, 64,  82,  31,  72,  70,  50,  224, 28,  207, 8,   54,  229, 110, 238,
        41,  198, 205, 198, 16,  234, 170, 76,  45,  102, 191, 152, 232, 255, 142, 91,  21,  112,
        213, 239, 70,  75,  170, 248, 69,  117, 172, 3,   195, 19,  207, 138, 28,  34,  70,  58,
        71,  24,  41,  153, 166, 147, 125, 65,  56,  174, 204, 95,  134, 104, 3,   184, 47,  52,
        30,  254, 147, 172, 53,  122, 51,  117, 151, 124, 4,   222, 143, 165, 139, 103, 217, 20,
        39,  156, 92,  135, 160, 202, 3,   55,  228, 98,  213, 82,  136, 153, 203, 226, 172, 219,
        197, 131, 49,  164, 133, 4,   128, 19,  221, 141, 76,  247, 54,  8,   45,  31,  37,  188,
        94,  155, 122, 11,  247, 177, 107, 69,  163, 173, 175, 13,  73,  184, 214, 176, 173, 121,
        182, 225, 31,  119, 142, 55,  222, 24,  188, 114, 73,  210, 74,  25,  51,  197, 149, 19,
        186, 126, 111, 32,  247, 142, 109, 79,  117, 219, 223, 136, 89,  17,  14,  129, 231, 66,
        136, 111, 249, 39,  185, 60,  162, 175, 233, 148, 227, 23,  108, 151, 218, 218, 187, 47,
        158, 40,  234, 161, 15,  88,  104, 33,  46,  222, 235, 218, 207, 240, 73,  161, 57,  243,
        253, 222, 114, 116, 202, 206, 184, 106, 87,  131, 59,  163, 38,  78,  214, 162, 83,  158,
        77,  161, 68,  196, 229, 206, 156, 86,  68,  197, 82,  225, 159, 180, 23,  210, 133, 111,
        8,   49,  0,   137, 20,  164, 115, 50,  147, 51,  128, 99,  118, 21,  52,  193, 58,  160,
        127, 130, 61,  199, 168, 41,  157, 58,  14,  154, 37,  205, 189, 159, 50,  121, 138, 46,
        68,  233, 77,  227, 184, 110, 164, 158, 129, 16,  33,  223, 134, 236, 190, 140, 254, 153,
        206, 214, 58,  221, 207, 221, 133, 26,  144, 34,  2,   224, 45,  20,  95,  19,  72,  99,
        71,  63,  235, 118, 157, 98,  67,  22,  250, 188, 169, 202, 252, 153, 128, 207, 91,  195,
        13,  111, 221, 14,  136, 242, 255, 254, 66,  12,  111, 2,   22,  213, 82,  176, 35,  174,
        244, 221, 30,  156, 173, 1,   12,  199, 31,  66,  249, 42,  74,  1,   90,  42,  60,  253,
        194, 245, 179, 100, 37,  47,  31,  115, 29,  195, 49,  166, 191, 229, 19,  49,  9,   55,
        132, 210, 245, 46,  176, 113, 107, 175, 36,  60,  40,  162, 95,  217, 112, 92,  55,  129,
        183, 23,  245, 87,  249, 225, 54,  14,  85,  111, 192, 107, 177, 4,   244, 51,  56,  86,
        152, 124, 123, 60,  98,  178, 81,  119, 86,  23,  87,  79,  68,  54,  223, 47,  197, 36,
        248, 162, 240, 1,   30,  173, 168, 44,  148, 144, 210, 247, 237, 203, 128, 252, 192, 5,
        104, 216, 144, 65,  6,   62,  82,  238, 28,  223, 4,   13,  180, 82,  156, 150, 216, 122,
        121, 2,   144, 131, 135, 58,  29,  58,  237, 254, 58,  124, 71,  157, 188, 202, 6,   8,
        86,  16,  205, 158, 227, 21,  149, 220, 86,  27,  29,  159, 156, 38,  104, 84,  216, 165,
        49,  237, 247, 62,  88,  242, 43,  19,  18,  206, 161, 190, 182, 73,  255, 38,  74,  170,
        62,  213, 129, 224, 196, 233, 229, 242, 107, 7,   17,  26,  139, 173, 141, 36,  92,  36,
        17,  2,   169, 74,  148, 70,  31,  41,  8,   150, 71,  83,  224, 44,  97,  214, 191, 251,
        171, 72,  18,  239, 241, 206, 62,  253, 67,  189, 108, 68,  50,  239, 64,  183, 120, 51,
        55,  19,  200, 139, 74,  181, 36,  41,  182, 83,  211, 52,  200, 78,  186, 36,  247, 185,
        232, 77,  222, 10,  159, 13,  241, 62,  207, 248, 207, 172, 233, 101, 214, 232, 196, 181,
        42,  97,  141, 157, 141, 235, 147, 114, 24,  228, 238, 18,  155, 181, 109, 229, 130, 115,
        12,  155, 19,  139, 16,  167, 238, 131, 49,  195, 67,  63,  237, 206, 61,  141, 248, 187,
        52,  63,  109, 240, 176, 108, 163, 234, 56,  194, 129, 162, 224, 219, 43,  101, 30,  107,
        144, 166, 144, 232, 5,   51,  194, 59,  249, 177, 124, 63,  77,  20,  17,  70,  151, 207,
        151, 12,  97,  107, 189, 80,  95,  143, 84,  61,  17,  115, 50,  131, 172, 147, 235, 165,
        161, 68,  140, 107, 22,  242, 194, 248, 196, 168, 159, 34,  1,   251, 84,  55,  210, 214,
        77,  69,  144, 209, 90,  86,  21,  26,  194, 191, 88,  28,  252, 202, 167, 142, 78,  78,
        105, 103, 79,  173, 130, 209, 234, 240, 219, 199, 118, 156, 99,  95,  190, 169, 237, 94,
        21,  171, 19,  87,  23,  140, 248, 41,  1,   223, 150, 229, 167, 176, 69,  69,  72,  77,
        224, 193, 21,  229, 64,  54,  12,  36,  102, 12,  27,  48,  243, 110, 204, 21,  56,  100,
        203, 88,  184, 143, 253, 37,  190, 4,   173, 214, 39,  140, 52,  221, 180, 25,  212, 185,
        12,  92,  45,  251, 195, 60,  35,  133, 16,  81,  103, 231, 189, 23,  78,  114, 33,  15,
        55,  245, 230, 17,  105, 118, 184, 32,  128, 30,  21,  51,  242, 110, 74,  237, 114, 176,
        154, 65,  199, 114, 226, 251, 246, 151, 66,  2,   101, 39,  226, 65,  187, 23,  36,  120,
        86,  130, 101, 240, 252, 199, 80,  171, 205, 87,  199, 38,  119, 96,  76,  162, 175, 210,
        125, 124, 42,  7,   142, 181, 166, 63,  4,   29,  215, 66,  121, 242, 230, 195, 89,  83,
        93,  78,  46,  252, 213, 121, 170, 152, 94,  242, 217, 123, 182, 80,  54,  104, 218, 76,
        242, 22,  252, 193, 11,  79,  160, 154, 76,  67,  253, 69,  221, 74,  185, 60,  167, 216,
        16,  59,  41,  112, 75,  100, 152, 21,  106, 253, 138, 150, 128, 25,  165, 34,  41,  11,
        184, 175, 107, 70,  212, 1,   116, 143, 104, 82,  14,  29,  167, 102, 153, 96,  179, 232,
        233, 39,  147, 53,  141, 176, 34,  180, 186, 166, 180, 15,  63,  52,  97,  85,  84,  21,
        242, 26,  151, 173, 244, 56,  187, 240, 142, 93,  28,  93,  48,  126, 227, 142, 45,  247,
        151, 59,  2,   179, 201, 55,  122, 86,  38,  201, 43,  169, 186, 50,  47,  31,  167, 14,
        82,  223, 4,   57,  127, 135, 92,  239, 39,  83,  108, 71,  213, 230, 49,  108, 102, 183,
        21,  42,  172, 181, 26,  207, 140, 0,   193, 234, 201, 59,  89,  44,  144, 199, 146, 143,
        170, 33,  174, 16,  240, 254, 184, 232, 215, 237, 187, 222, 235, 227, 166, 149, 113, 244,
        42,  27,  216, 215, 206, 126, 7,   201, 86,  6,   192, 24,  241, 206, 50,  30,  123, 51,
        58,  161, 128, 143, 173, 41,  199, 160, 238, 82,  173, 125, 25,  202, 186, 248, 210, 198,
        15,  10,  60,  172, 185, 108, 155, 25,  17,  203, 82,  88,  194, 209, 153, 169, 130, 138,
        74,  34,  155, 163, 75,  158, 249, 144, 87,  6,   2,   32,  133, 173, 241, 117, 58,  104,
        148, 26,  219, 43,  198, 153, 214, 97,  147, 75,  222, 58,  255, 22,  74,  243, 179, 254,
        237, 145, 131, 122, 40,  207, 140, 252, 193, 100, 168, 126, 30,  155, 107, 160, 69,  1,
        72,  107, 36,  175, 149, 21,  90,  115, 60,  45,  4,   228, 62,  2,   210, 8,   227, 193,
        191, 139, 186, 67,  36,  115, 170, 199, 162, 212, 183, 30,  193, 26,  180, 242, 87,  141,
        129, 55,  113, 44,  1,   93,  224, 154, 156, 25,  104, 127, 215, 59,  33,  222, 61,  219,
        117, 228, 52,  0,   117, 36,  100, 208, 241, 160, 136, 94,  50,  124, 164, 26,  9,   245,
        29,  186, 119, 144, 230, 125, 129, 153, 195, 10,  72,  223, 159, 15,  190, 102, 196, 23,
        58,  124, 83,  101, 47,  77,  43,  225, 224, 59,  97,  168, 179, 35,  224, 216, 87,  167,
        102, 184, 48,  181, 44,  47,  253, 171, 187, 26,  112, 61,  157, 130, 52,  5,   217, 44,
        219, 104, 99,  201, 219, 28,  173, 184, 175, 222, 159, 218, 120, 213, 26,  245, 71,  167,
        60,  168, 191, 50,  63,  250, 115, 112, 70,  142, 190, 213, 62,  16,  97,  224, 220, 42,
        96,  230, 145, 208, 24,  225, 51,  138, 184, 156, 54,  37,  95,  233, 66,  36,  96,  29,
        49,  150, 3,   218, 11,  198, 10,  7,   63,  93,  109, 6,   168, 22,  70,  246, 172, 48,
        224, 149, 122, 136, 236, 167, 159, 241, 139, 212, 46,  65,  50,  163, 103, 55,  150, 91,
        122, 103, 220, 7,   141, 96,  33,  235, 186, 99,  53,  19,  175, 51,  105, 218, 213, 136,
        93,  66,  112, 125, 53,  255, 25,  93,  245, 10,  13,  78,  84,  143, 232, 230, 41,  45,
        76,  208, 80,  26,  106, 7,   123, 200, 222, 252, 226, 91,  169, 157, 189, 135, 121, 248,
        173, 119, 187, 246, 214, 165, 35,  136, 205, 192, 234, 131, 219, 237, 149, 6,   18,  43,
        244, 253, 0,   9,   150, 118, 185, 11,  222, 148, 235, 26,  187, 1,   124, 96,  243, 157,
        48,  23,  253, 55,  235, 212, 220, 237, 23,  235, 244, 11,  43,  202, 248, 194, 42,  69,
        193, 160, 183, 152, 99,  100, 21,  234, 237, 217, 125, 102, 67,  116, 227, 206, 193, 10,
        89,  14,  93,  248, 208, 226, 234, 113, 37,  208, 251, 89,  152, 21,  99,  47,  181, 51,
        167, 166, 11,  7,   250, 101, 80,  164, 82,  61,  115, 182, 236, 14,  222, 72,  209, 95,
        130, 238, 108, 253, 18,  131, 229, 230, 17,  27,  17,  121, 233, 39,  34,  39,  216, 226,
        129, 1,   164, 201, 104, 255, 66,  51,  206, 10,  96,  91,  222, 157, 84,  252, 98,  33,
        77,  91,  130, 214, 29,  186, 117, 181, 61,  248, 202, 126, 233, 75,  84,  33,  105, 200,
        245, 51,  149, 202, 9,   35,  139, 205, 239, 205, 21,  69,  41,  73,  185, 129, 1,   151,
        3,   231, 204, 139, 56,  196, 158, 192, 101, 102, 249, 197, 254, 220, 10,  136, 49,  230,
        22,  209, 87,  243, 180, 161, 186, 157, 18,  243, 91,  197, 120, 224, 60,  77,  103, 241,
        92,  176, 31,  54,  118, 118, 44,  190, 204, 129, 222, 44,  210, 114, 191, 41,  233, 205,
        160, 101, 89,  231, 224, 154, 68,  245, 93,  141, 151, 187, 45,  88,  22,  239, 210, 43,
        158, 188, 91,  185, 243, 193, 74,  184, 186, 83,  166, 115, 125, 198, 106, 21,  209, 140,
        78,  116, 28,  99,  14,  3,   138, 228, 24,  45,  140, 208, 191, 205, 110, 212, 40,  42,
        253, 105, 78,  149, 26,  241, 189, 13,  107, 117, 141, 128, 142, 241, 135, 245, 225, 209,
        75,  42,  124, 48,  180, 166, 246, 240, 245, 19,  42,  254, 235, 248, 180, 200, 138, 4,
        29,  121, 204, 165, 99,  44,  199, 126, 33,  241, 172, 49,  155, 59,  183, 247, 6,   150,
        163, 227, 78,  54,  4,   191, 38,  124, 46,  6,   151, 13,  197, 172, 200, 197, 239, 190,
        119, 232, 157, 52,  148, 19,  82,  3,   9,   168, 90,  132, 108, 42,  27,  89,  116, 207,
        129, 100, 7,   181, 47,  125, 122, 213, 106, 187, 16,  60,  21,  136, 148, 165, 214, 103,
        182, 230, 1,   7,   165, 197, 33,  182, 202, 98,  102, 145, 33,  208, 133, 139, 22,  28,
        97,  246, 100, 33,  6,   149, 185, 98,  130, 2,   118, 202, 252, 227, 206, 0,   182, 101,
        116, 21,  93,  120, 202, 204, 97,  58,  74,  86,  46,  217, 89,  249, 235, 105, 175, 198,
        204, 21,  243, 97,  77,  120, 71,  137, 222, 238, 100, 141, 97,  111, 121, 65,  64,  132,
        205, 91,  253, 131, 188, 94,  29,  77,  46,  11,  39,  242, 230, 98,  17,  98,  202, 22,
        227, 66,  108, 158, 17,  205, 19,  233, 104, 5,   206, 164, 99,  185, 220, 69,  194, 206,
        31,  144, 247, 141, 127, 163, 36,  94,  19,  1,   207, 70,  213, 231, 130, 225, 42,  204,
        106, 86,  154, 189, 35,  213, 176, 35,  67,  6,   217, 209, 71,  170, 170, 76,  229, 69,
        236, 82,  37,  148, 77,  233, 38,  42,  185, 18,  191, 95,  52,  207, 133, 154, 152, 67,
        94,  98,  15,  49,  191, 224, 176, 102, 187, 35,  42,  76,  19,  143, 105, 108, 32,  196,
        207, 134, 215, 139, 111, 188, 1,   12,  66,  77,  66,  149, 173, 203, 218, 225, 86,  70,
        156, 78,  156, 161, 95,  100, 52,  217, 52,  112, 55,  172, 64,  106, 233, 235, 199, 21,
        106, 168, 3,   126, 67,  108, 66,  243, 230, 116, 203, 227, 149, 33,  1,   126, 161, 55,
        10,  160, 160, 141, 252, 104, 56,  21,  100, 33,  129, 62,  130, 60,  248, 28,  86,  237,
        214, 85,  110, 230, 180, 2,   91,  206, 56,  213, 251, 237, 227, 236, 164, 83,  158, 19,
        165, 14,  254, 167, 157, 69,  14,  67,  182, 111, 144, 37,  220, 215, 5,   232, 113, 10,
        212, 24,  38,  40,  235, 19,  250, 38,  128, 250, 171, 211, 5,   2,   107, 184, 28,  239,
        180, 102, 52,  79,  179, 218, 222, 21,  78,  248, 127, 57,  57,  89,  223, 191, 188, 178,
        103, 42,  82,  3,   246, 209, 177, 42,  188, 99,  16,  60,  152, 124, 161, 138, 106, 40,
        242, 180, 187, 234, 184, 140, 206, 100, 122, 47,  124, 249, 35,  85,  65,  200, 188, 135,
        99,  75,  219, 71,  68,  219, 89,  186, 123, 92,  21,  64,  88,  182, 139, 34,  107, 232,
        248, 236, 57,  67,  43,  97,  82,  87,  83,  214, 238, 15,  210, 75,  18,  242, 100, 125,
        215, 156, 142, 123, 238, 48,  241, 128, 210, 251, 189, 164, 200, 159, 66,  113, 91,  93,
        6,   94,  66,  252, 183, 103, 163, 101, 150, 114, 25,  81,  24,  79,  159, 170, 254, 219,
        87,  36,  205, 100, 162, 124, 106, 26,  64,  86,  158, 168, 149, 92,  12,  102, 139, 43,
        178, 255, 189, 27,  235, 50,  22,  216, 217, 144, 234, 215, 121, 72,  100, 30,  114, 163,
        127, 17,  152, 80,  182, 166, 105, 223, 236, 181, 255, 205, 211, 141, 24,  132, 217, 106,
        26,  240, 60,  46,  30,  213, 119, 25,  212, 74,  253, 176, 96,  231, 232, 46,  241, 34,
        195, 19,  208, 245, 249, 27,  231, 83,  69,  160, 177, 196, 199, 26,  79,  226, 68,  144,
        111, 43,  47,  146, 105, 166, 134, 2,   73,  240, 195, 206, 68,  44,  223, 45,  88,  134,
        241, 130, 142, 131, 47,  86,  185, 69,  1,   225, 26,  79,  240, 35,  234, 46,  229, 156,
        148, 132, 242, 146, 148, 133, 239, 127, 2,   7,   103, 226, 89,  92,  249, 38,  4,   43,
        226, 209, 99,  218, 83,  192, 239, 81,  51,  2,   193, 100, 1,   205, 55,  177, 126, 25,
        219, 56,  49,  154, 193, 29,  150, 130, 126, 58,  70,  18,  27,  223, 177, 61,  81,  15,
        218, 164, 166, 52,  59,  23,  186, 234, 33,  129, 54,  231, 44,  225, 109, 248, 40,  56,
        189, 11,  219, 19,  70,  241, 187, 21,  235, 143, 88,  58,  30,  101, 133, 14,  251, 87,
        216, 221, 173, 117, 22,  44,  252, 44,  72,  250, 205, 113, 52,  100, 82,  141, 39,  133,
        134, 197, 9,   4,   80,  45,  36,  12,  212, 90,  197, 219, 15,  172, 12,  106, 10,  160,
        204, 233, 245, 64,  137, 79,  49,  68,  108, 216, 131, 0,   30,  119, 229, 38,  125, 68,
        103, 91,  165, 172, 118, 72,  21,  45,  229, 184, 192, 44,  80,  136, 7,   79,  18,  22,
        172, 45,  4,   3,   37,  205, 118, 117, 62,  95,  95,  182, 4,   128, 21,  55,  208, 42,
        120, 9,   123, 233, 230, 54,  225, 88,  151, 191, 252, 163, 72,  96,  136, 20,  100, 8,
        158, 148, 246, 78,  117, 240, 195, 156, 29,  64,  128, 42,  193, 79,  65,  119, 64,  163,
        94,  55,  103, 216, 7,   136, 48,  227, 17,  49,  255, 184, 164, 23,  200, 45,  240, 114,
        173, 221, 53,  49,  165, 212, 147, 209, 151, 82,  176, 179, 113, 123, 252, 220, 12,  78,
        95,  99,  43,  231, 233, 163, 79,  247, 203, 207, 77,  139, 246, 10,  72,  61,  172, 239,
        200, 186, 126, 238, 25,  130, 148, 67,  234, 41,  86,  164, 176, 182, 111, 241, 226, 42,
        210, 154, 164, 21,  20,  238, 71,  7,   191, 212, 155, 14,  117, 191, 142, 12,  52,  110,
        16,  47,  51,  249, 250, 126, 99,  60,  169, 219, 158, 30,  240, 97,  208, 138, 237, 30,
        108, 96,  71,  95,  165, 2,   222, 164, 174, 137, 175, 53,  236, 124, 144, 206, 196, 174,
        242, 138, 255, 238, 139, 237, 101, 162, 236, 246, 110, 88,  84,  78,  248, 39,  159, 159,
        37,  172, 22,  5,   204, 31,  190, 71,  161, 170, 162, 63,  245, 208, 19,  23,  106, 153,
        39,  77,  209, 205, 145, 47,  56,  68,  226, 123, 140, 11,  24,  152, 99,  206, 2,   151,
        22,  114, 212, 204, 146, 31,  250, 91,  101, 185, 192, 66,  201, 52,  182, 190, 5,   170,
        114, 79,  63,  231, 37,  59,  52,  26,  120, 207, 163, 220, 5,   227, 39,  22,  65,  229,
        194, 224, 173, 127, 43,  217, 232, 230, 164, 238, 134, 1,   182, 102, 107, 147, 152, 63,
        236, 228, 175, 253, 135, 110, 209, 224, 140, 121, 112, 29,  239, 209, 121, 36,  38,  128,
        77,  179, 0,   217, 102, 209, 162, 12,  173, 24,  32,  73,  221, 204, 94,  21,  239, 28,
        128, 70,  250, 43,  133, 138, 179, 59,  130, 202, 193, 182, 175, 158, 74,  167, 14,  70,
        225, 252, 45,  255, 73,  200, 36,  151, 170, 188, 24,  64,  154, 142, 7,   234, 209, 150,
        200, 37,  137, 61,  97,  35,  234, 109, 103, 95,  145, 139, 150, 37,  199, 110, 135, 41,
        250, 207, 168, 195, 33,  39,  5,   220, 140, 219, 46,  203, 43,  82,  161, 151, 221, 206,
        40,  85,  6,   175, 111, 209, 7,   24,  110, 51,  50,  91,  105, 114, 199, 138, 191, 232,
        103, 91,  24,  81,  83,  145, 16,  116, 131, 71,  199, 214, 111, 145, 64,  108, 109, 2,
        162, 193, 134, 32,  196, 246, 29,  21,  140, 193, 201, 82,  160, 14,  36,  60,  54,  66,
        177, 164, 1,   59,  119, 9,   121, 131, 83,  131, 140, 196, 59,  184, 46,  133, 88,  159,
        228, 27,  232, 134, 141, 7,   174, 207, 119, 149, 7,   172, 215, 5,   30,  164, 123, 70,
        72,  84,  146, 137, 111, 181, 116, 186, 205, 156, 220, 133, 192, 143, 131, 205, 216, 58,
        31,  241, 55,  149, 164, 177, 64,  10,  65,  32,  68,  186, 42,  80,  94,  30,  194, 17,
        0,   82,  189, 39,  72,  189, 218, 125, 113, 152, 152, 201, 148, 243, 188, 81,  186, 17,
        121, 126, 5,   191, 91,  90,  129, 128, 201, 89,  194, 56,  66,  16,  21,  243, 232, 23,
        18,  60,  86,  93,  21,  134, 21,  165, 169, 212, 109, 216, 160, 204, 220, 212, 13,  216,
        132, 160, 104, 161, 180, 113, 163, 127, 178, 105, 19,  73,  31,  29,  87,  53,  70,  40,
        199, 149, 1,   17,  7,   27,  82,  175, 232, 244, 102, 164, 24,  181, 226, 44,  64,  75,
        96,  184, 210, 154, 249, 88,  47,  17,  210, 19,  133, 179, 31,  183, 153, 143, 195, 250,
        131, 101, 255, 60,  162, 63,  204, 6,   101, 196, 19,  161, 131, 114, 25,  19,  69,  78,
        77,  123, 247, 91,  154, 240, 208, 179, 99,  41,  15,  177, 164, 16,  83,  141, 237, 92,
        117, 22,  186, 166, 57,  79,  72,  155, 97,  228, 188, 91,  151, 90,  196, 169, 202, 35,
        8,   105, 175, 58,  57,  5,   162, 103, 214, 138, 149, 14,  118, 10,  27,  22,  121, 92,
        95,  86,  25,  3,   59,  41,  78,  104, 134, 237, 19,  146, 91,  51,  94,  229, 93,  120,
        133, 115, 63,  219, 132, 24,  200, 0,   40,  168, 223, 220, 240, 19,  163, 126, 174, 194,
        40,  55,  152, 248, 116, 162, 90,  210, 50,  92,  116, 46,  89,  253, 51,  196, 197, 72,
        192, 119, 80,  159, 114, 195, 37,  41,  231, 40,  86,  13,  5,   225, 115, 244, 65,  115,
        232, 18,  27,  54,  43,  253, 57,  184, 230, 254, 160, 106, 139, 44,  71,  142, 246, 97,
        232, 114, 227, 71,  148, 103, 205, 244, 105, 250, 26,  138, 0,   95,  10,  247, 199, 148,
        244, 216, 146, 62,  140, 171, 87,  238, 215, 60,  123, 208, 102, 165, 156, 51,  32,  211,
        230, 200, 226, 240, 120, 228, 81,  134, 209, 175, 42,  214, 131, 188, 147, 60,  136, 167,
        149, 143, 150, 224, 103, 52,  11,  99,  176, 227, 93,  120, 124, 192, 16,  211, 113, 179,
        50,  227, 253, 206, 102, 83,  57,  71,  246, 197, 223, 117, 8,   195, 145, 9,   76,  195,
        236, 108, 27,  171, 200, 124, 206, 243, 168, 191, 55,  64,  220, 32,  252, 52,  58,  100,
        46,  227, 78,  193, 88,  40,  2,   154, 174, 141, 210, 206, 124, 242, 139, 73,  247, 239,
        6,   15,  158, 4,   233, 232, 98,  46,  136, 156, 152, 227, 214, 242, 159, 5,   34,  252,
        28,  231, 253, 155, 76,  17,  241, 127, 70,  13,  90,  113, 149, 72,  167, 95,  101, 145,
        153, 187, 255, 164, 94,  101, 113, 184, 211, 54,  186, 211, 148, 224, 206, 60,  36,  179,
        49,  219, 71,  77,  240, 15,  233, 217, 120, 78,  74,  211, 230, 15,  166, 108, 152, 173,
        75,  2,   188, 234, 233, 45,  218, 138, 39,  9,   20,  57,  158, 17,  103, 69,  117, 171,
        187, 76,  182, 161, 184, 246, 115, 118, 181, 105, 18,  133, 149, 150, 192, 190, 81,  158,
        1,   26,  86,  238, 127, 210, 234, 225, 230, 234, 92,  125, 220, 23,  103, 181, 183, 186,
        56,  129, 11,  150, 132, 238, 113, 208, 36,  56,  50,  55,  74,  146, 187, 83,  134, 255,
        232, 251, 138, 138, 77,  202, 193, 71,  38,  237, 148, 115, 213, 100, 93,  165, 222, 231,
        161, 138, 110, 155, 159, 60,  155, 245, 170, 110, 233, 236, 37,  30,  207, 215, 137, 60,
        117, 57,  126, 254, 187, 71,  136, 162, 107, 120, 195, 2,   27,  67,  92,  6,   70,  66,
        10,  127, 247, 29,  190, 248, 18,  35,  53,  54,  70,  12,  36,  66,  215, 182, 14,  43,
        77,  250, 233, 24,  203, 128, 199, 99,  216, 232, 88,  41,  75,  15,  135, 139, 200, 42,
        226, 21,  240, 161, 199, 91,  132, 227, 206, 26,  62,  252, 181, 108, 77,  146, 135, 149,
        185, 166, 229, 142, 140, 92,  25,  59,  98,  105, 214, 46,  147, 185, 228, 236, 242, 149,
        104, 24,  99,  44,  115, 101, 74,  149, 65,  15,  115, 108, 62,  78,  238, 106, 60,  59,
        147, 204, 51,  128, 133, 82,  62,  203, 97,  103, 58,  166, 187, 142, 254, 184, 225, 170,
        243, 149, 239, 197, 90,  212, 57,  217, 132, 212, 150, 48,  188, 59,  174, 10,  0,   2,
        67,  54,  115, 218, 63,  189, 165, 67,  161, 49,  89,  30,  159, 24,  225, 157, 243, 192,
        157, 246, 97,  81,  67,  127, 210, 151, 205, 27,  128, 20,  187, 36,  59,  8,   92,  157,
        190, 44,  31,  200, 47,  188, 210, 237, 101, 181, 125, 10,  115, 46,  241, 90,  107, 186,
        239, 13,  55,  168, 120, 55,  18,  44,  99,  78,  164, 178, 203, 123, 115, 28,  16,  113,
        163, 86,  175, 130, 156, 103, 114, 124, 94,  2,   216, 57,  32,  187, 28,  5,   78,  203,
        86,  208, 79,  128, 206, 38,  200, 212, 137, 255, 120, 4,   51,  18,  112, 54,  36,  172,
        210, 108, 215, 199, 248, 162, 183, 71,  10,  32,  239, 88,  18,  151, 217, 191, 137, 101,
        190, 199, 133, 209, 68,  167, 234, 154, 170, 189, 189, 144, 48,  23,  58,  154, 114, 125,
        89,  95,  76,  136, 249, 79,  43,  86,  117, 104, 176, 46,  242, 29,  241, 137, 203, 45,
        71,  7,   105, 16,  34,  194, 229, 92,  178, 62,  41,  45,  77,  24,  236, 53,  198, 134,
        127, 208, 204, 94,  219, 205, 86,  40,  36,  50,  116, 250, 32,  63,  10,  207, 155, 92,
        8,   177, 246, 230, 96,  223, 154, 43,  3,   141, 241, 234, 71,  161, 117, 218, 89,  251,
        156, 193, 21,  249, 154, 89,  185, 233, 31,  42,  0,   84,  168, 154, 56,  143, 50,  50,
        151, 56,  171, 84,  110, 130, 251, 110, 194, 115, 116, 254, 100, 246, 135, 180, 1,   4,
        52,  243, 47,  116, 244, 48,  47,  51,  191, 117, 161, 138, 144, 43,  24,  214, 30,  161,
        142, 13,  114, 149, 102, 51,  93,  217, 238, 179, 90,  104, 191, 37,  183, 13,  101, 90,
        226, 81,  120, 116, 144, 165, 189, 143, 59,  172, 54,  58,  217, 83,  210, 100, 17,  27,
        39,  249, 210, 216, 153, 186, 75,  154, 25,  200, 75,  0,   236, 252, 189, 53,  163, 91,
        209, 122, 239, 225, 18,  76,  11,  131, 102, 154, 31,  161, 105, 252, 4,   143, 186, 15,
        226, 112, 130, 125, 177, 129, 251, 146, 145, 214, 112, 110, 137, 188, 186, 123, 17,  106,
        39,  4,   162, 16,  116, 189, 65,  77,  93,  28,  32,  81,  193, 26,  203, 100, 156, 5,
        84,  124, 57,  183, 155, 93,  121, 5,   251, 56,  136, 16,  206, 198, 64,  161, 73,  69,
        84,  225, 155, 54,  164, 58,  198, 24,  213, 20,  182, 16,  42,  1,   190, 227, 87,  208,
        23,  203, 104, 90,  54,  72,  253, 63,  144, 74,  89,  6,   111, 180, 124, 86,  67,  172,
        75,  58,  250, 244, 170, 155, 81,  244, 48,  4,   217, 77,  14,  140, 121, 101, 100, 81,
        13,  73,  222, 123, 31,  251, 32,  63,  174, 119, 8,   122, 227, 27,  48,  184, 45,  169,
        55,  228, 213, 4,   166, 34,  187, 100, 251, 74,  12,  146, 165, 209, 17,  41,  116, 125,
        56,  87,  120, 98,  3,   237, 52,  38,  13,  198, 136, 241, 38,  136, 55,  223, 254, 235,
        250, 122, 243, 25,  29,  25,  249, 30,  251, 147, 99,  92,  12,  50,  181, 30,  247, 68,
        69,  212, 227, 178, 211, 21,  89,  250, 76,  70,  169, 109, 118, 73,  141, 144, 237, 81,
        89,  54,  227, 237, 70,  65,  249, 250, 119, 183, 21,  154, 103, 251, 83,  157, 27,  134,
        217, 1,   1,   68,  212, 203, 81,  165, 160, 46,  49,  118, 117, 69,  246, 46,  93,  101,
        251, 224, 126, 217};
    result["big"] = std::string(reinterpret_cast<const char*>(buffer), 40000);
    return result;
  }

  class MockInputStream : public InputStream {
   public:
    ~MockInputStream() override;
    MOCK_CONST_METHOD0(getLength, uint64_t());
    MOCK_CONST_METHOD0(getName, const std::string&());
    MOCK_METHOD3(read, void(void*, uint64_t, uint64_t));
    MOCK_CONST_METHOD0(getNaturalReadSize, uint64_t());
  };

  MockInputStream::~MockInputStream() {
    // PASS
  }

  TEST(TestMatch, serializedConstructor) {
    orc::ReaderOptions opts;
    std::string filename = findExample("demo-12-zlib.orc");

    // open a file
    std::unique_ptr<orc::Reader> reader =
        orc::createReader(orc::readLocalFile(filename, opts.getReaderMetrics()), opts);

    // for the next reader copy the serialized tail
    std::string tail = reader->getSerializedFileTail();
    opts.setSerializedFileTail(tail);

    // We insist on no calls to the input stream when looking at the file
    // information.
    MockInputStream* fakeStream = new MockInputStream();
    EXPECT_CALL(*fakeStream, getLength()).Times(0);
    EXPECT_CALL(*fakeStream, getName()).Times(0);
    EXPECT_CALL(*fakeStream, read(testing::_, testing::_, testing::_)).Times(0);

    std::unique_ptr<orc::Reader> reader2 =
        orc::createReader(std::unique_ptr<InputStream>(fakeStream), opts);

    EXPECT_EQ(1920800, reader2->getNumberOfRows());
    EXPECT_EQ(CompressionKind_ZLIB, reader2->getCompression());
  }

}  // namespace orc
