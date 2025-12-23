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

#include "orc/Exceptions.hh"
#include "wrap/gtest-wrapper.h"

namespace orc {

  TEST(TestColumnPrinter, BooleanColumnPrinter) {
    std::string line;
    std::unique_ptr<Type> type = createPrimitiveType(BOOLEAN);
    std::unique_ptr<ColumnPrinter> printer = createColumnPrinter(line, type.get());
    LongVectorBatch batch(1024, *getDefaultPool());
    const char* expected[] = {"true", "false", "true"};
    batch.numElements = 3;
    batch.hasNulls = false;
    batch.data[0] = 1;
    batch.data[1] = 0;
    batch.data[2] = 1;
    printer->reset(batch);
    for (uint64_t i = 0; i < 3; ++i) {
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected[i], line);
    }
    const char* expected2[] = {"null", "null", "true", "false"};
    batch.numElements = 4;
    batch.data[3] = false;
    batch.hasNulls = true;
    batch.notNull[0] = false;
    batch.notNull[1] = false;
    batch.notNull[2] = true;
    batch.notNull[3] = true;
    printer->reset(batch);
    for (uint64_t i = 0; i < 4; ++i) {
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected2[i], line);
    }
  }

  TEST(TestColumnPrinter, LongColumnPrinter) {
    std::string line;
    std::unique_ptr<Type> type = createPrimitiveType(LONG);
    std::unique_ptr<ColumnPrinter> printer = createColumnPrinter(line, type.get());
    LongVectorBatch batch(1024, *getDefaultPool());
    batch.numElements = 2;
    batch.hasNulls = false;
    batch.data[0] = 9223372036854775807LL;
    batch.data[1] = -9223372036854775807LL;
    printer->reset(batch);
    const char* expected[] = {"9223372036854775807", "-9223372036854775807"};
    for (uint64_t i = 0; i < 2; ++i) {
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected[i], line);
    }
    batch.numElements = 3;
    batch.hasNulls = true;
    batch.data[0] = 127;
    batch.data[1] = -127;
    batch.data[2] = 123;
    batch.notNull[0] = true;
    batch.notNull[1] = true;
    batch.notNull[2] = false;
    printer->reset(batch);
    const char* expected2[] = {"127", "-127", "null"};
    for (uint64_t i = 0; i < 3; ++i) {
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected2[i], line);
    }
  }

  TEST(TestColumnPrinter, DoubleColumnPrinter) {
    std::string line;
    std::unique_ptr<Type> type = createPrimitiveType(DOUBLE);
    std::unique_ptr<ColumnPrinter> printer = createColumnPrinter(line, type.get());
    DoubleVectorBatch batch(1024, *getDefaultPool());
    batch.numElements = 2;
    batch.hasNulls = false;
    batch.data[0] = 1234.5;
    batch.data[1] = -1234.5;
    printer->reset(batch);
    const char* expected[] = {"1234.5", "-1234.5"};
    for (uint64_t i = 0; i < 2; ++i) {
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected[i], line);
    }
    batch.numElements = 3;
    batch.hasNulls = true;
    batch.data[0] = 9999.125;
    batch.data[1] = -9999.125;
    batch.data[2] = 100000;
    batch.notNull[0] = true;
    batch.notNull[1] = true;
    batch.notNull[2] = false;
    printer->reset(batch);
    const char* expected2[] = {"9999.125", "-9999.125", "null"};
    for (uint64_t i = 0; i < 3; ++i) {
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected2[i], line);
    }
  }

  TEST(TestColumnPrinter, TimestampColumnPrinter) {
    std::string line;
    std::unique_ptr<Type> type = createPrimitiveType(TIMESTAMP);
    std::unique_ptr<ColumnPrinter> printer = createColumnPrinter(line, type.get());
    TimestampVectorBatch batch(1024, *getDefaultPool());
    batch.numElements = 12;
    batch.hasNulls = false;
    batch.data[0] = 1420070400;
    batch.data[1] = 963273600;
    batch.data[2] = 1426172459;
    batch.data[3] = 1426172459;
    batch.data[4] = 1426172459;
    batch.data[5] = 1426172459;
    batch.data[6] = 1426172459;
    batch.data[7] = 1426172459;
    batch.data[8] = 1426172459;
    batch.data[9] = 1426172459;
    batch.data[10] = 1426172459;
    batch.data[11] = 1426172459;
    batch.nanoseconds[0] = 0;
    batch.nanoseconds[1] = 0;
    batch.nanoseconds[2] = 0;
    batch.nanoseconds[3] = 1;
    batch.nanoseconds[4] = 10;
    batch.nanoseconds[5] = 100;
    batch.nanoseconds[6] = 1000;
    batch.nanoseconds[7] = 10000;
    batch.nanoseconds[8] = 100000;
    batch.nanoseconds[9] = 1000000;
    batch.nanoseconds[10] = 10000000;
    batch.nanoseconds[11] = 100000000;
    const char* expected[] = {
        "\"2015-01-01 00:00:00.0\"",        "\"2000-07-11 00:00:00.0\"",
        "\"2015-03-12 15:00:59.0\"",        "\"2015-03-12 15:00:59.000000001\"",
        "\"2015-03-12 15:00:59.00000001\"", "\"2015-03-12 15:00:59.0000001\"",
        "\"2015-03-12 15:00:59.000001\"",   "\"2015-03-12 15:00:59.00001\"",
        "\"2015-03-12 15:00:59.0001\"",     "\"2015-03-12 15:00:59.001\"",
        "\"2015-03-12 15:00:59.01\"",       "\"2015-03-12 15:00:59.1\""};
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected[i], line) << "for i = " << i;
    }
    batch.hasNulls = true;
    for (size_t i = 0; i < batch.numElements; ++i) {
      batch.notNull[i] = i % 2;
    }
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      if (i % 2) {
        EXPECT_EQ(expected[i], line) << "for i = " << i;
      } else {
        EXPECT_EQ("null", line) << "for i = " << i;
      }
    }
  }

  TEST(TestColumnPrinter, DateColumnPrinter) {
    std::string line;
    std::unique_ptr<Type> type = createPrimitiveType(DATE);
    std::unique_ptr<ColumnPrinter> printer = createColumnPrinter(line, type.get());
    LongVectorBatch batch(1024, *getDefaultPool());
    batch.numElements = 10;
    batch.hasNulls = false;
    batch.data[0] = 0;
    batch.data[1] = 11738;
    batch.data[2] = -165;
    batch.data[3] = -33165;
    batch.data[4] = 10489;
    batch.data[5] = -5171;
    batch.data[6] = 11016;
    batch.data[7] = 5763;
    batch.data[8] = 16729;
    batch.data[9] = 12275;
    const char* expected[] = {
        "\"1970-01-01\"", "\"2002-02-20\"", "\"1969-07-20\"", "\"1879-03-14\"", "\"1998-09-20\"",
        "\"1955-11-05\"", "\"2000-02-29\"", "\"1985-10-12\"", "\"2015-10-21\"", "\"2003-08-11\""};
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
#ifndef HAS_PRE_1970
      if (batch.data[i] < 0) continue;
#endif
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected[i], line) << "for i = " << i;
    }
    batch.hasNulls = true;
    for (size_t i = 0; i < batch.numElements; ++i) {
      batch.notNull[i] = i % 2;
    }
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
#ifndef HAS_PRE_1970
      if (batch.data[i] < 0) continue;
#endif
      line.clear();
      printer->printRow(i);
      if (i % 2) {
        EXPECT_EQ(expected[i], line) << "for i = " << i;
      } else {
        EXPECT_EQ("null", line) << "for i = " << i;
      }
    }
  }

  TEST(TestColumnPrinter, Decimal64ColumnPrinter) {
    std::string line;
    std::unique_ptr<Type> type = createDecimalType(16, 5);
    std::unique_ptr<ColumnPrinter> printer = createColumnPrinter(line, type.get());
    Decimal64VectorBatch batch(1024, *getDefaultPool());
    batch.numElements = 10;
    batch.hasNulls = false;
    batch.scale = 5;
    batch.values[0] = 0;
    batch.values[1] = 1;
    batch.values[2] = -10;
    batch.values[3] = 100;
    batch.values[4] = 1000;
    batch.values[5] = 10000;
    batch.values[6] = 100000;
    batch.values[7] = 1000000;
    batch.values[8] = -10000000;
    batch.values[9] = 100000000;
    const char* expected[] = {"0.00000", "0.00001", "-0.00010", "0.00100",    "0.01000",
                              "0.10000", "1.00000", "10.00000", "-100.00000", "1000.00000"};
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected[i], line) << "for i = " << i;
    }
    batch.hasNulls = true;
    for (size_t i = 0; i < batch.numElements; ++i) {
      batch.notNull[i] = i % 2;
    }
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      if (i % 2) {
        EXPECT_EQ(expected[i], line) << "for i = " << i;
      } else {
        EXPECT_EQ("null", line) << "for i = " << i;
      }
    }
  }

  TEST(TestColumnPrinter, Decimal128ColumnPrinter) {
    std::string line;
    std::unique_ptr<Type> type = createDecimalType(30, 5);
    std::unique_ptr<ColumnPrinter> printer = createColumnPrinter(line, type.get());
    Decimal128VectorBatch batch(1024, *getDefaultPool());
    batch.numElements = 10;
    batch.hasNulls = false;
    batch.scale = 5;
    batch.values[0] = 0;
    batch.values[1] = 1;
    batch.values[2] = -10;
    batch.values[3] = 100;
    batch.values[4] = 1000;
    batch.values[5] = 10000;
    batch.values[6] = 100000;
    batch.values[7] = 1000000;
    batch.values[8] = -10000000;
    batch.values[9] = 100000000;
    const char* expected[] = {"0.00000", "0.00001", "-0.00010", "0.00100",    "0.01000",
                              "0.10000", "1.00000", "10.00000", "-100.00000", "1000.00000"};
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected[i], line) << "for i = " << i;
    }
    batch.hasNulls = true;
    for (size_t i = 0; i < batch.numElements; ++i) {
      batch.notNull[i] = i % 2;
    }
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      if (i % 2) {
        EXPECT_EQ(expected[i], line) << "for i = " << i;
      } else {
        EXPECT_EQ("null", line) << "for i = " << i;
      }
    }
  }

  TEST(TestColumnPrinter, StringColumnPrinter) {
    std::string line;
    std::unique_ptr<Type> type = createPrimitiveType(STRING);
    std::unique_ptr<ColumnPrinter> printer = createColumnPrinter(line, type.get());
    StringVectorBatch batch(1024, *getDefaultPool());
    const char* blob = "thisisatest\b\f\n\r\t\\\"'";
    batch.numElements = 5;
    batch.hasNulls = false;
    batch.data[0] = const_cast<char*>(blob);
    batch.length[0] = 4;
    batch.length[1] = 2;
    batch.length[2] = 1;
    batch.length[3] = 4;
    batch.length[4] = 8;
    for (size_t i = 1; i < 5; ++i) {
      batch.data[i] = batch.data.data()[i - 1] + static_cast<size_t>(batch.length[i - 1]);
    }
    const char* expected[] = {"\"this\"", "\"is\"", "\"a\"", "\"test\"",
                              "\"\\b\\f\\n\\r\\t\\\\\\\"'\""};
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected[i], line) << "for i = " << i;
    }
    batch.hasNulls = true;
    for (size_t i = 0; i < batch.numElements; ++i) {
      batch.notNull[i] = i % 2;
    }
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      if (i % 2) {
        EXPECT_EQ(expected[i], line) << "for i = " << i;
      } else {
        EXPECT_EQ("null", line) << "for i = " << i;
      }
    }
  }

  TEST(TestColumnPrinter, BinaryColumnPrinter) {
    std::string line;
    std::unique_ptr<Type> type = createPrimitiveType(BINARY);
    std::unique_ptr<ColumnPrinter> printer = createColumnPrinter(line, type.get());
    StringVectorBatch batch(1024, *getDefaultPool());
    char blob[45];
    for (size_t i = 0; i < sizeof(blob); ++i) {
      blob[i] = static_cast<char>(i);
    }
    batch.numElements = 10;
    batch.hasNulls = false;
    batch.data[0] = blob;
    batch.length[0] = 0;
    for (size_t i = 1; i < batch.numElements; ++i) {
      batch.length[i] = static_cast<int64_t>(i);
      batch.data[i] = batch.data.data()[i - 1] + batch.length[i - 1];
    }
    printer->reset(batch);
    const char* expected[] = {"[]",
                              "[0]",
                              "[1, 2]",
                              "[3, 4, 5]",
                              "[6, 7, 8, 9]",
                              "[10, 11, 12, 13, 14]",
                              "[15, 16, 17, 18, 19, 20]",
                              "[21, 22, 23, 24, 25, 26, 27]",
                              "[28, 29, 30, 31, 32, 33, 34, 35]",
                              "[36, 37, 38, 39, 40, 41, 42, 43, 44]"};
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected[i], line) << "for i = " << i;
    }
    batch.hasNulls = true;
    for (size_t i = 0; i < batch.numElements; ++i) {
      batch.notNull[i] = i % 2;
    }
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      if (i % 2) {
        EXPECT_EQ(expected[i], line) << "for i = " << i;
      } else {
        EXPECT_EQ("null", line) << "for i = " << i;
      }
    }
  }

  TEST(TestColumnPrinter, ListColumnPrinter) {
    std::string line;
    std::unique_ptr<Type> type = createListType(createPrimitiveType(LONG));
    std::unique_ptr<ColumnPrinter> printer = createColumnPrinter(line, type.get());
    ListVectorBatch batch(1024, *getDefaultPool());
    LongVectorBatch* longBatch = new LongVectorBatch(1024, *getDefaultPool());
    batch.elements = std::unique_ptr<ColumnVectorBatch>(longBatch);
    batch.numElements = 10;
    batch.hasNulls = false;
    batch.offsets[0] = 0;
    for (size_t i = 1; i <= batch.numElements; ++i) {
      batch.offsets[i] = batch.offsets[i - 1] + static_cast<int64_t>(i - 1);
    }
    longBatch->numElements = 45;
    longBatch->hasNulls = false;
    for (size_t i = 0; i < longBatch->numElements; ++i) {
      longBatch->data[i] = static_cast<int64_t>(i);
    }
    const char* expected[] = {"[]",
                              "[0]",
                              "[1, 2]",
                              "[3, 4, 5]",
                              "[6, 7, 8, 9]",
                              "[10, 11, 12, 13, 14]",
                              "[15, 16, 17, 18, 19, 20]",
                              "[21, 22, 23, 24, 25, 26, 27]",
                              "[28, 29, 30, 31, 32, 33, 34, 35]",
                              "[36, 37, 38, 39, 40, 41, 42, 43, 44]"};
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected[i], line) << "for i = " << i;
    }
    batch.hasNulls = true;
    for (size_t i = 0; i < batch.numElements; ++i) {
      batch.notNull[i] = i % 2;
    }
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      if (i % 2) {
        EXPECT_EQ(expected[i], line) << "for i = " << i;
      } else {
        EXPECT_EQ("null", line) << "for i = " << i;
      }
    }
  }

  TEST(TestColumnPrinter, MapColumnPrinter) {
    std::string line;
    std::unique_ptr<Type> type =
        createMapType(createPrimitiveType(LONG), createPrimitiveType(LONG));
    std::unique_ptr<ColumnPrinter> printer = createColumnPrinter(line, type.get());
    MapVectorBatch batch(1024, *getDefaultPool());
    LongVectorBatch* keyBatch = new LongVectorBatch(1024, *getDefaultPool());
    LongVectorBatch* valueBatch = new LongVectorBatch(1024, *getDefaultPool());
    batch.keys = std::unique_ptr<ColumnVectorBatch>(keyBatch);
    batch.elements = std::unique_ptr<ColumnVectorBatch>(valueBatch);
    batch.numElements = 4;
    batch.hasNulls = false;
    batch.offsets[0] = 0;
    for (size_t i = 1; i <= batch.numElements; ++i) {
      batch.offsets[i] = batch.offsets[i - 1] + static_cast<int64_t>(i - 1);
    }
    keyBatch->numElements = 6;
    keyBatch->hasNulls = false;
    valueBatch->numElements = 6;
    valueBatch->hasNulls = false;
    for (size_t i = 0; i < keyBatch->numElements; ++i) {
      keyBatch->data[i] = static_cast<int64_t>(i);
      valueBatch->data[i] = static_cast<int64_t>(2 * i);
    }
    const char* expected[] = {"[]", "[{\"key\": 0, \"value\": 0}]",
                              ("[{\"key\": 1, \"value\": 2},"
                               " {\"key\": 2, \"value\": 4}]"),
                              ("[{\"key\": 3, \"value\": 6},"
                               " {\"key\": 4, \"value\": 8},"
                               " {\"key\": 5, \"value\": 10}]")};
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected[i], line) << "for i = " << i;
    }
    batch.hasNulls = true;
    for (size_t i = 0; i < batch.numElements; ++i) {
      batch.notNull[i] = i % 2;
    }
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      if (i % 2) {
        EXPECT_EQ(expected[i], line) << "for i = " << i;
      } else {
        EXPECT_EQ("null", line) << "for i = " << i;
      }
    }
  }

  TEST(TestColumnPrinter, StructColumnPrinter) {
    std::string line;
    std::unique_ptr<Type> type = createStructType();
    type->addStructField("first", createPrimitiveType(LONG));
    type->addStructField("second", createPrimitiveType(LONG));
    std::unique_ptr<ColumnPrinter> printer = createColumnPrinter(line, type.get());
    StructVectorBatch batch(1024, *getDefaultPool());
    LongVectorBatch* firstBatch = new LongVectorBatch(1024, *getDefaultPool());
    LongVectorBatch* secondBatch = new LongVectorBatch(1024, *getDefaultPool());
    batch.fields.push_back(firstBatch);
    batch.fields.push_back(secondBatch);
    batch.numElements = 10;
    batch.hasNulls = false;
    firstBatch->numElements = 10;
    firstBatch->hasNulls = false;
    secondBatch->numElements = 10;
    secondBatch->hasNulls = false;
    for (size_t i = 0; i < firstBatch->numElements; ++i) {
      firstBatch->data[i] = static_cast<int64_t>(i);
      secondBatch->data[i] = static_cast<int64_t>(2 * i);
    }
    const char* expected[] = {"{\"first\": 0, \"second\": 0}",  "{\"first\": 1, \"second\": 2}",
                              "{\"first\": 2, \"second\": 4}",  "{\"first\": 3, \"second\": 6}",
                              "{\"first\": 4, \"second\": 8}",  "{\"first\": 5, \"second\": 10}",
                              "{\"first\": 6, \"second\": 12}", "{\"first\": 7, \"second\": 14}",
                              "{\"first\": 8, \"second\": 16}", "{\"first\": 9, \"second\": 18}"};
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      EXPECT_EQ(expected[i], line) << "for i = " << i;
    }
    batch.hasNulls = true;
    for (size_t i = 0; i < batch.numElements; ++i) {
      batch.notNull[i] = i % 2;
    }
    printer->reset(batch);
    for (uint64_t i = 0; i < batch.numElements; ++i) {
      line.clear();
      printer->printRow(i);
      if (i % 2) {
        EXPECT_EQ(expected[i], line) << "for i = " << i;
      } else {
        EXPECT_EQ("null", line) << "for i = " << i;
      }
    }
  }
}  // namespace orc
