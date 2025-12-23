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

#include "wrap/gtest-wrapper.h"
#include "wrap/orc-proto-wrapper.hh"

#include "MemoryOutputStream.hh"

namespace orc {
  TEST(BufferedOutputStream, block_aligned) {
    MemoryOutputStream memStream(1024);
    MemoryPool* pool = getDefaultPool();

    uint64_t capacity = 1000;
    uint64_t block = 10;
    WriterMetrics metrics;
    BufferedOutputStream bufStream(*pool, &memStream, capacity, block, &metrics);
    for (int i = 0; i < 100; ++i) {
      char* buf;
      int len;
      EXPECT_TRUE(bufStream.Next(reinterpret_cast<void**>(&buf), &len));
      EXPECT_EQ(10, len);
      for (int j = 0; j < 10; ++j) {
        buf[j] = static_cast<char>('a' + j);
      }
    }

    bufStream.flush();
    EXPECT_EQ(1000, memStream.getLength());
    for (int i = 0; i < 1000; ++i) {
      EXPECT_EQ(memStream.getData()[i], 'a' + i % 10);
    }
#if ENABLE_METRICS
    EXPECT_EQ(metrics.IOCount.load(), 1);
#endif
  }

  TEST(BufferedOutputStream, block_not_aligned) {
    MemoryOutputStream memStream(1024);
    MemoryPool* pool = getDefaultPool();

    uint64_t capacity = 20;
    uint64_t block = 10;
    WriterMetrics metrics;
    BufferedOutputStream bufStream(*pool, &memStream, capacity, block, &metrics);

    char* buf;
    int len;
    EXPECT_TRUE(bufStream.Next(reinterpret_cast<void**>(&buf), &len));
    EXPECT_EQ(10, len);

    for (int i = 0; i < 7; ++i) {
      buf[i] = static_cast<char>('a' + i);
    }

    bufStream.BackUp(3);
    bufStream.flush();

    EXPECT_EQ(7, memStream.getLength());
    for (int i = 0; i < 7; ++i) {
      EXPECT_EQ(memStream.getData()[i], 'a' + i);
    }

    EXPECT_TRUE(bufStream.Next(reinterpret_cast<void**>(&buf), &len));
    EXPECT_EQ(10, len);

    for (int i = 0; i < 5; ++i) {
      buf[i] = static_cast<char>('a' + i);
    }

    bufStream.BackUp(5);
    bufStream.flush();

    EXPECT_EQ(12, memStream.getLength());
    for (int i = 0; i < 7; ++i) {
      EXPECT_EQ(memStream.getData()[i], 'a' + i);
    }

    for (int i = 0; i < 5; ++i) {
      EXPECT_EQ(memStream.getData()[i + 7], 'a' + i);
    }
#if ENABLE_METRICS
    EXPECT_EQ(metrics.IOCount.load(), 2);
#endif
  }

  TEST(BufferedOutputStream, protobuff_serialization) {
    MemoryOutputStream memStream(1024);
    MemoryPool* pool = getDefaultPool();

    uint64_t capacity = 20;
    uint64_t block = 10;
    WriterMetrics metrics;
    BufferedOutputStream bufStream(*pool, &memStream, capacity, block, &metrics);

    proto::PostScript ps;
    ps.set_footerlength(197934);
    ps.set_compression(proto::ZLIB);
    ps.add_version(6);
    ps.add_version(20);
    ps.set_metadatalength(100);
    ps.set_writerversion(789);
    ps.set_magic("protobuff_serialization");

    EXPECT_TRUE(ps.SerializeToZeroCopyStream(&bufStream));
    bufStream.flush();
    EXPECT_EQ(ps.ByteSizeLong(), memStream.getLength());

    proto::PostScript ps2;
    ps2.ParseFromArray(memStream.getData(), static_cast<int>(memStream.getLength()));

    EXPECT_EQ(ps.footerlength(), ps2.footerlength());
    EXPECT_EQ(ps.compression(), ps2.compression());
    EXPECT_EQ(ps.version(0), ps2.version(0));
    EXPECT_EQ(ps.version(1), ps2.version(1));
    EXPECT_EQ(ps.metadatalength(), ps2.metadatalength());
    EXPECT_EQ(ps.writerversion(), ps2.writerversion());
    EXPECT_EQ(ps.magic(), ps2.magic());
  }
}  // namespace orc
