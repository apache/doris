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

#include <thread>

#include "io/io_common.h"

namespace doris::io {

TEST(IOContextTest, CopiesKeepAnIndependentStopSnapshot) {
    IOContext source {.reader_type = ReaderType::READER_QUERY, .should_stop = true};
    source.expiration_time = 123;
    IOContext copied = source;
    IOContext assigned;
    assigned = source;
    source.should_stop = false;
    EXPECT_TRUE(copied.should_stop);
    EXPECT_TRUE(assigned.should_stop);
    EXPECT_FALSE(source.should_stop);
    EXPECT_EQ(copied.reader_type, ReaderType::READER_QUERY);
    EXPECT_EQ(assigned.expiration_time, 123);
    assigned.should_stop = false;
    EXPECT_TRUE(copied.should_stop);
}

TEST(IOContextTest, ConcurrentStopReadsAndSnapshots) {
    IOContext source;
    size_t observed[2] = {};
    // There is deliberately no lock between the stop writer and the reader, as in scanner
    // cancellation. Running this test under a race detector must not report should_stop.
    std::thread reader([&] {
        for (int i = 0; i < 100000; ++i) {
            IOContext copied = source;
            ++observed[static_cast<bool>(copied.should_stop)];
        }
    });
    for (int i = 0; i < 100000; ++i) {
        source.should_stop = (i % 2 == 0);
    }
    source.should_stop = true;
    reader.join();
    EXPECT_TRUE(source.should_stop);
    EXPECT_EQ(observed[0] + observed[1], 100000);
    EXPECT_LE(observed[1], 100000);
}

} // namespace doris::io
