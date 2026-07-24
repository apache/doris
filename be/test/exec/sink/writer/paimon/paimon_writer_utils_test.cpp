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

#include "exec/sink/writer/paimon/paimon_writer_utils.h"

#include <gtest/gtest.h>

namespace doris {

TEST(PaimonWriterUtilsTest, KeepsConfiguredSizeWhenAdaptiveDisabled) {
    EXPECT_EQ(256L * 1024L * 1024L, get_paimon_write_buffer_size(256L * 1024L * 1024L, false, 500));
    EXPECT_EQ(256L * 1024L * 1024L, get_paimon_write_buffer_size(256L * 1024L * 1024L, true, 0));
}

TEST(PaimonWriterUtilsTest, ShrinksBufferByBucketThreshold) {
    EXPECT_EQ(256L * 1024L * 1024L, get_paimon_write_buffer_size(256L * 1024L * 1024L, true, 10));
    EXPECT_EQ(128L * 1024L * 1024L, get_paimon_write_buffer_size(256L * 1024L * 1024L, true, 50));
    EXPECT_EQ(64L * 1024L * 1024L, get_paimon_write_buffer_size(256L * 1024L * 1024L, true, 200));
    EXPECT_EQ(32L * 1024L * 1024L, get_paimon_write_buffer_size(256L * 1024L * 1024L, true, 500));
}

TEST(PaimonWriterUtilsTest, NeverGrowsBeyondConfiguredSize) {
    EXPECT_EQ(16L * 1024L * 1024L, get_paimon_write_buffer_size(16L * 1024L * 1024L, true, 500));
    EXPECT_EQ(48L * 1024L * 1024L, get_paimon_write_buffer_size(48L * 1024L * 1024L, true, 200));
}

} // namespace doris
