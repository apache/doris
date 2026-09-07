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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/partitioner/writer_assigner.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace doris {

TEST(WriterAssignerTest, IdentityPreservesLogicalPartition) {
    IdentityWriterAssigner assigner(3);
    std::vector<uint32_t> partition_ids {2, 0, 1, 2};
    std::vector<uint32_t> writer_ids;

    ASSERT_TRUE(assigner.assign(partition_ids, nullptr, partition_ids.size(), 64, writer_ids).ok());
    EXPECT_EQ(partition_ids, writer_ids);
}

TEST(WriterAssignerTest, IdentityRejectsInvalidLogicalPartition) {
    IdentityWriterAssigner assigner(2);
    std::vector<uint32_t> partition_ids {0, 2};
    std::vector<uint32_t> writer_ids;

    Status status = assigner.assign(partition_ids, nullptr, partition_ids.size(), 64, writer_ids);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("exceeds writer count"), std::string::npos);
}

TEST(WriterAssignerTest, SkewedRejectsInvalidLogicalPartition) {
    SkewedWriterAssigner assigner(4, 2, 1, 1, 1);
    std::vector<uint32_t> partition_ids {0, 4};
    std::vector<uint32_t> writer_ids;

    Status status = assigner.assign(partition_ids, nullptr, partition_ids.size(), 64, writer_ids);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("exceeds partition count"), std::string::npos);
}

} // namespace doris
