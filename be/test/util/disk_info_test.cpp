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

#include "util/disk_info.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"

namespace doris {

TEST(DiskInfoTest, StripPartitionSuffix) {
    // Regular disks: the trailing digits are a partition number, strip them.
    EXPECT_EQ(DiskInfo::strip_partition_suffix("sda"), "sda");
    EXPECT_EQ(DiskInfo::strip_partition_suffix("sda1"), "sda");
    EXPECT_EQ(DiskInfo::strip_partition_suffix("sda15"), "sda");
    EXPECT_EQ(DiskInfo::strip_partition_suffix("nvme0n1p1"), "nvme0n1p");

    // Device-mapper (LVM) devices: dm-N is the whole device, not a partition
    // of some "dm" disk, so distinct dm-N devices must stay distinct.
    EXPECT_EQ(DiskInfo::strip_partition_suffix("dm-0"), "dm-0");
    EXPECT_EQ(DiskInfo::strip_partition_suffix("dm-1"), "dm-1");
    EXPECT_NE(DiskInfo::strip_partition_suffix("dm-0"), DiskInfo::strip_partition_suffix("dm-1"));
}

} // namespace doris
