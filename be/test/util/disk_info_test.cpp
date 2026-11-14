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

TEST(DiskInfoTest, DiskNameFromSysfsPath) {
    // Partitions: sysfs nests a partition's directory one level below its
    // whole-device parent, so the parent's name is what matters, not any
    // naming convention in the partition's own name.
    EXPECT_EQ(DiskInfo::disk_name_from_sysfs_path(
                      "/sys/devices/pci0000:00/ata1/host0/target0:0:0/0:0:0:0/block/sda/sda1",
                      /*is_partition=*/true),
              "sda");
    EXPECT_EQ(DiskInfo::disk_name_from_sysfs_path(
                      "/sys/devices/pci0000:00/nvme0/nvme0n1/nvme0n1p1", /*is_partition=*/true),
              "nvme0n1");
    EXPECT_EQ(DiskInfo::disk_name_from_sysfs_path("/sys/devices/platform/mmcblk0/mmcblk0p1",
                                                   /*is_partition=*/true),
              "mmcblk0");

    // Whole devices (no "partition" file in their sysfs directory): keep the
    // leaf name as-is, including ones with trailing digits that are not a
    // partition number, like md127, and device-mapper's dm-N.
    EXPECT_EQ(DiskInfo::disk_name_from_sysfs_path("/sys/devices/virtual/block/md127",
                                                   /*is_partition=*/false),
              "md127");
    EXPECT_EQ(DiskInfo::disk_name_from_sysfs_path("/sys/devices/virtual/block/dm-0",
                                                   /*is_partition=*/false),
              "dm-0");
    EXPECT_EQ(DiskInfo::disk_name_from_sysfs_path("/sys/devices/virtual/block/dm-1",
                                                   /*is_partition=*/false),
              "dm-1");
    EXPECT_NE(DiskInfo::disk_name_from_sysfs_path("/sys/devices/virtual/block/dm-0", false),
              DiskInfo::disk_name_from_sysfs_path("/sys/devices/virtual/block/dm-1", false));
}

} // namespace doris
