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

package org.apache.doris.catalog;

import org.apache.doris.thrift.TResourceType;

import com.google.common.collect.ImmutableSortedMap;

// ResourceType used to identify what kind of resource
// Set /sys/block/sdx/queue/schedule/group_idle,slice_idle to zero
// One can experience an overall throughput drop if you have created multiple
// groups and put applications in that group which are not driving enough
// IO to keep disk busy. In that case set group_idle=0, and CFQ will not idle
// on individual groups and throughput should improve.
// From https://www.kernel.org/doc/Documentation/cgroups/blkio-controller.txt

public enum ResourceType {
    CPU_SHARE("CPU_SHARE", TResourceType.TRESOURCE_CPU_SHARE, 1000),
    IO_SHARE("IO_SHARE", TResourceType.TRESOURCE_IO_SHARE, 1000),
    SSD_READ_IOPS("SSD_READ_IOPS", TResourceType.TRESOURCE_SSD_READ_IOPS, 100000),
    SSD_WRITE_IOPS("SSD_WRITE_IOPS", TResourceType.TRESOURCE_SSD_WRITE_IOPS, 100000),
    SSD_READ_MBPS("SSD_READ_MBPS", TResourceType.TRESOURCE_SSD_READ_MBPS, 200),
    SSD_WRITE_MBPS("SSD_WRITE_MBPS", TResourceType.TRESOURCE_SSD_WRITE_MBPS, 200),
    HDD_READ_IOPS("HDD_READ_IOPS", TResourceType.TRESOURCE_HDD_READ_IOPS, 2000),
    HDD_WRITE_IOPS("HDD_WRITE_IOPS", TResourceType.TRESOURCE_HDD_WRITE_IOPS, 2000),
    HDD_READ_MBPS("HDD_READ_MBPS", TResourceType.TRESOURCE_HDD_READ_MBPS, 200),
    HDD_WRITE_MBPS("HDD_WRITE_MBPS", TResourceType.TRESOURCE_HDD_WRITE_MBPS, 200);

    static ImmutableSortedMap<String, ResourceType> typeByDesc;

    static {
        ImmutableSortedMap.Builder<String, ResourceType> builder =
                ImmutableSortedMap.orderedBy(String.CASE_INSENSITIVE_ORDER);
        for (ResourceType type : ResourceType.values()) {
            builder.put(type.desc, type);
        }
        typeByDesc = builder.build();
    }

    private final String desc;
    private final TResourceType tType;
    private final int defaultValue;

    private ResourceType(String desc, TResourceType tType, int value) {
        this.desc = desc;
        this.tType = tType;
        this.defaultValue = value;
    }

    public String getDesc() {
        return desc;
    }

    public int getDefaultValue() {
        return defaultValue;
    }

    public static ResourceType fromDesc(String desc) {
        return typeByDesc.get(desc);
    }

    public TResourceType toThrift() {
        return tType;
    }

    public static ResourceType fromThrift(TResourceType tType) {
        switch (tType) {
            case TRESOURCE_CPU_SHARE:
                return CPU_SHARE;
            case TRESOURCE_IO_SHARE:
                return IO_SHARE;
            case TRESOURCE_SSD_READ_IOPS:
                return SSD_READ_IOPS;
            case TRESOURCE_SSD_WRITE_IOPS:
                return SSD_WRITE_IOPS;
            case TRESOURCE_SSD_READ_MBPS:
                return SSD_READ_MBPS;
            case TRESOURCE_SSD_WRITE_MBPS:
                return SSD_WRITE_MBPS;
            case TRESOURCE_HDD_READ_IOPS:
                return HDD_READ_IOPS;
            case TRESOURCE_HDD_WRITE_IOPS:
                return HDD_WRITE_IOPS;
            case TRESOURCE_HDD_READ_MBPS:
                return HDD_READ_MBPS;
            case TRESOURCE_HDD_WRITE_MBPS:
                return HDD_WRITE_MBPS;
        }
        return null;
    }

    @Override
    public String toString() {
        return getDesc();
    }
}
