// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.catalog;

import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DiskInfo implements Writable {
    public enum DiskState {
        ONLINE,
        OFFLINE
    }
    
    private static final long DEFAULT_CAPACITY_B = 1024 * 1024 * 1024 * 1024L; // 1T

    private String rootPath;
    private long totalCapacityB;
    private long availableCapacityB;
    private DiskState state;

    private DiskInfo() {
        // for persist
    }

    public DiskInfo(String rootPath) {
        this.rootPath = rootPath;
        this.totalCapacityB = DEFAULT_CAPACITY_B;
        this.availableCapacityB = DEFAULT_CAPACITY_B;
        this.state = DiskState.ONLINE;
    }

    public String getRootPath() {
        return rootPath;
    }

    public long getTotalCapacityB() {
        return totalCapacityB;
    }

    public void setTotalCapacityB(long totalCapacityB) {
        this.totalCapacityB = totalCapacityB;
    }

    public long getAvailableCapacityB() {
        return availableCapacityB;
    }

    public void setAvailableCapacityB(long availableCapacityB) {
        this.availableCapacityB = availableCapacityB;
    }

    public DiskState getState() {
        return state;
    }

    public void setState(DiskState state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return "DiskInfo [rootPath=" + rootPath + ", totalCapacityB=" + totalCapacityB + ", availableCapacityB="
                + availableCapacityB + ", state=" + state + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, rootPath);
        out.writeLong(totalCapacityB);
        out.writeLong(availableCapacityB);
        Text.writeString(out, state.name());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.rootPath = Text.readString(in);
        this.totalCapacityB = in.readLong();
        this.availableCapacityB = in.readLong();
        this.state = DiskState.valueOf(Text.readString(in));
    }

    public static DiskInfo read(DataInput in) throws IOException {
        DiskInfo diskInfo = new DiskInfo();
        diskInfo.readFields(in);
        return diskInfo;
    }
}
