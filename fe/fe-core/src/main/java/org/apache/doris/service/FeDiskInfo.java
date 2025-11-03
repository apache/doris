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

package org.apache.doris.service;

import org.apache.doris.common.io.DiskUtils;
import org.apache.doris.thrift.TDiskInfo;

import java.util.ArrayList;
import java.util.List;

public class FeDiskInfo {
    private String dirType;
    private String dir;
    private DiskUtils.Df spaceInfo;

    public FeDiskInfo() {
    }

    public FeDiskInfo(String dirType, String dir, DiskUtils.Df spaceInfo) {
        this.dirType = dirType;
        this.dir = dir;
        this.spaceInfo = spaceInfo;
    }

    public String getDirType() {
        return dirType;
    }

    public void setDirType(String dirType) {
        this.dirType = dirType;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public DiskUtils.Df getSpaceInfo() {
        return spaceInfo;
    }

    public void setSpaceInfo(DiskUtils.Df spaceInfo) {
        this.spaceInfo = spaceInfo;
    }

    public TDiskInfo toThrift() {
        if (this.getSpaceInfo() == null) {
            return new TDiskInfo(getDirType(),
                getDir(),
                "",
                0,
                0,
                0,
                0,
                "");
        }
        return new TDiskInfo(getDirType(),
                getDir(),
                getSpaceInfo().fileSystem,
                getSpaceInfo().blocks,
                getSpaceInfo().used,
                getSpaceInfo().available,
                getSpaceInfo().useRate,
                getSpaceInfo().mountedOn);
    }

    public static FeDiskInfo fromThrift(TDiskInfo diskInfo) {
        DiskUtils.Df df = new DiskUtils.Df();
        df.fileSystem = diskInfo.getFilesystem();
        df.blocks = diskInfo.getBlocks();
        df.used = diskInfo.getUsed();
        df.available = diskInfo.getAvailable();
        df.useRate = diskInfo.getUseRate();
        df.mountedOn = diskInfo.getMountedOn();
        return new FeDiskInfo(diskInfo.getDirType(), diskInfo.getDir(), df);
    }

    public static List<TDiskInfo> toThrifts(List<FeDiskInfo> diskInfos) {
        if (diskInfos == null) {
            return null;
        }
        List<TDiskInfo> r = new ArrayList<TDiskInfo>(diskInfos.size());
        for (FeDiskInfo d : diskInfos) {
            r.add(d.toThrift());
        }
        return r;
    }

    public static List<FeDiskInfo> fromThrifts(List<TDiskInfo> diskInfos) {
        if (diskInfos == null) {
            return null;
        }
        List<FeDiskInfo> r = new ArrayList<FeDiskInfo>(diskInfos.size());
        for (TDiskInfo d : diskInfos) {
            r.add(FeDiskInfo.fromThrift(d));
        }
        return r;
    }
}
