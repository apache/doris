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

import org.apache.doris.common.Config;
import org.apache.doris.common.io.DiskUtils;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.MultiLoadMgr;
import org.apache.doris.thrift.TDiskInfo;

import java.util.ArrayList;
import java.util.List;

// Execute environment, used to save other module, need to singleton
public class ExecuteEnv {
    private static volatile ExecuteEnv INSTANCE;
    private MultiLoadMgr multiLoadMgr;
    private ConnectScheduler scheduler;
    private long startupTime;

    public static class DiskInfo {
        private String dirType;
        private String dir;

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

        private DiskUtils.Df spaceInfo;

        public DiskInfo(String dirType, String dir, DiskUtils.Df spaceInfo) {
            this.dirType = dirType;
            this.dir = dir;
            this.spaceInfo = spaceInfo;
        }
    }

    private List<DiskInfo> diskInfos;

    private ExecuteEnv() {
        multiLoadMgr = new MultiLoadMgr();
        scheduler = new ConnectScheduler(Config.qe_max_connection);
        startupTime = System.currentTimeMillis();
        diskInfos = new ArrayList<DiskInfo>() {{
                add(new DiskInfo("meta", Config.meta_dir, DiskUtils.df(Config.meta_dir)));
                add(new DiskInfo("log", Config.sys_log_dir, DiskUtils.df(Config.sys_log_dir)));
                add(new DiskInfo("audit-log", Config.audit_log_dir, DiskUtils.df(Config.audit_log_dir)));
                add(new DiskInfo("temp", Config.tmp_dir, DiskUtils.df(Config.tmp_dir)));
            }};
    }

    public static ExecuteEnv getInstance() {
        if (INSTANCE == null) {
            synchronized (ExecuteEnv.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ExecuteEnv();
                }
            }
        }
        return INSTANCE;
    }

    public ConnectScheduler getScheduler() {
        return scheduler;
    }

    public MultiLoadMgr getMultiLoadMgr() {
        return multiLoadMgr;
    }

    public long getStartupTime() {
        return startupTime;
    }

    public List<DiskInfo> getDiskInfos() {
        return this.diskInfos;
    }

    public List<DiskInfo> refreshAndGetDiskInfo(boolean refresh) {
        for (DiskInfo disk : diskInfos) {
            DiskUtils.Df df = DiskUtils.df(disk.dir);
            if (df != null) {
                disk.setSpaceInfo(df);
            }
        }
        return diskInfos;
    }

    public static List<TDiskInfo> toThrift(List<DiskInfo> diskInfos) {
        if (diskInfos == null) {
            return null;
        }
        List<TDiskInfo> r = new ArrayList<TDiskInfo>(diskInfos.size());
        for (DiskInfo d : diskInfos) {
            r.add(new TDiskInfo(d.getDirType(),
                        d.getDir(),
                        d.getSpaceInfo().fileSystem,
                        d.getSpaceInfo().blocks,
                        d.getSpaceInfo().used,
                        d.getSpaceInfo().available,
                        d.getSpaceInfo().useRate,
                        d.getSpaceInfo().mountedOn));
        }
        return r;
    }

    public static List<DiskInfo> fromThrift(List<TDiskInfo> diskInfos) {
        if (diskInfos == null) {
            return null;
        }
        List<DiskInfo> r = new ArrayList<DiskInfo>(diskInfos.size());
        for (TDiskInfo d : diskInfos) {
            DiskUtils.Df df = new DiskUtils.Df();
            df.fileSystem = d.getFilesystem();
            df.blocks = d.getBlocks();
            df.used = d.getUsed();
            df.available = d.getAvailable();
            df.useRate = d.getUseRate();
            df.mountedOn = d.getMountedOn();
            DiskInfo disk = new DiskInfo(d.getDirType(), d.getDir(), df);
            r.add(disk);
        }
        return r;
    }
}
